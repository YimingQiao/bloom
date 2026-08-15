#!/usr/bin/env python3
"""Validate and summarize the full prepared/instant benchmark matrix."""

import argparse
import csv
import gzip
import io
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median


METHODS = ("prepared", "instant")
STATES = ("warm", "cold")
WORKLOADS = {
    "ceb": ("CEB IMDB", 3133, {"warm": 1, "cold": 1}),
    "job_uncompressed": ("JOB (uncompressed)", 113, {"warm": 2, "cold": 3}),
    "job": ("JOB (compressed)", 113, {"warm": 2, "cold": 3}),
    "stats_ceb": ("STATS-CEB", 146, {"warm": 2, "cold": 3}),
    "ceb_stack": ("CEB Stack", 6191, {"warm": 1, "cold": 1}),
    "tpch_sf10": ("TPC-H SF10", 22, {"warm": 2, "cold": 3}),
    "tpcds_sf10": ("TPC-DS SF10", 99, {"warm": 2, "cold": 3}),
    "appian": ("Appian", 8, {"warm": 2, "cold": 3}),
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def load_matrix(root):
    matrix = {}
    for state in STATES:
        for workload, (_, query_count, repetitions) in WORKLOADS.items():
            expected = query_count * repetitions[state]
            for method in METHODS:
                path = root / state / workload / method / "results.jsonl"
                rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
                if len(rows) != expected:
                    raise RuntimeError(f"{path}: expected {expected} rows, found {len(rows)}")
                matrix[state, workload, method] = rows
    return matrix


def grouped(rows, field):
    values = defaultdict(list)
    for row in rows:
        values[row["query"]].append(row[field])
    return values


def query_medians(rows, field):
    return {query: median(values) for query, values in grouped(rows, field).items()}


def boundary_tie_equivalent(root, state, workload, query):
    if workload != "ceb_stack" or not query.startswith("q13__"):
        return False
    outputs = {}
    for method in METHODS:
        raw_path = root / state / workload / method / "raw" / f"{query}-r1.log.gz"
        if not raw_path.is_file():
            return False
        with gzip.open(raw_path, "rt", encoding="utf-8") as handle:
            raw = handle.read()
        try:
            stdout = raw.split("[stdout]\n", 1)[1].split("\n[stderr]\n", 1)[0]
        except IndexError:
            return False
        outputs[method] = [row for row in csv.reader(io.StringIO(stdout)) if len(row) == 2]
    prepared = Counter(map(tuple, outputs["prepared"]))
    instant = Counter(map(tuple, outputs["instant"]))
    prepared_only = list((prepared - instant).elements())
    instant_only = list((instant - prepared).elements())
    if not prepared_only or len(prepared_only) != len(instant_only):
        return False
    try:
        prepared_boundary = min(int(row[1]) for row in outputs["prepared"])
        instant_boundary = min(int(row[1]) for row in outputs["instant"])
    except (IndexError, ValueError):
        return False
    differing_counts = {int(row[1]) for row in prepared_only + instant_only}
    return (
        len(outputs["prepared"]) == 100
        and len(outputs["instant"]) == 100
        and differing_counts == {prepared_boundary} == {instant_boundary}
    )


def validate(matrix, root):
    failures = []
    measured_runs = 0
    cold_runs = 0
    cold_residency_checks = 0
    hash_matches = 0
    query_pairs = 0
    boundary_ties = []
    for state in STATES:
        for workload, (_, query_count, repetitions) in WORKLOADS.items():
            method_hashes = {}
            expected_repetitions = repetitions[state]
            for method in METHODS:
                rows = matrix[state, workload, method]
                measured_runs += len(rows)
                failures.extend(
                    (state, workload, method, row["query"], "execution")
                    for row in rows
                    if row["returncode"] != 0 or row["query_ms"] is None
                )
                if state == "cold":
                    cold_runs += len(rows)
                    checked_rows = [row for row in rows if row.get("residency_checked", True)]
                    cold_residency_checks += len(checked_rows)
                    failures.extend(
                        (state, workload, method, row["query"], "resident")
                        for row in checked_rows
                        if row["resident_before"] != 0
                    )
                by_hash = grouped(rows, "result_sha256")
                if len(by_hash) != query_count:
                    failures.append((state, workload, method, "query_count", len(by_hash)))
                method_hashes[method] = {}
                for query, hashes in by_hash.items():
                    if len(hashes) != expected_repetitions:
                        failures.append((state, workload, method, query, "repetition_count"))
                    unique = set(hashes)
                    if len(unique) != 1:
                        failures.append((state, workload, method, query, "unstable_hash"))
                    method_hashes[method][query] = next(iter(unique))
            for query, prepared_hash in method_hashes["prepared"].items():
                query_pairs += 1
                if prepared_hash == method_hashes["instant"].get(query):
                    hash_matches += 1
                elif boundary_tie_equivalent(root, state, workload, query):
                    boundary_ties.append((state, workload, query))
                else:
                    failures.append((state, workload, query, "prepared_instant_hash"))
    if failures:
        raise RuntimeError("validation failed: " + repr(failures[:30]))
    return {
        "measured_runs": measured_runs,
        "cold_runs": cold_runs,
        "cold_residency_checks": cold_residency_checks,
        "query_pairs": query_pairs,
        "hash_matches": hash_matches,
        "boundary_ties": len(boundary_ties),
        "boundary_tie_queries": boundary_ties,
    }


def fmt_seconds(milliseconds):
    return f"{milliseconds / 1000:.3f} s"


def main():
    args = parse_args()
    root = args.results.resolve()
    matrix = load_matrix(root)
    checks = validate(matrix, root)
    summary = {"validation": checks, "states": {}}

    for state in STATES:
        summary["states"][state] = {}
        for workload in WORKLOADS:
            times = {
                method: sum(query_medians(matrix[state, workload, method], "query_ms").values())
                for method in METHODS
            }
            inputs = {
                method: sum(query_medians(matrix[state, workload, method], "input_bytes").values())
                for method in METHODS
            }
            summary["states"][state][workload] = {
                "time_ms": times,
                "input_bytes": inputs,
                "instant_over_prepared": times["instant"] / times["prepared"],
            }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix(".json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )

    query_csv = args.output.parent / "QUERY_RESULTS.csv"
    with query_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "state",
                "workload",
                "query",
                "prepared_ms",
                "instant_ms",
                "instant_over_prepared",
                "prepared_input_bytes",
                "instant_input_bytes",
                "result_hash_match",
                "result_status",
            ]
        )
        for state in STATES:
            for workload in WORKLOADS:
                times = {
                    method: query_medians(matrix[state, workload, method], "query_ms")
                    for method in METHODS
                }
                inputs = {
                    method: query_medians(matrix[state, workload, method], "input_bytes")
                    for method in METHODS
                }
                hashes = {
                    method: {
                        query: next(iter(set(values)))
                        for query, values in grouped(
                            matrix[state, workload, method], "result_sha256"
                        ).items()
                    }
                    for method in METHODS
                }
                for query in sorted(times["prepared"]):
                    hash_match = hashes["prepared"][query] == hashes["instant"][query]
                    tie_equivalent = (
                        not hash_match and boundary_tie_equivalent(root, state, workload, query)
                    )
                    writer.writerow(
                        [
                            state,
                            workload,
                            query,
                            f"{times['prepared'][query]:.3f}",
                            f"{times['instant'][query]:.3f}",
                            f"{times['instant'][query] / times['prepared'][query]:.6f}",
                            f"{inputs['prepared'][query]:.0f}",
                            f"{inputs['instant'][query]:.0f}",
                            str(hash_match).lower(),
                            "match" if hash_match else "boundary_tie" if tie_equivalent else "mismatch",
                        ]
                    )

    runs_csv = args.output.parent / "RUNS.csv"
    fields = [
        "state",
        "workload",
        "method",
        "query",
        "repetition",
        "query_ms",
        "process_wall_ms",
        "input_bytes",
        "resident_before",
        "resident_after",
        "result_sha256",
        "returncode",
        "residency_checked",
    ]
    with runs_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for state in STATES:
            for workload in WORKLOADS:
                for method in METHODS:
                    for row in sorted(
                        matrix[state, workload, method],
                        key=lambda item: (item["query"], item["repetition"]),
                    ):
                        writer.writerow(
                            {
                                field: row.get(field, True if field == "residency_checked" else None)
                                for field in fields
                            }
                        )

    lines = [
        "# Full prepared/instant benchmark result",
        "",
        "Complete-query time includes optimization, excitation, materialization, instant sampling, and final execution. "
        "Prepared sample loading happens before the timer. Normal workloads use two warm and three cold repetitions; "
        "CEB IMDB and CEB Stack use one repetition per state. Each cell is the sum of per-query medians.",
        "",
    ]
    for state, title in (("warm", "Warm base data"), ("cold", "Cold SSD base data")):
        lines.extend(
            [
                f"## {title}",
                "",
                "| Workload | Prepared | Instant | Instant / prepared |",
                "|---|---:|---:|---:|",
            ]
        )
        for workload, (label, _, _) in WORKLOADS.items():
            result = summary["states"][state][workload]
            lines.append(
                f"| {label} | {fmt_seconds(result['time_ms']['prepared'])} | "
                f"{fmt_seconds(result['time_ms']['instant'])} | "
                f"{result['instant_over_prepared']:.3f}x |"
            )
        prepared_total = sum(
            summary["states"][state][workload]["time_ms"]["prepared"] for workload in WORKLOADS
        )
        instant_total = sum(
            summary["states"][state][workload]["time_ms"]["instant"] for workload in WORKLOADS
        )
        lines.extend(
            [
                "",
                f"Aggregate instant/prepared over the listed workload totals: **{instant_total / prepared_total:.3f}x**.",
                "",
            ]
        )

    lines.extend(
        [
            "## Cold physical input",
            "",
            "| Workload | Prepared | Instant | Instant / prepared |",
            "|---|---:|---:|---:|",
        ]
    )
    for workload, (label, _, _) in WORKLOADS.items():
        values = summary["states"]["cold"][workload]["input_bytes"]
        lines.append(
            f"| {label} | {values['prepared'] / 2**30:.3f} GiB | "
            f"{values['instant'] / 2**30:.3f} GiB | "
            f"{values['instant'] / values['prepared']:.3f}x |"
        )

    lines.extend(
        [
            "",
            "## Validation",
            "",
            f"- {checks['measured_runs']}/{checks['measured_runs']} measured executions succeeded.",
            f"- All {checks['cold_runs']} cold executions issued base-file eviction; "
            f"{checks['cold_residency_checks']}/{checks['cold_residency_checks']} scheduled full-file residency gates observed zero bytes.",
            f"- Prepared and instant result-bag hashes match for {checks['hash_matches']}/{checks['query_pairs']} state/query pairs.",
            f"- {checks['boundary_ties']} additional CEB Stack Q13 pairs differ only at a non-total top-100 boundary: "
            "both outputs contain 100 rows, and every substituted row has the same count at the shared cutoff.",
            "",
        ]
    )
    args.output.write_text("\n".join(lines), encoding="utf-8")
    print(args.output)
    print(query_csv)
    print(runs_csv)


if __name__ == "__main__":
    main()
