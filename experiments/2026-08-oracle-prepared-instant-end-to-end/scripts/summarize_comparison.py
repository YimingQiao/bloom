#!/usr/bin/env python3
"""Validate and summarize the complete oracle/prepared/instant matrix."""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median


METHODS = ("oracle", "prepared", "instant")
STATES = ("warm", "cold")
WORKLOADS = {
    "job": ("JOB", 113),
    "tpch_sf10": ("TPC-H SF10", 22),
    "appian": ("Appian", 8),
}
REPETITIONS = {"warm": 2, "cold": 3}
TIE_AMBIGUOUS = {("appian", "q05"), ("appian", "q08")}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument(
        "--oracle-results",
        type=Path,
        help="optional matrix root supplying unchanged oracle records",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def load_matrix(root, oracle_root=None):
    matrix = {}
    for state in STATES:
        for workload, (_, query_count) in WORKLOADS.items():
            for method in METHODS:
                method_root = oracle_root if method == "oracle" and oracle_root else root
                path = method_root / state / workload / method / "results.jsonl"
                rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
                expected = query_count * REPETITIONS[state]
                if len(rows) != expected:
                    raise RuntimeError(f"{path}: expected {expected} rows, found {len(rows)}")
                matrix[state, workload, method] = rows
    return matrix


def query_medians(rows, field):
    values = defaultdict(list)
    for row in rows:
        values[row["query"]].append(row[field])
    return {query: median(samples) for query, samples in values.items()}


def total_median(matrix, state, workload, method, field):
    return sum(query_medians(matrix[state, workload, method], field).values())


def validate(matrix):
    failures = []
    total_runs = 0
    cold_runs = 0
    oracle_runs = 0
    bloom_hash_matches = 0
    oracle_hash_matches = 0
    oracle_hash_comparable = 0
    for key, rows in matrix.items():
        state, workload, method = key
        total_runs += len(rows)
        failures.extend((state, workload, method, row["query"]) for row in rows if row["returncode"] != 0)
        if state == "cold":
            cold_runs += len(rows)
            failures.extend(
                (state, workload, method, row["query"], "resident")
                for row in rows
                if row["resident_before"] != 0
            )
        if method == "oracle":
            oracle_runs += len(rows)
            failures.extend(
                (state, workload, method, row["query"], "cache_mutation")
                for row in rows
                if row["oracle_cache_changed"]
            )

    for state in STATES:
        for workload, (_, query_count) in WORKLOADS.items():
            hashes = {}
            for method in METHODS:
                by_query = defaultdict(set)
                for row in matrix[state, workload, method]:
                    by_query[row["query"]].add(row["result_sha256"])
                for query, values in by_query.items():
                    if len(values) != 1:
                        failures.append((state, workload, method, query, "unstable_hash"))
                hashes[method] = {query: next(iter(values)) for query, values in by_query.items()}
            for query in hashes["prepared"]:
                if hashes["prepared"][query] != hashes["instant"][query]:
                    failures.append((state, workload, query, "prepared_instant_hash"))
                else:
                    bloom_hash_matches += 1
                if (workload, query) not in TIE_AMBIGUOUS:
                    oracle_hash_comparable += 1
                    if hashes["prepared"][query] != hashes["oracle"][query]:
                        failures.append((state, workload, query, "oracle_hash"))
                    else:
                        oracle_hash_matches += 1
            if len(hashes["prepared"]) != query_count:
                failures.append((state, workload, "query_count"))
    if failures:
        raise RuntimeError("validation failed: " + repr(failures[:20]))
    return {
        "total_runs": total_runs,
        "cold_runs": cold_runs,
        "oracle_runs": oracle_runs,
        "bloom_hash_matches": bloom_hash_matches,
        "oracle_hash_matches": oracle_hash_matches,
        "oracle_hash_comparable": oracle_hash_comparable,
    }


def fmt_seconds(ms):
    return f"{ms / 1000:.3f} s"


def main():
    args = parse_args()
    matrix = load_matrix(
        args.results.resolve(),
        args.oracle_results.resolve() if args.oracle_results else None,
    )
    checks = validate(matrix)
    summary = {"validation": checks, "states": {}}

    for state in STATES:
        state_summary = {}
        for workload in WORKLOADS:
            times = {
                method: total_median(matrix, state, workload, method, "query_ms") for method in METHODS
            }
            io = {
                method: total_median(matrix, state, workload, method, "input_bytes") for method in METHODS
            }
            state_summary[workload] = {
                "time_ms": times,
                "input_bytes": io,
                "instant_over_prepared": times["instant"] / times["prepared"],
            }
        summary["states"][state] = state_summary

    audits = {}
    for workload in WORKLOADS:
        audit_path = args.results.resolve() / "cold" / workload / "prepared" / "preload_audit.json"
        if audit_path.is_file():
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            audits[workload] = {
                "base_resident_bytes": audit["resident_after"],
                "physical_input_bytes": audit["physical_input_bytes"],
            }
            continue
        # Backward compatibility for result sets produced before preload audit
        # became an independent artifact.
        rows = matrix["cold", workload, "prepared"]
        embedded = next((row for row in rows if row["preload_resident_bytes"] is not None), None)
        if embedded:
            audits[workload] = {
                "base_resident_bytes": embedded["preload_resident_bytes"],
                "physical_input_bytes": embedded["preload_input_bytes"],
            }
    summary["prepared_preload_audit"] = audits

    job_prepared = query_medians(matrix["cold", "job", "prepared"], "query_ms")
    job_instant = query_medians(matrix["cold", "job", "instant"], "query_ms")
    job_prepared_io = query_medians(matrix["cold", "job", "prepared"], "input_bytes")
    job_instant_io = query_medians(matrix["cold", "job", "instant"], "input_bytes")
    buckets = []
    for label, lower, upper in (("<100 ms", 0, 100), ("100-500 ms", 100, 500), (">=500 ms", 500, float("inf"))):
        queries = [query for query, value in job_prepared.items() if lower <= value < upper]
        prepared_ms = sum(job_prepared[query] for query in queries)
        instant_ms = sum(job_instant[query] for query in queries)
        prepared_io = sum(job_prepared_io[query] for query in queries)
        instant_io = sum(job_instant_io[query] for query in queries)
        buckets.append(
            {
                "bucket": label,
                "queries": len(queries),
                "prepared_ms": prepared_ms,
                "instant_ms": instant_ms,
                "time_ratio": instant_ms / prepared_ms,
                "io_ratio": instant_io / prepared_io,
            }
        )
    summary["job_cold_buckets"] = buckets

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix(".json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    query_results_path = args.output.parent / "QUERY_RESULTS.csv"
    with query_results_path.open("w", encoding="utf-8", newline="") as query_results:
        writer = csv.writer(query_results)
        writer.writerow(
            [
                "state",
                "workload",
                "query",
                "oracle_ms",
                "prepared_ms",
                "instant_ms",
                "instant_over_prepared",
                "prepared_input_bytes",
                "instant_input_bytes",
                "prepared_instant_hash_match",
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
                    for method in ("prepared", "instant")
                }
                hashes = {}
                for method in ("prepared", "instant"):
                    by_query = defaultdict(set)
                    for row in matrix[state, workload, method]:
                        by_query[row["query"]].add(row["result_sha256"])
                    hashes[method] = {query: next(iter(values)) for query, values in by_query.items()}
                for query in sorted(times["prepared"]):
                    writer.writerow(
                        [
                            state,
                            workload,
                            query,
                            f"{times['oracle'][query]:.3f}",
                            f"{times['prepared'][query]:.3f}",
                            f"{times['instant'][query]:.3f}",
                            f"{times['instant'][query] / times['prepared'][query]:.6f}",
                            f"{inputs['prepared'][query]:.0f}",
                            f"{inputs['instant'][query]:.0f}",
                            str(hashes["prepared"][query] == hashes["instant"][query]).lower(),
                        ]
                    )

    run_results_path = args.output.parent / "RUNS.csv"
    with run_results_path.open("w", encoding="utf-8", newline="") as run_results:
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
            "oracle_cache_changed",
        ]
        writer = csv.DictWriter(run_results, fieldnames=fields)
        writer.writeheader()
        for state in STATES:
            for workload in WORKLOADS:
                for method in METHODS:
                    for row in sorted(
                        matrix[state, workload, method],
                        key=lambda item: (item["query"], item["repetition"]),
                    ):
                        writer.writerow({field: row[field] for field in fields})

    lines = [
        "# End-to-end result",
        "",
        "Complete-query time includes optimization, adaptive excitation, materialization, and final execution. "
        "Each cell is the sum of per-query medians. Warm has two repetitions; cold SSD has three.",
        "",
    ]
    for state, title in (("warm", "Warm base data"), ("cold", "Cold SSD base data")):
        lines.extend(
            [
                f"## {title}",
                "",
                "| Workload | Oracle | Prepared | Instant | Instant / prepared |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for workload, (label, _) in WORKLOADS.items():
            data = summary["states"][state][workload]
            times = data["time_ms"]
            lines.append(
                f"| {label} | {fmt_seconds(times['oracle'])} | {fmt_seconds(times['prepared'])} | "
                f"{fmt_seconds(times['instant'])} | {data['instant_over_prepared']:.3f}x |"
            )
        prepared_total = sum(summary["states"][state][w]["time_ms"]["prepared"] for w in WORKLOADS)
        instant_total = sum(summary["states"][state][w]["time_ms"]["instant"] for w in WORKLOADS)
        lines.extend(["", f"Combined instant/prepared: **{instant_total / prepared_total:.3f}x**.", ""])

    lines.extend(
        [
            "## Cold physical input",
            "",
            "| Workload | Prepared | Instant | Instant / prepared |",
            "|---|---:|---:|---:|",
        ]
    )
    for workload, (label, _) in WORKLOADS.items():
        data = summary["states"]["cold"][workload]["input_bytes"]
        lines.append(
            f"| {label} | {data['prepared'] / 2**30:.3f} GiB | {data['instant'] / 2**30:.3f} GiB | "
            f"{data['instant'] / data['prepared']:.3f}x |"
        )

    lines.extend(
        [
            "",
            "## Prepared preload audit",
            "",
            "The prepared sample cache was loaded after evicting the base database. These bytes are catalog pages; "
            "the preload did not scan table payloads.",
            "",
            "| Workload | Base data made resident | Physical input |",
            "|---|---:|---:|",
        ]
    )
    for workload, (label, _) in WORKLOADS.items():
        audit = audits[workload]
        lines.append(
            f"| {label} | {audit['base_resident_bytes'] / 2**20:.3f} MiB | "
            f"{audit['physical_input_bytes'] / 2**20:.3f} MiB |"
        )

    lines.extend(
        [
            "",
            "## JOB cold-query buckets",
            "",
            "| Prepared query time | Queries | Prepared sum | Instant sum | Time ratio | Input ratio |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for bucket in buckets:
        lines.append(
            f"| {bucket['bucket']} | {bucket['queries']} | {fmt_seconds(bucket['prepared_ms'])} | "
            f"{fmt_seconds(bucket['instant_ms'])} | {bucket['time_ratio']:.3f}x | {bucket['io_ratio']:.3f}x |"
        )

    lines.extend(
        [
            "",
            "## Validation",
            "",
            f"- {checks['total_runs']}/{checks['total_runs']} measured executions succeeded.",
            f"- {checks['cold_runs']}/{checks['cold_runs']} cold executions started with zero resident base-data bytes.",
            f"- {checks['oracle_runs']}/{checks['oracle_runs']} oracle executions left the primed cardinality cache unchanged.",
            f"- Prepared and instant result-bag hashes match for {checks['bloom_hash_matches']}/{checks['bloom_hash_matches']} state/query pairs.",
            f"- Oracle hashes match for {checks['oracle_hash_matches']}/{checks['oracle_hash_comparable']} comparable state/query pairs. "
            "Appian Q5/Q8 are excluded because non-total ORDER BY keys make the LIMIT 500 boundary tie-ambiguous across DuckDB versions.",
            "",
        ]
    )
    args.output.write_text("\n".join(lines), encoding="utf-8")
    print(args.output)
    print(query_results_path)
    print(run_results_path)


if __name__ == "__main__":
    main()
