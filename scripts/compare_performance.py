#!/usr/bin/env python3
"""Compare Bloom performance between a base and candidate benchmark runner.

The comparison intentionally covers only JOB and TPC-H SF1. Each round runs
the full workload once per binary and alternates which binary runs first. A
query slowdown of at least 10% is reported, while the CI gate follows DuckDB's
aggregate policy: fail when the workload geomean grows by at least 10% or 50ms.
"""

import argparse
import math
import os
import re
import shlex
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUN_BENCHMARK = ROOT / "scripts" / "run_benchmark.py"
WORKLOAD_QUERY_COUNTS = {"imdb": 113, "tpch_sf1": 22}
TIMING = re.compile(
    r"^(benchmark/\S+\.benchmark)\t\d+\t"
    r"((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$"
)


def positive_integer(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-runner", type=Path, required=True)
    parser.add_argument("--candidate-runner", type=Path, required=True)
    parser.add_argument(
        "--workload",
        action="append",
        choices=sorted(WORKLOAD_QUERY_COUNTS),
        dest="workloads",
        help="Workload to compare; repeat to select both (default: both)",
    )
    parser.add_argument("--threads", type=positive_integer, default=1)
    parser.add_argument("--timed-runs", type=positive_integer, default=5)
    parser.add_argument(
        "--rounds",
        type=positive_integer,
        default=2,
        help="Full paired workload rounds (default: 2, giving 10 samples per binary)",
    )
    parser.add_argument("--regression-ratio", type=float, default=1.10)
    parser.add_argument("--geomean-regression-seconds", type=float, default=0.050)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "benchmark_results" / "performance_regression",
    )
    parser.add_argument("--no-fail", action="store_true", help="Report regressions without returning failure")
    return parser.parse_args()


def parse_timings(output, expected_runs):
    timings = defaultdict(list)
    for line in output.splitlines():
        match = TIMING.match(line)
        if match:
            timings[match.group(1)].append(float(match.group(2)))
    incomplete = {
        query: len(samples)
        for query, samples in timings.items()
        if len(samples) != expected_runs
    }
    if not timings or incomplete:
        detail = incomplete or "no timing rows"
        raise RuntimeError(f"incomplete benchmark timings: {detail}")
    return timings


def run_once(args, workload, side, runner, round_index):
    result_path = args.out_dir / f"{workload}.{side}.round{round_index + 1}.out"
    log_path = args.out_dir / f"{workload}.{side}.round{round_index + 1}.log"
    command = [
        sys.executable,
        str(RUN_BENCHMARK),
        "--workload",
        workload,
        "--runner",
        str(runner),
        "--threads",
        str(args.threads),
        "--timed-runs",
        str(args.timed_runs),
        "--out",
        str(result_path),
    ]
    print(f">>> {workload} {side} round {round_index + 1}: {shlex.join(command)}", flush=True)
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    log_path.write_text(
        f"$ {shlex.join(command)}\n\n[stdout]\n{completed.stdout}\n[stderr]\n{completed.stderr}",
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{workload} {side} round {round_index + 1} failed with exit code "
            f"{completed.returncode}; see {log_path}"
        )
    return parse_timings(completed.stderr, args.timed_runs)


def merge_timings(target, current):
    if target and target.keys() != current.keys():
        missing = sorted(target.keys() - current.keys())
        added = sorted(current.keys() - target.keys())
        raise RuntimeError(f"query-set changed between rounds: missing={missing}, added={added}")
    for query, samples in current.items():
        target[query].extend(samples)


def geometric_mean(values):
    if not values or any(value <= 0 for value in values):
        raise RuntimeError("geometric mean requires positive timings")
    return math.exp(math.fsum(math.log(value) for value in values) / len(values))


def summarize_timings(workload, base_timings, candidate_timings, expected_samples, regression_ratio):
    if base_timings.keys() != candidate_timings.keys():
        base_only = sorted(base_timings.keys() - candidate_timings.keys())
        candidate_only = sorted(candidate_timings.keys() - base_timings.keys())
        raise RuntimeError(f"query-set mismatch: base-only={base_only}, candidate-only={candidate_only}")
    expected_queries = WORKLOAD_QUERY_COUNTS[workload]
    if len(base_timings) != expected_queries:
        raise RuntimeError(f"{workload} produced {len(base_timings)} queries; expected {expected_queries}")

    rows = []
    for query in sorted(base_timings):
        base_samples = base_timings[query]
        candidate_samples = candidate_timings[query]
        if len(base_samples) != expected_samples or len(candidate_samples) != expected_samples:
            raise RuntimeError(
                f"{query} has {len(base_samples)} base and {len(candidate_samples)} candidate "
                f"samples; expected {expected_samples}"
            )
        base_median = statistics.median(base_samples)
        candidate_median = statistics.median(candidate_samples)
        if base_median <= 0 or candidate_median <= 0:
            raise RuntimeError(f"{query} produced a non-positive timing")
        rows.append(
            {
                "query": query,
                "base": base_median,
                "candidate": candidate_median,
                "ratio": candidate_median / base_median,
            }
        )

    base_geomean = geometric_mean([row["base"] for row in rows])
    candidate_geomean = geometric_mean([row["candidate"] for row in rows])
    return {
        "workload": workload,
        "rows": rows,
        "query_regressions": [row for row in rows if row["ratio"] >= regression_ratio],
        "base_geomean": base_geomean,
        "candidate_geomean": candidate_geomean,
        "geomean_ratio": candidate_geomean / base_geomean,
    }


def format_seconds(value):
    if abs(value) >= 1:
        return f"{value:.3f} s"
    return f"{value * 1000:.1f} ms"


def append_step_summary(lines):
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n")


def emit_annotation(level, title, message):
    if os.getenv("GITHUB_ACTIONS") == "true":
        print(f"::{level} title={title}::{message}")


def report_summary(summary, regression_ratio, geomean_seconds, no_fail):
    base = summary["base_geomean"]
    candidate = summary["candidate_geomean"]
    ratio = summary["geomean_ratio"]
    delta = candidate - base
    gate_failed = ratio >= regression_ratio or delta >= geomean_seconds
    status = "REGRESSION" if gate_failed else "PASS"
    lines = [
        f"## Performance regression: `{summary['workload']}` — {status}",
        "",
        "| Metric | Base | Candidate | Change |",
        "| --- | ---: | ---: | ---: |",
        f"| Query geomean | {format_seconds(base)} | {format_seconds(candidate)} "
        f"| {(ratio - 1) * 100:+.1f}% |",
        "",
    ]
    regressions = summary["query_regressions"]
    if regressions:
        lines += [
            f"Queries with median slowdown of at least {(regression_ratio - 1) * 100:.0f}%:",
            "",
            "| Query | Base | Candidate | Change |",
            "| --- | ---: | ---: | ---: |",
        ]
        for row in sorted(regressions, key=lambda item: item["ratio"], reverse=True):
            lines.append(
                f"| `{Path(row['query']).stem}` | {format_seconds(row['base'])} "
                f"| {format_seconds(row['candidate'])} | {(row['ratio'] - 1) * 100:+.1f}% |"
            )
            emit_annotation(
                "warning",
                "Performance query regression",
                f"{summary['workload']} {Path(row['query']).stem} slowed by "
                f"{(row['ratio'] - 1) * 100:.1f}%",
            )
        lines.append("")
    if gate_failed:
        message = (
            f"{summary['workload']} geomean changed from {format_seconds(base)} to "
            f"{format_seconds(candidate)} ({(ratio - 1) * 100:+.1f}%, {format_seconds(delta)})"
        )
        emit_annotation("warning" if no_fail else "error", "Performance geomean regression", message)
    print("\n".join(lines))
    append_step_summary(lines)
    return gate_failed


def compare_workload(args, workload):
    timings = {"base": defaultdict(list), "candidate": defaultdict(list)}
    runners = {"base": args.base_runner, "candidate": args.candidate_runner}
    for round_index in range(args.rounds):
        order = ("base", "candidate") if round_index % 2 == 0 else ("candidate", "base")
        for side in order:
            current = run_once(args, workload, side, runners[side], round_index)
            merge_timings(timings[side], current)
    return summarize_timings(
        workload,
        timings["base"],
        timings["candidate"],
        args.rounds * args.timed_runs,
        args.regression_ratio,
    )


def main():
    args = parse_args()
    args.base_runner = args.base_runner.resolve()
    args.candidate_runner = args.candidate_runner.resolve()
    for label, runner in (("base", args.base_runner), ("candidate", args.candidate_runner)):
        if not runner.is_file():
            raise SystemExit(f"{label} benchmark runner not found: {runner}")
    if not math.isfinite(args.regression_ratio) or args.regression_ratio <= 1:
        raise SystemExit("--regression-ratio must be greater than 1")
    if not math.isfinite(args.geomean_regression_seconds) or args.geomean_regression_seconds <= 0:
        raise SystemExit("--geomean-regression-seconds must be greater than zero")

    args.out_dir = args.out_dir.resolve()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    workloads = args.workloads or list(WORKLOAD_QUERY_COUNTS)
    failed = False
    try:
        for workload in workloads:
            summary = compare_workload(args, workload)
            failed |= report_summary(
                summary,
                args.regression_ratio,
                args.geomean_regression_seconds,
                args.no_fail,
            )
    except RuntimeError as error:
        emit_annotation("error", "Performance benchmark failure", str(error))
        print(f"performance comparison failed: {error}", file=sys.stderr)
        return 1
    return 1 if failed and not args.no_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
