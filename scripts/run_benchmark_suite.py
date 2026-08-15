#!/usr/bin/env python3
"""Compare Bloom configurations with DuckDB's native benchmark runner.

Runs DuckDB's native benchmark_runner sequentially for every configuration
and thread count, then prints a Markdown summary from per-query medians. Every
query receives the runner's discarded warmup before its timed executions.
"""

import argparse
import re
import statistics
import subprocess
import sys
from math import exp, log
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts" / "run_benchmark.py"
TIMING = re.compile(r"^(benchmark/\S+\.benchmark)\t\d+\t(\d+(?:\.\d+)?)\s*$")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", default="imdb",
                        help="Workload name understood by run_benchmark.py (imdb, tpch_sf1, ...)")
    parser.add_argument("--db", type=Path, help="Database file override")
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--timed-runs", type=int, default=5)
    parser.add_argument(
        "--comparison",
        choices=("baseline", "sampling"),
        default="baseline",
        help="Compare Bloom with DuckDB, or prepared with instant sampling",
    )
    parser.add_argument(
        "--sampling-mode",
        choices=("prepared", "instant"),
        default="prepared",
        help="Sampling path for Bloom in a baseline comparison (default: prepared)",
    )
    parser.add_argument("--sample-seed", type=int, default=2, help="Sampling seed (default: 2)")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "benchmark_results",
        help="Directory for raw per-run timing logs",
    )
    return parser.parse_args()


def run_config(args, threads, sampling_mode, baseline=False):
    """Run one configuration and return {query: median_seconds}."""
    rpt_tag = "rpt" if sampling_mode == "prepared" else f"rpt_{sampling_mode}"
    tag = f"{args.workload}_{'base' if baseline else rpt_tag}_t{threads}"
    command = [
        sys.executable,
        str(RUNNER),
        "--workload",
        args.workload,
        "--threads",
        str(threads),
        "--timed-runs",
        str(args.timed_runs),
        "--sampling-mode",
        sampling_mode,
        "--sample-seed",
        str(args.sample_seed),
    ]
    if args.db:
        command += ["--db", str(args.db)]
    if baseline:
        command.append("--baseline")

    print(f">>> {tag}: {' '.join(command)}", flush=True)
    runs = {}
    log_path = args.out_dir / f"{tag}.log"
    with subprocess.Popen(command, stderr=subprocess.PIPE, text=True) as proc, open(
        log_path, "w", encoding="utf-8"
    ) as log_file:
        for line in proc.stderr:
            log_file.write(line)
            match = TIMING.match(line)
            if match:
                runs.setdefault(match.group(1), []).append(float(match.group(2)))
    if proc.returncode != 0:
        raise SystemExit(f"{tag} failed with exit code {proc.returncode}; see {log_path}")
    incomplete = {query: len(times) for query, times in runs.items() if len(times) != args.timed_runs}
    if not runs or incomplete:
        raise SystemExit(
            f"{tag} produced incomplete timings: {incomplete or 'no timing rows'}; see {log_path}"
        )
    return {query: statistics.median(times) for query, times in runs.items()}


def geomean(values):
    return exp(sum(log(v) for v in values) / len(values))


def require_same_queries(left_name, left, right_name, right):
    if left.keys() != right.keys():
        left_only = sorted(left.keys() - right.keys())
        right_only = sorted(right.keys() - left.keys())
        raise SystemExit(
            f"query-set mismatch: {left_name}-only={left_only}, {right_name}-only={right_only}"
        )
    return sorted(left)


def summarize_baseline(threads, rpt, base):
    queries = require_same_queries("Bloom", rpt, "baseline", base)
    speedups = [base[query] / rpt[query] for query in queries]
    total_rpt = sum(rpt[query] for query in queries)
    total_base = sum(base[query] for query in queries)
    faster = sum(speedup > 1.0 for speedup in speedups)
    return (
        f"| {threads} | {total_base:.3f} | {total_rpt:.3f} | {total_base / total_rpt:.3f}x "
        f"| {geomean(speedups):.3f}x | {faster}/{len(queries)} |"
    )


def summarize_sampling(threads, prepared, instant):
    queries = require_same_queries("prepared", prepared, "instant", instant)
    ratios = [instant[query] / prepared[query] for query in queries]
    total_prepared = sum(prepared[query] for query in queries)
    total_instant = sum(instant[query] for query in queries)
    faster = sum(ratio < 1.0 for ratio in ratios)
    return (
        f"| {threads} | {total_prepared:.3f} | {total_instant:.3f} "
        f"| {total_instant / total_prepared:.3f}x | {geomean(ratios):.3f}x "
        f"| {faster}/{len(queries)} |"
    )


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for threads in args.threads:
        if args.comparison == "sampling":
            prepared = run_config(args, threads, "prepared")
            instant = run_config(args, threads, "instant")
            rows.append(summarize_sampling(threads, prepared, instant))
            continue
        rpt = run_config(args, threads, args.sampling_mode)
        base = run_config(args, threads, args.sampling_mode, baseline=True)
        rows.append(summarize_baseline(threads, rpt, base))

    if args.comparison == "sampling":
        print(
            "\n| Threads | Prepared total (s) | Instant total (s) | Instant/prepared "
            "| Per-query geomean | Instant faster |"
        )
    else:
        print(
            "\n| Threads | Baseline total (s) | Bloom total (s) | Total speedup "
            "| Per-query geomean | Queries faster |"
        )
    print("|---:|---:|---:|---:|---:|---:|")
    print("\n".join(rows))


if __name__ == "__main__":
    main()
