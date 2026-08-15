#!/usr/bin/env python3
"""One-click benchmark suite: Bloom RPT vs DuckDB baseline.

Runs DuckDB's native benchmark_runner sequentially for every configuration
(RPT on/off x thread counts), then prints a Markdown summary with total
median times and the geomean of per-query median speedups.
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
        "--sampling-mode",
        choices=("prepared", "instant"),
        default="prepared",
        help="Sampling path for Bloom runs (default: prepared)",
    )
    parser.add_argument("--sample-seed", type=int, default=2, help="Sampling seed (default: 2)")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "benchmark_results",
        help="Directory for raw per-run timing logs",
    )
    return parser.parse_args()


def run_config(args, threads, baseline):
    """Run one configuration and return {query: median_seconds}."""
    rpt_tag = "rpt" if args.sampling_mode == "prepared" else f"rpt_{args.sampling_mode}"
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
        args.sampling_mode,
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
    return {query: statistics.median(times) for query, times in runs.items()}


def geomean(values):
    return exp(sum(log(v) for v in values) / len(values))


def summarize(threads, rpt, base):
    queries = sorted(rpt.keys() & base.keys())
    if not queries:
        raise SystemExit("no common queries between RPT and baseline runs")
    speedups = [base[q] / rpt[q] for q in queries]
    total_rpt = sum(rpt[q] for q in queries)
    total_base = sum(base[q] for q in queries)
    faster = sum(s > 1.0 for s in speedups)
    return (
        f"| {threads} | {total_base:.3f} | {total_rpt:.3f} | {total_base / total_rpt:.3f}x "
        f"| {geomean(speedups):.3f}x | {faster}/{len(queries)} |"
    )


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for threads in args.threads:
        rpt = run_config(args, threads, baseline=False)
        base = run_config(args, threads, baseline=True)
        rows.append(summarize(threads, rpt, base))

    print("\n| Threads | Baseline total (s) | Bloom total (s) | Total speedup "
          "| Per-query geomean | Queries faster |")
    print("|---:|---:|---:|---:|---:|---:|")
    print("\n".join(rows))


if __name__ == "__main__":
    main()
