#!/usr/bin/env python3
"""Validate and summarize two benchmark_runner timing logs."""

import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

TIMING = re.compile(
    r"^(benchmark/\S+\.benchmark)\t(\d+)\t"
    r"((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$"
)
RESULT = re.compile(r"^(benchmark/\S+\.benchmark)\t(\d+)\t(\S+)\s*$")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpt-log", type=Path, required=True)
    parser.add_argument("--baseline-log", type=Path)
    parser.add_argument("--expected-queries", type=int, required=True)
    parser.add_argument("--timed-runs", type=int, default=5)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Report progress instead of failing on an incomplete log",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args()


def read_log(path):
    timings = defaultdict(dict)
    failures = []
    duplicates = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        match = TIMING.match(line)
        if match:
            query, run, value = match.groups()
            run = int(run)
            if run in timings[query]:
                duplicates.append({"query": query, "run": run, "line": line_number})
            timings[query][run] = float(value)
            continue
        match = RESULT.match(line)
        if match and match.group(3).upper() not in {"TIMING"}:
            failures.append(
                {
                    "query": match.group(1),
                    "run": int(match.group(2)),
                    "result": match.group(3),
                    "line": line_number,
                }
            )
    return timings, failures, duplicates


def summarize_side(path, expected_queries, timed_runs):
    timings, failures, duplicates = read_log(path)
    complete = {
        query: statistics.median(runs.values())
        for query, runs in timings.items()
        if set(runs) == set(range(1, timed_runs + 1))
    }
    partial = {
        query: sorted(runs)
        for query, runs in timings.items()
        if set(runs) != set(range(1, timed_runs + 1))
    }
    return {
        "path": str(path),
        "expected_queries": expected_queries,
        "seen_queries": len(timings),
        "complete_queries": len(complete),
        "partial_queries": partial,
        "failures": failures,
        "duplicates": duplicates,
        "medians": complete,
        "total_median_seconds": sum(complete.values()),
        "complete": (
            len(complete) == expected_queries
            and not partial
            and not failures
            and not duplicates
        ),
    }


def public_side(side):
    return {key: value for key, value in side.items() if key != "medians"}


def main():
    args = parse_args()
    rpt = summarize_side(args.rpt_log, args.expected_queries, args.timed_runs)
    output = {"rpt": public_side(rpt)}

    baseline = None
    if args.baseline_log:
        baseline = summarize_side(args.baseline_log, args.expected_queries, args.timed_runs)
        common = rpt["medians"].keys() & baseline["medians"].keys()
        comparison = {
            "common_complete_queries": len(common),
            "baseline_total_seconds": sum(baseline["medians"][query] for query in common),
            "rpt_total_seconds": sum(rpt["medians"][query] for query in common),
            "queries_faster_with_rpt": sum(
                baseline["medians"][query] > rpt["medians"][query] for query in common
            ),
        }
        if comparison["rpt_total_seconds"]:
            comparison["total_speedup"] = (
                comparison["baseline_total_seconds"] / comparison["rpt_total_seconds"]
            )
        output["baseline"] = public_side(baseline)
        output["comparison"] = comparison

    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        for name, side in (("RPT", rpt), ("baseline", baseline)):
            if side is None:
                continue
            print(
                f"{name}: {side['complete_queries']}/{side['expected_queries']} complete, "
                f"{side['seen_queries']} seen, {side['total_median_seconds']:.6f} s"
            )
            if side["partial_queries"]:
                print(f"{name}: {len(side['partial_queries'])} partial queries")
            if side["failures"]:
                print(f"{name}: {len(side['failures'])} failed timing rows")
            if side["duplicates"]:
                print(f"{name}: {len(side['duplicates'])} duplicate timing rows")
        if baseline:
            comparison = output["comparison"]
            print(
                f"comparison: {comparison['common_complete_queries']} common queries, "
                f"{comparison.get('total_speedup', 0):.6f}x total speedup, "
                f"{comparison['queries_faster_with_rpt']} queries faster with RPT"
            )

    complete = rpt["complete"] and (baseline is None or baseline["complete"])
    if not complete and not args.allow_incomplete:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
