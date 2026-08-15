#!/usr/bin/env python3
"""Run the full same-engine prepared/instant benchmark matrix."""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CELL_RUNNER = (
    ROOT
    / "experiments/2026-08-oracle-prepared-instant-end-to-end/scripts/run_comparison.py"
)
SUMMARIZER = Path(__file__).resolve().parent / "summarize.py"
DEFAULT_SOURCE = (
    ROOT
    / "experiments/2026-08-oracle-prepared-instant-end-to-end/results/reproduced/final-source"
)
WORKLOADS = {
    "ceb": {"repetitions": {"warm": 1, "cold": 1}},
    "job_uncompressed": {"repetitions": {"warm": 2, "cold": 3}},
    "job": {"repetitions": {"warm": 2, "cold": 3}, "reusable": True},
    "stats_ceb": {"repetitions": {"warm": 2, "cold": 3}},
    "ceb_stack": {"repetitions": {"warm": 1, "cold": 1}},
    "tpch_sf10": {"repetitions": {"warm": 2, "cold": 3}, "reusable": True},
    "tpcds_sf10": {"repetitions": {"warm": 2, "cold": 3}},
    "appian": {"repetitions": {"warm": 2, "cold": 3}, "reusable": True},
}
METHODS = ("prepared", "instant")
STATES = ("warm", "cold")
LARGE_WORKLOADS = {"ceb", "ceb_stack"}


def parse_names(value, known, option):
    names = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = set(names) - set(known)
    if unknown:
        raise SystemExit(f"{option}: unknown names: {','.join(sorted(unknown))}")
    return names


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--source-results", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument(
        "--workloads",
        default=",".join(WORKLOADS),
        help="comma-separated subset; the report is generated once all cells exist",
    )
    parser.add_argument(
        "--skip-prime",
        default="",
        help="comma-separated workloads whose persistent sample cache was already audited",
    )
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def run(command):
    print("+ " + " ".join(map(str, command)), flush=True)
    subprocess.run(command, check=True)


def copy_reusable_cell(source, destination, resume):
    source_result = source / "results.jsonl"
    destination_result = destination / "results.jsonl"
    if destination_result.is_file() and resume:
        print(f"reuse existing: {destination_result}", flush=True)
        return
    if not source_result.is_file():
        raise RuntimeError(f"reusable result is missing: {source_result}")
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_result, destination_result)
    source_audit = source / "preload_audit.json"
    if source_audit.is_file():
        shutil.copy2(source_audit, destination / source_audit.name)
    print(f"copied validated cell: {source_result}", flush=True)


def matrix_complete(output):
    return all(
        (output / state / workload / method / "results.jsonl").is_file()
        for workload in WORKLOADS
        for state in STATES
        for method in METHODS
    )


def main():
    args = parse_args()
    if args.threads < 1 or args.progress_every < 1:
        raise SystemExit("--threads and --progress-every must be positive")
    selected = parse_names(args.workloads, WORKLOADS, "--workloads")
    skip_prime = set(parse_names(args.skip_prime, WORKLOADS, "--skip-prime"))
    output = args.output.resolve()
    source = args.source_results.resolve()
    if output.exists() and any(output.iterdir()) and not args.resume:
        raise SystemExit(f"output is not empty: {output}; pass --resume or choose a new directory")
    output.mkdir(parents=True, exist_ok=True)

    for workload in selected:
        if not WORKLOADS[workload].get("reusable"):
            continue
        for state in STATES:
            for method in METHODS:
                copy_reusable_cell(
                    source / state / workload / method,
                    output / state / workload / method,
                    args.resume,
                )

    # Building maintained samples is outside the comparison. A discarded full
    # pass ensures a clean cache on a new machine. An already-audited cache can
    # be selected explicitly to avoid repeating a multi-hour CEB pass.
    for workload in selected:
        if WORKLOADS[workload].get("reusable") or workload in skip_prime:
            continue
        prime = output / "prepared_prime" / workload
        command = [
            sys.executable,
            str(CELL_RUNNER),
            "--workload",
            workload,
            "--method",
            "prepared",
            "--state",
            "warm",
            "--output",
            str(prime),
            "--repetitions",
            "1",
            "--threads",
            str(args.threads),
            "--progress-every",
            str(args.progress_every),
        ]
        if args.resume:
            command.append("--resume")
        run(command)

    for state in STATES:
        for workload in selected:
            if WORKLOADS[workload].get("reusable"):
                continue
            for method in METHODS:
                cell = output / state / workload / method
                command = [
                    sys.executable,
                    str(CELL_RUNNER),
                    "--workload",
                    workload,
                    "--method",
                    method,
                    "--state",
                    state,
                    "--output",
                    str(cell),
                    "--repetitions",
                    str(WORKLOADS[workload]["repetitions"][state]),
                    "--threads",
                    str(args.threads),
                    "--progress-every",
                    str(args.progress_every),
                    "--residency-check-every",
                    "50" if workload in LARGE_WORKLOADS else "1",
                ]
                if args.resume:
                    command.append("--resume")
                run(command)

    if matrix_complete(output):
        run(
            [
                sys.executable,
                str(SUMMARIZER),
                "--results",
                str(output),
                "--output",
                str(output / "REPORT.md"),
            ]
        )
    else:
        print("selected cells complete; full matrix is not yet available for summary", flush=True)


if __name__ == "__main__":
    main()
