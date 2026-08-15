#!/usr/bin/env python3
"""Run and validate the complete oracle/prepared/instant benchmark matrix."""

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
RUNNER = SCRIPT_DIR / "run_comparison.py"
SUMMARIZER = SCRIPT_DIR / "summarize_comparison.py"
WORKLOADS = ("job", "tpch_sf10", "appian")
METHODS = ("oracle", "prepared", "instant")
STATES = {"warm": 2, "cold": 3}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="skip a matrix cell when its results.jsonl already exists",
    )
    return parser.parse_args()


def run(command):
    print("+ " + " ".join(map(str, command)), flush=True)
    subprocess.run(command, check=True)


def main():
    args = parse_args()
    if args.threads < 1:
        raise SystemExit("--threads must be positive")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()) and not args.resume:
        raise SystemExit(f"output is not empty: {output}; choose a new directory or pass --resume")
    output.mkdir(parents=True, exist_ok=True)

    # Prepared samples are intentionally maintained outside the timed query.
    # Exercise the complete workload once so every table sample exists before
    # any result that is kept as a measurement.
    for workload in WORKLOADS:
        prime_output = output / "prepared_prime" / workload
        if args.resume and (prime_output / "results.jsonl").is_file():
            print(f"reuse: {prime_output / 'results.jsonl'}", flush=True)
            continue
        run(
            [
                sys.executable,
                str(RUNNER),
                "--workload",
                workload,
                "--method",
                "prepared",
                "--state",
                "warm",
                "--output",
                str(prime_output),
                "--repetitions",
                "1",
                "--threads",
                str(args.threads),
            ]
        )

    for state, repetitions in STATES.items():
        for workload in WORKLOADS:
            for method in METHODS:
                cell = output / state / workload / method
                result_file = cell / "results.jsonl"
                if args.resume and result_file.is_file():
                    print(f"reuse: {result_file}", flush=True)
                    continue
                command = [
                    sys.executable,
                    str(RUNNER),
                    "--workload",
                    workload,
                    "--method",
                    method,
                    "--state",
                    state,
                    "--output",
                    str(cell),
                    "--repetitions",
                    str(repetitions),
                    "--threads",
                    str(args.threads),
                ]
                if method == "oracle":
                    command.append("--prime-oracle-cache")
                run(command)

    for workload in WORKLOADS:
        cell = output / "cold" / workload / "prepared"
        audit = cell / "preload_audit.json"
        if args.resume and audit.is_file():
            print(f"reuse: {audit}", flush=True)
            continue
        run(
            [
                sys.executable,
                str(RUNNER),
                "--workload",
                workload,
                "--method",
                "prepared",
                "--state",
                "cold",
                "--output",
                str(cell),
                "--threads",
                str(args.threads),
                "--preload-audit-only",
            ]
        )

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


if __name__ == "__main__":
    main()
