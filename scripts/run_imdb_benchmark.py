#!/usr/bin/env python3
"""Run DuckDB's native IMDB benchmarks with the Bloom extension loaded."""

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DUCKDB_BENCHMARK = ROOT / "duckdb" / "benchmark"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=ROOT / "duckdb" / "duckdb_benchmark_data" / "imdb.duckdb",
        help="Existing IMDB DuckDB database",
    )
    parser.add_argument(
        "--runner",
        type=Path,
        default=ROOT / "build" / "release" / "benchmark" / "benchmark_runner",
    )
    parser.add_argument("--pattern", default="benchmark/imdb/.*.benchmark")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--timed-runs", type=int, default=5)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument(
        "--copy-db",
        action="store_true",
        help="Copy the database into the temporary root instead of symlinking it",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Run the same workload with RPT_ENABLE=0",
    )
    return parser.parse_args()


def write_benchmark_root(root: Path, db: Path, copy_db: bool):
    imdb_dir = root / "benchmark" / "imdb"
    data_dir = root / "duckdb_benchmark_data"
    imdb_dir.mkdir(parents=True)
    data_dir.mkdir()

    for benchmark in (DUCKDB_BENCHMARK / "imdb").glob("*.benchmark"):
        shutil.copy2(benchmark, imdb_dir / benchmark.name)

    template = f"""# name: ${{FILE_PATH}}
# description: ${{DESCRIPTION}}
# group: [imdb]

name Q${{QUERY_NUMBER_PADDED}}
group imdb

require bloom
require parquet

cache imdb.duckdb

load {DUCKDB_BENCHMARK / 'imdb' / 'init' / 'load.sql'}

run {DUCKDB_BENCHMARK / 'imdb_plan_cost' / 'queries'}/${{QUERY_NUMBER_PADDED}}.sql

result {DUCKDB_BENCHMARK / 'imdb' / 'answers'}/${{QUERY_NUMBER_PADDED}}.csv
"""
    (imdb_dir / "imdb.benchmark.in").write_text(template, encoding="ascii")

    target = data_dir / "imdb.duckdb"
    if copy_db:
        shutil.copy2(db, target)
    else:
        target.symlink_to(db)


def main():
    args = parse_args()
    runner = args.runner.resolve()
    db = args.db.resolve()
    if not runner.is_file():
        raise SystemExit(f"benchmark runner not found: {runner}")
    if not db.is_file():
        raise SystemExit(f"IMDB database not found: {db}")
    if args.threads < 1 or args.timed_runs < 1:
        raise SystemExit("--threads and --timed-runs must be positive")

    with tempfile.TemporaryDirectory(prefix="bloom-imdb-") as temp:
        benchmark_root = Path(temp)
        write_benchmark_root(benchmark_root, db, args.copy_db)

        command = [
            str(runner),
            "--root-dir",
            str(benchmark_root),
            args.pattern,
            f"--threads={args.threads}",
            "--timed-runs",
            str(args.timed_runs),
            "--disable-timeout",
        ]
        if args.out:
            command.append(f"--out={args.out.resolve()}")
        if args.log:
            command.append(f"--log={args.log.resolve()}")

        env = os.environ.copy()
        env.setdefault("RPT_SAMPLE_CACHE_DIR", str(db) + ".rpt_samples")
        if args.baseline:
            env["RPT_ENABLE"] = "0"
        return subprocess.run(command, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
