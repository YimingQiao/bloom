#!/usr/bin/env python3
"""Run one benchmark workload through DuckDB's native benchmark_runner.

The Bloom extension is statically linked into the runner, so every query runs
with predicate transfer enabled; pass --baseline to disable it (RPT_ENABLE=0).
A temporary benchmark root is synthesized per run; databases and RPT sample
caches persist in .bench_cache/data so repeated runs stay warm.
"""

import argparse
import os
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DUCKDB = ROOT / "duckdb"
DATA_DIR = ROOT / ".bench_cache" / "data"

# Each workload: where its queries/answers live, the cached database name, the
# SQL that can (re)generate the database, and required extensions.
WORKLOADS = {
    "imdb": {
        "cache": "imdb.duckdb",
        "queries": DUCKDB / "benchmark" / "imdb_plan_cost" / "queries",
        "answers": DUCKDB / "benchmark" / "imdb" / "answers",
        "load_file": DUCKDB / "benchmark" / "imdb" / "init" / "load.sql",
        "require": [],
    },
    "tpch_sf1": {
        "cache": "tpch_sf1.duckdb",
        "queries": DUCKDB / "extension" / "tpch" / "dbgen" / "queries",
        "answers": DUCKDB / "extension" / "tpch" / "dbgen" / "answers" / "sf1",
        "load_sql": "CALL dbgen(sf=1);",
        "require": ["tpch"],
    },
    "tpch_sf10": {
        "cache": "tpch_sf10.duckdb",
        "queries": DUCKDB / "extension" / "tpch" / "dbgen" / "queries",
        "answers": ROOT / "benchmark" / "answers" / "tpch_sf10",
        "load_sql": "CALL dbgen(sf=10);",
        "require": ["tpch"],
    },
    "tpcds_sf1": {
        "cache": "tpcds_sf1.duckdb",
        "queries": ROOT / "benchmark" / "queries" / "tpcds",
        "answers": ROOT / "benchmark" / "answers" / "tpcds_sf1",
        "load_sql": "CALL dsdgen(sf=1);",
        "require": ["tpcds"],
    },
    "tpcds_sf10": {
        "cache": "tpcds_sf10.duckdb",
        "queries": ROOT / "benchmark" / "queries" / "tpcds",
        "answers": ROOT / "benchmark" / "answers" / "tpcds_sf10",
        "load_sql": "CALL dsdgen(sf=10);",
        "require": ["tpcds"],
    },
    "appian": {
        "cache": "ads.5m.duck",
        "queries": DUCKDB / "benchmark" / "appian_benchmarks" / "queries",
        "answers": None,
        "load_sql": (
            "LOAD httpfs;\n"
            "ATTACH 'https://blobs.duckdb.org/data/appian_benchmark_data.duckdb' AS appian_db (READ_ONLY);\n"
            + "\n".join(
                f"CREATE TABLE {t} AS SELECT * FROM appian_db.{t};"
                for t in [
                    "AddressView", "CustomerView", "OrderView", "CategoryView",
                    "OrderItemNovelty_Update", "ProductView", "CreditCardView",
                    "OrderItemView", "TaxRecordView",
                ]
            )
        ),
        "require": ["httpfs"],  # only enforced when the database must be generated
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=sorted(WORKLOADS), default="imdb")
    parser.add_argument("--db", type=Path, help="Database file (default: .bench_cache/data/<cache>)")
    parser.add_argument("--runner", type=Path, default=ROOT / "build" / "release" / "benchmark" / "benchmark_runner")
    parser.add_argument("--pattern", help="Benchmark regex (default: the whole workload)")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--timed-runs", type=int, default=5)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--baseline", action="store_true", help="Run with RPT_ENABLE=0")
    return parser.parse_args()


def write_benchmark_root(root: Path, workload: str, db: "Path | None"):
    spec = WORKLOADS[workload]
    bench_dir = root / "benchmark" / workload
    bench_dir.mkdir(parents=True)

    # The cache database lives in the persistent data dir; benchmark_runner
    # resolves (and generates) it through this symlink.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (root / "duckdb_benchmark_data").symlink_to(DATA_DIR)
    if db is not None:
        target = DATA_DIR / spec["cache"]
        if target.is_symlink() or target.exists():
            target.unlink()
        target.symlink_to(db)

    if "load_sql" in spec:
        (bench_dir / "load.sql").write_text(spec["load_sql"] + "\n", encoding="utf-8")
        load_path = f"benchmark/{workload}/load.sql"
    else:
        load_path = str(spec["load_file"])

    db_exists = (DATA_DIR / spec["cache"]).exists()
    requires = ["bloom"] + ([] if db_exists else spec["require"])
    for query_file in sorted(spec["queries"].glob("*.sql")):
        query_id = query_file.stem
        lines = [
            f"# name: benchmark/{workload}/{query_id}.benchmark",
            f"# group: [{workload}]",
            "",
            f"name Q{query_id}",
            f"group {workload}",
            "",
        ]
        lines += [f"require {r}" for r in requires]
        lines += ["", f"cache {spec['cache']}", "", f"load {load_path}", "", f"run {query_file}"]
        answer = spec["answers"] / f"{query_id}.csv" if spec["answers"] else None
        if answer and answer.is_file():
            lines += ["", f"result {answer}"]
        (bench_dir / f"{query_id}.benchmark").write_text("\n".join(lines) + "\n", encoding="ascii")


def main():
    args = parse_args()
    runner = args.runner.resolve()
    if not runner.is_file():
        raise SystemExit(f"benchmark runner not found: {runner} (build with BUILD_BENCHMARK=1)")
    db = args.db.resolve() if args.db else None
    if db is not None and not db.is_file():
        raise SystemExit(f"database not found: {db}")

    with tempfile.TemporaryDirectory(prefix=f"bloom-{args.workload}-") as temp:
        benchmark_root = Path(temp)
        write_benchmark_root(benchmark_root, args.workload, db)

        pattern = args.pattern or f"benchmark/{args.workload}/.*.benchmark"
        command = [
            str(runner),
            "--root-dir", str(benchmark_root),
            pattern,
            f"--threads={args.threads}",
            "--timed-runs", str(args.timed_runs),
            "--disable-timeout",
        ]
        if args.out:
            command.append(f"--out={args.out.resolve()}")
        if args.log:
            command.append(f"--log={args.log.resolve()}")

        env = os.environ.copy()
        sample_dir = ROOT / ".bench_cache" / "rpt_samples"
        sample_dir.mkdir(parents=True, exist_ok=True)
        env.setdefault("RPT_SAMPLE_CACHE_DIR", str(sample_dir))
        if args.baseline:
            env["RPT_ENABLE"] = "0"
        return subprocess.run(command, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
