#!/usr/bin/env python3
"""Run one benchmark workload through DuckDB's native benchmark_runner.

The Bloom extension is statically linked into the runner, so every query runs
with predicate transfer enabled; pass --baseline to disable it (RPT_ENABLE=0).
A temporary benchmark root is synthesized per run; databases and RPT sample
caches persist in .bench_cache/data so repeated runs stay warm.
"""

import argparse
import os
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DUCKDB = ROOT / "duckdb"
DATA_DIR = ROOT / ".bench_cache" / "data"
CEB_QUERY_DIR = ROOT / ".bench_cache" / "ceb" / "queries" / "1f39e9aa85ee64249f60bfa59543e8707b228644"
CEB_STACK_QUERY_DIR = (
    ROOT
    / ".bench_cache"
    / "ceb_stack"
    / "queries"
    / "6dd6a8699046a61a365722bf28c90acbf7764a2baf206114012b9cf2c8b7b918"
    / "stack"
)
STATS_CEB_ROOT = (
    ROOT
    / ".bench_cache"
    / "stats_ceb"
    / "assets"
    / "670cb8d4bf4cbfa32f94fdf17f33973d3fd67d1b"
)

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
    "ceb_imdb": {
        "cache": "imdb.duckdb",
        "queries": CEB_QUERY_DIR / "ceb-imdb-3k",
        "answers": None,
        "load_file": DUCKDB / "benchmark" / "imdb" / "init" / "load.sql",
        "require": [],
        "recursive_queries": True,
        "prepare_ceb": True,
    },
    "ceb_imdb_full": {
        "cache": "imdb.duckdb",
        "queries": CEB_QUERY_DIR / "ceb-imdb-13k",
        "answers": None,
        "load_file": DUCKDB / "benchmark" / "imdb" / "init" / "load.sql",
        "require": [],
        "recursive_queries": True,
        "prepare_ceb": True,
    },
    "ceb_stack": {
        "cache": "ceb_stack.duckdb",
        "queries": CEB_STACK_QUERY_DIR,
        "answers": None,
        "load_sql": "SET schema='public';",
        "require": [],
        "recursive_queries": True,
        "prepare_ceb_stack": True,
    },
    "stats_ceb": {
        "cache": "stats_ceb.duckdb",
        "queries": STATS_CEB_ROOT / "queries",
        "answers": STATS_CEB_ROOT / "answers",
        "load_sql": "SELECT 1;",
        "require": [],
        "prepare_stats_ceb": True,
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
    parser.add_argument(
        "--required-extension",
        default="bloom",
        help="Statically linked extension required by generated benchmarks",
    )
    parser.add_argument("--pattern", help="Benchmark regex (default: the whole workload)")
    parser.add_argument(
        "--exclude-pattern",
        help="Skip generated benchmark paths matching this regex",
    )
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--timed-runs", type=int, default=5)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--baseline", action="store_true", help="Run with RPT_ENABLE=0")
    return parser.parse_args()


def write_benchmark_root(
    root: Path,
    workload: str,
    db: "Path | None",
    required_extension: str,
    exclude_pattern: "re.Pattern | None" = None,
):
    spec = WORKLOADS[workload]
    bench_dir = root / "benchmark" / workload
    bench_dir.mkdir(parents=True)

    # Keep the benchmark runner's cache path isolated. The individual database
    # symlink points either to the persistent Bloom cache or to --db, so an
    # external database never replaces a shared cache entry.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_data = root / "duckdb_benchmark_data"
    benchmark_data.mkdir()
    database = db or (DATA_DIR / spec["cache"]).resolve()
    (benchmark_data / spec["cache"]).symlink_to(database)

    if "load_sql" in spec:
        (bench_dir / "load.sql").write_text(spec["load_sql"] + "\n", encoding="utf-8")
        load_path = f"benchmark/{workload}/load.sql"
    else:
        load_path = str(spec["load_file"])

    db_exists = database.exists()
    requires = ([required_extension] if required_extension else []) + (
        [] if db_exists else spec["require"]
    )
    query_glob = (
        spec["queries"].rglob("*.sql")
        if spec.get("recursive_queries")
        else spec["queries"].glob("*.sql")
    )
    for query_file in sorted(query_glob):
        if spec.get("recursive_queries"):
            relative = query_file.relative_to(spec["queries"])
            query_id = "__".join(relative.with_suffix("").parts)
        else:
            query_id = query_file.stem
        benchmark_name = f"benchmark/{workload}/{query_id}.benchmark"
        if exclude_pattern and exclude_pattern.search(benchmark_name):
            continue
        lines = [
            f"# name: {benchmark_name}",
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

    if WORKLOADS[args.workload].get("prepare_ceb"):
        from prepare_ceb import prepare

        prepare()
    if WORKLOADS[args.workload].get("prepare_ceb_stack"):
        from prepare_ceb_stack import prepare

        prepare(
            ROOT / "build" / "release" / "duckdb",
            build_db=db is None,
        )
    if WORKLOADS[args.workload].get("prepare_stats_ceb"):
        from prepare_stats_ceb import prepare

        prepare(
            ROOT / "build" / "release" / "duckdb",
            build_db=db is None,
        )

    with tempfile.TemporaryDirectory(prefix=f"bloom-{args.workload}-") as temp:
        benchmark_root = Path(temp)
        write_benchmark_root(
            benchmark_root,
            args.workload,
            db,
            args.required_extension,
            re.compile(args.exclude_pattern) if args.exclude_pattern else None,
        )

        pattern = args.pattern or f"benchmark/{args.workload}/.*.benchmark"
        command = [
            str(runner),
            "--root-dir", str(benchmark_root),
            pattern,
            f"--threads={args.threads}",
            "--disable-timeout",
        ]
        help_result = subprocess.run(
            [str(runner), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        runner_help = help_result.stdout + help_result.stderr
        if "--timed-runs" in runner_help:
            command += ["--timed-runs", str(args.timed_runs)]
        elif args.timed_runs != 5:
            print(
                "warning: this benchmark_runner only supports its default "
                "five timed runs; ignoring --timed-runs",
            )
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            command.append(f"--out={args.out.resolve()}")
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
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
