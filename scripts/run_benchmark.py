#!/usr/bin/env python3
"""Run one workload through DuckDB's same-process native benchmark runner.

The Bloom extension is statically linked into the runner, so every query runs
with predicate transfer enabled; pass --baseline to disable it (RPT_ENABLE=0).
A temporary benchmark root is synthesized per run; databases and RPT sample
caches persist in .bench_cache/data. Existing databases are read sequentially
into the OS page cache before the runner starts, then the runner discards one
query warmup before its timed runs. This protocol is distinct from the
fresh-process prepared/instant experiments.
"""

import argparse
import os
import re
import subprocess
import sys
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
        "require": ["httpfs", "parquet"],
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
        "init_sql": "SET schema='public';",
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


def can_open_read_write(database: Path) -> bool:
    """Return whether the runner can open an existing cache read-write."""
    try:
        with database.open("r+b"):
            return True
    except OSError:
        return False


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
    parser.add_argument(
        "--sampling-mode",
        choices=("prepared", "instant"),
        default="prepared",
        help="Sampling path to benchmark (default: prepared)",
    )
    parser.add_argument("--sample-seed", type=int, default=2, help="Sampling seed (default: 2)")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--baseline", action="store_true", help="Run with RPT_ENABLE=0")
    parser.add_argument(
        "--late-materialize",
        action="store_true",
        help="Enable RPT rowid-based late materialization",
    )
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

    # Keep the benchmark runner's cache path isolated. A writable database gets
    # an individual symlink, so the runner can replace only that link after an
    # open failure. A read-only database is attached below without a cache link.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_data = root / "duckdb_benchmark_data"
    benchmark_data.mkdir()
    database = db or (DATA_DIR / spec["cache"]).resolve()

    db_exists = database.is_file()
    database_is_read_only = db_exists and not can_open_read_write(database)
    if not database_is_read_only:
        (benchmark_data / spec["cache"]).symlink_to(database)

    if "load_sql" in spec:
        (bench_dir / "load.sql").write_text(spec["load_sql"] + "\n", encoding="utf-8")
        load_path = f"benchmark/{workload}/load.sql"
    else:
        load_path = str(spec["load_file"])

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
        init_queries = []
        if database_is_read_only:
            # The native benchmark runner opens `cache` databases read-write.
            # Use an in-memory connection with a read-only attachment when the
            # existing database cannot be opened that way (e.g. an external
            # Codex workspace path). This also prevents the runner's broad
            # open-error recovery from replacing the cache link with an empty
            # database and unexpectedly executing the remote load SQL.
            database_sql = str(database).replace("'", "''")
            lines += ["", "storage transient"]
            init_queries += [
                f"ATTACH '{database_sql}' AS bloom_benchmark_db (READ_ONLY);",
                "USE bloom_benchmark_db;",
            ]
        else:
            lines += ["", f"cache {spec['cache']}"]
            if not db_exists:
                lines += ["", f"load {load_path}"]
            # An existing cache must never fall through to remote regeneration
            # merely because the runner failed to open it. With no load query,
            # that condition becomes a visible benchmark ERROR below.
        if spec.get("init_sql"):
            init_queries.append(spec["init_sql"])
        if init_queries:
            lines += ["", "init", *init_queries]
        lines += ["", f"run {query_file}"]
        answer = spec["answers"] / f"{query_id}.csv" if spec["answers"] else None
        if answer and answer.is_file():
            lines += ["", f"result {answer}"]
        (bench_dir / f"{query_id}.benchmark").write_text("\n".join(lines) + "\n", encoding="ascii")

    return database


def warm_page_cache(database: Path):
    """Read an existing benchmark database before any measured process starts."""
    if not database.is_file():
        return
    with database.open("rb", buffering=0) as source:
        while source.read(16 * 1024 * 1024):
            pass


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
        database = write_benchmark_root(
            benchmark_root,
            args.workload,
            db,
            args.required_extension,
            re.compile(args.exclude_pattern) if args.exclude_pattern else None,
        )
        warm_page_cache(database)

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
        result_path = args.out.resolve() if args.out else benchmark_root / "runner_results.out"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        command.append(f"--out={result_path}")
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
            command.append(f"--log={args.log.resolve()}")

        env = os.environ.copy()
        sample_dir = ROOT / ".bench_cache" / "rpt_samples"
        sample_dir.mkdir(parents=True, exist_ok=True)
        env.setdefault("RPT_SAMPLE_CACHE_DIR", str(sample_dir))
        env["RPT_ENABLE"] = "0" if args.baseline else "1"
        env["RPT_LATE_MATERIALIZE"] = "1" if args.late_materialize else "0"
        env["RPT_SAMPLE_MODE"] = args.sampling_mode
        env["RPT_SAMPLE_SEED"] = str(args.sample_seed)
        completed = subprocess.run(command, env=env, check=False)
        if completed.returncode != 0:
            return completed.returncode

        # benchmark_runner reports per-benchmark initialization/query failures
        # as text in --out but can still exit zero. A successful timing file is
        # non-empty and contains only floating-point seconds.
        if not result_path.is_file():
            print(f"benchmark runner failed: no timing output at {result_path}", file=sys.stderr)
            return 1
        result_lines = [
            line.strip()
            for line in result_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        invalid_results = []
        for line in result_lines:
            try:
                float(line)
            except ValueError:
                invalid_results.append(line)
        if not result_lines or invalid_results:
            detail = invalid_results[0] if invalid_results else "no timing rows"
            print(f"benchmark runner failed: {detail}; see {result_path}", file=sys.stderr)
            return 1
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
