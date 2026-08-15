#!/usr/bin/env python3
"""Collect cold-base RPT phase timings with prepared samples already resident."""

import argparse
import gzip
import json
import os
import re
import resource
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
NATIVE_ROOT = Path.home() / "projects/native-predicate-transfer"
WORKLOADS = {
    "job": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/imdb_compressed.duckdb",
        "queries": ROOT / "duckdb/benchmark/imdb_plan_cost/queries",
    },
    "tpch_sf10": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/tpch_sf10.duckdb",
        "queries": ROOT / "duckdb/extension/tpch/dbgen/queries",
    },
    "appian": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/ads.5m.duck",
        "queries": ROOT / "duckdb/benchmark/appian_benchmarks/queries",
    },
}
KEY_VALUE = re.compile(r"([A-Za-z_]+)=([^\s]+)")
EXECUTION_SIGNATURE = re.compile(r"RPTExecutionSignature hash=([0-9a-f]+)")
PLAN_SIGNATURE = re.compile(r"RPTPlanSignature hash=([0-9a-f]+)")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=sorted(WORKLOADS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--queries", help="Comma-separated query stems")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--sample-cache-dir", type=Path, default=ROOT / ".bench_cache/rpt_samples"
    )
    parser.add_argument("--duckdb", type=Path, default=ROOT / "build/release/duckdb")
    return parser.parse_args()


def evict_file(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(descriptor)


def resident_bytes(path):
    result = subprocess.run(
        ["fincore", "--bytes", "--noheadings", "--output", "RES", str(path)],
        text=True,
        capture_output=True,
        check=True,
    )
    return int(result.stdout.strip())


def number(value):
    try:
        return float(value.removesuffix("ms"))
    except (AttributeError, ValueError):
        return 0.0


def parse_log(text):
    timing = {}
    for line in text.splitlines():
        if "[RPT-Timing]" in line:
            timing = dict(KEY_VALUE.findall(line))
    executions = EXECUTION_SIGNATURE.findall(text)
    plans = PLAN_SIGNATURE.findall(text)
    preload = re.findall(r"\[RPT-SamplePreload\] loaded=(\d+)", text)
    return {
        "rpt_init_ms": number(timing.get("init_estimates", "0")),
        "rpt_materialize_ms": number(timing.get("materialize", "0")),
        "rpt_build_bf_ms": number(timing.get("build_bf", "0")),
        "rpt_re_estimate_ms": number(timing.get("re_estimate", "0")),
        "rpt_finalize_ms": number(timing.get("finalize", "0")),
        "rpt_total_ms": number(timing.get("total", "0")),
        "execution_hash": executions[-1] if executions else None,
        "plan_hash": plans[-1] if plans else None,
        "preloaded_samples": int(preload[-1]) if preload else 0,
    }


def main():
    args = parse_args()
    config = WORKLOADS[args.workload]
    db = config["db"].resolve()
    query_dir = config["queries"].resolve()
    duckdb = args.duckdb.resolve()
    output = args.output.resolve()
    cache = args.sample_cache_dir.resolve()
    if not db.is_file() or not query_dir.is_dir() or not duckdb.is_file():
        raise SystemExit("database, query directory, or DuckDB shell is missing")
    queries = sorted(query_dir.glob("*.sql"))
    if args.queries:
        requested = set(args.queries.split(","))
        queries = [path for path in queries if path.stem in requested]
        missing = requested - {path.stem for path in queries}
        if missing:
            raise SystemExit(f"unknown query ids: {','.join(sorted(missing))}")
    output.mkdir(parents=True, exist_ok=True)
    raw = output / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(
        {
            "RPT_ENABLE": "1",
            "RPT_SAMPLE_MODE": "prepared",
            "RPT_SAMPLE_CACHE_DIR": str(cache),
            "RPT_SAMPLE_MEMORY_CACHE": "1",
            "RPT_SAMPLE_SEED": "2",
            "RPT_LOG_TRANSFER_STEPS": "0",
        }
    )
    result_path = output / "prepared-cold-phases.jsonl"
    with result_path.open("w", encoding="utf-8") as result_file:
        for query_file in queries:
            evict_file(db)
            resident_before = resident_bytes(db)
            query = query_file.read_text(encoding="utf-8").strip()
            sql = (
                f"SET threads={args.threads};\n"
                "SET enable_progress_bar=false;\n"
                "SET rpt_log_transfer_steps=false;\n"
                "SELECT * FROM rpt_preload_samples();\n"
                "SET rpt_log_transfer_steps=true;\n"
                "PREPARE rpt_cold_prepared AS\n"
                f"{query}\n"
            )
            usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            result = subprocess.run(
                [str(duckdb), "-batch", "-noheader", "-csv", str(db)],
                input=sql,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                check=False,
            )
            usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
            raw_path = raw / f"{query_file.stem}.log.gz"
            with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
                handle.write(result.stdout)
            record = {
                "workload": args.workload,
                "query": query_file.stem,
                "returncode": result.returncode,
                "resident_before": resident_before,
                "input_bytes": (usage_after.ru_inblock - usage_before.ru_inblock) * 512,
                **parse_log(result.stdout),
            }
            result_file.write(json.dumps(record, sort_keys=True) + "\n")
            result_file.flush()
            print(
                f"{args.workload} {query_file.stem}: total={record['rpt_total_ms']:.1f}ms "
                f"materialize={record['rpt_materialize_ms']:.1f}ms "
                f"read={record['input_bytes'] / 2**20:.1f}MiB",
                flush=True,
            )
            if result.returncode:
                raise RuntimeError(f"query failed; inspect {raw_path}")
    print(result_path, flush=True)


if __name__ == "__main__":
    main()
