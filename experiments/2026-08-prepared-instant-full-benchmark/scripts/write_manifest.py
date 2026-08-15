#!/usr/bin/env python3
"""Fingerprint the full prepared/instant experiment inputs."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
NATIVE_ROOT = Path.home() / "projects/native-predicate-transfer"
WORKLOADS = {
    "ceb": {
        "database": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/imdb_compressed.duckdb",
        "queries": ROOT
        / ".bench_cache/ceb/queries/1f39e9aa85ee64249f60bfa59543e8707b228644/ceb-imdb-3k",
        "recursive": True,
    },
    "job_uncompressed": {
        "database": NATIVE_ROOT
        / "duckdb/duckdb_benchmark_data/imdb_pre_regen_20260401 copy.duckdb",
        "queries": ROOT / "duckdb/benchmark/imdb_plan_cost/queries",
    },
    "job": {
        "database": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/imdb_compressed.duckdb",
        "queries": ROOT / "duckdb/benchmark/imdb_plan_cost/queries",
    },
    "stats_ceb": {
        "database": ROOT / ".bench_cache/data/stats_ceb.duckdb",
        "queries": ROOT
        / ".bench_cache/stats_ceb/assets/670cb8d4bf4cbfa32f94fdf17f33973d3fd67d1b/queries",
    },
    "ceb_stack": {
        "database": ROOT / ".bench_cache/data/ceb_stack.duckdb",
        "queries": ROOT
        / ".bench_cache/ceb_stack/queries"
        / "6dd6a8699046a61a365722bf28c90acbf7764a2baf206114012b9cf2c8b7b918/stack",
        "recursive": True,
    },
    "tpch_sf10": {
        "database": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/tpch_sf10.duckdb",
        "queries": ROOT / "duckdb/extension/tpch/dbgen/queries",
    },
    "tpcds_sf10": {
        "database": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/tpcds_sf10.duckdb",
        "queries": ROOT / "benchmark/queries/tpcds",
    },
    "appian": {
        "database": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/ads.5m.duck",
        "queries": ROOT / "duckdb/benchmark/appian_benchmarks/queries",
    },
}


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def tree_hash(files, base):
    digest = hashlib.sha256()
    for path in sorted(files):
        relative = path.relative_to(base).as_posix().encode()
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        data = path.read_bytes()
        digest.update(len(data).to_bytes(8, "little"))
        digest.update(data)
    return digest.hexdigest()


def git(*arguments, cwd=ROOT):
    return subprocess.check_output(["git", "-C", str(cwd), *arguments], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    source_files = [ROOT / "CMakeLists.txt"]
    for directory in (ROOT / "src", ROOT / "test", Path(__file__).resolve().parent):
        source_files.extend(
            path
            for path in directory.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
        )
    sample_files = sorted((ROOT / ".bench_cache/rpt_samples").glob("*.sample"))
    database_hashes = {}
    workloads = {}
    for name, spec in WORKLOADS.items():
        database = spec["database"].resolve()
        database_key = str(database)
        if database_key not in database_hashes:
            database_hashes[database_key] = sha256_file(database)
        query_dir = spec["queries"].resolve()
        query_files = sorted(
            query_dir.rglob("*.sql") if spec.get("recursive") else query_dir.glob("*.sql")
        )
        workloads[name] = {
            "queries": len(query_files),
            "database_bytes": database.stat().st_size,
            "database_sha256": database_hashes[database_key],
            "query_set_sha256": tree_hash(query_files, query_dir),
        }

    manifest = {
        "date": args.date,
        "machine": {
            "os": f"{platform.system()} {platform.release()} {platform.machine()}",
            "logical_cpus": os.cpu_count(),
        },
        "source": {
            "bloom_head_commit": git("rev-parse", "HEAD"),
            "bloom_source_tree_sha256": tree_hash(source_files, ROOT),
            "duckdb_commit": git("rev-parse", "HEAD", cwd=ROOT / "duckdb"),
            "duckdb_binary_sha256": sha256_file(ROOT / "build/release/duckdb"),
            "prepared_sample_cache_sha256": tree_hash(
                sample_files, ROOT / ".bench_cache/rpt_samples"
            ),
            "prepared_sample_files": len(sample_files),
        },
        "protocol": {
            "query_threads": 1,
            "fresh_process_per_query": True,
            "normal_repetitions": {"warm": 2, "cold": 3},
            "large_ceb_repetitions": {"warm": 1, "cold": 1},
            "aggregation": "sum of per-query medians",
            "cold_state": "POSIX_FADV_DONTNEED before every query",
            "residency_gate": {
                "normal_workloads": "full-file fincore before every query",
                "ceb_and_ceb_stack": "full-file fincore on first, every 50th, and final query",
            },
            "prepared": {"target_rows": 10000, "preloaded_before_timer": True},
            "instant_warm": {
                "access": "scattered",
                "access_points": 256,
                "rows_per_access": 32,
                "parallelism": 8,
                "seed": 2,
            },
            "instant_cold": {
                "access": "block",
                "windows": 16,
                "parallelism": 4,
                "target_rows": 10000,
                "seed": 2,
            },
        },
        "workloads": workloads,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
