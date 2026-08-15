#!/usr/bin/env python3
"""End-to-end oracle/prepared/instant comparison with warm or cold base data."""

import argparse
import gzip
import hashlib
import json
import os
import re
import resource
import shutil
import subprocess
import time
from pathlib import Path
from statistics import median


BLOOM_ROOT = Path(__file__).resolve().parents[3]
NATIVE_ROOT = Path.home() / "projects" / "native-predicate-transfer"
RUN_TIME = re.compile(r"Run Time \(s\): real ([0-9.]+)")

WORKLOADS = {
    "job": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/imdb_compressed.duckdb",
        "queries": BLOOM_ROOT / "duckdb/benchmark/imdb_plan_cost/queries",
    },
    "job_uncompressed": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/imdb_pre_regen_20260401 copy.duckdb",
        "queries": BLOOM_ROOT / "duckdb/benchmark/imdb_plan_cost/queries",
    },
    "ceb": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/imdb_compressed.duckdb",
        "queries": BLOOM_ROOT
        / ".bench_cache/ceb/queries/1f39e9aa85ee64249f60bfa59543e8707b228644/ceb-imdb-3k",
        "recursive": True,
    },
    "ceb_stack": {
        "db": BLOOM_ROOT / ".bench_cache/data/ceb_stack.duckdb",
        "queries": BLOOM_ROOT
        / ".bench_cache/ceb_stack/queries"
        / "6dd6a8699046a61a365722bf28c90acbf7764a2baf206114012b9cf2c8b7b918/stack",
        "recursive": True,
        "setup": "SET schema='public';\n",
    },
    "stats_ceb": {
        "db": BLOOM_ROOT / ".bench_cache/data/stats_ceb.duckdb",
        "queries": BLOOM_ROOT
        / ".bench_cache/stats_ceb/assets/670cb8d4bf4cbfa32f94fdf17f33973d3fd67d1b/queries",
    },
    "tpch_sf10": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/tpch_sf10.duckdb",
        "queries": BLOOM_ROOT / "duckdb/extension/tpch/dbgen/queries",
    },
    "appian": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/ads.5m.duck",
        "queries": BLOOM_ROOT / "duckdb/benchmark/appian_benchmarks/queries",
    },
    "tpcds_sf10": {
        "db": NATIVE_ROOT / "duckdb/duckdb_benchmark_data/tpcds_sf10.duckdb",
        "queries": BLOOM_ROOT / "benchmark/queries/tpcds",
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=sorted(WORKLOADS), required=True)
    parser.add_argument("--method", choices=("oracle", "prepared", "instant"), required=True)
    parser.add_argument("--state", choices=("warm", "cold"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--queries", help="Comma-separated query stems")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--sample-seed", type=int, default=2)
    parser.add_argument("--instant-access-points", type=int, default=256)
    parser.add_argument("--instant-rows-per-access", type=int, default=32)
    parser.add_argument("--instant-block-windows", type=int, default=16)
    parser.add_argument(
        "--instant-snapshot",
        action="store_true",
        help="use the opt-in transaction-snapshot path for native instant sampling",
    )
    parser.add_argument(
        "--transfer-log",
        action="store_true",
        help="record transfer/sample diagnostics for plan-quality audits (not performance runs)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="append only missing query/repetition records to an interrupted cell",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1,
        help="print progress every N scheduled records (default: every record)",
    )
    parser.add_argument(
        "--residency-check-every",
        type=int,
        default=1,
        help="run the full-file fincore gate every N records; eviction still happens every cold record",
    )
    parser.add_argument("--sample-cache-dir", type=Path, default=BLOOM_ROOT / ".bench_cache/rpt_samples")
    parser.add_argument("--oracle-cache", type=Path, default=NATIVE_ROOT / "rpt_cardinality_cache.txt")
    parser.add_argument("--prime-oracle-cache", action="store_true")
    parser.add_argument("--preload-audit", action="store_true")
    parser.add_argument(
        "--preload-audit-only",
        action="store_true",
        help="audit prepared-sample preload after base-file eviction, write preload_audit.json, and exit",
    )
    return parser.parse_args()


def evict_file(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(descriptor)


def warm_file(path):
    subprocess.run(["dd", f"if={path}", "of=/dev/null", "bs=32M", "status=none"], check=True)


def resident_bytes(path):
    result = subprocess.run(
        ["fincore", "--bytes", "--noheadings", "--output", "RES", str(path)],
        text=True,
        capture_output=True,
        check=True,
    )
    return int(result.stdout.strip())


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
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


def result_hash(stdout):
    lines = [line for line in stdout.splitlines() if not line.startswith("Run Time (s):")]
    # Compare result bags, not incidental output order from different plans.
    return hashlib.sha256(("\n".join(sorted(lines)) + "\n").encode()).hexdigest()


def query_id(query_dir, query_file):
    return "__".join(query_file.relative_to(query_dir).with_suffix("").parts)


def selected_queries(query_dir, recursive, selection):
    query_files = sorted(query_dir.rglob("*.sql") if recursive else query_dir.glob("*.sql"))
    if not selection:
        return query_files
    requested = set(selection.split(","))
    result = [path for path in query_files if query_id(query_dir, path) in requested]
    missing = requested - {query_id(query_dir, path) for path in result}
    if missing:
        raise SystemExit(f"unknown queries: {','.join(sorted(missing))}")
    return result


def common_setup(threads, workload_setup=""):
    return f"SET threads={threads};\nSET enable_progress_bar=false;\n{workload_setup}"


def prepared_preload(query, threads, timed, workload_setup="", transfer_log=False):
    sql = common_setup(threads, workload_setup)
    sql += "SET rpt_log_transfer_steps=false;\n"
    sql += ".once /dev/null\n"
    sql += "SELECT * FROM rpt_preload_samples();\n"
    if timed:
        sql += f"SET rpt_log_transfer_steps={'true' if transfer_log else 'false'};\n"
        sql += ".timer on\n" + query + "\n"
    return sql


def measured_sql(method, query, threads, workload_setup="", transfer_log=False):
    if method == "prepared":
        return prepared_preload(query, threads, True, workload_setup, transfer_log)
    return common_setup(threads, workload_setup) + ".timer on\n" + query + "\n"


def method_config(args, oracle_workdir):
    env = os.environ.copy()
    if args.method == "oracle":
        env.update({"RPT_ENABLE": "1", "RPT_USE_ORACLE": "1", "RPT_LOG_TRANSFER_STEPS": "0"})
        return NATIVE_ROOT / "build/release/duckdb", oracle_workdir, env
    env.update(
        {
            "RPT_ENABLE": "1",
            "RPT_SAMPLE_MODE": args.method,
            "RPT_INSTANT_ACCESS": "scattered" if args.state == "warm" else "block",
            "RPT_INSTANT_SNAPSHOT": "1" if args.instant_snapshot else "0",
            "RPT_SAMPLE_CACHE_DIR": str(args.sample_cache_dir.resolve()),
            "RPT_SAMPLE_MEMORY_CACHE": "1",
            "RPT_INSTANT_ACCESS_POINTS": str(args.instant_access_points),
            "RPT_INSTANT_ROWS_PER_ACCESS": str(args.instant_rows_per_access),
            "RPT_INSTANT_BLOCK_WINDOWS": str(args.instant_block_windows),
            "RPT_SAMPLE_SEED": str(args.sample_seed),
            "RPT_LOG_TRANSFER_STEPS": "1" if args.transfer_log else "0",
        }
    )
    return BLOOM_ROOT / "build/release/duckdb", BLOOM_ROOT, env


def run_process(binary, cwd, env, db, sql):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.perf_counter()
    result = subprocess.run(
        [str(binary), "-readonly", "-batch", "-noheader", "-csv", str(db)],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=cwd,
        env=env,
        check=False,
    )
    wall_ms = (time.perf_counter() - started) * 1000
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    return result, wall_ms, (after.ru_inblock - before.ru_inblock) * 512


def source_files():
    files = [BLOOM_ROOT / "CMakeLists.txt"]
    files.extend(path for path in (BLOOM_ROOT / "src").rglob("*") if path.is_file())
    return files


def verify_current_binary(binary, method):
    if method == "oracle":
        return None
    newest = max(source_files(), key=lambda path: path.stat().st_mtime_ns)
    if binary.stat().st_mtime_ns < newest.stat().st_mtime_ns:
        raise SystemExit(
            f"DuckDB shell is older than {newest.relative_to(BLOOM_ROOT)}; "
            "rebuild the duckdb target before benchmarking"
        )
    return newest


def write_run_manifest(args, binary, db, query_dir, query_files, newest_source, filename="run_manifest.json"):
    sources = source_files() if args.method != "oracle" else []
    sample_files = (
        sorted(args.sample_cache_dir.resolve().glob("*.sample"))
        if args.method == "prepared"
        else []
    )
    manifest = {
        "workload": args.workload,
        "method": args.method,
        "state": args.state,
        "repetitions": args.repetitions,
        "threads": args.threads,
        "sample_seed": args.sample_seed,
        "instant_access_points": args.instant_access_points,
        "instant_rows_per_access": args.instant_rows_per_access,
        "instant_block_windows": args.instant_block_windows,
        "instant_snapshot": args.instant_snapshot,
        "transfer_log": args.transfer_log,
        "binary": str(binary.resolve()),
        "binary_bytes": binary.stat().st_size,
        "binary_mtime_ns": binary.stat().st_mtime_ns,
        "binary_sha256": file_hash(binary),
        "source_tree_sha256": tree_hash(sources, BLOOM_ROOT) if sources else None,
        "newest_source": (
            str(newest_source.relative_to(BLOOM_ROOT)) if newest_source is not None else None
        ),
        "database": str(db),
        "database_bytes": db.stat().st_size,
        "database_sha256": file_hash(db),
        "queries": len(query_files),
        "query_set_sha256": tree_hash(query_files, query_dir),
        "sample_cache": str(args.sample_cache_dir.resolve()) if sample_files else None,
        "sample_files": len(sample_files),
        "sample_cache_sha256": (
            tree_hash(sample_files, args.sample_cache_dir.resolve()) if sample_files else None
        ),
    }
    path = args.output / filename
    if filename == "run_manifest.json" and args.resume and (args.output / "results.jsonl").is_file():
        if not path.is_file():
            raise RuntimeError(f"cannot safely resume without {path}")
        previous = json.loads(path.read_text(encoding="utf-8"))
        if previous != manifest:
            raise RuntimeError(f"benchmark inputs changed since {path} was written")
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def establish_state(db, state, verify=True):
    if state == "cold":
        if not verify:
            evict_file(db)
            return None
        for _ in range(3):
            evict_file(db)
            resident = resident_bytes(db)
            if resident == 0:
                return resident
        raise RuntimeError(f"could not evict the complete database: {resident} bytes remain resident")
    if not verify:
        return None
    resident = resident_bytes(db)
    if resident < db.stat().st_size:
        warm_file(db)
        resident = resident_bytes(db)
    if resident < db.stat().st_size:
        raise RuntimeError(f"could not make the complete database resident: {resident}/{db.stat().st_size}")
    return resident


def audit_prepared_preload(args, binary, workdir, env, db, workload_setup):
    resident_before = establish_state(db, "cold")
    result, wall_ms, input_bytes = run_process(
        binary, workdir, env, db, prepared_preload("", args.threads, False, workload_setup)
    )
    if result.returncode:
        raise RuntimeError(f"prepared preload audit failed: {result.stderr}")
    audit = {
        "workload": args.workload,
        "resident_before": resident_before,
        "resident_after": resident_bytes(db),
        "physical_input_bytes": input_bytes,
        "process_wall_ms": wall_ms,
    }
    path = args.output / "preload_audit.json"
    path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"prepared preload audit: {path}", flush=True)
    return audit


def prime_oracle(args, query_files, query_dir, db, binary, workdir, env, workload_setup):
    for index, query_file in enumerate(query_files, 1):
        establish_state(db, "warm")
        query = query_file.read_text(encoding="utf-8").strip()
        result, _, _ = run_process(
            binary, workdir, env, db, common_setup(args.threads, workload_setup) + query + "\n"
        )
        if result.returncode:
            raise RuntimeError(f"oracle prime failed for {query_file.stem}: {result.stderr}")
        if index == 1 or index % 10 == 0 or index == len(query_files):
            print(
                f"oracle prime {index}/{len(query_files)}: {query_id(query_dir, query_file)}",
                flush=True,
            )


def main():
    args = parse_args()
    if (
        args.repetitions < 1
        or args.threads < 1
        or args.sample_seed < 0
        or args.instant_access_points < 1
        or args.instant_rows_per_access < 1
        or args.instant_block_windows < 1
        or args.progress_every < 1
        or args.residency_check_every < 1
    ):
        raise SystemExit(
            "repetitions, threads, progress-every, and residency-check-every must be positive; "
            "sample-seed must be non-negative; instant sampling budgets must be positive"
        )
    spec = WORKLOADS[args.workload]
    db = spec["db"].resolve()
    query_dir = spec["queries"].resolve()
    workload_setup = spec.get("setup", "")
    args.output = args.output.resolve()
    if not db.is_file() or not query_dir.is_dir():
        raise SystemExit("database or query directory is missing")
    query_files = selected_queries(query_dir, spec.get("recursive", False), args.queries)
    args.output.mkdir(parents=True, exist_ok=True)
    raw_dir = args.output / "raw"
    raw_dir.mkdir(exist_ok=True)

    oracle_workdir = args.output / "oracle_workdir"
    if args.method == "oracle":
        oracle_workdir.mkdir(exist_ok=True)
        local_cache = oracle_workdir / "rpt_cardinality_cache.txt"
        if not local_cache.exists():
            shutil.copy2(args.oracle_cache.resolve(), local_cache)
    binary, workdir, env = method_config(args, oracle_workdir)
    if not binary.is_file():
        raise SystemExit(f"DuckDB shell is missing: {binary}")
    newest_source = verify_current_binary(binary, args.method)
    manifest_name = "preload_run_manifest.json" if args.preload_audit_only else "run_manifest.json"
    write_run_manifest(args, binary, db, query_dir, query_files, newest_source, manifest_name)

    if args.preload_audit_only:
        if args.method != "prepared":
            raise SystemExit("--preload-audit-only requires --method prepared")
        audit_prepared_preload(args, binary, workdir, env, db, workload_setup)
        return

    if args.method == "oracle" and args.prime_oracle_cache:
        prime_oracle(args, query_files, query_dir, db, binary, workdir, env, workload_setup)
    oracle_cache = oracle_workdir / "rpt_cardinality_cache.txt"
    oracle_hash = file_hash(oracle_cache) if args.method == "oracle" else None

    result_path = args.output / "results.jsonl"
    completed = set()
    output_mode = "w"
    if args.resume and result_path.is_file():
        for line_number, line in enumerate(result_path.read_text(encoding="utf-8").splitlines(), 1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise RuntimeError(f"{result_path}:{line_number}: invalid JSON: {error}") from error
            identity = (row["repetition"], row["query"])
            if identity in completed:
                raise RuntimeError(f"{result_path}: duplicate completed record {identity}")
            if (
                row["workload"] != args.workload
                or row["method"] != args.method
                or row["state"] != args.state
                or row.get("instant_snapshot", False) != args.instant_snapshot
                or row.get("transfer_log", False) != args.transfer_log
                or row["returncode"] != 0
                or row["query_ms"] is None
            ):
                raise RuntimeError(f"{result_path}: incompatible or failed resumed record {identity}")
            completed.add(identity)
        output_mode = "a"
        print(f"resume: {len(completed)} completed records in {result_path}", flush=True)

    with result_path.open(output_mode, encoding="utf-8") as output:
        for repetition in range(1, args.repetitions + 1):
            for query_index, query_file in enumerate(query_files, 1):
                current_query_id = query_id(query_dir, query_file)
                if (repetition, current_query_id) in completed:
                    continue
                query = query_file.read_text(encoding="utf-8").strip()
                scheduled_index = (repetition - 1) * len(query_files) + query_index
                scheduled_total = args.repetitions * len(query_files)
                residency_checked = (
                    scheduled_index == 1
                    or scheduled_index % args.residency_check_every == 0
                    or scheduled_index == scheduled_total
                )
                preload_resident = None
                preload_input = None
                if (
                    args.method == "prepared"
                    and args.preload_audit
                    and repetition == 1
                    and query_file == query_files[0]
                ):
                    establish_state(db, "cold")
                    audit, _, preload_input = run_process(
                        binary,
                        workdir,
                        env,
                        db,
                        prepared_preload(query, args.threads, False, workload_setup),
                    )
                    if audit.returncode:
                        raise RuntimeError(f"preload audit failed for {query_file.stem}: {audit.stderr}")
                    preload_resident = resident_bytes(db)

                resident_before = establish_state(db, args.state, residency_checked)
                result, wall_ms, input_bytes = run_process(
                    binary,
                    workdir,
                    env,
                    db,
                    measured_sql(
                        args.method,
                        query,
                        args.threads,
                        workload_setup,
                        args.transfer_log,
                    ),
                )
                combined = result.stdout + result.stderr
                timings = [float(value) * 1000 for value in RUN_TIME.findall(combined)]
                cache_changed = False
                if args.method == "oracle":
                    current_hash = file_hash(oracle_cache)
                    cache_changed = current_hash != oracle_hash
                    oracle_hash = current_hash
                raw_path = raw_dir / f"{current_query_id}-r{repetition}.log.gz"
                with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
                    handle.write("[stdout]\n" + result.stdout + "\n[stderr]\n" + result.stderr)
                record = {
                    "workload": args.workload,
                    "method": args.method,
                    "state": args.state,
                    "instant_snapshot": args.instant_snapshot,
                    "transfer_log": args.transfer_log,
                    "query": current_query_id,
                    "repetition": repetition,
                    "returncode": result.returncode,
                    "query_ms": timings[-1] if timings else None,
                    "process_wall_ms": wall_ms,
                    "input_bytes": input_bytes,
                    "resident_before": resident_before,
                    "resident_after": resident_bytes(db) if residency_checked else None,
                    "residency_checked": residency_checked,
                    "result_sha256": result_hash(result.stdout),
                    "oracle_cache_changed": cache_changed,
                    "preload_resident_bytes": preload_resident,
                    "preload_input_bytes": preload_input,
                }
                output.write(json.dumps(record, sort_keys=True) + "\n")
                output.flush()
                if (
                    scheduled_index == 1
                    or scheduled_index % args.progress_every == 0
                    or scheduled_index == scheduled_total
                ):
                    print(
                        f"{args.workload} {args.method} {args.state} "
                        f"{scheduled_index}/{scheduled_total} {current_query_id} r{repetition}: "
                        f"{record['query_ms'] if record['query_ms'] is not None else 0:.1f}ms "
                        f"read={input_bytes / (1024 * 1024):.1f}MiB "
                        f"resident={'unchecked' if resident_before is None else f'{resident_before / (1024 * 1024):.1f}MiB'}",
                        flush=True,
                    )
                if result.returncode or record["query_ms"] is None:
                    raise RuntimeError(f"query failed: inspect {raw_path}")
                if cache_changed:
                    raise RuntimeError(
                        f"oracle cache miss in measured query {current_query_id}; prime and rerun this output"
                    )
    print(f"results: {result_path}")


if __name__ == "__main__":
    main()
