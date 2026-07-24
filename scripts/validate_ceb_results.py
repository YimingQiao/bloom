#!/usr/bin/env python3
"""Compare CEB query results with Bloom RPT disabled and enabled.

CEB does not ship golden result files. This validator executes each query in
separate DuckDB processes with Bloom RPT disabled and enabled, then compares
the CSV results as multisets.
"""

import argparse
import csv
import io
import json
import os
import re
import subprocess
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_QUERY_ROOT = (
    ROOT
    / ".bench_cache"
    / "ceb"
    / "queries"
    / "1f39e9aa85ee64249f60bfa59543e8707b228644"
    / "ceb-imdb-3k"
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--query-root", type=Path, default=DEFAULT_QUERY_ROOT)
    parser.add_argument(
        "--duckdb",
        type=Path,
        default=ROOT / "build" / "release" / "duckdb",
        help="DuckDB CLI built from the same commit as Bloom",
    )
    parser.add_argument(
        "--extension",
        type=Path,
        default=ROOT / "build" / "release" / "extension" / "bloom" / "bloom.duckdb_extension",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "benchmark_results" / "ceb_imdb_result_validation.jsonl",
    )
    parser.add_argument("--pattern", help="Regex matched against each relative query path")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Recheck queries already recorded as equal in the output JSONL",
    )
    return parser.parse_args()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def query_sql(query_path, extension_path, enable_rpt):
    query = query_path.read_text(encoding="utf-8").strip()
    return f"""
LOAD {sql_string(extension_path)};
SET enable_rpt = {str(enable_rpt).lower()};
{query}
"""


def execute_query(duckdb, db, extension, query_path, enable_rpt, env):
    return subprocess.run(
        [
            str(duckdb),
            "-unsigned",
            "-csv",
            "-noheader",
            str(db),
            "-c",
            query_sql(query_path, extension, enable_rpt),
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def csv_multiset(output):
    return Counter(tuple(row) for row in csv.reader(io.StringIO(output)))


def completed_queries(output_path):
    completed = set()
    if not output_path.is_file():
        return completed
    for line in output_path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("equal") is True:
            completed.add(record["query"])
    return completed


def append_record(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, sort_keys=True) + "\n")
        output.flush()


def main():
    args = parse_args()
    db = args.db.resolve()
    query_root = args.query_root.resolve()
    duckdb = args.duckdb.resolve()
    extension = args.extension.resolve()
    for path, description in (
        (db, "database"),
        (query_root, "query root"),
        (duckdb, "DuckDB CLI"),
        (extension, "Bloom extension"),
    ):
        if not path.exists():
            raise SystemExit(f"{description} not found: {path}")

    pattern = re.compile(args.pattern) if args.pattern else None
    already_completed = set() if args.no_resume else completed_queries(args.output)
    queries = []
    for query_path in sorted(query_root.rglob("*.sql")):
        relative = str(query_path.relative_to(query_root))
        if relative in already_completed or pattern and not pattern.search(relative):
            continue
        queries.append((relative, query_path))
    if args.limit is not None:
        queries = queries[: args.limit]

    env = os.environ.copy()
    sample_dir = ROOT / ".bench_cache" / "rpt_samples"
    sample_dir.mkdir(parents=True, exist_ok=True)
    env.setdefault("RPT_SAMPLE_CACHE_DIR", str(sample_dir))

    failures = 0
    for index, (relative, query_path) in enumerate(queries, 1):
        started = time.monotonic()
        baseline = execute_query(duckdb, db, extension, query_path, False, env)
        bloom = execute_query(duckdb, db, extension, query_path, True, env)
        both_succeeded = baseline.returncode == 0 and bloom.returncode == 0
        equal = both_succeeded and csv_multiset(baseline.stdout) == csv_multiset(bloom.stdout)
        record = {
            "query": relative,
            "equal": equal,
            "baseline_returncode": baseline.returncode,
            "bloom_returncode": bloom.returncode,
            "elapsed_seconds": time.monotonic() - started,
        }
        if not equal:
            failures += 1
            record["baseline_stdout"] = baseline.stdout[-4000:]
            record["bloom_stdout"] = bloom.stdout[-4000:]
            record["baseline_stderr"] = baseline.stderr.strip()[-4000:]
            record["bloom_stderr"] = bloom.stderr.strip()[-4000:]
        append_record(args.output, record)
        status = "equal" if equal else "FAILED"
        print(f"[{index}/{len(queries)}] {status}: {relative}", flush=True)
        if not equal and args.fail_fast:
            break

    if failures:
        raise SystemExit(f"{failures} CEB result comparisons failed")


if __name__ == "__main__":
    main()
