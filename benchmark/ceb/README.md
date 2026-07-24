# CEB IMDB workloads

Bloom's benchmark runner supports the two IMDB query sets from the
[Cardinality Estimation Benchmark](https://github.com/learnedsystems/CEB):

| Runner workload | Upstream name | Queries | Templates |
|---|---|---:|---:|
| `ceb_imdb` | CEB IMDB unique-plan subset / 3K | 3,133 | 16 |
| `ceb_imdb_full` | CEB IMDB full / 13K | 13,646 | 16 |

The original CEB repository describes these workloads and is MIT licensed.
Its historical Dropbox SQL links are no longer reliable, so
`scripts/prepare_ceb.py` downloads the same plain SQL files from
[`RyanMarcus/imdb_pg_dataset`](https://github.com/RyanMarcus/imdb_pg_dataset),
pinned to commit `1f39e9aa85ee64249f60bfa59543e8707b228644`. The archive is
verified with SHA-256
`43f4b5984db5b281968a3f548a93cb00cbd8bad7850ce366641592117958754c`.

Queries are downloaded at runtime rather than vendored into this repository.
Both extracted workloads and the source archive live under the gitignored
`.bench_cache/ceb/` directory.

Prepare the SQL:

```bash
python3 scripts/prepare_ceb.py --print-root
```

Run a Bloom-versus-baseline comparison:

```bash
python3 scripts/run_benchmark_suite.py --workload ceb_imdb --threads 1
python3 scripts/run_benchmark_suite.py --workload ceb_imdb_full --threads 1
```

Before reporting a result, strictly check that both logs contain every query
and all five timed runs:

```bash
python3 scripts/summarize_benchmark.py \
  --rpt-log benchmark_results/ceb_imdb_rpt_t1.log \
  --baseline-log benchmark_results/ceb_imdb_base_t1.log \
  --expected-queries 3133 \
  --timed-runs 5
```

Both workloads use the standard JOB/IMDB schema. When no `--db` is supplied,
`scripts/run_benchmark.py` reuses `.bench_cache/data/imdb.duckdb`, creating it
from DuckDB's published JOB Parquet files if necessary. Pass `--db` to use an
explicit compatible IMDB database.

Generated benchmark names use `<template>__<query-file-stem>`, for example
`1a__1a1003`.

The SQL bundle has no golden result files. DuckDB's benchmark runner therefore
checks that every statement binds and executes successfully, but it does not
compare returned values with upstream answers. Run the separate multiset
validator before publishing benchmark numbers:

```bash
python3 scripts/validate_ceb_results.py \
  --db /path/to/imdb.duckdb \
  --output benchmark_results/ceb_imdb_result_validation.jsonl
```

The validator runs every query in separate DuckDB processes with RPT disabled
and enabled, then compares the CSV rows as multisets. Separate processes keep
the two optimizer executions isolated. Its JSONL output is resumable.
