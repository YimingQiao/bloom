# Bloom

[![Status: WIP](https://img.shields.io/badge/status-WIP-orange)](#)
[![Version: 0.0.1](https://img.shields.io/badge/version-0.0.1-blue)](https://github.com/YimingQiao/bloom/tree/v0.0.1)

A DuckDB extension for **robust predicate transfer (RPT)**. It propagates Bloom
and bitmap filters bidirectionally across a query's joins, pruning each table's
rows before the joins run. It loads into **unmodified** DuckDB — no patched build.

## How it works

- **Adaptive-excitation optimizer.** An optimizer extension rewrites the logical
  plan: it samples each table, estimates cardinalities, and floods Bloom/bitmap
  filters along join edges in rounds (smallest table first), re-estimating after
  each transfer. Highly-filtered tables are materialized in memory and reused.
- **Filter pushdown on unmodified DuckDB.** Each transfer filter is wrapped in a
  DuckDB built-in `ExpressionFilter` and pushed into the base scan's table
  filters, so it runs on the scan's fast path and also drives **row-group
  statistics pruning**. This is why Bloom targets DuckDB `main` — it relies on
  the extensible table-filter API ([duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633),
  merged 2026-02-12), which is not in the v1.5.x stable line.
- **Sampling only.** Bloom uses the sampling cardinality estimator. The research
  prototype's oracle execution path and its DuckDB-patching `RPTTableFilter` are
  intentionally not included.

**Target:** DuckDB `main`, pinned to commit `21aca042`. See
[`docs/UPDATING.md`](docs/UPDATING.md) to move the pin forward.

## Build

```bash
git submodule update --init --recursive
make release                 # or: GEN=ninja make release
```

The loadable extension lands at
`build/release/extension/bloom/bloom.duckdb_extension`. It is enabled by default
once loaded.

## Configuration

Every setting can be changed at runtime with `SET <name> = <value>`; each also
reads an `RPT_*` environment variable at load time, which is handy for
benchmark-runner jobs. The two most useful are `enable_rpt` (turn RPT off for an
A/B comparison) and `rpt_sample_cache_dir`.

| Setting | Env var | Default | Meaning |
|---|---|---|---|
| `enable_rpt` | `RPT_ENABLE` | `true` | Master on/off switch for the optimizer. |
| `rpt_sample_cache_dir` | `RPT_SAMPLE_CACHE_DIR` | `auto` | Where per-table samples are cached. `auto` uses `<database>.rpt_samples/`; set a path to share a cache, or `''` to disable disk caching. |
| `rpt_sample_size` | `RPT_SAMPLE_SIZE` | `10000` | Target rows for the **on-disk per-table sample** used to estimate base-table cardinalities. |
| `rpt_sample_rate` | `RPT_SAMPLE_RATE` | `0.01` | Chunk-sampling fraction for estimating cardinalities of **already-materialized intermediate data** during flooding. Distinct from `rpt_sample_size`. |

Advanced / diagnostic:

| Setting | Env var | Default | Meaning |
|---|---|---|---|
| `rpt_sample_memory_cache` | `RPT_SAMPLE_MEMORY_CACHE` | `true` | Also keep samples in the process object cache to skip disk reloads across queries. |
| `rpt_log_transfer_steps` | `RPT_LOG_TRANSFER_STEPS` | `false` | Print the transfer plan (cardinalities, rounds, timeline) to stderr. |
| `rpt_late_materialize` | `RPT_LATE_MATERIALIZE` | `false` | **Experimental.** Rowid-based late materialization. Correct but currently a net slowdown (the rowid re-fetch beats column materialization only for very selective transfers on wide tables); left off by default. |

## Benchmarks

Bloom RPT vs. the DuckDB baseline (both with DuckDB's built-in hash-join Bloom
filters), measured with DuckDB's native `benchmark_runner`: one untimed warmup
plus five timed runs per query, each query summarized by its median. Both sides
read the same database file (2026-07-23, pinned DuckDB `21aca042`).

The latest post-rewrite checks completed every reported query correctly.
Unless noted otherwise, these are one-timed-run health checks, so treat 1–3%
deltas as noise rather than a new stable benchmark. The uncompressed
single-thread row compares the current Bloom run with the comparable historical
five-run DuckDB baseline.

Single-thread:

| Workload | Database | Queries | DuckDB baseline | Bloom | Total speedup | Geomean | Faster |
|---|---:|---:|---:|---:|---:|---:|---:|
| IMDB (JOB) | uncompressed, 4.12 GB | 113 | 29.277 s | 19.133 s | **1.530×** | **1.465×** | 82/113 |
| IMDB (JOB) | compressed, 2.05 GB | 113 | 35.162 s | 24.951 s | **1.409×** | **1.351×** | 81/113 |
| TPC-H SF10 | 2.68 GB | 22 | 19.885 s | 17.785 s | **1.118×** | **1.077×** | 12/22 |
| TPC-DS SF10 | 3.19 GB | 99 | 76.750 s | 74.290 s | **1.033×** | **1.044×** | 50/99 |

Eight threads:

| Workload | Database | Queries | DuckDB baseline | Bloom | Total speedup | Geomean | Faster |
|---|---:|---:|---:|---:|---:|---:|---:|
| IMDB (JOB) | uncompressed, 4.12 GB | 113 | 7.167 s | 5.170 s | **1.386×** | **1.296×** | 73/113 |
| IMDB (JOB) | compressed, 2.05 GB | 113 | 8.123 s | 6.042 s | **1.344×** | **1.258×** | 72/113 |
| TPC-H SF10 | 2.68 GB | 22 | 3.209 s | 3.291 s | 0.975× | 0.952× | 9/22 |
| TPC-DS SF10 | 3.19 GB | 99 | 14.649 s | 14.660 s | 0.999× | 0.995× | 47/99 |

TPC-DS 8-thread execution is the post-CTE-lifter check: all 99 queries,
including the eight queries that previously failed, completed correctly. The
uncompressed IMDB rerun also completed all 113 queries; an earlier intermittent
process crash did not reproduce.

IMDB — a many-join workload — is where predicate transfer pays off most. On
TPC-H/TPC-DS the gains are smaller (fewer, larger joins already served well by
DuckDB's own filters), and at 8 threads short queries can regress because the
transfer phase's serial parts don't shrink with thread count. `docs/porting-memory.md`
has the measured breakdown and the open optimization directions.

### Reproducing

Build the runner (add the data generators for TPC-H / TPC-DS):

```bash
CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release -j48
```

Run a whole workload, RPT vs. baseline, at one or more thread counts:

```bash
python3 scripts/run_benchmark_suite.py --workload imdb --threads 1 8
python3 scripts/run_benchmark_suite.py --workload tpch_sf10
python3 scripts/run_benchmark_suite.py --workload tpcds_sf10
```

It prints the total-time / speedup / geomean table above and writes raw
per-run timings under `benchmark_results/`. The reported scale-factor
workloads are `imdb`, `tpch_sf10`, `tpcds_sf10`, and `appian`.

For a single configuration or a quick spot check, call the lower-level runner
directly (`--baseline` disables RPT; `--pattern` selects queries):

```bash
python3 scripts/run_benchmark.py --workload imdb --db /path/to/imdb.duckdb
python3 scripts/run_benchmark.py --workload imdb --db /path/to/imdb.duckdb --baseline
python3 scripts/run_benchmark.py --workload tpcds_sf10 --pattern 'benchmark/tpcds_sf10/03.benchmark' --timed-runs 1
```

Both scripts only synthesize temporary benchmark definitions that `require
bloom`; execution, timing, and result verification stay in DuckDB's native
runner. Databases and sample caches persist under `.bench_cache/`. Provide a
database with `--db`, or let the runner generate one via the `tpch`/`tpcds`
extensions on first use.

### CEB IMDB workloads

Bloom also integrates the IMDB workloads from the
[Cardinality Estimation Benchmark](https://github.com/learnedsystems/CEB):

- `ceb_imdb`: the recommended 3,133-query unique-plan subset.
- `ceb_imdb_full`: the complete 13,646-query workload.

The first run automatically downloads the pinned SQL archive, verifies its
SHA-256 checksum, and caches the extracted queries under `.bench_cache/ceb/`.
If `imdb.duckdb` is absent, the runner also builds it automatically from
DuckDB's JOB Parquet files. You can prepare the SQL without running it:

```bash
python3 scripts/prepare_ceb.py --print-root
```

Run the recommended workload or the full workload:

```bash
python3 scripts/run_benchmark_suite.py --workload ceb_imdb --threads 1
python3 scripts/run_benchmark_suite.py --workload ceb_imdb_full --threads 1
```

The full workload is substantially more expensive. For a smoke test, select
one generated benchmark by its `<template>__<query-file-stem>` name:

```bash
python3 scripts/run_benchmark.py --workload ceb_imdb \
  --pattern 'benchmark/ceb_imdb/1a__.*.benchmark' --timed-runs 1
```

CEB does not publish result-answer files with this SQL bundle, so these runs
verify successful query execution and compare Bloom against the DuckDB
baseline; they do not compare result values against golden files. Query
provenance and the pinned source revision are documented in
[`benchmark/ceb/README.md`](benchmark/ceb/README.md).
