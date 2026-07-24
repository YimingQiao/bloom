# Bloom

[![Version: 0.0.1](https://img.shields.io/badge/version-0.0.1-blue)](extension_config.cmake)
[![CI](https://github.com/YimingQiao/bloom/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/YimingQiao/bloom/actions/workflows/MainDistributionPipeline.yml)

A DuckDB extension for **robust predicate transfer (RPT)**. Unlike approaches
that design a complete transfer plan before executing it, Bloom combines
sampling-based estimates with execution-guided decisions. Sampling predicts
which transfers are likely to be effective; Bloom then executes one transfer,
observes the result, and decides the next step using the updated cardinalities.
This combination keeps predicate transfer both adaptive and efficient.

DuckDB already pushes Bloom filters from selective hash joins into probe-side
scans ([duckdb/duckdb#19502](https://github.com/duckdb/duckdb/pull/19502)).
Bloom extends this join-local, one-way optimization into bidirectional
predicate transfer across multiple joins.

## How it works

- **Sampling-guided, execution-driven transfer.** Sampling estimates the
  potential benefit of candidate transfers before they run. Bloom chooses one
  Bloom/bitmap-filter transfer, executes it, and re-estimates the affected tables
  before choosing again. Sampling avoids unproductive work, while execution
  feedback prevents the plan from being locked to its initial estimates.
  Highly-filtered tables are materialized in memory and reused.
- **Scan-level filter pushdown.** Each transfer filter is wrapped in a DuckDB
  built-in `ExpressionFilter` and pushed into the base scan's table filters, so
  it runs on the scan's fast path and also drives **row-group statistics
  pruning**. This is why Bloom targets DuckDB `main` — it relies on the
  extensible table-filter API ([duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633),
  merged 2026-02-12), which is not in the v1.5.x stable line.

**Target:** DuckDB `main`, pinned to commit `21aca042`. Bloom is preparing for
its first supported release with DuckDB 1.6; see [RELEASING.md](RELEASING.md)
for the compatibility and release gates.

## Build

```bash
git submodule update --init --recursive
make release                 # or: GEN=ninja make release
```

The loadable extension lands at
`build/release/extension/bloom/bloom.duckdb_extension`. It is enabled by default
once loaded:

```sql
LOAD 'build/release/extension/bloom/bloom.duckdb_extension';
SELECT current_setting('enable_rpt');
```

Until the DuckDB 1.6 community release, the extension must be built and loaded
against the exact pinned DuckDB commit above. DuckDB extensions are
version-specific; a binary built for another DuckDB version is rejected.

## Benchmarks

Bloom RPT vs. the DuckDB baseline, measured with DuckDB's native
`benchmark_runner`: one untimed warmup followed by five timed runs per query,
taking the median as the query time. Both sides retain DuckDB's built-in join
filters and read the same database file (2026-07-23, pinned DuckDB
`21aca042`). The results below use one thread.

The latest post-rewrite checks completed every reported query correctly.

| Workload | Database | Queries | DuckDB baseline | Bloom | Total speedup |
|---|---:|---:|---:|---:|---:|
| CEB | compressed, 2.05 GB | 3,133 | 1,625.939 s | 727.335 s | **2.235×** |
| IMDB (JOB) | uncompressed, 4.12 GB | 113 | 29.277 s | 19.047 s | **1.537×** |
| IMDB (JOB) | compressed, 2.05 GB | 113 | 35.162 s | 24.511 s | **1.435×** |
| TPC-H SF10 | 2.68 GB | 22 | 19.885 s | 17.685 s | **1.124×** |
| TPC-DS SF10 | 3.19 GB | 99 | 76.750 s | 73.393 s | **1.046×** |

IMDB — especially CEB's larger collection of many-join queries — is where
predicate transfer pays off most. On TPC-H/TPC-DS the gains are smaller because
their fewer, larger joins are already served well by DuckDB's own filters.

### Reproducing

Build the runner (add the data generators for TPC-H / TPC-DS):

```bash
CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release -j48
```

Run a whole workload, RPT vs. baseline, with one thread:

```bash
python3 scripts/run_benchmark_suite.py --workload imdb --threads 1
python3 scripts/run_benchmark_suite.py --workload ceb_imdb --threads 1
python3 scripts/run_benchmark_suite.py --workload tpch_sf10 --threads 1
python3 scripts/run_benchmark_suite.py --workload tpcds_sf10 --threads 1
```

It prints a total-time and speedup summary and writes raw per-run timings under
`benchmark_results/`. The reported scale-factor workloads are `imdb`,
`tpch_sf10`, `tpcds_sf10`, and `appian`.

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
extensions on first use. CEB SQL is downloaded and checksum-verified
automatically.

## Roadmap

- **Improve multi-threaded execution.** Bloom currently separates predicate
  transfer from the main query execution, which limits parallelism in the
  transfer phase. Future work will focus on making transfer fully parallel and
  better integrated with DuckDB's multi-threaded execution.

## Configuration

Every setting can be changed at runtime with `SET <name> = <value>`; each also
reads an `RPT_*` environment variable at load time. The two most useful are
`enable_rpt` (turn RPT off for an A/B comparison) and
`rpt_sample_cache_dir`.

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
