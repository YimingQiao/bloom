# Bloom

A DuckDB extension for **robust predicate transfer (RPT)**. It propagates Bloom
and bitmap filters bidirectionally across a query's joins, pruning each table's
rows before the joins run. It loads into **unmodified** DuckDB — no patched build.

## How it works

- **Adaptive-excitation optimizer.** An optimizer extension rewrites the logical
  plan: it samples each table, estimates cardinalities, and floods Bloom/bitmap
  filters along join edges in rounds (smallest table first), re-estimating after
  each transfer. Highly-filtered tables are materialized in memory and reused.
- **Filter pushdown on stock DuckDB.** Each transfer filter is wrapped in a
  DuckDB built-in `ExpressionFilter` and pushed into the base scan's table
  filters, so it runs on the scan's fast path and also drives **row-group
  statistics pruning**. This is why Bloom targets DuckDB `main` — it relies on
  the extensible table-filter API ([duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633),
  merged 2026-02-12), which is not in the v1.5.x stable line.
- **Sampling only.** Bloom uses the sampling cardinality estimator. The research
  prototype's oracle execution path and its DuckDB-patching `RPTTableFilter` are
  intentionally not included.

**Target:** stock DuckDB `main`, pinned to commit `21aca042`. See
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

Bloom RPT vs. stock DuckDB (both with DuckDB's built-in hash-join Bloom
filters), measured with DuckDB's native `benchmark_runner`: one untimed warmup
plus five timed runs per query, each query summarized by its median. Both sides
read the same database file (2026-07-23, pinned DuckDB `21aca042`).

**Total time** (sum of per-query medians) and **per-query speedup geomean**,
single thread:

| Workload | Queries | DuckDB baseline | Bloom | Total speedup | Geomean | Faster |
|---|---:|---:|---:|---:|---:|---:|
| IMDB (JOB) | 113 | 28.71 s | 19.14 s | **1.50×** | **1.44×** | 83/113 |
| TPC-H SF1 | 22 | 1.87 s | 1.74 s | 1.08× | 1.04× | 12/22 |
| TPC-H SF10 | 22 | 20.33 s | 18.03 s | 1.13× | 1.08× | 14/22 |
| TPC-DS SF1 | 99 | 8.35 s | 8.20 s | 1.02× | 1.00× | 36/99 |
| TPC-DS SF10 | 99 | 77.02 s | 75.34 s | 1.02× | 1.03× | 44/99 |
| Appian | 8 | 40.48 s | 42.30 s | 0.96× | 1.05× | 3/8 |

Multi-threaded (8 threads):

| Workload | Queries | DuckDB baseline | Bloom | Total speedup | Geomean | Faster |
|---|---:|---:|---:|---:|---:|---:|
| IMDB (JOB) | 113 | 7.18 s | 5.01 s | **1.43×** | **1.31×** | 68/113 |
| TPC-H SF10 | 22 | 3.30 s | 3.45 s | 0.96× | 0.94× | 8/22 |

IMDB — a many-join workload — is where predicate transfer pays off most. On
TPC-H/TPC-DS the gains are smaller (fewer, larger joins already served well by
DuckDB's own filters), and at 8 threads short queries can regress because the
transfer phase's serial parts don't shrink with thread count. `docs/porting-memory.md`
has the measured breakdown and the open optimization directions.

### Reproducing

Build the runner (add the data generators for TPC-H / TPC-DS):

```bash
CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release -j$(nproc)
```

Run a whole workload, RPT vs. baseline, at one or more thread counts:

```bash
python3 scripts/run_benchmark_suite.py --workload imdb --threads 1 8
python3 scripts/run_benchmark_suite.py --workload tpch_sf10
python3 scripts/run_benchmark_suite.py --workload tpcds_sf1
```

It prints the total-time / speedup / geomean table above and writes raw
per-run timings under `benchmark_results/`. Workloads: `imdb`, `tpch_sf1`,
`tpch_sf10`, `tpcds_sf1`, `tpcds_sf10`, `appian`.

For a single configuration or a quick spot check, call the lower-level runner
directly (`--baseline` disables RPT; `--pattern` selects queries):

```bash
python3 scripts/run_benchmark.py --workload imdb --db /path/to/imdb.duckdb
python3 scripts/run_benchmark.py --workload imdb --db /path/to/imdb.duckdb --baseline
python3 scripts/run_benchmark.py --workload tpcds_sf1 --pattern 'benchmark/tpcds_sf1/03.benchmark' --timed-runs 1
```

Both scripts only synthesize temporary benchmark definitions that `require
bloom`; execution, timing, and result verification stay in DuckDB's native
runner. Databases and sample caches persist under `.bench_cache/`. Provide a
database with `--db`, or let the runner generate one via the `tpch`/`tpcds`
extensions on first use.
