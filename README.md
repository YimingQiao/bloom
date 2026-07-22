# Bloom

A DuckDB extension for **robust predicate transfer**. It propagates Bloom and
bitmap filters across joins to prune intermediate results before the joins run.

## Design

- **Target:** stock **DuckDB `main` (v1.6-dev)**, pinned to commit `21aca042`
  (2026-07-21). Installs and loads on unmodified DuckDB — no patched build.
  We target v1.6 rather than the v1.5.x stable line because the extensible
  table-filter API ([duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633),
  merged 2026-02-12) only landed on `main`; v1.5.4 predates it.
- **Approach:** filters are applied via the **expression-callback** path — a Bloom/
  bitmap filter is wrapped in a `ScalarFunction` (probes the filter per chunk) inside
  a DuckDB built-in `ExpressionFilter`, pushed into the base scan's table filters.
  With #20633's `SetFilterPruneCallback` this also does **row-group statistics
  pruning**, all on stock DuckDB (no internals patch). The research prototype used a
  native `RPTTableFilter` that required patching DuckDB; this path needs none.
- Registers an **optimizer extension** that rewrites the logical plan to build and
  push predicate-transfer filters.

Ported from the research prototype's adaptive-excitation implementation. Bloom
uses the sampling cardinality estimator only; the prototype's oracle execution
path is intentionally not included.

## Status

- [x] Stock DuckDB v1.6-dev integration without submodule patches
- [x] Adaptive-excitation predicate-transfer optimizer
- [x] Bloom and bitmap filters through expression-callback pushdown
- [x] Sampling cardinality estimator with memory and disk caches
- [x] SQL settings and environment-variable configuration
- [x] Extension SQL test and IMDB benchmark-runner validation

## Build

```
git submodule update --init --recursive
make release                               # or: GEN=ninja make release
```

Loadable extension lands at `build/release/extension/bloom/bloom.duckdb_extension`.

## Configuration

The extension is enabled by default. Settings can be changed with SQL; the
equivalent `RPT_*` environment variables are useful for benchmark runner jobs.

| SQL setting | Environment variable | Default |
|---|---|---:|
| `enable_rpt` | `RPT_ENABLE` | `true` |
| `rpt_sample_cache_dir` | `RPT_SAMPLE_CACHE_DIR` | `auto` (`<database>.rpt_samples/`) |
| `rpt_sample_size` | `RPT_SAMPLE_SIZE` | `10000` |
| `rpt_sample_rate` | `RPT_SAMPLE_RATE` | `0.01` |
| `rpt_sample_memory_cache` | `RPT_SAMPLE_MEMORY_CACHE` | `true` |
| `rpt_log_transfer_steps` | `RPT_LOG_TRANSFER_STEPS` | `false` |
| `rpt_late_materialize` | `RPT_LATE_MATERIALIZE` | `false` |

## IMDB Benchmark

Build DuckDB's native runner, then run the IMDB benchmark sequentially:

```bash
BUILD_BENCHMARK=1 make release -j2
python3 scripts/run_imdb_benchmark.py --db /path/to/imdb.duckdb
```

The wrapper only creates temporary benchmark definitions containing
`require bloom`; execution, timing, and result verification remain in DuckDB's
native runner. Use `--baseline` for a stock-planner comparison, or
`--pattern benchmark/imdb/32a.benchmark --timed-runs 1` for a quick check.
