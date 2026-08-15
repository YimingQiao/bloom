# Bloom

[![Version: 0.0.1](https://img.shields.io/badge/version-0.0.1-blue)](extension_config.cmake)
[![CI](https://github.com/YimingQiao/bloom/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/YimingQiao/bloom/actions/workflows/MainDistributionPipeline.yml)

Bloom speeds up complex join queries in DuckDB by moving selective filters
across the join graph. Built on **robust predicate transfer (RPT)**, it uses
Bloom and bitmap filters to prune rows before they reach the joins.

Instead of fixing the entire transfer plan up front, Bloom combines lightweight
sampling with execution feedback. Sampling suggests which transfer to try next;
after each step, Bloom observes the new cardinalities and adjusts its decision.
Together, these signals avoid low-value transfers and keep later decisions
grounded in the query's actual behavior.

DuckDB already applies Bloom filters within selective hash joins
([duckdb/duckdb#19502](https://github.com/duckdb/duckdb/pull/19502)). Bloom
complements this by moving filters in both directions and across multiple joins.

## How it works

- **Estimate.** Sample the tables and rank promising filter transfers.
- **Transfer and adapt.** Apply one transfer, update the affected cardinalities,
  and choose again. Selective intermediate results are materialized and reused.
- **Filter at the scan.** Push transfer filters into DuckDB table scans, where
  they can also help with row-group pruning.

## Compatibility

Bloom currently targets DuckDB `main` at commit `21aca042`. It relies on the
extensible table-filter API introduced in
[duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633), so current
builds must use this exact DuckDB commit. Formal support will start with
DuckDB's next stable release. See [RELEASING.md](RELEASING.md) for the release
checklist.

## Build and load

```bash
git submodule update --init --recursive
make release -j"$(nproc)"         # or: GEN=ninja make release -j"$(nproc)"
```

The extension is written to
`build/release/extension/bloom/bloom.duckdb_extension`:

```sql
LOAD 'build/release/extension/bloom/bloom.duckdb_extension';
SELECT current_setting('enable_rpt');
```

## Benchmarks

The results below compare Bloom with the DuckDB baseline using one thread. Each
query gets one warmup and five timed runs, and the table reports the median.
Both sides use the same database and keep DuckDB's built-in join filters
enabled. All reported queries completed correctly.

| Workload | Database | Queries | DuckDB baseline | Bloom | Total speedup |
|---|---:|---:|---:|---:|---:|
| [CEB IMDB](https://github.com/learnedsystems/ceb) | compressed, 2.05 GB | 3,133 | 1,625.939 s | 727.335 s | **2.235×** |
| [JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | uncompressed, 4.12 GB | 113 | 29.277 s | 19.047 s | **1.537×** |
| [JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | compressed, 2.05 GB | 113 | 35.162 s | 24.511 s | **1.435×** |
| [STATS-CEB](https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark) | simplified Stack Overflow, 22 MB | 146 | 359.363 s | 258.417 s | **1.391×** |
| [CEB Stack](https://rmarcus.info/stack.html) | Stack Overflow, 51 GB | 6,191 | 4,295.507 s | 3,131.301 s | **1.372×** |
| TPC-H SF10 | 2.68 GB | 22 | 19.885 s | 17.685 s | **1.124×** |
| TPC-DS SF10 | 3.19 GB | 99 | 76.750 s | 73.393 s | **1.046×** |

[CEB IMDB](https://learnedsystems.mit.edu/cardinality-estimation-benchmark/)
and [CEB Stack](https://rmarcus.info/stack.html) are the IMDB and Stack Overflow
Cardinality Estimation Benchmark workloads, respectively;
[STATS-CEB](https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark)
is a separate 146-query workload over simplified Stack Overflow data; and
[JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) is the original 113-query
Join Order Benchmark over the IMDB data set.

Bloom sees its largest gains on the many-join CEB and JOB workloads. TPC-H and
TPC-DS benefit less because DuckDB's built-in join filters already handle much
of their filtering.

The absolute times below are not a regression comparison with the table above.
The main table discards a query warmup in the same DuckDB process. The sampling
comparison intentionally starts a fresh DuckDB process for every measured
execution; `warm` means that the database file is resident in the OS page cache,
while DuckDB's buffer manager and decompression state still begin cold. Compare
prepared with instant within this protocol, and use the native benchmark runner
above when checking code regressions against the main table.

### Prepared and instant sampling

Bloom supports maintained 10K-row samples and query-time sampling directly
from storage. The table below compares the two paths with one DuckDB query
thread and the same database files; instant sampling runs bounded tasks on
DuckDB's asynchronous task pool. Every measured execution starts in a fresh
process. Warm runs use two repetitions per query; cold SSD runs use three. The
two large CEB workloads use one complete pass per state. Prepared samples are
loaded before timing; instant sampling is timed as part of the query.

| Workload | Warm prepared | Warm instant | Warm ratio | Cold prepared | Cold instant | Cold ratio |
|---|---:|---:|---:|---:|---:|---:|
| CEB IMDB | 1,071.752 s | 1,083.087 s | 1.011x | 1,687.834 s | 1,646.929 s | 0.976x |
| JOB (uncompressed) | 36.849 s | 33.367 s | 0.906x | 95.293 s | 94.667 s | 0.993x |
| JOB (compressed) | 33.214 s | 32.465 s | 0.977x | 50.723 s | 51.484 s | 1.015x |
| STATS-CEB | 259.322 s | 258.909 s | 0.998x | 257.528 s | 257.326 s | 0.999x |
| CEB Stack | 3,801.884 s | 3,815.170 s | 1.003x | 6,529.778 s | 6,634.349 s | 1.016x |
| TPC-H SF10 | 23.613 s | 21.995 s | 0.932x | 40.585 s | 40.345 s | 0.994x |
| TPC-DS SF10 | 84.493 s | 82.566 s | 0.977x | 120.805 s | 119.426 s | 0.989x |
| Appian | 39.349 s | 40.279 s | 1.024x | 42.433 s | 42.006 s | 0.990x |

Across the listed workload totals, instant/prepared is **1.003x** with warm
data and **1.007x** from cold SSD. All 42,306 measured executions succeeded.
Prepared and instant produce identical result bags for 19,533/19,650
state/query pairs; the remaining 117 are verified non-total top-100 boundary
ties in CEB Stack Q13. See the
[full methodology and validation](experiments/2026-08-prepared-instant-full-benchmark/README.md),
[per-query results](experiments/2026-08-prepared-instant-full-benchmark/results/final/QUERY_RESULTS.csv),
and [individual run records](experiments/2026-08-prepared-instant-full-benchmark/results/final/RUNS.csv).

### Running the benchmarks

The commands below use the same native benchmark-runner procedure as the main
Bloom-versus-baseline table: one same-process warmup followed by timed runs.
They do not reproduce the fresh-process prepared/instant table. First build the
runner with the TPC-H and TPC-DS data generators:

```bash
CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release -j"$(nproc)"
```

Run a workload with Bloom and the baseline:

```bash
python3 scripts/run_benchmark_suite.py --workload imdb --threads 1
python3 scripts/run_benchmark_suite.py --workload ceb_imdb --threads 1
python3 scripts/run_benchmark_suite.py --workload tpch_sf10 --threads 1
python3 scripts/run_benchmark_suite.py --workload tpcds_sf10 --threads 1
```

Results and raw timings are written under `benchmark_results/`. For a quick
spot check, use the lower-level runner; `--baseline` disables Bloom and
`--pattern` selects queries:

```bash
python3 scripts/run_benchmark.py --workload imdb --db /path/to/imdb.duckdb
```

Databases and sample caches are kept under `.bench_cache/`. Pass `--db` to use
an existing database. CEB SQL is downloaded and checksum-verified automatically.
The prepared/instant matrix has its own cache-state gates and runner documented
in [its experiment README](experiments/2026-08-prepared-instant-full-benchmark/README.md).

## Roadmap

- **Improve multi-threaded execution.** Bloom currently runs filter transfer
  separately from the main query, which limits parallelism during that phase.

## Configuration

Bloom works without tuning. Prepared sampling is the default: a 10K-row
reservoir per table is persisted and reused across queries. Instant sampling
reads a fresh, query-local sample directly from DuckDB storage or a single
Parquet file and never creates or consumes a prepared sample.

The main settings are:

```sql
SET enable_rpt = false;
SET rpt_sample_cache_dir = '/path/to/cache';
SET rpt_sample_mode = 'instant';
```

`enable_rpt` defaults to `true`; disable it for troubleshooting or an A/B
comparison. `rpt_sample_cache_dir` defaults to `auto`, which stores samples
under `<database>.rpt_samples/`. Set another directory to share a cache, or `''`
to disable disk caching. Prepared samples are keyed by `rpt_sample_seed`, whose
cross-workload-validated default is `2`; changing the seed creates an independent
cache entry and never reuses the previous sample.

`rpt_sample_mode` accepts `prepared` (the default) and `instant`. For resident
native data, instant sampling reads 256 stratified access points x 32 contiguous
rows with an internal task limit of eight. Its performance-first path reads
narrow ranges directly from DuckDB base column segments.

For cold native data, select the block-aligned prefetch policy:

```sql
SET rpt_sample_mode = 'instant';
SET rpt_instant_access = 'block';
```

Parquet instant sampling automatically uses stratified row groups and supports
one file per table. Advanced sample-size and access-shape budgets are exposed
under the `rpt_instant_*` settings for reproducible experiments. Task fan-out is an
internal bounded implementation detail and does not modify DuckDB's
`async_threads` setting.
