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
| [JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | compressed, 2.05 GB | 113 | 34.951 s | 24.490 s | **1.427×** |
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

### Prepared and instant sampling

Prepared sampling reuses a maintained 10K-row reservoir. The default resident-
data instant policy instead reads 256 stratified access points of 32 contiguous
rows directly from storage. The current comparison uses the same native runner
as the main table: one discarded warmup per query, five timed runs, one query
thread, a resident database file, and the sum of per-query medians. Instant
sampling is part of every warmup and timed query.

| Workload | Queries | Prepared | Instant | Instant/prepared | Per-query geomean | Instant faster |
|---|---:|---:|---:|---:|---:|---:|
| JOB (compressed) | 113 | 24.490 s | 25.621 s | 1.046× | 1.103× | 6/113 |

The two paths use different row-selection algorithms; the shared seed makes
each path reproducible but does not make their sampled rows identical. In this
run all 113 queries preserve the same directed transfer-edge set, while 48
preserve the complete excitation sequence. Queries with a changed sequence are
1.043× prepared time in aggregate, compared with 1.054× for queries whose
sequence is identical, so the overall difference is dominated by physical
instant-sampling work rather than a workload-level plan regression.

The prepared total is the same measurement reported in the main benchmark
table. A separate historical experiment covers eight workloads with explicit
OS-warm and cold-SSD residency gates. It starts a fresh DuckDB process and does
not run a query warmup, so its absolute times are intentionally kept in its
[full methodology and validation](experiments/2026-08-prepared-instant-full-benchmark/README.md)
rather than mixed with the query-warm results above. Its
[per-query results](experiments/2026-08-prepared-instant-full-benchmark/results/final/QUERY_RESULTS.csv)
and all 42,306 [individual run records](experiments/2026-08-prepared-instant-full-benchmark/results/final/RUNS.csv)
remain available.

### Running the benchmarks

The commands below use the same native benchmark-runner procedure as both
tables above: one same-process warmup followed by timed runs. First build the
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

Compare prepared and instant sampling with the identical procedure:

```bash
python3 scripts/run_benchmark_suite.py \
  --workload imdb --threads 1 --comparison sampling
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
one file per table. `rpt_sample_size` controls prepared reservoirs and the
target row count for block and Parquet instant sampling. Scattered sampling has
an explicit physical shape: `rpt_instant_access_points` multiplied by
`rpt_instant_rows_per_access`. Task fan-out is an internal bounded
implementation detail and does not modify DuckDB's `async_threads` setting.
