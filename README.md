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
| [CEB Stack](https://rmarcus.info/stack.html) | Stack Overflow, 51 GB | 6,191 | 4,295.507 s | 3,131.301 s | **1.372×** |
| [STATS-CEB](https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark) | simplified Stack Overflow, 22 MB | 146 | 359.363 s | 258.417 s | **1.391×** |
| IMDB ([JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)) | uncompressed, 4.12 GB | 113 | 29.277 s | 19.047 s | **1.537×** |
| IMDB ([JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)) | compressed, 2.05 GB | 113 | 35.162 s | 24.511 s | **1.435×** |
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

### Running the benchmarks

The scripts use the same workloads and measurement procedure as the table
above. First build the runner with the TPC-H and TPC-DS data generators:

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

## Roadmap

- **Improve multi-threaded execution.** Bloom currently runs filter transfer
  separately from the main query, which limits parallelism during that phase.

## Configuration

Bloom works without tuning. Two settings are useful in normal operation:

```sql
SET enable_rpt = false;
SET rpt_sample_cache_dir = '/path/to/cache';
```

`enable_rpt` defaults to `true`; disable it for troubleshooting or an A/B
comparison. `rpt_sample_cache_dir` defaults to `auto`, which stores samples
under `<database>.rpt_samples/`. Set another directory to share a cache, or `''`
to disable disk caching.
