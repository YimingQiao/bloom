# Benchmarks

[Back to Bloom](../../README.md)

## DuckDB 2.0

See the [current measurements and methodology](duckdb-2.0/README.md) and
[all per-query timings](duckdb-2.0/timings.csv).

## Historical results

The results below were measured on the previous DuckDB development commit
`21aca042`; they are historical results, not DuckDB 2.0 measurements.
They compare Bloom's default prepared sampling with the DuckDB
baseline using one thread. The database is brought into the OS page cache
before each benchmark process starts. Each query then gets one same-process
warmup and five timed runs; CEB Stack uses one timed run because of its scale.
The table reports the sum of per-query medians, or the single observations for
CEB Stack. Both sides use the same database and keep DuckDB's built-in join
filters enabled. All reported queries completed successfully.

| Workload | Database | Queries | DuckDB baseline | Bloom | Total speedup |
|---|---:|---:|---:|---:|---:|
| [CEB IMDB](https://github.com/learnedsystems/ceb) | compressed, 2.05 GB | 3,133 | 1,625.939 s | 737.424 s | **2.205×** |
| [JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | uncompressed, 4.12 GB | 113 | 28.418 s | 18.931 s | **1.501×** |
| [JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | compressed, 2.05 GB | 113 | 34.847 s | 24.477 s | **1.424×** |
| [STATS-CEB](https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark) | simplified Stack Overflow, 22 MB | 146 | 359.363 s | 258.908 s | **1.388×** |
| [CEB Stack](https://rmarcus.info/stack.html) | Stack Overflow, 51 GB | 6,191 | 4,295.507 s | 3,131.301 s | **1.372×** |
| TPC-H SF10 | 2.68 GB | 22 | 19.888 s | 17.782 s | **1.118×** |
| TPC-DS SF10 | 3.19 GB | 99 | 77.596 s | 73.687 s | **1.053×** |

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

## Running the benchmarks

Run these commands from the repository root. The native benchmark runner uses
one same-process warmup followed by timed runs. First build it with the TPC-H
and TPC-DS data generators:

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

The Performance Regression workflow runs the complete JOB and TPC-H SF1
workloads against the base commit, the candidate, and the candidate with Bloom
disabled. It reports Bloom's total and per-query-geomean speedups over DuckDB,
as well as commit-to-commit query slowdowns of at least 10%. CI fails when a
workload's commit-to-commit query-time geomean increases by at least 10% or
50ms.
