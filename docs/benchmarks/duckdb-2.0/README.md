# DuckDB 2.0 benchmark measurements

[timings.csv](timings.csv) contains all five timed observations for each
configuration, plus per-query medians and speedups: 113 JOB queries and
22 TPC-H SF10 queries, with no queries omitted.

## Versions and setup

- Bloom v0.0.3, measured at commit
  `6635559f15b7365dfce7b005b3a7c15ccc7d5ba7`.
- DuckDB v2.0.0, pinned to
  `d54cbbe3df17e8e5ce68312f483b4155836112aa`.
- One thread for both configurations, on the same machine and database files.
- JOB: compressed IMDB, 2.05 GB. TPC-H: scale factor 10, 2.68 GB.
- Bloom defaults: prepared sampling, 10,000 rows per table, seed 2;
  experimental late materialization disabled.
- The baseline disables Bloom. DuckDB's native join filters remain enabled
  for both configurations.

The runner brings the database into the OS page cache before starting each
benchmark process. Each query has one discarded same-process warmup followed
by five timed runs. Prepared samples are reused; sample creation or loading
during warmup is excluded. These measurements describe warm query latency,
not cold-start latency. Timed queries include optimization and execution,
including Bloom's optimizer-time work. Result verification runs outside the
timed interval. All queries passed the runner's result checks.

These are measurements from one machine and one sampling seed, not performance
guarantees across hardware, workloads, or cold caches. The earlier DuckDB
development-build results in the main README are a separate historical run,
not a controlled before/after experiment for the version upgrade.

## Reading the data

The `imdb` workload is JOB; `tpch_sf10` is TPC-H SF10. All timing columns are
in seconds. Run columns preserve the raw observations; medians and ratios
are calculated from those observations.

- **Per-query speedup:** `baseline_median_s / bloom_median_s`.
  Values below 1 indicate a slowdown.
- **Total speedup:** `sum(baseline_median_s) / sum(bloom_median_s)`.
  This is not the average of the per-query ratios.
- **Queries at least 2× faster:** count per-query speedups greater than or
  equal to 2, before rounding.

JOB totals are 28.427 s for DuckDB and 20.114 s for Bloom (**1.413×**).
Of 113 queries, 77 are faster, 36 are slower, and 22 achieve at least 2×.
The maximum is 8.688× (query 12b), rounded to **8.69×** in the headline;
the minimum is 0.132× (query 11b).
TPC-H SF10 totals are 17.180 s and 15.336 s (**1.120×**).

## Reproducing the procedure

From the repository root, build the native benchmark runner:

```bash
git submodule update --init --recursive
CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release -j"$(nproc)"
```

Run the same settings against your database files:

```bash
python3 scripts/run_benchmark_suite.py \
  --workload imdb --db /path/to/imdb_compressed.duckdb \
  --threads 1 --timed-runs 5 --sample-seed 2

python3 scripts/run_benchmark_suite.py \
  --workload tpch_sf10 --db /path/to/tpch_sf10.duckdb \
  --threads 1 --timed-runs 5 --sample-seed 2
```

The suite runs Bloom and the baseline, verifies that both produced the same
query set with all five timings, and writes logs under `benchmark_results/`.
Timing values will vary between runs.
