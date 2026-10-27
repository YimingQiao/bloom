<h1 align="center">Bloom</h1>

<p align="center"><strong>Faster multi-join queries in DuckDB.</strong><br>
Adaptive predicate transfer, built into your query plan.</p>

<p align="center">
  <a href="extension_config.cmake"><img src="https://img.shields.io/badge/Bloom-0.0.3-1e5688" alt="Bloom 0.0.3"></a>
  <a href="#build-and-load"><img src="https://img.shields.io/badge/DuckDB-2.0.0-1e5688" alt="DuckDB 2.0.0"></a>
  <a href="https://github.com/YimingQiao/bloom/actions/workflows/MainDistributionPipeline.yml"><img src="https://github.com/YimingQiao/bloom/actions/workflows/MainDistributionPipeline.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-64748b" alt="License: MIT"></a>
</p>

<p align="center">
  <a href="#build-and-load">Get started</a> ·
  <a href="#try-it-in-duckdb">SQL demo</a> ·
  <a href="#benchmarks">Benchmarks</a> ·
  <a href="#how-it-works">How it works</a> ·
  <a href="#configuration">Configuration</a>
</p>

Bloom speeds up complex join queries in DuckDB by moving selective filters
across the join graph. Built on **robust predicate transfer (RPT)**, it uses
Bloom and bitmap filters to prune rows before they reach the joins.

**Up to 8.69× faster on an individual JOB query.** Measured with Bloom v0.0.3
and DuckDB v2.0.0, one thread, on compressed IMDB.
[Try the SQL demo ↓](#try-it-in-duckdb) ·
[Full benchmark results and methodology →](#benchmarks)

## How it works

When a query combines several tables, Bloom uses what it learns from one table
to rule out irrelevant rows in the others. It adjusts as it goes, so later
steps have less data to process. Your SQL and query results stay the same.

## Build and load

Bloom v0.0.3 is built for **DuckDB v2.0.0**.

```bash
git submodule update --init --recursive
make release -j"$(nproc)"         # or: GEN=ninja make release -j"$(nproc)"
```

The extension is written to
`build/release/extension/bloom/bloom.duckdb_extension`:

```sql
LOAD 'build/release/extension/bloom/bloom.duckdb_extension';
SELECT current_setting('enable_bloom');
```

## Try it in DuckDB

Run the same JOB query before and after loading Bloom. Start a fresh session
with a standard DuckDB **v2.0.0** binary and your compressed IMDB database;
run from this repository's root so the extension path below resolves:

```bash
duckdb -unsigned /path/to/imdb.duckdb
```

In the CLI, use `.timer on` to display query times. In a UI backed by native
DuckDB, use its execution-time display. First define JOB query **12b** as a
temporary view:

<details>
<summary><strong>SQL setup · JOB query 12b</strong></summary>

```sql
SET threads = 1;

CREATE TEMP VIEW job_12b AS
SELECT MIN(mi.info) AS budget,
       MIN(t.title) AS unsuccsessful_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t
WHERE cn.country_code = '[us]'
  AND ct.kind IS NOT NULL
  AND (ct.kind = 'production companies' OR ct.kind = 'distributors')
  AND it1.info = 'budget'
  AND it2.info = 'bottom 10 rank'
  AND t.production_year > 2000
  AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mc.movie_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;
```

The view names the [original JOB query](https://github.com/duckdb/duckdb/blob/d54cbbe3df17e8e5ce68312f483b4155836112aa/benchmark/imdb_plan_cost/queries/12b.sql);
it does not cache its result.

</details>

Run each statement separately. Warm up each configuration once, then compare
the time of the following `SELECT`:

```sql
-- DuckDB
SELECT * FROM job_12b; -- warmup
SELECT * FROM job_12b; -- timed

LOAD 'build/release/extension/bloom/bloom.duckdb_extension';

-- DuckDB + Bloom
SELECT * FROM job_12b; -- warmup, including sample preparation
SELECT * FROM job_12b; -- timed
```

A local CLI run returned the same result with median times of **171 ms → 36 ms
(4.75×)** over five timed executions per configuration. Repeat each timed
`SELECT` five times to make the same comparison. Your timings will vary;
this demo shows one selected query. See the [benchmarks](#benchmarks) for
results across the complete workload.

## Benchmarks

### DuckDB 2.0

| Workload | Database | Queries | DuckDB baseline | Bloom | Total speedup |
|---|---:|---:|---:|---:|---:|
| [JOB](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | compressed IMDB, 2.05 GB | 113 | 28.427 s | 20.114 s | **1.413×** |
| TPC-H SF10 | 2.68 GB | 22 | 17.180 s | 15.336 s | **1.120×** |

The largest JOB gain is query **12b**, from 188.038 ms to 21.643 ms.
Across the complete workload, 77 queries are faster and 36 are slower;
per-query speedups range from
0.132× to 8.688×. The largest slowdown is query 11b, from 13.945 ms to
105.267 ms. The headline maximum describes an individual query, while the
total speedup above includes all 113 queries.

Both workloads use one thread, default prepared sampling (10,000 rows per
table, seed 2), and DuckDB's built-in join filters enabled on both sides.
The database is brought into the OS page cache before each benchmark process;
each query gets one discarded warmup and five timed runs. Reported totals are
**sums of per-query medians**, including Bloom's optimizer-time work but
excluding initial sample preparation during warmup. All queries completed
successfully and passed the benchmark runner's result checks.

[Full per-query timings](docs/benchmarks/duckdb-2.0/timings.csv) ·
[Measurement details and reproduction](docs/benchmarks/duckdb-2.0/README.md)

<details>
<summary><strong>Historical results · seven workloads on the previous DuckDB development build</strong></summary>

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

</details>

### Running the benchmarks

The commands below use the same native benchmark-runner procedure as the table
above: one same-process warmup followed by timed runs. First build the
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

The Performance Regression workflow runs the complete JOB and TPC-H SF1
workloads against the base commit, the candidate, and the candidate with Bloom
disabled. It reports Bloom's total and per-query-geomean speedups over DuckDB,
as well as commit-to-commit query slowdowns of at least 10%. CI fails when a
workload's commit-to-commit query-time geomean increases by at least 10% or
50ms.

## Configuration

Bloom works without tuning. Prepared sampling is the default: a 10K-row
reservoir per table is persisted and reused across queries. Set
`bloom_sample_mode` to `instant` to use a fresh, query-local sample from native
DuckDB storage or one or more Parquet files without using a prepared sample.

The main settings are:

```sql
SET enable_bloom = false;
SET bloom_memory_limit = '512MB';
SET bloom_late_materialize = true;
SET bloom_sample_cache_dir = '/path/to/cache';
SET bloom_sample_mode = 'instant';
SET bloom_excitation_mode = 'join_key_ndv';
```

`enable_bloom` defaults to `true`; disable it for troubleshooting or an A/B
comparison. `bloom_memory_limit` defaults to `auto`, which admits optimizer-time
Bloom work against at most 25% of currently available DuckDB operator memory.
Each phase must also obtain its complete share from DuckDB's temporary-memory
manager. Bloom's large sample/materialized collections and filter payloads use
DuckDB's non-spill `BufferAllocator`, so they count against the database-wide
memory limit without becoming temporary-storage blocks; large task-local build
states are included in admission estimates. If admission fails
before the first materialization, Bloom keeps DuckDB's original plan; after
transfer has begun it stops adding new transfer rounds. The reservation for
tables and filters retained by the rewritten plan remains active until those
execution objects are destroyed. Set the limit to `0B` to force the fallback
path. Allocation failures that occur after admission are reported to the query;
they are not silently converted into a fallback.

`EXPLAIN`, queries containing volatile expressions, and reads from a database
with uncommitted changes bypass Bloom and remain on DuckDB's native path. This
prevents optimizer-time execution from evaluating side effects or mixing
prepared samples with transaction-local state.

`bloom_sample_cache_dir` defaults to `auto`, which stores samples
under `<database>.bloom_samples/`. Set another directory to share a cache, or `''`
to disable disk caching. Prepared samples are keyed by `bloom_sample_seed`, whose
cross-workload-validated default is `2`; changing the seed creates an independent
cache entry and never reuses the previous sample. The optional
`bloom_sample_memory_cache` also keeps DuckDB's conservative
`ObjectCache` eviction reservation in addition to the sample's allocator charge;
set it to `false` on especially tight memory limits while retaining disk caching.

`bloom_sample_mode` accepts `prepared` (the default) and `instant`.

`bloom_excitation_mode` selects how repeated transfers decide whether a join-key
domain contains new information. It accepts `table_size` (the default) and
`join_key_ndv`; the latter uses exact NDV changes for supported single-column
integer equality domains and falls back safely when an exact domain is not
available.

## Observability

Bloom exposes its optimizer decisions through DuckDB's structured logging.
Collection is disabled by default. Enable it, run the workload you want to
inspect, and query the events from SQL:

```sql
CALL enable_logging('Bloom', storage = 'memory');
CALL truncate_duckdb_logs();

-- Run the queries to diagnose here.

SELECT *
FROM duckdb_logs_parsed('Bloom')
ORDER BY connection_id, query_id, event_sequence;

CALL disable_logging();
```

For example, selected columns from a two-table join can look like this:

| event_sequence | event | source_table | destination_table | source_rows | estimated_destination_rows_before | estimated_destination_rows_after | completed_sources | transfer_count | elapsed_ms |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `start` | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
| 2 | `transfer` | `memory.main.demo_right` | `memory.main.demo_left` | 100 | 10000 | 122 | NULL | NULL | NULL |
| 3 | `transfer` | `memory.main.demo_left` | `memory.main.demo_right` | 100 | 122 | 100 | NULL | NULL | NULL |
| 4 | `transfer_complete` | NULL | NULL | NULL | NULL | NULL | 2 | 2 | 2.058 |

All values above come from typed log columns; the elapsed time is illustrative
and depends on the workload and machine.

The log shows why Bloom did not run (`skipped.reason`), which tables exchanged
filters and how their estimated cardinalities changed (`transfer`), where a
memory admission stopped (`memory_stop`), and the final work and timing summary
(`transfer_complete`). `whole_query_fallback` means no Bloom work was retained
and DuckDB's original plan ran; `partial_stop` means completed transfers were
kept. Use `event_sequence` for order within a query—a relation has no inherent
row order. `connection_id` and `query_id` identify the query, while less common
event-specific values are available in the `info` map. `EXPLAIN` emits no Bloom
events.

DuckDB can persist the same events to a file instead:

```sql
CALL enable_logging(
    'Bloom',
    storage = 'file',
    storage_path = 'bloom-monitor.csv'
);
```

Logging has non-zero cost when enabled. Prefer memory storage for temporary
diagnosis, use file storage only when persistence is required, and keep logging
disabled for performance benchmarks. DuckDB's logging storage is separate from
`bloom_memory_limit`, so clear or rotate it as needed.

`SET bloom_log_transfer_steps = true` remains available as a separate option for
verbose, human-readable diagnostics on stderr.

## Late materialization

`bloom_late_materialize` is experimental and defaults to `false`. When enabled,
Bloom materializations keep only transfer columns and row IDs where possible;
the resulting row-ID bitmap filters DuckDB's original table scan, which reads
output-only columns during normal query execution. This is most useful when a
large transfer source has wide or string payload columns used only by the final
projection, grouping, or aggregation. Across ten CEB Stack q15 variants, it
improved geomean runtime by 1.263x over standard Bloom with identical results. The
extra bitmap construction is not universally beneficial, so enable it per
workload after benchmarking.

## Related projects

Bloom, BloomPG, and Bloom for Apache DataFusion are sibling projects exploring
robust predicate transfer across different query engines.

- [Bloom](https://github.com/YimingQiao/bloom) — DuckDB extension
  (this repository).
- [BloomPG](https://github.com/YimingQiao/bloompg) — PostgreSQL extension.
- [Bloom for Apache DataFusion](https://github.com/YimingQiao/datafusion-bloom)
  — Apache DataFusion library.

## References

- Yiming Qiao, Peter Boncz, and Huanchen Zhang,
  [*Robust Predicate Transfer with Dynamic Execution*](https://duckdb.org/library/robust-predicate-transfer-vldb/),
  PVLDB 2026.
- Junyi Zhao, Kai Su, Yifei Yang, Xiangyao Yu, Paraschos Koutris, and Huanchen
  Zhang,
  [*Debunking the Myth of Join Ordering: Toward Robust SQL Analytics*](https://arxiv.org/abs/2502.15181),
  SIGMOD 2025.
- Yifei Yang, Hangdong Zhao, Xiangyao Yu, and Paraschos Koutris,
  [*Predicate Transfer: Efficient Pre-Filtering on Multi-Join Queries*](https://arxiv.org/abs/2307.15255),
  CIDR 2024.
