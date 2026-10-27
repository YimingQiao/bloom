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
  <a href="docs/USAGE.md">Documentation</a>
</p>

Bloom is a DuckDB extension that speeds up queries across multiple tables.

**Up to 8.69× faster on an individual JOB query.**
[Try the SQL demo ↓](#try-it-in-duckdb) · [See the measurements →](#benchmarks)

## How it works

Bloom uses what it learns from one table to rule out irrelevant rows in others.
It adjusts as it goes, so later steps process less data. Your SQL and results
stay the same.

## Build and load

For **DuckDB v2.0.0**. Build from this repository:

```bash
git submodule update --init --recursive
make release -j"$(nproc)"
```

Load the extension in DuckDB started with `-unsigned`:

```sql
LOAD 'build/release/extension/bloom/bloom.duckdb_extension';
```

## Try it in DuckDB

From the repository root, open your compressed IMDB database in a fresh
session of standard DuckDB v2.0.0:

```bash
duckdb -unsigned /path/to/imdb.duckdb
```

Use `.timer on` in the CLI, or the execution time shown by your native DuckDB
UI. Define JOB query **12b** below, then run each statement separately.

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

Warm up once on each side, then compare:

```sql
-- DuckDB
SELECT * FROM job_12b; -- warmup
SELECT * FROM job_12b; -- timed

LOAD 'build/release/extension/bloom/bloom.duckdb_extension';

-- DuckDB + Bloom
SELECT * FROM job_12b; -- warmup, including sample preparation
SELECT * FROM job_12b; -- timed
```

Example: **171 ms → 36 ms (4.75×)**, with identical results. These are medians
of five timed runs after warmup; repeat each timed `SELECT` five times for the
same comparison. Timings vary by machine and query.

## Benchmarks

DuckDB v2.0.0, one thread. Totals are sums of per-query medians from five timed
runs after warmup; JOB uses compressed IMDB.

| Workload | Queries | DuckDB | Bloom | Total speedup |
|---|---:|---:|---:|---:|
| JOB | 113 | 28.427 s | 20.114 s | **1.413×** |
| TPC-H SF10 | 22 | 17.180 s | 15.336 s | **1.120×** |

The **8.69×** headline is JOB query 12b: 188.038 ms → 21.643 ms.
Results vary across queries, including slowdowns.

[Methodology and reproduction](docs/benchmarks/duckdb-2.0/README.md) ·
[Per-query timings](docs/benchmarks/duckdb-2.0/timings.csv) ·
[Historical results](docs/benchmarks/README.md)

## Documentation

- [Configuration and logging](docs/USAGE.md)
- [Release checklist](RELEASING.md)

## Related projects

[BloomPG](https://github.com/YimingQiao/bloompg) for PostgreSQL ·
[Bloom for Apache DataFusion](https://github.com/YimingQiao/datafusion-bloom)

## Reference

Yiming Qiao, Peter Boncz, and Huanchen Zhang,
[*Robust Predicate Transfer with Dynamic Execution*](https://duckdb.org/library/robust-predicate-transfer-vldb/),
PVLDB 2026.
