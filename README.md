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
  <a href="#benchmarks">Benchmarks</a> ·
  <a href="#how-it-works">How it works</a> ·
  <a href="docs/USAGE.md">Documentation</a>
</p>

Bloom is a DuckDB extension for accelerating analytical queries that join many
tables. It is designed for queries where filtering one table can eliminate
substantial work in the others, including workloads such as the Join Order
Benchmark (JOB).

Built on **robust predicate transfer (RPT)**, Bloom filters data early and adapts
its decisions as it learns more about the query. Load the extension and keep
using your existing SQL: Bloom applies these optimizations automatically within
DuckDB's query planner.

**Up to 8.69× faster on an individual JOB query.**
[See the measurements →](#benchmarks)

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
