# End-to-end result

Complete-query time includes optimization, adaptive excitation, materialization, and final execution. Each cell is the sum of per-query medians. Warm has two repetitions; cold SSD has three.
Every execution starts in a fresh DuckDB process with no query warmup. Warm means complete OS page-cache residency, not a warm DuckDB buffer manager, so these absolute totals are not regression-comparable to the main README benchmark table.

## Warm base data

| Workload | Oracle | Prepared | Instant | Instant / prepared |
|---|---:|---:|---:|---:|
| JOB | 37.950 s | 33.214 s | 32.465 s | 0.977x |
| TPC-H SF10 | 25.802 s | 23.613 s | 21.995 s | 0.932x |
| Appian | 40.038 s | 39.349 s | 40.279 s | 1.024x |

Combined instant/prepared: **0.985x**.

## Cold SSD base data

| Workload | Oracle | Prepared | Instant | Instant / prepared |
|---|---:|---:|---:|---:|
| JOB | 52.912 s | 50.723 s | 51.484 s | 1.015x |
| TPC-H SF10 | 42.442 s | 40.585 s | 40.345 s | 0.994x |
| Appian | 42.201 s | 42.433 s | 42.006 s | 0.990x |

Combined instant/prepared: **1.001x**.

## Cold physical input

| Workload | Prepared | Instant | Instant / prepared |
|---|---:|---:|---:|
| JOB | 16.311 GiB | 18.143 GiB | 1.112x |
| TPC-H SF10 | 13.027 GiB | 12.915 GiB | 0.991x |
| Appian | 1.587 GiB | 1.569 GiB | 0.988x |

## Prepared preload audit

The prepared sample cache was loaded after evicting the base database. These bytes are catalog pages; the preload did not scan table payloads.

| Workload | Base data made resident | Physical input |
|---|---:|---:|
| JOB | 1.844 MiB | 1.844 MiB |
| TPC-H SF10 | 1.594 MiB | 1.594 MiB |
| Appian | 1.594 MiB | 1.594 MiB |

## JOB cold-query buckets

| Prepared query time | Queries | Prepared sum | Instant sum | Time ratio | Input ratio |
|---|---:|---:|---:|---:|---:|
| <100 ms | 17 | 1.000 s | 1.495 s | 1.495x | 2.200x |
| 100-500 ms | 54 | 13.989 s | 14.331 s | 1.024x | 1.204x |
| >=500 ms | 42 | 35.734 s | 35.658 s | 0.998x | 1.049x |

## Validation

- 2145/2145 measured executions succeeded.
- 1287/1287 cold executions started with zero resident base-data bytes.
- 715/715 oracle executions left the primed cardinality cache unchanged.
- Prepared and instant result-bag hashes match for 286/286 state/query pairs.
- Oracle hashes match for 282/282 comparable state/query pairs. Appian Q5/Q8 are excluded because non-total ORDER BY keys make the LIMIT 500 boundary tie-ambiguous across DuckDB versions.
