# Full prepared/instant benchmark result

Complete-query time includes optimization, excitation, materialization, instant sampling, and final execution. Prepared sample loading happens before the timer. Normal workloads use two warm and three cold repetitions; CEB IMDB and CEB Stack use one repetition per state. Each cell is the sum of per-query medians.
Every execution starts in a fresh DuckDB process with no query warmup. Warm means complete OS page-cache residency, not a warm DuckDB buffer manager, so these absolute totals are not regression-comparable to the main README benchmark table.

## Warm base data

| Workload | Prepared | Instant | Instant / prepared |
|---|---:|---:|---:|
| CEB IMDB | 1071.752 s | 1083.087 s | 1.011x |
| JOB (uncompressed) | 36.849 s | 33.367 s | 0.906x |
| JOB (compressed) | 33.214 s | 32.465 s | 0.977x |
| STATS-CEB | 259.322 s | 258.909 s | 0.998x |
| CEB Stack | 3801.884 s | 3815.170 s | 1.003x |
| TPC-H SF10 | 23.613 s | 21.995 s | 0.932x |
| TPC-DS SF10 | 84.493 s | 82.566 s | 0.977x |
| Appian | 39.349 s | 40.279 s | 1.024x |

Aggregate instant/prepared over the listed workload totals: **1.003x**.

## Cold SSD base data

| Workload | Prepared | Instant | Instant / prepared |
|---|---:|---:|---:|
| CEB IMDB | 1687.834 s | 1646.929 s | 0.976x |
| JOB (uncompressed) | 95.293 s | 94.667 s | 0.993x |
| JOB (compressed) | 50.723 s | 51.484 s | 1.015x |
| STATS-CEB | 257.528 s | 257.326 s | 0.999x |
| CEB Stack | 6529.778 s | 6634.349 s | 1.016x |
| TPC-H SF10 | 40.585 s | 40.345 s | 0.994x |
| TPC-DS SF10 | 120.805 s | 119.426 s | 0.989x |
| Appian | 42.433 s | 42.006 s | 0.990x |

Aggregate instant/prepared over the listed workload totals: **1.007x**.

## Cold physical input

| Workload | Prepared | Instant | Instant / prepared |
|---|---:|---:|---:|
| CEB IMDB | 595.167 GiB | 652.281 GiB | 1.096x |
| JOB (uncompressed) | 32.796 GiB | 35.259 GiB | 1.075x |
| JOB (compressed) | 16.311 GiB | 18.143 GiB | 1.112x |
| STATS-CEB | 0.603 GiB | 0.619 GiB | 1.027x |
| CEB Stack | 1518.866 GiB | 1660.914 GiB | 1.094x |
| TPC-H SF10 | 13.027 GiB | 12.915 GiB | 0.991x |
| TPC-DS SF10 | 25.868 GiB | 25.864 GiB | 1.000x |
| Appian | 1.587 GiB | 1.569 GiB | 0.988x |

## Validation

- 42306/42306 measured executions succeeded.
- All 21654 cold executions issued base-file eviction; 3384/3384 scheduled full-file residency gates observed zero bytes.
- Prepared and instant result-bag hashes match for 19533/19650 state/query pairs.
- 117 additional CEB Stack Q13 pairs differ only at a non-total top-100 boundary: both outputs contain 100 rows, and every substituted row has the same count at the shared cutoff.
