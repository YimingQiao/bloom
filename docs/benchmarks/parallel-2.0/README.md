# DuckDB 2.0 parallel materialization measurements

Bloom 0.0.4 includes the parallel materialization change measured at
`48831c60849ea6fc98f3d6c136f6465941b3aff4`. The community baseline is Bloom
0.0.3 at `e81c0f02891a4bc9b962a8acf748312b3376c4e5`.

The previous collector serialized order-preserving materialization scans.
The updated collector uses DuckDB batch indexes to collect these scans in
parallel and combines batches in source order. Sources without batch-index
support retain the serial path. Retained data still uses BufferAllocator.

## Complete JOB workload, eight threads

| Configuration | Sum of per-query medians |
|---|---:|
| Native DuckDB | 6.192 s |
| Bloom 0.0.3 | 12.241 s |
| Updated Bloom | 5.524 s |

Updated Bloom is 2.216× faster than the old extension by total time, and
1.121× faster than native DuckDB. Of 113 queries, 53 are faster than native,
51 are slower, and 9 tie at the CLI timer's millisecond precision. All
three configurations returned identical CSV results for every query and run.

[Per-query medians](full_job_8_threads.csv) ·
[Raw timed observations](job_raw_timings.csv)

## Thread-count comparison

The following smaller selection contains JOB 01a, 06a, 11b, 12b, 17a, 26c
and TPC-H SF10 Q3, Q5, Q9. It includes known wins and regressions and is not
a substitute for either complete benchmark.

| Threads | Native DuckDB | Bloom 0.0.3 | Updated Bloom |
|---|---:|---:|---:|
| 1 | 4.765 s | 3.049 s | 3.024 s |
| 4 | 1.355 s | 2.175 s | 1.019 s |
| 8 | 0.799 s | 2.057 s | 0.619 s |

[Selected-query medians](selected_queries.csv)

Regressions against native DuckDB remain. Additional twenty-observation
checks at eight threads measured JOB 24b at 38.5 ms native versus 67.5 ms
updated Bloom, and 29a at 40 ms versus 68 ms. These are approximately 75%
and 70% slower, despite improving substantially over the old extension.
The initial small regressions against old Bloom on 25b and 27c narrowed to
36→37 ms and 18→18 ms in these checks.

[Additional checks](rechecked_queries_8_threads.csv); columns are milliseconds.

## Method

- Official Linux x86-64 DuckDB 2.0.0 CLI, commit `d54cbbe3df`.
- Intel Xeon Platinum 8474C host; no CPU affinity set for the reported runs.
- Compressed IMDB and TPC-H SF10 databases, opened read-only.
- Default prepared sampling and `preserve_insertion_order=true`; disk sample
  cache reused. Logging disabled during measurement.
- Full JOB: one discarded warmup and three timed queries per process,
  rotating native/old/new order between queries. Reported medians use all
  three timed observations.
- Selection: two rounds with reversed configuration order, each with one
  warmup and five timed queries. Medians use ten timed observations.
- Additional checks: two rounds, one warmup and ten timed queries each.
- CLI wall-clock query timer, including planning and execution but excluding
  process startup. Its output has one-millisecond precision.

These are warm query measurements: sample creation and loading during warmup
are excluded. They do not establish cold-start performance or guarantee the
same speedups on other machines. The single-thread historical benchmark in
the main README remains a separate measurement.
