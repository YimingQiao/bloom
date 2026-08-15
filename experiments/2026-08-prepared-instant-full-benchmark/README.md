# Prepared versus instant sampling: full benchmark coverage

This experiment extends the focused oracle/prepared/instant comparison to
every workload currently reported by Bloom: CEB IMDB, uncompressed JOB,
compressed JOB, STATS-CEB, CEB Stack, TPC-H SF10, TPC-DS SF10, and Appian.

The validated result is in [`results/final/REPORT.md`](results/final/REPORT.md).
Per-query medians and result status are in
[`results/final/QUERY_RESULTS.csv`](results/final/QUERY_RESULTS.csv); all 42,306
measured executions are in [`results/final/RUNS.csv`](results/final/RUNS.csv).
Exact source, binary, database, query-set, sample-cache, protocol, and machine
fingerprints are in [`MANIFEST.json`](MANIFEST.json).

Across all listed workload totals, instant/prepared is **1.003x** with warm
base data and **1.007x** from cold SSD. Instant sampling therefore removes
sample maintenance here while remaining within 0.7% of prepared end-to-end
time at the aggregate level. Workload-level ratios range from 0.906x to 1.024x
warm and from 0.976x to 1.016x cold.

Prepared and instant use the same Bloom binary, database, SQL, thread count,
and cache-state gate. Prepared 10K samples are loaded before timing. Instant
sampling is part of the timed query: warm data uses 256 x 32 scattered native
accesses with eight sampling workers, while cold data uses 16 block-aligned
windows with four workers. Query execution itself is single-threaded.

Warm measurements use two repetitions and cold measurements use three. The
two large CEB workloads use one repetition in each state, as previously agreed,
because they contain 3,133 and 6,191 queries. Every query starts in a fresh
process. Cold runs evict the database before every query. Full-file residency
is required to be zero before every normal-workload run and before the first,
every 50th, and final query of each large CEB cell; warm runs require the
complete database file to be resident. Reported workload time is the sum of
per-query medians (or the single observation for the large CEB workloads).

The result-hash gate is causal and same-engine: prepared and instant must
produce the same result bag for every state/query pair. Oracle is intentionally
not synthesized for workloads without a complete exact-cardinality cache. Its
validated three-workload comparison remains in
[`../2026-08-oracle-prepared-instant-end-to-end/`](../2026-08-oracle-prepared-instant-end-to-end/).

All 42,306 executions succeeded, and all 3,384 scheduled cold-residency gates
observed zero resident bytes. Result bags match exactly for 19,533/19,650
state/query pairs. The other 117 are all CEB Stack Q13: its SQL orders only by
`COUNT(*)` before `LIMIT 100`, and the validator confirms that both methods
return 100 rows and differ only among rows tied at the same cutoff count.

Run the complete matrix from the repository root:

```bash
python3 experiments/2026-08-prepared-instant-full-benchmark/scripts/run_matrix.py \
  --output experiments/2026-08-prepared-instant-full-benchmark/results/reproduced/local
```

Use `--resume` after interruption. The underlying cell runner appends only
missing query/repetition records and rejects duplicate, failed, or incompatible
records. Raw output is ignored by Git; the scripts, methodology, aggregate
report, per-query results, and validation records are the curated artifact.
