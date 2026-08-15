# Oracle, prepared sampling, and instant sampling

The complete result is in [`results/final/REPORT.md`](results/final/REPORT.md).
Per-query medians, physical input, ratios, and result-hash checks are in
[`results/final/QUERY_RESULTS.csv`](results/final/QUERY_RESULTS.csv).
All 2,145 individual measured executions and their correctness gates are in
[`results/final/RUNS.csv`](results/final/RUNS.csv).
Exact source, machine, database, query-set, and sampling configuration are in
[`MANIFEST.json`](MANIFEST.json).
The query-by-query explanation of every observed Instant win is in
[`results/final/INSTANT_WINS.md`](results/final/INSTANT_WINS.md).
Across JOB, TPC-H SF10, and Appian, instant sampling is 0.985x prepared-sample
time with warm base data and 1.001x with every query starting from cold SSD.
The cold result is the key finding: removing maintained samples changes the
combined end-to-end time by less than one tenth of one percent in this run.

## Goal

Measure how much practicality is lost while Bloom evolves from exact oracle
cardinalities, to maintained 10K reservoirs, to query-time sampling. The main
metric is complete end-to-end query latency. Sampling latency and transfer-plan
quality remain explanatory metrics, not substitutes for this comparison.

## Methods

- **Oracle:** the exact-cardinality path in
  `~/projects/native-predicate-transfer`, with its cardinality cache completely
  primed before measurement. The cache file must not change during a timed run.
- **Prepared:** persistent 10K reservoirs are loaded into DuckDB's in-process
  ObjectCache before the timer starts. `rpt_preload_samples()` directly
  deserializes existing sample files; it never optimizes a user query, builds a
  missing sample, or scans base-table payloads.
- **Instant:** no maintained sample. Warm data uses 256 x 32 stratified native
  accesses with an internal task limit of eight; cold data uses 16 block-aligned
  windows with an internal task limit of four. Seed 2 is fixed for
  reproducibility. `instant/scattered` and `instant/block` remain public access
  policies; task fan-out is not user-configurable.

## Data states

- **Warm:** the complete base database is read into the OS page cache before
  the measured series. Every fresh query process has a `fincore` residency
  gate, and missing pages are re-warmed before timing.
- **Cold SSD:** the base database is evicted with `POSIX_FADV_DONTNEED` before
  every fresh query process and `fincore` must report zero resident bytes. The
  oracle information and prepared samples are allowed to be resident; only the
  original database tables are required to start on SSD.

Every record retains query time, process input bytes, database residency,
result hash, and (for prepared runs) an optional preload-only residency audit.
The preload audit is run once per workload because the preload operation is
query-independent.
The compared workloads are JOB, TPC-H SF10, and Appian. DuckDB query execution
uses one thread; instant sampling uses the bounded tasks described above.

Warm runs use two repetitions; cold runs use three. Each reported workload
time is the sum of per-query medians. Every query runs in a fresh process.
Cold runs issue `POSIX_FADV_DONTNEED` and require `fincore` to report zero
resident base-data bytes immediately before process launch. Warm runs require
the complete file to be resident before every process, repairing residency if
necessary without repeatedly evicting the file.

Thus `warm` means OS page-cache residency, not a warmed DuckDB buffer manager:
there is no query warmup in the new process. These absolute prepared totals are
not regression-comparable to the same-process benchmark-runner totals in the
main README; only same-protocol method ratios are compared here.

## Validation

- All 2,145 measured executions succeeded.
- All 1,287 cold executions passed the zero-residency gate.
- All 715 measured oracle executions left the primed oracle cache unchanged.
- Prepared and instant result-bag hashes match for all 286 state/query pairs.
- Oracle matches all 282 comparable state/query pairs. Appian Q5 and Q8 use a
  non-total `ORDER BY` followed by `LIMIT 500`; different DuckDB versions may
  legally select different rows from the boundary tie, so those two are
  excluded from cross-version hash comparison.

The prepared preload was audited after base-file eviction. It made only
1.59--1.84 MiB of each base database resident (catalog pages) and did not scan
table payloads. A diagnostic TPCH Q2 run then reported
`source=prepared_memory_cache`
for every sampled table.

## Accuracy and sampling cost

Independent quality and latency sweeps were used to select the instant
sampling configuration before this end-to-end run. Against prepared 10K
reservoirs, warm instant sampling preserves the unique
directed transfer-edge set for 113/113 JOB and 22/22 TPC-H queries. Exact
execution sequences match 51/113 and 19/22: sampling changes some multiplicity
and order decisions without changing which relationships are transferable.
The median estimate error relative to prepared is 1.20x for JOB and 1.01x for
TPC-H; 93.2% and 100% of comparable estimates are within 2x.

Isolated warm instant-sampling cost is 8--13 ms per sampled query. Cold direct
sampling costs 27--59 ms per sampled query in the focused latency experiment.
Prepared ObjectCache lookup is microseconds, but building and serializing the
maintained reservoirs costs 1.12--5.62 s per database in these workloads and
must be paid again after invalidation.

## Interpretation

Prepared samples remain the fastest and most accurate way to answer any one
sampling request after their maintenance cost has been paid. Instant sampling
nevertheless reaches the same workload-level performance here because its
small planning cost is often absorbed by the full query, sampled SSD pages are
reused by execution, and modest estimate differences sometimes avoid later
materialization work.

The limitation is visible in cold JOB. Instant sampling reads 1.112x as many
bytes and is 1.015x prepared time overall. Its 17 prepared-under-100-ms queries
are 1.495x slower, while the 42 queries at or above 500 ms are 0.998x. A fixed
16-window cold budget is therefore too large for some very short queries. The
next improvement should make the disk budget conditional on expected query
work or stop sampling once the decision is already stable; it should not
increase the global sample size.

## Version boundary

The same-engine prepared/instant comparison uses Bloom's DuckDB commit
`21aca0424f` (`v1.6.0-dev11127`). Oracle uses the exact-cardinality path in
`~/projects/native-predicate-transfer` at repository commit `8c0c9dd`, whose
DuckDB submodule is `52286012996` (`v1.5.0-dev8583`). Consequently oracle is an
algorithmic reference, not a controlled same-binary performance comparison.
Absolute prepared-versus-instant differences are the causal comparison.
Prepared and instant were rerun after the production two-mode refactor. The
unchanged oracle records are reused from the preceding validated run under the
same database, thread count, cache-state gates, and repetition policy.

The curated run predates the seed-aware `rpt_sample_v4` cache identity. Its
prepared measurements used the preserved `v3` sample files, whose key did not
include `rpt_sample_seed`; the historical manifest therefore cannot identify a
prepared sample by seed. A current `v4` run may select different rows and a
different transfer plan even when the database and nominal seed are unchanged.
Code-regression comparisons against this report must reuse the preserved `v3`
artifacts or establish a new fingerprinted `v4` baseline rather than compare
uncontrolled sample realizations.

The executable experiment is [`scripts/run_comparison.py`](scripts/run_comparison.py).
[`scripts/summarize_comparison.py`](scripts/summarize_comparison.py) validates
the complete matrix and regenerates `REPORT.md` plus machine-readable
`REPORT.json`, `QUERY_RESULTS.csv`, and `RUNS.csv`. Local raw logs and JSONL are written
below `results/reproduced/` and intentionally ignored by Git; the curated
results in `results/final/` are the reviewable artifact.

Run the complete matrix from the repository root with:

```bash
python3 experiments/2026-08-oracle-prepared-instant-end-to-end/scripts/run_matrix.py \
  --output experiments/2026-08-oracle-prepared-instant-end-to-end/results/reproduced/local
```

Each cell now writes `run_manifest.json` with the executable, source tree,
database, query set, sample cache, and protocol fingerprints. The runner also
refuses a Bloom executable older than its source tree. It refuses failed
queries, cold runs with resident base pages, oracle cache mutations, incomplete
matrices, unstable hashes, and result mismatches.
It first runs a discarded prepared pass to populate every maintained sample and
primes each oracle work directory before timing. Use `--resume` only to continue
an interrupted run in the same output tree.
