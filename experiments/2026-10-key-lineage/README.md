# Complete-key L/C lineage engine experiment

Branch: `experiment/key-lineage-lc-20261021`, based on `9a88899`.
DuckDB: `21aca0424f1faf78b593b1e6fbfdd4846624c987`.

The integration into the main branch targets DuckDB 2.0.0 and uses separate L/C
lineage by default. Set `bloom_seperate_lineage=false` (or
`BLOOM_SEPERATE_LINEAGE=0`) to compare with legacy lineage in the same binary.
The branch and DuckDB revisions above identify the original experiment;
RESULTS.md preserves its historical measurements and settings.

## What changes

- Track **L** (result expression) and **C** (satisfied constraints) separately
  for each complete join key. Never split a composite key into independent
  column filters.
- A received filter remains pending. Only materialization/compaction commits
  its constraints to C. Sampling determines queue order, not satisfaction.
- Same-key and contained-key restrictions preserve/extend L. A batch that
  changes rows through a noncontained key receives a fresh projection token.
  Historical C is retained. A no-shrink batch changes C but leaves L unchanged.
- Filter cache snapshots are taken **after** the source's pending filters are
  applied. Tokens and snapshots are immutable.
- A Bloom filter's acceptance domain is not its source's exact projection.
  Approximate filters receive independent predicate tokens; exact bitmap /
  prefix-range filters can carry the source L. This sacrifices pruning rather
  than claiming that false positives satisfy an exact-domain constraint.
- Scheduling remains the legacy policy: the same initial activation,
  sampling-based reactivation, estimated cardinalities/baselines, small-table
  skip, estimated-zero shortcut, and optional exact-NDV suppression. Pending
  receipt only invalidates the source lineage snapshot; it does not activate
  a table. Actual row decrease updates L/C, not the scheduler.
- Preserve filter subsumption for exact filters. In key mode, source L cannot
  prove containment between approximate Bloom acceptance domains, so that
  lineage-based shortcut is conservatively skipped for approximate filters.
- CDC aliases retain separate mutable states because their materialized copies
  can be compacted independently.

## Physical implementation limits

Filtering is batched to retain scan fusion. The first GET scan already applies
single-column predicates (despite its name, `MakeOptional` with threshold 1.0
builds a mandatory expression). We do not scan again merely to prove this.
For that first scan, individual before/after filter counts are unavailable:
contained predicates are conservatively included in L, and cross-key predicates
cause fresh projection tokens. Later compactions observe total row decrease;
a batch with any cross-key predicate is conservatively treated as cross-key.
This can retain more tokens / perform more transfers than the ideal per-filter
feedback simulator. There is no extra distinct-key computation.

The historical results in RESULTS.md compare **complete policies** (through
`6a4ef19`), including all-table initial activation. They do not describe the
current lineage-only revision, which restores the legacy scheduling policy.
The current implementation checks query-result equivalence; it does not
establish exact full reduction for Bloom
filters, nullable keys, cyclic joins, protected optimizer regions, or queries
where memory admission stops transfer. Selective initial activation and
sampling/size-based skipping can also omit necessary transfers even with exact
filters; safe lineage pruning alone does not guarantee full reduction under
those scheduling heuristics. Materialized tuple counts are observed
physical allocations (initial results plus compacted replacements), not the
paper's abstract optimal `2*O` cost and not byte traffic.

## Build and checks

```sh
cmake -S duckdb -B build/release -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DDUCKDB_EXTENSION_CONFIGS="$PWD/extension_config.cmake" \
  -DEXTENSION_STATIC_BUILD=ON -DBUILD_BENCHMARKS=ON \
  -DBUILD_UNITTESTS=ON -DENABLE_UNITTEST_CPP_TESTS=OFF
cmake --build build/release --target duckdb benchmark_runner unittest -j24
BLOOM_SEPERATE_LINEAGE=0 build/release/test/unittest --test-dir "$PWD" 'test/sql/*'
BLOOM_SEPERATE_LINEAGE=1 build/release/test/unittest --test-dir "$PWD" 'test/sql/*'
c++ -std=c++17 -O0 -g -I src/include -I duckdb/src/include \
  -I duckdb/third_party/fmt/include test/cpp/key_lineage_test.cpp \
  src/predicate_transfer/transfer_plan/lineage_tracker.cpp \
  -L build/release/src -Wl,-rpath,"$PWD/build/release/src" -lduckdb \
  -o build/key_lineage_test
build/key_lineage_test
```

The experiment theme (harness, data pointers, raw logs, manifests and reports):
`/home/yiming/projects/sspt-experiments/key-lineage-bloom/`.
Databases stay in their existing locations and are opened read-only.
The harness attempts 70 synthetic/random bag-equivalence fixtures and JOB (113),
STATS-CEB (146), TPC-H SF1 (22). The completed paired comparison covers
113 / 142 / 22 queries; STATS-CEB 58, 120, 122, 126 hit the 20-second reference
timeout. See RESULTS.md and the small results/ snapshot. Benchmark queries run unchanged. Diagnostics
are collected separately from timings. Native runner: one warmup, three timed
runs, one thread; compare the sum of per-query medians and paired-query ratios.

```sh
python3 /home/yiming/projects/sspt-experiments/key-lineage-bloom/code/run.py \
  --root "$PWD" \
  --out /home/yiming/projects/sspt-experiments/key-lineage-bloom/results/NEW-RUN \
  --fixtures --runs 3
```

## Lineage-only revision validation (2026-10-21)

Restored the legacy initial activation, reactivation thresholds, estimated
cardinality/baseline updates, small-table / estimated-zero skips, and exact-NDV
suppression. L/C commits no longer decide whether a table broadcasts. Estimated
zero and NDV shortcuts leave C untouched rather than claiming physical filter
application. Exact-filter subsumption is restored using L/C snapshots.

Both engine modes pass 30 SQL tests (810 assertions each). The C++ lineage
invariant test passes; 70 fixtures agree across disabled/legacy/L/C modes.
JOB 07a, STATS-CEB 138 and TPC-H q18 also agree with disabled-Bloom results.
All 73 paired diagnostic logs have identical initial-state blocks (72 have
transfer initialization; one has none). This checks policy preservation and
query correctness, not full-reduction coverage.

Q18 now materializes 149,999 rows and builds one filter in both modes, removing
the earlier all-table-activation effect. No updated workload-wide performance
or full-reduction claim is made. Raw validation is in the experiment theme's
`results/lineage-only-validation/`; historical totals above are unchanged.
