# Complete-key lineage

Bloom on DuckDB 2.0 uses separated lineage for every complete join key. The
legacy lineage implementation and its configuration switch have been removed.

## Update policy

- **L** describes the current result; **C** records received constraints.
- A received batch updates C immediately, before physical materialization.
- The scheduler already estimates the destination after receiving filters.
  If that estimate decreases, update L too. This adds no sampling pass.
- A filter on a contained key contributes its incoming tokens. An estimated
  reduction through another key appends a fresh projection token to both L and
  C, retaining earlier tokens. No estimated reduction leaves L unchanged.
- Outgoing filters carry a snapshot of L. Transfers are skipped when the
  receiver's C contains that information. Physical filtering still uses the
  existing materialization and compaction path.
- Exact filters carry source lineage. Approximate Bloom acceptance predicates
  have separate tokens. Composite keys remain complete keys, and aliases keep
  independent mutable states.

Sampling decides whether to advance result lineage; inaccurate estimates can
therefore miss useful transfers or cause extra work. This policy does not claim
exact full reduction. Existing scheduling thresholds, memory admission, and
single-column estimation support are unchanged. Composite filters participate
in estimation through the materialized scanner path; no separate composite
sampling or distinct-projection scan is added.

## Validation

```sh
build/release/test/unittest --test-dir "$PWD" 'test/sql/*'
c++ -std=c++17 -O0 -I src/include -I duckdb/src/include \
  -I duckdb/third_party/fmt/include test/cpp/key_lineage_test.cpp \
  src/predicate_transfer/transfer_plan/lineage_tracker.cpp \
  -L build/release/src -Wl,-rpath,"$PWD/build/release/src" -lduckdb \
  -o build/key_lineage_test
build/key_lineage_test
python3 experiments/2026-10-key-lineage/run.py \
  --build-dir build/release --out build/lineage-check --only-fixtures
```

The harness checks 70 synthetic/random fixtures against native DuckDB. Omit
`--only-fixtures` and add `--fixtures --runs 3` to include JOB (113 queries),
STATS-CEB (146), and TPC-H SF1 (22). Database paths are defined in `workloads()`;
existing databases are opened read-only. Each timing uses one warmup and three
measured runs on one thread, with diagnostics collected separately. Timed-out
queries are recorded in `failures.json` and excluded from paired comparisons.

## DuckDB 2.0 integration measurements

The receipt-based policy was compared with legacy lineage at `c42f605`
(`bloom_seperate_lineage=false`), using the same DuckDB 2.0 build and one thread.
The old binary was preserved before removing the implementation.

| Sampling | Workload | Queries | Legacy total | New total | Change |
| --- | --- | ---: | ---: | ---: | ---: |
| Prepared | JOB | 113 | 20.416 s | 20.447 s | +0.16% |
| Prepared | TPC-H SF1 | 22 | 1.454 s | 1.458 s | +0.26% |
| Instant | JOB | 113 | 22.478 s | 22.628 s | +0.67% |
| Instant | STATS-CEB | 142 | 64.361 s | 64.954 s | +0.92% |
| Instant | TPC-H SF1 | 22 | 1.535 s | 1.536 s | +0.07% |

Totals sum per-query medians. Prepared measurements use two alternating rounds
with three timed runs per round; instant measurements use the harness protocol
above. Prepared JOB 23a initially showed a 33% slowdown, which did not reproduce
in an alternating 18-sample recheck (180.2 ms legacy, 178.1 ms new).
All 277 completed instant pairs matched the native results. STATS-CEB 58, 107,
120, and 122 exceeded the 20-second native-reference timeout and are omitted
from both sides. See [the integration report](results/duckdb-2.0-sampling.json)
for aggregate timings, filter counts, and validation details. These measurements
show similar aggregate performance, not a guarantee for every query.

## Historical results

[RESULTS.md](RESULTS.md) and `results/` preserve the original experiment on
branch `experiment/key-lineage-lc-20261021`, based on `9a88899` and DuckDB
`21aca0424f1faf78b593b1e6fbfdd4846624c987`. Those measurements include an earlier
scheduling policy and do not measure the current estimate-driven implementation.
The original raw logs and manifests live under
`/home/yiming/projects/sspt-experiments/key-lineage-bloom/`.
