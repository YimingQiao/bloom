# Configuration and logging

[Back to Bloom](../README.md)

## Configuration

Bloom works without tuning. Instant sampling is the default: it acquires a
query-local sample from native DuckDB storage or one or more Parquet files.
Set `bloom_sample_mode` to `prepared` to persist and reuse a sample across queries.

The main settings are:

```sql
SET enable_bloom = false;
SET memory_limit = '512MB';
SET bloom_sample_cache_dir = '/path/to/cache';
SET bloom_sample_mode = 'instant';
SET bloom_excitation_mode = 'join_key_ndv';
```

`enable_bloom` defaults to `true`; disable it for troubleshooting or an A/B
comparison. Bloom uses currently available DuckDB operator memory, governed by
DuckDB's `memory_limit`, without a separate Bloom cap.
Each phase must also obtain its complete share from DuckDB's temporary-memory
manager. Bloom's large sample/materialized collections and filter payloads use
DuckDB's non-spill `BufferAllocator`, so they count against the database-wide
memory limit without becoming temporary-storage blocks; large task-local build
states are included in admission estimates. If admission fails before any source
has completed processing, Bloom releases its query-local state and keeps DuckDB's
original plan. Otherwise, it stops adding transfer rounds and reuses the
completed materializations and filters in subsequent execution. The reservation for
tables and filters retained by the rewritten plan remains active until those
execution objects are destroyed. Allocation failures that occur after admission
are reported to the query; they are not silently converted into a fallback.

The memory budget is admission control, not a guarantee that a query will fit.
Concurrent queries share DuckDB's memory limit, and scans, joins, and variable
length values can need more memory after admission. Under tight limits, reduce
concurrent queries or threads, disable the optional sample memory cache, or use
`SET enable_bloom = false` to run the original plan. Native execution can
also run out of memory. Bloom's tests check cleanup and subsequent execution
after allocation failures, including concurrent queries.

`EXPLAIN`, queries containing volatile expressions, and reads from a database
with uncommitted changes bypass Bloom and remain on DuckDB's native path. This
prevents optimizer-time execution from evaluating side effects or mixing
prepared samples with transaction-local state.

In prepared mode, `bloom_sample_cache_dir` defaults to `auto`, which stores samples
under `<database>.bloom_samples/`. Set another directory to share a cache, or `''`
to disable disk caching. Prepared samples are keyed by `bloom_sample_seed`, whose
cross-workload-validated default is `2`; changing the seed creates an independent
cache entry and never reuses the previous sample. The optional
`bloom_sample_memory_cache` also keeps DuckDB's conservative
`ObjectCache` eviction reservation in addition to the sample's allocator charge;
set it to `false` on especially tight memory limits while retaining disk caching.

This build uses sample cache format v6, which distinguishes generated-column
layouts. Samples from earlier formats are rebuilt on first use.

`bloom_sample_mode` accepts `instant` (the default) and `prepared`.

Prepared sampling reads table data to build its reservoirs on first use. That
cost can exceed the query's native execution time; the benchmark's warmup
excludes it. Reuse the sample cache for repeated analytical queries. For
one-off queries, `SET bloom_sample_mode = 'instant'` avoids building persistent
reservoirs, but uses a different sampling method and may choose a different
transfer plan. Measure end-to-end latency for the workload before choosing a
mode. `SET enable_bloom = false` provides the native comparison.

Bloom tracks result lineage and received constraints separately for each complete
join key. Received filters update the constraints immediately. The cardinality
estimate already used by the scheduler decides whether to update result lineage;
this adds no sampling pass. Cross-key reductions append a new projection version
while retaining earlier tokens. This is an estimate-driven transfer policy, not
a guarantee of full reduction when estimates are inaccurate.

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
disabled for performance benchmarks. Clear or rotate stored logs as needed.

`SET bloom_log_transfer_steps = true` remains available as a separate option for
verbose, human-readable diagnostics on stderr.

For in-memory `ColumnDataCollection` (CDC) data, the estimator uniformly selects
up to `bloom_in_memory_sample_size` rows without replacement (default: 4,096).
Collections at or below this limit are evaluated in full. Set it independently
of base-table sampling, for example `SET bloom_in_memory_sample_size = 4096`.
The unfiltered sample is cached for the current materialized collection and
reused with the current predicates and transfer filters. Replacing or extending the collection invalidates its sample; adding a required column also
rebuilds it. Sampling never modifies the source data. When a partial sample has
no survivors, the estimator returns a small positive row count (the weight of one sample row, rounded down), rather than scanning the
full collection or declaring it empty. A census can still return an exact zero.

`bloom_sample_seed` controls reproducibility. CDC sampling has no block/row mode
switch; the former `bloom_sample_rate` setting is removed. Base-table DuckDB and
Parquet sampling settings are unchanged.

CDC sampling uses the same logging interface. Filter on `event='cdc_sample'`:

```sql
CALL enable_logging('Bloom', storage = 'memory');
-- Run a query.
SELECT query_id, event_sequence, destination_table_id, phase,
       info['sample_rows']::UBIGINT AS sample_rows,
       info['sample_reused'] AS sample_reused,
       info['estimate']::UBIGINT AS estimate
FROM duckdb_logs_parsed('Bloom')
WHERE event = 'cdc_sample'
ORDER BY connection_id, query_id, event_sequence;
```

`info` reports the sampling design, sampled and surviving rows, source chunks
visited, cache reuse, and acquisition/filtering time. `sampling_se` is an
estimated standard error of the survival fraction, not a confidence guarantee;
it is zero for a census. `estimate_status=sampled_zero_floor` identifies a
zero-survivor partial sample whose estimate was raised to a positive value.

For exact diagnostic verification of the **same CDC and filters**, use DEBUG:

```sql
CALL enable_logging('Bloom', storage = 'memory');
SET logging_level = 'debug';
```

This additionally filters the entire CDC for each partial sample, recording
`exact_rows`, `signed_error_rows`, `absolute_error_fraction` (absolute row error
divided by current CDC size), `audit_extra_rows`, and `audit_ms`. The audit does
not change the returned estimate. Exact here means exact application of these
filters, including any Bloom false positives, rather than final join cardinality.
INFO logging adds no data scans. Disable logging for timing measurements.
Prepared/instant base-table sampling is outside this CDC diagnostic.
