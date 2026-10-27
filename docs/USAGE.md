# Configuration and logging

[Back to Bloom](../README.md)

## Configuration

Bloom works without tuning. Prepared sampling is the default: a 10K-row
reservoir per table is persisted and reused across queries. Set
`bloom_sample_mode` to `instant` to use a fresh, query-local sample from native
DuckDB storage or one or more Parquet files without using a prepared sample.

The main settings are:

```sql
SET enable_bloom = false;
SET bloom_memory_limit = '512MB';
SET bloom_sample_cache_dir = '/path/to/cache';
SET bloom_sample_mode = 'instant';
SET bloom_excitation_mode = 'join_key_ndv';
```

`enable_bloom` defaults to `true`; disable it for troubleshooting or an A/B
comparison. `bloom_memory_limit` defaults to `auto`, which admits optimizer-time
Bloom work against at most 25% of currently available DuckDB operator memory.
Each phase must also obtain its complete share from DuckDB's temporary-memory
manager. Bloom's large sample/materialized collections and filter payloads use
DuckDB's non-spill `BufferAllocator`, so they count against the database-wide
memory limit without becoming temporary-storage blocks; large task-local build
states are included in admission estimates. If admission fails
before the first materialization, Bloom keeps DuckDB's original plan; after
transfer has begun it stops adding new transfer rounds. The reservation for
tables and filters retained by the rewritten plan remains active until those
execution objects are destroyed. Set the limit to `0B` to force the fallback
path. Allocation failures that occur after admission are reported to the query;
they are not silently converted into a fallback.

`EXPLAIN`, queries containing volatile expressions, and reads from a database
with uncommitted changes bypass Bloom and remain on DuckDB's native path. This
prevents optimizer-time execution from evaluating side effects or mixing
prepared samples with transaction-local state.

`bloom_sample_cache_dir` defaults to `auto`, which stores samples
under `<database>.bloom_samples/`. Set another directory to share a cache, or `''`
to disable disk caching. Prepared samples are keyed by `bloom_sample_seed`, whose
cross-workload-validated default is `2`; changing the seed creates an independent
cache entry and never reuses the previous sample. The optional
`bloom_sample_memory_cache` also keeps DuckDB's conservative
`ObjectCache` eviction reservation in addition to the sample's allocator charge;
set it to `false` on especially tight memory limits while retaining disk caching.

`bloom_sample_mode` accepts `prepared` (the default) and `instant`.

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
disabled for performance benchmarks. DuckDB's logging storage is separate from
`bloom_memory_limit`, so clear or rotate it as needed.

`SET bloom_log_transfer_steps = true` remains available as a separate option for
verbose, human-readable diagnostics on stderr.
