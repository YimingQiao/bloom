# STATS-CEB

Bloom's benchmark runner supports the 146-query
[STATS-CEB](https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark)
workload. STATS-CEB is a separate cardinality-estimation benchmark from the
IMDb and StackExchange workloads in the original CEB repository.

`scripts/prepare_stats_ceb.py` downloads the upstream repository archive pinned
to commit `670cb8d4bf4cbfa32f94fdf17f33973d3fd67d1b`, verifies its SHA-256 digest,
extracts the simplified STATS CSV data, splits the workload into 146 SQL files,
and builds `.bench_cache/data/stats_ceb.duckdb`.

Each upstream workload line contains its true result cardinality. The preparer
turns these into benchmark answer files, so every timed run checks its result
instead of measuring execution alone.

Prepare the workload and database:

```bash
python3 scripts/prepare_stats_ceb.py --print-root
```

Run the single-thread Bloom-versus-DuckDB comparison:

```bash
python3 scripts/run_benchmark_suite.py \
  --workload stats_ceb \
  --threads 1
```
