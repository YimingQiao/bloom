# Repository scripts

The top-level scripts are reusable repository tools. One-off experiment logic
belongs under the dated directory in `experiments/`.

## Build and benchmark

- `run_benchmark_suite.py`: compare Bloom with the DuckDB baseline, or compare
  prepared with instant sampling under the same query-warm protocol. Select the
  latter with `--comparison sampling`.
- `run_benchmark.py`: lower-level workload runner used by the suite and
  research experiments.
- `summarize_benchmark.py`: aggregate benchmark timing files.
- `package_release_artifacts.py`: assemble local release bundles.

## Workload preparation and validation

- `prepare_ceb.py`, `prepare_ceb_stack.py`, and `prepare_stats_ceb.py`: fetch,
  verify, and prepare the supported CEB-family workloads.
- `validate_ceb_results.py`: validate CEB result completeness and correctness.

## Sampling experiments

Sampling runners are versioned with their settings and results under
`experiments/`. The current prepared/instant matrix starts at
`experiments/2026-08-oracle-prepared-instant-end-to-end/scripts/run_matrix.py`.
Older parameter sweeps are historical artifacts tied to their recorded source
revision; obsolete development settings are not exposed as repository-wide
tools.
