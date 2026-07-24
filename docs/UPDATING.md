# Updating the pinned DuckDB version

Bloom targets DuckDB `main` (currently pinned to `21aca042`) because it needs
the extensible table-filter API ([duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633)),
which is not in a supported stable release yet. To move the pin forward:

1. Update the `duckdb` submodule:

   ```bash
   git -C duckdb fetch origin main
   git -C duckdb checkout <new-commit>
   git submodule update --init --recursive
   ```

2. Set the same full commit in
   `.github/workflows/MainDistributionPipeline.yml` and
   `.github/workflows/ReleaseCandidate.yml`.
3. Update the short commit shown in `README.md`.
4. Build and run the local gates:

   ```bash
   make release -j"$(nproc)"
   make format-check
   make test -j"$(nproc)"
   python3 scripts/run_benchmark.py --workload imdb --timed-runs 2
   python3 scripts/run_benchmark.py --workload tpch_sf10 --timed-runs 2
   ```

5. Run both modes of the manual `DuckDB Full Compatibility` workflow. Before a
   release, also run `Release Candidate`.

Bloom builds against DuckDB's internal C++ API, which is not stable; a pin
bump may require source changes. Useful resources for tracking API changes:

- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ header in the `duckdb` submodule
