# Updating the pinned DuckDB version

Bloom targets DuckDB `main` (currently pinned to `21aca042`) because it needs
the extensible table-filter API ([duckdb/duckdb#20633](https://github.com/duckdb/duckdb/pull/20633)),
which is not in any stable release yet. To move the pin forward:

1. Bump the `duckdb` submodule to the new commit and rebuild:
   ```bash
   cd duckdb && git fetch origin main && git checkout <new-commit> && cd ..
   git submodule update --init --recursive
   make release
   ```
2. Update `duckdb_version` in `.github/workflows/MainDistributionPipeline.yml`
   to the same commit hash. Keep `ci_tools_version: main` and update the
   `extension-ci-tools` submodule alongside it if the reusable workflows
   changed.
3. Update the pinned commit mentioned in `README.md`.
4. Run the tests and the IMDB benchmark suite before pushing:
   ```bash
   make test
   python3 scripts/run_benchmark_suite.py --db /path/to/imdb.duckdb
   ```

Once a stable DuckDB release contains #20633, switch `duckdb_version` and the
submodule to that tag and pin `ci_tools_version` to the matching
`extension-ci-tools` release branch.

Bloom builds against DuckDB's internal C++ API, which is not stable; a pin
bump may require code fixes. Useful resources for tracking API changes:

- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ header in the `duckdb` submodule
