# Updating the pinned DuckDB version

Bloom targets DuckDB **v2.0.0**, pinned to
`d54cbbe3df17e8e5ce68312f483b4155836112aa`. It builds against the unmodified
release source. To update to another DuckDB release:

1. Update the `duckdb` submodule:

   ```bash
   git -C duckdb fetch origin --tags
   git -C duckdb checkout <new-commit>
   git submodule update --init --recursive
   ```

2. Set the same full commit and release version in the distribution and
   release-candidate workflows. Update the explicit version used by Makefile
   and direct CMake builds in CI as well.
3. Update the supported version in `README.md`, `RELEASING.md`, and the
   community extension description. Keep the extension CI tools pinned.
4. Build and run the local gates:

   ```bash
   make release -j"$(nproc)"
   make format-check
   make test -j"$(nproc)"
   python3 scripts/run_benchmark.py --workload imdb --timed-runs 2
   python3 scripts/run_benchmark.py --workload tpch_sf10 --timed-runs 2
   ```

5. Run both modes of the manual `DuckDB Full Compatibility` workflow and the
   performance comparison. Follow [RELEASING.md](../RELEASING.md) before
   publishing a release.

Bloom builds against DuckDB's internal C++ API, which is not stable; a pin
bump may require source changes. Useful resources for tracking API changes:

- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ header in the `duckdb` submodule
