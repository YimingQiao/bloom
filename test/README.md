# Testing Bloom

Bloom's extension tests are written as
[SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) under `test/sql`.

Build and run the extension test suite:

```bash
make test -j"$(nproc)"
```

For a debug build:

```bash
make test_debug -j"$(nproc)"
```

The manual `DuckDB Full Compatibility` GitHub Actions workflow runs DuckDB's
full SQL test suite with Bloom loaded, both with Bloom disabled and enabled.

Sampling regression tests also check the sample survivor counts against native
query execution, using the built CLI:

```bash
python3 -m unittest discover --start-directory test/python -p test_sample_filters.py
```

These tests default to `build/release/duckdb`. Set `BLOOM_TEST_DUCKDB` to use another
Bloom-enabled build; they are skipped if the CLI has not been built.
