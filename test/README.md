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
full SQL test suite with Bloom loaded, both with RPT disabled and enabled.
