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

CDC sample diagnostics have a standalone regression check for chunk dispersion,
short tail chunks, deterministic selection, zero-estimate confirmation, and
identical estimates with logging enabled or disabled:

```bash
c++ -std=c++17 -Isrc/include -Iduckdb/src/include \
    test/cpp/cdc_sample_stats_test.cpp -Lbuild/release/src \
    -Wl,-rpath,"$PWD/build/release/src" -lduckdb \
    -o build/release/cdc_sample_stats_test
build/release/cdc_sample_stats_test
```

The paper lineage tests exercise whole-expression projection, CQ containment,
composite-key correlation, immutable snapshots, separated L/C, and the full
three-table paper example. They also compare inferred containments against
all 256 instances of two binary relations on a two-value domain.

```bash
c++ -std=c++17 -Isrc/include -Iduckdb/src/include \
    test/cpp/key_lineage_test.cpp -Lbuild/release/src \
    -Wl,-rpath,"$PWD/build/release/src" -lduckdb \
    -o build/release/key_lineage_test
build/release/key_lineage_test
build/release/test/unittest --test-dir "$PWD" 'test/sql/transfer/*' --reporter compact
```

`LineageTracker` registers all join keys before the first snapshot of a table.
Its result expression changes only on estimated shrinkage; its constraint
expression records every newly attached filter. Exact filters carry source CQs;
Bloom filters retain separate opaque acceptance predicates, since false positives
do not certify exact source membership. The implementation retains the existing
activation, estimation, and memory policies, so it does not claim exact full
reduction under estimation errors or approximate filters.
