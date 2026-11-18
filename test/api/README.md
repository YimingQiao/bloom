Standalone ART-transfer boundary check (from the extension root):

    g++ -O2 -std=c++17 -Isrc/include -Iduckdb/src/include -Iduckdb/third_party/fmt/include test/api/art_transfer.cpp -Lbuild/release/src -Wl,-rpath,"$PWD/build/release/src" -lduckdb -lpthread -o /tmp/bloom-art-transfer-test
    /tmp/bloom-art-transfer-test

Checks strict 2,048 input/output boundaries, unknown estimates, signed exact-key
enumeration, missing/expression/composite indexes, binding copies, and rejection
of coarsened prefix filters. SQL execution regressions are in
test/sql/transfer/art_index.test.

Standalone pass-bitmap profiling check:

    g++ -O2 -std=c++17 -Isrc/include -Iduckdb/src/include -Iduckdb/third_party/fmt/include test/api/filter_profiling.cpp -Lbuild/release/src -Wl,-rpath,"$PWD/build/release/src" -lduckdb -lpthread -o /tmp/bloom-filter-profiling-test
    /tmp/bloom-filter-profiling-test

Checks correlated conjunctions, OR grouping, NULLs, partial bitmap words,
constant/empty results, native PRF evaluation, and reuse by filter identity plus
column binding and refreshing pending predicates on a reused sample. It also compares accumulated
cost-prefix counts against direct row evaluation, including empty samples,
runtime-only filters, NULLs and partial bitmap words. End-to-end profiling regressions:

    build/release/test/unittest --stdin < test/sql/transfer/filter_profiling.test
