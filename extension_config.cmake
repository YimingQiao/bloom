duckdb_extension_load(bloom
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    EXTENSION_VERSION v0.0.2
)

# To reproduce the TPC-H / TPC-DS benchmark suites, build the runner with the
# data generators too:
#   CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release -j"$(nproc)"
# They are intentionally not loaded here so the distribution build stays lean.
