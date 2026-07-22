# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(bloom
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# To reproduce the TPC-H / TPC-DS benchmark suites, build the runner with the
# data generators too:
#   CORE_EXTENSIONS='tpch;tpcds' BUILD_BENCHMARK=1 make release
# They are intentionally not loaded here so the distribution build stays lean.
