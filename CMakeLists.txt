cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME bloom)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/bloom_extension.cpp
    src/predicate_transfer/cardinality_estimation/cardinality_estimator.cpp
    src/predicate_transfer/cardinality_estimation/sampling_estimator.cpp
    src/predicate_transfer/filter/bitmap_filter.cpp
    src/predicate_transfer/filter/bloom_filter.cpp
    src/predicate_transfer/filter/table_filter.cpp
    src/predicate_transfer/materialized_cte_lifter.cpp
    src/predicate_transfer/predicate_transfer_optimizer.cpp
    src/predicate_transfer/table_operator_manager.cpp
    src/predicate_transfer/table_scanner.cpp
    src/predicate_transfer/transfer_plan/base_graph_manager.cpp
    src/predicate_transfer/transfer_plan/dag.cpp
    src/predicate_transfer/transfer_plan/excitation_graph_manager.cpp
    src/predicate_transfer/transfer_plan/lineage_tracker.cpp
    src/predicate_transfer/transfer_plan/rpt_filter_cache.cpp
    src/predicate_transfer/transfer_plan/transfer_executor.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
