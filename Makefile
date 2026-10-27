PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# DuckDB 2.0 no longer derives release versions from Git tags.
OVERRIDE_GIT_DESCRIBE := v2.0.0
EXT_FLAGS += -DLINK_CORE_EXTENSIONS=ON

EXT_NAME=bloom
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

include extension-ci-tools/makefiles/duckdb_extension.Makefile
