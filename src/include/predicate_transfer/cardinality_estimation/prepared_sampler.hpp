#pragma once

#include "predicate_transfer/config.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

class ClientContext;
class LogicalOperator;

//! Return a query-independent, full-width prepared sample for one table scan.
//! The module owns memory/disk reuse and builds a reservoir only on a cache miss.
shared_ptr<ColumnDataCollection> GetOrCreatePreparedSample(ClientContext &context, const RPTSamplingConfig &config,
                                                           unique_ptr<LogicalOperator> scan, idx_t total_rows,
                                                           uint64_t table_seed);

//! Load every matching persisted DuckDB-table sample into ObjectCache without
//! scanning table payloads or building missing samples.
idx_t PreloadPreparedSamples(ClientContext &context, const RPTSamplingConfig &config);

} // namespace duckdb
