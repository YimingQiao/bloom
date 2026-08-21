#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ClientContext;
class PhysicalOperator;
class PreparedStatementData;

//! Build an optimizer-time result collector whose retained data is allocated
//! through DuckDB's BufferAllocator. Unlike BUFFER_MANAGED query results these
//! allocations count against the buffer-pool limit but cannot be spilled.
unique_ptr<PhysicalOperator> GetRPTResultCollector(ClientContext &context, PreparedStatementData &data);

} // namespace duckdb
