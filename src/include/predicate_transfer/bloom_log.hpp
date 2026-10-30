#pragma once

#include "duckdb/common/common.hpp"
#include <utility>

namespace duckdb {

class ClientContext;
class DatabaseInstance;

//! Register the SQL-queryable structured log used by Bloom observability.
void RegisterBloomLogType(DatabaseInstance &db);

//! Check once at the query boundary before collecting timing or row metrics.
bool BloomStructuredLoggingEnabled(ClientContext &context);

//! DEBUG Bloom logging additionally verifies CDC estimates by exact filtering.
bool BloomSampleAuditEnabled(ClientContext &context);
void LogBloomCDCSample(ClientContext &context, idx_t table_index, const char *phase,
                       const vector<std::pair<string, string>> &info);

void LogBloomSkipped(ClientContext &context, const char *phase, const char *reason);

void LogBloomStarted(ClientContext &context, const char *sampling_mode, const char *excitation_mode, idx_t target_rows,
                     idx_t estimated_sample_bytes, idx_t budget_bytes);

void LogBloomMemoryStopped(ClientContext &context, const char *phase, bool whole_query_fallback, idx_t requested_bytes,
                           idx_t retained_bytes, idx_t budget_bytes, idx_t completed_sources);

void LogBloomTransfer(ClientContext &context, idx_t round, idx_t step_id, idx_t source_table_id,
                      const string &source_table, idx_t destination_table_id, const string &destination_table,
                      idx_t source_rows, idx_t destination_rows_before, idx_t destination_rows_after, idx_t key_count,
                      bool destination_active_after, double round_elapsed_ms);

void LogBloomTransferCompleted(ClientContext &context, bool partial_stop, idx_t rounds, idx_t transfer_count,
                               idx_t completed_sources, idx_t retained_bytes, idx_t budget_bytes, double init_ms,
                               double materialize_ms, double build_filter_ms, double estimate_ms, double finalize_ms);

} // namespace duckdb
