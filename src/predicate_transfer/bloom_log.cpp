#include "predicate_transfer/bloom_log.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>

namespace duckdb {

namespace {

// Shared by transfer and estimator events, including nested optimization scopes.
// Reset at query boundaries rather than restarting at each graph manager.
class BloomLogSequenceState : public ClientContextState {
public:
	atomic<idx_t> sequence {0};
	void QueryBegin(ClientContext &) override {
		sequence = 0;
	}
};

static idx_t NextBloomLogSequence(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<BloomLogSequenceState>("bloom_log_sequence");
	return ++state->sequence;
}

class BloomLogType : public LogType {
public:
	static constexpr const char *NAME = "Bloom";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	BloomLogType() : LogType(NAME, LEVEL, GetLogType()) {
	}

	static LogicalType GetLogType() {
		child_list_t<LogicalType> fields = {
		    {"event", LogicalType::VARCHAR},
		    {"phase", LogicalType::VARCHAR},
		    {"status", LogicalType::VARCHAR},
		    {"reason", LogicalType::VARCHAR},
		    {"event_sequence", LogicalType::UBIGINT},
		    {"round_id", LogicalType::UBIGINT},
		    {"step_id", LogicalType::UBIGINT},
		    {"source_table_id", LogicalType::UBIGINT},
		    {"source_table", LogicalType::VARCHAR},
		    {"destination_table_id", LogicalType::UBIGINT},
		    {"destination_table", LogicalType::VARCHAR},
		    {"source_rows", LogicalType::UBIGINT},
		    {"estimated_destination_rows_before", LogicalType::UBIGINT},
		    {"estimated_destination_rows_after", LogicalType::UBIGINT},
		    {"requested_bytes", LogicalType::UBIGINT},
		    {"retained_bytes", LogicalType::UBIGINT},
		    {"budget_bytes", LogicalType::UBIGINT},
		    {"completed_sources", LogicalType::UBIGINT},
		    {"rounds_considered", LogicalType::UBIGINT},
		    {"transfer_count", LogicalType::UBIGINT},
		    {"round_elapsed_ms", LogicalType::DOUBLE},
		    {"elapsed_ms", LogicalType::DOUBLE},
		    {"info", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
		};
		return LogicalType::STRUCT(std::move(fields));
	}

	static string ConstructLogMessage(const char *event, const char *phase, const char *status,
	                                  child_list_t<Value> event_fields, const vector<pair<string, string>> &info) {
		child_list_t<Value> fields = {
		    {"event", Value(event)},
		    {"phase", OptionalString(phase)},
		    {"status", OptionalString(status)},
		    {"reason", Value()},
		    {"event_sequence", Value()},
		    {"round_id", Value()},
		    {"step_id", Value()},
		    {"source_table_id", Value()},
		    {"source_table", Value()},
		    {"destination_table_id", Value()},
		    {"destination_table", Value()},
		    {"source_rows", Value()},
		    {"estimated_destination_rows_before", Value()},
		    {"estimated_destination_rows_after", Value()},
		    {"requested_bytes", Value()},
		    {"retained_bytes", Value()},
		    {"budget_bytes", Value()},
		    {"completed_sources", Value()},
		    {"rounds_considered", Value()},
		    {"transfer_count", Value()},
		    {"round_elapsed_ms", Value()},
		    {"elapsed_ms", Value()},
		    {"info", StringMap(info)},
		};
		for (auto &event_field : event_fields) {
			auto field = std::find_if(fields.begin(), fields.end(),
			                          [&](const auto &candidate) { return candidate.first == event_field.first; });
			D_ASSERT(field != fields.end());
			D_ASSERT(field->second.IsNull());
			field->second = std::move(event_field.second);
		}
		return Value::STRUCT(std::move(fields)).ToString();
	}

private:
	static Value OptionalString(const char *value) {
		return value ? Value(value) : Value();
	}

	static Value StringMap(const vector<pair<string, string>> &entries) {
		vector<Value> keys;
		vector<Value> values;
		keys.reserve(entries.size());
		values.reserve(entries.size());
		for (auto &entry : entries) {
			keys.emplace_back(entry.first);
			values.emplace_back(entry.second);
		}
		return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
	}
};

static Logger *GetBloomLogger(ClientContext &context) {
	auto &logger = Logger::Get(context);
	return logger.ShouldLog(BloomLogType::NAME, BloomLogType::LEVEL) ? &logger : nullptr;
}

static string Milliseconds(double value) {
	return StringUtil::Format("%.3f", value);
}

} // namespace

void RegisterBloomLogType(DatabaseInstance &db) {
	db.GetLogManager().RegisterLogType(make_uniq<BloomLogType>());
}

bool BloomStructuredLoggingEnabled(ClientContext &context) {
	return GetBloomLogger(context) != nullptr;
}

bool BloomSampleAuditEnabled(ClientContext &context) {
	return Logger::Get(context).ShouldLog(BloomLogType::NAME, LogLevel::LOG_DEBUG);
}

void LogBloomCDCSample(ClientContext &context, idx_t table_index, const char *phase,
                       const vector<pair<string, string>> &info) {
	auto logger = GetBloomLogger(context);
	if (!logger) {
		return;
	}
	child_list_t<Value> fields = {
	    {"event_sequence", Value::UBIGINT(NextBloomLogSequence(context))},
	    {"destination_table_id", table_index == DConstants::INVALID_INDEX ? Value() : Value::UBIGINT(table_index)},
	};
	logger->WriteLog(BloomLogType::NAME, BloomLogType::LEVEL,
	                 BloomLogType::ConstructLogMessage("cdc_sample", phase, "observed", std::move(fields), info));
}

void LogBloomSkipped(ClientContext &context, const char *phase, const char *reason) {
	auto logger = GetBloomLogger(context);
	if (!logger) {
		return;
	}
	vector<pair<string, string>> info;
	child_list_t<Value> fields = {
	    {"reason", Value(reason)},
	    {"event_sequence", Value::UBIGINT(NextBloomLogSequence(context))},
	};
	logger->WriteLog(BloomLogType::NAME, BloomLogType::LEVEL,
	                 BloomLogType::ConstructLogMessage("skipped", phase, "skipped", std::move(fields), info));
}

void LogBloomStarted(ClientContext &context, const char *sampling_mode, const char *excitation_mode, idx_t target_rows,
                     idx_t estimated_sample_bytes, idx_t budget_bytes) {
	auto logger = GetBloomLogger(context);
	if (!logger) {
		return;
	}
	vector<pair<string, string>> info = {
	    {"sampling_mode", sampling_mode},
	    {"excitation_mode", excitation_mode},
	    {"target_rows", to_string(target_rows)},
	    {"estimated_sample_bytes", to_string(estimated_sample_bytes)},
	};
	child_list_t<Value> fields = {
	    {"event_sequence", Value::UBIGINT(NextBloomLogSequence(context))},
	    {"budget_bytes", Value::UBIGINT(budget_bytes)},
	};
	logger->WriteLog(BloomLogType::NAME, BloomLogType::LEVEL,
	                 BloomLogType::ConstructLogMessage("start", "sampling", "running", std::move(fields), info));
}

void LogBloomMemoryStopped(ClientContext &context, const char *phase, bool whole_query_fallback, idx_t requested_bytes,
                           idx_t retained_bytes, idx_t budget_bytes, idx_t completed_sources) {
	auto logger = GetBloomLogger(context);
	if (!logger) {
		return;
	}
	vector<pair<string, string>> info;
	child_list_t<Value> fields = {
	    {"event_sequence", Value::UBIGINT(NextBloomLogSequence(context))},
	    {"requested_bytes", Value::UBIGINT(requested_bytes)},
	    {"retained_bytes", Value::UBIGINT(retained_bytes)},
	    {"budget_bytes", Value::UBIGINT(budget_bytes)},
	    {"completed_sources", Value::UBIGINT(completed_sources)},
	};
	logger->WriteLog(BloomLogType::NAME, BloomLogType::LEVEL,
	                 BloomLogType::ConstructLogMessage("memory_stop", phase,
	                                                   whole_query_fallback ? "whole_query_fallback" : "partial_stop",
	                                                   std::move(fields), info));
}

void LogBloomTransfer(ClientContext &context, idx_t round, idx_t step_id, idx_t source_table_id,
                      const string &source_table, idx_t destination_table_id, const string &destination_table,
                      idx_t source_rows, idx_t destination_rows_before, idx_t destination_rows_after, idx_t key_count,
                      bool destination_active_after, double round_elapsed_ms) {
	auto logger = GetBloomLogger(context);
	if (!logger) {
		return;
	}
	vector<pair<string, string>> info = {
	    {"key_count", to_string(key_count)},
	    {"destination_active_after", destination_active_after ? "true" : "false"},
	};
	child_list_t<Value> fields = {
	    {"event_sequence", Value::UBIGINT(NextBloomLogSequence(context))},
	    {"round_id", Value::UBIGINT(round)},
	    {"step_id", Value::UBIGINT(step_id)},
	    {"source_table_id", Value::UBIGINT(source_table_id)},
	    {"source_table", Value(source_table)},
	    {"destination_table_id", Value::UBIGINT(destination_table_id)},
	    {"destination_table", Value(destination_table)},
	    {"source_rows", Value::UBIGINT(source_rows)},
	    {"estimated_destination_rows_before", Value::UBIGINT(destination_rows_before)},
	    {"estimated_destination_rows_after", Value::UBIGINT(destination_rows_after)},
	    {"round_elapsed_ms", Value::DOUBLE(round_elapsed_ms)},
	};
	logger->WriteLog(BloomLogType::NAME, BloomLogType::LEVEL,
	                 BloomLogType::ConstructLogMessage("transfer", "transfer", "applied", std::move(fields), info));
}

void LogBloomTransferCompleted(ClientContext &context, bool partial_stop, idx_t rounds, idx_t transfer_count,
                               idx_t completed_sources, idx_t retained_bytes, idx_t budget_bytes, double init_ms,
                               double materialize_ms, double build_filter_ms, double estimate_ms, double finalize_ms) {
	auto logger = GetBloomLogger(context);
	if (!logger) {
		return;
	}
	vector<pair<string, string>> info = {
	    {"init_ms", Milliseconds(init_ms)},
	    {"materialize_ms", Milliseconds(materialize_ms)},
	    {"build_filter_ms", Milliseconds(build_filter_ms)},
	    {"estimate_ms", Milliseconds(estimate_ms)},
	    {"finalize_ms", Milliseconds(finalize_ms)},
	};
	auto elapsed_ms = init_ms + materialize_ms + build_filter_ms + estimate_ms + finalize_ms;
	child_list_t<Value> fields = {
	    {"event_sequence", Value::UBIGINT(NextBloomLogSequence(context))},
	    {"retained_bytes", Value::UBIGINT(retained_bytes)},
	    {"budget_bytes", Value::UBIGINT(budget_bytes)},
	    {"completed_sources", Value::UBIGINT(completed_sources)},
	    {"rounds_considered", Value::UBIGINT(rounds)},
	    {"transfer_count", Value::UBIGINT(transfer_count)},
	    {"elapsed_ms", Value::DOUBLE(elapsed_ms)},
	};
	logger->WriteLog(BloomLogType::NAME, BloomLogType::LEVEL,
	                 BloomLogType::ConstructLogMessage("transfer_complete", "transfer_plan",
	                                                   partial_stop ? "partial_stop" : "applied", std::move(fields),
	                                                   info));
}

} // namespace duckdb
