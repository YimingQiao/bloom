#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator/sampling_estimator.hpp"

#include <cmath>
#include <iostream>
#include <sstream>
#include <stdexcept>

using namespace duckdb;

static void Check(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

static string Field(const string &log, const string &key) {
	const auto start = log.find(" " + key + "=");
	if (start == string::npos) {
		throw std::runtime_error("missing diagnostic field " + key + " in " + log);
	}
	const auto value_start = start + key.size() + 2;
	return log.substr(value_start, log.find_first_of(" \n", value_start) - value_start);
}

static string Run(Connection &con, const vector<std::pair<idx_t, idx_t>> &chunks, double rate, idx_t expected,
                  bool audit = false) {
	vector<LogicalType> types {LogicalType::BOOLEAN};
	auto cdc = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	for (auto shape : chunks) {
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), types);
		for (idx_t row = 0; row < shape.first; row++) {
			chunk.data[0].Append(Value::BOOLEAN(row < shape.second));
		}
		chunk.CheckCardinality(shape.first);
		cdc->Append(chunk);
	}
	auto get = make_uniq<LogicalColumnDataGet>(TableIndex(7), types, std::move(cdc));
	LogicalFilter filter(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 0));
	filter.children.push_back(std::move(get));
	RPTSamplingConfig config;
	config.intermediate_rate = rate;
	SamplingCardinalityEstimator estimator(*con.context, config);
	Check(!con.Query("CALL disable_logging()")->HasError(), "cannot disable diagnostics");
	std::ostringstream disabled;
	auto *previous = std::cerr.rdbuf(disabled.rdbuf());
	const auto before = estimator.Estimate(filter);
	std::cerr.rdbuf(previous);
	Check(disabled.str().empty(), "disabled diagnostics must be silent");
	Check(!con.Query("CALL truncate_duckdb_logs()")->HasError(), "cannot clear diagnostics");
	Check(!con.Query(audit ? "CALL enable_logging('Bloom', storage='memory', level='debug')"
	                       : "CALL enable_logging('Bloom', storage='memory', level='info')")
	           ->HasError(),
	      "cannot enable diagnostics");
	Check(!con.Query(audit ? "SET logging_level='debug'" : "SET logging_level='info'")->HasError(),
	      "cannot set diagnostic level");
	std::ostringstream enabled;
	previous = std::cerr.rdbuf(enabled.rdbuf());
	const auto after = estimator.Estimate(filter);
	std::cerr.rdbuf(previous);
	Check(before == expected && after == expected, "diagnostics must preserve the estimate");
	Check(enabled.str().empty(), "structured diagnostics must not use stderr");
	Logger::Flush(*con.context);
	auto rows = con.Query("SELECT ' phase=' || phase || ' ' || "
	                      "array_to_string(list_transform(map_entries(info), lambda x: x.key || '=' || x.value), ' ') "
	                      "FROM duckdb_logs_parsed('Bloom') WHERE event='cdc_sample' ORDER BY event_sequence");
	rows->ThrowIfError("cannot read structured diagnostics: ");
	string log;
	while (auto chunk = rows->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			log += chunk->data[0].GetValue(i).ToString() + "\n";
		}
	}
	return log;
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	const idx_t n = STANDARD_VECTOR_SIZE;
	// Chunk rates 0, 1/2, 1: include the expression's zero-survivor early exit.
	auto log = Run(con, {{n, 0}, {n, n / 2}, {n, n}}, 1.0, n + n / 2);
	Check(Field(log, "sampled_chunks") == "3", "unexpected chunk count");
	Check(Field(log, "zero_chunks") == "1" && Field(log, "full_chunks") == "1", "extreme chunks lost");
	Check(std::abs(std::stod(Field(log, "chunk_rate_variance")) - 0.25) < 1e-12, "incorrect variance");
	Check(Field(log, "sampling_se") == "0", "census has no sampling error");
	// Same survivor fraction in every chunk must have zero dispersion.
	log = Run(con, {{n, n / 2}, {n, n / 2}, {n, n / 2}}, 1.0, 3 * n / 2);
	Check(Field(log, "chunk_rate_variance") == "0", "identical chunks must have zero variance");
	// A short tail makes row-weighted and chunk-weighted fractions different.
	log = Run(con, {{n, n}, {n, n / 2}, {17, 0}}, 1.0, n + n / 2);
	Check(Field(log, "sample_rows") == std::to_string(2 * n + 17), "tail size was assumed");
	Check(std::stod(Field(log, "row_survival")) > std::stod(Field(log, "chunk_rate_mean")), "weights conflated");
	Check(log.find("2:17:0") != string::npos, "tail chunk not recorded");
	// Partial deterministic samples have no justified random-sampling SE.
	log = Run(con, {{n, n / 2}, {n, n}, {n, n / 2}, {n, 0}}, 0.5, 2 * n);
	Check(Field(log, "design") == "deterministic_spaced", "sampling design changed");
	Check(Field(log, "sampling_se") == "NA", "partial deterministic sample cannot claim SE");
	Check(Field(log, "chunks") == "0:" + std::to_string(n) + ":" + std::to_string(n / 2) + ",2:" + std::to_string(n) +
	                                  ":" + std::to_string(n / 2),
	      "selection changed");
	// A one-chunk zero estimate triggers an exact confirmation, logged separately.
	log = Run(con, {{n, 0}, {n, 1}}, 0.01, 1);
	Check(Field(log, "chunk_rate_variance") == "NA", "one chunk does not determine sample variance");
	Check(log.find("phase=init_zero_confirm") != string::npos, "missing zero confirmation phase");
	Check(log.find("design=census") != string::npos, "confirmation must be full scan");
	log = Run(con, {}, 0.01, 0);
	Check(log.empty(), "empty CDC has no chunks to report");
	// Audit detects a biased all-pass estimate caused by a short tail, but
	// must not change the scheduler's original chunk-count extrapolation.
	log = Run(con, {{n, n}, {17, 17}}, 0.01, 2 * n, true);
	Check(Field(log, "exact_rows") == std::to_string(n + 17), "audit truth is wrong");
	Check(Field(log, "estimate") == std::to_string(2 * n), "audit changed the estimate");
	Check(Field(log, "audit_extra_rows") == std::to_string(n + 17), "audit work not reported");
	// A homogeneous-looking sample can miss a different unobserved chunk.
	log = Run(con, {{n, n / 2}, {n, 0}, {n, n / 2}, {n, 0}}, 0.5, 2 * n, true);
	Check(Field(log, "chunk_rate_variance") == "0", "sample should look homogeneous");
	Check(Field(log, "exact_rows") == std::to_string(n), "audit must see unobserved chunks");
	Check(std::stod(Field(log, "absolute_error_fraction")) == 0.25, "audit error fraction is wrong");
	std::cout << "CDC sample statistics: 8 cases passed\n";
}
