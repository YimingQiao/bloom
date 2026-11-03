#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator/sampling_estimator.hpp"
#include "predicate_transfer/filter/bitmap_filter.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"
#include "predicate_transfer/table_scanner/table_scanner.hpp"

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

static string Run(Connection &con, const vector<std::pair<idx_t, idx_t>> &chunks, idx_t expected, bool audit = false,
                  idx_t budget = 4096) {
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
	config.in_memory_sample_size = budget;
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

// Exercise borrowed vectors across expression, bitmap and composite-key BF filters.
static void CheckBorrowedFilterChain(Connection &con, idx_t row_count = 32785, bool managed = true,
                                     bool projected = false) {
	con.Query("CALL disable_logging()")->ThrowIfError();
	con.Query("SET enable_bloom=false")->ThrowIfError();
	auto result = con.Query("SELECT repeat('long payload ', 4) || i::VARCHAR AS payload, i % 97 AS k1, "
	                        "[i, NULL, i+1] AS nested, i % 2 = 0 AS keep, "
	                        "CASE WHEN i % 11 = 0 THEN NULL ELSE i % 89 END AS k2 "
	                        "FROM range(" +
	                        std::to_string(row_count) + ") t(i)");
	result->ThrowIfError();
	auto cdc = managed ? make_uniq<ColumnDataCollection>(*con.context, result->GetTypes())
	                   : make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), result->GetTypes());
	ColumnDataCollection original(Allocator::DefaultAllocator(), result->GetTypes());
	while (auto chunk = result->Fetch()) {
		cdc->Append(*chunk);
		original.Append(*chunk);
	}
	auto *source = cdc.get();
	auto get = make_uniq<LogicalColumnDataGet>(TableIndex(7), result->GetTypes(), std::move(cdc));
	LogicalFilter filter(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 3));
	filter.children.push_back(std::move(get));
	if (projected) {
		filter.projection_map = {ProjectionIndex(4), ProjectionIndex(1)};
	}
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	TableScanner scanner(optimizer, *con.context, filter);
	for (idx_t col : {idx_t(1), idx_t(4)}) {
		auto bitmap = make_shared_ptr<BitmapFilter>(*con.context, BitmapFilterConfig(0, 100));
		auto keys = con.Query(col == 1 ? "SELECT i FROM range(97) t(i) WHERE i % 3 = 0"
		                               : "SELECT i FROM range(89) t(i) WHERE i % 5 = 0");
		keys->ThrowIfError();
		while (auto chunk = keys->Fetch()) {
			bitmap->Insert(*chunk, {0});
		}
		bitmap->SetValid();
		scanner.AddFilter(ColumnBinding(TableIndex(7), ProjectionIndex(col)), bitmap, col);
	}
	auto bloom = make_shared_ptr<CacheSectorizedBF>(*con.context, BloomFilterConfig(), 1000);
	auto keys = con.Query("SELECT i, j FROM range(97) a(i), range(89) b(j) WHERE (i+j) % 7 = 0");
	keys->ThrowIfError();
	while (auto chunk = keys->Fetch()) {
		bloom->Insert(*chunk, {0, 1});
	}
	bloom->SetValid();
	scanner.AddFilter(vector<ColumnBinding> {ColumnBinding(TableIndex(7), ProjectionIndex(1)),
	                                         ColumnBinding(TableIndex(7), ProjectionIndex(4))},
	                  bloom, 3);
	DataChunk chunk;
	scanner.InitScanChunk(chunk);
	idx_t expected = 0;
	while (scanner.Scan(chunk)) {
		expected += chunk.size();
	}
	Check(expected > 0 && expected < original.Count(), "filter-chain reference must reduce the input");
	RPTSamplingConfig config;
	SamplingCardinalityEstimator estimator(*con.context, config);
	const auto sample_expected = row_count > 4096 ? estimator.Estimate(scanner, {}) : expected;
	Check(sample_expected > 0 && sample_expected <= row_count, "invalid row-sample estimate");
	for (idx_t repeat = 0; repeat < 20; repeat++) {
		Check(estimator.Estimate(scanner, {}) == sample_expected,
		      "borrowed filter chain differs from normal execution");
	}
	string error;
	Check(ColumnDataCollection::ResultEquals(original, *source, error, true), "filtering changed the source CDC");
}

static void CheckConstantPredicates(Connection &con) {
	con.Query("CALL disable_logging()")->ThrowIfError();
	for (idx_t row_count : {idx_t(17), idx_t(2051), idx_t(32785)}) {
		auto result = con.Query("SELECT i FROM range(" + std::to_string(row_count) + ") t(i)");
		result->ThrowIfError();
		ColumnDataCollection cdc(*con.context, result->GetTypes());
		while (auto chunk = result->Fetch())
			cdc.Append(*chunk);
		for (bool accepted : {false, true}) {
			LogicalFilter filter(make_uniq<BoundConstantExpression>(Value::BOOLEAN(accepted)));
			filter.children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(7), cdc.Types(), cdc));
			{
				RPTSamplingConfig config;
				SamplingCardinalityEstimator estimator(*con.context, config);
				const auto expected = accepted ? row_count
				                      : row_count > config.in_memory_sample_size
				                          ? row_count / config.in_memory_sample_size
				                          : 0;
				Check(estimator.Estimate(filter) == expected, "zero-column sample lost cardinality");
			}
		}
	}
}

static void CheckProjectedValues(Connection &con) {
	con.Query("CALL disable_logging()")->ThrowIfError();
	auto result = con.Query("SELECT i, repeat('payload-', 8) AS text, i+1 AS unused, "
	                        "[42::BIGINT, NULL, 99::BIGINT] AS nested FROM range(32785) t(i)");
	result->ThrowIfError();
	ColumnDataCollection cdc(*con.context, result->GetTypes());
	ColumnDataCollection original(Allocator::DefaultAllocator(), result->GetTypes());
	while (auto chunk = result->Fetch()) {
		cdc.Append(*chunk);
		original.Append(*chunk);
	}
	string payload;
	for (idx_t i = 0; i < 8; i++)
		payload += "payload-";
	auto string_test = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
	                                                     make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, 1),
	                                                     make_uniq<BoundConstantExpression>(Value(payload)));
	auto list_test = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundReferenceExpression>(cdc.Types()[3], 3),
	    make_uniq<BoundConstantExpression>(
	        Value::LIST(LogicalType::BIGINT, {Value::BIGINT(42), Value(LogicalType::BIGINT), Value::BIGINT(99)})));
	LogicalFilter filter(make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(string_test),
	                                                           std::move(list_test)));
	filter.children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(7), cdc.Types(), cdc));
	{
		RPTSamplingConfig config;
		SamplingCardinalityEstimator estimator(*con.context, config);
		Check(estimator.Estimate(filter) == cdc.Count(), "projected strings/lists changed during gathering");
	}
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	TableScanner scanner(optimizer, *con.context, filter);
	{
		RPTSamplingConfig cached_config;
		SamplingCardinalityEstimator cached(*con.context, cached_config);
		for (idx_t repeat = 0; repeat < 4; repeat++)
			Check(cached.Estimate(scanner, {}) == cdc.Count(), "nested/string sample changed");
	}
	string error;
	Check(ColumnDataCollection::ResultEquals(original, cdc, error, true),
	      "projected sample modified variable-length data");
}

static void CheckDirectGatherValues(Connection &con) {
	con.Query("CALL disable_logging()")->ThrowIfError();
	con.Query("SET enable_bloom=false")->ThrowIfError();
	for (idx_t count : {idx_t(17), idx_t(32785)}) {
		auto result = con.Query("SELECT i%2=0 AS b, (i%100)::TINYINT AS i8, (i%1000)::SMALLINT AS i16, "
		                        "i::INTEGER AS i32, i::BIGINT AS i64, (i%100)::UTINYINT AS u8, i::USMALLINT AS u16, "
		                        "i::UINTEGER AS u32, i::UBIGINT AS u64, i::HUGEINT AS i128, i::UHUGEINT AS u128, "
		                        "(i/3)::FLOAT AS f, (i/7)::DOUBLE AS d, i::DECIMAL(18,2) AS dec, "
		                        "DATE '2000-01-01'+i::INTEGER AS dt, i * INTERVAL '1 second' AS iv, "
		                        "CASE WHEN i%7=0 THEN NULL ELSE i END AS nullable FROM range(" +
		                        std::to_string(count) + ") t(i)");
		result->ThrowIfError();
		ColumnDataCollection memory(Allocator::DefaultAllocator(), result->GetTypes());
		ColumnDataCollection managed(*con.context, result->GetTypes());
		ColumnDataCollection memory_tail(Allocator::DefaultAllocator(), result->GetTypes());
		ColumnDataCollection managed_tail(*con.context, result->GetTypes());
		vector<Value> constants;
		while (auto chunk = result->Fetch()) {
			if (constants.empty())
				for (idx_t c = 0; c < chunk->ColumnCount(); c++)
					constants.push_back(chunk->GetValue(c, 1));
			if (memory.Count() < 8192) {
				memory.Append(*chunk);
				managed.Append(*chunk);
			} else {
				memory_tail.Append(*chunk);
				managed_tail.Append(*chunk);
			}
		}
		memory.Combine(memory_tail);
		managed.Combine(managed_tail);
		for (idx_t col = 0; col < memory.Types().size(); col++) {
			for (idx_t seed = 0; seed < 3; seed++) {
				RPTSamplingConfig config;
				config.seed = seed;
				SamplingCardinalityEstimator estimator(*con.context, config);
				auto make_filter = [&](ColumnDataCollection &source) {
					auto f = make_uniq<LogicalFilter>(
					    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN,
						                                  make_uniq<BoundReferenceExpression>(source.Types()[col], col),
						                                  make_uniq<BoundConstantExpression>(constants[col])));
					f->children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(7), source.Types(), source));
					return f;
				};
				auto a = make_filter(memory), b = make_filter(managed);
				Check(estimator.Estimate(*a) == estimator.Estimate(*b), "direct gather differs from managed reference");
			}
		}
	}
}

static string EstimateCacheLog(Connection &con, SamplingCardinalityEstimator &estimator, TableScanner &scanner,
                               idx_t &estimate) {
	con.Query("CALL truncate_duckdb_logs()")->ThrowIfError();
	con.Query("CALL enable_logging('Bloom', storage='memory', level='info')")->ThrowIfError();
	estimate = estimator.Estimate(scanner, {});
	Logger::Flush(*con.context);
	auto result =
	    con.Query("SELECT ' sample_reused=' || info['sample_reused'] || ' gather_path=' || info['gather_path'] || "
		          "' read_chunks=' || info['read_chunks'] FROM duckdb_logs_parsed('Bloom') "
		          "WHERE event='cdc_sample' AND phase='transfer'");
	result->ThrowIfError();
	Check(result->RowCount() == 1, "missing cache diagnostic");
	auto log = result->Fetch()->GetValue(0, 0).ToString();
	con.Query("CALL disable_logging()")->ThrowIfError();
	return log;
}

static void CheckSampleCacheLifecycle(Connection &con) {
	con.Query("SET enable_bloom=false")->ThrowIfError();
	auto result = con.Query("SELECT i%97 AS k, i::BIGINT AS id FROM range(32785) t(i)");
	result->ThrowIfError();
	auto source = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), result->GetTypes());
	while (auto chunk = result->Fetch())
		source->Append(*chunk);
	LogicalFilter filter(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	filter.children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(7), source->Types(), std::move(source)));
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	TableScanner scanner(optimizer, *con.context, filter);
	RPTSamplingConfig config;
	SamplingCardinalityEstimator estimator(*con.context, config);
	idx_t estimate = 0;
	auto log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(estimate == 32785 && Field(log, "sample_reused") == "false", "initial zero-column sample failed");
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(estimate == 32785 && Field(log, "sample_reused") == "true", "zero-column cached sample lost cardinality");
	auto add_filter = [&](idx_t modulus) {
		auto bitmap = make_shared_ptr<BitmapFilter>(*con.context, BitmapFilterConfig(0, 100));
		auto keys = con.Query("SELECT i FROM range(97) t(i) WHERE i%" + std::to_string(modulus) + "=0");
		while (auto chunk = keys->Fetch())
			bitmap->Insert(*chunk, {0});
		bitmap->SetValid();
		scanner.AddFilter(ColumnBinding(TableIndex(7), ProjectionIndex(0)), bitmap, modulus);
	};
	add_filter(2);
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(Field(log, "sample_reused") == "false" && Field(log, "gather_path") == "direct",
	      "new sample column must be acquired");
	const auto first = estimate;
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(estimate == first && Field(log, "sample_reused") == "true" && Field(log, "read_chunks") == "0",
	      "cache did not avoid source reads");
	add_filter(3);
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	SamplingCardinalityEstimator fresh(*con.context, config);
	Check(Field(log, "sample_reused") == "true" && estimate == fresh.Estimate(scanner, {}),
	      "changed filter corrupted cached sample");
	Check(estimator.MemoryUsage() > 0, "sample memory was not accounted");
	auto old_source = weak_ptr<ColumnDataCollection>(scanner.GetDataReference());
	scanner.Compact();
	Check(old_source.expired(), "sample cache retained old CDC");
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(Field(log, "sample_reused") == "false" && estimate == scanner.Count(), "replacement CDC reused stale sample");
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(Field(log, "sample_reused") == "true", "replacement CDC did not cache");
	auto extra = con.Query("SELECT 0::BIGINT, 0::BIGINT FROM range(23)");
	while (auto chunk = extra->Fetch())
		scanner.GetData()->Append(*chunk);
	log = EstimateCacheLog(con, estimator, scanner, estimate);
	Check(Field(log, "sample_reused") == "false" && estimate == scanner.Count(), "append reused stale sample");
	auto empty = make_shared_ptr<BitmapFilter>(*con.context, BitmapFilterConfig(0, 100));
	empty->SetValid();
	scanner.AddFilter(ColumnBinding(TableIndex(7), ProjectionIndex(0)), empty, 99);
	for (idx_t repeat = 0; repeat < 2; repeat++) {
		log = EstimateCacheLog(con, estimator, scanner, estimate);
		const auto expected =
		    scanner.Count() > config.in_memory_sample_size ? scanner.Count() / config.in_memory_sample_size : 0;
		Check(estimate == expected && Field(log, "sample_reused") == (repeat ? "true" : "false"),
		      "zero-survivor evaluation overwrote cached sample");
	}
}

static void CheckCachedTwoChunks(Connection &con) {
	con.Query("CALL disable_logging()")->ThrowIfError();
	auto result = con.Query("SELECT i::BIGINT AS id FROM range(10003) t(i)");
	result->ThrowIfError();
	auto source = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), result->GetTypes());
	while (auto chunk = result->Fetch())
		source->Append(*chunk);
	LogicalFilter filter(BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                       make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0),
	                                                       make_uniq<BoundConstantExpression>(Value::BIGINT(0))));
	filter.children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(7), source->Types(), std::move(source)));
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);
	TableScanner scanner(optimizer, *con.context, filter);
	// Every row passes; both sampled chunks must contribute on acquisition and reuse.
	for (idx_t seed : {idx_t(0), idx_t(1), idx_t(17)}) {
		RPTSamplingConfig config;
		config.seed = seed;
		SamplingCardinalityEstimator estimator(*con.context, config);
		Check(estimator.Estimate(scanner, {}) == 10003, "4096-row sample lost rows in its second chunk");
		Check(estimator.Estimate(scanner, {}) == 10003, "4096-row cached sample lost rows");
	}
}

static void CheckRareSurvivor(Connection &con) {
	con.Query("CALL disable_logging()")->ThrowIfError();
	auto result = con.Query("SELECT i = 9999 AS keep FROM range(10000) t(i)");
	result->ThrowIfError();
	ColumnDataCollection cdc(Allocator::DefaultAllocator(), result->GetTypes());
	while (auto chunk = result->Fetch())
		cdc.Append(*chunk);
	LogicalFilter filter(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 0));
	filter.children.push_back(make_uniq<LogicalColumnDataGet>(TableIndex(7), cdc.Types(), cdc));
	for (idx_t seed = 0; seed < 16; seed++) {
		RPTSamplingConfig config;
		config.seed = seed;
		SamplingCardinalityEstimator estimator(*con.context, config);
		const auto estimate = estimator.Estimate(filter);
		Check(estimate == 2, "rare survivor must receive a small positive estimate without a full scan");
	}
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	const idx_t n = STANDARD_VECTOR_SIZE;
	// Census boundaries straddle one and two execution vectors.
	string log;
	for (idx_t count : {idx_t(1), idx_t(17), idx_t(2047), idx_t(2048), idx_t(2049), idx_t(4095), idx_t(4096)}) {
		vector<std::pair<idx_t, idx_t>> chunks;
		idx_t remaining = count, expected = 0;
		while (remaining) {
			const auto rows = MinValue(remaining, n);
			chunks.emplace_back(rows, rows / 2);
			expected += rows / 2;
			remaining -= rows;
		}
		log = Run(con, chunks, expected);
		Check(Field(log, "design") == "census", "small table must use all rows");
		Check(Field(log, "sample_rows") == std::to_string(count), "census lost rows at vector boundary");
		Check(Field(log, "sampling_se") == "0", "census has sampling error");
	}
	for (idx_t count : {idx_t(4097), idx_t(10257), idx_t(32785)}) {
		vector<std::pair<idx_t, idx_t>> chunks;
		for (idx_t remaining = count; remaining;) {
			const auto rows = MinValue(remaining, n);
			chunks.emplace_back(rows, rows);
			remaining -= rows;
		}
		log = Run(con, chunks, count, true);
		Check(Field(log, "sample_rows") == "4096", "large table must sample exactly 4096 rows");
		Check(Field(log, "design") == "uniform_rows_without_replacement", "incorrect row sampling design");
		Check(Field(log, "exact_rows") == std::to_string(count), "full audit lost rows");
	}
	for (idx_t budget : {idx_t(1), idx_t(1024), idx_t(4096), idx_t(8192)}) {
		log = Run(con, {{n, n}, {n, n}, {n, n}, {n, n}, {n, n}}, 5 * n, false, budget);
		Check(Field(log, "sample_rows") == std::to_string(budget), "configured row budget ignored");
		log = Run(con, {{17, 17}}, 17, false, budget);
		Check(Field(log, "sample_rows") == std::to_string(MinValue<idx_t>(17, budget)), "small-table budget ignored");
	}
	log = Run(con, {{n, 0}, {n, 0}, {17, 0}}, 1);
	Check(Field(log, "estimate_status") == "sampled_zero_floor", "missing low-selectivity estimate status");
	Check(Field(log, "sample_rows") == "4096" && Field(log, "survivors") == "0", "zero sample changed its row budget");
	Check(Field(log, "audit") == "false" && log.find("exact_rows=") == string::npos &&
	          log.find("zero_confirm") == string::npos,
	      "zero sample must not trigger a full scan");
	log = Run(con, {{17, 0}}, 0);
	Check(Field(log, "estimate_status") == "exact", "census must still return exact zero");
	log = Run(con, {}, 0);
	Check(log.empty(), "empty table has no sampled rows");
	for (bool managed : {false, true}) {
		for (bool projected : {false, true}) {
			for (idx_t count : {idx_t(701), idx_t(2048), idx_t(4095), idx_t(4096), idx_t(32785)}) {
				CheckBorrowedFilterChain(con, count, managed, projected);
			}
		}
	}
	CheckCachedTwoChunks(con);
	CheckRareSurvivor(con);
	CheckDirectGatherValues(con);
	CheckSampleCacheLifecycle(con);
	CheckConstantPredicates(con);
	CheckProjectedValues(con);
	std::cout << "CDC sample statistics: 4096-row sampling/census, direct-gather types and multi-segment layouts, "
	             "cache lifecycle, borrowed/projected filters and constant predicates passed\n";
}
