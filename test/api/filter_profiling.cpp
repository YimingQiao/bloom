// Standalone API regression; see test/api/README.md.
#include "duckdb.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator/filter_profiler.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <cmath>
#include <limits>
using namespace duckdb;
static void Require(bool ok, const char *what) {
	if (!ok)
		throw std::runtime_error(what);
}
// Independent reference: count matching sample rows directly, without bitmap
// prefix accumulation, using the same measured costs and ordering policy.
static void CheckLocalCost(const SampleFilterProfiler &profiler, const vector<idx_t> &active) {
	vector<idx_t> runtime, local;
	const auto &observations = profiler.Observations();
	for (auto id : active) {
		(observations[id].expression ? local : runtime).push_back(id);
	}
	auto rank = [&](idx_t id) {
		auto rejected = profiler.SampleSize() - observations[id].passed;
		return rejected ? observations[id].evaluation_ns / rejected : std::numeric_limits<double>::infinity();
	};
	std::stable_sort(local.begin(), local.end(), [&](idx_t a, idx_t b) { return rank(a) < rank(b); });
	vector<idx_t> counts(local.size(), 0);
	for (idx_t row = 0; row < profiler.SampleSize(); row++) {
		auto passes = [&](idx_t id) {
			return observations[id].passes[row / 64] & (uint64_t(1) << (row % 64));
		};
		if (!std::all_of(runtime.begin(), runtime.end(), passes)) {
			continue;
		}
		for (idx_t i = 0; i < local.size(); i++) {
			counts[i]++;
			if (!passes(local[i])) {
				break;
			}
		}
	}
	double expected = 0;
	for (idx_t i = 0; i < local.size() && profiler.SampleSize(); i++) {
		expected += observations[local[i]].evaluation_ns * double(counts[i]) / double(profiler.SampleSize());
	}
	Require(std::abs(profiler.EstimateLocalEvaluationNS(active) - expected) <= 1e-8,
	        "cost prefix counts match direct row evaluation");
}
int main() {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::INTEGER,
	                           LogicalType::INTEGER};
	ColumnDataCollection sample(BufferAllocator::Get(ctx), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	const idx_t rows = 4099;
	idx_t local_count = 0, runtime_count = 0, joint_count = 0, second_count = 0;
	for (idx_t start = 0; start < rows; start += STANDARD_VECTOR_SIZE) {
		chunk.Reset();
		auto n = MinValue<idx_t>(rows - start, STANDARD_VECTOR_SIZE);

		for (idx_t j = 0; j < n; j++) {
			auto i = start + j;
			bool valid = i % 17 != 0;
			bool a = i % 3 == 0, b = i % 3 == 0, c = i % 5 == 0;
			chunk.data[0].Append(valid ? Value::BOOLEAN(a) : Value(LogicalType::BOOLEAN));
			chunk.data[1].Append(Value::BOOLEAN(b));
			chunk.data[2].Append(Value::BOOLEAN(c));
			chunk.data[3].Append(valid ? Value::INTEGER(i % 7) : Value(LogicalType::INTEGER));
			chunk.data[4].Append(Value::INTEGER(i % 11));
			local_count += valid && a && (b || c);
			runtime_count += valid && i % 7 < 2;
			joint_count += valid && a && (b || c) && i % 7 < 2;
			second_count += i % 11 < 2;
		}
		chunk.CheckCardinality(n);
		sample.Append(chunk);
	}
	SampleFilterProfiler profiler(ctx, sample, {0, 1, 2, 3, 4});
	auto root = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	root->GetChildrenMutable().push_back(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 0));
	auto either = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
	either->GetChildrenMutable().push_back(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 1));
	either->GetChildrenMutable().push_back(make_uniq<BoundReferenceExpression>(LogicalType::BOOLEAN, 2));
	root->GetChildrenMutable().push_back(std::move(either));
	profiler.AddLocal(*root);
	Require(profiler.LocalFilters().size() == 2, "AND split, OR retained");
	Require(profiler.Count({}) == rows, "empty conjunction");
	Require(profiler.Count(profiler.LocalFilters()) == local_count, "NULL and correlated conjunction");
	BoundConstantExpression always(Value::BOOLEAN(true));
	profiler.AddLocal(always);
	Require(profiler.Count({2}) == rows, "all-true bitmap including partial last word");
	auto filter = make_shared_ptr<DuckDBPrefixRangeFilterAdapter>(ctx, LogicalType::INTEGER, 0, 1, 2);
	DataChunk keys;
	keys.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	keys.data[0].Append(Value::INTEGER(0));
	keys.data[0].Append(Value::INTEGER(1));
	keys.CheckCardinality(2);
	filter->Insert(keys, {0});
	filter->SetValid();
	ScannerFilterSet::Entry entry;
	entry.filter = filter;
	entry.chunk_cols = {3};
	auto id = profiler.AddRuntime(entry);
	Require(profiler.Count({id}) == runtime_count, "runtime filter on raw rows");
	auto elapsed = profiler.Observations()[id].evaluation_ns;
	Require(profiler.AddRuntime(entry) == id && profiler.Observations()[id].evaluation_ns == elapsed,
	        "cached immutable filter");
	auto active = profiler.LocalFilters();
	active.push_back(id);
	Require(profiler.Count(active) == joint_count, "intersection equals full joint evaluation");
	CheckLocalCost(profiler, {});
	CheckLocalCost(profiler, {id});
	CheckLocalCost(profiler, {2});
	CheckLocalCost(profiler, profiler.LocalFilters());
	CheckLocalCost(profiler, active);
	entry.chunk_cols = {4};
	auto second = profiler.AddRuntime(entry);
	Require(second != id && profiler.Count({second}) == second_count, "same filter different bound column");
	active.push_back(second);
	CheckLocalCost(profiler, active);
	Require(profiler.Count(profiler.LocalFilters()) == local_count, "runtime probing preserves raw sample");
	BoundConstantExpression never(Value(LogicalType::BOOLEAN));
	profiler.AddLocal(never);
	Require(profiler.Count(profiler.LocalFilters()) == 0, "NULL predicate rejects every row");
	CheckLocalCost(profiler, profiler.LocalFilters());
	Require(profiler.MemoryUsage() > 0, "bitmap memory accounted");
	auto observation_count = profiler.Observations().size();
	auto local_elapsed = profiler.Observations()[0].evaluation_ns;
	profiler.ClearLocal();
	profiler.AddLocal(always);
	Require(profiler.Count(profiler.LocalFilters()) == rows, "refresh active predicates on the same sample");
	profiler.AddLocal(*root);
	Require(profiler.Count(profiler.LocalFilters()) == local_count, "new active set affects the estimate");
	Require(profiler.Observations().size() == observation_count &&
	            profiler.Observations()[0].evaluation_ns == local_elapsed,
	        "refresh reuses previously evaluated bitmaps");
	ColumnDataCollection empty(BufferAllocator::Get(ctx), types);
	SampleFilterProfiler empty_profiler(ctx, empty, {0, 1, 2, 3, 4});
	empty_profiler.AddLocal(always);
	Require(empty_profiler.Count({0}) == 0, "empty sample");
	CheckLocalCost(empty_profiler, {0});
	std::cout << "Passed filter-profile bitmap, correlation, NULL, OR, cache, binding and empty-sample checks\n";
}
