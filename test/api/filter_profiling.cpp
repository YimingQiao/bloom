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
// Check the chosen order and cost against all permutations using direct row evaluation.
static void CheckFilterCost(const SampleFilterProfiler &profiler, vector<idx_t> active) {
	auto cost = [&](const vector<idx_t> &order) {
		double total = 0;
		for (idx_t row = 0; row < profiler.SampleSize(); row++) {
			for (auto id : order) {
				const auto &obs = profiler.Observations()[id];
				total += obs.evaluation_ns / profiler.SampleSize();
				if (!(obs.passes[row / 64] & (uint64_t(1) << (row % 64)))) {
					break;
				}
			}
		}
		return total;
	};
	std::sort(active.begin(), active.end());
	double best = std::numeric_limits<double>::infinity();
	do {
		best = std::min(best, cost(active));
	} while (std::next_permutation(active.begin(), active.end()));
	vector<idx_t> order;
	auto estimate = profiler.EstimateEvaluationNS(active, &order);
	Require(std::abs(estimate - best) <= 1e-9 * (1 + best), "minimum over all sample orders");
	Require(std::abs(estimate - cost(order)) <= 1e-9 * (1 + best), "chosen order attains minimum");
	if (profiler.SampleSize()) {
		std::sort(order.begin(), order.end());
		Require(order == active, "chosen order contains every pending filter");
	}
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
	Require(profiler.EstimateLocalEvaluationNS({id}) == 0, "received filters contribute no local predicate cost");
	auto elapsed = profiler.Observations()[id].evaluation_ns;
	Require(profiler.AddRuntime(entry) == id && profiler.Observations()[id].evaluation_ns == elapsed,
	        "cached immutable filter");
	auto active = profiler.LocalFilters();
	active.push_back(id);
	Require(profiler.Count(active) == joint_count, "intersection equals full joint evaluation");
	CheckFilterCost(profiler, {});
	CheckFilterCost(profiler, {id});
	CheckFilterCost(profiler, {2});
	CheckFilterCost(profiler, profiler.LocalFilters());
	CheckFilterCost(profiler, active);
	entry.chunk_cols = {4};
	auto second = profiler.AddRuntime(entry);
	Require(second != id && profiler.Count({second}) == second_count, "same filter different bound column");
	active.push_back(second);
	CheckFilterCost(profiler, {id, 0, second});
	CheckFilterCost(profiler, active);
	Require(profiler.Count(profiler.LocalFilters()) == local_count, "runtime probing preserves raw sample");
	BoundConstantExpression never(Value(LogicalType::BOOLEAN));
	profiler.AddLocal(never);
	Require(profiler.Count(profiler.LocalFilters()) == 0, "NULL predicate rejects every row");
	CheckFilterCost(profiler, profiler.LocalFilters());
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
	CheckFilterCost(empty_profiler, {0});
	std::cout << "Passed filter-profile bitmap, correlation, NULL, OR, cache, binding and empty-sample checks\n";
}
