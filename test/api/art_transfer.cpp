// Standalone integration check; see test/api/README.md for the build command.
#include "duckdb.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"
#include "predicate_transfer/table_scanner/index_scan.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/client_context.hpp"
#include <iostream>
#include <stdexcept>
using namespace duckdb;
static void Require(bool condition, const char *message) {
	if (!condition)
		throw std::runtime_error(message);
}
int main() {
	DuckDB db(nullptr);
	Connection con(db);
	auto sql = [&](const string &query) {
		auto result = con.Query(query);
		result->ThrowIfError();
	};
	sql("SET enable_bloom=false");
	sql("CREATE TABLE target AS SELECT (i-4096)::INTEGER k,i::BIGINT p FROM range(10000) t(i)");
	sql("CREATE INDEX target_k ON target(k)");
	sql("CREATE TABLE no_index AS SELECT * FROM target");
	sql("CREATE TABLE expression_index AS SELECT * FROM target");
	sql("CREATE INDEX expr_k ON expression_index((k+1))");
	sql("CREATE TABLE composite_index AS SELECT * FROM target");
	sql("CREATE INDEX compound_k ON composite_index(k,p)");
	sql("BEGIN");
	auto &ctx = *con.context;
	idx_t checks = 0;
	for (idx_t n : {idx_t(1), idx_t(32), idx_t(2047), idx_t(2048)}) {
		auto filter = make_shared_ptr<DuckDBPrefixRangeFilterAdapter>(ctx, LogicalType::INTEGER, -4096, 4096, n);
		DataChunk keys;
		keys.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		{
			auto writer = FlatVector::Writer<int32_t>(keys.data[0], n);
			for (idx_t i = 0; i < n; i++)
				writer.WriteValue(int32_t(i) - 4096);
		}
		keys.CheckCardinality(n);
		filter->Insert(keys, {0});
		filter->SetValid();
		vector<Value> values;
		Require(filter->GetPrefixRangeFilter().TryGetExactValues(values, 2047) == (n < 2048), "enumeration limit");
		if (n < 2048) {
			Require(values.size() == n, "enumerated cardinality");
			for (idx_t i = 0; i < n; i++)
				Require(values[i].GetValue<int32_t>() == int32_t(i) - 4096, "signed exact key");
		}
		for (auto name : {"target", "no_index", "expression_index", "composite_index"}) {
			auto &table = Catalog::GetEntry<DuckTableEntry>(ctx, QualifiedName("", "main", name));
			for (auto estimate : {optional_idx(2047), optional_idx(2048), optional_idx()}) {
				LogicalGet get(TableIndex(10), BoundTableFunction(TableScanFunction::GetFunction()),
				               make_uniq<TableScanBindData>(table), {LogicalType::INTEGER, LogicalType::BIGINT},
				               {Identifier("k"), Identifier("p")});
				get.AddColumnId(0);
				get.AddColumnId(1);
				ScannerFilterSet filters;
				filters.Add(ColumnBinding(TableIndex(10), ProjectionIndex(0)), filter, 0);
				bool selected = TryInstallRPTIndexScan(ctx, get, filters.Entries(), estimate);
				bool expected =
				    string(name) == "target" && n < 2048 && estimate.IsValid() && estimate.GetIndex() < 2048;
				Require(selected == expected, "strict threshold or index eligibility");
				if (selected) {
					auto copy = get.bind_data->Copy();
					Require(copy->Equals(*get.bind_data), "copied probe keys");
				}
				checks++;
			}
		}
	}
	auto coarse = PrefixRangeFilter::CreatePrefixRangeFilter(LogicalType::INTEGER);
	coarse->Initialize(ctx, 1, Value::INTEGER(0), Value::INTEGER(65535), 64);
	auto state = coarse->InitializeBuildState(ctx);
	Vector key(LogicalType::INTEGER);
	{
		auto writer = FlatVector::Writer<int32_t>(key, 1);
		writer.WriteValue(123);
	}
	coarse->InsertKeys(key, *state);
	coarse->MergeBuildState(*state);
	vector<Value> values;
	Require(!coarse->TryGetExactValues(values, 2047), "coarsened bitmap must not enumerate exact keys");
	sql("ROLLBACK");
	std::cout << "Passed " << checks
	          << " eligibility checks, exact signed-key enumeration and coarsened-filter rejection\n";
}
