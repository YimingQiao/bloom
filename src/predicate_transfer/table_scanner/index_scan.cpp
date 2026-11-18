#include "predicate_transfer/table_scanner/index_scan.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/index_lock.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <algorithm>
#include <iostream>

namespace duckdb {

static constexpr idx_t RPT_INDEX_LIMIT = 2048;

//! Keep exact probe keys with native scan binding; all original predicates remain
//! above Fetch and still filter transaction-local rows.
struct RPTIndexScanBindData : TableScanBindData {
	RPTIndexScanBindData(const TableScanBindData &source, Identifier index_name_p, vector<Value> keys_p)
	    : TableScanBindData(source.table), index_name(std::move(index_name_p)), keys(std::move(keys_p)) {
		column_ids = source.column_ids;
		is_index_scan = source.is_index_scan;
		is_create_index = source.is_create_index;
		order_options = source.order_options ? make_uniq<RowGroupOrderOptions>(*source.order_options) : nullptr;
		partitions_to_scan =
		    source.partitions_to_scan ? make_uniq<unordered_set<idx_t>>(*source.partitions_to_scan) : nullptr;
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RPTIndexScanBindData>(*this, index_name, keys);
	}

	bool Equals(const FunctionData &other) const override {
		auto candidate = dynamic_cast<const RPTIndexScanBindData *>(&other);
		return candidate && &candidate->table == &table && candidate->index_name == index_name &&
		       candidate->keys == keys;
	}

	Identifier index_name;
	vector<Value> keys;
};

unique_ptr<GlobalTableFunctionState> InitializeRPTIndexScan(ClientContext &context, TableFunctionInitInput &input,
                                                            const Identifier &index_name, const vector<Value> &keys) {
	auto &bind = input.bind_data->Cast<TableScanBindData>();
	auto &storage = bind.table.Cast<DuckTableEntry>().GetStorage();
	if (input.sample_options || bind.partitions_to_scan || bind.is_create_index) {
		return TableScanFunction::GetFunction().init_global(context, input);
	}
	// Preserve the native vacuum/read pairing and include index deltas. Native
	// Fetch handles visibility; native scan state also visits local storage.
	auto vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();
	auto &info = storage.GetDataTableInfo();
	info->BindIndexes(context, ART::TYPE_NAME);
	auto entry = info->GetIndexes().FindEntry(index_name);
	if (!entry) {
		return TableScanFunction::GetFunction().init_global(context, input);
	}
	auto handle = entry->GetReadHandle<ART>();
	vector<row_t> row_ids;
	ArenaAllocator key_arena(Allocator::Get(context)), output_arena(Allocator::Get(context));
	unsafe_vector<ARTKey> output_keys(STANDARD_VECTOR_SIZE), output_ids(STANDARD_VECTOR_SIZE);
	KeyRowIdOutput output(output_arena, output_keys, output_ids, STANDARD_VECTOR_SIZE);
	auto probe = [&](const ART &art) {
		IndexLock lock(art);
		for (auto value : keys) {
			key_arena.Reset();
			auto key = ARTKey::CreateKey(key_arena, value, art.storage_version);
			auto leaf = ARTOperator::Lookup(art, art.tree, key, 0);
			if (!leaf) {
				continue;
			}
			Iterator iterator(art);
			for (idx_t b = 0; b < key.len; b++) {
				iterator.current_key.Push(key[b]);
			}
			iterator.FindMinimum(leaf.Get());
			ARTScanResult result;
			do {
				output.Reset();
				result = iterator.Scan(ARTKey(), output, false);
				for (idx_t i = 0; i < output.Count(); i++) {
					row_ids.push_back(output_ids[i].GetRowId());
				}
			} while (result == ARTScanResult::PAUSED);
		}
	};
	probe(*handle.operator->());
	for (auto delta : {IndexDeltaType::DELETED_ROWS_IN_USE, IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT}) {
		auto delta_index = handle.FindDelta(delta);
		if (delta_index) {
			probe(*delta_index);
		}
	}
	std::sort(row_ids.begin(), row_ids.end());
	row_ids.erase(std::unique(row_ids.begin(), row_ids.end()), row_ids.end());
	Value log;
	if (context.TryGetCurrentSetting("bloom_log_transfer_steps", log) && log.GetValue<bool>()) {
		std::cerr << "      [BloomIndexScan] table=" << bind.table.name << " keys=" << keys.size()
		          << " candidate_rows=" << row_ids.size() << "\n";
	}
	return TableScanFunction::InitIndexScan(context, input, row_ids, std::move(vacuum_lock));
}

static unique_ptr<GlobalTableFunctionState> RPTIndexScanInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<RPTIndexScanBindData>();
	return InitializeRPTIndexScan(context, input, bind.index_name, bind.keys);
}

bool TryInstallRPTIndexScan(ClientContext &context, LogicalOperator &plan,
                            const vector<ScannerFilterSet::Entry> &filters, optional_idx estimated_rows) {
	if (!estimated_rows.IsValid() || estimated_rows.GetIndex() >= RPT_INDEX_LIMIT) {
		return false;
	}
	auto get = ScannerFilterSet::GetPushdownScan(plan);
	if (!get || !get->GetTable() || !get->GetTable()->IsDuckTable() || get->table_filters.HasMultiColumnFilters() ||
	    get->ordinality_idx.IsValid()) {
		return false;
	}
	auto native = dynamic_cast<TableScanBindData *>(get->bind_data.get());
	if (!native || native->is_create_index || native->partitions_to_scan || native->order_options) {
		return false;
	}
	auto &table = native->table.Cast<DuckTableEntry>();
	auto &info = table.GetStorage().GetDataTableInfo();
	if (info->GetIndexes().Empty()) {
		return false;
	}
	info->BindIndexes(context, ART::TYPE_NAME);
	for (auto &filter : filters) {
		if (filter.bindings.size() != 1 || !filter.filter || !filter.filter->IsValid()) {
			continue;
		}
		auto prefix = dynamic_cast<DuckDBPrefixRangeFilterAdapter *>(filter.filter.get());
		if (!prefix) {
			continue;
		}
		auto type = ScannerFilterSet::GetPushdownType(*get, filter.bindings.front());
		if (!type || !type->IsIntegral()) {
			continue;
		}
		auto storage_column = table.GetStorageIndex(get->GetColumnIds()[filter.bindings.front().column_index]);
		for (auto entry : info->GetIndexes().IndexEntries()) {
			if (entry->GetBindState() != IndexBindState::BOUND || entry->GetIndexType() != ART::TYPE_NAME) {
				continue;
			}
			auto art = entry->GetReadHandle<ART>();
			auto columns = art->GetColumnIds();
			if (columns.size() != 1 || columns[0] != storage_column.GetPrimaryIndex() ||
			    art->UnboundExpressionCount() != 1 ||
			    art->CopyUnboundExpression(0)->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			vector<Value> keys;
			if (!prefix->GetPrefixRangeFilter().TryGetExactValues(keys, RPT_INDEX_LIMIT - 1) || keys.empty()) {
				continue;
			}
			for (auto &key : keys) {
				key = key.DefaultCastAs(*type);
			}
			get->bind_data = make_uniq<RPTIndexScanBindData>(*native, art->GetIndexName(), std::move(keys));
			get->function.init_global = RPTIndexScanInit;
			// Native index Fetch does not evaluate table filters on stored rows.
			// Have the physical planner place every predicate above the scan.
			get->function.supports_pushdown_type = [](const FunctionData &, idx_t) {
				return false;
			};
			Value log;
			if (context.TryGetCurrentSetting("bloom_log_transfer_steps", log) && log.GetValue<bool>()) {
				std::cerr << "      [BloomIndexSelect] table=" << table.name
				          << " estimated_rows=" << estimated_rows.GetIndex() << "\n";
			}
			return true;
		}
	}
	return false;
}

} // namespace duckdb
