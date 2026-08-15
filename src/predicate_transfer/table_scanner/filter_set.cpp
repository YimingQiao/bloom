#include "predicate_transfer/table_scanner/filter_set.hpp"
#include "predicate_transfer/filter/table_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>
#include <functional>
#include <unordered_map>

namespace duckdb {

void ScannerFilterSet::Add(ColumnBinding binding, shared_ptr<RPTFilter> filter, size_t identity_hash) {
	Add(vector<ColumnBinding> {binding}, std::move(filter), identity_hash);
}

void ScannerFilterSet::Add(const vector<ColumnBinding> &bindings, shared_ptr<RPTFilter> filter, size_t identity_hash) {
	Entry entry;
	entry.bindings = bindings;
	entry.chunk_cols.resize(bindings.size(), DConstants::INVALID_INDEX);
	entry.filter = std::move(filter);
	entry.identity_hash = identity_hash;
	entries_.push_back(std::move(entry));
}

size_t ScannerFilterSet::Fingerprint() const {
	size_t fingerprint = 0;
	for (auto &entry : entries_) {
		size_t hash = entry.identity_hash;
		for (auto &binding : entry.bindings) {
			hash = hash * 1315423911u +
			       (std::hash<idx_t> {}(binding.table_index.index) * 3 + std::hash<idx_t> {}(binding.column_index));
		}
		fingerprint ^= hash;
	}
	return fingerprint;
}

static void CollectGetsByTableIndex(LogicalOperator &op, unordered_map<idx_t, LogicalGet *> &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		result.emplace(get.table_index.index, &get);
	}
	for (auto &child : op.children) {
		CollectGetsByTableIndex(*child, result);
	}
}

void ScannerFilterSet::Pushdown(LogicalOperator &plan) {
	if (entries_.empty()) {
		return;
	}

	unordered_map<idx_t, LogicalGet *> gets_by_table_index;
	CollectGetsByTableIndex(plan, gets_by_table_index);
	if (gets_by_table_index.empty()) {
		return;
	}

	auto pushed_end = std::remove_if(entries_.begin(), entries_.end(), [&](Entry &entry) {
		if (!entry.filter || entry.bindings.size() != 1) {
			return false;
		}
		auto &binding = entry.bindings.front();
		auto get_it = gets_by_table_index.find(binding.table_index.index);
		if (get_it == gets_by_table_index.end()) {
			return false;
		}
		auto &get = *get_it->second;
		auto &column_ids = get.GetMutableColumnIds();
		idx_t projection_position = binding.column_index;
		if (projection_position >= column_ids.size() || column_ids[projection_position].IsVirtualColumn()) {
			return false;
		}
		auto storage_column = column_ids[projection_position].GetPrimaryIndex();
		auto key_type =
		    storage_column < get.returned_types.size() ? get.returned_types[storage_column] : LogicalType::BIGINT;
		get.table_filters.PushFilter(ProjectionIndex(projection_position),
		                             RPTTableFilter::MakeOptional(entry.filter, key_type));
		return true;
	});
	entries_.erase(pushed_end, entries_.end());
}

void ScannerFilterSet::Resolve(const vector<ColumnBinding> &output_bindings) {
	for (auto &entry : entries_) {
		entry.chunk_cols.clear();
		entry.chunk_cols.reserve(entry.bindings.size());
		for (auto &binding : entry.bindings) {
			idx_t chunk_column = DConstants::INVALID_INDEX;
			for (idx_t column = 0; column < output_bindings.size(); column++) {
				if (output_bindings[column] == binding) {
					chunk_column = column;
					break;
				}
			}
			entry.chunk_cols.push_back(chunk_column);
		}
	}
}

void ScannerFilterSet::Apply(DataChunk &chunk) const {
	if (chunk.size() == 0) {
		return;
	}
	SelectionVector selection(STANDARD_VECTOR_SIZE);
	for (auto &entry : entries_) {
		if (chunk.size() == 0) {
			break;
		}
		if (!entry.filter || entry.chunk_cols.empty()) {
			continue;
		}
		if (std::any_of(entry.chunk_cols.begin(), entry.chunk_cols.end(),
		                [&](idx_t column) { return column >= chunk.ColumnCount(); })) {
			continue;
		}
		size_t count = chunk.size();
		entry.filter->Lookup(chunk, entry.chunk_cols, selection, count);
		if (count < chunk.size()) {
			chunk.Slice(selection, count);
			chunk.Flatten();
		}
	}
}

} // namespace duckdb
