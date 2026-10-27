#include "predicate_transfer/table_scanner/filter_set.hpp"
#include "predicate_transfer/filter/table_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>

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

LogicalGet *ScannerFilterSet::GetPushdownScan(LogicalOperator &plan) {
	auto *leaf = &plan;
	// Left-input filtering preserves a MARK join's marks (including NULLs).
	// Stop at value/layout boundaries such as TOP_N/LIMIT or projections.
	while (true) {
		if (leaf->type == LogicalOperatorType::LOGICAL_FILTER && leaf->children.size() == 1) {
			leaf = leaf->children[0].get();
		} else if (leaf->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		           leaf->Cast<LogicalComparisonJoin>().join_type == JoinType::MARK) {
			leaf = leaf->children[0].get();
		} else {
			break;
		}
	}
	return leaf->type == LogicalOperatorType::LOGICAL_GET ? &leaf->Cast<LogicalGet>() : nullptr;
}

const LogicalType *ScannerFilterSet::GetPushdownType(const LogicalGet &get, ColumnBinding binding) {
	if (binding.table_index != get.table_index) {
		return nullptr;
	}
	auto &column_ids = get.GetColumnIds();
	idx_t pos = binding.column_index;
	if (pos >= column_ids.size() || column_ids[pos].IsVirtualColumn()) {
		return nullptr;
	}
	return &get.GetColumnType(column_ids[pos]);
}

void ScannerFilterSet::Pushdown(LogicalOperator &plan) {
	if (entries_.empty()) {
		return;
	}

	auto scan = GetPushdownScan(plan);
	if (!scan) {
		return;
	}

	auto pushed_end = std::remove_if(entries_.begin(), entries_.end(), [&](Entry &entry) {
		if (!entry.filter || entry.bindings.size() != 1) {
			return false;
		}
		auto &binding = entry.bindings.front();
		auto key_type = GetPushdownType(*scan, binding);
		if (!key_type) {
			return false;
		}
		scan->table_filters.PushFilter(binding.column_index, RPTTableFilter::MakeOptional(entry.filter, *key_type));
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
