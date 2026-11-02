#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"
#include "predicate_transfer/transfer_plan/transfer_types.hpp"
#include "duckdb/common/assert.hpp"
#include <algorithm>

namespace duckdb {
void LineageTracker::Clear() {
	tables_.clear();
	implications_.clear();
	next_symbol_ = 0;
}

void LineageTracker::SeedKey(const vector<ColumnBinding> &key) {
	D_ASSERT(!key.empty());
	const auto table_id = key.front().table_index.index;
	auto entry = tables_.find(table_id);
	if (entry == tables_.end()) {
		TableState state;
		state.symbol = ++next_symbol_;
		entry = tables_.emplace(table_id, std::move(state)).first;
	}
	auto &table = entry->second;
	for (auto &column : key) {
		D_ASSERT(column.table_index.index == table_id);
		if (std::find(table.columns.begin(), table.columns.end(), column) == table.columns.end()) {
			D_ASSERT(!table.result);
			table.columns.push_back(column);
		}
	}
	std::sort(table.columns.begin(), table.columns.end(),
	          [](const ColumnBinding &a, const ColumnBinding &b) { return a.column_index < b.column_index; });
}

LineageTracker::TableState &LineageTracker::FindTable(idx_t table_id) const {
	auto &table = tables_.at(table_id);
	if (!table.result) {
		table.result = LineageExpression::Relation(table.symbol, table.columns.size());
		table.constraints = table.result;
	}
	return table;
}

vector<idx_t> LineageTracker::Positions(const TableState &table, const vector<ColumnBinding> &key) {
	vector<idx_t> positions;
	for (auto &column : key) {
		auto found = std::find(table.columns.begin(), table.columns.end(), column);
		D_ASSERT(found != table.columns.end());
		positions.push_back(found - table.columns.begin());
	}
	return positions;
}

LineageTracker::Snapshot LineageTracker::Project(const vector<ColumnBinding> &key, bool constraints) const {
	D_ASSERT(!key.empty());
	auto &table = FindTable(key.front().table_index.index);
	auto positions = Positions(table, key);
	string cache_key;
	for (auto position : positions) {
		cache_key += std::to_string(position) + ",";
	}
	// Identical L and C can share outgoing projections as well as their body.
	auto &cache =
	    constraints && table.constraints != table.result ? table.constraint_projections : table.result_projections;
	auto entry = cache.find(cache_key);
	if (entry == cache.end()) {
		auto &expression = constraints ? table.constraints : table.result;
		entry = cache.emplace(cache_key, expression->Project(positions)).first;
	}
	return {entry->second, key};
}

LineageTracker::Snapshot LineageTracker::MakeSnapshot(const vector<ColumnBinding> &key) const {
	return Project(key, false);
}
LineageTracker::Snapshot LineageTracker::MakeConstraintSnapshot(const vector<ColumnBinding> &key) const {
	return Project(key, true);
}
bool LineageTracker::Implies(const Snapshot &premise, const Snapshot &condition) {
	D_ASSERT(premise.expression && condition.expression);
	return LineageExpression::Implies(*premise.expression, *condition.expression);
}
bool LineageTracker::EdgeCarriesNewInfo(const EdgeInfo &edge) const {
	return !ImpliesCached(MakeConstraintSnapshot(edge.right_bindings), MakeSnapshot(edge.left_bindings));
}
bool LineageTracker::ImpliesCached(const Snapshot &premise, const Snapshot &condition) const {
	ImplicationKey key {premise.expression, condition.expression};
	auto found = implications_.find(key);
	if (found != implications_.end()) {
		return found->second;
	}
	const auto result = Implies(premise, condition);
	// Bound retained snapshots. Eviction only causes recomputation, never a
	// different inference result. Clear() releases everything between queries.
	if (implications_.size() >= 256) {
		implications_.clear();
	}
	implications_.emplace(std::move(key), result);
	return result;
}
size_t LineageTracker::Snapshot::StableHash() const {
	D_ASSERT(expression);
	size_t hash = expression->SignatureHash();
	for (auto &column : key_columns) {
		hash = hash * 1315423911u + std::hash<idx_t> {}(column.table_index.index);
		hash = hash * 1315423911u + std::hash<idx_t> {}(column.column_index.GetIndexUnsafe());
	}
	return hash;
}
LineageTracker::Snapshot LineageTracker::NewPredicate(const vector<ColumnBinding> &key) {
	return {LineageExpression::Relation(++next_symbol_, key.size()), key};
}
void LineageTracker::ReceiveFilters(idx_t table_id, const vector<pair<vector<ColumnBinding>, Snapshot>> &filters,
                                    bool estimated_shrink) {
	auto &table = FindTable(table_id);
	for (auto &filter : filters) {
		auto positions = Positions(table, filter.first);
		const bool shared_result = table.result == table.constraints;
		auto constraints = LineageExpression::Conjoin(table.constraints, filter.second.expression, positions);
		if (constraints != table.constraints) {
			table.constraints = std::move(constraints);
			table.constraint_projections.clear();
		}
		if (estimated_shrink) {
			auto result = shared_result ? table.constraints
			                            : LineageExpression::Conjoin(table.result, filter.second.expression, positions);
			if (result != table.result) {
				table.result = std::move(result);
				table.result_projections.clear();
			}
		}
	}
}
} // namespace duckdb
