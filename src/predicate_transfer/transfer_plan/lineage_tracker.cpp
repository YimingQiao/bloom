#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"

#include "predicate_transfer/transfer_plan/transfer_types.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>

namespace duckdb {

void LineageTracker::Clear() {
	keys_.clear();
	next_token_ = 0;
}

bool LineageTracker::EdgeCarriesNewInfo(const EdgeInfo &edge) const {
	auto &source = FindKey(edge.left_bindings).result;
	auto &dest = FindKey(edge.right_bindings).constraints;
	return std::any_of(source.begin(), source.end(), [&](idx_t token) { return !dest.count(token); });
}

size_t LineageTracker::Snapshot::StableHash() const {
	vector<idx_t> sorted_lineages(lineages.begin(), lineages.end());
	std::sort(sorted_lineages.begin(), sorted_lineages.end());
	size_t h = 0xa5a5a5a5a5a5a5a5ULL;
	for (auto l : sorted_lineages) {
		h = h * 1315423911u + std::hash<idx_t> {}(l);
	}
	for (auto &c : key_columns) {
		h = h * 1315423911u + (std::hash<idx_t> {}(c.table_index.index) * 3 + std::hash<idx_t> {}(c.column_index));
	}
	return h;
}

LineageTracker::Snapshot LineageTracker::MakeSnapshot(const vector<ColumnBinding> &key) const {
	return {FindKey(key).result, key};
}

bool LineageTracker::IsSubset(const Snapshot &a, const Snapshot &b) {
	return std::all_of(a.lineages.begin(), a.lineages.end(), [&](idx_t token) { return b.lineages.count(token); });
}

static vector<ColumnBinding> SortedKey(vector<ColumnBinding> key) {
	std::sort(key.begin(), key.end(), [](const ColumnBinding &a, const ColumnBinding &b) {
		return a.table_index.index < b.table_index.index ||
		       (a.table_index == b.table_index && a.column_index < b.column_index);
	});
	return key;
}

void LineageTracker::SeedKey(const vector<ColumnBinding> &key) {
	D_ASSERT(!key.empty());
	auto sorted = SortedKey(key);
	auto &keys = keys_[key.front().table_index.index];
	for (auto &state : keys) {
		if (state.key == sorted) {
			return;
		}
	}
	auto token = ++next_token_;
	keys.push_back({std::move(sorted), {token}, {token}});
}

const LineageTracker::KeyState &LineageTracker::FindKey(const vector<ColumnBinding> &key) const {
	auto sorted = SortedKey(key);
	for (auto &state : keys_.at(key.front().table_index.index)) {
		if (state.key == sorted) {
			return state;
		}
	}
	throw InternalException("Unregistered complete key in L/C lineage");
}

LineageTracker::Snapshot LineageTracker::NewPredicate(const vector<ColumnBinding> &key) {
	Snapshot snapshot;
	snapshot.key_columns = key;
	snapshot.lineages.insert(++next_token_);
	return snapshot;
}

void LineageTracker::ReceiveFilters(idx_t table_id, const vector<pair<vector<ColumnBinding>, Snapshot>> &filters,
                                    bool estimated_shrink) {
	for (auto &state : keys_[table_id]) {
		bool cross_key = false;
		LineageSet incoming;
		for (auto &filter : filters) {
			bool contained = std::all_of(filter.first.begin(), filter.first.end(), [&](const ColumnBinding &col) {
				return std::find(state.key.begin(), state.key.end(), col) != state.key.end();
			});
			if (contained) {
				incoming.insert(filter.second.lineages.begin(), filter.second.lineages.end());
			} else {
				cross_key = true;
			}
		}
		state.constraints.insert(incoming.begin(), incoming.end());
		if (!estimated_shrink) {
			continue;
		}
		state.result.insert(incoming.begin(), incoming.end());
		if (cross_key) {
			auto token = ++next_token_;
			state.result.insert(token);
			state.constraints.insert(token);
		}
	}
}

} // namespace duckdb
