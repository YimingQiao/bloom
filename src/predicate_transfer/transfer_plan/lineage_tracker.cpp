#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"

#include "predicate_transfer/transfer_plan/dag.hpp"
#include "duckdb/common/assert.hpp"

#include <algorithm>

namespace duckdb {

void LineageTracker::Clear() {
	table_to_lineage_.clear();
	per_lineage_.clear();
	per_column_.clear();
}

void LineageTracker::RegisterAliasGroup(const LineageSet &group) {
	if (group.size() < 2) {
		return;
	}
	// Pick the smallest table_id as the canonical lineage_id for this group.
	idx_t canonical = *std::min_element(group.begin(), group.end());
	// Ordering contract: must run before InitTable/SeedColumn for these tables;
	// otherwise those seedings already landed in per_lineage_ / per_column_
	// under the wrong (non-canonical) keys and this registration can't fix it.
	for (auto id : group) {
		D_ASSERT(per_lineage_.find(id) == per_lineage_.end());
		if (id != canonical) {
			table_to_lineage_[id] = canonical;
		}
	}
}

idx_t LineageTracker::LineageOf(idx_t table_id) const {
	auto it = table_to_lineage_.find(table_id);
	return it == table_to_lineage_.end() ? table_id : it->second;
}

ColumnBinding LineageTracker::Canon(const ColumnBinding &col) const {
	return ColumnBinding(TableIndex(LineageOf(col.table_index.index)), col.column_index);
}

void LineageTracker::InitTable(idx_t table_id) {
	idx_t L = LineageOf(table_id);
	per_lineage_[L] = LineageSet {L};
}

void LineageTracker::SeedColumn(ColumnBinding col) {
	if (!kColumnGranular) {
		return;
	}
	auto canon = Canon(col);
	per_column_[canon].insert(canon);
}

LineageTracker::ColumnSet LineageTracker::UnionColumnLineage(const vector<ColumnBinding> &cols) const {
	ColumnSet out;
	if (!kColumnGranular) {
		return out;
	}
	for (auto &b : cols) {
		auto it = per_column_.find(Canon(b));
		D_ASSERT(it != per_column_.end());
		out.insert(it->second.begin(), it->second.end());
	}
	return out;
}

bool LineageTracker::EdgeCarriesNewInfo(const EdgeInfo &edge) const {
	if (kColumnGranular) {
		// BF is transmitted pairwise src_i → dst_i — check each pair.
		D_ASSERT(edge.left_bindings.size() == edge.right_bindings.size());
		for (idx_t i = 0; i < edge.left_bindings.size(); ++i) {
			auto src_it = per_column_.find(Canon(edge.left_bindings[i]));
			if (src_it == per_column_.end()) {
				continue;
			}
			auto dst_it = per_column_.find(Canon(edge.right_bindings[i]));
			if (dst_it == per_column_.end()) {
				return true;
			}
			for (auto &elem : src_it->second) {
				if (!dst_it->second.count(elem)) {
					return true;
				}
			}
		}
		return false;
	}
	// Lineage-level: any contributing lineage on the source missing from dst?
	D_ASSERT(!edge.left_bindings.empty());
	D_ASSERT(!edge.right_bindings.empty());
	idx_t src_L = LineageOf(edge.left_bindings.front().table_index.index);
	idx_t dst_L = LineageOf(edge.right_bindings.front().table_index.index);
	if (src_L == dst_L) {
		return false;
	}
	auto src_it = per_lineage_.find(src_L);
	if (src_it == per_lineage_.end()) {
		return false;
	}
	auto dst_it = per_lineage_.find(dst_L);
	if (dst_it == per_lineage_.end()) {
		return true;
	}
	for (auto id : src_it->second) {
		if (!dst_it->second.count(id)) {
			return true;
		}
	}
	return false;
}

void LineageTracker::Propagate(const GraphEdge &edge) {
	idx_t src_L = LineageOf(edge.source);
	idx_t dst_L = LineageOf(edge.destination);
	auto &dst_lineage = per_lineage_[dst_L];
	dst_lineage.insert(src_L);
	auto src_it = per_lineage_.find(src_L);
	if (src_it != per_lineage_.end()) {
		dst_lineage.insert(src_it->second.begin(), src_it->second.end());
	}

	if (kColumnGranular) {
		ColumnSet src_union = UnionColumnLineage(edge.source_columns);
		for (auto &dst_col : edge.dest_columns) {
			auto &dst_set = per_column_[Canon(dst_col)];
			dst_set.insert(src_union.begin(), src_union.end());
		}
	}
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

LineageTracker::Snapshot LineageTracker::MakeSnapshot(idx_t source_table_id,
                                                      const vector<ColumnBinding> &source_cols) const {
	Snapshot s;
	idx_t L = LineageOf(source_table_id);
	auto it = per_lineage_.find(L);
	if (it != per_lineage_.end()) {
		s.lineages = it->second;
	} else {
		s.lineages.insert(L);
	}
	if (kColumnGranular) {
		s.columns = UnionColumnLineage(source_cols);
	}
	s.key_columns.reserve(source_cols.size());
	for (auto &col : source_cols) {
		s.key_columns.push_back(Canon(col));
	}
	return s;
}

const LineageTracker::LineageSet &LineageTracker::Lineage(idx_t table_id) const {
	static const LineageSet empty;
	auto it = per_lineage_.find(LineageOf(table_id));
	return it == per_lineage_.end() ? empty : it->second;
}

bool LineageTracker::IsSubset(const Snapshot &a, const Snapshot &b) {
	if (kColumnGranular) {
		for (auto &e : a.columns) {
			if (!b.columns.count(e)) {
				return false;
			}
		}
		return true;
	}
	for (auto &id : a.lineages) {
		if (!b.lineages.count(id)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
