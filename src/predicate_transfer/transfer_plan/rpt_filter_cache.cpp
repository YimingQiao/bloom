#include "predicate_transfer/transfer_plan/rpt_filter_cache.hpp"

#include <algorithm>

namespace duckdb {

RPTFilterCache::Key RPTFilterCache::Canonicalise(const LineageTracker::Snapshot &snapshot) {
	Key k;
	k.lineages.assign(snapshot.lineages.begin(), snapshot.lineages.end());
	std::sort(k.lineages.begin(), k.lineages.end());
	// key_columns arrive in the edge's canonical order (ActivateTables sorts the
	// key vector) — keep that order, it is part of the BF's identity.
	k.key_columns.reserve(snapshot.key_columns.size());
	for (auto &c : snapshot.key_columns) {
		k.key_columns.emplace_back(c.table_index.index, c.column_index);
	}
	return k;
}

shared_ptr<RPTFilter> RPTFilterCache::Lookup(const LineageTracker::Snapshot &snapshot) const {
	auto key = Canonicalise(snapshot);
	auto it = cache_.find(key);
	return it == cache_.end() ? nullptr : it->second;
}

void RPTFilterCache::Insert(const LineageTracker::Snapshot &snapshot, shared_ptr<RPTFilter> filter) {
	cache_.emplace(Canonicalise(snapshot), std::move(filter));
}

} // namespace duckdb
