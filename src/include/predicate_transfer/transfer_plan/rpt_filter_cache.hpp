#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"

namespace duckdb {

class RPTFilter;

//! Reuses transfer filters across rounds within one Optimize call. Cache
//! identity includes the source, immutable lineage snapshot, and ordered key
//! columns. Separate lineage keeps CDC aliases independent; legacy lineage
//! canonicalises aliases so equivalent snapshots can share filters.
class RPTFilterCache {
public:
	shared_ptr<RPTFilter> Lookup(idx_t source_lineage_id, const LineageTracker::Snapshot &snapshot) const;
	void Insert(idx_t source_lineage_id, const LineageTracker::Snapshot &snapshot, shared_ptr<RPTFilter> filter);

private:
	struct Key {
		idx_t source_lineage_id;
		vector<idx_t> lineages;
		vector<pair<idx_t, idx_t>> columns;
		//! Canonical source key columns, in edge order — identifies WHICH
		//! columns (and how many) the cached BF hashes. Per-column lineage
		//! sets of different columns can converge under composite transfers,
		//! so `columns` alone cannot tell a 2-key BF from a 1-key BF over the
		//! same data (TPC-H q09: a single-column probe of a cached composite
		//! BF wrongly dropped ~all rows → empty result).
		vector<pair<idx_t, idx_t>> key_columns;
		bool operator==(const Key &other) const {
			return source_lineage_id == other.source_lineage_id && lineages == other.lineages &&
			       columns == other.columns && key_columns == other.key_columns;
		}
	};
	struct KeyHash {
		size_t operator()(const Key &k) const {
			size_t h = std::hash<idx_t> {}(k.source_lineage_id);
			for (auto l : k.lineages) {
				h = h * 1315423911u + std::hash<idx_t> {}(l);
			}
			for (auto &c : k.columns) {
				h = h * 1315423911u + (std::hash<idx_t> {}(c.first) ^ std::hash<idx_t> {}(c.second));
			}
			for (auto &c : k.key_columns) {
				h = h * 1315423911u + (std::hash<idx_t> {}(c.first) * 3 + std::hash<idx_t> {}(c.second));
			}
			return h;
		}
	};

	static Key Canonicalise(idx_t source_lineage_id, const LineageTracker::Snapshot &snapshot);

	unordered_map<Key, shared_ptr<RPTFilter>, KeyHash> cache_;
};

} // namespace duckdb
