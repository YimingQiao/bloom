#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"

namespace duckdb {

class RPTFilter;

//! Cross-flooding-round cache for built RPT transfer filters.
//!
//! The per-step cache used to key by source `ColumnBinding` (table_index,
//! column_index). Two aliases of a lifted CTE — same underlying data,
//! different table_index — therefore missed it and rebuilt byte-identical BFs.
//!
//! RPTFilterCache instead keys by the source's **lineage snapshot**. A snapshot
//! captures both the origin rowsets (post-alias canonicalisation) and the
//! column-level lineage; two sources with equal snapshots have equal
//! post-filter rowsets on the cached columns, so their BFs are interchangeable.
//! Built filters persist across flooding rounds (whole `Optimize` call).
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
