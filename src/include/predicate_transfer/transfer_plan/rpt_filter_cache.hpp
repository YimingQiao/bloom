#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"

namespace duckdb {

class RPTFilter;

//! Reuses transfer filters across rounds within one Optimize call. Cache
//! identity includes immutable lineage tokens and ordered key columns. Key
//! bindings retain table identity, so independently filtered aliases stay distinct.
class RPTFilterCache {
public:
	shared_ptr<RPTFilter> Lookup(const LineageTracker::Snapshot &snapshot) const;
	void Insert(const LineageTracker::Snapshot &snapshot, shared_ptr<RPTFilter> filter);

private:
	struct Key {
		vector<idx_t> lineages;
		//! Key order and arity are part of the physical filter identity.
		vector<pair<idx_t, idx_t>> key_columns;
		bool operator==(const Key &other) const {
			return lineages == other.lineages && key_columns == other.key_columns;
		}
	};
	struct KeyHash {
		size_t operator()(const Key &k) const {
			size_t h = 0;
			for (auto l : k.lineages) {
				h = h * 1315423911u + std::hash<idx_t> {}(l);
			}
			for (auto &c : k.key_columns) {
				h = h * 1315423911u + (std::hash<idx_t> {}(c.first) * 3 + std::hash<idx_t> {}(c.second));
			}
			return h;
		}
	};

	static Key Canonicalise(const LineageTracker::Snapshot &snapshot);

	unordered_map<Key, shared_ptr<RPTFilter>, KeyHash> cache_;
};

} // namespace duckdb
