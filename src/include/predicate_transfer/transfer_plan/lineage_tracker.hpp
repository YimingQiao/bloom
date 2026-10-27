#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class EdgeInfo;

//! Tracks result lineage (L) and satisfied constraints (C) for each complete
//! join key. Immutable tokens identify exact projections or Bloom acceptance
//! predicates. C includes received filters; L changes when the existing
//! cardinality estimate predicts that their batch reduces the rowset.
class LineageTracker {
public:
	using LineageSet = unordered_set<idx_t>;

	struct Snapshot {
		LineageSet lineages;
		//! Ordered source columns identify the physical filter's key and arity.
		vector<ColumnBinding> key_columns;

		//! Content-stable within one tracker lifetime, not a persistent cache key.
		size_t StableHash() const;
	};

	void Clear();
	void SeedKey(const vector<ColumnBinding> &key);
	bool EdgeCarriesNewInfo(const EdgeInfo &edge) const;
	Snapshot MakeSnapshot(const vector<ColumnBinding> &key) const;

	//! An approximate filter proves its acceptance predicate, not its source's
	//! exact domain. Give that predicate a separate immutable identity.
	Snapshot NewPredicate(const vector<ColumnBinding> &key);

	//! Record newly attached filters using the batch cardinality estimate.
	//! Cross-key shrinkage appends one projection version shared by L and C.
	void ReceiveFilters(idx_t table_id, const vector<pair<vector<ColumnBinding>, Snapshot>> &filters,
	                    bool estimated_shrink);

	//! Compare tokens; callers must check key compatibility separately.
	static bool IsSubset(const Snapshot &a, const Snapshot &b);

private:
	struct KeyState {
		vector<ColumnBinding> key;
		LineageSet result;
		LineageSet constraints;
	};
	const KeyState &FindKey(const vector<ColumnBinding> &key) const;
	unordered_map<idx_t, vector<KeyState>> keys_;
	idx_t next_token_ = 0;
};

} // namespace duckdb
