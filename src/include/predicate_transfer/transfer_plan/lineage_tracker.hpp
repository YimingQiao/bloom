#pragma once

#include "transfer_types.hpp"
#include "dag.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// LineageTracker
//===--------------------------------------------------------------------===//
//! Tracks result lineage (L) and satisfied constraints (C) for each complete
//! join key. Immutable tokens identify exact projections or Bloom acceptance
//! predicates. Receiving a filter does not change C; ApplyKeyFilters commits
//! feedback after the filter has actually run.
//!
//! The optional legacy mode tracks contributing columns (or tables with
//! BLOOM_LINEAGE_TABLE_GRANULARITY) and shares state across CDC aliases.
class LineageTracker {
public:
	using LineageSet = unordered_set<idx_t>;
	using ColumnSet = unordered_set<ColumnBinding, ColumnBindingHashFunc>;

#ifdef BLOOM_LINEAGE_TABLE_GRANULARITY
	static constexpr bool kColumnGranular = false;
#else
	static constexpr bool kColumnGranular = true;
#endif

	//! Snapshot of lineage at the moment a cascade filter is attached.
	//! - `columns` is populated only in column mode (used for subsumption).
	//! - `lineages` is always populated (used as the oracle cache key); in
	//!   runtime key mode it holds immutable predicate tokens instead of table IDs.
	//! - `key_columns` is the canonicalised source key columns in edge order:
	//!   it identifies WHICH columns the built BF hashes (and their arity).
	//!   Cache keys must include it — the per-column lineage sets of different
	//!   physical columns can converge (composite transfers smear the union of
	//!   all source columns into every destination column), after which a
	//!   single-column lookup would hit a multi-key BF built on the same data
	//!   (observed on TPC-H q09: supplier probed with 1 of 2 key columns →
	//!   nearly all rows wrongly filtered → empty query result).
	struct Snapshot {
		bool key_based = false;
		ColumnSet columns;
		LineageSet lineages;
		vector<ColumnBinding> key_columns;

		//! Content-stable hash: order-insensitive tokens and ordered key columns.
		//! Tokens belong to one tracker lifetime; this is not a persistent cache key.
		size_t StableHash() const;
	};

	void Clear();
	void EnableKeyLineage() {
		key_based_ = true;
	}
	void SeedKey(const vector<ColumnBinding> &key);
	//! Commit only filters actually evaluated on these rows. A batch containing
	//! a cross-key predicate conservatively creates a new projection token.
	void ApplyKeyFilters(idx_t table_id, const vector<pair<vector<ColumnBinding>, Snapshot>> &filters, bool shrunk,
	                     bool initial);
	//! An approximate filter proves its acceptance predicate, not its source's
	//! exact domain. Give that predicate a separate immutable identity.
	Snapshot NewPredicate(const vector<ColumnBinding> &key);

	//! Declare a set of table ids as aliases — all members share the same
	//! underlying data. Internally assigns one canonical lineage_id to the
	//! group so all per-table/per-column bookkeeping collapses to a single
	//! slot. Must be called BEFORE any InitTable / SeedColumn for the
	//! involved tables. Idempotent.
	void RegisterAliasGroup(const LineageSet &group);

	//! Seed a table's lineage to {LineageOf(table_id)}. Must be called once
	//! per table before any Propagate/Snapshot referencing it.
	void InitTable(idx_t table_id);

	//! Seed a join-key column with self-lineage (canonical, so alias peers
	//! share the slot). No-op in table mode.
	void SeedColumn(ColumnBinding col);

	//! Does sending `edge` add anything new to the destination?
	bool EdgeCarriesNewInfo(const EdgeInfo &edge) const;

	//! Propagate a transfer-plan edge: update destination lineage from source.
	void Propagate(const GraphEdge &edge);

	//! Snapshot lineage at cascade-filter attach time. `source_table_id` is
	//! the table being materialized; `source_cols` are the edge's source
	//! bindings (for column-mode subsumption).
	Snapshot MakeSnapshot(idx_t source_table_id, const vector<ColumnBinding> &source_cols) const;

	//! Return the lineage set for `table_id`, or an empty set if none.
	const LineageSet &Lineage(idx_t table_id) const;

	//! Compare immutable tokens in separate mode, or contributing columns/tables
	//! in legacy mode. Callers must check key compatibility separately.
	static bool IsSubset(const Snapshot &a, const Snapshot &b);

	idx_t LineageOf(idx_t table_id) const;

private:
	struct KeyState {
		vector<ColumnBinding> key;
		LineageSet result;
		LineageSet constraints;
	};
	const KeyState &FindKey(const vector<ColumnBinding> &key) const;
	unordered_map<idx_t, vector<KeyState>> keys_;
	idx_t next_token_ = 0;
	bool key_based_ = false;
	ColumnBinding Canon(const ColumnBinding &col) const;
	ColumnSet UnionColumnLineage(const vector<ColumnBinding> &cols) const;

	//! Non-canonical table_ids map to their alias group's canonical id.
	//! Non-aliases have no entry here (LineageOf returns the table_id itself).
	unordered_map<idx_t, idx_t> table_to_lineage_;
	unordered_map<idx_t, LineageSet> per_lineage_;
	//! Canonical join-key columns belonging to each lineage. Populated by
	//! SeedColumn and used to mark every key when the table rowset changes.
	unordered_map<idx_t, ColumnSet> join_columns_;
	unordered_map<ColumnBinding, ColumnSet, ColumnBindingHashFunc> per_column_;
};

} // namespace duckdb
