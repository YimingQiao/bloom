#pragma once

#include "transfer_types.hpp"
#include "dag.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// LineageTracker
//===--------------------------------------------------------------------===//
//! Tracks which lineages have contributed information to which destinations
//! during predicate-transfer flooding.
//!
//! Identity model:
//!   - Each logical table_id is mapped to a "lineage_id". Tables that share
//!     the same underlying data (CDC aliases — bare CHUNK_GETs pointing at
//!     one ColumnDataCollection) map to one canonical lineage_id, so their
//!     lineage state is literally the same slot. Edges inside the alias
//!     group become same-lineage and get pruned at EdgeCarriesNewInfo
//!     without any special casing.
//!   - Non-alias tables trivially map to their own table_id as lineage_id.
//!
//! Two compile-time modes:
//!   - Column granularity (default): per-(lineage_id, column) lineage. Needed
//!     to keep composite-key plans from pruning a legitimate edge as
//!     "already covered" when only one of the join keys has been exchanged.
//!   - Table granularity: per-lineage_id only.
//!
//! Select via -DRPT_LINEAGE_TABLE_GRANULARITY at build time.
class LineageTracker {
public:
	using LineageSet = unordered_set<idx_t>;
	using ColumnSet = unordered_set<ColumnBinding, ColumnBindingHashFunc>;
	// Kept for callers that still spell `TableSet`. Semantically a set of
	// lineage_ids (which for non-aliases equals table_ids).
	using TableSet = LineageSet;

#ifdef RPT_LINEAGE_TABLE_GRANULARITY
	static constexpr bool kColumnGranular = false;
#else
	static constexpr bool kColumnGranular = true;
#endif

	//! Snapshot of lineage at the moment a cascade filter is attached.
	//! - `columns` is populated only in column mode (used for subsumption).
	//! - `lineages` is always populated (used as the oracle cache key).
	//! - `key_columns` is the canonicalised source key columns in edge order:
	//!   it identifies WHICH columns the built BF hashes (and their arity).
	//!   Cache keys must include it — the per-column lineage sets of different
	//!   physical columns can converge (composite transfers smear the union of
	//!   all source columns into every destination column), after which a
	//!   single-column lookup would hit a multi-key BF built on the same data
	//!   (observed on TPC-H q09: supplier probed with 1 of 2 key columns →
	//!   nearly all rows wrongly filtered → empty query result).
	struct Snapshot {
		ColumnSet columns;
		LineageSet lineages;
		vector<ColumnBinding> key_columns;

		//! Content-stable hash (order-insensitive lineages + key columns in
		//! order). Safe as a cross-run cache identity, unlike filter pointers.
		size_t StableHash() const;
	};

	void Clear();

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

	//! Subset check between two snapshots. Picks the column- or lineage-level
	//! representation depending on the compile-time mode.
	static bool IsSubset(const Snapshot &a, const Snapshot &b);

	idx_t LineageOf(idx_t table_id) const;

private:
	ColumnBinding Canon(const ColumnBinding &col) const;
	ColumnSet UnionColumnLineage(const vector<ColumnBinding> &cols) const;

	//! Non-canonical table_ids map to their alias group's canonical id.
	//! Non-aliases have no entry here (LineageOf returns the table_id itself).
	unordered_map<idx_t, idx_t> table_to_lineage_;
	unordered_map<idx_t, LineageSet> per_lineage_;
	unordered_map<ColumnBinding, ColumnSet, ColumnBindingHashFunc> per_column_;
};

} // namespace duckdb
