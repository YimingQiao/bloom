#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "predicate_transfer/transfer_plan/lineage_expression.hpp"

namespace duckdb {
class EdgeInfo;

//! One result expression and one constraint expression per table occurrence,
//! over the union of its join-key attributes. Snapshots are immutable CQs.
class LineageTracker {
public:
	struct Snapshot {
		shared_ptr<const LineageExpression> expression;
		//! Ordered physical columns distinguish filter keys and aliases.
		vector<ColumnBinding> key_columns;
		size_t StableHash() const;
	};

	void Clear();
	//! Register all keys before taking the table's first snapshot.
	void SeedKey(const vector<ColumnBinding> &key);
	bool EdgeCarriesNewInfo(const EdgeInfo &edge) const;
	Snapshot MakeSnapshot(const vector<ColumnBinding> &key) const;
	Snapshot MakeConstraintSnapshot(const vector<ColumnBinding> &key) const;
	//! A Bloom acceptance predicate is distinct from its exact source domain.
	Snapshot NewPredicate(const vector<ColumnBinding> &key);
	void ReceiveFilters(idx_t table_id, const vector<pair<vector<ColumnBinding>, Snapshot>> &filters,
	                    bool estimated_shrink);
	static bool Implies(const Snapshot &premise, const Snapshot &condition);

private:
	struct TableState {
		idx_t symbol;
		vector<ColumnBinding> columns;
		shared_ptr<const LineageExpression> result;
		shared_ptr<const LineageExpression> constraints;
		unordered_map<string, shared_ptr<const LineageExpression>> result_projections;
		unordered_map<string, shared_ptr<const LineageExpression>> constraint_projections;
	};
	TableState &FindTable(idx_t table_id) const;
	static vector<idx_t> Positions(const TableState &table, const vector<ColumnBinding> &key);
	Snapshot Project(const vector<ColumnBinding> &key, bool constraints) const;
	mutable unordered_map<idx_t, TableState> tables_;
	idx_t next_symbol_ = 0;
};
} // namespace duckdb
