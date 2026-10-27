#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
//! Reject subtrees whose tasks may look up DuckDB's active query Executor,
//! which is not registered while Bloom runs during optimization.
const char *OptimizerExecutionUnsafeReason(const LogicalOperator &op, ClientContext &context);

class TableOperatorManager {
public:
	explicit TableOperatorManager(ClientContext &context) : context(context) {
	}

	ClientContext &context;

public:
	void Build(LogicalOperator &plan);
	const unordered_map<idx_t, reference<LogicalOperator>> &GetAllTableOperators() const {
		return table_operators;
	}
	const vector<reference<LogicalOperator>> &GetAllJoinOperators() const {
		return join_operators;
	}
	vector<reference<LogicalOperator>> GetTableInDFSOrder() const {
		return table_dfs_order;
	}

	LogicalOperator *GetTableOperator(idx_t table_idx);
	const LogicalOperator *GetTableOperator(idx_t table_idx) const;
	ColumnBinding GetRenaming(ColumnBinding col_binding);

	//! Clear stored operator references (call after plan modification to prevent dangling refs).
	void ClearTableOperators() {
		table_operators.clear();
		join_operators.clear();
		table_dfs_order.clear();
		contains_join_set_.clear();
	}

	static idx_t GetScalarTableIndex(const LogicalOperator &op);

	static string GetTableName(const LogicalOperator &op);
	string GetColumnName(const ColumnBinding &binding);

private:
	void AddTableOperator(LogicalOperator &op);

	//! Two-pass extraction:
	//!   Pass 1 (CollectJoins): walk the plan, collect inner equi-joins into
	//!     join_operators and build contains_join_set_ — the set of operators
	//!     whose subtree contains at least one inner equi-join.
	//!   Pass 2 (CollectTableOperators): admit any subtree NOT in
	//!     contains_join_set_ as a table operator. When descending through
	//!     contains_join_set_ operators, record PROJECTION/AGG renames so
	//!     upstream join conditions can resolve to the admitted subtree's
	//!     table_index.
	bool CollectJoins(LogicalOperator &op);
	void CollectTableOperators(LogicalOperator &op);
	void RecordRenameOneLevel(LogicalOperator &op);

	static bool IsInnerEquiJoin(const LogicalOperator &op);

	//! Recursively search an expression tree for a single BOUND_COLUMN_REF.
	//! Used to see through wrapping functions (e.g. __internal_compress_integral_uinteger).
	static void FindInnerColumnRef(Expression &expr, BoundColumnRefExpression *&out_ref);

	struct HashFunc {
		size_t operator()(const ColumnBinding &key) const {
			return std::hash<uint64_t> {}(key.table_index.index) ^ (std::hash<uint64_t> {}(key.column_index) << 1U);
		}
	};
	unordered_map<ColumnBinding, ColumnBinding, HashFunc> rename_col_bindings;

	vector<reference<LogicalOperator>> join_operators;
	vector<reference<LogicalOperator>> table_dfs_order;
	unordered_map<idx_t, reference<LogicalOperator>> table_operators;

	//! Operators whose subtree contains at least one inner equi-join.
	//! Populated by CollectJoins (pass 1), consumed by CollectTableOperators
	//! (pass 2). Cleared on ClearTableOperators.
	unordered_set<const LogicalOperator *> contains_join_set_;
};
} // namespace duckdb
