//===----------------------------------------------------------------------===//
//                         RPT Extension
//
// predicate_transfer/transfer_types.hpp
//
// Shared types for the predicate transfer subsystem:
//   EdgeInfo, TransferStep, JoinKeyTableGroup, ColumnBindingHashFunc.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"
#include "dag.hpp"
#include "predicate_transfer/table_operator_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ColumnBindingHashFunc
//===--------------------------------------------------------------------===//
struct ColumnBindingHashFunc {
	size_t operator()(const ColumnBinding &key) const {
		return std::hash<uint64_t> {}(key.table_index.index) ^ (std::hash<uint64_t> {}(key.column_index) << 1);
	}
};

//===--------------------------------------------------------------------===//
// EdgeInfo — describes one physical join between two base tables. The join
// may have multiple equality conditions; each condition contributes one entry
// to the parallel `*_bindings` / `return_types` vectors.
//===--------------------------------------------------------------------===//
class EdgeInfo {
public:
	vector<LogicalType> return_types;

	LogicalOperator &left_table;
	vector<ColumnBinding> left_bindings;

	LogicalOperator &right_table;
	vector<ColumnBinding> right_bindings;

	bool protect_left = false;
	bool protect_right = false;

	EdgeInfo(vector<LogicalType> types, LogicalOperator &left, vector<ColumnBinding> left_bindings,
	         LogicalOperator &right, vector<ColumnBinding> right_bindings)
	    : return_types(std::move(types)), left_table(left), left_bindings(std::move(left_bindings)), right_table(right),
	      right_bindings(std::move(right_bindings)) {
	}

public:
	idx_t KeyCount() const {
		return left_bindings.size();
	}

	bool operator==(const EdgeInfo &e) const {
		return return_types == e.return_types && left_bindings == e.left_bindings && right_bindings == e.right_bindings;
	}

	EdgeInfo Flip() const {
		EdgeInfo flipped(return_types, right_table, right_bindings, left_table, left_bindings);
		flipped.protect_left = protect_right;
		flipped.protect_right = protect_left;
		return flipped;
	}

	string ToString() const;
};

//===--------------------------------------------------------------------===//
// JoinKeyTableGroup — union-find group of tables sharing a join key
//===--------------------------------------------------------------------===//
struct JoinKeyTableGroup {
	unordered_map<idx_t, ColumnBinding> table_ids;
	//! Representative key type for this equivalence class. Set from the first
	//! join condition that seeded the group; used as the closure-edge type in
	//! DiscoverEdges, where we do not always have a reliable way to index into
	//! a table operator's output `types` (e.g. a compression projection may
	//! have shrunk `types` below the post-rename column_index).
	LogicalType return_type = LogicalType::INVALID;

	JoinKeyTableGroup(const idx_t table_id, ColumnBinding col, LogicalType type) : return_type(std::move(type)) {
		table_ids.emplace(table_id, col);
	}

	void Union(const JoinKeyTableGroup &other);
	string ToString() const;
};

//===--------------------------------------------------------------------===//
// Type aliases
//===--------------------------------------------------------------------===//
using BindingGroupMap = unordered_map<ColumnBinding, shared_ptr<JoinKeyTableGroup>, ColumnBindingHashFunc>;

//===--------------------------------------------------------------------===//
// TransferStep — one step in a transfer plan
//===--------------------------------------------------------------------===//
//! Represents a single step: scan `table`, build Bloom filters for `create_bf`.
struct TransferStep {
	LogicalOperator *table;
	vector<shared_ptr<GraphEdge>> create_bf;
	idx_t id = 0;
	vector<idx_t> depends_on; // step IDs this step must wait for
	string ToString() const;
};

} // namespace duckdb
