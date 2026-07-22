#pragma once

#include <utility>

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"

#include "predicate_transfer/table_operator_manager.hpp"
#include "transfer_types.hpp"
#include "dag.hpp"
#include "predicate_transfer/config.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BaseGraphManager — base class for predicate transfer strategies
//===--------------------------------------------------------------------===//
//! Builds a join graph from the logical plan and delegates plan generation
//! to a strategy-specific subclass (Yannakakis or Excitation).
class BaseGraphManager {
public:
	virtual ~BaseGraphManager() = default;

	explicit BaseGraphManager(Optimizer &optimizer, ClientContext &context, RPTOptimizerConfig config)
	    : optimizer(optimizer), context(context), table_operator_manager(context), config(std::move(config)) {
	}

	//===----------------------------------------------------------------===//
	// Public API
	//===----------------------------------------------------------------===//
	//! Extract tables and join edges from the logical plan, then generate
	//! the transfer plan via the strategy-specific ExecuteTransfer().
	bool Build(LogicalOperator &op);

	//! Debug output.
	virtual string TransferPlanToString() {
		return "";
	}
	virtual string TablesToString() {
		return "";
	}
	string EdgesToString() const;

	//===----------------------------------------------------------------===//
	// Shared infrastructure
	//===----------------------------------------------------------------===//
	Optimizer &optimizer;
	ClientContext &context;
	TableOperatorManager table_operator_manager;
	RPTOptimizerConfig config;

	//! The generated transfer plan — consumed by PredicateTransferOptimizer.
	vector<TransferStep> result_transfer_steps;

protected:
	//! Strategy hook: subclasses implement their own transfer execution logic.
	virtual void ExecuteTransfer() = 0;

	//! Discover transitive edges from table join key groups.
	void DiscoverEdges();

private:
	//! Parse join operators to populate neighbor_matrix.
	void ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators);

	//! Emit an edge and its flipped twin into neighbor_matrix. `forward_types`
	//! carry the per-key types for the left→right direction; the flipped twin
	//! re-resolves its types against right_node so each direction's source
	//! types stay aligned with the source table's output.
	void AddEdgePair(LogicalOperator &left_node, vector<ColumnBinding> left_bindings, LogicalOperator &right_node,
	                 vector<ColumnBinding> right_bindings, vector<LogicalType> forward_types, bool protect_left,
	                 bool protect_right);

protected:
	//! Adjacency matrix: table_id → neighbor_id → vector<EdgeInfo>.
	unordered_map<idx_t, unordered_map<idx_t, vector<EdgeInfo>>> neighbor_matrix;
	//! Union-find groups of join keys.
	BindingGroupMap table_groups;
};

} // namespace duckdb
