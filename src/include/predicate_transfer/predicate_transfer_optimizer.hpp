#pragma once

#include "predicate_transfer/transfer_plan/base_graph_manager.hpp"
#include "predicate_transfer/transfer_plan/excitation_graph/excitation_graph_manager.hpp"
#include "predicate_transfer/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

class PredicateTransferOptimizer {
public:
	explicit PredicateTransferOptimizer(Optimizer &optimizer, ClientContext &context, const RPTOptimizerConfig &config)
	    : optimizer(optimizer), config(config) {
		graph_manager = make_uniq<ExcitationGraphManager>(optimizer, context, config);
	}

	//! Single-pass optimizer: build transfer graph, run excitation, rewrite the query plan.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	string GetDebugInfo() const;
	Optimizer &GetOptimizer() {
		return optimizer;
	}

private:
	string cached_debug_info;

	//! Rewrite the final query plan using materialized tables, row-id filters, and direct filters.
	unique_ptr<LogicalOperator> RewriteQueryPlan(unique_ptr<LogicalOperator> plan);

	unique_ptr<BaseGraphManager> graph_manager;
	Optimizer &optimizer;
	RPTOptimizerConfig config;
};

} // namespace duckdb
