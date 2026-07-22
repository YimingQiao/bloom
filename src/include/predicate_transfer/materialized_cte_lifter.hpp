#pragma once

#include <utility>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "predicate_transfer/config.hpp"

namespace duckdb {

//! MaterializedCTELifter walks a logical plan top-down and lifts every
//! LOGICAL_MATERIALIZED_CTE node out of the tree by:
//!   1. Running a fresh PredicateTransferOptimizer pass on the CTE definition
//!      (children[0]) as its own scope.
//!   2. Executing the optimized definition into a ColumnDataCollection.
//!   3. Replacing every LOGICAL_CTE_REF in the main query (children[1]) whose
//!      cte_index matches the lifted CTE with a LogicalColumnDataGet that
//!      shares the materialized data via shared_ptr.
//!   4. Returning children[1] in place of the MATERIALIZED_CTE node.
//!
//! After lifting, the resulting plan contains zero MATERIALIZED_CTE nodes; the
//! materialized CTE results appear as LOGICAL_CHUNK_GET operators that the
//! parent PT scope picks up as ordinary table operators.
//!
//! LOGICAL_RECURSIVE_CTE is not handled here.
class MaterializedCTELifter {
public:
	MaterializedCTELifter(Optimizer &optimizer, ClientContext &context, RPTOptimizerConfig config)
	    : optimizer(optimizer), context(context), config(std::move(config)) {
	}

	//! Top-down recursive walker. Returns the rewritten plan.
	unique_ptr<LogicalOperator> Lift(unique_ptr<LogicalOperator> op);

private:
	//! Execute a logical plan and return the resulting ColumnDataCollection.
	unique_ptr<ColumnDataCollection> ExecutePlan(unique_ptr<LogicalOperator> plan);

	//! Replace every LOGICAL_CTE_REF whose cte_index matches `target_cte_index`
	//! with a LogicalColumnDataGet that scans `shared_data`.
	unique_ptr<LogicalOperator> ReplaceCTERefs(unique_ptr<LogicalOperator> op, TableIndex target_cte_index,
	                                           const shared_ptr<ColumnDataCollection> &shared_data);

	Optimizer &optimizer;
	ClientContext &context;
	RPTOptimizerConfig config;
};

} // namespace duckdb
