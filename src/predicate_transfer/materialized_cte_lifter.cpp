#include "predicate_transfer/materialized_cte_lifter.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "predicate_transfer/predicate_transfer_optimizer.hpp"

namespace duckdb {

//! A CTE is "self-selective" when a FILTER sits above a GROUP BY on the
//! left spine — the aggregation already compresses rows and the filter
//! drops them further, so RPT materialisation of the internals rarely
//! pays off.
//! Any AGGREGATE_AND_GROUP_BY anywhere in the subtree.
static bool ContainsAggregate(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsAggregate(*child)) {
			return true;
		}
	}
	return false;
}

static bool IsFilterOverAggregate(const LogicalOperator &op) {
	const LogicalOperator *cur = &op;
	while (cur) {
		if (cur->type == LogicalOperatorType::LOGICAL_FILTER) {
			const LogicalOperator *below = cur;
			while (!below->children.empty()) {
				below = below->children[0].get();
				auto bt = below->type;
				if (bt == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
					return true;
				}
				if (bt != LogicalOperatorType::LOGICAL_PROJECTION && bt != LogicalOperatorType::LOGICAL_FILTER) {
					break;
				}
			}
		}
		if (cur->children.empty()) {
			break;
		}
		cur = cur->children[0].get();
	}
	return false;
}

static bool ContainsCTERef(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsCTERef(*child)) {
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> MaterializedCTELifter::Lift(unique_ptr<LogicalOperator> op) {
	if (!op) {
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		// Recursive CTEs are out of scope for this lifter; the surrounding
		// PredicateTransferOptimizer handles them via independent sub-scopes.
		return op;
	}
	if (op->type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		for (auto &child : op->children) {
			child = Lift(std::move(child));
		}
		return op;
	}

	// TODO(pattern-A): when children[0] contains a nested MATERIALIZED_CTE
	// (rare; only occurs when SQL nests a WITH inside another WITH's definition
	// body, e.g. `WITH outer AS (WITH inner AS (...) SELECT FROM inner) ...`),
	// that inner CTE is NOT lifted by this walker — it stays as a normal
	// MATERIALIZED_CTE node and DuckDB executes it at runtime via PhysicalCTE.
	// Correctness is preserved; we just miss the opportunity to run PT on the
	// inner CTE's definition. Revisit if this pattern shows up in a workload.
	//
	// For the common case (sibling chain, nesting only on the right spine),
	// children[0] is already self-contained at this point.

	auto &mat_cte = op->Cast<LogicalMaterializedCTE>();
	auto cte_index = mat_cte.table_index;

	// A definition that still refers to an earlier, retained CTE is not
	// self-contained and cannot be executed during optimization.
	if (ContainsCTERef(*op->children[0])) {
		if (config.log_transfer_steps) {
			fprintf(stderr, "[RPT-Excitation] CTE %llu retained: unresolved_cte_dependency\n",
			        static_cast<unsigned long long>(cte_index.index));
		}
		op->children[1] = Lift(std::move(op->children[1]));
		return op;
	}

	// Eagerly lifting aggregate CTEs can be much more expensive even when
	// their operators are executor-safe. Keep them in DuckDB's runtime plan.
	bool skip_for_aggregate = (config.skip_cte_with_filter_agg && IsFilterOverAggregate(*op->children[0])) ||
	                          (config.skip_cte_with_agg && ContainsAggregate(*op->children[0]));
	if (skip_for_aggregate) {
		if (config.log_transfer_steps) {
			fprintf(stderr, "[RPT-Excitation] CTE %llu retained: aggregate_cte_not_eagerly_lifted\n",
			        static_cast<unsigned long long>(cte_index.index));
		}
		op->children[1] = Lift(std::move(op->children[1]));
		return op;
	}

	// 1. Check the untouched logical definition before running child RPT or
	//    constructing a physical plan.
	string unsafe_reason;
	if (!IsSafeForOptimizerExecution(*op->children[0], unsafe_reason)) {
		if (config.log_transfer_steps) {
			fprintf(stderr, "[RPT-Excitation] CTE %llu retained: %s\n",
			        static_cast<unsigned long long>(cte_index.index), unsafe_reason.c_str());
		}
		op->children[1] = Lift(std::move(op->children[1]));
		return op;
	}

	unique_ptr<LogicalOperator> cte_def = std::move(op->children[0]);
	PredicateTransferOptimizer sub_optimizer(optimizer, context, config);
	cte_def = sub_optimizer.Optimize(std::move(cte_def));

	// 2. Execute the (possibly optimized) definition.
	auto collection = ExecutePlan(std::move(cte_def));
	D_ASSERT(collection);
	shared_ptr<ColumnDataCollection> shared_data(collection.release());

	// 3. Rewrite all CTE_REFs inside children[1] that point at this CTE.
	op->children[1] = ReplaceCTERefs(std::move(op->children[1]), cte_index, shared_data);

	// 4. Recurse: walk down the right spine to lift any nested MATERIALIZED_CTE
	//    whose body has now become self-contained.
	return Lift(std::move(op->children[1]));
}

unique_ptr<LogicalOperator> MaterializedCTELifter::ReplaceCTERefs(unique_ptr<LogicalOperator> op,
                                                                  TableIndex target_cte_index,
                                                                  const shared_ptr<ColumnDataCollection> &shared_data) {
	if (!op) {
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte_ref = op->Cast<LogicalCTERef>();
		if (cte_ref.cte_index == target_cte_index) {
			// Use the CTE_REF's own table_index so upstream column references
			// (which use that index) keep resolving. Default 0..N column
			// bindings match LogicalCTERef's defaults.
			optionally_owned_ptr<ColumnDataCollection> owned(shared_data);
			return make_uniq<LogicalColumnDataGet>(cte_ref.table_index, cte_ref.chunk_types, std::move(owned));
		}
		return op;
	}
	// Do not descend into nested RECURSIVE_CTE subtrees — they have their own
	// scope and CTE_REFs there belong to the recursive CTE itself.
	if (op->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return op;
	}
	for (auto &child : op->children) {
		child = ReplaceCTERefs(std::move(child), target_cte_index, shared_data);
	}
	return op;
}

bool MaterializedCTELifter::IsSafeForOptimizerExecution(const LogicalOperator &op, string &unsafe_reason) const {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		bool has_equality = false;
		idx_t range_count = 0;
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			auto comparison = condition.GetComparisonType();
			if (comparison == ExpressionType::COMPARE_EQUAL ||
			    comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				has_equality = true;
			} else if (comparison == ExpressionType::COMPARE_LESSTHAN ||
			           comparison == ExpressionType::COMPARE_GREATERTHAN ||
			           comparison == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
			           comparison == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				range_count++;
			}
		}

		// A pure range comparison can become PIECEWISE_MERGE_JOIN / IE_JOIN.
		// With prefer_range_joins enabled, a mixed join with at least two range
		// keys can become IE_JOIN as well. Their materialization tasks always
		// look up the active query Executor, even with one scheduler thread.
		bool prefers_iejoin = range_count >= 2 && Settings::Get<PreferRangeJoinsSetting>(context);
		if (range_count > 0 && (!has_equality || prefers_iejoin)) {
			unsafe_reason = "logical_range_join_may_require_active_executor";
			return false;
		}

		auto thread_count = TaskScheduler::GetScheduler(context).NumberOfThreads();
		if (has_equality && (thread_count > 1 || context.config.verify_parallelism)) {
			// Equality comparisons normally become PhysicalHashJoin. Its
			// runtime build count can trigger parallel finalize regardless of
			// the optimizer's cardinality estimate.
			unsafe_reason = "logical_hash_join_may_require_active_executor";
			return false;
		}
	}

	for (auto &child : op.children) {
		if (!IsSafeForOptimizerExecution(*child, unsafe_reason)) {
			return false;
		}
	}
	return true;
}

unique_ptr<ColumnDataCollection> MaterializedCTELifter::ExecutePlan(unique_ptr<LogicalOperator> plan) {
	plan->ResolveOperatorTypes();

	PhysicalPlanGenerator generator(context);
	auto physical_plan = generator.Plan(std::move(plan));

	PreparedStatementData stmt_data(StatementType::SELECT_STATEMENT);
	stmt_data.physical_plan = std::move(physical_plan);
	stmt_data.memory_type = QueryResultMemoryType::IN_MEMORY;
	stmt_data.output_type = QueryResultOutputType::FORCE_MATERIALIZED;

	auto &root = stmt_data.physical_plan->Root();
	stmt_data.types = root.types;
	stmt_data.names.resize(stmt_data.types.size());
	for (idx_t i = 0; i < stmt_data.types.size(); i++) {
		stmt_data.names[i] = Identifier("col" + std::to_string(i));
	}

	auto &client_data = ClientData::Get(context);
	auto previous_profiler = client_data.profiler;
	client_data.profiler = make_shared_ptr<QueryProfiler>(context);
	Executor executor(context);
	auto collector = PhysicalResultCollector::GetResultCollector(context, stmt_data);
	executor.Initialize(std::move(collector));

	while (executor.ExecuteTask() != PendingExecutionResult::EXECUTION_FINISHED) {
	}
	auto result = executor.GetResult();
	D_ASSERT(result);
	executor.CancelTasks();
	if (result->HasError()) {
		client_data.profiler = std::move(previous_profiler);
		result->ThrowError();
	}
	auto &mat_result = result->Cast<MaterializedQueryResult>();
	unique_ptr<ColumnDataCollection> result_data = mat_result.TakeCollection();

	client_data.profiler = std::move(previous_profiler);
	return result_data;
}

} // namespace duckdb
