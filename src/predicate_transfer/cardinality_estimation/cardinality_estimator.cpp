#include "predicate_transfer/cardinality_estimation/cardinality_estimator.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

shared_ptr<ColumnDataCollection> ExecutePlanAndCollect(ClientContext &context, unique_ptr<LogicalOperator> plan) {
	auto col_types = plan->types;

	PhysicalPlanGenerator generator(context);
	auto physical_plan = generator.Plan(std::move(plan));

	PreparedStatementData data(StatementType::SELECT_STATEMENT);
	data.physical_plan = std::move(physical_plan);
	data.memory_type = QueryResultMemoryType::IN_MEMORY;
	data.output_type = QueryResultOutputType::FORCE_MATERIALIZED;

	auto &root = data.physical_plan->Root();
	data.types = root.types;
	data.names.resize(data.types.size());
	for (idx_t i = 0; i < data.types.size(); i++) {
		data.names[i] = Identifier("col" + std::to_string(i));
	}

	auto &client_data = ClientData::Get(context);
	auto saved_profiler = client_data.profiler;
	client_data.profiler = make_shared_ptr<QueryProfiler>(context);

	Executor executor(context);
	auto collector = PhysicalResultCollector::GetResultCollector(context, data);
	executor.Initialize(std::move(collector));

	shared_ptr<ColumnDataCollection> result_cdc;
	try {
		while (executor.ExecuteTask() != PendingExecutionResult::EXECUTION_FINISHED) {
		}

		auto result = executor.GetResult();
		auto &mat_result = result->Cast<MaterializedQueryResult>();
		result_cdc = shared_ptr<ColumnDataCollection>(mat_result.TakeCollection().release());
		if (!result_cdc) {
			result_cdc = make_shared_ptr<ColumnDataCollection>(context, col_types);
		}
	} catch (...) {
		client_data.profiler = std::move(saved_profiler);
		throw;
	}

	client_data.profiler = std::move(saved_profiler);
	return result_cdc;
}

} // namespace duckdb
