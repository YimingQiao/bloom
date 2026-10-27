#include "predicate_transfer/cardinality_estimation/cardinality_estimator.hpp"
#include "predicate_transfer/rpt_result_collector.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

shared_ptr<ColumnDataCollection> ExecutePlanAndCollect(ClientContext &context, unique_ptr<LogicalOperator> plan) {
	auto col_types = plan->types;

	PhysicalPlanGenerator generator(context);
	auto physical_plan = generator.Plan(std::move(plan));

	PreparedStatementData data(StatementType::SELECT_STATEMENT);
	data.physical_plan = std::move(physical_plan);

	auto &root = data.physical_plan->Root();
	data.types = root.types;
	data.names.resize(data.types.size());
	for (idx_t i = 0; i < data.types.size(); i++) {
		data.names[i] = Identifier("col" + std::to_string(i));
	}

	auto &client_data = ClientData::Get(context);
	auto previous_profiler = client_data.profiler;
	client_data.profiler = make_shared_ptr<QueryProfiler>(context);
	Executor executor(context);
	auto collector = GetRPTResultCollector(context, data);
	executor.Initialize(std::move(collector));

	while (executor.ExecuteTask() != QueryResultState::FINISHED) {
	}

	auto result = executor.GetResult();
	D_ASSERT(result);
	executor.CancelTasks();
	if (result->HasError()) {
		client_data.profiler = std::move(previous_profiler);
		result->ThrowError();
	}
	auto result_cdc = shared_ptr<ColumnDataCollection>(result->TakeCollection().release());
	if (!result_cdc) {
		result_cdc = make_shared_ptr<ColumnDataCollection>(BufferAllocator::Get(context), col_types);
	}
	client_data.profiler = std::move(previous_profiler);
	return result_cdc;
}

} // namespace duckdb
