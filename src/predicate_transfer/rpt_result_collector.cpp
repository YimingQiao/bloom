#include "predicate_transfer/rpt_result_collector.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
namespace {

class RPTResultCollector : public PhysicalResultCollector {
public:
	RPTResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data, bool parallel_p)
	    : PhysicalResultCollector(physical_plan, data), parallel(parallel_p) {
	}

	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	bool ParallelSink() const override {
		return parallel;
	}
	bool SinkOrderDependent() const override {
		return true;
	}

private:
	unique_ptr<ColumnDataCollection> CreateRPTCollection(ClientContext &context) const {
		return make_uniq<ColumnDataCollection>(BufferAllocator::Get(context), types);
	}

	bool parallel;
};

class RPTCollectorGlobalState : public GlobalSinkState {
public:
	mutex lock;
	unique_ptr<ColumnDataCollection> collection;
	//! Weak to avoid creating a cycle through a materialized query result.
	weak_ptr<ClientContext> context;
};

class RPTCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

SinkResultType RPTResultCollector::Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &local = input.local_state.Cast<RPTCollectorLocalState>();
	local.collection->Append(local.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType RPTResultCollector::Combine(ExecutionContext &, OperatorSinkCombineInput &input) const {
	auto &global = input.global_state.Cast<RPTCollectorGlobalState>();
	auto &local = input.local_state.Cast<RPTCollectorLocalState>();
	if (local.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}
	lock_guard<mutex> guard(global.lock);
	if (!global.collection) {
		global.collection = std::move(local.collection);
	} else {
		global.collection->Combine(*local.collection);
	}
	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> RPTResultCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<RPTCollectorLocalState>();
	result->collection = CreateRPTCollection(context.client);
	result->collection->InitializeAppend(result->append_state);
	return std::move(result);
}

unique_ptr<GlobalSinkState> RPTResultCollector::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<RPTCollectorGlobalState>();
	result->context = context.shared_from_this();
	return std::move(result);
}

unique_ptr<QueryResult> RPTResultCollector::GetResult(GlobalSinkState &state) const {
	auto &global = state.Cast<RPTCollectorGlobalState>();
	auto context = global.context.lock();
	if (!context) {
		throw InternalException("Bloom result collector lost its ClientContext");
	}
	if (!global.collection) {
		global.collection = CreateRPTCollection(*context);
	}
	return make_uniq<MaterializedQueryResult>(statement_type, properties, IdentifiersToStrings(names),
	                                          std::move(global.collection), context->GetClientProperties());
}

} // namespace

unique_ptr<PhysicalOperator> GetRPTResultCollector(ClientContext &context, PreparedStatementData &data) {
	auto &root = data.physical_plan->Root();
	// A non-parallel sink preserves order for the few optimizer-time plans where
	// DuckDB requires it. Bloom does not need the batch collector's parallelism.
	auto parallel = !PhysicalPlanGenerator::PreserveInsertionOrder(context, root);
	return make_uniq<RPTResultCollector>(*data.physical_plan, data, parallel);
}

} // namespace duckdb
