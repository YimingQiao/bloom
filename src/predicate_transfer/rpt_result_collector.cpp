#include "predicate_transfer/rpt_result_collector.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
namespace {

class RPTResultCollector : public PhysicalResultCollector {
public:
	RPTResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data, bool parallel_p, bool batch_ordered_p)
	    : PhysicalResultCollector(physical_plan, data), parallel(parallel_p), batch_ordered(batch_ordered_p) {
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
		return !batch_ordered;
	}
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return batch_ordered ? OperatorPartitionInfo::BatchIndex() : PhysicalOperator::RequiredPartitionInfo();
	}

private:
	unique_ptr<ColumnDataCollection> CreateRPTCollection(ClientContext &context) const {
		return make_uniq<ColumnDataCollection>(BufferAllocator::Get(context), types);
	}

	bool parallel;
	bool batch_ordered;
};

class RPTCollectorGlobalState : public GlobalSinkState {
public:
	mutex lock;
	unique_ptr<ColumnDataCollection> collection;
	//! Retain batches separately until they can be combined in source order.
	map<idx_t, unique_ptr<ColumnDataCollection>> batches;
	//! Weak to avoid creating a cycle through a materialized query result.
	weak_ptr<ClientContext> context;
};

class RPTCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	map<idx_t, unique_ptr<ColumnDataCollection>> batches;
	ColumnDataAppendState append_state;
	idx_t batch_index = DConstants::INVALID_INDEX;
};

SinkResultType RPTResultCollector::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &local = input.local_state.Cast<RPTCollectorLocalState>();
	if (batch_ordered) {
		auto batch_index = local.partition_info.batch_index.GetIndex();
		if (batch_index != local.batch_index) {
			if (local.collection->Count() > 0) {
				D_ASSERT(local.batches.find(local.batch_index) == local.batches.end());
				local.batches.emplace(local.batch_index, std::move(local.collection));
				local.collection = CreateRPTCollection(context.client);
				local.collection->InitializeAppend(local.append_state);
			}
			local.batch_index = batch_index;
		}
	}
	local.collection->Append(local.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType RPTResultCollector::Combine(ExecutionContext &, OperatorSinkCombineInput &input) const {
	auto &global = input.global_state.Cast<RPTCollectorGlobalState>();
	auto &local = input.local_state.Cast<RPTCollectorLocalState>();
	if (batch_ordered) {
		if (local.collection->Count() > 0) {
			local.batches.emplace(local.batch_index, std::move(local.collection));
		}
		lock_guard<mutex> guard(global.lock);
		for (auto &batch : local.batches) {
			D_ASSERT(global.batches.find(batch.first) == global.batches.end());
			global.batches.emplace(batch.first, std::move(batch.second));
		}
		local.batches.clear();
		return SinkCombineResultType::FINISHED;
	}
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
	for (auto &batch : global.batches) {
		if (!global.collection) {
			global.collection = std::move(batch.second);
		} else {
			global.collection->Combine(*batch.second);
		}
	}
	global.batches.clear();
	if (!global.collection) {
		global.collection = CreateRPTCollection(*context);
	}
	return make_uniq<QueryResult>(statement_type, properties, names, std::move(global.collection),
	                              context->GetClientProperties());
}

} // namespace

unique_ptr<PhysicalOperator> GetRPTResultCollector(ClientContext &context, PreparedStatementData &data) {
	auto &root = data.physical_plan->Root();
	// Preserve source order without serializing scans that supply batch indexes.
	// Collections retain BufferAllocator accounting and merge in batch order.
	auto preserve_order = PhysicalPlanGenerator::PreserveInsertionOrder(context, root);
	auto batch_ordered = preserve_order && PhysicalPlanGenerator::UseBatchIndex(context, root);
	return make_uniq<RPTResultCollector>(*data.physical_plan, data, !preserve_order || batch_ordered, batch_ordered);
}

} // namespace duckdb
