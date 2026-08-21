#include "predicate_transfer/table_scanner/table_scanner.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <functional>

namespace duckdb {

TableScanner::TableScanner(Optimizer &optimizer, ClientContext &context, LogicalOperator &table_op,
                           bool enable_late_materialization)
    : context_(context), materialization_(optimizer, context, table_op, enable_late_materialization) {
}

void TableScanner::SetRequiredColumns(const column_binding_set_t &bindings) {
	materialization_.SetRequiredColumns(bindings);
}

void TableScanner::Materialize() {
	materialization_.Materialize(filters_);
}

void TableScanner::AddFilter(ColumnBinding binding, shared_ptr<RPTFilter> filter, size_t identity_hash) {
	filters_.Add(binding, std::move(filter), identity_hash);
	if (materialization_.IsMaterialized()) {
		filters_.Resolve(materialization_.GetOutputBindings());
	}
}

void TableScanner::AddFilter(const vector<ColumnBinding> &bindings, shared_ptr<RPTFilter> filter,
                             size_t identity_hash) {
	filters_.Add(bindings, std::move(filter), identity_hash);
	if (materialization_.IsMaterialized()) {
		filters_.Resolve(materialization_.GetOutputBindings());
	}
}

size_t TableScanner::FilterStateFingerprint() const {
	return filters_.Fingerprint();
}

void TableScanner::InitScanChunk(DataChunk &chunk) const {
	materialization_.InitScanChunk(chunk);
}

bool TableScanner::Scan(DataChunk &chunk) {
	while (materialization_.Scan(chunk)) {
		filters_.Apply(chunk);
		if (chunk.size() > 0) {
			return true;
		}
	}
	return false;
}

void TableScanner::ResetScan() {
	materialization_.ResetScan();
}

idx_t TableScanner::Count() const {
	return materialization_.Count();
}

namespace {

class ParallelCompactScanTask : public BaseExecutorTask {
public:
	using ScanFunction = std::function<void(idx_t, DataChunk &)>;

	ParallelCompactScanTask(TaskExecutor &executor, const ColumnDataCollection &collection,
	                        ColumnDataParallelScanState &scan_state, idx_t task_id, ScanFunction &function)
	    : BaseExecutorTask(executor), collection(collection), scan_state(scan_state), task_id(task_id),
	      function(function) {
	}

	void ExecuteTask() override {
		ColumnDataLocalScanState local_state;
		DataChunk chunk;
		collection.InitializeScanChunk(chunk);
		while (collection.Scan(scan_state, local_state, chunk)) {
			if (chunk.size() > 0) {
				function(task_id, chunk);
			}
		}
	}

	string TaskType() const override {
		return "RPTParallelCompactScan";
	}

private:
	const ColumnDataCollection &collection;
	ColumnDataParallelScanState &scan_state;
	idx_t task_id;
	ScanFunction &function;
};

static void ParallelCompactScan(ClientContext &context, const ColumnDataCollection &collection, idx_t task_count,
                                ParallelCompactScanTask::ScanFunction function) {
	ColumnDataParallelScanState scan_state;
	collection.InitializeScan(scan_state);
	TaskExecutor executor(context);
	for (idx_t task_id = 0; task_id < task_count; task_id++) {
		executor.ScheduleTask(make_uniq<ParallelCompactScanTask>(executor, collection, scan_state, task_id, function));
	}
	executor.WorkOnTasks();
}

} // namespace

idx_t RPTScanTaskCount(ClientContext &context, const ColumnDataCollection &collection) {
	// Filtering can leave a collection with many nearly empty chunks. Bound the
	// lane count by both physical chunks and surviving rows so task-local setup
	// does not dominate a small transfer.
	static constexpr idx_t MIN_CHUNKS_PER_TASK = 8;
	static constexpr idx_t MIN_ROWS_PER_TASK = static_cast<idx_t>(STANDARD_VECTOR_SIZE) * 4;
	auto chunk_count = collection.ChunkCount();
	auto row_count = collection.Count();
	auto chunk_tasks = chunk_count / MIN_CHUNKS_PER_TASK + (chunk_count % MIN_CHUNKS_PER_TASK != 0);
	auto row_tasks = row_count / MIN_ROWS_PER_TASK + (row_count % MIN_ROWS_PER_TASK != 0);
	auto tasks = MaxValue<idx_t>(MinValue<idx_t>(chunk_tasks, row_tasks), 1);
	return MinValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), tasks);
}

// Compact — Evaluate deferred filters into a new flat collection
//===--------------------------------------------------------------------===//

TableScanner::CompactResult TableScanner::Compact(const vector<StatsRequest> &stats_requests) {
	CompactResult result;
	result.column_stats.resize(stats_requests.size());
	auto *data = materialization_.GetData();
	auto *pending_expression = materialization_.GetPendingExpression();
	if (!data) {
		return result;
	}
	if (filters_.Empty() && !pending_expression && stats_requests.empty()) {
		result.row_count = Count();
		return result;
	}

	ResetScan();
	DataChunk chunk;
	InitScanChunk(chunk);

	vector<idx_t> collected_request_indices;
	for (idx_t request_idx = 0; request_idx < stats_requests.size(); request_idx++) {
		auto &request = stats_requests[request_idx];
		if (request.chunk_col < chunk.ColumnCount() && request.type.IsIntegral() &&
		    request.type != LogicalType::HUGEINT && request.type != LogicalType::UHUGEINT) {
			collected_request_indices.push_back(request_idx);
		}
	}
	bool collect_min_max = !collected_request_indices.empty();

	vector<unique_ptr<Expression>> min_max_aggregates;
	unique_ptr<GlobalUngroupedAggregateState> global_aggregate_state;
	unique_ptr<LocalUngroupedAggregateState> local_aggregate_state;
	if (collect_min_max) {
		for (auto request_idx : collected_request_indices) {
			for (auto &aggr : {MinFunction::GetFunction(), MaxFunction::GetFunction()}) {
				FunctionBinder function_binder(context_);
				vector<unique_ptr<Expression>> aggr_children;
				aggr_children.push_back(make_uniq<BoundReferenceExpression>(stats_requests[request_idx].type,
				                                                            stats_requests[request_idx].chunk_col));
				auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
				                                                       AggregateType::NON_DISTINCT);
				min_max_aggregates.push_back(std::move(aggr_expr));
			}
		}

		global_aggregate_state =
		    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context_), min_max_aggregates);
		local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*global_aggregate_state);
	}

	unique_ptr<ColumnDataCollection> new_data;
	if (!filters_.Empty() || pending_expression) {
		// New CDC matches the (possibly narrowed) chunk schema that Scan emits.
		new_data = make_uniq<ColumnDataCollection>(BufferAllocator::Get(context_), chunk.GetTypes());
	}

	auto task_count = RPTScanTaskCount(context_, *data);
	if (task_count <= 1) {
		while (Scan(chunk)) {
			if (collect_min_max) {
				for (idx_t stats_idx = 0; stats_idx < collected_request_indices.size(); stats_idx++) {
					auto chunk_col = stats_requests[collected_request_indices[stats_idx]].chunk_col;
					local_aggregate_state->Sink(chunk, chunk_col, stats_idx * 2, chunk.size());
					local_aggregate_state->Sink(chunk, chunk_col, stats_idx * 2 + 1, chunk.size());
				}
			}
			if (new_data) {
				// Append flattens the chunk natively, preserving only valid rows.
				new_data->Append(chunk);
			}
		}
	} else {
		vector<unique_ptr<ExpressionExecutor>> local_expression_executors;
		vector<unique_ptr<DataChunk>> local_projection_chunks;
		if (pending_expression) {
			local_expression_executors.reserve(task_count);
			local_projection_chunks.reserve(task_count);
			for (idx_t task_id = 0; task_id < task_count; task_id++) {
				unique_ptr<ExpressionExecutor> executor;
				if (!pending_expression->expressions.empty()) {
					executor = make_uniq<ExpressionExecutor>(context_, *pending_expression->expressions.front());
				}
				local_expression_executors.push_back(std::move(executor));

				unique_ptr<DataChunk> projection_chunk;
				if (!pending_expression->projection_map.empty()) {
					projection_chunk = make_uniq<DataChunk>();
					projection_chunk->Initialize(BufferAllocator::Get(context_), chunk.GetTypes());
				}
				local_projection_chunks.push_back(std::move(projection_chunk));
			}
		}

		vector<unique_ptr<LocalUngroupedAggregateState>> local_aggregate_states;
		if (collect_min_max) {
			local_aggregate_state.reset();
			local_aggregate_states.reserve(task_count);
			for (idx_t task_id = 0; task_id < task_count; task_id++) {
				local_aggregate_states.push_back(make_uniq<LocalUngroupedAggregateState>(*global_aggregate_state));
			}
		}

		vector<unique_ptr<ColumnDataCollection>> local_collections;
		if (new_data) {
			new_data.reset();
			local_collections.reserve(task_count);
			for (idx_t task_id = 0; task_id < task_count; task_id++) {
				local_collections.push_back(
				    make_uniq<ColumnDataCollection>(BufferAllocator::Get(context_), chunk.GetTypes()));
			}
		}

		ParallelCompactScan(context_, *data, task_count, [&](idx_t task_id, DataChunk &scan_chunk) {
			DataChunk *filtered_chunk = &scan_chunk;
			if (pending_expression) {
				auto &pending = *pending_expression;
				auto original_count = scan_chunk.size();
				auto selected_count = original_count;
				SelectionVector selected(STANDARD_VECTOR_SIZE);
				if (local_expression_executors[task_id]) {
					selected_count = local_expression_executors[task_id]->SelectExpression(scan_chunk, selected);
				}
				if (selected_count == 0) {
					return;
				}

				if (!pending.projection_map.empty()) {
					auto &projected = *local_projection_chunks[task_id];
					projected.Reset();
					projected.SetCardinalityUnsafe(selected_count);
					for (idx_t col_idx = 0; col_idx < projected.ColumnCount(); col_idx++) {
						auto source_idx = pending.projection_map[col_idx];
						if (selected_count < original_count) {
							projected.data[col_idx].Slice(scan_chunk.data[source_idx], selected, selected_count);
						} else {
							projected.data[col_idx].Reference(scan_chunk.data[source_idx]);
						}
					}
					filtered_chunk = &projected;
				} else if (selected_count < original_count) {
					scan_chunk.Slice(selected, selected_count);
					scan_chunk.Flatten();
				}
			}

			if (!filters_.Empty()) {
				filters_.Apply(*filtered_chunk);
			}
			if (filtered_chunk->size() == 0) {
				return;
			}
			if (collect_min_max) {
				auto &aggregate_state = *local_aggregate_states[task_id];
				for (idx_t stats_idx = 0; stats_idx < collected_request_indices.size(); stats_idx++) {
					auto chunk_col = stats_requests[collected_request_indices[stats_idx]].chunk_col;
					aggregate_state.Sink(*filtered_chunk, chunk_col, stats_idx * 2, filtered_chunk->size());
					aggregate_state.Sink(*filtered_chunk, chunk_col, stats_idx * 2 + 1, filtered_chunk->size());
				}
			}
			if (!local_collections.empty()) {
				local_collections[task_id]->Append(*filtered_chunk);
			}
		});

		for (auto &aggregate_state : local_aggregate_states) {
			global_aggregate_state->Combine(*aggregate_state);
		}
		if (!local_collections.empty()) {
			new_data = make_uniq<ColumnDataCollection>(BufferAllocator::Get(context_), chunk.GetTypes());
			for (auto &local_collection : local_collections) {
				new_data->Combine(*local_collection);
			}
		}
	}

	if (collect_min_max) {
		if (local_aggregate_state) {
			global_aggregate_state->Combine(*local_aggregate_state);
		}
		vector<LogicalType> min_max_types;
		for (auto &aggr_expr : min_max_aggregates) {
			min_max_types.push_back(aggr_expr->GetReturnType());
		}
		DataChunk final_min_max;
		final_min_max.Initialize(BufferAllocator::Get(context_), min_max_types);
		global_aggregate_state->Finalize(final_min_max);

		for (idx_t stats_idx = 0; stats_idx < collected_request_indices.size(); stats_idx++) {
			auto min_val = final_min_max.data[stats_idx * 2].GetValue(0);
			auto max_val = final_min_max.data[stats_idx * 2 + 1].GetValue(0);
			if (!min_val.IsNull() && !max_val.IsNull() && min_val.type().IsIntegral() &&
			    max_val.type() == min_val.type()) {
				auto &stats = result.column_stats[collected_request_indices[stats_idx]];
				stats.has_min_max = true;
				stats.observed_min = static_cast<int64_t>(IntegralValue::Get(min_val).lower);
				stats.observed_max = static_cast<int64_t>(IntegralValue::Get(max_val).lower);
			}
		}
	}

	if (new_data) {
		materialization_.ReplaceData(std::move(new_data));
		filters_.Clear();
	}
	result.row_count = Count();
	ResetScan();
	return result;
}

} // namespace duckdb
