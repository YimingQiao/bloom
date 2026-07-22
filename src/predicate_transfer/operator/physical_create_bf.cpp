#include "predicate_transfer/operator/physical_create_bf.hpp"
#include "predicate_transfer/filter/bitmap_filter.hpp"
#include "predicate_transfer/filter/table_filter.hpp"

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalCreateBF::PhysicalCreateBF(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                   const vector<FilterPhysicalPlan> &filter_plans,
                                   vector<shared_ptr<DynamicTableFilterSet>> dynamic_filter_sets,
                                   vector<vector<ColumnBinding>> &dynamic_filter_cols, idx_t estimated_cardinality,
                                   bool is_probing_side, const Config &config)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      is_probing_side(is_probing_side), is_successful(true), filter_plans(filter_plans), unique_rpt_filters(),
      min_max_applied_cols(std::move(dynamic_filter_cols)), min_max_to_create(std::move(dynamic_filter_sets)),
      config(config) {
	for (size_t i = 0; i < filter_plans.size(); ++i) {
		auto &plan = filter_plans[i];
		auto &cols_build = plan.bound_cols;

		if (unique_rpt_filters.find(cols_build) == unique_rpt_filters.end()) {
			unique_rpt_filters[cols_build] = make_shared_ptr<RPTFilterWrapper>();
		}
		auto bf = unique_rpt_filters[cols_build];
		bf_to_create.emplace_back(make_shared_ptr<RPTFilterUsage>(bf));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op)
	    : context(context), op(op),
	      num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
	      temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)), is_selectivity_checked(false),
	      num_input_rows(0), total_row_size(0) {
		data_collection = make_uniq<ColumnDataCollection>(context, op.types);

		// init min max aggregation
		vector<AggregateFunction> aggr_functions;
		aggr_functions.push_back(MinFunction::GetFunction());
		aggr_functions.push_back(MaxFunction::GetFunction());

		for (idx_t i = 0; i < op.filter_plans.size(); ++i) {
			if (op.min_max_to_create[i] == nullptr) {
				continue;
			}

			auto &plan = op.filter_plans[i];
			for (idx_t j = 0; j < plan.bound_cols.size(); ++j) {
				auto &expr = plan.logical_plan->build[j];
				auto &bound_col_idx = plan.bound_cols[j];
				auto &type = plan.logical_plan->return_types[j];

				if (col_to_expr.count(bound_col_idx)) {
					continue;
				}
				col_to_expr[bound_col_idx] = min_max_aggregates.size();

				for (auto &aggr : aggr_functions) {
					FunctionBinder function_binder(context);
					vector<unique_ptr<Expression>> aggr_children;
					unique_ptr<Expression> bound_col_expr = make_uniq<BoundColumnRefExpression>(type, expr);
					aggr_children.push_back(std::move(bound_col_expr));
					auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
					                                                       AggregateType::NON_DISTINCT);
					min_max_aggregates.push_back(std::move(aggr_expr));
				}
			}
		}

		global_aggregate_state =
		    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context), min_max_aggregates);
	}

	void FinalizeMinMax() {
		vector<LogicalType> min_max_types;
		for (auto &aggr_expr : min_max_aggregates) {
			min_max_types.push_back(aggr_expr->GetReturnType());
		}
		auto final_min_max = make_uniq<DataChunk>();
		final_min_max->Initialize(Allocator::DefaultAllocator(), min_max_types);
		global_aggregate_state->Finalize(*final_min_max);

		// create a filter for each of the aggregates
		for (idx_t idx = 0; idx < op.filter_plans.size(); idx++) {
			auto &filter_plan = op.filter_plans[idx];
			auto &dynamic_filters = op.min_max_to_create[idx];
			auto &applied_bindings = op.min_max_applied_cols[idx];
			if (dynamic_filters == nullptr) {
				continue;
			}

			for (idx_t expr_idx = 0; expr_idx < filter_plan.bound_cols.size(); expr_idx++) {
				auto apply_col_index = applied_bindings[expr_idx].column_index;
				idx_t build_col_index = filter_plan.bound_cols[expr_idx];
				idx_t agg_idx = col_to_expr[build_col_index];

				auto min_val = final_min_max->data[agg_idx].GetValue(0);
				auto max_val = final_min_max->data[agg_idx + 1].GetValue(0);

				if (min_val.IsNull() || max_val.IsNull()) {
					// it means that no rows can pass the min-max filter...
					min_val = Value::MaximumValue(min_val.type());
					max_val = Value::MinimumValue(max_val.type());
				}

				if (min_val.type().IsIntegral() && max_val.type() == min_val.type() &&
				    min_val.type() != LogicalType::HUGEINT && min_val.type() != LogicalType::UHUGEINT) {
					int64_t lower = IntegralValue::Get(min_val).lower;
					int64_t upper = IntegralValue::Get(max_val).lower;
					val_ranges[build_col_index] = make_pair(lower, upper);
				}
			}
		}
	}

	void FinalizeOrFilter() {
		// create a filter for each of the aggregates
		idx_t num_rows = data_collection->Count();
		auto dynamic_or_filter_threshold = Settings::Get<DynamicOrFilterThresholdSetting>(context);
		// std::cerr << "FinalizeOrFilter: " << num_rows << ", " << dynamic_or_filter_threshold << std::endl;
		if (num_rows > dynamic_or_filter_threshold) {
			return;
		}
		if (!op.config.use_dynamic_in_filter) {
			return;
		}
		DataChunk chunk;
		data_collection->InitializeScanChunk(chunk);
		const auto chunk_count = data_collection->ChunkCount();
		unordered_map<idx_t, value_set_t> distinct_value_map;
		for (idx_t i = 0; i < chunk_count; i++) {
			data_collection->FetchChunk(i, chunk);
			for (auto &col_pair : col_to_expr) {
				auto col_idx = col_pair.first;
				for (int j = 0; j < chunk.size(); j++) {
					distinct_value_map[col_idx].emplace(chunk.GetValue(col_idx, j));
				}
			}
		}
		for (idx_t idx = 0; idx < op.filter_plans.size(); idx++) {
			auto &filter_plan = op.filter_plans[idx];
			auto &dynamic_filters = op.min_max_to_create[idx];
			auto &applied_bindings = op.min_max_applied_cols[idx];
			if (dynamic_filters == nullptr) {
				continue;
			}

			for (idx_t expr_idx = 0; expr_idx < filter_plan.bound_cols.size(); expr_idx++) {
				auto apply_col_index = applied_bindings[expr_idx].column_index;
				idx_t build_col_index = filter_plan.bound_cols[expr_idx];

				auto &distinct_values = distinct_value_map[build_col_index];
				vector<Value> in_list(distinct_values.begin(), distinct_values.end());

				if (in_list.empty() || FilterCombiner::ContainsNull(in_list) || FilterCombiner::IsDenseRange(in_list)) {
					continue;
				}

				auto input_type = filter_plan.logical_plan->return_types[expr_idx];
				unique_ptr<Expression> column =
				    make_uniq<BoundReferenceExpression>(input_type, storage_t(0));
				auto in_expression = ExpressionFilter::CreateInExpression(std::move(column), std::move(in_list));
				auto optional_expression =
				    CreateOptionalFilterExpression(std::move(in_expression), input_type);
				auto filter = make_uniq<ExpressionFilter>(std::move(optional_expression));
				dynamic_filters->PushFilter(op, apply_col_index, std::move(filter));
			}
		}
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);

public:
	ClientContext &context;
	const PhysicalCreateBF &op;

	const idx_t num_threads;
	unique_ptr<ColumnDataCollection> data_collection;
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;

	//! Global Min/Max aggregates for filter pushdown
	unordered_map<idx_t, idx_t> col_to_expr;
	unique_ptr<GlobalUngroupedAggregateState> global_aggregate_state;
	vector<unique_ptr<Expression>> min_max_aggregates;

	//! If memory is not enough, give up creating BFs
	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	// runtime statistics
	atomic<bool> is_selectivity_checked;
	atomic<int64_t> num_input_rows;
	atomic<int64_t> total_row_size;
	unordered_map<idx_t, pair<int64_t, int64_t>> val_ranges;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op) : client_context(context) {
		auto &gstate = op.sink_state->Cast<CreateBFGlobalSinkState>();
		local_data = make_uniq<ColumnDataCollection>(context, op.types);
		col_to_expr = gstate.col_to_expr;
		local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*gstate.global_aggregate_state);

		// How much is the memory that used to materialize the table and build BFs?
		temporary_memory_state = TemporaryMemoryManager::Get(context).Register(context);
		temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, op.GetMaxThreadMemory(context));
	}

	void SinkMinMaxFilter(DataChunk &chunk) const {
		for (auto &pair : col_to_expr) {
			idx_t col_idx = pair.first;
			idx_t expr_idx = pair.second;

			// min and max
			local_aggregate_state->Sink(chunk, col_idx, expr_idx, chunk.size());
			local_aggregate_state->Sink(chunk, col_idx, expr_idx + 1, chunk.size());
		}
	}

	ClientContext &client_context;
	unique_ptr<ColumnDataCollection> local_data;

	//! Local Min/Max aggregates for filter pushdown
	unordered_map<idx_t, idx_t> col_to_expr;
	unique_ptr<LocalUngroupedAggregateState> local_aggregate_state;

	//! If memory is not enough, give up creating BFs
	unique_ptr<TemporaryMemoryState> temporary_memory_state;
};

bool PhysicalCreateBF::GiveUpBFCreation(const DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<CreateBFLocalSinkState>();
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();

	// Stop: OOM
	if (lstate.local_data->AllocationSize() + chunk.GetAllocationSize() >=
	    lstate.temporary_memory_state->GetReservation()) {
		is_successful = false;
		return true;
	}

	/*
	// Early Stop: Unfiltered Table or estimated OOM
	if (!gstate.is_selectivity_checked) {
	    gstate.num_input_rows += static_cast<int64_t>(chunk.size());
	    gstate.total_row_size += static_cast<int64_t>(chunk.GetAllocationSize());

	    if (this_pipeline->num_source_chunks > 32) {
	        gstate.is_selectivity_checked = true;

	        // 0. Since one Bloom filter is already created from this table, we believe it is worthy to create the
	        // second, because the second bloom filter is only created in the backward stage and is to be distributed.
	        if (gstate.op.this_pipeline->GetSource()->type == PhysicalOperatorType::CREATE_BF) {
	            return false;
	        }

	        // 1. Check the selectivity: a high selectivity means that the base table is not filtered. It is not
	        // beneficial to build a BF on a full table.
	        ProgressData progress;
	        this_pipeline->GetProgress(progress);
	        double progress_percent = progress.done / progress.total;
	        double input_rows = static_cast<double>(gstate.num_input_rows);
	        double source_rows = static_cast<double>(this_pipeline->num_source_chunks * STANDARD_VECTOR_SIZE);
	        double selectivity = input_rows / source_rows;
	        double row_length = static_cast<double>(gstate.total_row_size) / input_rows;

	        // TODO: Currently, the number of threads affect the accuracy of progress percent.
	        if (gstate.num_threads > 8) {
	            if (selectivity > 0.35 || (row_length > 40 && selectivity > 0.2)) {
	                is_successful = false;
	                return true;
	            }
	        } else {
	            if (progress_percent < 0.65 && (selectivity > 0.35 || (row_length > 40 && selectivity > 0.2))) {
	                is_successful = false;
	                return true;
	            }
	        }

	        // 2. Estimate the lower bound of required memory, which is used to materialize this table. If it is very
	        // large, give up creating BF.
	        double estimated_num_rows = static_cast<double>(gstate.num_input_rows) / progress_percent;
	        idx_t per_tuple_size = chunk.GetAllocationSize() / chunk.size();
	        idx_t estimated_required_memory = static_cast<idx_t>(estimated_num_rows) * per_tuple_size;
	        if (estimated_required_memory >= lstate.temporary_memory_state->GetReservation()) {
	            is_successful = false;
	            return true;
	        }
	    }
	} */

	return false;
}

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();

	if (!is_successful || GiveUpBFCreation(chunk, input)) {
		return SinkResultType::FINISHED;
	}

	// Bloomfilter
	state.local_data->Append(chunk);

	// min-max
	for (auto &pair : state.col_to_expr) {
		idx_t col_idx = pair.first;
		idx_t expr_idx = pair.second;

		state.local_aggregate_state->Sink(chunk, col_idx, expr_idx, chunk.size());
		state.local_aggregate_state->Sink(chunk, col_idx, expr_idx + 1, chunk.size());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();

	// give up creating filters
	if (!is_successful) {
		return SinkCombineResultType::FINISHED;
	}

	// min-max aggregation
	gstate.global_aggregate_state->Combine(*state.local_aggregate_state);

	lock_guard<annotated_mutex> guard(gstate.lock);
	size_t global_size = gstate.temporary_memory_state->GetMinimumReservation() + state.local_data->SizeInBytes();
	gstate.temporary_memory_state->SetMinimumReservation(global_size);
	gstate.temporary_memory_state->SetRemainingSizeAndUpdateReservation(context.client, global_size);

	gstate.local_data_collections.push_back(std::move(state.local_data));
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
//! If we have only one thread, always finalize single-threaded.
static bool FinalizeSingleThreaded(const CreateBFGlobalSinkState &sink) {
	// if only one thread, finalize single-threaded
	const auto num_threads = NumericCast<idx_t>(sink.num_threads);
	if (num_threads == 1) {
		return true;
	}

	// if we want to verify parallelism, finalize parallel
	if (sink.context.config.verify_parallelism) {
		return false;
	}

	if (sink.data_collection->Count() < 1048576) {
		return true;
	}

	return false;
}

class CreateBFFinalizeTask : public ExecutorTask {
public:
	CreateBFFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, CreateBFGlobalSinkState &sink_p,
	                     idx_t chunk_idx_from_p, idx_t chunk_idx_to_p)
	    : ExecutorTask(context, std::move(event_p), sink_p.op), sink(sink_p), chunk_idx_from(chunk_idx_from_p),
	      chunk_idx_to(chunk_idx_to_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		DataChunk chunk;
		sink.data_collection->InitializeScanChunk(chunk);
		for (idx_t i = chunk_idx_from; i < chunk_idx_to; i++) {
			sink.data_collection->FetchChunk(i, chunk);
			for (auto &pair : sink.op.unique_rpt_filters) {
				auto &cols_build = pair.first;
				auto &bf = pair.second->filter;
				bf->Insert(chunk, cols_build);
			}
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	CreateBFGlobalSinkState &sink;
	idx_t chunk_idx_from;
	idx_t chunk_idx_to;
};

class CreateBFFinalizeEvent : public BasePipelineEvent {
public:
	CreateBFFinalizeEvent(Pipeline &pipeline_p, CreateBFGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	CreateBFGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> finalize_tasks;
		auto &collection = sink.data_collection;
		const auto chunk_count = collection->ChunkCount();

		if (FinalizeSingleThreaded(sink)) {
			// Single-threaded finalize
			finalize_tasks.push_back(
			    make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink, 0U, chunk_count));
		} else {
			// Parallel finalize
			const idx_t chunks_per_task = context.config.verify_parallelism ? 1 : CHUNKS_PER_TASK;
			for (idx_t chunk_idx = 0; chunk_idx < chunk_count; chunk_idx += chunks_per_task) {
				auto chunk_idx_to = MinValue<idx_t>(chunk_idx + chunks_per_task, chunk_count);
				finalize_tasks.push_back(
				    make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink, chunk_idx, chunk_idx_to));
			}
		}
		SetTasks(std::move(finalize_tasks));
	}

	void FinishEvent() override {
		for (auto &pair : sink.op.unique_rpt_filters) {
			auto &bf = pair.second;
			bf->filter->SetValid();
		}
	}

	static constexpr idx_t CHUNKS_PER_TASK = 64;
};

void CreateBFGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	auto new_event = make_shared_ptr<CreateBFFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(new_event));
}

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<CreateBFGlobalSinkState>();

	// Give up creating filters
	if (!is_successful) {
		return SinkFinalizeType::READY;
	}

	// Finalize min-max
	sink.FinalizeMinMax();

	// Collect local data
	for (auto &local_data : sink.local_data_collections) {
		sink.data_collection->Combine(*local_data);
	}
	sink.local_data_collections.clear();
	sink.FinalizeOrFilter();

	// Initialize the bloom filter
	uint32_t num_rows = static_cast<uint32_t>(sink.data_collection->Count());
	for (auto &pair : unique_rpt_filters) {
		auto &bf = pair.second;
		auto id = pair.first;
		if (config.use_bitmap_filter && id.size() == 1 && sink.val_ranges.count(id[0])) {
			auto range = sink.val_ranges[id[0]];
			if (range.second >= range.first &&
			    (range.second - range.first <= 128 * num_rows || range.second - range.first <= 8e6)) {
				bf->filter = make_shared_ptr<BitmapFilter>(context, BitmapFilterConfig(range.first, range.second));
			} else if (range.second < range.first) { // empty?
				bf->filter = make_shared_ptr<BitmapFilter>(context, BitmapFilterConfig(range.first, range.second));
			}
		}
		if (!bf->filter) {
			bf->filter = make_shared_ptr<ActiveBloomFilter>(context, BloomFilterConfig(), num_rows);
		}
	}

	sink.ScheduleFinalize(pipeline, event);
	// set dynamic filter?

	if (config.use_table_filter_bf) {
		for (idx_t idx = 0; idx < filter_plans.size(); idx++) {
			auto &filter_plan = filter_plans[idx];
			auto &dynamic_filters = min_max_to_create[idx];
			auto &applied_bindings = min_max_applied_cols[idx];
			// key contains only one column
			// can use dynamic filter
			// TODO
			if (applied_bindings.size() == 1) {
				// std::cerr << applied_bindings[0].ToString() << " " << size_t(bf_to_create[idx]->GetFilter().get()) <<
				// " " << ((BitmapFilter*)bf_to_create[idx]->GetFilter().get())->GetConfig().lower_bound << ", " <<
				// ((BitmapFilter*)bf_to_create[idx]->GetFilter().get())->GetConfig().upper_bound << std::endl;
				dynamic_filters->PushFilter(*this, applied_bindings[0].column_index,
				                            RPTTableFilter::MakeOptional(bf_to_create[idx]->GetFilter(),
				                                                         filter_plan.logical_plan->return_types[0]));
			}
		}
	}

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateBFGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateBFLocalSinkState>(context.client, *this);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateBFGlobalSourceState : public GlobalSourceState {
public:
	explicit CreateBFGlobalSourceState(const ColumnDataCollection &collection)
	    : max_threads(MaxValue<idx_t>(collection.ChunkCount(), 1)), data_collection(collection) {
		collection.InitializeScan(global_scan_state);
	}

	idx_t MaxThreads() override {
		return max_threads;
	}

	const idx_t max_threads;
	const ColumnDataCollection &data_collection;
	ColumnDataParallelScanState global_scan_state;
};

class CreateBFLocalSourceState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

InsertionOrderPreservingMap<string> PhysicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;

	result["Name"] = "PhysicalRPTCreateBF";
	result["BF Number"] = std::to_string(bf_to_create.size());
	result["ID"] = "0x" + std::to_string(reinterpret_cast<size_t>(this));

	string min_max_filter;
	for (auto &filter : min_max_applied_cols) {
		for (auto &v : filter) {
			min_max_filter += std::to_string(v.table_index.index) + "." + std::to_string(v.column_index) + " ";
		}
	}
	result["Min-Max Filter"] = min_max_filter;

	return result;
}

unique_ptr<GlobalSourceState> PhysicalCreateBF::GetGlobalSourceState(ClientContext &context) const {
	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	return make_uniq<CreateBFGlobalSourceState>(*gstate.data_collection);
}

unique_ptr<LocalSourceState> PhysicalCreateBF::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<CreateBFLocalSourceState>();
}

SourceResultType PhysicalCreateBF::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSourceState>();
	auto &lstate = input.local_state.Cast<CreateBFLocalSourceState>();
	gstate.data_collection.Scan(gstate.global_scan_state, lstate.local_scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	// operator is a sink, build a pipeline
	D_ASSERT(children.size() == 1);

	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();

		child_meta_pipeline.Build(children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}

ProgressData PhysicalCreateBF::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	auto &source_state = gstate.Cast<CreateBFGlobalSourceState>();

	ProgressData res;
	res.total = static_cast<double>(source_state.data_collection.Count());

	lock_guard<mutex> guard(source_state.global_scan_state.lock);
	res.done = static_cast<double>(source_state.global_scan_state.scan_state.current_row_index);
	return res;
}

void PhysicalCreateBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);
	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();

		child_meta_pipeline.Build(children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}
} // namespace duckdb
