#include "predicate_transfer/cardinality_estimation/instant_sampler/common.hpp"

#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {
namespace {

using instant_sampler_internal::AllocateProportionalQuotas;
using instant_sampler_internal::DirectStorageScanTiming;
using instant_sampler_internal::INSTANT_PARQUET_TASK_LIMIT;
using instant_sampler_internal::SelectStratifiedExactlyK;

static optional_idx FindVirtualColumn(const LogicalGet &get, const string &name) {
	for (auto &entry : get.virtual_columns) {
		if (StringUtil::CIEquals(entry.second.name.GetIdentifierName(), name)) {
			return optional_idx(entry.first);
		}
	}
	return optional_idx();
}

static unique_ptr<Expression> BuildRowNumberRangeFilter(const vector<InstantSampleRange> &ranges) {
	vector<unique_ptr<Expression>> range_expressions;
	range_expressions.reserve(ranges.size());
	for (auto &range : ranges) {
		auto lower = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_GREATERTHANOREQUALTO, make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0),
		    make_uniq<BoundConstantExpression>(Value::BIGINT(static_cast<int64_t>(range.row_start))));
		auto upper = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_LESSTHANOREQUALTO, make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0),
		    make_uniq<BoundConstantExpression>(
		        Value::BIGINT(static_cast<int64_t>(range.row_start + range.row_count - 1))));
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		conjunction->GetChildrenMutable().push_back(std::move(lower));
		conjunction->GetChildrenMutable().push_back(std::move(upper));
		range_expressions.push_back(std::move(conjunction));
	}
	if (range_expressions.empty()) {
		return nullptr;
	}
	if (range_expressions.size() == 1) {
		return std::move(range_expressions[0]);
	}
	auto disjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
	for (auto &expression : range_expressions) {
		disjunction->GetChildrenMutable().push_back(std::move(expression));
	}
	return std::move(disjunction);
}

static bool ConfigureInstantParquetSample(ClientContext &context, LogicalGet &get, idx_t target_rows,
                                          idx_t target_row_groups, uint64_t seed, InstantParquetSamplePlan &plan) {
	D_ASSERT(target_rows > 0);
	D_ASSERT(target_row_groups > 0);
	if (!get.function.get_partition_stats) {
		return false;
	}
	auto row_number_column = FindVirtualColumn(get, "file_row_number");
	if (!row_number_column.IsValid()) {
		return false;
	}

	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partitions = get.function.get_partition_stats(context, input);
	plan.total_row_groups = partitions.size();
	if (partitions.empty()) {
		return false;
	}

	// file_row_number is file-relative. Accept only a single, contiguous row
	// coordinate space; multi-file scans reset row_start and must use a future
	// (file_index, row_number) composite filter instead of silently sampling the
	// wrong row groups.
	idx_t total_rows = 0;
	vector<idx_t> sampleable_partitions;
	for (idx_t partition_index = 0; partition_index < partitions.size(); partition_index++) {
		auto &partition = partitions[partition_index];
		if (!partition.row_start.IsValid() || partition.row_start.GetIndex() != total_rows ||
		    partition.count_type != CountType::COUNT_EXACT) {
			return false;
		}
		total_rows += partition.count;
		if (partition.count > 0) {
			sampleable_partitions.push_back(partition_index);
		}
	}
	if (total_rows == 0 || sampleable_partitions.empty()) {
		return false;
	}
	plan.source_rows = total_rows;

	// Every selected row group must contribute at least one row. Besides
	// avoiding zero-quota work, this bounds the filter expression when a caller
	// requests more row groups than sample rows.
	plan.selected_row_groups =
	    MinValue<idx_t>(target_rows, MinValue<idx_t>(target_row_groups, sampleable_partitions.size()));
	std::mt19937_64 random(seed);
	auto selected_positions = SelectStratifiedExactlyK(sampleable_partitions.size(), plan.selected_row_groups, random);
	vector<idx_t> selected;
	selected.reserve(selected_positions.size());
	for (auto position : selected_positions) {
		selected.push_back(sampleable_partitions[position]);
	}
	plan.candidate_rows = 0;
	for (auto partition_index : selected) {
		plan.candidate_rows += partitions[partition_index].count;
	}
	if (plan.candidate_rows == 0) {
		return false;
	}

	auto actual_target = MinValue(target_rows, plan.candidate_rows);
	vector<idx_t> capacities;
	capacities.reserve(selected.size());
	for (auto partition_index : selected) {
		capacities.push_back(partitions[partition_index].count);
	}
	vector<idx_t> quotas(selected.size(), 1);
	if (actual_target > selected.size()) {
		vector<idx_t> remaining_capacities;
		remaining_capacities.reserve(capacities.size());
		for (auto capacity : capacities) {
			D_ASSERT(capacity > 0);
			remaining_capacities.push_back(capacity - 1);
		}
		auto extra_quotas = AllocateProportionalQuotas(remaining_capacities, actual_target - selected.size());
		for (idx_t i = 0; i < quotas.size(); i++) {
			quotas[i] += extra_quotas[i];
		}
	}

	vector<InstantSampleRange> ranges;
	ranges.reserve(selected.size());
	for (idx_t i = 0; i < selected.size(); i++) {
		auto &partition = partitions[selected[i]];
		D_ASSERT(quotas[i] > 0 && quotas[i] <= partition.count);
		if (quotas[i] == partition.count) {
			ranges.push_back({partition.row_start.GetIndex(), partition.count});
			continue;
		}
		// A circular contiguous window gives every row the same inclusion
		// probability while retaining page locality. Only a window crossing the
		// row-group boundary needs a second physical range.
		std::uniform_int_distribution<idx_t> offset_distribution(0, partition.count - 1);
		auto window_offset = offset_distribution(random);
		auto first_count = MinValue<idx_t>(quotas[i], partition.count - window_offset);
		ranges.push_back({partition.row_start.GetIndex() + window_offset, first_count});
		if (first_count < quotas[i]) {
			ranges.push_back({partition.row_start.GetIndex(), quotas[i] - first_count});
		}
	}
	D_ASSERT(!ranges.empty());

	auto &column_ids = get.GetMutableColumnIds();
	auto payload_count = column_ids.size();
	column_ids.emplace_back(row_number_column.GetIndex());
	get.projection_ids.clear();
	for (idx_t i = 0; i < payload_count; i++) {
		get.projection_ids.emplace_back(i);
	}
	plan.ranges = std::move(ranges);
	return true;
}

struct DirectParquetWorkerResult {
	shared_ptr<ColumnDataCollection> sample;
	idx_t sampled_rows = 0;
	DirectStorageScanTiming timing;
};

class DirectParquetSampleSharedState {
public:
	DirectParquetSampleSharedState(ClientContext &context, const TableFunction &function,
	                               unique_ptr<FunctionData> bind_data, vector<ColumnIndex> column_ids,
	                               vector<idx_t> projection_ids, unique_ptr<TableFilterSet> filters,
	                               vector<LogicalType> output_types)
	    : context(context), function(function), bind_data(std::move(bind_data)), column_ids(std::move(column_ids)),
	      projection_ids(std::move(projection_ids)), filters(std::move(filters)),
	      output_types(std::move(output_types)) {
	}

	bool Initialize() {
		TableFunctionInitInput init_input(bind_data.get(), column_ids, projection_ids, filters.get());
		global_state = function.init_global(context, init_input);
		if (!global_state) {
			return false;
		}
		// The sampler drives the table function synchronously. Keep one shared
		// Parquet global state for all local scan states, but do not add a nested
		// read-ahead executor underneath the sampling executor.
		global_state->Cast<MultiFileGlobalState>().read_ahead.reset();
		return true;
	}

	ClientContext &context;
	const TableFunction &function;
	unique_ptr<FunctionData> bind_data;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	unique_ptr<TableFilterSet> filters;
	vector<LogicalType> output_types;
	// Declared last so it is destroyed first, before the non-owning bind/filter
	// pointers retained by MultiFileGlobalState can become invalid.
	unique_ptr<GlobalTableFunctionState> global_state;
};

class DirectParquetSampleWorker {
public:
	DirectParquetSampleWorker(shared_ptr<DirectParquetSampleSharedState> shared_state,
	                          unique_ptr<Expression> local_predicate, bool collect_timing)
	    : shared_state(std::move(shared_state)), local_predicate(std::move(local_predicate)),
	      collect_timing(collect_timing) {
	}

	void Run() {
		auto &shared = *shared_state;
		// Expression execution state is thread-local. Build and destroy it on
		// the same sampling worker instead of moving an executor created by the
		// optimizer thread across thread boundaries.
		auto task_predicate = std::move(local_predicate);
		unique_ptr<ExpressionExecutor> task_filter_executor;
		if (task_predicate) {
			task_filter_executor = make_uniq<ExpressionExecutor>(shared.context);
			task_filter_executor->AddExpression(*task_predicate);
		}
		auto task_started =
		    collect_timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
		auto timing = collect_timing ? &result.timing : nullptr;
		auto local_output = make_shared_ptr<ColumnDataCollection>(Allocator::DefaultAllocator(), shared.output_types);
		ColumnDataAppendState append_state;
		local_output->InitializeAppend(append_state);

		ThreadContext thread_context(shared.context);
		ExecutionContext execution_context(shared.context, thread_context, nullptr);
		TableFunctionInitInput init_input(shared.bind_data.get(), shared.column_ids, shared.projection_ids,
		                                  shared.filters.get());
		auto local_state = shared.function.init_local(execution_context, init_input, shared.global_state.get());
		if (!local_state) {
			result.sample = std::move(local_output);
			return;
		}

		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), shared.output_types);
		SelectionVector selection(STANDARD_VECTOR_SIZE);
		idx_t task_sampled_rows = 0;
		while (true) {
			chunk.Reset();
			TableFunctionInput input(shared.bind_data.get(), local_state.get(), shared.global_state.get());
			input.async_result = AsyncResultType::IMPLICIT;
			input.results_execution_mode = AsyncResultsExecutionMode::SYNCHRONOUS;
			auto decode_started = timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
			shared.function.function(shared.context, input, chunk);
			if (timing) {
				timing->decode_ms +=
				    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - decode_started)
				        .count();
			}

			auto result_type = input.async_result.GetResultType();
			if (result_type == AsyncResultType::BLOCKED || result_type == AsyncResultType::INVALID) {
				throw InternalException("Direct Parquet sample returned an invalid synchronous scan state");
			}
			if (chunk.size() > 0) {
				task_sampled_rows += chunk.size();
				if (task_filter_executor) {
					auto filter_started =
					    timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
					auto survivor_count = task_filter_executor->SelectExpression(chunk, selection);
					if (timing) {
						timing->filter_ms +=
						    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - filter_started)
						        .count();
					}
					if (survivor_count == 0) {
						if (result_type == AsyncResultType::FINISHED) {
							break;
						}
						continue;
					}
					if (survivor_count < chunk.size()) {
						chunk.Slice(selection, survivor_count);
						chunk.Flatten();
					}
				}
				auto append_started =
				    timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
				local_output->Append(append_state, chunk);
				if (timing) {
					timing->append_ms +=
					    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - append_started)
					        .count();
				}
			}

			if (result_type == AsyncResultType::FINISHED ||
			    (result_type == AsyncResultType::IMPLICIT && chunk.size() == 0)) {
				break;
			}
			if (result_type == AsyncResultType::HAVE_MORE_OUTPUT && chunk.size() == 0) {
				throw InternalException("Direct Parquet sample produced empty HAVE_MORE_OUTPUT");
			}
		}

		result.sampled_rows = task_sampled_rows;
		if (timing) {
			result.timing.task_wall_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - task_started).count();
		}
		result.sample = std::move(local_output);
	}

	const DirectParquetWorkerResult &GetResult() const {
		return result;
	}

private:
	shared_ptr<DirectParquetSampleSharedState> shared_state;
	unique_ptr<Expression> local_predicate;
	bool collect_timing;
	DirectParquetWorkerResult result;
};

class DirectParquetSampleTask : public BaseExecutorTask {
public:
	DirectParquetSampleTask(TaskExecutor &executor, DirectParquetSampleWorker &worker)
	    : BaseExecutorTask(executor), worker(worker) {
	}

	void ExecuteTask() override {
		worker.Run();
	}

	string TaskType() const override {
		return "RPTDirectParquetSample";
	}

private:
	DirectParquetSampleWorker &worker;
};

} // namespace

InstantSampleResult BuildInstantParquetSample(ClientContext &context, LogicalGet &get,
                                              const InstantParquetSamplePlan &plan, bool collect_timing,
                                              const Expression *local_predicate) {
	InstantSampleResult result;
	result.source = InstantSampleSource::PARQUET;
	result.total_row_groups = plan.total_row_groups;
	result.selected_row_groups = plan.selected_row_groups;
	result.candidate_rows = plan.candidate_rows;
	result.sample = make_shared_ptr<ColumnDataCollection>(Allocator::DefaultAllocator(), plan.output_types);
	D_ASSERT(get.function.function);
	D_ASSERT(get.function.init_global);
	D_ASSERT(get.function.init_local);
	D_ASSERT(get.bind_data);
	D_ASSERT(plan);
	D_ASSERT(!plan.output_types.empty());
	if (!get.function.function || !get.function.init_global || !get.function.init_local || !get.bind_data || !plan ||
	    plan.output_types.empty()) {
		throw InternalException("Parquet instant sample is missing a required scan callback or bind data");
	}
	D_ASSERT(!plan.ranges.empty());
	if (plan.ranges.empty()) {
		throw InternalException("Parquet instant sample has no row ranges");
	}

	vector<idx_t> projection_ids;
	projection_ids.reserve(get.projection_ids.size());
	for (auto projection_id : get.projection_ids) {
		projection_ids.push_back(projection_id.GetIndex());
	}

	auto scan_started = std::chrono::steady_clock::now();
	auto row_filter = BuildRowNumberRangeFilter(plan.ranges);
	D_ASSERT(row_filter);
	auto filters = make_uniq<TableFilterSet>();
	filters->PushFilter(ProjectionIndex(plan.output_types.size()), make_uniq<ExpressionFilter>(std::move(row_filter)));
	auto bind_data = get.bind_data->Copy();
	if (!bind_data) {
		throw InternalException("Parquet instant sampler could not copy bind data");
	}
	auto shared_state = make_shared_ptr<DirectParquetSampleSharedState>(context, get.function, std::move(bind_data),
	                                                                    get.GetColumnIds(), std::move(projection_ids),
	                                                                    std::move(filters), plan.output_types);
	if (!shared_state->Initialize()) {
		throw InternalException("Parquet instant sample could not initialize its global scan state");
	}

	// Row-group initialization and positioning dominate tiny Parquet samples,
	// so preserve one parallel work unit per selected group up to the bound.
	auto task_count = MinValue<idx_t>(INSTANT_PARQUET_TASK_LIMIT, plan.selected_row_groups);
	D_ASSERT(task_count > 0);
	result.task_count = task_count;
	vector<unique_ptr<DirectParquetSampleWorker>> workers;
	workers.reserve(task_count);
	for (idx_t task = 0; task < task_count; task++) {
		workers.push_back(make_uniq<DirectParquetSampleWorker>(
		    shared_state, local_predicate ? local_predicate->Copy() : nullptr, collect_timing));
	}

	if (task_count == 1) {
		auto wait_started = std::chrono::steady_clock::now();
		workers.front()->Run();
		if (collect_timing) {
			result.wait_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - wait_started).count();
		}
	} else {
		auto scheduler_setup_started = std::chrono::steady_clock::now();
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		if (collect_timing) {
			result.scheduler_setup_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - scheduler_setup_started)
			        .count();
		}
		auto schedule_started = std::chrono::steady_clock::now();
		for (auto &worker : workers) {
			executor.ScheduleTask(make_uniq<DirectParquetSampleTask>(executor, *worker));
		}
		if (collect_timing) {
			result.schedule_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - schedule_started).count();
		}
		auto wait_started = std::chrono::steady_clock::now();
		executor.WorkOnTasks();
		if (collect_timing) {
			result.wait_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - wait_started).count();
		}
	}

	if (collect_timing) {
		for (auto &worker : workers) {
			auto &timing = worker->GetResult().timing;
			result.decode_ms += timing.decode_ms;
			result.filter_ms += timing.filter_ms;
			result.append_ms += timing.append_ms;
			result.task_wall_ms += timing.task_wall_ms;
		}
	}
	for (auto &worker : workers) {
		result.sampled_rows += worker->GetResult().sampled_rows;
	}
	auto combine_started = std::chrono::steady_clock::now();
	for (auto &worker : workers) {
		if (worker->GetResult().sample) {
			result.sample->Combine(*worker->GetResult().sample);
		}
	}
	if (collect_timing) {
		result.combine_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - combine_started).count();
	}
	result.scan_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - scan_started).count();
	result.decoded_rows = result.sampled_rows;
	result.status = InstantSampleStatus::SUCCESS;
	return result;
}

InstantParquetSamplePlan PlanInstantParquetSample(ClientContext &context, LogicalGet &get, idx_t target_rows,
                                                  idx_t target_row_groups, uint64_t seed) {
	InstantParquetSamplePlan plan;
	for (auto &column_id : get.GetColumnIds()) {
		plan.output_types.push_back(get.GetColumnType(column_id));
	}
	plan.valid = ConfigureInstantParquetSample(context, get, target_rows, target_row_groups, seed, plan);
	return plan;
}

} // namespace duckdb
