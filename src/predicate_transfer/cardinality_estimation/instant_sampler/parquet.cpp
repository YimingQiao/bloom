#include "predicate_transfer/cardinality_estimation/instant_sampler/common.hpp"

#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {
namespace {

using instant_sampler_internal::AllocateProportionalQuotas;
using instant_sampler_internal::DirectStorageScanTiming;
using instant_sampler_internal::SelectStratifiedExactlyK;

static idx_t ParquetTaskCount(ClientContext &context, idx_t work_units) {
	D_ASSERT(work_units > 0);
	auto async_threads = TaskScheduler::GetScheduler(context).NumberOfAsyncThreads();
	return MinValue<idx_t>(work_units, MaxValue<idx_t>(async_threads, 1));
}

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

struct ParquetRowGroup {
	idx_t file_index;
	idx_t row_start;
	idx_t row_count;
};

struct ParquetMetadataLoadJob {
	ClientContext &context;
	const TableFunction &function;
	const MultiFileBindData &source_bind;
	const vector<OpenFileInfo> &files;
	vector<vector<InstantSampleRange>> &file_row_groups;
	vector<uint8_t> &valid_files;
	idx_t task_count;
};

static void LoadParquetMetadataSlice(idx_t task_id, ParquetMetadataLoadJob &job,
                                     unique_ptr<FunctionData> partition_bind_data) {
	auto &partition_bind = partition_bind_data->Cast<MultiFileBindData>();
	GlobalTableFunctionState reader_state;
	auto file_begin = task_id * job.files.size() / job.task_count;
	auto file_end = (task_id + 1) * job.files.size() / job.task_count;
	for (idx_t file_index = file_begin; file_index < file_end; file_index++) {
		shared_ptr<BaseFileReader> reader;
		if (job.source_bind.union_readers.size() == job.files.size()) {
			reader = job.source_bind.multi_file_reader->CreateReader(
			    job.context, reader_state, *job.source_bind.union_readers[file_index], job.source_bind);
		} else if (file_index == 0 && job.source_bind.initial_reader &&
		           job.source_bind.initial_reader->GetFileName() == job.files[file_index].path) {
			reader = job.source_bind.initial_reader;
		} else {
			reader = job.source_bind.multi_file_reader->CreateReader(job.context, reader_state, job.files[file_index],
			                                                         file_index, job.source_bind);
		}
		if (!reader) {
			throw InternalException("Parquet instant sampler could not open metadata reader");
		}

		vector<OpenFileInfo> one_file;
		one_file.push_back(job.files[file_index]);
		partition_bind.file_list = make_shared_ptr<SimpleMultiFileList>(std::move(one_file));
		partition_bind.initial_reader = std::move(reader);
		{
			GetPartitionStatsInput input(job.function, &partition_bind);
			auto partitions = job.function.get_partition_stats(job.context, input);
			auto &row_groups = job.file_row_groups[file_index];
			row_groups.reserve(partitions.size());
			idx_t expected_row_start = 0;
			bool valid = true;
			for (auto &partition : partitions) {
				if (!partition.row_start.IsValid() || partition.row_start.GetIndex() != expected_row_start ||
				    partition.count_type != CountType::COUNT_EXACT) {
					valid = false;
					break;
				}
				row_groups.push_back({partition.row_start.GetIndex(), partition.count});
				expected_row_start += partition.count;
			}
			if (!valid) {
				row_groups.clear();
				job.valid_files[file_index] = 0;
			}
		}
		partition_bind.initial_reader.reset();
	}
}

class ParquetMetadataLoadTask : public BaseExecutorTask {
public:
	ParquetMetadataLoadTask(TaskExecutor &executor, idx_t task_id, ParquetMetadataLoadJob &job,
	                        unique_ptr<FunctionData> partition_bind_data)
	    : BaseExecutorTask(executor), task_id(task_id), job(job), partition_bind_data(std::move(partition_bind_data)) {
	}

	void ExecuteTask() override {
		LoadParquetMetadataSlice(task_id, job, std::move(partition_bind_data));
	}

	string TaskType() const override {
		return "RPTParquetMetadataLoad";
	}

private:
	idx_t task_id;
	ParquetMetadataLoadJob &job;
	unique_ptr<FunctionData> partition_bind_data;
};

static unique_ptr<FunctionData> CreatePartitionStatsBind(const MultiFileBindData &source_bind) {
	auto bind_data = source_bind.Copy();
	if (!bind_data) {
		throw InternalException("Parquet instant sampler could not copy metadata bind data");
	}
	auto &partition_bind = bind_data->Cast<MultiFileBindData>();
	partition_bind.initial_reader.reset();
	partition_bind.union_readers.clear();
	return bind_data;
}

static bool LoadMultiFileParquetRowGroups(ClientContext &context, LogicalGet &get, MultiFileBindData &source_bind,
                                          const vector<OpenFileInfo> &files,
                                          vector<vector<InstantSampleRange>> &file_row_groups) {
	D_ASSERT(files.size() > 1);
	file_row_groups.resize(files.size());
	vector<uint8_t> valid_files(files.size(), 1);
	auto task_count = ParquetTaskCount(context, files.size());
	D_ASSERT(task_count > 0);
	ParquetMetadataLoadJob job {context, get.function, source_bind, files, file_row_groups, valid_files, task_count};

	if (task_count == 1) {
		LoadParquetMetadataSlice(0, job, CreatePartitionStatsBind(source_bind));
	} else {
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		for (idx_t task_id = 0; task_id < task_count; task_id++) {
			auto partition_bind = CreatePartitionStatsBind(source_bind);
			executor.ScheduleTask(
			    make_uniq<ParquetMetadataLoadTask>(executor, task_id, job, std::move(partition_bind)));
		}
		executor.WorkOnTasks();
	}
	for (auto valid : valid_files) {
		if (!valid) {
			return false;
		}
	}
	return true;
}

static bool CollectParquetRowGroups(ClientContext &context, LogicalGet &get, InstantParquetSamplePlan &plan,
                                    vector<ParquetRowGroup> &row_groups) {
	if (!get.bind_data) {
		return false;
	}
	auto &multi_bind = get.bind_data->Cast<MultiFileBindData>();
	auto expand_result = multi_bind.file_list->GetExpandResult();
	if (expand_result == FileExpandResult::NO_FILES) {
		return false;
	}

	if (expand_result == FileExpandResult::SINGLE_FILE) {
		plan.total_files = 1;
		GetPartitionStatsInput input(get.function, get.bind_data.get());
		auto partitions = get.function.get_partition_stats(context, input);
		row_groups.reserve(partitions.size());
		idx_t expected_row_start = 0;
		for (auto &partition : partitions) {
			if (!partition.row_start.IsValid() || partition.row_start.GetIndex() != expected_row_start ||
			    partition.count_type != CountType::COUNT_EXACT) {
				return false;
			}
			row_groups.push_back({0, partition.row_start.GetIndex(), partition.count});
			expected_row_start += partition.count;
		}
		return !row_groups.empty();
	}

	auto files = multi_bind.file_list->GetAllFiles();
	if (files.size() <= 1) {
		throw InternalException("Parquet multi-file expansion did not produce multiple files");
	}
	plan.total_files = files.size();
	vector<vector<InstantSampleRange>> file_row_groups;
	if (!LoadMultiFileParquetRowGroups(context, get, multi_bind, files, file_row_groups)) {
		return false;
	}
	for (idx_t file_index = 0; file_index < file_row_groups.size(); file_index++) {
		for (auto &row_group : file_row_groups[file_index]) {
			row_groups.push_back({file_index, row_group.row_start, row_group.row_count});
		}
	}
	return !row_groups.empty();
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

	vector<ParquetRowGroup> row_groups;
	if (!CollectParquetRowGroups(context, get, plan, row_groups)) {
		return false;
	}
	plan.total_row_groups = row_groups.size();
	idx_t total_rows = 0;
	vector<idx_t> sampleable_partitions;
	for (idx_t partition_index = 0; partition_index < row_groups.size(); partition_index++) {
		total_rows += row_groups[partition_index].row_count;
		if (row_groups[partition_index].row_count > 0) {
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
		plan.candidate_rows += row_groups[partition_index].row_count;
	}
	if (plan.candidate_rows == 0) {
		return false;
	}

	auto actual_target = MinValue(target_rows, plan.candidate_rows);
	vector<idx_t> capacities;
	capacities.reserve(selected.size());
	for (auto partition_index : selected) {
		capacities.push_back(row_groups[partition_index].row_count);
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

	vector<vector<InstantSampleRange>> ranges_by_file(plan.total_files);
	vector<idx_t> selected_groups_by_file(plan.total_files, 0);
	for (idx_t i = 0; i < selected.size(); i++) {
		auto &partition = row_groups[selected[i]];
		D_ASSERT(partition.file_index < ranges_by_file.size());
		D_ASSERT(quotas[i] > 0 && quotas[i] <= partition.row_count);
		selected_groups_by_file[partition.file_index]++;
		auto &ranges = ranges_by_file[partition.file_index];
		if (quotas[i] == partition.row_count) {
			ranges.push_back({partition.row_start, partition.row_count});
			continue;
		}
		// A circular contiguous window gives every row the same inclusion
		// probability while retaining page locality. Only a window crossing the
		// row-group boundary needs a second physical range.
		std::uniform_int_distribution<idx_t> offset_distribution(0, partition.row_count - 1);
		auto window_offset = offset_distribution(random);
		auto first_count = MinValue<idx_t>(quotas[i], partition.row_count - window_offset);
		ranges.push_back({partition.row_start + window_offset, first_count});
		if (first_count < quotas[i]) {
			ranges.push_back({partition.row_start, quotas[i] - first_count});
		}
	}
	for (idx_t file_index = 0; file_index < ranges_by_file.size(); file_index++) {
		if (ranges_by_file[file_index].empty()) {
			continue;
		}
		plan.files.push_back({file_index, selected_groups_by_file[file_index], std::move(ranges_by_file[file_index])});
	}
	D_ASSERT(!plan.files.empty());

	auto &column_ids = get.GetMutableColumnIds();
	auto payload_count = column_ids.size();
	column_ids.emplace_back(row_number_column.GetIndex());
	get.projection_ids.clear();
	for (idx_t i = 0; i < payload_count; i++) {
		get.projection_ids.emplace_back(i);
	}
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
	DirectParquetSampleWorker(vector<shared_ptr<DirectParquetSampleSharedState>> shared_states,
	                          unique_ptr<Expression> local_predicate, bool collect_timing)
	    : shared_states(std::move(shared_states)), local_predicate(std::move(local_predicate)),
	      collect_timing(collect_timing) {
	}

	void Run() {
		D_ASSERT(!shared_states.empty());
		auto &first = *shared_states.front();
		// Expression execution state is thread-local. Build and destroy it on
		// the same sampling worker instead of moving an executor created by the
		// optimizer thread across thread boundaries.
		auto task_predicate = std::move(local_predicate);
		unique_ptr<ExpressionExecutor> task_filter_executor;
		if (task_predicate) {
			task_filter_executor = make_uniq<ExpressionExecutor>(first.context);
			task_filter_executor->AddExpression(*task_predicate);
		}
		auto task_started =
		    collect_timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
		auto timing = collect_timing ? &result.timing : nullptr;
		auto &allocator = BufferAllocator::Get(first.context);
		auto local_output = make_shared_ptr<ColumnDataCollection>(allocator, first.output_types);
		ColumnDataAppendState append_state;
		local_output->InitializeAppend(append_state);

		ThreadContext thread_context(first.context);
		ExecutionContext execution_context(first.context, thread_context, nullptr);
		DataChunk chunk;
		chunk.Initialize(allocator, first.output_types);
		SelectionVector selection(STANDARD_VECTOR_SIZE);
		idx_t task_sampled_rows = 0;
		for (auto &shared_state : shared_states) {
			auto &shared = *shared_state;
			D_ASSERT(&shared.context == &first.context);
			D_ASSERT(shared.output_types == first.output_types);
			TableFunctionInitInput init_input(shared.bind_data.get(), shared.column_ids, shared.projection_ids,
			                                  shared.filters.get());
			auto local_state = shared.function.init_local(execution_context, init_input, shared.global_state.get());
			if (!local_state) {
				continue;
			}

			while (true) {
				chunk.Reset();
				TableFunctionInput input(shared.bind_data.get(), local_state.get(), shared.global_state.get());
				input.async_result = AsyncResultType::IMPLICIT;
				input.results_execution_mode = AsyncResultsExecutionMode::SYNCHRONOUS;
				auto decode_started =
				    timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
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
							timing->filter_ms += std::chrono::duration<double, std::milli>(
							                         std::chrono::steady_clock::now() - filter_started)
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
	vector<shared_ptr<DirectParquetSampleSharedState>> shared_states;
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
	result.sample = make_shared_ptr<ColumnDataCollection>(BufferAllocator::Get(context), plan.output_types);
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
	D_ASSERT(!plan.files.empty());
	if (plan.files.empty()) {
		throw InternalException("Parquet instant sample has no file scan domains");
	}

	vector<idx_t> projection_ids;
	projection_ids.reserve(get.projection_ids.size());
	for (auto projection_id : get.projection_ids) {
		projection_ids.push_back(projection_id.GetIndex());
	}
	auto scan_started = std::chrono::steady_clock::now();
	auto &source_bind = get.bind_data->Cast<MultiFileBindData>();
	vector<OpenFileInfo> source_files;
	if (plan.total_files > 1) {
		source_files = source_bind.file_list->GetAllFiles();
		if (source_files.size() != plan.total_files) {
			throw InternalException("Parquet instant sample file expansion changed between planning and execution");
		}
	}

	vector<shared_ptr<DirectParquetSampleSharedState>> shared_states;
	shared_states.reserve(plan.files.size());
	for (auto &file_sample : plan.files) {
		D_ASSERT(file_sample.selected_row_groups > 0);
		D_ASSERT(!file_sample.ranges.empty());
		if (file_sample.ranges.empty()) {
			throw InternalException("Parquet instant sample file has no row ranges");
		}
		auto row_filter = BuildRowNumberRangeFilter(file_sample.ranges);
		D_ASSERT(row_filter);
		auto filters = make_uniq<TableFilterSet>();
		filters->PushFilter(ProjectionIndex(plan.output_types.size()),
		                    make_uniq<ExpressionFilter>(std::move(row_filter)));

		auto bind_data = get.bind_data->Copy();
		if (!bind_data) {
			throw InternalException("Parquet instant sampler could not copy bind data");
		}
		if (plan.total_files > 1) {
			if (file_sample.file_index >= source_files.size()) {
				throw InternalException("Parquet instant sample references an invalid file index");
			}
			auto &file_bind = bind_data->Cast<MultiFileBindData>();
			vector<OpenFileInfo> one_file;
			one_file.push_back(source_files[file_sample.file_index]);
			file_bind.file_list = make_shared_ptr<SimpleMultiFileList>(std::move(one_file));
			file_bind.initial_reader.reset();
			file_bind.union_readers.clear();
			if (source_bind.union_readers.size() == source_files.size()) {
				GlobalTableFunctionState reader_state;
				file_bind.initial_reader = source_bind.multi_file_reader->CreateReader(
				    context, reader_state, *source_bind.union_readers[file_sample.file_index], source_bind);
			} else if (file_sample.file_index == 0 && source_bind.initial_reader &&
			           source_bind.initial_reader->GetFileName() == source_files.front().path) {
				file_bind.initial_reader = source_bind.initial_reader;
			}
		}

		auto shared_state = make_shared_ptr<DirectParquetSampleSharedState>(context, get.function, std::move(bind_data),
		                                                                    get.GetColumnIds(), projection_ids,
		                                                                    std::move(filters), plan.output_types);
		if (!shared_state->Initialize()) {
			throw InternalException("Parquet instant sample could not initialize its global scan state");
		}
		shared_states.push_back(std::move(shared_state));
	}

	// Row-group initialization and positioning dominate tiny Parquet samples,
	// so preserve one parallel work unit per selected group up to the current
	// async pool size. When more files are selected, workers drain several
	// isolated file domains instead of creating an executor per file.
	auto task_count = ParquetTaskCount(context, plan.selected_row_groups);
	D_ASSERT(task_count > 0);
	result.task_count = task_count;
	vector<vector<shared_ptr<DirectParquetSampleSharedState>>> worker_states(task_count);
	if (shared_states.size() >= task_count) {
		vector<idx_t> state_order(shared_states.size());
		vector<idx_t> state_weights(shared_states.size(), 0);
		for (idx_t state_index = 0; state_index < shared_states.size(); state_index++) {
			state_order[state_index] = state_index;
			for (auto &range : plan.files[state_index].ranges) {
				state_weights[state_index] += range.row_count;
			}
		}
		std::sort(state_order.begin(), state_order.end(),
		          [&](idx_t left, idx_t right) { return state_weights[left] > state_weights[right]; });
		vector<idx_t> worker_weights(task_count, 0);
		for (auto state_index : state_order) {
			auto worker_index = std::min_element(worker_weights.begin(), worker_weights.end()) - worker_weights.begin();
			worker_states[worker_index].push_back(shared_states[state_index]);
			worker_weights[worker_index] += state_weights[state_index];
		}
	} else {
		vector<idx_t> additional_capacities;
		additional_capacities.reserve(plan.files.size());
		for (auto &file_sample : plan.files) {
			D_ASSERT(file_sample.selected_row_groups > 0);
			additional_capacities.push_back(file_sample.selected_row_groups - 1);
		}
		auto additional_workers = AllocateProportionalQuotas(additional_capacities, task_count - shared_states.size());
		idx_t worker_index = 0;
		for (idx_t state_index = 0; state_index < shared_states.size(); state_index++) {
			auto state_workers = 1 + additional_workers[state_index];
			for (idx_t state_worker = 0; state_worker < state_workers; state_worker++) {
				D_ASSERT(worker_index < worker_states.size());
				worker_states[worker_index++].push_back(shared_states[state_index]);
			}
		}
		D_ASSERT(worker_index == worker_states.size());
	}

	vector<unique_ptr<DirectParquetSampleWorker>> workers;
	workers.reserve(task_count);
	for (idx_t task = 0; task < task_count; task++) {
		D_ASSERT(!worker_states[task].empty());
		workers.push_back(make_uniq<DirectParquetSampleWorker>(
		    std::move(worker_states[task]), local_predicate ? local_predicate->Copy() : nullptr, collect_timing));
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
