#include "predicate_transfer/cardinality_estimation/instant_sampler/common.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/transaction/transaction_data.hpp"

#include <algorithm>
#include <chrono>
#include <random>

namespace duckdb {
namespace instant_sampler_internal {

static void ScanBaseLeafRange(ColumnData &column, ColumnScanState &scan_state, idx_t offset, idx_t count,
                              Vector &result) {
	idx_t scanned = 0;
	while (scanned < count) {
		auto segment = column.GetSegmentTree().GetSegment(offset + scanned);
		if (!segment) {
			throw InternalException("Instant sample range is outside a column segment");
		}
		auto segment_offset = offset + scanned - segment->GetRowStart();
		auto scan_count = MinValue<idx_t>(count - scanned, segment->GetCount() - segment_offset);
		if (scanned > 0) {
			column.InitializeScanWithOffset(scan_state, offset + scanned);
		}
		segment->GetNode().InitializeScan(scan_state);
		if (segment_offset > 0) {
			segment->GetNode().Skip(scan_state);
		}
		segment->GetNode().Scan(scan_state, scan_count, result, scanned, ScanVectorType::SCAN_FLAT_VECTOR);
		scanned += scan_count;
	}
}

static void ScanBasePhysicalRange(ColumnData &column, ColumnScanState &scan_state, idx_t offset, idx_t count,
                                  Vector &result) {
	// The direct mode is intentionally independent of DuckDB's update/delete
	// overlays. Scan immutable base segments and their base validity segments
	// without inspecting ColumnData::updates; this also avoids a precheck/read
	// race with a concurrent update. Direct sampling rejects nested/geometry
	// columns before reaching this helper, so every column here is standard.
	auto &standard_column = column.Cast<StandardColumnData>();
	if (scan_state.child_states.size() != 1) {
		throw InternalException("Base instant sample expected one validity scan state");
	}
	auto &validity = standard_column.GetValidityData();
	auto &validity_state = scan_state.child_states[0];
	// StandardColumnData initializes the value and validity states together.
	// Reuse those exact-offset states and scan directly into the destination;
	// allocating and copying a temporary vector for every physical segment is
	// unnecessary for SCAN_FLAT_VECTOR.
	column.InitializeScanWithOffset(scan_state, offset);
	ScanBaseLeafRange(column, scan_state, offset, count, result);
	// A value range that crosses physical segments can reinitialize the parent
	// StandardColumnData and, with it, this child state. Restore the original
	// validity offset before scanning the validity stream.
	validity.InitializeScanWithOffset(validity_state, offset);
	ScanBaseLeafRange(validity, validity_state, offset, count, result);
}

bool SupportsBasePhysicalScan(const LogicalType &type) {
	// Nested types have child storage layouts, while GEOMETRY uses GeoColumnData
	// despite having a scalar physical type. Snapshot mode can still sample them
	// through DuckDB's normal row-group scan.
	return !type.IsNested() && type.id() != LogicalTypeId::GEOMETRY;
}

idx_t CeilDivide(idx_t value, idx_t divisor) {
	D_ASSERT(divisor > 0);
	return value / divisor + (value % divisor != 0);
}

static idx_t ScanDirectRange(ClientContext &context, RowGroupCollection &collection,
                             RowGroupSegmentTree &row_group_tree, TransactionData transaction,
                             const vector<StorageIndex> &column_ids, TableScanState &scan_state, DataChunk &scan_chunk,
                             DataChunk &result_chunk, const InstantSampleRange &range, idx_t result_offset,
                             bool snapshot_safe, DirectStorageScanTiming *timing,
                             const vector<idx_t> *sample_offsets = nullptr) {
	auto locate_started = timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	auto row_group = row_group_tree.GetSegment(range.row_start);
	if (!row_group) {
		throw InternalException("Instant sample row is outside the table row groups");
	}
	auto local_offset = range.row_start - row_group->GetRowStart();
	if (range.row_count > row_group->GetCount() - local_offset) {
		throw InternalException("Instant sample range crosses a DuckDB row group");
	}
	auto &row_group_data = row_group->GetNode();
	if (timing) {
		timing->locate_ms +=
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - locate_started).count();
	}

	auto decode_started = timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	scan_chunk.Reset();
	SelectionVector selected(STANDARD_VECTOR_SIZE);
	idx_t selected_count = 0;
	auto desired_count = sample_offsets ? sample_offsets->size() : range.row_count;
	if (snapshot_safe) {
		auto vector_index = local_offset / STANDARD_VECTOR_SIZE;
		auto vector_start = vector_index * STANDARD_VECTOR_SIZE;
		auto offset_in_vector = local_offset - vector_start;
		if (range.row_count > STANDARD_VECTOR_SIZE - offset_in_vector) {
			throw InternalException("Snapshot-safe instant sample range crosses a DuckDB vector boundary");
		}
		auto vector_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, row_group->GetCount() - vector_start);
		auto scan_end = row_group->GetRowStart() + vector_start + vector_count;
		if (!RowGroupCollection::InitializeScanInRowGroup(context, scan_state.table_state, collection, *row_group,
		                                                  vector_index, scan_end)) {
			throw InternalException("Instant sample could not initialize its row-group scan");
		}

		row_group_data.Scan(ScanOptions(transaction), scan_state.table_state, scan_chunk);
		auto visible_count = scan_chunk.size();
		auto desired_at = [&](idx_t index) {
			auto relative_offset = sample_offsets ? (*sample_offsets)[index] : index;
			D_ASSERT(relative_offset < range.row_count);
			return offset_in_vector + relative_offset;
		};
		if (visible_count == vector_count) {
			for (idx_t desired_index = 0; desired_index < desired_count; desired_index++) {
				selected.set_index(selected_count++, desired_at(desired_index));
			}
		} else {
			idx_t visible_index = 0;
			idx_t desired_index = 0;
			while (visible_index < visible_count && desired_index < desired_count) {
				auto source_index = scan_state.table_state.valid_sel.get_index(visible_index);
				auto desired_offset = desired_at(desired_index);
				if (source_index < desired_offset) {
					visible_index++;
				} else if (source_index > desired_offset) {
					desired_index++;
				} else {
					selected.set_index(selected_count++, visible_index);
					visible_index++;
					desired_index++;
				}
			}
		}
	} else {
		// Sampling only guides the optimizer, so the performance-first path reads
		// the narrow base-segment range. It deliberately ignores update overlays,
		// delete visibility, and transaction-local rows; the actual query is still
		// executed through DuckDB's normal MVCC-aware table scan.
		for (idx_t col = 0; col < column_ids.size(); col++) {
			auto &column = row_group_data.GetRawColumnData(column_ids[col]);
			ScanBasePhysicalRange(column, scan_state.table_state.column_scans[col], local_offset, range.row_count,
			                      scan_chunk.data[col]);
		}
		scan_chunk.SetChildCardinality(range.row_count);
		for (idx_t desired_index = 0; desired_index < desired_count; desired_index++) {
			auto relative_offset = sample_offsets ? (*sample_offsets)[desired_index] : desired_index;
			D_ASSERT(relative_offset < range.row_count);
			selected.set_index(selected_count++, relative_offset);
		}
	}
	if (selected_count > 0) {
		scan_chunk.Slice(selected, selected_count);
		scan_chunk.Flatten();
		for (idx_t col = 0; col < result_chunk.ColumnCount(); col++) {
			VectorOperations::Copy(scan_chunk.data[col], result_chunk.data[col], selected_count, 0, result_offset);
		}
	}
	result_chunk.SetChildCardinality(result_offset + selected_count);
	if (timing) {
		timing->decode_ms +=
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - decode_started).count();
	}
	return selected_count;
}

struct StorageSampleJob {
	ClientContext &context;
	RowGroupCollection &collection;
	RowGroupSegmentTree &row_group_tree;
	TransactionData transaction;
	const vector<StorageIndex> &column_ids;
	const vector<LogicalType> &column_types;
	const vector<SelectedStorageVector> &sample_vectors;
	vector<shared_ptr<ColumnDataCollection>> &outputs;
	vector<idx_t> &sampled_rows;
	vector<DirectStorageScanTiming> *timings;
	idx_t task_count;
	bool snapshot_safe;
};

static void ExecuteStorageSampleTask(idx_t task_id, StorageSampleJob &job, unique_ptr<Expression> local_predicate) {
	auto task_started = job.timings ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	auto timing = job.timings ? &(*job.timings)[task_id] : nullptr;
	auto &allocator = BufferAllocator::Get(job.context);
	auto local = make_shared_ptr<ColumnDataCollection>(allocator, job.column_types);
	ColumnDataAppendState append_state;
	local->InitializeAppend(append_state);
	TableScanState scan_state;
	scan_state.Initialize(job.column_ids, job.context);
	if (!job.snapshot_safe) {
		// Direct scans initialize each selected column at the exact sampled
		// offset. Build the stable child-state shape once per task instead of
		// running RowGroup::InitializeScanWithOffset for every micro-range and
		// immediately overwriting all of those states.
		scan_state.table_state.Initialize(job.context, job.collection.GetTypes());
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(allocator, job.column_types);
	DataChunk chunk;
	chunk.Initialize(allocator, job.column_types);
	unique_ptr<ExpressionExecutor> filter_executor;
	if (local_predicate) {
		filter_executor = make_uniq<ExpressionExecutor>(job.context);
		filter_executor->AddExpression(*local_predicate);
	}
	// Vectors are physically sorted. Give each task one contiguous slice so
	// its vector scans retain physical locality.
	auto vector_begin = task_id * job.sample_vectors.size() / job.task_count;
	auto vector_end = (task_id + 1) * job.sample_vectors.size() / job.task_count;
	idx_t task_sampled_rows = 0;
	SelectionVector selection(STANDARD_VECTOR_SIZE);
	auto flush = [&]() {
		if (chunk.size() == 0) {
			return;
		}
		if (filter_executor) {
			auto filter_started = timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
			auto survivor_count = filter_executor->SelectExpression(chunk, selection);
			if (timing) {
				timing->filter_ms +=
				    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - filter_started)
				        .count();
			}
			if (survivor_count == 0) {
				chunk.Reset();
				return;
			}
			if (survivor_count < chunk.size()) {
				chunk.Slice(selection, survivor_count);
				chunk.Flatten();
			}
		}
		auto append_started = timing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
		local->Append(append_state, chunk);
		if (timing) {
			timing->append_ms +=
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - append_started).count();
		}
		chunk.Reset();
	};
	for (idx_t index = vector_begin; index < vector_end; index++) {
		auto &sample_vector = job.sample_vectors[index];
		auto candidate_count = sample_vector.sample_offsets.size();
		D_ASSERT(candidate_count <= STANDARD_VECTOR_SIZE);
		if (candidate_count > STANDARD_VECTOR_SIZE - chunk.size()) {
			flush();
		}
		InstantSampleRange range {sample_vector.row_start, sample_vector.row_count};
		task_sampled_rows += ScanDirectRange(job.context, job.collection, job.row_group_tree, job.transaction,
		                                     job.column_ids, scan_state, scan_chunk, chunk, range, chunk.size(),
		                                     job.snapshot_safe, timing, &sample_vector.sample_offsets);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			flush();
		}
	}
	flush();
	job.sampled_rows[task_id] = task_sampled_rows;
	if (timing) {
		timing->task_wall_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - task_started).count();
	}
	job.outputs[task_id] = std::move(local);
}

class StorageSampleTask : public BaseExecutorTask {
public:
	StorageSampleTask(TaskExecutor &executor, idx_t task_id, StorageSampleJob &job,
	                  unique_ptr<Expression> local_predicate)
	    : BaseExecutorTask(executor), task_id(task_id), job(job), local_predicate(std::move(local_predicate)) {
	}

	void ExecuteTask() override {
		ExecuteStorageSampleTask(task_id, job, std::move(local_predicate));
	}

	string TaskType() const override {
		return "RPTStorageSample";
	}

private:
	idx_t task_id;
	StorageSampleJob &job;
	unique_ptr<Expression> local_predicate;
};

class DiskStoragePrefetchTask : public BaseExecutorTask {
public:
	DiskStoragePrefetchTask(TaskExecutor &executor, idx_t task_id, idx_t task_count, ClientContext &context,
	                        BufferManager &buffer_manager, const vector<shared_ptr<BlockHandle>> &handles)
	    : BaseExecutorTask(executor), task_id(task_id), task_count(task_count), context(context),
	      buffer_manager(buffer_manager), handles(handles) {
	}

	void ExecuteTask() override {
		auto begin = task_id * handles.size() / task_count;
		auto end = (task_id + 1) * handles.size() / task_count;
		vector<shared_ptr<BlockHandle>> local_handles;
		local_handles.reserve(end - begin);
		for (idx_t i = begin; i < end; i++) {
			local_handles.push_back(handles[i]);
		}
		buffer_manager.Prefetch(QueryContext(context), local_handles);
	}

	string TaskType() const override {
		return "RPTDiskStoragePrefetch";
	}

private:
	idx_t task_id;
	idx_t task_count;
	ClientContext &context;
	BufferManager &buffer_manager;
	const vector<shared_ptr<BlockHandle>> &handles;
};

vector<idx_t> SelectStratifiedExactlyK(idx_t population_size, idx_t target_size, std::mt19937_64 &random) {
	vector<idx_t> result;
	if (population_size == 0 || target_size == 0) {
		return result;
	}
	target_size = MinValue<idx_t>(population_size, target_size);
	result.reserve(target_size);
	for (idx_t i = 0; i < target_size; i++) {
		auto begin = i * population_size / target_size;
		auto end = (i + 1) * population_size / target_size;
		std::uniform_int_distribution<idx_t> distribution(begin, end - 1);
		result.push_back(distribution(random));
	}
	return result;
}

vector<idx_t> SelectWindowRows(idx_t window_size, idx_t target_size, std::mt19937_64 &random) {
	vector<idx_t> result;
	if (window_size == 0 || target_size == 0) {
		return result;
	}
	target_size = MinValue(target_size, window_size);
	result.reserve(target_size);
	std::uniform_int_distribution<idx_t> distribution(0, window_size - 1);
	auto phase = distribution(random);
	for (idx_t i = 0; i < target_size; i++) {
		result.push_back((phase + i * window_size / target_size) % window_size);
	}
	std::sort(result.begin(), result.end());
	return result;
}

vector<idx_t> AllocateProportionalQuotas(const vector<idx_t> &capacities, idx_t target_size) {
	vector<idx_t> quotas(capacities.size(), 0);
	idx_t population_size = 0;
	for (auto capacity : capacities) {
		population_size += capacity;
	}
	if (population_size == 0 || target_size == 0) {
		return quotas;
	}
	target_size = MinValue(target_size, population_size);
	vector<pair<long double, idx_t>> fractional;
	fractional.reserve(capacities.size());
	idx_t allocated = 0;
	for (idx_t i = 0; i < capacities.size(); i++) {
		auto exact = static_cast<long double>(target_size) * static_cast<long double>(capacities[i]) /
		             static_cast<long double>(population_size);
		quotas[i] = MinValue<idx_t>(capacities[i], static_cast<idx_t>(exact));
		allocated += quotas[i];
		fractional.emplace_back(exact - static_cast<long double>(quotas[i]), i);
	}
	std::sort(fractional.begin(), fractional.end(), [](const auto &left, const auto &right) {
		return left.first == right.first ? left.second < right.second : left.first > right.first;
	});
	for (auto &remainder : fractional) {
		if (allocated == target_size) {
			break;
		}
		auto index = remainder.second;
		if (quotas[index] < capacities[index]) {
			quotas[index]++;
			allocated++;
		}
	}
	D_ASSERT(allocated == target_size);
	return quotas;
}

void PrefetchStorageBlocks(ClientContext &context, BufferManager &buffer_manager,
                           const vector<shared_ptr<BlockHandle>> &blocks, InstantSampleResult &result) {
	if (blocks.empty()) {
		return;
	}
	auto prefetch_started = std::chrono::steady_clock::now();
	TaskExecutor prefetch_executor(context, TaskSchedulerType::ASYNC);
	auto prefetch_tasks = MinValue<idx_t>(INSTANT_BLOCK_TASK_LIMIT, blocks.size());
	result.prefetch_task_count = prefetch_tasks;
	for (idx_t task = 0; task < prefetch_tasks; task++) {
		prefetch_executor.ScheduleTask(make_uniq<DiskStoragePrefetchTask>(prefetch_executor, task, prefetch_tasks,
		                                                                  context, buffer_manager, blocks));
	}
	prefetch_executor.WorkOnTasks();
	result.prefetch_ms =
	    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - prefetch_started).count();
}

void ExecuteStorageScan(ClientContext &context, RowGroupCollection &collection, RowGroupSegmentTree &row_group_tree,
                        const TransactionData &transaction, const vector<StorageIndex> &column_ids,
                        const vector<LogicalType> &column_types, const vector<SelectedStorageVector> &sample_vectors,
                        idx_t task_limit, bool collect_timing, bool snapshot_safe, const Expression *local_predicate,
                        InstantSampleResult &result) {
	if (sample_vectors.empty()) {
		return;
	}
	D_ASSERT(task_limit > 0);
	idx_t decoded_rows = 0;
	for (auto &sample_vector : sample_vectors) {
		decoded_rows += sample_vector.row_count;
	}
	D_ASSERT(decoded_rows > 0);
	// Tiny dimensions do not contain enough decoding work to repay task
	// scheduling. Keep at least half a DuckDB vector of physical input per
	// worker; the normal 8K scattered sample still uses all eight workers.
	auto useful_tasks = CeilDivide(decoded_rows, STANDARD_VECTOR_SIZE / 2);
	auto task_count = MinValue<idx_t>(task_limit, MinValue<idx_t>(sample_vectors.size(), useful_tasks));
	D_ASSERT(task_count > 0);
	result.task_count = task_count;
	vector<shared_ptr<ColumnDataCollection>> task_outputs(task_count);
	vector<idx_t> task_sampled_rows(task_count, 0);
	vector<unique_ptr<Expression>> local_predicates(task_count);
	if (local_predicate) {
		for (auto &task_predicate : local_predicates) {
			task_predicate = local_predicate->Copy();
		}
	}
	vector<DirectStorageScanTiming> task_timings;
	if (collect_timing) {
		task_timings.resize(task_count);
	}
	StorageSampleJob job {
	    context,      collection,     row_group_tree, transaction,       column_ids,
	    column_types, sample_vectors, task_outputs,   task_sampled_rows, collect_timing ? &task_timings : nullptr,
	    task_count,   snapshot_safe};
	if (task_count == 1) {
		auto wait_started = std::chrono::steady_clock::now();
		ExecuteStorageSampleTask(0, job, std::move(local_predicates[0]));
		if (collect_timing) {
			result.wait_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - wait_started).count();
		}
	} else {
		// Every object borrowed by a task remains alive until WorkOnTasks returns.
		auto scheduler_setup_started = std::chrono::steady_clock::now();
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		if (collect_timing) {
			result.scheduler_setup_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - scheduler_setup_started)
			        .count();
		}
		auto schedule_started = std::chrono::steady_clock::now();
		for (idx_t task = 0; task < task_count; task++) {
			executor.ScheduleTask(make_uniq<StorageSampleTask>(executor, task, job, std::move(local_predicates[task])));
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
		for (auto &timing : task_timings) {
			result.locate_ms += timing.locate_ms;
			result.decode_ms += timing.decode_ms;
			result.filter_ms += timing.filter_ms;
			result.append_ms += timing.append_ms;
			result.task_wall_ms += timing.task_wall_ms;
		}
	}
	for (auto sampled_rows : task_sampled_rows) {
		result.sampled_rows += sampled_rows;
	}
	auto combine_started = std::chrono::steady_clock::now();
	for (auto &task_output : task_outputs) {
		if (task_output) {
			result.sample->Combine(*task_output);
		}
	}
	if (collect_timing) {
		result.combine_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - combine_started).count();
	}
}

} // namespace instant_sampler_internal
} // namespace duckdb
