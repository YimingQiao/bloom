#include "predicate_transfer/transfer_plan/transfer_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "predicate_transfer/filter/bitmap_filter.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"

#include <chrono>
#include <iostream>
#include <limits>

namespace duckdb {
namespace {

static constexpr uint64_t MAX_EXACT_BITMAP_SPAN = 8000000;

//! A materialized transfer source no longer has pending scanner filters after
//! Compact(), so it can be scanned directly through CDC's parallel scan state.
class ParallelCollectionScanTask : public BaseExecutorTask {
public:
	using ScanFunction = std::function<void(idx_t, DataChunk &)>;

	ParallelCollectionScanTask(TaskExecutor &executor, const ColumnDataCollection &collection,
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
		return "RPTParallelCollectionScan";
	}

private:
	const ColumnDataCollection &collection;
	ColumnDataParallelScanState &scan_state;
	idx_t task_id;
	ScanFunction &function;
};

class IndexedExecutorTask final : public BaseExecutorTask {
public:
	using Function = std::function<void(idx_t)>;

	IndexedExecutorTask(TaskExecutor &executor, idx_t task_id, const char *task_type, Function &function)
	    : BaseExecutorTask(executor), task_id(task_id), task_type(task_type), function(function) {
	}

	void ExecuteTask() override {
		function(task_id);
	}

	string TaskType() const override {
		return task_type;
	}

private:
	idx_t task_id;
	const char *task_type;
	Function &function;
};

static void ExecuteIndexedTasks(ClientContext &context, idx_t task_count, const char *task_type,
                                IndexedExecutorTask::Function function) {
	D_ASSERT(task_count > 0);
	if (task_count == 1 || TaskScheduler::GetScheduler(context).NumberOfThreads() <= 1) {
		for (idx_t task_id = 0; task_id < task_count; task_id++) {
			function(task_id);
		}
		return;
	}

	TaskExecutor executor(context);
	for (idx_t task_id = 0; task_id < task_count; task_id++) {
		executor.ScheduleTask(make_uniq<IndexedExecutorTask>(executor, task_id, task_type, function));
	}
	executor.WorkOnTasks();
}

static void ScanCollection(ClientContext &context, const ColumnDataCollection &collection, idx_t task_count,
                           ParallelCollectionScanTask::ScanFunction function) {
	if (task_count <= 1) {
		DataChunk chunk;
		collection.InitializeScanChunk(chunk);
		for (idx_t chunk_idx = 0; chunk_idx < collection.ChunkCount(); chunk_idx++) {
			collection.FetchChunk(chunk_idx, chunk);
			if (chunk.size() > 0) {
				function(0, chunk);
			}
		}
		return;
	}

	ColumnDataParallelScanState scan_state;
	collection.InitializeScan(scan_state);
	TaskExecutor executor(context);
	for (idx_t task_id = 0; task_id < task_count; task_id++) {
		executor.ScheduleTask(
		    make_uniq<ParallelCollectionScanTask>(executor, collection, scan_state, task_id, function));
	}
	executor.WorkOnTasks();
}

} // namespace

TransferExecutor::TransferExecutor(Optimizer &optimizer, ClientContext &context, const RPTOptimizerConfig &config)
    : optimizer_(optimizer), context_(context), config_(config) {
}

TableScanner *TransferExecutor::Find(LogicalOperator &op) const {
	auto it = scanners_.find(&op);
	return it == scanners_.end() ? nullptr : it->second.get();
}

TableScanner *TransferExecutor::Register(LogicalOperator &op) {
	auto existing = scanners_.find(&op);
	if (existing != scanners_.end()) {
		return existing->second.get();
	}
	auto scanner = make_uniq<TableScanner>(optimizer_, context_, op, config_.late_materialize_flag);
	auto *raw = scanner.get();
	if (raw->IsMaterialized()) {
		borrowed_data_[&op] = raw->GetData();
	}
	scanners_[&op] = std::move(scanner);
	return raw;
}

idx_t TransferExecutor::MaterializedMemoryUsage() const {
	unordered_set<const ColumnDataCollection *> seen;
	idx_t total = 0;
	for (auto &entry : scanners_) {
		auto *data = entry.second->GetData();
		if (!data) {
			continue;
		}
		auto borrowed = borrowed_data_.find(entry.first);
		if ((borrowed != borrowed_data_.end() && borrowed->second == data) || !seen.insert(data).second) {
			continue;
		}
		auto allocation = data->AllocationSize();
		total = allocation > std::numeric_limits<idx_t>::max() - total ? std::numeric_limits<idx_t>::max()
		                                                               : total + allocation;
	}
	return total;
}

bool TransferExecutor::OwnsMaterializedData(LogicalOperator &op) const {
	auto *scanner = Find(op);
	if (!scanner || !scanner->GetData()) {
		return false;
	}
	auto borrowed = borrowed_data_.find(&op);
	return borrowed == borrowed_data_.end() || borrowed->second != scanner->GetData();
}

void TransferExecutor::Remove(LogicalOperator &op) {
	scanners_.erase(&op);
	borrowed_data_.erase(&op);
}

bool TransferExecutor::IsMaterialized(LogicalOperator &op) const {
	auto *s = Find(op);
	return s && s->IsMaterialized();
}

TableScanner *TransferExecutor::EnsureMaterialized(LogicalOperator &op, const column_binding_set_t &required) {
	auto *scanner = Register(op);
	if (scanner->IsMaterialized()) {
		return scanner;
	}
	if (!required.empty()) {
		scanner->SetRequiredColumns(required);
	}
	scanner->Materialize();
	return scanner;
}

void TransferExecutor::AttachFilterToScanner(LogicalOperator &op, const vector<ColumnBinding> &dest_bindings,
                                             const shared_ptr<RPTFilter> &filter, size_t identity_hash) {
	auto it = scanners_.find(&op);
	if (it == scanners_.end()) {
		return;
	}
	it->second->AddFilter(dest_bindings, filter, identity_hash);
}

vector<shared_ptr<RPTFilter>> TransferExecutor::BuildTransferFilters(LogicalOperator &op,
                                                                     const vector<FilterBuildSpec> &specs) {
	using Clock = std::chrono::steady_clock;
	auto started = Clock::now();
	auto log_timing = ClientConfig::GetConfig(context_).enable_profiler || config_.log_transfer_steps;
	vector<shared_ptr<RPTFilter>> result(specs.size());
	if (specs.empty()) {
		return result;
	}
	auto scanner_it = scanners_.find(&op);
	if (scanner_it == scanners_.end()) {
		return result;
	}
	auto &scanner = *scanner_it->second;

	auto *collection = scanner.GetData();
	if (!collection) {
		return result;
	}
	DataChunk output_chunk;
	scanner.InitScanChunk(output_chunk);

	vector<vector<idx_t>> chunk_cols(specs.size());
	vector<bool> integral_key(specs.size(), false);
	vector<idx_t> stats_request_indices(specs.size(), DConstants::INVALID_INDEX);
	vector<TableScanner::StatsRequest> stats_requests;
	vector<DuckDBPrefixRangeFilterAdapter *> prefix_filters(specs.size(), nullptr);
	vector<BitmapFilterConfig> exact_domain_configs(specs.size());
	vector<bool> track_exact_domain(specs.size(), false);
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		auto &spec = specs[spec_idx];
		if (spec.key_bindings.empty() || spec.key_bindings.size() != spec.key_types.size()) {
			continue;
		}
		for (auto &binding : spec.key_bindings) {
			auto chunk_col = scanner.FindChunkCol(binding);
			if (chunk_col == DConstants::INVALID_INDEX || chunk_col >= output_chunk.ColumnCount()) {
				chunk_cols[spec_idx].clear();
				break;
			}
			chunk_cols[spec_idx].push_back(chunk_col);
		}
		integral_key[spec_idx] = chunk_cols[spec_idx].size() == 1 && spec.key_types.front().IsIntegral() &&
		                         spec.key_types.front() != LogicalType::UBIGINT &&
		                         spec.key_types.front() != LogicalType::HUGEINT &&
		                         spec.key_types.front() != LogicalType::UHUGEINT;
		if (integral_key[spec_idx]) {
			stats_request_indices[spec_idx] = stats_requests.size();
			stats_requests.push_back({chunk_cols[spec_idx][0], spec.key_types.front()});
		}
	}
	auto setup_finished = Clock::now();

	// Apply deferred filters and collect all integral-key ranges in one scan.
	auto compact_result = scanner.Compact(stats_requests);
	auto compact_finished = Clock::now();
	collection = scanner.GetData();
	D_ASSERT(collection);
	auto task_count = RPTScanTaskCount(context_, *collection);
	auto total_rows = compact_result.row_count;
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		if (chunk_cols[spec_idx].empty()) {
			continue;
		}
		bool use_bitmap = false;
		int64_t stats_min = 0, stats_max = 0;
		auto stats_idx = stats_request_indices[spec_idx];
		const auto has_min_max =
		    stats_idx != DConstants::INVALID_INDEX && compact_result.column_stats[stats_idx].has_min_max;
		if (has_min_max) {
			stats_min = compact_result.column_stats[stats_idx].observed_min;
			stats_max = compact_result.column_stats[stats_idx].observed_max;
		}
		if (has_min_max && stats_min <= stats_max) {
			auto range = static_cast<uint64_t>(stats_max) - static_cast<uint64_t>(stats_min);
			auto row_budget = total_rows > std::numeric_limits<uint64_t>::max() / 128
			                      ? std::numeric_limits<uint64_t>::max()
			                      : static_cast<uint64_t>(total_rows) * 128;
			use_bitmap = range <= row_budget || range <= MAX_EXACT_BITMAP_SPAN;
		} else if (integral_key[spec_idx] && total_rows == 0) {
			use_bitmap = true;
			stats_min = 1;
			stats_max = 0;
		}
		if (use_bitmap && total_rows > 0) {
			auto filter = make_shared_ptr<DuckDBPrefixRangeFilterAdapter>(context_, specs[spec_idx].key_types.front(),
			                                                              stats_min, stats_max, total_rows);
			prefix_filters[spec_idx] = filter.get();
			result[spec_idx] = std::move(filter);
			const auto domain_span = static_cast<uint64_t>(stats_max) - static_cast<uint64_t>(stats_min);
			if (specs[spec_idx].track_exact_domain && domain_span <= MAX_EXACT_BITMAP_SPAN) {
				exact_domain_configs[spec_idx] = BitmapFilterConfig(stats_min, stats_max);
				track_exact_domain[spec_idx] = true;
			}
		} else if (use_bitmap) {
			result[spec_idx] = make_shared_ptr<BitmapFilter>(context_, BitmapFilterConfig(stats_min, stats_max));
		} else {
			result[spec_idx] =
			    make_shared_ptr<ActiveBloomFilter>(context_, BloomFilterConfig(), static_cast<uint32_t>(total_rows));
		}
	}
	vector<vector<unique_ptr<PrefixRangeFilter::BuildState>>> prefix_build_states(specs.size());
	vector<bool> shared_prefix_states(specs.size(), false);
	static constexpr idx_t MAX_PREFIX_RANGE_PRIVATE_MEMORY = 16ULL * 1024ULL * 1024ULL;
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		if (!result[spec_idx]) {
			continue;
		}
		if (prefix_filters[spec_idx] && task_count > 1) {
			auto state_size = prefix_filters[spec_idx]->GetBuildStateSize();
			D_ASSERT(state_size > 0);
			// Dense, compact ranges benefit from non-atomic task-private states. A
			// wide range would replicate too much memory and pay to merge every
			// replica, so all scan tasks use DuckDB's parallel insertion API on one
			// shared state instead.
			auto lane_count = state_size <= MAX_PREFIX_RANGE_PRIVATE_MEMORY / task_count ? task_count : 1;
			prefix_build_states[spec_idx].resize(lane_count);
			shared_prefix_states[spec_idx] = lane_count < task_count;
		}
	}
	vector<vector<unique_ptr<BitmapFilter>>> local_domain_filters(specs.size());
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		if (track_exact_domain[spec_idx]) {
			local_domain_filters[spec_idx].resize(task_count);
		}
	}
	idx_t parallel_prefix_filter_count = 0;
	if (task_count > 1) {
		for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
			if (prefix_filters[spec_idx]) {
				parallel_prefix_filter_count++;
			}
		}
	}
	vector<idx_t> shared_state_specs;
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		if (shared_prefix_states[spec_idx]) {
			D_ASSERT(prefix_build_states[spec_idx].size() == 1);
			shared_state_specs.push_back(spec_idx);
		}
	}
	if (!shared_state_specs.empty()) {
		ExecuteIndexedTasks(context_, shared_state_specs.size(), "RPTPrefixStateInitialize", [&](idx_t task_id) {
			auto spec_idx = shared_state_specs[task_id];
			prefix_build_states[spec_idx][0] = prefix_filters[spec_idx]->InitializeBuildState(context_);
		});
	}
	auto initialize_finished = Clock::now();
	ScanCollection(context_, *collection, task_count, [&](idx_t task_id, DataChunk &chunk) {
		for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
			if (!result[spec_idx]) {
				continue;
			}
			if (!prefix_build_states[spec_idx].empty()) {
				auto lane_id = task_id % prefix_build_states[spec_idx].size();
				auto &local_state = prefix_build_states[spec_idx][lane_id];
				if (!local_state) {
					local_state = prefix_filters[spec_idx]->InitializeBuildState(context_);
				}
				prefix_filters[spec_idx]->InsertBuildState(chunk, chunk_cols[spec_idx], *local_state,
				                                           shared_prefix_states[spec_idx]);
			} else {
				result[spec_idx]->Insert(chunk, chunk_cols[spec_idx]);
			}
			if (!local_domain_filters[spec_idx].empty()) {
				auto &domain_filter = local_domain_filters[spec_idx][task_id];
				if (!domain_filter) {
					domain_filter = make_uniq<BitmapFilter>(context_, exact_domain_configs[spec_idx]);
				}
				domain_filter->InsertTaskLocal(chunk, chunk_cols[spec_idx]);
			}
		}
	});
	auto insert_finished = Clock::now();
	IndexedExecutorTask::Function finalize_filter = [&](idx_t spec_idx) {
		for (auto &state : prefix_build_states[spec_idx]) {
			if (state) {
				prefix_filters[spec_idx]->MergeBuildState(*state);
			}
		}
		if (result[spec_idx]) {
			result[spec_idx]->SetValid();
		}
		if (prefix_filters[spec_idx] && !local_domain_filters[spec_idx].empty()) {
			BitmapFilter *merged_domain = nullptr;
			for (auto &domain_filter : local_domain_filters[spec_idx]) {
				if (!domain_filter) {
					continue;
				}
				if (!merged_domain) {
					merged_domain = domain_filter.get();
				} else {
					merged_domain->MergeTaskLocal(*domain_filter);
				}
			}
			idx_t distinct = 0;
			if (merged_domain) {
				auto exact_distinct = merged_domain->ExactDistinctCount();
				D_ASSERT(exact_distinct.IsValid());
				distinct = exact_distinct.GetIndex();
			}
			prefix_filters[spec_idx]->SetExactDistinctCount(distinct);
		}
	};
	if (parallel_prefix_filter_count > 1) {
		ExecuteIndexedTasks(context_, specs.size(), "RPTFilterFinalize", finalize_filter);
	} else {
		for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
			finalize_filter(spec_idx);
		}
	}
	if (log_timing) {
		auto elapsed = [](Clock::time_point begin, Clock::time_point end) {
			return std::chrono::duration<double, std::milli>(end - begin).count();
		};
		auto finished = Clock::now();
		std::cerr << "    [Bloom-FilterBuildTiming] rows=" << total_rows << " specs=" << specs.size()
		          << " tasks=" << task_count << " shared_prefix=" << shared_state_specs.size()
		          << " setup=" << elapsed(started, setup_finished)
		          << "ms compact=" << elapsed(setup_finished, compact_finished)
		          << "ms initialize=" << elapsed(compact_finished, initialize_finished)
		          << "ms insert=" << elapsed(initialize_finished, insert_finished)
		          << "ms merge=" << elapsed(insert_finished, finished) << "ms total=" << elapsed(started, finished)
		          << "ms\n";
	}
	return result;
}

shared_ptr<RPTFilter> TransferExecutor::FinalizeRowIDBitmap(LogicalOperator &op) {
	auto scanner_it = scanners_.find(&op);
	if (scanner_it == scanners_.end()) {
		return nullptr;
	}
	auto &scanner = *scanner_it->second;

	// Compact unconditionally: flooding may leave pending BFs that never
	// triggered a Compact, and BuildMemoryScan would otherwise see unshrunk data.
	scanner.Compact();

	idx_t rowid_chunk_col = scanner.GetRowIdChunkCol();
	if (rowid_chunk_col == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	auto *collection = scanner.GetData();
	if (!collection || rowid_chunk_col >= collection->ColumnCount()) {
		return nullptr;
	}

	auto task_count = RPTScanTaskCount(context_, *collection);
	vector<int64_t> local_min_rids(task_count, std::numeric_limits<int64_t>::max());
	vector<int64_t> local_max_rids(task_count, std::numeric_limits<int64_t>::min());

	ScanCollection(context_, *collection, task_count, [&](idx_t task_id, DataChunk &chunk) {
		auto &rid_vec = chunk.data[rowid_chunk_col];
		rid_vec.Flatten();
		auto rid_data = FlatVector::GetData<int64_t>(rid_vec);
		for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
			local_min_rids[task_id] = MinValue(local_min_rids[task_id], rid_data[row_idx]);
			local_max_rids[task_id] = MaxValue(local_max_rids[task_id], rid_data[row_idx]);
		}
	});

	int64_t min_rid = std::numeric_limits<int64_t>::max();
	int64_t max_rid = std::numeric_limits<int64_t>::min();
	for (idx_t task_id = 0; task_id < task_count; task_id++) {
		min_rid = MinValue(min_rid, local_min_rids[task_id]);
		max_rid = MaxValue(max_rid, local_max_rids[task_id]);
	}
	if (scanner.Count() == 0) {
		min_rid = 1;
		max_rid = 0;
	}

	auto bitmap = make_shared_ptr<BitmapFilter>(context_, BitmapFilterConfig(min_rid, max_rid));
	const vector<idx_t> row_id_columns {rowid_chunk_col};
	ScanCollection(context_, *collection, task_count,
	               [&](idx_t, DataChunk &chunk) { bitmap->Insert(chunk, row_id_columns); });
	bitmap->SetValid();
	return bitmap;
}

} // namespace duckdb
