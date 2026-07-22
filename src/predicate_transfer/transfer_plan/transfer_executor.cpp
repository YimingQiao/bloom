#include "predicate_transfer/transfer_plan/transfer_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "predicate_transfer/filter/bitmap_filter.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"

namespace duckdb {
namespace {

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
	scanners_[&op] = std::move(scanner);
	return raw;
}

bool TransferExecutor::IsMaterialized(LogicalOperator &op) const {
	auto *s = Find(op);
	return s && s->IsMaterialized();
}

TableScanner *
TransferExecutor::EnsureMaterialized(LogicalOperator &op,
                                     const unordered_set<ColumnBinding, ColumnBindingHashFunc> &required) {
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
		                         spec.key_types.front() != LogicalType::HUGEINT &&
		                         spec.key_types.front() != LogicalType::UHUGEINT;
		if (integral_key[spec_idx]) {
			stats_request_indices[spec_idx] = stats_requests.size();
			stats_requests.push_back({chunk_cols[spec_idx][0], spec.key_types.front()});
		}
	}

	// Apply deferred filters and collect all integral-key ranges in one scan.
	auto compact_result = scanner.Compact(stats_requests);
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
		if (stats_idx != DConstants::INVALID_INDEX && compact_result.column_stats[stats_idx].has_min_max) {
			stats_min = compact_result.column_stats[stats_idx].observed_min;
			stats_max = compact_result.column_stats[stats_idx].observed_max;
			auto range = stats_max - stats_min;
			use_bitmap = range >= 0 && (range <= static_cast<int64_t>(128 * total_rows) || range <= 8000000);
		} else if (integral_key[spec_idx] && total_rows == 0) {
			use_bitmap = true;
			stats_min = 1;
			stats_max = 0;
		}
		if (use_bitmap && total_rows > 0) {
			result[spec_idx] = make_shared_ptr<DuckDBPrefixRangeFilterAdapter>(
			    context_, specs[spec_idx].key_types.front(), stats_min, stats_max, total_rows);
		} else if (use_bitmap) {
			result[spec_idx] = make_shared_ptr<BitmapFilter>(context_, BitmapFilterConfig(stats_min, stats_max));
		} else {
			result[spec_idx] =
			    make_shared_ptr<ActiveBloomFilter>(context_, BloomFilterConfig(), static_cast<uint32_t>(total_rows));
		}
	}

	vector<DuckDBPrefixRangeFilterAdapter *> prefix_filters(specs.size(), nullptr);
	vector<vector<unique_ptr<PrefixRangeFilter::BuildState>>> local_prefix_states(specs.size());
	vector<bool> shared_prefix_lanes(specs.size(), false);
	static constexpr idx_t MAX_PREFIX_RANGE_BUILD_MEMORY = 16ULL * 1024ULL * 1024ULL;
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		if (!result[spec_idx]) {
			continue;
		}
		prefix_filters[spec_idx] = dynamic_cast<DuckDBPrefixRangeFilterAdapter *>(result[spec_idx].get());
		if (prefix_filters[spec_idx] && task_count > 1) {
			auto state_size = prefix_filters[spec_idx]->GetLocalBuildStateSize();
			D_ASSERT(state_size > 0);
			auto lane_count =
			    MinValue<idx_t>(task_count, MaxValue<idx_t>(MAX_PREFIX_RANGE_BUILD_MEMORY / state_size, 1));
			shared_prefix_lanes[spec_idx] = lane_count < task_count;
			for (idx_t lane_id = 0; lane_id < lane_count; lane_id++) {
				local_prefix_states[spec_idx].push_back(prefix_filters[spec_idx]->InitializeLocalBuildState(context_));
			}
		}
	}
	ScanCollection(context_, *collection, task_count, [&](idx_t task_id, DataChunk &chunk) {
		for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
			if (!result[spec_idx]) {
				continue;
			}
			if (!local_prefix_states[spec_idx].empty()) {
				auto lane_id = task_id % local_prefix_states[spec_idx].size();
				prefix_filters[spec_idx]->InsertLocal(chunk, chunk_cols[spec_idx],
				                                      *local_prefix_states[spec_idx][lane_id],
				                                      shared_prefix_lanes[spec_idx]);
			} else {
				result[spec_idx]->Insert(chunk, chunk_cols[spec_idx]);
			}
		}
	});
	for (idx_t spec_idx = 0; spec_idx < specs.size(); spec_idx++) {
		for (auto &state : local_prefix_states[spec_idx]) {
			prefix_filters[spec_idx]->MergeLocalBuildState(*state);
		}
		if (result[spec_idx]) {
			result[spec_idx]->SetValid();
		}
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
	vector<vector<int64_t>> local_rids(task_count);
	for (auto &rids : local_rids) {
		rids.reserve(scanner.Count() / task_count + STANDARD_VECTOR_SIZE);
	}

	ScanCollection(context_, *collection, task_count, [&](idx_t task_id, DataChunk &chunk) {
		auto &rid_vec = chunk.data[rowid_chunk_col];
		rid_vec.Flatten();
		auto rid_data = FlatVector::GetData<int64_t>(rid_vec);
		auto &rids = local_rids[task_id];
		rids.insert(rids.end(), rid_data, rid_data + chunk.size());
	});

	int64_t min_rid = std::numeric_limits<int64_t>::max();
	int64_t max_rid = std::numeric_limits<int64_t>::min();
	for (auto &rids : local_rids) {
		for (auto rid : rids) {
			min_rid = MinValue(min_rid, rid);
			max_rid = MaxValue(max_rid, rid);
		}
	}
	if (scanner.Count() == 0) {
		min_rid = 1;
		max_rid = 0;
	}

	auto bitmap = make_shared_ptr<BitmapFilter>(context_, BitmapFilterConfig(min_rid, max_rid));
	ParallelCollectionScanTask::ScanFunction insert_rids = [&](idx_t task_id, DataChunk &) {
		DataChunk tmp_chunk;
		tmp_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
		auto &rids = local_rids[task_id];
		for (idx_t offset = 0; offset < rids.size(); offset += STANDARD_VECTOR_SIZE) {
			auto batch = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rids.size() - offset);
			tmp_chunk.Reset();
			tmp_chunk.SetCardinalityUnsafe(batch);
			auto *data = FlatVector::GetDataMutable<int64_t>(tmp_chunk.data[0]);
			memcpy(data, rids.data() + offset, batch * sizeof(int64_t));
			static const vector<idx_t> rid_insert_cols = {0};
			bitmap->Insert(tmp_chunk, rid_insert_cols);
		}
	};
	// Reuse one task per local vector. The dummy collection scan gives each
	// task a stable id, while the callback performs the corresponding insert.
	if (task_count == 1) {
		DataChunk dummy;
		insert_rids(0, dummy);
	} else {
		TaskExecutor executor(context_);
		class RowIDInsertTask final : public BaseExecutorTask {
		public:
			RowIDInsertTask(TaskExecutor &executor, idx_t task_id, ParallelCollectionScanTask::ScanFunction &function)
			    : BaseExecutorTask(executor), task_id(task_id), function(function) {
			}
			void ExecuteTask() override {
				DataChunk dummy;
				function(task_id, dummy);
			}
			string TaskType() const override {
				return "RPTRowIDBitmapInsert";
			}

		private:
			idx_t task_id;
			ParallelCollectionScanTask::ScanFunction &function;
		};
		for (idx_t task_id = 0; task_id < task_count; task_id++) {
			executor.ScheduleTask(make_uniq<RowIDInsertTask>(executor, task_id, insert_rids));
		}
		executor.WorkOnTasks();
	}
	bitmap->SetValid();
	return bitmap;
}

} // namespace duckdb
