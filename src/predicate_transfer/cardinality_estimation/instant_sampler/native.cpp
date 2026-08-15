#include "predicate_transfer/cardinality_estimation/instant_sampler/common.hpp"

#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/transaction_data.hpp"

#include <algorithm>
#include <chrono>
#include <unordered_set>

namespace duckdb {
namespace {

using instant_sampler_internal::AllocateProportionalQuotas;
using instant_sampler_internal::CeilDivide;
using instant_sampler_internal::ExecuteStorageScan;
using instant_sampler_internal::INSTANT_BLOCK_TASK_LIMIT;
using instant_sampler_internal::INSTANT_SCATTERED_TASK_LIMIT;
using instant_sampler_internal::PhysicalRowGroupSpan;
using instant_sampler_internal::PrefetchStorageBlocks;
using instant_sampler_internal::SelectedStorageVector;
using instant_sampler_internal::SelectStratifiedExactlyK;
using instant_sampler_internal::SelectWindowRows;
using instant_sampler_internal::SupportsBasePhysicalScan;

struct NativeSampleSource {
	NativeSampleSource(shared_ptr<RowGroupCollection> collection_p, shared_ptr<RowGroupSegmentTree> row_groups_p,
	                   DuckTransaction &transaction_p)
	    : collection(std::move(collection_p)), row_groups(std::move(row_groups_p)), transaction(transaction_p) {
	}

	shared_ptr<RowGroupCollection> collection;
	shared_ptr<RowGroupSegmentTree> row_groups;
	TransactionData transaction;
	vector<PhysicalRowGroupSpan> spans;
};

static unique_ptr<NativeSampleSource> PrepareNativeSampleSource(ClientContext &context, DataTable &table,
                                                                const vector<StorageIndex> &column_ids,
                                                                const vector<LogicalType> &column_types,
                                                                idx_t access_width, bool snapshot_safe,
                                                                InstantSampleResult &result) {
	D_ASSERT(access_width > 0);
	auto collection = table.GetRowGroupCollection();
	D_ASSERT(collection);
	D_ASSERT(!column_ids.empty());
	D_ASSERT(column_ids.size() == column_types.size());
	if (!collection || column_ids.empty() || column_ids.size() != column_types.size()) {
		throw InternalException("Native instant sample received an invalid column layout");
	}
	if (!snapshot_safe && std::any_of(column_types.begin(), column_types.end(),
	                                  [](const LogicalType &type) { return !SupportsBasePhysicalScan(type); })) {
		result.unavailable_reason = "native direct sampling does not support this column layout";
		return nullptr;
	}

	auto &transaction = DuckTransaction::Get(context, table.GetAttached());
	if (snapshot_safe && LocalStorage::Get(transaction).Find(table)) {
		result.status = InstantSampleStatus::UNSUPPORTED_SNAPSHOT;
		result.unavailable_reason = "transaction-local table rows are not sampled";
		return nullptr;
	}

	auto row_groups = collection->GetRowGroups();
	auto source = make_uniq<NativeSampleSource>(collection, row_groups, transaction);
	for (auto &node : row_groups->SegmentNodes()) {
		auto row_count = node.GetCount();
		if (row_count == 0) {
			continue;
		}
		auto access_count = CeilDivide(row_count, access_width);
		source->spans.push_back({node.GetIndex(), node.GetRowStart(), row_count, access_count});
		result.candidate_rows += row_count;
		result.candidate_chunks += access_count;
	}
	result.total_row_groups = source->spans.size();
	result.sample = make_shared_ptr<ColumnDataCollection>(Allocator::DefaultAllocator(), column_types);
	result.status = InstantSampleStatus::SUCCESS;
	return source;
}

static void RecordDecodedRows(const vector<SelectedStorageVector> &ranges, InstantSampleResult &result) {
	for (auto &range : ranges) {
		result.decoded_rows += range.row_count;
	}
}

} // namespace

InstantSampleResult BuildInstantScatteredSample(ClientContext &context, DataTable &table,
                                                const vector<StorageIndex> &column_ids,
                                                const vector<LogicalType> &column_types, idx_t target_access_points,
                                                idx_t rows_per_access, bool collect_timing, bool snapshot_safe,
                                                const Expression *local_predicate, std::mt19937_64 &random) {
	InstantSampleResult result;
	result.source = InstantSampleSource::NATIVE_SCATTERED;
	auto metadata_started = std::chrono::steady_clock::now();
	D_ASSERT(target_access_points > 0);
	D_ASSERT(rows_per_access > 0);
	auto source =
	    PrepareNativeSampleSource(context, table, column_ids, column_types, rows_per_access, snapshot_safe, result);
	if (!source) {
		return result;
	}
	result.selected_row_groups = source->spans.size();
	result.selected_chunk_ordinals = SelectStratifiedExactlyK(result.candidate_chunks, target_access_points, random);

	// Map each selected access ordinal to a row-group-local range. Snapshot
	// scans consolidate ranges from the same vector because DuckDB applies MVCC
	// visibility while decoding a complete vector.
	vector<SelectedStorageVector> sample_vectors;
	sample_vectors.reserve(result.selected_chunk_ordinals.size());
	idx_t row_group_cursor = 0;
	idx_t first_access_in_group = 0;
	for (auto selected_ordinal : result.selected_chunk_ordinals) {
		while (row_group_cursor < source->spans.size()) {
			auto &span = source->spans[row_group_cursor];
			if (selected_ordinal < first_access_in_group + span.access_count) {
				auto local_access = selected_ordinal - first_access_in_group;
				auto row_offset = local_access * rows_per_access;
				auto count = MinValue<idx_t>(rows_per_access, span.row_count - row_offset);
				auto row_start = span.row_start + row_offset;
				result.selected_row_offsets.push_back(row_start);
				for (idx_t scanned = 0; scanned < count;) {
					auto absolute_row = row_start + scanned;
					auto local_row = absolute_row - span.row_start;
					auto vector_offset = local_row % STANDARD_VECTOR_SIZE;
					auto scan_count = MinValue<idx_t>(count - scanned, STANDARD_VECTOR_SIZE - vector_offset);
					if (snapshot_safe) {
						auto vector_start = absolute_row - vector_offset;
						auto vector_count =
						    MinValue<idx_t>(STANDARD_VECTOR_SIZE, span.row_start + span.row_count - vector_start);
						if (sample_vectors.empty() || sample_vectors.back().row_start != vector_start) {
							sample_vectors.push_back({vector_start, vector_count, {}});
						}
						auto &sample_offsets = sample_vectors.back().sample_offsets;
						for (idx_t offset = 0; offset < scan_count; offset++) {
							sample_offsets.push_back(vector_offset + offset);
						}
					} else {
						SelectedStorageVector selected {absolute_row, scan_count, {}};
						selected.sample_offsets.reserve(scan_count);
						for (idx_t offset = 0; offset < scan_count; offset++) {
							selected.sample_offsets.push_back(offset);
						}
						sample_vectors.push_back(std::move(selected));
					}
					scanned += scan_count;
				}
				break;
			}
			first_access_in_group += span.access_count;
			row_group_cursor++;
		}
	}
	RecordDecodedRows(sample_vectors, result);
	result.metadata_ms =
	    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - metadata_started).count();
	if (sample_vectors.empty()) {
		return result;
	}

	auto scan_started = std::chrono::steady_clock::now();
	ExecuteStorageScan(context, *source->collection, *source->row_groups, source->transaction, column_ids, column_types,
	                   sample_vectors, INSTANT_SCATTERED_TASK_LIMIT, collect_timing, snapshot_safe, local_predicate,
	                   result);
	result.scan_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - scan_started).count();
	return result;
}

InstantSampleResult BuildInstantBlockSample(ClientContext &context, DataTable &table,
                                            const vector<StorageIndex> &column_ids,
                                            const vector<LogicalType> &column_types, idx_t target_rows,
                                            idx_t target_windows, bool collect_timing, bool snapshot_safe,
                                            const Expression *local_predicate, std::mt19937_64 &random) {
	InstantSampleResult result;
	result.source = InstantSampleSource::NATIVE_BLOCK;
	auto metadata_started = std::chrono::steady_clock::now();
	D_ASSERT(target_rows > 0);
	D_ASSERT(target_windows > 0);
	auto source = PrepareNativeSampleSource(context, table, column_ids, column_types, STANDARD_VECTOR_SIZE,
	                                        snapshot_safe, result);
	if (!source) {
		return result;
	}
	if (source->spans.empty() || result.candidate_chunks == 0) {
		result.metadata_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - metadata_started).count();
		return result;
	}

	result.selected_chunk_ordinals =
	    SelectStratifiedExactlyK(result.candidate_chunks, MinValue(target_windows, result.candidate_chunks), random);
	vector<SelectedStorageVector> windows;
	windows.reserve(result.selected_chunk_ordinals.size());
	std::unordered_set<idx_t> selected_row_group_indexes;
	idx_t row_group_cursor = 0;
	idx_t first_window_in_group = 0;
	for (auto ordinal : result.selected_chunk_ordinals) {
		while (row_group_cursor < source->spans.size()) {
			auto &span = source->spans[row_group_cursor];
			if (ordinal < first_window_in_group + span.access_count) {
				auto local_window = ordinal - first_window_in_group;
				auto local_start = local_window * STANDARD_VECTOR_SIZE;
				auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, span.row_count - local_start);
				windows.push_back({span.row_start + local_start, count, {}});
				result.selected_row_offsets.push_back(span.row_start + local_start);
				selected_row_group_indexes.insert(span.index);
				break;
			}
			first_window_in_group += span.access_count;
			row_group_cursor++;
		}
	}
	result.selected_row_groups = selected_row_group_indexes.size();

	vector<idx_t> capacities;
	capacities.reserve(windows.size());
	for (auto &window : windows) {
		capacities.push_back(window.row_count);
	}
	auto quotas = AllocateProportionalQuotas(capacities, target_rows);
	for (idx_t i = 0; i < windows.size(); i++) {
		windows[i].sample_offsets = SelectWindowRows(windows[i].row_count, quotas[i], random);
	}
	windows.erase(std::remove_if(windows.begin(), windows.end(),
	                             [](const SelectedStorageVector &window) { return window.sample_offsets.empty(); }),
	              windows.end());
	RecordDecodedRows(windows, result);
	result.metadata_ms =
	    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - metadata_started).count();
	if (windows.empty()) {
		return result;
	}

	auto scan_started = std::chrono::steady_clock::now();
	// Resolve physical blocks through DuckDB's codec-aware prefetch interface.
	vector<shared_ptr<BlockHandle>> blocks;
	for (auto &window : windows) {
		auto row_group = source->row_groups->GetSegment(window.row_start);
		D_ASSERT(row_group);
		TableScanState prefetch_scan_state;
		prefetch_scan_state.Initialize(column_ids, context);
		auto vector_index = (window.row_start - row_group->GetRowStart()) / STANDARD_VECTOR_SIZE;
		if (!RowGroupCollection::InitializeScanInRowGroup(context, prefetch_scan_state.table_state, *source->collection,
		                                                  *row_group, vector_index,
		                                                  row_group->GetRowStart() + row_group->GetCount())) {
			throw InternalException("Instant sample could not initialize block prefetch");
		}
		PrefetchState prefetch_state;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto &column = row_group->GetNode().GetRawColumnData(column_ids[i]);
			column.InitializePrefetch(prefetch_state, prefetch_scan_state.table_state.column_scans[i],
			                          window.row_count);
		}
		for (auto &block : prefetch_state.blocks) {
			if (block) {
				blocks.push_back(block);
			}
		}
	}
	std::sort(blocks.begin(), blocks.end(),
	          [](const auto &left, const auto &right) { return left->BlockId() < right->BlockId(); });
	blocks.erase(std::unique(blocks.begin(), blocks.end(),
	                         [](const auto &left, const auto &right) { return left->BlockId() == right->BlockId(); }),
	             blocks.end());
	result.prefetched_blocks = blocks.size();
	PrefetchStorageBlocks(context, source->collection->GetBlockManager().GetBufferManager(), blocks, result);
	ExecuteStorageScan(context, *source->collection, *source->row_groups, source->transaction, column_ids, column_types,
	                   windows, INSTANT_BLOCK_TASK_LIMIT, collect_timing, snapshot_safe, local_predicate, result);
	result.scan_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - scan_started).count();
	return result;
}

} // namespace duckdb
