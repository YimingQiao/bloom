#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/storage/storage_index.hpp"

#include <random>

namespace duckdb {

class ClientContext;
class DataTable;
class Expression;
class LogicalGet;

//! Private facade for optimizer-time physical sampling. The estimator chooses
//! a backend; this module owns selection, scheduling, decoding, and diagnostics.
enum class InstantSampleStatus : uint8_t {
	SUCCESS,
	UNSUPPORTED_SOURCE,
	UNSUPPORTED_SNAPSHOT,
};

enum class InstantSampleSource : uint8_t {
	UNKNOWN,
	NATIVE_SCATTERED,
	NATIVE_BLOCK,
	PARQUET,
};

//! One physical row range selected by an instant sampler.
struct InstantSampleRange {
	idx_t row_start;
	idx_t row_count;
};

//! A bounded sample plan for one single-file Parquet scan.
struct InstantParquetSamplePlan {
	bool valid = false;
	idx_t total_row_groups = 0;
	idx_t selected_row_groups = 0;
	idx_t candidate_rows = 0;
	idx_t source_rows = 0;
	vector<LogicalType> output_types;
	vector<InstantSampleRange> ranges;

	explicit operator bool() const {
		return valid;
	}
};

//! Data and diagnostics returned by an instant physical sampler.
struct InstantSampleResult {
	InstantSampleSource source = InstantSampleSource::UNKNOWN;
	InstantSampleStatus status = InstantSampleStatus::UNSUPPORTED_SOURCE;
	string unavailable_reason;
	shared_ptr<ColumnDataCollection> sample;
	idx_t total_row_groups = 0;
	idx_t selected_row_groups = 0;
	idx_t candidate_rows = 0;
	idx_t candidate_chunks = 0;
	double metadata_ms = 0;
	double prefetch_ms = 0;
	double scan_ms = 0;
	double scheduler_setup_ms = 0;
	double schedule_ms = 0;
	double wait_ms = 0;
	double locate_ms = 0;
	double decode_ms = 0;
	double filter_ms = 0;
	double append_ms = 0;
	double combine_ms = 0;
	double task_wall_ms = 0;
	idx_t task_count = 0;
	idx_t prefetch_task_count = 0;
	idx_t prefetched_blocks = 0;
	idx_t decoded_rows = 0;
	idx_t sampled_rows = 0;
	vector<idx_t> selected_chunk_ordinals;
	vector<idx_t> selected_row_offsets;

	explicit operator bool() const {
		return status == InstantSampleStatus::SUCCESS;
	}
};

//! Add a bounded, stratified row-group selection to a single-file Parquet GET.
InstantParquetSamplePlan PlanInstantParquetSample(ClientContext &context, LogicalGet &get, idx_t target_rows,
                                                  idx_t target_row_groups, uint64_t seed);

//! Execute a configured Parquet sample as bounded tasks on DuckDB's async scheduler.
InstantSampleResult BuildInstantParquetSample(ClientContext &context, LogicalGet &get,
                                              const vector<LogicalType> &output_types,
                                              const vector<InstantSampleRange> &sample_ranges, bool collect_timing,
                                              const Expression *local_predicate);

//! Read many small stratified ranges. Intended for resident native storage.
InstantSampleResult BuildInstantScatteredSample(ClientContext &context, DataTable &table,
                                                const vector<StorageIndex> &column_ids,
                                                const vector<LogicalType> &column_types, idx_t target_access_points,
                                                idx_t rows_per_access, bool collect_timing, bool snapshot_safe,
                                                const Expression *local_predicate, std::mt19937_64 &random);

//! Prefetch and decode a few aligned windows. Intended for cold native storage.
InstantSampleResult BuildInstantBlockSample(ClientContext &context, DataTable &table,
                                            const vector<StorageIndex> &column_ids,
                                            const vector<LogicalType> &column_types, idx_t target_rows,
                                            idx_t target_windows, bool collect_timing, bool snapshot_safe,
                                            const Expression *local_predicate, std::mt19937_64 &random);

} // namespace duckdb
