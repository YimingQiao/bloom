#pragma once

#include "cardinality_estimator.hpp"
#include "predicate_transfer/config.hpp"

namespace duckdb {

class SamplingCardinalityEstimator : public RPTCardinalityEstimator {
public:
	explicit SamplingCardinalityEstimator(ClientContext &context, double sample_rate = 0.01,
	                                      idx_t sample_target = 10000, string cache_dir = "auto",
	                                      bool memory_cache = true)
	    : context_(context), sample_rate_(sample_rate), sample_target_(sample_target == 0 ? 1 : sample_target),
	      cache_dir_(std::move(cache_dir)), memory_cache_(memory_cache) {
	}

	idx_t Estimate(const LogicalOperator &op) override;
	idx_t Estimate(const LogicalOperator &op, const vector<DirectFilterInfo> &filters) override;
	idx_t Estimate(TableScanner &scanner, const vector<DirectFilterInfo> &filters) override;

private:
	//! A table-level raw-data sample (no local predicate applied). The stored
	//! sample holds ALL table columns in storage order — exactly one sample per
	//! (table, N), shared by every query. Each query reads it with
	//! column pruning (CDC subset scan over needed_columns), so estimation cost
	//! matches a per-query narrow sample. Local WHERE and BF filters are
	//! evaluated on the pruned view and extrapolated by
	//! total_rows / sample_row_count.
	struct SampleEntry {
		shared_ptr<ColumnDataCollection> sample_cdc; // full-width, storage order
		idx_t total_rows = 0;                        // full base-table row count (from catalog)
		idx_t sample_row_count = 0;                  // actual rows in sample_cdc (extrapolation denom)
		//! Storage-column positions this query references (narrow view layout).
		vector<column_t> needed_columns;
		//! Query binding of each narrow-view column, aligned with needed_columns.
		vector<ColumnBinding> output_bindings;
		//! Lazily materialized: the NARROW sample rows that pass op's local
		//! predicate. Repeated BF estimates during flooding scan only this
		//! (smaller) set instead of re-evaluating the local predicate.
		shared_ptr<ColumnDataCollection> local_cdc;
		idx_t local_survivors = 0;
		bool local_computed = false;
	};

	SampleEntry &GetOrCreateSample(const LogicalOperator &op);
	//! Stable sample identity: (database, table, schema fingerprint, N)
	//! — one sample per table.
	//! Empty when the scanned object is not a base table. Doubles as the
	//! in-memory ObjectCache key and (hashed) as the on-disk file name.
	string SampleCacheKey(const LogicalGet &get);
	//! Disk sample cache: file named by hash(key), chunk-based binary format
	//! (key + types + DataChunk stream). Empty cache_dir_ disables persistence.
	//! Load returns null on miss/error; Save is best-effort.
	string SampleCachePath(const LogicalGet &get, const string &key);
	shared_ptr<ColumnDataCollection> LoadSampleFromDisk(const string &key, const string &path);
	void SaveSampleToDisk(const string &key, const string &path, const ColumnDataCollection &cdc);
	//! Process-level sample reuse via the DatabaseInstance ObjectCache (LRU,
	//! buffer-pool accounted): repeated PREPAREs skip disk deserialization.
	shared_ptr<ColumnDataCollection> GetFromMemoryCache(const string &key);
	void PublishToMemoryCache(const string &key, const shared_ptr<ColumnDataCollection> &cdc);
	//! Materialize (once) the sample rows passing op's local predicate into
	//! sample.local_cdc, evaluating LogicalFilter exprs + GET.table_filters.
	void EnsureLocalFiltered(SampleEntry &sample, const LogicalOperator &op);
	//! Apply BF filters (if any) on top of the local-filtered sample, count
	//! survivors, extrapolate to the full table.
	idx_t EstimateOnSample(SampleEntry &sample, const LogicalOperator &op, const vector<DirectFilterInfo> &filters);
	idx_t SampleCDC(const LogicalOperator &op);

	ClientContext &context_;
	double sample_rate_;
	idx_t sample_target_;
	string cache_dir_;
	bool memory_cache_;
	unordered_map<const LogicalOperator *, SampleEntry> sample_cache_;
};

} // namespace duckdb
