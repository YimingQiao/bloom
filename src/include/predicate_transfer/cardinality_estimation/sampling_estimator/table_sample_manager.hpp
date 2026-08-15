#pragma once

#include "predicate_transfer/config.hpp"

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalGet;
class LogicalOperator;
class TableCatalogEntry;
struct InstantSampleResult;

//! Owns query-local table samples and their lazily materialized local-filter
//! views. It chooses prepared versus instant acquisition, while cardinality
//! extrapolation remains the estimator's responsibility.
class TableSampleManager {
public:
	struct Entry {
		shared_ptr<ColumnDataCollection> sample;
		string table_name;
		idx_t total_rows = 0;
		idx_t sampled_rows = 0;
		idx_t sample_access_points = 0;

		//! CDC positions read into the narrow estimation view.
		vector<column_t> sample_column_positions;
		//! Storage columns represented by the narrow view, in the same order.
		vector<column_t> needed_storage_columns;
		//! Query binding of each narrow-view column, aligned with both vectors.
		vector<ColumnBinding> output_bindings;

		//! Sample rows surviving the query's local predicate. Instant sampling
		//! normally builds this view during its physical scan; prepared sampling
		//! materializes it lazily on first use.
		shared_ptr<ColumnDataCollection> locally_filtered;
		idx_t local_survivors = 0;
		bool local_filter_evaluated = false;
		bool sample_acquisition_attempted = false;
	};

	TableSampleManager(ClientContext &context, RPTSamplingConfig config)
	    : context_(context), config_(std::move(config)) {
	}

	//! Register query-local table metadata without performing sample I/O.
	Entry &GetEntry(const LogicalOperator &op);
	//! Acquire the configured sample once, when an estimate actually needs it.
	void EnsureSample(Entry &sample, const LogicalOperator &op);
	//! Return an exact local cardinality when a catalog scan has no predicate,
	//! or when storage statistics prove all pushed-down filters true or false.
	//! Table functions and predicates not settled by statistics return invalid.
	optional_idx TryGetExactLocalCardinality(const Entry &sample, const LogicalOperator &op) const;
	void EnsureLocalFilter(Entry &sample, const LogicalOperator &op);

private:
	bool LogEnabled() const;
	unique_ptr<Expression> BuildLocalPredicate(const Entry &sample, const LogicalOperator &op,
	                                           const vector<LogicalType> &sample_types, idx_t &predicate_count,
	                                           bool emit_log) const;
	void BuildInstantSample(Entry &sample, const LogicalOperator &op, LogicalGet &get,
	                        optional_ptr<TableCatalogEntry> table, uint64_t table_seed);
	void AdoptInstantSample(Entry &sample, InstantSampleResult result, idx_t predicate_count, uint64_t table_seed);
	void LogInstantSample(const Entry &sample, const InstantSampleResult &result, idx_t predicate_count,
	                      uint64_t table_seed) const;

	ClientContext &context_;
	RPTSamplingConfig config_;
	unordered_map<const LogicalOperator *, Entry> entries_;
};

} // namespace duckdb
