#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "predicate_transfer/filter/filter.hpp"
#include "predicate_transfer/transfer_plan/transfer_types.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//! Task count for parallel scans over a materialized collection. Eight chunks
//! (~16K rows) per task amortize task-scheduling overhead while still engaging
//! every thread on mid-sized collections; collections under eight chunks stay
//! serial.
idx_t RPTScanTaskCount(ClientContext &context, const ColumnDataCollection &collection);

//===--------------------------------------------------------------------===//
// TableScanner
//===--------------------------------------------------------------------===//
//! Scans a table and stores all data in memory (ColumnDataCollection).
//! - Materialize(): one-time load via DuckDB executor (preserving filters)
//! - Scan(): iterate in-memory data with BF filters applied inline
//! - ResetScan(): re-iterate from the start
//!
//! Column addressing: all external APIs accept ColumnBinding.
//! Internally, chunk positions are resolved via GetColumnBindings()
//! (same approach as DuckDB's ColumnBindingResolver).
class TableScanner {
public:
	struct CompactResult {
		idx_t row_count = 0;
		struct ColumnStats {
			bool has_min_max = false;
			int64_t observed_min = 0;
			int64_t observed_max = 0;
		};
		vector<ColumnStats> column_stats;
	};
	struct StatsRequest {
		idx_t chunk_col;
		LogicalType type;
	};

	//! Takes a reference to the full table operator (e.g. Filter→Get or just Get).
	TableScanner(Optimizer &optimizer, ClientContext &context, LogicalOperator &table_op,
	             bool enable_late_materialization = false);

	//! Set which columns are required (join keys + filter keys).
	//! Must be called BEFORE Materialize(). Columns not in this set are pruned.
	void SetRequiredColumns(const unordered_set<ColumnBinding, ColumnBindingHashFunc> &bindings);

	void Materialize();

	//! Add a BF filter on the given column (identified by ColumnBinding).
	void AddFilter(ColumnBinding binding, shared_ptr<RPTFilter> filter, size_t identity_hash = 0);
	//! Add a composite-key filter — the filter is applied during Scan/Compact
	//! by combine-hashing the listed columns. A 1-element vector behaves the
	//! same as the single-binding overload.
	void AddFilter(const vector<ColumnBinding> &bindings, shared_ptr<RPTFilter> filter, size_t identity_hash = 0);
	//! Order-insensitive fingerprint over ALL attached filters (identity hash +
	//! key bindings each). The oracle mixes this into its cardinality-cache key:
	//! DirectFilterInfo alone cannot identify composite cascade filters, which
	//! previously let two different scanner filter states share one cached count.
	size_t FilterStateFingerprint() const;

	void InitScanChunk(DataChunk &chunk) const;
	bool Scan(DataChunk &chunk);
	void ResetScan();
	idx_t Count() const;

	//! Evaluates accumulated filters and creates a new dense ColumnDataCollection.
	//! Collects observed min/max for all requested integral key columns in the
	//! same scan that applies the deferred filters.
	CompactResult Compact(const vector<StatsRequest> &stats_requests = {});

	idx_t GetRowIdChunkCol() const {
		return rowid_chunk_col_;
	}

	//! Resolve a ColumnBinding to its chunk output position using output_bindings_.
	idx_t FindChunkCol(const ColumnBinding &binding) const;

	bool IsPruned() const {
		return is_pruned_;
	}
	bool IsMaterialized() const {
		return materialized_;
	}
	bool HasPendingExprFilter() const {
		return pending_expr_filter_ != nullptr;
	}

	//! Transfer ownership of the ColumnDataCollection out of the scanner.
	//! Returned as shared_ptr so callers that only need read access (e.g.
	//! SelectColumns) work, and so lifted CHUNK_GET scanners can share the
	//! same underlying collection with the rest of the plan.
	shared_ptr<ColumnDataCollection> TakeData() {
		return std::move(data_);
	}

	//! Output bindings of the materialized data (in chunk column order).
	const vector<ColumnBinding> &GetOutputBindings() const {
		return output_bindings_;
	}
	LogicalOperator &GetTableOp() const {
		return table_op_;
	}
	//! Peeled FILTER(...)→CHUNK_GET state for the zero-copy fast-path.
	struct PendingExprFilter {
		vector<unique_ptr<Expression>> exprs_holder;
		unique_ptr<ExpressionExecutor> executor;
		vector<idx_t> projection_map;
		DataChunk scratch;
	};

	struct FilterEntry {
		vector<ColumnBinding> bindings;
		vector<idx_t> chunk_cols;
		shared_ptr<RPTFilter> filter;
		//! Stable, content-derived identity of the attached filter (lineage
		//! snapshot hash). Mixed into the oracle's cardinality-cache key so two
		//! different scanner filter states never share a cached count.
		size_t identity_hash = 0;
	};

	ColumnDataCollection *GetData() const {
		return data_.get();
	}
	const vector<FilterEntry> &GetFilters() const {
		return filters_;
	}
	PendingExprFilter *GetPendingExprFilter() const {
		return pending_expr_filter_.get();
	}

private:
	void ApplyFilters(DataChunk &chunk);
	void InjectTableFilters(LogicalOperator &plan_copy);
	bool ExecutePlan(unique_ptr<LogicalOperator> plan_copy);
	//! RPT diagnostics enabled (profiler or rpt_log_transfer_steps).
	bool RptLogEnabled() const;

	Optimizer &optimizer_;
	ClientContext &context_;
	LogicalOperator &table_op_;
	bool enable_late_materialization_;
	vector<FilterEntry> filters_;
	unique_ptr<PendingExprFilter> pending_expr_filter_;

	shared_ptr<ColumnDataCollection> data_;
	ColumnDataScanState scan_state_;
	bool materialized_ = false;
	bool is_pruned_ = false;
	idx_t rowid_chunk_col_ = DConstants::INVALID_INDEX;

	//! Columns required for materialization (empty = keep all).
	unordered_set<ColumnBinding, ColumnBindingHashFunc> required_bindings_;
	//! Output bindings of the materialized plan, in chunk column order.
	vector<ColumnBinding> output_bindings_;
};

} // namespace duckdb
