#pragma once

#include "predicate_transfer/table_scanner/filter_set.hpp"
#include "predicate_transfer/table_scanner/materialization.hpp"

namespace duckdb {

//! Task count for parallel scans over a materialized collection. Eight chunks
//! (~16K rows) per task amortize scheduling while retaining parallelism.
idx_t RPTScanTaskCount(ClientContext &context, const ColumnDataCollection &collection);

//! Coordinates a materialized logical source with its transfer-filter set.
//! Materialization owns source/CDC/cursor state; ScannerFilterSet owns filters.
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

	TableScanner(Optimizer &optimizer, ClientContext &context, LogicalOperator &table_op,
	             bool enable_late_materialization = false);

	void SetRequiredColumns(const column_binding_set_t &bindings);
	void Materialize();

	void AddFilter(ColumnBinding binding, shared_ptr<RPTFilter> filter, size_t identity_hash = 0);
	void AddFilter(const vector<ColumnBinding> &bindings, shared_ptr<RPTFilter> filter, size_t identity_hash = 0);
	size_t FilterStateFingerprint() const;

	void InitScanChunk(DataChunk &chunk) const;
	bool Scan(DataChunk &chunk);
	void ResetScan();
	idx_t Count() const;
	CompactResult Compact(const vector<StatsRequest> &stats_requests = {});

	idx_t FindChunkCol(const ColumnBinding &binding) const {
		return materialization_.FindChunkCol(binding);
	}
	idx_t GetRowIdChunkCol() const {
		return materialization_.GetRowIdChunkCol();
	}
	bool IsPruned() const {
		return materialization_.IsPruned();
	}
	bool IsMaterialized() const {
		return materialization_.IsMaterialized();
	}
	bool HasPendingExprFilter() const {
		return materialization_.GetPendingExpression() != nullptr;
	}
	bool NeedsCompaction() const {
		return !filters_.Empty() || HasPendingExprFilter();
	}

	shared_ptr<ColumnDataCollection> TakeData() {
		return materialization_.TakeData();
	}
	ColumnDataCollection *GetData() const {
		return materialization_.GetData();
	}
	const vector<ColumnBinding> &GetOutputBindings() const {
		return materialization_.GetOutputBindings();
	}
	LogicalOperator &GetTableOp() const {
		return materialization_.GetTableOp();
	}
	const vector<ScannerFilterSet::Entry> &GetFilters() const {
		return filters_.Entries();
	}
	TableMaterialization::PendingExpression *GetPendingExprFilter() const {
		return materialization_.GetPendingExpression();
	}

private:
	ClientContext &context_;
	TableMaterialization materialization_;
	ScannerFilterSet filters_;
};

} // namespace duckdb
