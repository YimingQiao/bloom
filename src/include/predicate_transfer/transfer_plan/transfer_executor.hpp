#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/optimizer/optimizer.hpp"

#include "predicate_transfer/config.hpp"
#include "predicate_transfer/filter/filter.hpp"
#include "predicate_transfer/table_scanner/table_scanner.hpp"
#include "transfer_types.hpp"

namespace duckdb {

//! Owns TableScanners and every code path that touches in-memory table data
//! (scanning, compaction, filter construction, row-id bitmap collection). Has
//! no knowledge of the flooding algorithm — it only reacts to commands from
//! ExcitationGraphManager.
class TransferExecutor {
public:
	struct FilterBuildSpec {
		vector<ColumnBinding> key_bindings;
		vector<LogicalType> key_types;
		bool track_exact_domain = false;
	};

	TransferExecutor(Optimizer &optimizer, ClientContext &context, const RPTOptimizerConfig &config);

	//! Construct a TableScanner without driving Materialize. Idempotent.
	//! The TableScanner constructor's CHUNK_GET fast-path already marks
	//! the scanner materialized, so Register alone suffices for CHUNK_GET.
	TableScanner *Register(LogicalOperator &op);

	//! Register + materialize, pruning to `required`. Idempotent. Callers
	//! must attach cascade filters via AttachFilterToScanner *before*
	//! calling this so they can be pushed into the underlying Get.
	TableScanner *EnsureMaterialized(LogicalOperator &op, const column_binding_set_t &required);

	//! Registered AND materialized. Use this over Has() when the caller
	//! needs actual in-memory data.
	bool IsMaterialized(LogicalOperator &op) const;

	//! Build all distinct outgoing filters for one source together. Compaction,
	//! min/max discovery, and insertion are each performed at most once over the
	//! source collection, with the scan work shared by all requested filters.
	vector<shared_ptr<RPTFilter>> BuildTransferFilters(LogicalOperator &op, const vector<FilterBuildSpec> &specs);

	//! Compact pending filters, then build a row-id BitmapFilter over the
	//! surviving rows. Null if the op has no scanner or no row-id column.
	shared_ptr<RPTFilter> FinalizeRowIDBitmap(LogicalOperator &op);

	//! Attach a filter to the scanner of `op`. No-op if `op` has no scanner.
	//! `identity_hash` is the filter's stable lineage-derived identity, kept on
	//! the FilterEntry for the oracle's cardinality-cache key.
	void AttachFilterToScanner(LogicalOperator &op, const vector<ColumnBinding> &dest_bindings,
	                           const shared_ptr<RPTFilter> &filter, size_t identity_hash = 0);

	TableScanner *Find(LogicalOperator &op) const;
	//! Bytes owned by RPT-created materializations. Collections borrowed from an
	//! existing CHUNK_GET are excluded unless compaction replaces them.
	idx_t MaterializedMemoryUsage() const;
	bool OwnsMaterializedData(LogicalOperator &op) const;
	void Remove(LogicalOperator &op);
	bool Has(LogicalOperator &op) const {
		return Find(op) != nullptr;
	}
	void Clear() {
		scanners_.clear();
		borrowed_data_.clear();
	}

private:
	Optimizer &optimizer_;
	ClientContext &context_;
	const RPTOptimizerConfig &config_;
	unordered_map<LogicalOperator *, unique_ptr<TableScanner>> scanners_;
	unordered_map<LogicalOperator *, const ColumnDataCollection *> borrowed_data_;
};

} // namespace duckdb
