#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ScannerFilterSet;

//! Owns the lifetime of one logical table source after it is made scannable:
//! the logical source, materialized collection, output schema, local predicate,
//! projection, scan cursor, and late-materialization metadata.
class TableMaterialization {
public:
	struct PendingExpression {
		vector<unique_ptr<Expression>> expressions;
		unique_ptr<ExpressionExecutor> executor;
		vector<idx_t> projection_map;
		DataChunk scratch;
	};

	TableMaterialization(Optimizer &optimizer, ClientContext &context, LogicalOperator &table_op,
	                     bool enable_late_materialization);

	void SetRequiredColumns(const column_binding_set_t &bindings);
	void Materialize(ScannerFilterSet &filters);

	void InitScanChunk(DataChunk &chunk) const;
	bool Scan(DataChunk &chunk);
	void ResetScan();
	idx_t Count() const;
	idx_t FindChunkCol(const ColumnBinding &binding) const;

	void ReplaceData(unique_ptr<ColumnDataCollection> data);
	shared_ptr<ColumnDataCollection> TakeData() {
		return std::move(data_);
	}

	ColumnDataCollection *GetData() const {
		return data_.get();
	}
	PendingExpression *GetPendingExpression() const {
		return pending_expression_.get();
	}
	const vector<ColumnBinding> &GetOutputBindings() const {
		return output_bindings_;
	}
	LogicalOperator &GetTableOp() const {
		return table_op_;
	}
	idx_t GetRowIdChunkCol() const {
		return rowid_chunk_col_;
	}
	bool IsPruned() const {
		return is_pruned_;
	}
	bool IsMaterialized() const {
		return materialized_;
	}

private:
	bool ExecutePlan(unique_ptr<LogicalOperator> plan);
	bool LogEnabled() const;

	Optimizer &optimizer_;
	ClientContext &context_;
	LogicalOperator &table_op_;
	bool enable_late_materialization_;

	shared_ptr<ColumnDataCollection> data_;
	ColumnDataScanState scan_state_;
	unique_ptr<PendingExpression> pending_expression_;
	vector<ColumnBinding> output_bindings_;
	column_binding_set_t required_bindings_;

	bool materialized_ = false;
	bool is_pruned_ = false;
	idx_t rowid_chunk_col_ = DConstants::INVALID_INDEX;
};

} // namespace duckdb
