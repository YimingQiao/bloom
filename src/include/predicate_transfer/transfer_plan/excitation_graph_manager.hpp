#pragma once

#include "base_graph_manager.hpp"
#include "predicate_transfer/cardinality_estimation/cardinality_estimator.hpp"
#include "predicate_transfer/table_scanner.hpp"
#include "predicate_transfer/transfer_plan/rpt_filter_cache.hpp"
#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"
#include "predicate_transfer/transfer_plan/transfer_executor.hpp"

#include <functional>
#include <unordered_map>
#include <string>

namespace duckdb {

//! Per-table rewrite decision returned by GetTableResult(). RewriteQueryPlan
//! dispatches on `kind`: Empty → LogicalEmptyResult; MemoryScan →
//! BuildMemoryScan(*scanner); DefaultScan → keep op, inject filters into leaf Get.
struct TableTransferResult {
	enum class Kind { Empty, MemoryScan, DefaultScan };
	Kind kind;
	TableScanner *scanner;                          // MemoryScan only
	shared_ptr<RPTFilter> row_id_filter;            // DefaultScan (nullable)
	const vector<DirectFilterInfo> *direct_filters; // DefaultScan (nullable)
};

//! Lineage flooding for predicate transfer. Two phases:
//!   1. Flooding — cardinality estimation decides BF propagation
//!   2. Materialization — scan tables, apply BFs, collect row IDs
class ExcitationGraphManager : public BaseGraphManager {
public:
	using BaseGraphManager::BaseGraphManager;

	string TransferPlanToString() const override;
	string TablesToString() const override;

	TableTransferResult GetTableResult(idx_t table_id, LogicalOperator &op);

	void ClearMaterializedScanners() {
		executor_.Clear();
	}

	//! Tables to skip entirely (e.g. below TOP_N/LIMIT). Call before ExecuteTransfer().
	void SetProtectedTables(unordered_set<idx_t> protected_tables) {
		protected_tables_ = std::move(protected_tables);
	}

protected:
	void ExecuteTransfer() override;

private:
	void InitEstimator();

	// Phase 1: flooding
	void InitializeWorkingSet();
	void PruneRedundantSteps(vector<TransferStep> &steps);
	idx_t SelectSmallestActiveTable() const;
	idx_t ComputeBaseTableRows(const LogicalOperator &op) const;

	// Edge construction
	vector<shared_ptr<GraphEdge>> CollectOutgoingEdges(idx_t table_id) const;
	shared_ptr<GraphEdge> MakeBloomFilterEdge(idx_t source_id, idx_t dest_id, const EdgeInfo &edge) const;

	// Cardinality update
	void UpdateNeighborCardinalities(const vector<shared_ptr<GraphEdge>> &candidates);
	void PropagateZeroCardinality(idx_t table_id, const vector<shared_ptr<GraphEdge>> &candidates);

	// Manager-side helpers coordinating TransferExecutor
	void PrepareSourceTable(idx_t source_table_id);
	void GenerateJoinStageExecutionPlan();

	vector<shared_ptr<GraphEdge>> ActivateTables(idx_t source_table_id, const vector<shared_ptr<GraphEdge>> &edges);

	//! Filter attached to a destination during flooding.
	struct CascadeFilter {
		vector<ColumnBinding> bindings; // size 1 = single column, N = composite key
		shared_ptr<RPTFilter> filter;
		LineageTracker::Snapshot lineage; // for subsumption + oracle cache key
	};

	//! Flooding state — rebuilt per ExecuteTransfer() by InitializeWorkingSet.
	struct FloodingState {
		unordered_map<idx_t, double> cardinality;
		unordered_map<idx_t, double> cardinality_baseline;
		LineageTracker lineage_tracker;
		unordered_set<idx_t> active_nodes;
		unordered_map<idx_t, vector<idx_t>> dependencies;
		idx_t next_step_id = 0;
		//! dest_id → src_id → edges pointing TO dest_id. O(neighbors) incoming lookup.
		unordered_map<idx_t, unordered_map<idx_t, vector<const EdgeInfo *>>> reverse_neighbor_matrix;

		void Reset() {
			cardinality.clear();
			cardinality_baseline.clear();
			lineage_tracker.Clear();
			active_nodes.clear();
			dependencies.clear();
			reverse_neighbor_matrix.clear();
			next_step_id = 0;
		}
	};

	FloodingState state_;
	unordered_set<idx_t> protected_tables_; // survives Reset (external config)

	unique_ptr<RPTCardinalityEstimator> estimator_;
	TransferExecutor executor_ {optimizer, context, config};

	// Cascade / output state (scanners live in executor_)
	unordered_map<idx_t, vector<CascadeFilter>> cascade_filters_;
	unordered_map<idx_t, shared_ptr<RPTFilter>> row_id_filters_;
	unordered_map<idx_t, vector<DirectFilterInfo>> direct_filters_;
	RPTFilterCache filter_cache_;
};

} // namespace duckdb
