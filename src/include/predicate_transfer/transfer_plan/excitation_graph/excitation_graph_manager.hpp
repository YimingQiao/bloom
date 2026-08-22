#pragma once

#include "predicate_transfer/transfer_plan/base_graph_manager.hpp"
#include "predicate_transfer/cardinality_estimation/cardinality_estimator.hpp"
#include "predicate_transfer/table_scanner/table_scanner.hpp"
#include "predicate_transfer/transfer_plan/excitation_graph/equality_domain_tracker.hpp"
#include "predicate_transfer/transfer_plan/excitation_graph/working_set.hpp"
#include "predicate_transfer/transfer_plan/rpt_filter_cache.hpp"
#include "predicate_transfer/transfer_plan/transfer_executor.hpp"

#include <functional>
#include <sstream>
#include <unordered_map>
#include <string>

namespace duckdb {

class TemporaryMemoryState;

//! Per-table rewrite decision returned by GetTableResult(). RewriteQueryPlan
//! dispatches on `kind`: Empty → LogicalEmptyResult; MemoryScan →
//! BuildMemoryScan(*scanner); DefaultScan → keep op, inject filters into leaf Get.
struct TableTransferResult {
	enum class Kind { Empty, MemoryScan, DefaultScan };
	Kind kind;
	TableScanner *scanner;                          // MemoryScan only
	shared_ptr<RPTFilter> row_id_filter;            // DefaultScan (nullable)
	const vector<DirectFilterInfo> *direct_filters; // DefaultScan (nullable)
	bool owns_materialized_data = false;            // MemoryScan only
};

//! Lineage flooding for predicate transfer. Two phases:
//!   1. Flooding — cardinality estimation decides BF propagation
//!   2. Materialization — scan tables, apply BFs, collect row IDs
class ExcitationGraphManager : public BaseGraphManager {
public:
	using BaseGraphManager::BaseGraphManager;

	string TransferPlanToString() override;
	string TablesToString() override;

	TableTransferResult GetTableResult(idx_t table_id, LogicalOperator &op);

	void ClearMaterializedScanners() {
		executor_.Clear();
	}
	shared_ptr<ColumnDataCollection> RetainMaterializedDataForExecution(shared_ptr<ColumnDataCollection> data,
	                                                                    bool owns_materialized_data);
	shared_ptr<RPTFilter> RetainFilterForExecution(shared_ptr<RPTFilter> filter);
	void FinalizeMemoryReservationForExecution();

	//! Tables to skip entirely (e.g. below TOP_N/LIMIT). Call before ExecuteTransfer().
	void SetProtectedTables(unordered_set<idx_t> protected_tables) {
		protected_tables_ = std::move(protected_tables);
	}
	bool WholeQueryMemoryFallback() const {
		return memory_stopped_ && materialized_source_count_ == 0;
	}

protected:
	void ExecuteTransfer() override;

private:
	static idx_t SaturatingAdd(idx_t left, idx_t right);
	static idx_t SaturatingMultiply(idx_t left, idx_t right);
	static void AppendEdgeSignature(std::stringstream &canonical, idx_t source_id, const GraphEdge &edge);
	void InitEstimator();
	void InitializeMemoryBudget();
	idx_t EstimateInitialSampleMemory() const;
	idx_t EstimateCollectionMemory(idx_t rows, const vector<LogicalType> &types) const;
	void EstimateFilterMemory(idx_t rows, const vector<shared_ptr<GraphEdge>> &edges, idx_t &persistent,
	                          idx_t &temporary) const;
	idx_t RetainedMemoryUsage() const;
	bool FitsMemoryBudget(idx_t retained, idx_t additional);
	void ResizeMemoryReservation(idx_t size);
	void StopForMemory(const char *phase, idx_t requested, idx_t retained);
	void AccountFilterMemory(const shared_ptr<RPTFilter> &filter);

	// Phase 1: flooding
	void InitializeWorkingSet();
	void PruneRedundantSteps(vector<TransferStep> &steps);
	idx_t ComputeBaseTableRows(const LogicalOperator &op) const;
	idx_t ComputeBaseTableRowIdSpan(const LogicalOperator &op) const;

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

	ExcitationWorkingSet state_;
	EqualityDomainTracker equality_domains_;
	unordered_set<idx_t> protected_tables_; // survives Reset (external config)

	unique_ptr<RPTCardinalityEstimator> estimator_;
	TransferExecutor executor_ {optimizer, context, config};

	// Cascade / output state (scanners live in executor_)
	unordered_map<idx_t, vector<CascadeFilter>> cascade_filters_;
	unordered_map<idx_t, shared_ptr<RPTFilter>> row_id_filters_;
	unordered_map<idx_t, vector<DirectFilterInfo>> direct_filters_;
	RPTFilterCache filter_cache_;
	//! Every excitation round before final-plan pruning. This distinguishes
	//! identical final edge sets that were reached through different (and
	//! potentially much more expensive) materialization schedules.
	string execution_history_;
	idx_t execution_action_count_ = 0;
	idx_t execution_round_count_ = 0;

	//! Bloom uses FORCE_MATERIALIZED in-memory collections. Admission is therefore
	//! query-global and happens before every potentially large allocation; there
	//! is deliberately no spill path.
	idx_t memory_budget_ = 0;
	shared_ptr<TemporaryMemoryState> memory_state_;
	idx_t filter_memory_used_ = 0;
	unordered_set<const RPTFilter *> accounted_filters_;
	idx_t execution_memory_used_ = 0;
	unordered_set<const ColumnDataCollection *> execution_collections_;
	unordered_set<const RPTFilter *> execution_filters_;
	idx_t materialized_source_count_ = 0;
	idx_t structured_log_sequence_ = 0;
	bool memory_stopped_ = false;
};

} // namespace duckdb
