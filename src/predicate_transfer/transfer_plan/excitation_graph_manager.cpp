#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "predicate_transfer/transfer_plan/excitation_graph_manager.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator.hpp"
#include "predicate_transfer/table_operator_manager.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/client_config.hpp"

#include <chrono>
#include <iostream>
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Initialization
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::InitEstimator() {
	estimator_ =
	    make_uniq<SamplingCardinalityEstimator>(context, config.sample_rate, config.sample_materialization_size,
	                                            config.sample_cache_dir, config.sample_memory_cache);
}

idx_t ExcitationGraphManager::ComputeBaseTableRows(const LogicalOperator &op) const {
	// Walk through the operator tree to find the underlying LogicalGet.
	const LogicalOperator *leaf = &op;
	while (leaf->type != LogicalOperatorType::LOGICAL_GET && !leaf->children.empty()) {
		leaf = leaf->children[0].get();
	}

	idx_t base_rows = op.estimated_cardinality;
	if (leaf->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = leaf->Cast<LogicalGet>();
		if (auto entry = get.GetTable()) {
			base_rows = entry->GetStorage().GetTotalRows();
		}
	}
	return base_rows;
}

void ExcitationGraphManager::InitializeWorkingSet() {
	state_.Reset();
	filter_cache_ = RPTFilterCache {};

	const auto &all_tables = table_operator_manager.GetAllTableOperators();

	// Register CDC-alias groups before any SeedColumn so CTE mirrors share
	// lineage from the start (prevents self-to-self BF transfer).
	for (auto &entry : all_tables) {
		auto &aliases = table_operator_manager.GetCDCAliases(entry.first);
		if (aliases.size() >= 2) {
			state_.lineage_tracker.RegisterAliasGroup(aliases);
		}
	}

	// Seed only join-key columns — those are the only ones flooding reads.
	auto seed_column_lineage = [this](idx_t table_id) {
		auto nm_it = neighbor_matrix.find(table_id);
		if (nm_it == neighbor_matrix.end()) {
			return;
		}
		for (auto &neighbor_entry : nm_it->second) {
			for (auto &edge : neighbor_entry.second) {
				for (auto &b : edge.left_bindings) {
					state_.lineage_tracker.SeedColumn(b);
				}
			}
		}
	};

	auto init_lineage = [&](idx_t table_id) {
		state_.lineage_tracker.InitTable(table_id);
		seed_column_lineage(table_id);
	};

	for (auto &entry : all_tables) {
		idx_t table_id = entry.first;
		auto &op = entry.second.get();

		// Protected (e.g. below TOP_N/LIMIT): seed lineage only — do not excite.
		if (protected_tables_.count(table_id)) {
			init_lineage(table_id);
			continue;
		}

		// Lifted CTE (bare or FILTER+CHUNK_GET): baseline = CDC size from the
		// leaf; pre-register so RewriteQueryPlan can take MemoryScan.
		const LogicalOperator *leaf = &op;
		while (leaf->type == LogicalOperatorType::LOGICAL_FILTER && leaf->children.size() == 1) {
			leaf = leaf->children[0].get();
		}
		if (leaf->type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
			auto &chunk_get = leaf->Cast<LogicalColumnDataGet>();
			double base_rows = chunk_get.collection ? chunk_get.collection->Count() : 0;
			// If a FILTER sits above CHUNK_GET we need the estimator for the
			// post-filter cardinality; for bare CHUNK_GET card == baseline.
			double row_count = (leaf == &op) ? base_rows : estimator_->Estimate(op);
			state_.cardinality[table_id] = row_count;
			state_.cardinality_baseline[table_id] = base_rows;
			init_lineage(table_id);
			state_.active_nodes.insert(table_id);
			executor_.Register(op);
			continue;
		}

		double true_card = estimator_->Estimate(op);
		double base_rows = ComputeBaseTableRows(op);
		state_.cardinality[table_id] = true_card;
		state_.cardinality_baseline[table_id] = base_rows;

		// Only excite if local filters reduce cardinality enough.
		bool will_be_active = true_card < base_rows * config.excitation_threshold;
		init_lineage(table_id);
		if (will_be_active) {
			state_.active_nodes.insert(table_id);
		}
	}

	// Reverse adjacency for O(neighbors) incoming-edge lookup in PrepareSourceTable.
	for (auto &[src_id, neighbors] : neighbor_matrix) {
		for (auto &[dst_id, edges] : neighbors) {
			for (auto &edge : edges) {
				state_.reverse_neighbor_matrix[dst_id][src_id].push_back(&edge);
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Table selection
//===--------------------------------------------------------------------===//

idx_t ExcitationGraphManager::SelectSmallestActiveTable() const {
	idx_t best_id = std::numeric_limits<idx_t>::max();
	double best_card = std::numeric_limits<double>::infinity();

	for (auto id : state_.active_nodes) {
		auto it = state_.cardinality.find(id);
		if (it != state_.cardinality.end()) {
			double card = it->second;
			if (card < best_card) {
				best_card = card;
				best_id = id;
			}
		}
	}
	return best_id;
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Edge construction
//===--------------------------------------------------------------------===//

shared_ptr<GraphEdge> ExcitationGraphManager::MakeBloomFilterEdge(idx_t source_id, idx_t dest_id,
                                                                  const EdgeInfo &edge) const {
	auto bf_edge = make_shared_ptr<GraphEdge>(source_id, dest_id);
	bf_edge->return_types = edge.return_types;

	// Because EdgeInfo in neighbor_matrix is symmetrically flipped,
	// edge.left_bindings strictly correspond to the source_id, and
	// edge.right_bindings correspond to the dest_id.
	D_ASSERT(!edge.left_bindings.empty());
	D_ASSERT(edge.left_bindings.size() == edge.right_bindings.size());
	D_ASSERT(edge.left_bindings.front().table_index.index == source_id);
	D_ASSERT(edge.right_bindings.front().table_index.index == dest_id);

	bf_edge->source_columns = edge.left_bindings;
	bf_edge->dest_columns = edge.right_bindings;
	return bf_edge;
}

vector<shared_ptr<GraphEdge>> ExcitationGraphManager::CollectOutgoingEdges(idx_t table_id) const {
	vector<shared_ptr<GraphEdge>> result;
	auto it = neighbor_matrix.find(table_id);
	if (it == neighbor_matrix.end()) {
		return result;
	}

	for (auto &neighbor_pair : it->second) {
		idx_t neighbor_id = neighbor_pair.first;
		auto &edges = neighbor_pair.second;
		auto state_it = state_.cardinality.find(neighbor_id);
		if (state_it != state_.cardinality.end() && state_it->second == 0.0) {
			continue;
		}
		// Skip sending filters to tables that are already small enough —
		// filtering them further has negligible benefit but costs materialization.
		if (state_it != state_.cardinality.end() &&
		    state_it->second <= static_cast<double>(config.small_table_threshold)) {
			continue;
		}

		for (auto &edge : edges) {
			if (edge.protect_right) {
				continue;
			}
			if (!state_.lineage_tracker.EdgeCarriesNewInfo(edge)) {
				continue;
			}
			result.push_back(MakeBloomFilterEdge(table_id, neighbor_id, edge));
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Cardinality update
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::UpdateNeighborCardinalities(const vector<shared_ptr<GraphEdge>> &candidates) {
	unordered_set<idx_t> seen_neighbors;
	for (auto &edge : candidates) {
		idx_t neighbor_id = edge->destination;
		if (!seen_neighbors.insert(neighbor_id).second) {
			continue;
		}
		auto &neighbor_state = state_.cardinality[neighbor_id];

		auto &dest_filters = cascade_filters_[neighbor_id];
		auto *dest_op = table_operator_manager.GetTableOperator(neighbor_id);
		if (!dest_op) {
			continue;
		}
		// Single-column cascade filters become direct filters for the
		// cardinality oracle. Composite-key filters cannot be expressed as
		// per-column TableFilters, so we skip them here — they only
		// participate via the materialized scanner path.
		vector<DirectFilterInfo> direct_filters;
		direct_filters.reserve(dest_filters.size());
		for (auto &dest_filter : dest_filters) {
			if (dest_filter.bindings.size() != 1) {
				continue;
			}
			direct_filters.push_back({dest_filter.bindings.front(), dest_filter.filter, dest_filter.lineage.lineages});
		}

		auto *dest_scanner = executor_.Find(*dest_op);
		if (dest_scanner) {
			idx_t new_card = estimator_->Estimate(*dest_scanner, direct_filters);
			neighbor_state = static_cast<double>(new_card);
		} else {
			idx_t new_card = estimator_->Estimate(*dest_op, direct_filters);
			neighbor_state = static_cast<double>(new_card);
		}

		// Update both table-level and per-column lineage (the latter is read
		// by the next round's CollectOutgoingEdges and ActivateTable for
		// subsumption, in column-granular mode).
		state_.lineage_tracker.Propagate(*edge);

		state_.dependencies[neighbor_id].push_back(edge->source);

		auto current_card = neighbor_state;
		auto last_card = state_.cardinality_baseline[neighbor_id];
		if (current_card < last_card * config.excitation_threshold) {
			state_.active_nodes.insert(neighbor_id);
		}
	}
}

void ExcitationGraphManager::PropagateZeroCardinality(idx_t table_id, const vector<shared_ptr<GraphEdge>> &candidates) {
	unordered_set<idx_t> seen_neighbors;

	for (auto &edge : candidates) {
		idx_t neighbor_id = edge->destination;
		if (!seen_neighbors.insert(neighbor_id).second) {
			continue;
		}

		auto &neighbor_state = state_.cardinality[neighbor_id];
		if (neighbor_state == 0.0) {
			continue;
		}

		neighbor_state = 0.0;
		state_.cardinality_baseline[neighbor_id] = 0.0;

		// Destination inherits the source lineage even though the filter
		// itself is just an empty-result marker.
		state_.lineage_tracker.Propagate(*edge);

		state_.dependencies[neighbor_id].push_back(table_id);
		cascade_filters_[neighbor_id].clear();
		direct_filters_[neighbor_id].clear();
		row_id_filters_.erase(neighbor_id);
		state_.active_nodes.insert(neighbor_id);
	}
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Lineage Flooding
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::PruneRedundantSteps(vector<TransferStep> &steps) {
	if (steps.empty()) {
		return;
	}

	// === Phase 1: Flatten all edges into a linear timeline ===
	// Each entry records: (step_index, source_id, dest_id, edge_ptr)
	struct EdgeRecord {
		idx_t step_index;
		idx_t source_id;
		idx_t dest_id;
		shared_ptr<GraphEdge> edge;
	};
	vector<EdgeRecord> timeline;
	for (idx_t i = 0; i < steps.size(); i++) {
		idx_t src = TableOperatorManager::GetScalarTableIndex(*steps[i].table);
		for (auto &edge : steps[i].create_bf) {
			timeline.push_back({i, src, edge->destination, edge});
		}
	}

	// === Phase 2: Build a set of step indices where each node acts as SOURCE ===
	// source_steps[node_id] = sorted list of step indices where node_id sends
	unordered_map<idx_t, vector<idx_t>> source_steps;
	for (auto &rec : timeline) {
		source_steps[rec.source_id].push_back(rec.step_index);
	}

	// === Phase 3: For each (src, dst) pair, find edges that can be safely pruned ===
	// Key: (src, dst) -> list of EdgeRecords in chronological order
	struct PairHash {
		size_t operator()(const pair<idx_t, idx_t> &p) const {
			return std::hash<idx_t> {}(p.first) ^ (std::hash<idx_t> {}(p.second) * 0x9e3779b97f4a7c15ULL);
		}
	};
	unordered_map<pair<idx_t, idx_t>, vector<idx_t>, PairHash> pair_to_timeline_indices;
	for (idx_t i = 0; i < timeline.size(); i++) {
		auto key = make_pair(timeline[i].source_id, timeline[i].dest_id);
		pair_to_timeline_indices[key].push_back(i);
	}

	// Mark edges to remove
	unordered_set<idx_t> remove_timeline_indices;

	for (auto &entry : pair_to_timeline_indices) {
		auto &pair = entry.first;
		auto &indices = entry.second;
		if (indices.size() <= 1) {
			continue;
		}

		idx_t dest_id = pair.second;
		auto &dest_source_steps = source_steps[dest_id];

		// For consecutive edges E1 (earlier) and E2 (later) with same (src, dst):
		// E1 is safe to prune if dest_id did NOT act as a source between E1 and E2.
		for (idx_t k = 0; k + 1 < indices.size(); k++) {
			idx_t e1_step = timeline[indices[k]].step_index;
			idx_t e2_step = timeline[indices[k + 1]].step_index;

			// Check: did dest_id send any outgoing edges in steps (e1_step, e2_step)?
			// Use binary search on the sorted source_steps list.
			auto lo = std::upper_bound(dest_source_steps.begin(), dest_source_steps.end(), e1_step);
			bool dest_sent_between = (lo != dest_source_steps.end() && *lo < e2_step);

			if (!dest_sent_between) {
				// dest didn't use E1's info to send anything before E2 arrived.
				// E2 is from the same source and strictly stronger. E1 is safe to remove.
				remove_timeline_indices.insert(indices[k]);
			}
		}
	}

	// === Phase 4: Rebuild steps, excluding pruned edges ===
	// Reconstruct each step's create_bf by keeping only non-removed edges.
	unordered_set<idx_t> removed_edge_ptrs;
	for (idx_t ri : remove_timeline_indices) {
		// Use the raw pointer value as a unique identifier for this specific edge
		removed_edge_ptrs.insert(reinterpret_cast<uintptr_t>(timeline[ri].edge.get()));
	}

	steps.erase(std::remove_if(steps.begin(), steps.end(),
	                           [&](TransferStep &step) {
		                           step.create_bf.erase(std::remove_if(step.create_bf.begin(), step.create_bf.end(),
		                                                               [&](const shared_ptr<GraphEdge> &e) {
			                                                               return removed_edge_ptrs.count(
			                                                                   reinterpret_cast<uintptr_t>(e.get()));
		                                                               }),
		                                                step.create_bf.end());
		                           return step.create_bf.empty();
	                           }),
	            steps.end());
}

void ExcitationGraphManager::ExecuteTransfer() {
	result_transfer_steps.clear();
	executor_.Clear();
	cascade_filters_.clear();
	row_id_filters_.clear();
	direct_filters_.clear();

	bool rpt_log = ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps;

	// Per-phase wall-clock accounting, printed with the transfer log. All
	// timestamps are taken only when logging is enabled.
	using SteadyClock = std::chrono::steady_clock;
	auto elapsed_ms = [](SteadyClock::time_point a, SteadyClock::time_point b) {
		return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(b - a).count();
	};
	double init_ms = 0, mat_ms = 0, bf_ms = 0, est_ms = 0, finalize_ms = 0;

	auto t_init = SteadyClock::now();
	InitEstimator();
	InitializeWorkingSet();
	if (rpt_log) {
		init_ms = elapsed_ms(t_init, SteadyClock::now());
	}

	if (rpt_log) {
		std::cerr << "[RPT-Excitation] === Initial table cardinalities ===" << '\n';
		for (auto &[tid, state] : state_.cardinality) {
			auto *op = table_operator_manager.GetTableOperator(tid);
			std::string name = op ? TableOperatorManager::GetTableName(*op) : "?";
			bool active = state_.active_nodes.count(tid) > 0;
			std::cerr << "  [" << tid << "] " << name << "  card=" << static_cast<idx_t>(state)
			          << "  baseline=" << static_cast<idx_t>(state_.cardinality_baseline[tid])
			          << (active ? "  ACTIVE" : "") << '\n';
		}
		std::cerr << "[RPT-Excitation] active_nodes count: " << state_.active_nodes.size() << '\n';
	}

	idx_t excitation_round = 0;

	// Main adaptive-excitation loop:
	// 1. pick the smallest active source table A
	// 2. materialize A once (with any filters it has already received)
	// 3. determine which outgoing edges from A still carry new information
	// 4. build/apply A's outgoing filters to the destination tables B
	// 5. re-estimate each affected B after those filters are attached
	while (!state_.active_nodes.empty()) {
		idx_t table_id = SelectSmallestActiveTable();
		state_.active_nodes.erase(table_id);

		excitation_round++;
		if (rpt_log) {
			auto *src_op = table_operator_manager.GetTableOperator(table_id);
			std::string src_name = src_op ? TableOperatorManager::GetTableName(*src_op) : "?";
			std::cerr << "[RPT-Excitation] --- Round " << excitation_round << ": source=[" << table_id << "] "
			          << src_name << "  card=" << static_cast<idx_t>(state_.cardinality[table_id])
			          << "  remaining_active=" << state_.active_nodes.size() << '\n';
		}

		auto informative_candidates = CollectOutgoingEdges(table_id);
		if (informative_candidates.empty()) {
			continue;
		}

		if (state_.cardinality[table_id] == 0.0) {
			TransferStep step;
			step.id = state_.next_step_id++;
			step.table = table_operator_manager.GetTableOperator(table_id);
			step.depends_on = state_.dependencies[table_id];
			step.create_bf = informative_candidates;

			PropagateZeroCardinality(table_id, informative_candidates);
			result_transfer_steps.push_back(std::move(step));
			continue;
		}

		auto t_mat = SteadyClock::now();
		PrepareSourceTable(table_id);
		double round_mat = rpt_log ? elapsed_ms(t_mat, SteadyClock::now()) : 0;
		mat_ms += round_mat;
		state_.cardinality_baseline[table_id] = state_.cardinality[table_id];
		auto *source_op = table_operator_manager.GetTableOperator(table_id);

		TransferStep step;
		step.id = state_.next_step_id++;
		step.table = source_op;
		step.depends_on = state_.dependencies[table_id];

		auto t_bf = SteadyClock::now();
		auto effective_candidates = ActivateTables(table_id, informative_candidates);
		double round_bf = rpt_log ? elapsed_ms(t_bf, SteadyClock::now()) : 0;
		bf_ms += round_bf;

		if (effective_candidates.empty()) {
			if (rpt_log) {
				std::cerr << "    [timing] materialize=" << round_mat << "ms build_bf=" << round_bf << "ms" << '\n';
			}
			continue;
		}
		step.create_bf = effective_candidates;

		auto t_est = SteadyClock::now();
		UpdateNeighborCardinalities(effective_candidates);
		double round_est = rpt_log ? elapsed_ms(t_est, SteadyClock::now()) : 0;
		est_ms += round_est;
		if (rpt_log) {
			std::cerr << "    [timing] materialize=" << round_mat << "ms build_bf=" << round_bf
			          << "ms re_estimate=" << round_est << "ms" << '\n';
		}

		if (rpt_log) {
			for (auto &edge : effective_candidates) {
				auto *dest_op = table_operator_manager.GetTableOperator(edge->destination);
				std::string dest_name = dest_op ? TableOperatorManager::GetTableName(*dest_op) : "?";
				bool reactivated = state_.active_nodes.count(edge->destination) > 0;
				std::cerr << "    -> [" << edge->destination << "] " << dest_name
				          << "  card=" << static_cast<idx_t>(state_.cardinality[edge->destination])
				          << (reactivated ? "  RE-ACTIVATED" : "") << '\n';
			}
		}

		result_transfer_steps.push_back(std::move(step));
	}

	auto t_fin = SteadyClock::now();
	GenerateJoinStageExecutionPlan();
	if (rpt_log) {
		finalize_ms = elapsed_ms(t_fin, SteadyClock::now());
		std::cerr << "[RPT-Timing] init_estimates=" << init_ms << "ms materialize=" << mat_ms << "ms build_bf=" << bf_ms
		          << "ms re_estimate=" << est_ms << "ms finalize=" << finalize_ms
		          << "ms total=" << (init_ms + mat_ms + bf_ms + est_ms + finalize_ms) << "ms" << '\n';
	}
}

void ExcitationGraphManager::GenerateJoinStageExecutionPlan() {
	for (auto &entry : cascade_filters_) {
		idx_t table_id = entry.first;
		auto state_it = state_.cardinality.find(table_id);
		if (state_it != state_.cardinality.end() && state_it->second == 0.0) {
			continue;
		}
		auto &cascade_filters = entry.second;
		if (cascade_filters.empty()) {
			continue;
		}

		auto *op = table_operator_manager.GetTableOperator(table_id);
		if (!op) {
			continue;
		}

		if (executor_.IsMaterialized(*op)) {
			// Materialized destinations finish the join stage with a row-id
			// bitmap over their fully filtered in-memory data. The executor
			// compacts any still-pending BFs first.
			auto bitmap = executor_.FinalizeRowIDBitmap(*op);
			if (bitmap) {
				row_id_filters_[table_id] = bitmap;
			}
			continue;
		}

		// Non-materialized destinations stay on the disk-scan path: forward
		// only single-column cascade filters so RewriteQueryPlan can push
		// them into LogicalGet. Composite-key filters cannot be expressed
		// per-column, but the destination always materialises so we never
		// hit this branch with a composite anyway (the materialized branch
		// above handles them via the in-memory scanner + UseBF).
		auto &direct = direct_filters_[table_id];
		direct.reserve(direct.size() + cascade_filters.size());
		for (auto &cf : cascade_filters) {
			if (cf.bindings.size() != 1) {
				continue;
			}
			direct.push_back({cf.bindings.front(), cf.filter, cf.lineage.lineages});
		}
	}
}

TableTransferResult ExcitationGraphManager::GetTableResult(idx_t table_id, LogicalOperator &op) {
	auto state_it = state_.cardinality.find(table_id);
	bool zero_card = state_it != state_.cardinality.end() && state_it->second == 0.0;
	if (zero_card) {
		return {TableTransferResult::Kind::Empty, nullptr, nullptr, nullptr};
	}

	auto *scanner = executor_.Find(op);
	if (scanner && scanner->IsMaterialized()) {
		if (scanner->Count() == 0) {
			return {TableTransferResult::Kind::Empty, nullptr, nullptr, nullptr};
		}
		if (!scanner->IsPruned()) {
			return {TableTransferResult::Kind::MemoryScan, scanner, nullptr, nullptr};
		}
		// Pruned scanner falls back to disk scan with injected filters.
	}

	shared_ptr<RPTFilter> rid;
	auto rid_it = row_id_filters_.find(table_id);
	if (rid_it != row_id_filters_.end()) {
		rid = rid_it->second;
	}
	const vector<DirectFilterInfo> *df = nullptr;
	auto df_it = direct_filters_.find(table_id);
	if (df_it != direct_filters_.end()) {
		df = &df_it->second;
	}
	return {TableTransferResult::Kind::DefaultScan, nullptr, std::move(rid), df};
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Materialization helpers
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::PrepareSourceTable(idx_t source_table_id) {
	auto *op = table_operator_manager.GetTableOperator(source_table_id);
	if (!op) {
		return;
	}
	if (executor_.IsMaterialized(*op)) {
		return;
	}
	// Register up front so AttachFilterToScanner below can see the scanner.
	executor_.Register(*op);

	// Collect required columns: all join keys (outgoing + incoming) +
	// incoming cascade filter keys.
	unordered_set<ColumnBinding, ColumnBindingHashFunc> required;

	auto nm_it = neighbor_matrix.find(source_table_id);
	if (nm_it != neighbor_matrix.end()) {
		for (auto &[neighbor_id, edges] : nm_it->second) {
			for (auto &edge : edges) {
				for (auto &b : edge.left_bindings) {
					required.insert(b);
				}
			}
		}
	}
	auto rev_it = state_.reverse_neighbor_matrix.find(source_table_id);
	if (rev_it != state_.reverse_neighbor_matrix.end()) {
		for (auto &[src_id, edges] : rev_it->second) {
			for (auto *edge : edges) {
				for (auto &b : edge->right_bindings) {
					required.insert(b);
				}
			}
		}
	}

	// Incoming cascade filters: add required columns and attach to the
	// (possibly-already-registered) scanner so InjectTableFilters can push
	// single-column filters into the underlying GET at Materialize() time.
	auto bf_it = cascade_filters_.find(source_table_id);
	if (bf_it != cascade_filters_.end()) {
		for (auto &acc : bf_it->second) {
			for (auto &b : acc.bindings) {
				required.insert(b);
			}
			executor_.AttachFilterToScanner(*op, acc.bindings, acc.filter);
		}
	}

	executor_.EnsureMaterialized(*op, required);
}

vector<shared_ptr<GraphEdge>> ExcitationGraphManager::ActivateTables(idx_t source_table_id,
                                                                     const vector<shared_ptr<GraphEdge>> &edges) {
	struct PreparedActivation {
		shared_ptr<GraphEdge> edge;
		vector<ColumnBinding> sorted_src;
		vector<ColumnBinding> sorted_dst;
		vector<LogicalType> sorted_types;
		LineageTracker::Snapshot snapshot;
		shared_ptr<RPTFilter> filter;
		idx_t build_index = DConstants::INVALID_INDEX;
		bool cache_hit = false;
	};

	vector<PreparedActivation> prepared;
	prepared.reserve(edges.size());
	auto source_lineage_id = state_.lineage_tracker.LineageOf(source_table_id);
	vector<TransferExecutor::FilterBuildSpec> build_specs;

	for (auto &edge : edges) {
		D_ASSERT(!edge->source_columns.empty());
		D_ASSERT(edge->source_columns.size() == edge->dest_columns.size());
		PreparedActivation activation;
		activation.edge = edge;
		vector<idx_t> order(edge->source_columns.size());
		std::iota(order.begin(), order.end(), idx_t {0});
		std::sort(order.begin(), order.end(), [&](idx_t a, idx_t b) {
			const auto &left = edge->source_columns[a];
			const auto &right = edge->source_columns[b];
			return left.table_index.index == right.table_index.index ? left.column_index < right.column_index
			                                                         : left.table_index.index < right.table_index.index;
		});
		for (auto index : order) {
			activation.sorted_src.push_back(edge->source_columns[index]);
			activation.sorted_dst.push_back(edge->dest_columns[index]);
			activation.sorted_types.push_back(edge->return_types[index]);
		}
		activation.snapshot = state_.lineage_tracker.MakeSnapshot(source_table_id, activation.sorted_src);
		if (config.enable_filter_cache) {
			activation.filter = filter_cache_.Lookup(source_lineage_id, activation.snapshot);
			activation.cache_hit = activation.filter != nullptr;
		}
		if (!activation.filter) {
			for (idx_t existing = 0; existing < build_specs.size(); existing++) {
				if (build_specs[existing].key_bindings == activation.sorted_src &&
				    build_specs[existing].key_types == activation.sorted_types) {
					activation.build_index = existing;
					break;
				}
			}
			if (activation.build_index == DConstants::INVALID_INDEX) {
				activation.build_index = build_specs.size();
				build_specs.push_back({activation.sorted_src, activation.sorted_types});
			}
		}
		prepared.push_back(std::move(activation));
	}

	vector<shared_ptr<RPTFilter>> built_filters;
	auto *source_op = table_operator_manager.GetTableOperator(source_table_id);
	if (source_op && !build_specs.empty()) {
		built_filters = executor_.BuildTransferFilters(*source_op, build_specs);
	}

	vector<shared_ptr<GraphEdge>> effective;
	for (auto &activation : prepared) {
		if (!activation.filter && activation.build_index < built_filters.size()) {
			activation.filter = built_filters[activation.build_index];
			if (activation.filter && config.enable_filter_cache) {
				filter_cache_.Insert(source_lineage_id, activation.snapshot, activation.filter);
			}
		}
		if (ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps) {
			std::stringstream cols;
			cols << "(";
			for (idx_t i = 0; i < activation.sorted_src.size(); i++) {
				if (i) {
					cols << ",";
				}
				cols << activation.sorted_src[i].table_index.index << "." << activation.sorted_src[i].column_index;
			}
			cols << ")";
			std::cerr << "    [RPTFilterCache " << (activation.cache_hit ? "HIT " : "MISS") << "] src=["
			          << source_table_id << "] cols=" << cols.str() << " → dest=[" << activation.edge->destination
			          << "]" << '\n';
		}
		if (!activation.filter) {
			continue;
		}

		auto &dest_filters = cascade_filters_[activation.edge->destination];
		bool subsumed = false;
		for (auto it = dest_filters.begin(); it != dest_filters.end();) {
			if (it->bindings != activation.sorted_dst) {
				++it;
				continue;
			}
			const bool new_contains_old = LineageTracker::IsSubset(it->lineage, activation.snapshot);
			const bool old_contains_new = LineageTracker::IsSubset(activation.snapshot, it->lineage);
			if (old_contains_new && !new_contains_old) {
				subsumed = true;
				break;
			}
			if (new_contains_old && !old_contains_new) {
				it = dest_filters.erase(it);
			} else {
				++it;
			}
		}
		if (subsumed) {
			continue;
		}

		auto filter_identity = activation.snapshot.StableHash();
		CascadeFilter cf;
		cf.bindings = activation.sorted_dst;
		cf.filter = activation.filter;
		cf.lineage = std::move(activation.snapshot);
		dest_filters.push_back(std::move(cf));
		auto *dest_op = table_operator_manager.GetTableOperator(activation.edge->destination);
		if (dest_op) {
			executor_.AttachFilterToScanner(*dest_op, activation.sorted_dst, activation.filter, filter_identity);
		}
		effective.push_back(activation.edge);
	}
	return effective;
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Debug output
//===--------------------------------------------------------------------===//

string ExcitationGraphManager::TablesToString() const {
	stringstream ss;

	ss << "all_tables=(";
	for (auto &op : table_operator_manager.GetTableInDFSOrder()) {
		auto id = TableOperatorManager::GetScalarTableIndex(op);
		ss << "(id=" << id << ", name=" << TableOperatorManager::GetTableName(op);
		auto it = state_.cardinality.find(id);
		if (it != state_.cardinality.end()) {
			ss << ", cardinality=" << static_cast<idx_t>(it->second);
		}
		// Show table-level lineage set
		const auto &tl = state_.lineage_tracker.Lineage(id);
		if (!tl.empty()) {
			ss << ", lineage={";
			bool first = true;
			for (auto ancestor_id : tl) {
				if (!first) {
					ss << ",";
				}
				ss << ancestor_id;
				first = false;
			}
			ss << "}";
		}
		ss << "), ";
	}
	ss << ")\n";

	return ss.str();
}

string ExcitationGraphManager::TransferPlanToString() const {
	stringstream ss;

	// Prune redundant steps only when we actually generate the debug timeline
	auto &mutable_steps = const_cast<vector<TransferStep> &>(result_transfer_steps);
	const_cast<ExcitationGraphManager *>(this)->PruneRedundantSteps(mutable_steps);

	ss << "ExcitationTimeline={\n";
	for (auto &step : result_transfer_steps) {
		if (!step.table) {
			continue;
		}
		auto src_id = TableOperatorManager::GetScalarTableIndex(*step.table);
		auto src_name = TableOperatorManager::GetTableName(*step.table);
		ss << "\tstep_" << step.id << ": \"" << src_name << "\" (id=" << src_id << ")";

		if (!step.depends_on.empty()) {
			ss << " [depends_on=";
			for (idx_t i = 0; i < step.depends_on.size(); i++) {
				if (i > 0) {
					ss << ",";
				}
				ss << step.depends_on[i];
			}
			ss << "]";
		}
		ss << "\n";

		for (auto &edge : step.create_bf) {
			auto dest_name =
			    TableOperatorManager::GetTableName(*table_operator_manager.GetTableOperator(edge->destination));
			ss << "\t\t-> \"" << dest_name << "\" (id=" << edge->destination << ")";
			for (idx_t i = 0; i < edge->source_columns.size(); i++) {
				ss << " [col " << edge->source_columns[i].column_index << " -> col "
				   << edge->dest_columns[i].column_index << "]";
			}
			ss << "\n";
		}
	}
	ss << "}\n";

	return ss.str();
}

} // namespace duckdb
