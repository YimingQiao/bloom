#include "predicate_transfer/transfer_plan/excitation_graph/excitation_graph_manager.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator/sampling_estimator.hpp"
#include "predicate_transfer/table_operator_manager.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <numeric>
#include <sstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Initialization
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::InitEstimator() {
	estimator_ = make_uniq<SamplingCardinalityEstimator>(context, config.sampling);
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
	if (config.excitation_mode == RPTExcitationMode::JOIN_KEY_NDV) {
		equality_domains_.Reset(table_groups);
	} else {
		equality_domains_.Reset({});
	}

	const auto &all_tables = table_operator_manager.GetAllTableOperators();

	// Register CDC-alias groups before any SeedColumn so CTE mirrors share
	// lineage from the start (prevents self-to-self BF transfer).
	for (auto &entry : all_tables) {
		auto &aliases = table_operator_manager.GetCDCAliases(entry.first);
		if (aliases.size() >= 2) {
			state_.Lineage().RegisterAliasGroup(aliases);
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
					state_.Lineage().SeedColumn(b);
				}
			}
		}
	};

	auto init_lineage = [&](idx_t table_id) {
		state_.Lineage().InitTable(table_id);
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
			double base_rows = chunk_get.collection ? static_cast<double>(chunk_get.collection->Count()) : 0;
			// If a FILTER sits above CHUNK_GET we need the estimator for the
			// post-filter cardinality; for bare CHUNK_GET card == baseline.
			double row_count = (leaf == &op) ? base_rows : static_cast<double>(estimator_->Estimate(op));
			init_lineage(table_id);
			state_.AddTable(table_id, row_count, base_rows, true);
			executor_.Register(op);
			continue;
		}

		double true_card = static_cast<double>(estimator_->Estimate(op));
		double base_rows = static_cast<double>(ComputeBaseTableRows(op));
		// Only excite if local filters reduce cardinality enough.
		bool will_be_active = true_card < base_rows * config.excitation_threshold;
		init_lineage(table_id);
		state_.AddTable(table_id, true_card, base_rows, will_be_active);
	}

	// Reverse adjacency for O(neighbors) incoming-edge lookup in PrepareSourceTable.
	for (auto &[src_id, neighbors] : neighbor_matrix) {
		for (auto &[dst_id, edges] : neighbors) {
			for (auto &edge : edges) {
				state_.AddIncomingEdge(dst_id, src_id, edge);
			}
		}
	}
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
		if (!state_.HasCardinality(neighbor_id)) {
			continue;
		}
		auto neighbor_cardinality = state_.Cardinality(neighbor_id);
		if (neighbor_cardinality == 0.0) {
			continue;
		}
		// Skip sending filters to tables that are already small enough —
		// filtering them further has negligible benefit but costs materialization.
		if (neighbor_cardinality <= static_cast<double>(config.small_table_threshold)) {
			continue;
		}

		for (auto &edge : edges) {
			if (edge.protect_right) {
				continue;
			}
			if (!state_.Lineage().EdgeCarriesNewInfo(edge)) {
				continue;
			}
			result.push_back(MakeBloomFilterEdge(table_id, neighbor_id, edge));
		}
	}
	std::sort(result.begin(), result.end(), [](const shared_ptr<GraphEdge> &left, const shared_ptr<GraphEdge> &right) {
		auto edge_key = [](const GraphEdge &edge) {
			std::stringstream key;
			key << edge.destination << ":";
			vector<idx_t> order(edge.source_columns.size());
			std::iota(order.begin(), order.end(), idx_t {0});
			std::sort(order.begin(), order.end(), [&](idx_t a, idx_t b) {
				const auto &left_binding = edge.source_columns[a];
				const auto &right_binding = edge.source_columns[b];
				if (left_binding.table_index.index != right_binding.table_index.index) {
					return left_binding.table_index.index < right_binding.table_index.index;
				}
				if (left_binding.column_index != right_binding.column_index) {
					return left_binding.column_index < right_binding.column_index;
				}
				return edge.dest_columns[a].column_index < edge.dest_columns[b].column_index;
			});
			for (auto index : order) {
				key << edge.source_columns[index].column_index << ">" << edge.dest_columns[index].column_index << ",";
			}
			return key.str();
		};
		return edge_key(*left) < edge_key(*right);
	});
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
		if (!state_.HasCardinality(neighbor_id)) {
			continue;
		}
		auto prior_card = state_.Cardinality(neighbor_id);

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
		double current_card = dest_scanner ? static_cast<double>(estimator_->Estimate(*dest_scanner, direct_filters))
		                                   : static_cast<double>(estimator_->Estimate(*dest_op, direct_filters));

		// Update both table-level and per-column lineage (the latter is read
		// by the next round's CollectOutgoingEdges and ActivateTable for
		// subsumption, in column-granular mode).
		state_.Lineage().Propagate(*edge);

		state_.AddDependency(neighbor_id, edge->source);

		auto last_card = state_.Baseline(neighbor_id);
		bool reactivated = state_.UpdateCardinality(neighbor_id, current_card, config.excitation_threshold);
		if (ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps) {
			auto dest_name = TableOperatorManager::GetTableName(*dest_op);
			std::cerr << "  [RPT-Estimate] dest=[" << neighbor_id << "] name=" << dest_name
			          << " prior=" << static_cast<idx_t>(prior_card) << " baseline=" << static_cast<idx_t>(last_card)
			          << " estimate=" << static_cast<idx_t>(current_card)
			          << " decision_boundary=" << static_cast<idx_t>(last_card * config.excitation_threshold)
			          << " filters=" << direct_filters.size() << " source=" << (dest_scanner ? "materialized" : "base")
			          << " reactivate=" << (reactivated ? 1 : 0) << '\n';
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

		if (!state_.HasCardinality(neighbor_id) || state_.Cardinality(neighbor_id) == 0.0) {
			continue;
		}

		state_.MarkZero(neighbor_id);

		// Destination inherits the source lineage even though the filter
		// itself is just an empty-result marker.
		state_.Lineage().Propagate(*edge);

		state_.AddDependency(neighbor_id, table_id);
		cascade_filters_[neighbor_id].clear();
		direct_filters_[neighbor_id].clear();
		row_id_filters_.erase(neighbor_id);
	}
}

void ExcitationGraphManager::ExecuteTransfer() {
	result_transfer_steps.clear();
	execution_history_.clear();
	execution_action_count_ = 0;
	execution_round_count_ = 0;
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
		std::cerr << "[RPT-SamplingConfig] mode="
		          << (config.sampling.mode == RPTSamplingMode::PREPARED ? "prepared" : "instant")
		          << " target_rows=" << config.sampling.target_rows;
		if (config.sampling.mode == RPTSamplingMode::INSTANT) {
			std::cerr << " access="
			          << (config.sampling.instant_access == RPTInstantAccessMode::SCATTERED ? "scattered" : "block")
			          << " consistency=" << (config.sampling.instant_snapshot ? "snapshot" : "storage_direct")
			          << " access_points=" << config.sampling.instant_access_points
			          << " rows_per_access=" << config.sampling.instant_rows_per_access
			          << " block_windows=" << config.sampling.instant_block_windows
			          << " parquet_row_groups=" << config.sampling.instant_parquet_row_groups
			          << " seed=" << config.sampling.seed;
		}
		std::cerr << " excitation="
		          << (config.excitation_mode == RPTExcitationMode::TABLE_SIZE ? "table_size" : "join_key_ndv");
		if (config.excitation_mode == RPTExcitationMode::JOIN_KEY_NDV) {
			std::cerr << " exact_domains=" << equality_domains_.TrackedDomainCount();
		}
		std::cerr << '\n';
		std::cerr << "[RPT-Excitation] === Initial table cardinalities ===" << '\n';
		vector<idx_t> table_ids;
		table_ids.reserve(state_.Cardinalities().size());
		for (auto &entry : state_.Cardinalities()) {
			table_ids.push_back(entry.first);
		}
		std::sort(table_ids.begin(), table_ids.end());
		for (auto tid : table_ids) {
			auto cardinality = state_.Cardinality(tid);
			auto *op = table_operator_manager.GetTableOperator(tid);
			std::string name = op ? TableOperatorManager::GetTableName(*op) : "?";
			bool active = state_.IsActive(tid);
			std::cerr << "  [" << tid << "] " << name << "  card=" << static_cast<idx_t>(cardinality)
			          << "  baseline=" << static_cast<idx_t>(state_.Baseline(tid)) << (active ? "  ACTIVE" : "")
			          << '\n';
		}
		std::cerr << "[RPT-Excitation] active_nodes count: " << state_.ActiveTableCount() << '\n';
	}

	idx_t excitation_round = 0;
	std::stringstream execution_history;

	// Main adaptive-excitation loop:
	// 1. pick the smallest active source table A
	// 2. materialize A once (with any filters it has already received)
	// 3. determine which outgoing edges from A still carry new information
	// 4. build/apply A's outgoing filters to the destination tables B
	// 5. re-estimate each affected B after those filters are attached
	while (state_.HasActiveTables()) {
		idx_t table_id = state_.PopSmallestActiveTable();

		excitation_round++;
		if (rpt_log) {
			execution_history << "R" << excitation_round << ":S" << table_id;
		}
		if (rpt_log) {
			auto *src_op = table_operator_manager.GetTableOperator(table_id);
			std::string src_name = src_op ? TableOperatorManager::GetTableName(*src_op) : "?";
			std::cerr << "[RPT-Excitation] --- Round " << excitation_round << ": source=[" << table_id << "] "
			          << src_name << "  card=" << static_cast<idx_t>(state_.Cardinality(table_id))
			          << "  remaining_active=" << state_.ActiveTableCount() << '\n';
		}

		auto informative_candidates = CollectOutgoingEdges(table_id);
		if (informative_candidates.empty()) {
			if (rpt_log) {
				execution_history << ":no_edges;";
			}
			continue;
		}

		if (state_.Cardinality(table_id) == 0.0) {
			if (rpt_log) {
				execution_history << ":zero{";
				for (auto &edge : informative_candidates) {
					AppendEdgeSignature(execution_history, table_id, *edge);
					execution_action_count_++;
				}
				execution_history << "};";
			}
			TransferStep step;
			step.id = state_.NextStepId();
			step.table = table_operator_manager.GetTableOperator(table_id);
			step.depends_on = state_.Dependencies(table_id);
			step.create_bf = informative_candidates;

			PropagateZeroCardinality(table_id, informative_candidates);
			result_transfer_steps.push_back(std::move(step));
			continue;
		}

		auto t_mat = SteadyClock::now();
		PrepareSourceTable(table_id);
		double round_mat = rpt_log ? elapsed_ms(t_mat, SteadyClock::now()) : 0;
		auto estimated_source_card = state_.Cardinality(table_id);
		mat_ms += round_mat;
		state_.CommitBaseline(table_id);
		auto *source_op = table_operator_manager.GetTableOperator(table_id);
		if (rpt_log && source_op) {
			auto *source_scanner = executor_.Find(*source_op);
			std::cerr << "  [RPT-Materialized] source=[" << table_id
			          << "] name=" << TableOperatorManager::GetTableName(*source_op)
			          << " estimated=" << static_cast<idx_t>(estimated_source_card)
			          << " actual=" << (source_scanner ? source_scanner->Count() : 0) << '\n';
		}

		TransferStep step;
		step.id = state_.NextStepId();
		step.table = source_op;
		step.depends_on = state_.Dependencies(table_id);

		auto t_bf = SteadyClock::now();
		auto effective_candidates = ActivateTables(table_id, informative_candidates);
		double round_bf = rpt_log ? elapsed_ms(t_bf, SteadyClock::now()) : 0;
		bf_ms += round_bf;

		if (effective_candidates.empty()) {
			if (rpt_log) {
				execution_history << ":materialized_no_effective;";
			}
			if (rpt_log) {
				std::cerr << "    [timing] materialize=" << round_mat << "ms build_bf=" << round_bf << "ms" << '\n';
			}
			continue;
		}
		step.create_bf = effective_candidates;
		if (rpt_log) {
			execution_history << ":actions{";
			for (auto &edge : effective_candidates) {
				AppendEdgeSignature(execution_history, table_id, *edge);
				execution_action_count_++;
			}
			execution_history << "};";
		}

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
				bool reactivated = state_.IsActive(edge->destination);
				std::cerr << "    -> [" << edge->destination << "] " << dest_name
				          << "  card=" << static_cast<idx_t>(state_.Cardinality(edge->destination))
				          << (reactivated ? "  RE-ACTIVATED" : "") << '\n';
			}
		}

		result_transfer_steps.push_back(std::move(step));
	}
	if (rpt_log) {
		execution_history_ = execution_history.str();
		execution_round_count_ = excitation_round;
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

} // namespace duckdb
