#include "predicate_transfer/transfer_plan/excitation_graph/excitation_graph_manager.hpp"
#include "predicate_transfer/table_operator_manager.hpp"

#include <algorithm>
#include <iomanip>
#include <numeric>
#include <sstream>

namespace duckdb {

void ExcitationGraphManager::AppendEdgeSignature(std::stringstream &canonical, idx_t source_id, const GraphEdge &edge) {
	canonical << source_id << ">" << edge.destination << "(";
	vector<idx_t> order(edge.source_columns.size());
	std::iota(order.begin(), order.end(), idx_t {0});
	std::sort(order.begin(), order.end(), [&](idx_t a, idx_t b) {
		if (edge.source_columns[a].column_index != edge.source_columns[b].column_index) {
			return edge.source_columns[a].column_index < edge.source_columns[b].column_index;
		}
		return edge.dest_columns[a].column_index < edge.dest_columns[b].column_index;
	});
	for (auto index : order) {
		canonical << edge.source_columns[index].column_index << ":" << edge.dest_columns[index].column_index << ",";
	}
	canonical << ")#" << edge.filter_type << ";";
}

static uint64_t StablePlanSignature(const string &canonical) {
	uint64_t signature = 1469598103934665603ULL;
	for (auto byte : canonical) {
		signature ^= static_cast<uint8_t>(byte);
		signature *= 1099511628211ULL;
	}
	return signature;
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

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Debug output
//===--------------------------------------------------------------------===//

string ExcitationGraphManager::TablesToString() {
	stringstream ss;

	ss << "all_tables=(";
	for (auto &op : table_operator_manager.GetTableInDFSOrder()) {
		auto id = TableOperatorManager::GetScalarTableIndex(op);
		ss << "(id=" << id << ", name=" << TableOperatorManager::GetTableName(op);
		if (state_.HasCardinality(id)) {
			ss << ", cardinality=" << static_cast<idx_t>(state_.Cardinality(id));
		}
		// Show table-level lineage set
		const auto &tl = state_.Lineage().Lineage(id);
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

string ExcitationGraphManager::TransferPlanToString() {
	stringstream ss;
	auto execution_signature = StablePlanSignature(execution_history_);
	ss << "RPTExecutionSignature hash=" << std::hex << std::setw(16) << std::setfill('0') << execution_signature
	   << std::dec << " rounds=" << execution_round_count_ << " actions=" << execution_action_count_
	   << " canonical=" << execution_history_ << "\n";

	// Prune redundant steps only when we actually generate the debug timeline
	PruneRedundantSteps(result_transfer_steps);
	std::stringstream canonical;
	idx_t action_count = 0;
	for (auto &step : result_transfer_steps) {
		if (!step.table) {
			continue;
		}
		auto src_id = TableOperatorManager::GetScalarTableIndex(*step.table);
		for (auto &edge : step.create_bf) {
			AppendEdgeSignature(canonical, src_id, *edge);
			action_count++;
		}
	}
	auto signature = StablePlanSignature(canonical.str());
	ss << "RPTPlanSignature hash=" << std::hex << std::setw(16) << std::setfill('0') << signature << std::dec
	   << " actions=" << action_count << " canonical=" << canonical.str() << "\n";

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
