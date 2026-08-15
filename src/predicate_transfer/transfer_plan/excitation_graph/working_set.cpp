#include "predicate_transfer/transfer_plan/excitation_graph/working_set.hpp"

#include <limits>

namespace duckdb {

void ExcitationWorkingSet::Reset() {
	cardinalities_.clear();
	baselines_.clear();
	lineage_.Clear();
	active_tables_.clear();
	dependencies_.clear();
	incoming_edges_.clear();
	next_step_id_ = 0;
}

void ExcitationWorkingSet::AddTable(idx_t table_id, double cardinality, double baseline, bool active) {
	cardinalities_[table_id] = cardinality;
	baselines_[table_id] = baseline;
	if (active) {
		active_tables_.insert(table_id);
	} else {
		active_tables_.erase(table_id);
	}
}

bool ExcitationWorkingSet::HasCardinality(idx_t table_id) const {
	return cardinalities_.find(table_id) != cardinalities_.end();
}

double ExcitationWorkingSet::Cardinality(idx_t table_id) const {
	auto entry = cardinalities_.find(table_id);
	D_ASSERT(entry != cardinalities_.end());
	return entry->second;
}

double ExcitationWorkingSet::Baseline(idx_t table_id) const {
	auto entry = baselines_.find(table_id);
	D_ASSERT(entry != baselines_.end());
	return entry->second;
}

bool ExcitationWorkingSet::UpdateCardinality(idx_t table_id, double cardinality, double excitation_threshold) {
	D_ASSERT(HasCardinality(table_id));
	cardinalities_[table_id] = cardinality;
	bool reactivated = cardinality < Baseline(table_id) * excitation_threshold;
	if (reactivated) {
		active_tables_.insert(table_id);
	}
	return reactivated;
}

void ExcitationWorkingSet::CommitBaseline(idx_t table_id) {
	baselines_[table_id] = Cardinality(table_id);
}

void ExcitationWorkingSet::MarkZero(idx_t table_id) {
	D_ASSERT(HasCardinality(table_id));
	cardinalities_[table_id] = 0.0;
	baselines_[table_id] = 0.0;
	active_tables_.insert(table_id);
}

idx_t ExcitationWorkingSet::PopSmallestActiveTable() {
	D_ASSERT(!active_tables_.empty());
	idx_t best_id = std::numeric_limits<idx_t>::max();
	double best_cardinality = std::numeric_limits<double>::infinity();
	for (auto table_id : active_tables_) {
		auto cardinality = Cardinality(table_id);
		if (cardinality < best_cardinality || (cardinality == best_cardinality && table_id < best_id)) {
			best_cardinality = cardinality;
			best_id = table_id;
		}
	}
	D_ASSERT(best_id != std::numeric_limits<idx_t>::max());
	active_tables_.erase(best_id);
	return best_id;
}

void ExcitationWorkingSet::AddDependency(idx_t table_id, idx_t source_table_id) {
	dependencies_[table_id].push_back(source_table_id);
}

const vector<idx_t> &ExcitationWorkingSet::Dependencies(idx_t table_id) const {
	static const vector<idx_t> empty;
	auto entry = dependencies_.find(table_id);
	return entry == dependencies_.end() ? empty : entry->second;
}

void ExcitationWorkingSet::AddIncomingEdge(idx_t destination_id, idx_t source_id, const EdgeInfo &edge) {
	incoming_edges_[destination_id][source_id].push_back(&edge);
}

const ExcitationWorkingSet::IncomingEdges &ExcitationWorkingSet::Incoming(idx_t table_id) const {
	static const IncomingEdges empty;
	auto entry = incoming_edges_.find(table_id);
	return entry == incoming_edges_.end() ? empty : entry->second;
}

} // namespace duckdb
