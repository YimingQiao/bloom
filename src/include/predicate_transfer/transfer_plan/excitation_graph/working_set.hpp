#pragma once

#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//! Owns the mutable graph state for one excitation run. Cardinality changes,
//! active-table scheduling, dependencies, incoming-edge indexing, and lineage
//! all reset and evolve together.
class ExcitationWorkingSet {
public:
	using IncomingEdges = unordered_map<idx_t, vector<const EdgeInfo *>>;

	void Reset();
	void AddTable(idx_t table_id, double cardinality, double baseline, bool active);

	bool HasCardinality(idx_t table_id) const;
	double Cardinality(idx_t table_id) const;
	double Baseline(idx_t table_id) const;
	const unordered_map<idx_t, double> &Cardinalities() const {
		return cardinalities_;
	}

	//! Replaces a table estimate and reactivates it when it crosses the
	//! excitation threshold relative to its last committed baseline.
	bool UpdateCardinality(idx_t table_id, double cardinality, double excitation_threshold);
	void CommitBaseline(idx_t table_id);
	void MarkZero(idx_t table_id);

	bool HasActiveTables() const {
		return !active_tables_.empty();
	}
	bool IsActive(idx_t table_id) const {
		return active_tables_.count(table_id) != 0;
	}
	idx_t ActiveTableCount() const {
		return active_tables_.size();
	}
	idx_t PopSmallestActiveTable();

	void AddDependency(idx_t table_id, idx_t source_table_id);
	const vector<idx_t> &Dependencies(idx_t table_id) const;

	void AddIncomingEdge(idx_t destination_id, idx_t source_id, const EdgeInfo &edge);
	const IncomingEdges &Incoming(idx_t table_id) const;

	idx_t NextStepId() {
		return next_step_id_++;
	}

	LineageTracker &Lineage() {
		return lineage_;
	}
	const LineageTracker &Lineage() const {
		return lineage_;
	}

private:
	unordered_map<idx_t, double> cardinalities_;
	unordered_map<idx_t, double> baselines_;
	LineageTracker lineage_;
	unordered_set<idx_t> active_tables_;
	unordered_map<idx_t, vector<idx_t>> dependencies_;
	unordered_map<idx_t, IncomingEdges> incoming_edges_;
	idx_t next_step_id_ = 0;
};

} // namespace duckdb
