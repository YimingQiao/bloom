//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/transfer_plan/dag.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"

namespace duckdb {

struct FilterPlan {
	vector<ColumnBinding> build;
	vector<ColumnBinding> apply;
	vector<LogicalType> return_types;
	idx_t plan_id = -1;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<FilterPlan> Deserialize(Deserializer &deserializer);

	bool operator==(const FilterPlan &other) const;
	string ToString() const;
};

struct FilterPhysicalPlan {
	shared_ptr<FilterPlan> logical_plan;
	vector<idx_t> bound_cols;
};

class GraphEdge {
public:
	GraphEdge(idx_t source, idx_t destination) : source(source), destination(destination) {
	}

	idx_t source;
	idx_t destination;

	vector<ColumnBinding> source_columns;
	vector<ColumnBinding> dest_columns;
	vector<LogicalType> return_types;
	vector<shared_ptr<FilterPlan>> filter_plan;
};

//! A directed graph with explicit node set and edge list.
//! Maintains adjacency indices for efficient Out()/In() queries.
class DirectedGraph {
public:
	//! Adjacency queries (by index)
	const vector<shared_ptr<GraphEdge>> &Out(idx_t id) const;
	const vector<shared_ptr<GraphEdge>> &In(idx_t id) const;

	//! Add an edge. If an edge (from → to) already exists, returns the existing one.
	GraphEdge *AddEdge(idx_t from, idx_t to);
	GraphEdge *AddEdge(idx_t from, idx_t to, const ColumnBinding &source_col, const ColumnBinding &dest_col,
	                   const LogicalType &type);

	//! Mutation
	void RemoveInEdge(idx_t node_id, idx_t source);
	void ClearOut(idx_t node_id);
	void Clear();

private:
	//! All edges
	vector<shared_ptr<GraphEdge>> edges_;
	//! Adjacency indices (derived from edges_)
	unordered_map<idx_t, vector<shared_ptr<GraphEdge>>> out_index_;
	unordered_map<idx_t, vector<shared_ptr<GraphEdge>>> in_index_;
	static const vector<shared_ptr<GraphEdge>> empty_;
};

} // namespace duckdb
