#include "predicate_transfer/transfer_plan/dag.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

// ─── DirectedGraph ──────────────────────────────────────────────────────────

const vector<shared_ptr<GraphEdge>> DirectedGraph::empty_;

const vector<shared_ptr<GraphEdge>> &DirectedGraph::Out(idx_t id) const {
	auto it = out_index_.find(id);
	return it != out_index_.end() ? it->second : empty_;
}

const vector<shared_ptr<GraphEdge>> &DirectedGraph::In(idx_t id) const {
	auto it = in_index_.find(id);
	return it != in_index_.end() ? it->second : empty_;
}

GraphEdge *DirectedGraph::AddEdge(idx_t from, idx_t to) {
	// Check if edge already exists
	for (auto &e : out_index_[from]) {
		if (e->destination == to) {
			return e.get();
		}
	}
	auto edge = make_shared_ptr<GraphEdge>(from, to);
	edges_.push_back(edge);
	out_index_[from].push_back(edge);
	in_index_[to].push_back(edge);
	return edge.get();
}

GraphEdge *DirectedGraph::AddEdge(idx_t from, idx_t to, const ColumnBinding &source_col, const ColumnBinding &dest_col,
                                  const LogicalType &type) {
	auto *edge = AddEdge(from, to);
	edge->source_columns.push_back(source_col);
	edge->dest_columns.push_back(dest_col);
	edge->return_types.push_back(type);
	return edge;
}

void DirectedGraph::RemoveInEdge(idx_t node_id, idx_t source) {
	auto it = in_index_.find(node_id);
	if (it == in_index_.end())
		return;
	auto &edges = it->second;
	for (auto eit = edges.begin(); eit != edges.end(); eit++) {
		if ((*eit)->source == source) {
			edges.erase(eit);
			return;
		}
	}
}

void DirectedGraph::ClearOut(idx_t node_id) {
	out_index_[node_id].clear();
}

void DirectedGraph::Clear() {
	edges_.clear();
	out_index_.clear();
	in_index_.clear();
}
bool FilterPlan::operator==(const FilterPlan &other) const {
	return build == other.build && apply == other.apply && return_types == other.return_types;
}

void FilterPlan::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<ColumnBinding>>(200, "build", build);
	serializer.WritePropertyWithDefault<vector<ColumnBinding>>(201, "apply", apply);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(202, "return_types", return_types);
	serializer.WritePropertyWithDefault<idx_t>(205, "plan_id", plan_id);
}

unique_ptr<FilterPlan> FilterPlan::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<FilterPlan>(new FilterPlan());
	deserializer.ReadPropertyWithDefault<vector<ColumnBinding>>(200, "build", result->build);
	deserializer.ReadPropertyWithDefault<vector<ColumnBinding>>(201, "apply", result->apply);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(202, "return_types", result->return_types);
	deserializer.ReadPropertyWithDefault<idx_t>(205, "plan_id", result->plan_id);
	return result;
}

string FilterPlan::ToString() const {
	stringstream ss;
	ss << "plan_id=" << plan_id << ", ";

	auto vec_to_str = [&](vector<ColumnBinding> v) {
		for (auto &x : v) {
			ss << x.ToString() << ", ";
		}
	};
	auto vectype_to_str = [&](vector<LogicalType> v) {
		for (auto &x : v) {
			ss << x.ToString() << ", ";
		}
	};

	auto vecint_to_str = [&](vector<idx_t> v) {
		for (auto &x : v) {
			ss << x << ", ";
		}
	};
	ss << "build=(";
	vec_to_str(build);
	ss << "), ";

	ss << "apply=(";
	vec_to_str(apply);
	ss << "), ";

	ss << "return_types=(";
	vectype_to_str(return_types);
	ss << ")";

	return ss.str();
}

} // namespace duckdb
