#include "predicate_transfer/transfer_plan/excitation_graph/excitation_graph_manager.hpp"
#include "predicate_transfer/table_operator_manager.hpp"

#include <algorithm>
#include <iostream>
#include <numeric>
#include <sstream>

namespace duckdb {

void ExcitationGraphManager::GenerateJoinStageExecutionPlan() {
	for (auto &entry : cascade_filters_) {
		idx_t table_id = entry.first;
		if (state_.HasCardinality(table_id) && state_.Cardinality(table_id) == 0.0) {
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
		// above handles them via the in-memory scanner).
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
	// EmptyResult must come from ground truth, not an estimate: a flooding
	// cardinality of 0 can be a false zero (the estimation BF is built from the
	// source table's *sample*, so it has false negatives and can wrongly empty a
	// non-empty join). Only a materialized scanner with Count()==0 below, or a
	// pushed filter that empties the scan at execution, may drop all rows.
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
	column_binding_set_t required;

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
	for (auto &[src_id, edges] : state_.Incoming(source_table_id)) {
		for (auto *edge : edges) {
			for (auto &b : edge->right_bindings) {
				required.insert(b);
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
	auto source_lineage_id = state_.Lineage().LineageOf(source_table_id);
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
		activation.snapshot = state_.Lineage().MakeSnapshot(source_table_id, activation.sorted_src);
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
		activation.edge->filter_type = activation.filter->ToString();

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
		if (ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps) {
			std::cerr << "  [RPT-Transfer] src=[" << source_table_id << "] dest=[" << activation.edge->destination
			          << "] source_cols=";
			for (idx_t i = 0; i < activation.sorted_src.size(); i++) {
				if (i > 0) {
					std::cerr << ",";
				}
				std::cerr << activation.sorted_src[i].column_index;
			}
			std::cerr << " dest_cols=";
			for (idx_t i = 0; i < activation.sorted_dst.size(); i++) {
				if (i > 0) {
					std::cerr << ",";
				}
				std::cerr << activation.sorted_dst[i].column_index;
			}
			std::cerr << " filter=" << activation.edge->filter_type
			          << " cache=" << (activation.cache_hit ? "hit" : "miss") << '\n';
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

} // namespace duckdb
