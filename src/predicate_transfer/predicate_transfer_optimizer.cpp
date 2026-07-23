#include "predicate_transfer/predicate_transfer_optimizer.hpp"
#include "predicate_transfer/materialized_cte_lifter.hpp"
#include "predicate_transfer/table_operator_manager.hpp"
#include "predicate_transfer/transfer_plan/excitation_graph_manager.hpp"
#include "predicate_transfer/filter/table_filter.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/empty_result_pullup.hpp"
#include "duckdb/main/client_config.hpp"

namespace duckdb {

static bool ContainsMaterializedCTE(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsMaterializedCTE(*child)) {
			return true;
		}
	}
	return false;
}

//! Set operator at or below `op`, not descending into MATERIALIZED_CTE/RECURSIVE_CTE
//! definitions (those are their own PT scope).
static bool HasSetOperator(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return true;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		// CTE definition lives in children[0]; skip it — it's a separate scope.
		return op.children.size() > 1 && HasSetOperator(*op.children[1]);
	default:
		break;
	}
	for (auto &child : op.children) {
		if (HasSetOperator(*child)) {
			return true;
		}
	}
	return false;
}

//! Inequality join at or below `op`, with the same CTE-boundary behaviour.
static bool HasInequalityJoin(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return op.children.size() > 1 && HasInequalityJoin(*op.children[1]);
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			if (cond.IsComparison()) {
				auto cmp = cond.GetComparisonType();
				if (cmp == ExpressionType::COMPARE_LESSTHAN || cmp == ExpressionType::COMPARE_GREATERTHAN ||
				    cmp == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
				    cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
					return true;
				}
			}
		}
	}
	for (auto &child : op.children) {
		if (HasInequalityJoin(*child)) {
			return true;
		}
	}
	return false;
}

static bool IsJoinNode(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return true;
	default:
		return false;
	}
}

//! Count join nodes in the subtree, stopping at CTE definition boundaries
//! (children[0] of MATERIALIZED_CTE / RECURSIVE_CTE belongs to a separate scope).
static idx_t CountJoins(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return op.children.size() > 1 ? CountJoins(*op.children[1]) : 0;
	}
	idx_t total = IsJoinNode(op) ? 1 : 0;
	for (auto &child : op.children) {
		total += CountJoins(*child);
	}
	return total;
}

//! Find the root-most join node in `op`, descending through non-join
//! operators. Returns nullptr if there is no join above the next scope break.
static LogicalOperator *FindTopJoin(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return op.children.size() > 1 ? FindTopJoin(*op.children[1]) : nullptr;
	}
	if (IsJoinNode(op)) {
		return &op;
	}
	for (auto &child : op.children) {
		if (auto *found = FindTopJoin(*child)) {
			return found;
		}
	}
	return nullptr;
}

//! Structure-only left-deep check: every join's right subtree contains no joins.
static bool AllJoinRightSidesAreLeaf(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return op.children.size() > 1 ? AllJoinRightSidesAreLeaf(*op.children[1]) : true;
	}
	if (IsJoinNode(op) && op.children.size() == 2 && CountJoins(*op.children[1]) > 0) {
		return false;
	}
	for (auto &child : op.children) {
		if (!AllJoinRightSidesAreLeaf(*child)) {
			return false;
		}
	}
	return true;
}

//! Scope is "skip-worthy left-deep" iff:
//!   (1) contains ≥1 join, (2) every join's right subtree is join-free,
//!   (3) the leftmost (deepest-left) table is ≥ 10× the largest right-side table.
static bool IsLeftDeepJoinTree(LogicalOperator &op, ClientContext &context) {
	if (CountJoins(op) == 0) {
		return false;
	}
	if (!AllJoinRightSidesAreLeaf(op)) {
		return false;
	}
	auto *top_join = FindTopJoin(op);
	if (!top_join) {
		return false;
	}
	// Walk down the left spine, collecting every right-side subtree's cardinality.
	idx_t max_right_card = 0;
	auto *cur = top_join;
	while (IsJoinNode(*cur) && cur->children.size() == 2) {
		auto right_card = cur->children[1]->EstimateCardinality(context);
		if (right_card > max_right_card) {
			max_right_card = right_card;
		}
		cur = cur->children[0].get();
	}
	// `cur` is now the leftmost leaf of the join spine.
	idx_t left_card = cur->EstimateCardinality(context);
	if (max_right_card == 0) {
		return false;
	}
	return left_card >= max_right_card * 10;
}

//! True for operators whose children[0] starts a new pipeline (the rows do
//! not flow straight through into the parent). Joins/cross-products are NOT
//! breakers for the LEFT path — the left child is the probe side.
static bool BreaksLeftPipeline(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return true;
	default:
		return false;
	}
}

//! Table whose scan sits in the same pipeline as `op` via the left chain,
//! or INVALID_INDEX if no base-table scan is reached before a pipeline break.
static idx_t LeftPipelineTable(const LogicalOperator &op) {
	auto *cur = &op;
	while (!cur->children.empty()) {
		cur = cur->children[0].get();
		if (BreaksLeftPipeline(*cur)) {
			return DConstants::INVALID_INDEX;
		}
	}
	if (cur->type != LogicalOperatorType::LOGICAL_GET && cur->type != LogicalOperatorType::LOGICAL_CHUNK_GET) {
		return DConstants::INVALID_INDEX;
	}
	return TableOperatorManager::GetScalarTableIndex(*cur);
}

//! Tables whose scan shares a pipeline with an early-terminating operator
//! (TOP_N / LIMIT / MARK join) — RPT skips their excitation.
static unordered_set<idx_t> ComputeProtectedTables(const LogicalOperator &plan) {
	unordered_set<idx_t> protect;
	auto try_mark = [&](const LogicalOperator &op) {
		auto tid = LeftPipelineTable(op);
		if (tid != DConstants::INVALID_INDEX) {
			protect.insert(tid);
		}
	};
	std::function<void(const LogicalOperator &)> walk = [&](const LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_TOP_N || op.type == LogicalOperatorType::LOGICAL_LIMIT) {
			try_mark(op);
		} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		           op.Cast<LogicalComparisonJoin>().join_type == JoinType::MARK) {
			try_mark(op);
		}
		for (auto &child : op.children) {
			walk(*child);
		}
	};
	walk(plan);
	return protect;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (config.enable_materialized_cte_lifting) {
		// Lift LOGICAL_MATERIALIZED_CTE nodes before building the PT graph.
		// The implementation remains available, but is disabled by default
		// until optimizer-time parallel execution has a valid query Executor.
		MaterializedCTELifter lifter(optimizer, optimizer.context, config);
		plan = lifter.Lift(std::move(plan));
	} else if (ContainsMaterializedCTE(*plan)) {
		// Without lifting, the CTE definition and its consumer are separate
		// binding scopes. Do not let graph construction cross that boundary;
		// leave the complete plan to DuckDB's normal runtime CTE executor.
		if (config.log_transfer_steps) {
			fprintf(stderr, "[RPT-Excitation] scope skipped: materialized_cte_lifting_disabled\n");
		}
		return plan;
	}

	// Skip PT for the current scope when its main body contains set operators
	// or inequality joins. CTE definitions have already been PT'd independently
	// by the lifter above, so we don't lose their wins.
	const char *skip_reason = nullptr;
	if (config.skip_on_set_operator && HasSetOperator(*plan)) {
		skip_reason = "set_operator";
	} else if (config.skip_on_inequality_join && HasInequalityJoin(*plan)) {
		skip_reason = "inequality_join";
	} else if (config.skip_left_deep_join_tree && IsLeftDeepJoinTree(*plan, optimizer.context)) {
		skip_reason = "left_deep_join_tree";
	}
	if (skip_reason) {
		if (config.log_transfer_steps) {
			fprintf(stderr, "[RPT-Excitation] scope skipped: %s\n", skip_reason);
		}
		return plan;
	}

	if (config.enable_table_protection) {
		if (auto *emgr = dynamic_cast<ExcitationGraphManager *>(graph_manager.get())) {
			emgr->SetProtectedTables(ComputeProtectedTables(*plan));
		}
	}

	graph_manager->Build(*plan);

	if (ClientConfig::GetConfig(optimizer.context).enable_profiler || config.log_transfer_steps) {
		cached_debug_info = GetDebugInfo();
	}

	plan = RewriteQueryPlan(std::move(plan));
	// Clear table_operators: RewriteQueryPlan may have replaced LogicalGet
	// nodes, making stored references dangling.
	graph_manager->table_operator_manager.ClearTableOperators();

	return plan;
}

string PredicateTransferOptimizer::GetDebugInfo() const {
	if (!cached_debug_info.empty()) {
		return cached_debug_info;
	}
	if (graph_manager->table_operator_manager.GetAllTableOperators().empty()) {
		return "";
	}
	stringstream ss;
	ss << "tables=" << graph_manager->TablesToString() << "\n";
	ss << "edges=" << graph_manager->EdgesToString() << "\n";
	ss << "transfer_plan=" << graph_manager->TransferPlanToString() << "\n";
	ss << "transfer_steps=(";
	for (auto &step : graph_manager->result_transfer_steps) {
		ss << step.ToString() << "\n";
	}
	ss << ")\n\n";
	return ss.str();
}

//===--------------------------------------------------------------------===//
// Helper: select and reorder columns from a ColumnDataCollection
//===--------------------------------------------------------------------===//
static unique_ptr<ColumnDataCollection> SelectColumns(ColumnDataCollection &src, const vector<idx_t> &col_positions) {
	auto &src_types = src.Types();
	vector<LogicalType> dst_types;
	for (auto pos : col_positions) {
		dst_types.push_back(src_types[pos]);
	}

	auto result = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), dst_types);

	ColumnDataScanState scan_state;
	src.InitializeScan(scan_state);

	DataChunk src_chunk, dst_chunk;
	src_chunk.Initialize(Allocator::DefaultAllocator(), src_types);
	dst_chunk.Initialize(Allocator::DefaultAllocator(), dst_types);

	while (true) {
		src_chunk.Reset();
		src.Scan(scan_state, src_chunk);
		if (src_chunk.size() == 0) {
			break;
		}

		dst_chunk.Reset();
		dst_chunk.SetCardinalityUnsafe(src_chunk.size());
		for (idx_t i = 0; i < col_positions.size(); i++) {
			dst_chunk.data[i].Reference(src_chunk.data[col_positions[i]]);
		}
		result->Append(dst_chunk);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Build a LogicalColumnDataGet that replaces a materialized subtree.
// Strips the rowid column and produces columns matching `wanted_bindings`.
//===--------------------------------------------------------------------===//
static unique_ptr<LogicalOperator> BuildMemoryScan(TableScanner &scanner, const vector<ColumnBinding> &wanted_bindings,
                                                   const vector<LogicalType> &wanted_types, TableIndex table_index) {
	// A MemoryScan replaces the subtree with a flat ColumnDataGet exposing
	// exactly wanted_bindings/wanted_types. If the source op reports a
	// different number of bindings than types (seen when an upstream pass left
	// a projection_map and types out of sync), the replacement would produce
	// an operator whose GetColumnBindings() disagrees with types and break
	// binding resolution downstream — fall back to passthrough instead.
	if (wanted_bindings.size() != wanted_types.size()) {
		return nullptr;
	}
	const auto &output_bindings = scanner.GetOutputBindings();
	auto rowid_chunk_col = scanner.GetRowIdChunkCol();

	if (getenv("RPT_DEBUG_BINDINGS")) {
		fprintf(stderr, "[MemScan] new_table=%llu wanted_bindings=[", (unsigned long long)table_index.index);
		for (auto &b : wanted_bindings) {
			fprintf(stderr, "(%llu,%llu) ", (unsigned long long)b.table_index.index,
			        (unsigned long long)b.column_index);
		}
		fprintf(stderr, "] output_bindings=[");
		for (auto &b : output_bindings) {
			fprintf(stderr, "(%llu,%llu) ", (unsigned long long)b.table_index.index,
			        (unsigned long long)b.column_index);
		}
		fprintf(stderr, "]\n");
	}

	// Build lookup: output_binding → chunk column index (excluding rowid)
	unordered_map<ColumnBinding, idx_t, ColumnBindingHashFunc> binding_to_col;
	idx_t col_idx = 0;
	for (idx_t i = 0; i < output_bindings.size(); i++) {
		if (i == rowid_chunk_col) {
			continue;
		}
		binding_to_col[output_bindings[i]] = col_idx++;
	}

	// Map each wanted binding → position in the data (post-rowid-strip)
	vector<idx_t> col_positions;
	for (auto &b : wanted_bindings) {
		auto it = binding_to_col.find(b);
		if (it != binding_to_col.end()) {
			col_positions.push_back(it->second);
		} else {
			return nullptr; // Column not available
		}
	}

	// Build the strip list: all columns except rowid
	vector<idx_t> strip_positions;
	for (idx_t i = 0; i < output_bindings.size(); i++) {
		if (i != rowid_chunk_col) {
			strip_positions.push_back(i);
		}
	}

	// Check if we need reordering beyond just stripping rowid
	bool needs_reorder = (col_positions.size() != strip_positions.size());
	for (idx_t i = 0; !needs_reorder && i < col_positions.size(); i++) {
		if (col_positions[i] != i) {
			needs_reorder = true;
		}
	}

	auto data = scanner.TakeData();
	if (!data) {
		return nullptr;
	}

	unique_ptr<ColumnDataCollection> final_data;
	if (!needs_reorder) {
		// Just strip rowid
		final_data = SelectColumns(*data, strip_positions);
	} else {
		// Strip rowid + reorder in one pass: compose strip_positions[col_positions[i]]
		vector<idx_t> combined;
		for (auto pos : col_positions) {
			combined.push_back(strip_positions[pos]);
		}
		final_data = SelectColumns(*data, combined);
	}

	return make_uniq<LogicalColumnDataGet>(table_index, wanted_types, std::move(final_data));
}

static LogicalGet *FindLeafGet(LogicalOperator &op) {
	auto *leaf = &op;
	while (leaf->type != LogicalOperatorType::LOGICAL_GET && !leaf->children.empty()) {
		leaf = leaf->children[0].get();
	}
	if (leaf->type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	return &leaf->Cast<LogicalGet>();
}

static void InjectDefaultScanFilters(LogicalGet &get, const shared_ptr<RPTFilter> &row_id_filter,
                                     const vector<DirectFilterInfo> *direct_filters) {
	auto &column_ids = get.GetMutableColumnIds();

	if (row_id_filter) {
		idx_t row_id_col_index = 0;
		bool found = false;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].IsRowIdColumn()) {
				row_id_col_index = i;
				found = true;
				break;
			}
		}
		if (!found) {
			row_id_col_index = column_ids.size();
			column_ids.push_back(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID));
		}
		get.table_filters.PushFilter(ProjectionIndex(row_id_col_index),
		                             RPTTableFilter::MakeOptional(row_id_filter, LogicalType::ROW_TYPE));
	}

	if (!direct_filters) {
		return;
	}
	for (auto &df : *direct_filters) {
		idx_t pos = df.binding.column_index;
		if (pos >= column_ids.size() || column_ids[pos].IsVirtualColumn()) {
			continue;
		}
		auto col_id = column_ids[pos].GetPrimaryIndex();
		auto key_type = (col_id < get.returned_types.size()) ? get.returned_types[col_id] : LogicalType::BIGINT;
		get.table_filters.PushFilter(ProjectionIndex(pos), RPTTableFilter::MakeOptional(df.filter, key_type));
	}
}

//===--------------------------------------------------------------------===//
// Final query plan rewrite
//===--------------------------------------------------------------------===//

unique_ptr<LogicalOperator> PredicateTransferOptimizer::RewriteQueryPlan(unique_ptr<LogicalOperator> plan) {
	auto &excitation_mgr = static_cast<ExcitationGraphManager &>(*graph_manager);
	auto enable_profiler = ClientConfig::GetConfig(optimizer.context).enable_profiler || config.log_transfer_steps;
	ColumnBindingReplacer binding_replacer;

	std::function<unique_ptr<LogicalOperator>(unique_ptr<LogicalOperator>)> inject =
	    [&](unique_ptr<LogicalOperator> op) -> unique_ptr<LogicalOperator> {
		// Only ops that were registered as table-operator roots have a rewrite
		// decision; everything else (joins, top-level projections, etc.) is
		// just a pass-through — recurse into children and return as-is.
		auto table_id = TableOperatorManager::GetScalarTableIndex(*op);
		auto *table_op = excitation_mgr.table_operator_manager.GetTableOperator(table_id);
		if (table_op != op.get()) {
			for (auto &child : op->children) {
				if (child) {
					child = inject(std::move(child));
				}
			}
			return op;
		}

		auto result = excitation_mgr.GetTableResult(table_id, *op);
		switch (result.kind) {
		case TableTransferResult::Kind::Empty:
			if (enable_profiler) {
				fprintf(stderr, "\t[RewriteQueryPlan] table_id=%lu → EmptyResult\n", table_id);
			}
			return make_uniq<LogicalEmptyResult>(op->types, op->GetColumnBindings());

		case TableTransferResult::Kind::MemoryScan: {
			// Pending expr filter never baked — the original op still holds
			// the FILTER chain; MemoryScan would strip it.
			if (result.scanner->HasPendingExprFilter()) {
				if (enable_profiler) {
					fprintf(stderr, "\t[RewriteQueryPlan] table_id=%lu → Passthrough\n", table_id);
				}
				return op;
			}
			if (enable_profiler) {
				fprintf(stderr, "\t[RewriteQueryPlan] table_id=%lu → MemoryScan\n", table_id);
			}
			// IMPORTANT: re-resolve types before reading. LogicalFilter (and any
			// op with a projection_map) maps types through projection_map at
			// ResolveTypes time; if an upstream optimizer pass updated
			// projection_map without re-resolving, op->types stays stale and
			// disagrees with op->GetColumnBindings(), which produces a
			// "inequal num bindings/types" failure in ColumnBindingResolver.
			op->ResolveOperatorTypes();
			auto old_bindings = op->GetColumnBindings();
			auto new_table_index = optimizer.binder.GenerateTableIndex();
			if (auto scan = BuildMemoryScan(*result.scanner, old_bindings, op->types, new_table_index)) {
				auto new_bindings = scan->GetColumnBindings();
				D_ASSERT(old_bindings.size() == new_bindings.size());
				for (idx_t i = 0; i < old_bindings.size(); i++) {
					binding_replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
				}
				return scan;
			}
			// A wanted binding is missing from the materialized data (seen with
			// late materialization). Never splice a null into the plan — keep
			// the original subtree; it is correct, just without the RPT rewrite.
			if (enable_profiler) {
				fprintf(stderr, "\t[RewriteQueryPlan] table_id=%lu → MemoryScan FAILED, falling back to Passthrough\n",
				        table_id);
			}
			return op;
		}

		case TableTransferResult::Kind::DefaultScan:
			if (enable_profiler) {
				fprintf(stderr, "\t[RewriteQueryPlan] table_id=%lu → DefaultScan\n", table_id);
			}
			if (auto *get = FindLeafGet(*op)) {
				InjectDefaultScanFilters(*get, result.row_id_filter, result.direct_filters);
			}
			return op;
		}
		return op;
	};

	plan = inject(std::move(plan));
	if (!binding_replacer.replacement_bindings.empty()) {
		binding_replacer.VisitOperator(*plan);
	}
	EmptyResultPullup empty_result_pullup;
	plan = empty_result_pullup.Optimize(std::move(plan));
	excitation_mgr.ClearMaterializedScanners();
	return plan;
}

} // namespace duckdb
