#include "predicate_transfer/table_operator_manager.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

void TableOperatorManager::FindInnerColumnRef(Expression &expr, BoundColumnRefExpression *&out_ref) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		if (out_ref) {
			// Multiple column refs found — ambiguous, bail out
			out_ref = nullptr;
			return;
		}
		out_ref = &expr.Cast<BoundColumnRefExpression>();
		return;
	}
	// Only recurse into DuckDB internal functions (compression/decompression).
	// These are bijective and safe to see through. User-visible functions
	// like col + 1 could produce wrong BF values if resolved through.
	if (expr.GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName().GetIdentifierName().rfind("__internal_", 0) != 0) {
			return;
		}
	}
	// Scalar-subquery error wrapping: DuckDB binds scalar subqueries as
	//   CASE WHEN count > 1 THEN error('...') ELSE first_agg END
	// (see plan_subquery.cpp). The WHEN/THEN branch is a row-count guard
	// that has nothing to do with the value flow; semantically the result
	// equals the ELSE expression. Recurse only into ELSE so PROJECTION
	// rename mapping can see through to the underlying column ref.
	if (expr.GetExpressionType() == ExpressionType::CASE_EXPR) {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		auto &checks = case_expr.CaseChecksMutable();
		auto &else_expr = case_expr.ElseMutable();
		bool is_scalar_subquery_guard =
		    checks.size() == 1 && else_expr && checks[0].then_expr &&
		    checks[0].then_expr->GetExpressionType() == ExpressionType::BOUND_FUNCTION &&
		    checks[0].then_expr->Cast<BoundFunctionExpression>().Function().GetName() == "error";
		if (is_scalar_subquery_guard) {
			FindInnerColumnRef(*else_expr, out_ref);
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { FindInnerColumnRef(child, out_ref); });
}

void TableOperatorManager::Build(LogicalOperator &plan) {
	contains_join_set_.clear();
	CollectJoins(plan);
	CollectTableOperators(plan);
	ComputeCDCAliasGroups();
}

void TableOperatorManager::ComputeCDCAliasGroups() {
	// Group tables by the pointer to their underlying ColumnDataCollection.
	// Two LogicalColumnDataGet ops that share a CDC (common_subplan fan-out)
	// end up with different table_indexes but the same data, and RPT must
	// not propagate filters between them: the "filtered cardinality" of one
	// through the other is trivially the unfiltered cardinality.
	// Walk down through FILTER wrappers to find an underlying CHUNK_GET.
	// FILTER+CHUNK_GET refs to the same CDC are grouped with their bare peers
	// — the FILTER stays in the plan but lineage is shared, so CDC-internal
	// BF transfer between aliases is short-circuited as same-lineage.
	auto find_chunk_get = [](const LogicalOperator &op) -> const LogicalColumnDataGet * {
		const LogicalOperator *cur = &op;
		while (cur && cur->type != LogicalOperatorType::LOGICAL_CHUNK_GET) {
			if (cur->type == LogicalOperatorType::LOGICAL_FILTER && !cur->children.empty()) {
				cur = cur->children[0].get();
			} else {
				return nullptr;
			}
		}
		return cur ? &cur->Cast<LogicalColumnDataGet>() : nullptr;
	};
	vector<pair<const ColumnDataCollection *, vector<idx_t>>> by_cdc;
	for (auto &operator_ref : table_dfs_order) {
		auto &op = operator_ref.get();
		auto table_id = GetScalarTableIndex(op);
		auto canonical = table_operators.find(table_id);
		if (canonical == table_operators.end() || &canonical->second.get() != &op) {
			continue;
		}
		auto *cdg = find_chunk_get(op);
		if (!cdg || !cdg->collection) {
			continue;
		}
		idx_t group_index = by_cdc.size();
		for (idx_t i = 0; i < by_cdc.size(); i++) {
			if (by_cdc[i].first == cdg->collection.get()) {
				group_index = i;
				break;
			}
		}
		if (group_index == by_cdc.size()) {
			by_cdc.emplace_back(cdg->collection.get(), vector<idx_t> {});
		}
		by_cdc[group_index].second.push_back(table_id);
	}
	for (auto &grp : by_cdc) {
		if (grp.second.size() < 2) {
			continue;
		}
		unordered_set<idx_t> alias_set(grp.second.begin(), grp.second.end());
		for (auto id : grp.second) {
			cdc_alias_groups_[id] = alias_set;
		}
	}
}

const unordered_set<idx_t> &TableOperatorManager::GetCDCAliases(idx_t table_id) const {
	static const unordered_set<idx_t> empty;
	auto it = cdc_alias_groups_.find(table_id);
	return it == cdc_alias_groups_.end() ? empty : it->second;
}

LogicalOperator *TableOperatorManager::GetTableOperator(idx_t table_idx) {
	auto itr = table_operators.find(table_idx);
	if (itr == table_operators.end()) {
		return nullptr;
	}

	return &itr->second.get();
}

const LogicalOperator *TableOperatorManager::GetTableOperator(idx_t table_idx) const {
	auto itr = table_operators.find(table_idx);
	if (itr == table_operators.end()) {
		return nullptr;
	}

	return &itr->second.get();
}

ColumnBinding TableOperatorManager::GetRenaming(ColumnBinding binding) {
	auto itr = rename_col_bindings.find(binding);
	while (itr != rename_col_bindings.end()) {
		binding = itr->second;
		itr = rename_col_bindings.find(binding);
	}
	return binding;
}

string TableOperatorManager::GetTableName(const LogicalOperator &op) {
	string ret;
	auto params = op.ParamsToString();

	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (params.contains("Table")) {
			ret = params.at("Table");
		} else {
			ret = "Unknown";
		}
		return ret;
	}

	if (op.type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
		return "MemoryScan";
	}

	if (params.contains("Table")) {
		ret = params.at("Table");
	} else if (op.children.size() != 1) {
		ret = "Unknown";
	} else {
		ret = GetTableName(*op.children[0]);
	}

	return ret;
}

string TableOperatorManager::GetColumnName(const ColumnBinding &binding) {
	auto op = GetTableOperator(binding.table_index.index);

	// If the table operator was wrapped in a filter, unwrap it to find the base table names
	while (op && op->type == LogicalOperatorType::LOGICAL_FILTER && !op->children.empty()) {
		op = op->children[0].get();
	}

	if (op && op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (binding.column_index < get.GetColumnIds().size()) {
			auto &col_idx = get.GetColumnIds()[binding.column_index];
			if (col_idx.IsRowIdColumn()) {
				return "rowid";
			}
			if (col_idx.HasPrimaryIndex()) {
				auto idx = col_idx.GetPrimaryIndex();
				if (idx < get.names.size()) {
					return get.names[idx].GetIdentifierName();
				}
			} else {
				return col_idx.GetFieldName();
			}
		}
	}

	return "unknown_column_" + std::to_string(binding.column_index);
}

idx_t TableOperatorManager::GetScalarTableIndex(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		return op.GetTableIndex()[0].index;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		return GetScalarTableIndex(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return op.GetTableIndex()[1].index;
	}
	default: {
		// For joins folded into a FILTER (e.g. MARK JOIN for IN),
		// traverse the left child to find the base table index
		if (op.children.empty()) {
			return std::numeric_limits<idx_t>::max();
		}
		return GetScalarTableIndex(*op.children[0]);
	}
	}
}

void TableOperatorManager::AddTableOperator(LogicalOperator &op) {
	op.estimated_cardinality = op.EstimateCardinality(context);
	table_dfs_order.push_back(op);

	idx_t table_idx = GetScalarTableIndex(op);
	if (table_idx != std::numeric_limits<idx_t>::max() && table_operators.find(table_idx) == table_operators.end()) {
		table_operators.emplace(table_idx, op);
	}
}

//===--------------------------------------------------------------------===//
// Two-pass extraction
//===--------------------------------------------------------------------===//
//
// Conceptual model: PT cuts the plan tree at every inner equi-join boundary.
// The children of those joins are exactly the table operators; everything
// between joins (FILTER, PROJECTION, AGG, CROSS_PRODUCT, MARK JOIN, …) is
// just *part of* the table operator subtree on that side.
//
//   Pass 1 — CollectJoins: walk the tree, collect inner equi-joins into
//     join_operators, and mark every operator whose subtree contains at
//     least one inner equi-join in contains_join_set_.
//
//   Pass 2 — CollectTableOperators: any subtree NOT in contains_join_set_
//     becomes a table operator (admitted at its root). When descending
//     through contains_join_set_ operators, record PROJECTION/AGG renames
//     so upstream join conditions can resolve through to the admitted
//     subtree's table_index.
//
// Operators that are opaque to the outer scope (RECURSIVE_CTE, WINDOW,
// UNION/EXCEPT/INTERSECT) are NOT recursed into in pass 1 — joins inside
// them are private to that subtree and irrelevant to the outer join graph.
// In pass 2 they fall into the "no inner equi-join below" branch and get
// admitted as a single table operator each.

bool TableOperatorManager::IsInnerEquiJoin(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	switch (join.join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::SEMI:
	case JoinType::RIGHT_SEMI:
		break;
	default:
		return false;
	}
	return std::any_of(join.conditions.begin(), join.conditions.end(), [](const JoinCondition &jc) {
		return jc.IsComparison() && jc.GetComparisonType() == ExpressionType::COMPARE_EQUAL &&
		       jc.GetLHS().GetExpressionType() == ExpressionType::BOUND_COLUMN_REF &&
		       jc.GetRHS().GetExpressionType() == ExpressionType::BOUND_COLUMN_REF;
	});
}

bool TableOperatorManager::CollectJoins(LogicalOperator &op) {
	// CTE_REF / RECURSIVE_CTE: skip entirely. CTE_REF is a leaf reference and
	// RECURSIVE_CTE is handled separately at the top of pass 2.
	if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE || op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return false;
	}
	// Opaque value-producing subtrees (WINDOW / UNION / EXCEPT / INTERSECT)
	// cannot be admitted as table operators: their column bindings span
	// multiple table_indices (passthrough child bindings + the opaque op's
	// own table_index), and BuildMemoryScan in the rewrite phase collapses
	// everything to a single table_index, losing passthrough bindings. Joins
	// above the opaque op that reference passthrough bindings would then fail
	// to resolve (e.g. q44's outer i_item_sk = item_sk where item_sk is a
	// store_sales binding passed through a WINDOW).
	//
	// We must therefore force pass 2 to descend through the opaque op AND
	// through any wrapping FILTER/etc. above it, all the way to deeper inner
	// GETs which can be admitted as their own table operators. To do this we
	// mark the opaque op (and recursively, every ancestor) with
	// contains_join_set_ by returning true upward — even though there's no
	// real inner equi-join "boundary" at this level. We also collect any
	// real inner joins inside the opaque subtree as part of the global join
	// graph, since those joins may carry edges that PT can use.
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		for (auto &child : op.children) {
			CollectJoins(*child);
		}
		contains_join_set_.insert(&op);
		return true; // force pass 2 to descend through us
	}
	default:
		break;
	}

	// LOGICAL_DELIM_GET is a *descent boundary*, not a join. The cardinality
	// oracle cannot execute a subtree containing a DELIM_GET descendant
	// standalone — PhysicalColumnDataScan(DELIM_SCAN) requires its parent
	// PhysicalDelimJoin to register a delim_join_dependency, and that parent
	// is not part of the copied subtree. So we must descend down to the
	// DELIM_GET itself and admit it as its own table operator (matching the
	// historical behavior of the old extractor). Returning true here marks
	// every ancestor as needing descent in pass 2.
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}

	bool subtree_has_boundary = false;
	if (IsInnerEquiJoin(op)) {
		join_operators.push_back(op);
		subtree_has_boundary = true;
	}
	for (auto &child : op.children) {
		if (CollectJoins(*child)) {
			subtree_has_boundary = true;
		}
	}
	if (subtree_has_boundary) {
		contains_join_set_.insert(&op);
	}
	return subtree_has_boundary;
}

void TableOperatorManager::RecordRenameOneLevel(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto old_refs = op.GetColumnBindings();
		for (size_t i = 0; i < op.expressions.size(); i++) {
			if (op.expressions[i]->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = op.expressions[i]->Cast<BoundColumnRefExpression>();
				rename_col_bindings.insert({old_refs[i], col_ref.Binding()});
			} else {
				// See through wrapping functions (e.g. __internal_compress_integral_*)
				// and scalar-subquery error guards.
				BoundColumnRefExpression *inner_ref = nullptr;
				FindInnerColumnRef(*op.expressions[i], inner_ref);
				if (inner_ref) {
					rename_col_bindings.insert({old_refs[i], inner_ref->Binding()});
				}
			}
		}
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op.Cast<LogicalAggregate>();
		if (agg.groups.empty()) {
			return;
		}
		auto old_refs = agg.GetColumnBindings();
		for (size_t i = 0; i < agg.groups.size(); i++) {
			if (agg.groups[i]->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = agg.groups[i]->Cast<BoundColumnRefExpression>();
				rename_col_bindings.insert({old_refs[i], col_ref.Binding()});
			}
		}
	}
}

//! Operators that are *transparent* — never admit them as table operator
//! roots, always record renames (if applicable) and recurse into children.
//! These are pass-through wrappers from PT's perspective: their column
//! bindings rename underlying columns, and the BF data / filter injection
//! must operate on the underlying GET's column types, not on the wrapper's
//! decompressed/aggregated types. (This matches the historical behavior of
//! the old extractor: PROJECTION and grouped AGG were never table operator
//! roots — they only contributed to rename_col_bindings.)
static bool IsTransparentForTableOp(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		return true;
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op.Cast<LogicalAggregate>();
		return !agg.groups.empty();
	}
	return false;
}

void TableOperatorManager::CollectTableOperators(LogicalOperator &op) {
	// CTE_REF is not a base table; only valid inside RECURSIVE_CTE which we
	// also skip. Defensive return at top level.
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return;
	}
	// DELIM_GET cannot be executed standalone via the cardinality oracle —
	// PhysicalColumnDataScan(DELIM_SCAN) requires its parent PhysicalDelimJoin
	// to register a delim_join_dependency. Skip it entirely. The correlated
	// subquery's delim side carries no useful filter information for PT.
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return;
	}
	// Transparent wrappers (PROJECTION, grouped AGG): never admit as a table
	// operator root. Record their renames so upstream join conditions can
	// resolve through to the deeper subtree's bindings, then descend.
	if (IsTransparentForTableOp(op)) {
		RecordRenameOneLevel(op);
		for (auto &child : op.children) {
			CollectTableOperators(*child);
		}
		return;
	}
	// Opaque value-producing operators (WINDOW / UNION / EXCEPT / INTERSECT):
	// recurse through them without admitting them as a table operator. We
	// cannot admit them — see the matching comment in CollectJoins. Pass 1
	// has already inserted them into contains_join_set_ to force descent
	// through any wrapping FILTER above them; we still need this explicit
	// case here in case the opaque op is the root of the subtree we're
	// visiting.
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		for (auto &child : op.children) {
			CollectTableOperators(*child);
		}
		return;
	default:
		break;
	}
	if (!contains_join_set_.count(&op)) {
		// Maximal subtree without an inner equi-join, rooted at a non-
		// transparent operator (FILTER, GET, CHUNK_GET, CROSS_PRODUCT, MARK
		// JOIN, OUTER JOIN, …). Admit it.
		AddTableOperator(op);
		return;
	}
	// Subtree contains inner equi-join(s); descend.
	for (auto &child : op.children) {
		CollectTableOperators(*child);
	}
}

} // namespace duckdb
