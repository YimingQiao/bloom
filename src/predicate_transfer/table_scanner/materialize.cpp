#include "predicate_transfer/table_scanner/materialization.hpp"
#include "predicate_transfer/rpt_result_collector.hpp"
#include "predicate_transfer/table_scanner/filter_set.hpp"
#include "predicate_transfer/table_scanner/index_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

#include <algorithm>
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>

namespace duckdb {

using MaterializeClock = std::chrono::steady_clock;

static double MaterializeElapsedMs(MaterializeClock::time_point begin, MaterializeClock::time_point end) {
	return std::chrono::duration<double, std::milli>(end - begin).count();
}

// Expose the row ID through the same FILTER/MARK chain used to install the
// final disk filter. Projection maps index the child's output, not the GET.
static ColumnBinding ExposeScanRowId(LogicalOperator &op, LogicalGet &get) {
	if (&op == &get) {
		auto &columns = get.GetMutableColumnIds();
		idx_t index = 0;
		while (index < columns.size() && !columns[index].IsRowIdColumn()) {
			++index;
		}
		if (index == columns.size()) {
			columns.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
		}
		auto &ids = get.projection_ids;
		if (!ids.empty() && std::find(ids.begin(), ids.end(), ProjectionIndex(index)) == ids.end()) {
			ids.emplace_back(index);
		}
		return ColumnBinding(get.table_index, ProjectionIndex(index));
	}
	auto &child = *op.children[0];
	auto &map = op.type == LogicalOperatorType::LOGICAL_FILTER ? op.Cast<LogicalFilter>().projection_map
	                                                          : op.Cast<LogicalComparisonJoin>().left_projection_map;
	const auto old_bindings = child.GetColumnBindings();
	auto row_id = ExposeScanRowId(child, get);
	if (!map.empty()) {
		const auto bindings = child.GetColumnBindings();
		// A MARK column moves when its left input gains an output.
		for (auto &index : map) {
			index = ProjectionIndex(std::find(bindings.begin(), bindings.end(), old_bindings[index]) - bindings.begin());
		}
		const auto pos = ProjectionIndex(std::find(bindings.begin(), bindings.end(), row_id) - bindings.begin());
		if (std::find(map.begin(), map.end(), pos) == map.end()) {
			map.push_back(pos);
		}
	}
	return row_id;
}

bool TableMaterialization::LogEnabled() const {
	if (ClientConfig::GetConfig(context_).enable_profiler) {
		return true;
	}
	Value v;
	return context_.TryGetCurrentSetting("bloom_log_transfer_steps", v) && v.GetValue<bool>();
}

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//

TableMaterialization::TableMaterialization(Optimizer &optimizer, ClientContext &context, LogicalOperator &table_op,
                                           bool enable_late_materialization)
    : optimizer_(optimizer), context_(context), table_op_(table_op),
      enable_late_materialization_(enable_late_materialization) {
	// Step 1: walk down any FILTER chain to find the leaf.
	LogicalOperator *leaf = &table_op_;
	vector<LogicalFilter *> filter_chain;
	while (leaf->type == LogicalOperatorType::LOGICAL_FILTER && leaf->children.size() == 1) {
		filter_chain.push_back(&leaf->Cast<LogicalFilter>());
		leaf = leaf->children[0].get();
	}

	// Step 2: dispatch on leaf type. Only CHUNK_GET qualifies for the fast-path;
	// anything else (LOGICAL_GET, UNION, etc.) falls through to Materialize().
	if (leaf->type != LogicalOperatorType::LOGICAL_CHUNK_GET) {
		return;
	}

	auto &chunk_get = leaf->Cast<LogicalColumnDataGet>();
	if (chunk_get.collection.get_owned_shared()) {
		data_ = chunk_get.collection.get_owned_shared();
	} else {
		D_ASSERT(chunk_get.collection);
		auto *raw = chunk_get.collection.get();
		data_ = shared_ptr<ColumnDataCollection>(raw, [](ColumnDataCollection *) {});
	}
	table_op_.ResolveOperatorTypes();
	output_bindings_ = table_op_.GetColumnBindings();
	materialized_ = true;
	data_->InitializeScan(scan_state_);

	if (filter_chain.empty()) {
		return;
	}

	// Walk filter_chain bottom-up, rewriting each filter's expressions to
	// BoundReference against raw CDC positions and composing per-FILTER
	// projection_maps into a single output→raw mapping.
	auto chunk_bindings = chunk_get.GetColumnBindings();
	vector<ColumnBinding> current_bindings = chunk_bindings;
	vector<idx_t> current_positions;
	current_positions.reserve(chunk_bindings.size());
	for (idx_t i = 0; i < chunk_bindings.size(); i++) {
		current_positions.push_back(i);
	}

	std::function<void(unique_ptr<Expression> &)> rewrite_refs = [&](unique_ptr<Expression> &expr) {
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &cref = expr->Cast<BoundColumnRefExpression>();
			idx_t binding_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < current_bindings.size(); i++) {
				if (current_bindings[i] == cref.Binding()) {
					binding_idx = i;
					break;
				}
			}
			D_ASSERT(binding_idx != DConstants::INVALID_INDEX);
			if (binding_idx == DConstants::INVALID_INDEX) {
				throw InternalException("Bloom could not bind CHUNK_GET filter column %s", cref.Binding().ToString());
			}
			expr = make_uniq<BoundReferenceExpression>(cref.GetAlias(), cref.GetReturnType(),
			                                           current_positions[binding_idx]);
			return;
		}
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) { rewrite_refs(child); });
	};

	vector<unique_ptr<Expression>> rewritten;
	for (auto it = filter_chain.rbegin(); it != filter_chain.rend(); ++it) {
		auto *f = *it;
		for (auto &e : f->expressions) {
			auto cloned = e->Copy();
			rewrite_refs(cloned);
			rewritten.push_back(std::move(cloned));
		}
		if (f->HasProjectionMap()) {
			vector<ColumnBinding> nb;
			vector<idx_t> np;
			nb.reserve(f->projection_map.size());
			np.reserve(f->projection_map.size());
			for (auto idx : f->projection_map) {
				nb.push_back(current_bindings[idx]);
				np.push_back(current_positions[idx]);
			}
			current_bindings = std::move(nb);
			current_positions = std::move(np);
		}
	}
	D_ASSERT(current_bindings == output_bindings_);

	// Determine whether a final projection is needed (non-identity mapping).
	bool needs_projection = current_positions.size() != chunk_bindings.size();
	for (idx_t i = 0; !needs_projection && i < current_positions.size(); i++) {
		if (current_positions[i] != i) {
			needs_projection = true;
		}
	}

	pending_expression_ = make_uniq<PendingExpression>();
	if (!rewritten.empty()) {
		unique_ptr<Expression> final_expr;
		if (rewritten.size() == 1) {
			final_expr = std::move(rewritten[0]);
		} else {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &e : rewritten) {
				conj->GetChildrenMutable().push_back(std::move(e));
			}
			final_expr = std::move(conj);
		}
		pending_expression_->executor = make_uniq<ExpressionExecutor>(context_);
		pending_expression_->executor->AddExpression(*final_expr);
		pending_expression_->expressions.push_back(std::move(final_expr));
	}
	if (needs_projection) {
		pending_expression_->projection_map = std::move(current_positions);
	}
	pending_expression_->scratch.Initialize(BufferAllocator::Get(context_), data_->Types());
}

//===--------------------------------------------------------------------===//
// Materialize — load data via Executor with filters preserved
//===--------------------------------------------------------------------===//

//! Helper: execute a plan via Executor and store the result in data_.
bool TableMaterialization::ExecutePlan(unique_ptr<LogicalOperator> plan_copy) {
	auto total_start = MaterializeClock::now();
	auto log_timing = LogEnabled();
	auto col_types = plan_copy->types;
	if (col_types.empty()) {
		return false;
	}

	PhysicalPlanGenerator generator(context_);
	auto physical_plan = generator.Plan(std::move(plan_copy));
	auto physical_plan_end = MaterializeClock::now();

	PreparedStatementData stmt_data(StatementType::SELECT_STATEMENT);
	stmt_data.physical_plan = std::move(physical_plan);

	auto &root = stmt_data.physical_plan->Root();
	stmt_data.types = root.types;
	stmt_data.names.resize(stmt_data.types.size());
	for (idx_t i = 0; i < stmt_data.types.size(); i++) {
		stmt_data.names[i] = Identifier("col" + std::to_string(i));
	}
	auto statement_end = MaterializeClock::now();

	auto &client_data = ClientData::Get(context_);
	auto previous_profiler = client_data.profiler;
	client_data.profiler = make_shared_ptr<QueryProfiler>(context_);
	Executor executor(context_);
	auto collector = GetRPTResultCollector(context_, stmt_data);
	executor.Initialize(std::move(collector));
	auto executor_init_end = MaterializeClock::now();

	while (executor.ExecuteTask() != QueryResultState::FINISHED) {
	}
	auto execute_tasks_end = MaterializeClock::now();

	auto result = executor.GetResult();
	D_ASSERT(result);
	executor.CancelTasks();
	if (result->HasError()) {
		client_data.profiler = std::move(previous_profiler);
		result->ThrowError();
	}
	auto get_result_end = MaterializeClock::now();
	data_ = shared_ptr<ColumnDataCollection>(result->TakeCollection().release());
	if (!data_) {
		data_ = shared_ptr<ColumnDataCollection>(
		    make_uniq<ColumnDataCollection>(BufferAllocator::Get(context_), result->GetTypes()).release());
	}
	auto take_collection_end = MaterializeClock::now();
	client_data.profiler = std::move(previous_profiler);
	auto cleanup_end = MaterializeClock::now();

	materialized_ = true;
	data_->InitializeScan(scan_state_);
	auto scan_init_end = MaterializeClock::now();
	if (log_timing) {
		std::cerr << "      [MaterializeExecuteTiming] physical_plan="
		          << MaterializeElapsedMs(total_start, physical_plan_end)
		          << "ms statement=" << MaterializeElapsedMs(physical_plan_end, statement_end)
		          << "ms executor_init=" << MaterializeElapsedMs(statement_end, executor_init_end)
		          << "ms execute_tasks=" << MaterializeElapsedMs(executor_init_end, execute_tasks_end)
		          << "ms get_result=" << MaterializeElapsedMs(execute_tasks_end, get_result_end)
		          << "ms take_collection=" << MaterializeElapsedMs(get_result_end, take_collection_end)
		          << "ms cleanup=" << MaterializeElapsedMs(take_collection_end, cleanup_end)
		          << "ms scan_init=" << MaterializeElapsedMs(cleanup_end, scan_init_end)
		          << "ms total=" << MaterializeElapsedMs(total_start, scan_init_end) << "ms\n";
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Materialize — unified entry point for all plan shapes
//===--------------------------------------------------------------------===//
//
// Three-step pipeline applied to every table operator subtree:
//   1. Filter pushdown — push each pending BF/Bitmap filter into a matching
//      LogicalGet reached through filters or MARK left inputs, preserving semantics.
//      Pushed filters are removed from the set; un-pushed ones (e.g. on
//      grouped-AGG output, on CHUNK_GET, on virtual columns) stay and will
//      be applied in-memory by Scan() / Compact() after execution.
//   2. Late materialization — only when the leaf is a single LogicalGet:
//      column-prune via RemoveUnusedColumns and inject rowid for late
//      materialization in the join graph.
//   3. Execute — run the (possibly modified) subtree via Executor, store
//      the result in data_, then resolve chunk_col for any remaining
//      in-memory filters.

static void LogResidualFilters(LogicalOperator &op, const char *phase) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			std::cerr << "        [MaterializeResidualFilter] phase=" << phase << " expr=" << expr->ToString() << '\n';
		}
	}
	for (auto &child : op.children) {
		LogResidualFilters(*child, phase);
	}
}

static void CollectMaterializeGets(LogicalOperator &op, unordered_map<idx_t, LogicalGet *> &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		result.emplace(get.table_index.index, &get);
	}
	for (auto &child : op.children) {
		CollectMaterializeGets(*child, result);
	}
}

//! Log which storage columns each GET in the materialize subtree reads and the
//! exact filters attached to each column. Logging both before and after Bloom
//! injection separates DuckDB's local predicates from transfer filters.
static void LogMaterializePlan(LogicalOperator &plan, const char *phase) {
	unordered_map<idx_t, LogicalGet *> gets;
	CollectMaterializeGets(plan, gets);
	for (auto &entry : gets) {
		auto &get = *entry.second;
		auto table = get.GetTable();
		std::cerr << "      [MaterializePlan] phase=" << phase << " scan=" << (table ? table->name : "?") << " cols=(";
		for (auto &col_id : get.GetColumnIds()) {
			auto idx = col_id.GetPrimaryIndex();
			bool named = table && idx < table->GetColumns().LogicalColumnCount();
			std::cerr << (named ? table->GetColumns().GetColumn(LogicalIndex(idx)).Name() : "rowid") << " ";
		}
		std::cerr << ") filter_columns=" << get.table_filters.FilterCount() << '\n';
		vector<string> filter_labels;
		for (auto &filter_entry : get.table_filters) {
			auto projection_idx = filter_entry.GetIndex().GetIndex();
			string column_name = "?";
			idx_t storage_idx = DConstants::INVALID_INDEX;
			if (projection_idx < get.GetColumnIds().size()) {
				auto &column_id = get.GetColumnIds()[projection_idx];
				if (!column_id.IsVirtualColumn()) {
					storage_idx = column_id.GetPrimaryIndex();
					if (table && storage_idx < table->GetColumns().LogicalColumnCount()) {
						column_name =
						    table->GetColumns().GetColumn(LogicalIndex(storage_idx)).Name().GetIdentifierName();
					}
				}
			}
			auto &filter = filter_entry.Filter().Cast<ExpressionFilter>();
			filter_labels.push_back(column_name + ":" + filter.ToString(column_name));
			std::cerr << "        [MaterializeScanFilter] phase=" << phase << " projection=" << projection_idx
			          << " storage=";
			if (storage_idx == DConstants::INVALID_INDEX) {
				std::cerr << "?";
			} else {
				std::cerr << storage_idx;
			}
			std::cerr << " column=" << column_name << " expr=" << filter.ToString(column_name) << '\n';
		}
		auto initial_order = ExpressionHeuristics::GetInitialOrder(get.table_filters);
		std::cerr << "        [MaterializeInitialFilterOrder] phase=" << phase << " order=(";
		for (auto filter_idx : initial_order) {
			if (filter_idx < filter_labels.size()) {
				std::cerr << filter_labels[filter_idx] << " ";
			}
		}
		std::cerr << ")\n";
	}
	LogResidualFilters(plan, phase);
}

//! Statistics propagation can leave range predicates above an already-filtered
//! scan. An isolated materialization cannot rely on the enclosing join's runtime
//! filters to prune that scan. Add native optional filters for storage pruning,
//! preserving the original FILTER and its row-level evaluation order.
static void AddMaterializationRangePruning(LogicalOperator &op) {
	for (auto &child : op.children) {
		AddMaterializationRangePruning(*child);
	}
	if (op.type != LogicalOperatorType::LOGICAL_FILTER || op.children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op.children[0]->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || !get.function.filter_pushdown || get.ordinality_idx.IsValid()) {
		return;
	}
	for (auto &entry : get.table_filters) {
		auto &expr = *entry.Filter().Cast<ExpressionFilter>().expr;
		if (expr.CanThrow() || expr.IsVolatile() || ExpressionBarrier::Contains(expr)) {
			return;
		}
	}
	for (auto &expression : op.expressions) {
		auto &expr = *expression;
		// Revisit only column-to-constant ranges introduced by statistics.
		// Moving other residuals (notably string OR predicates) into the scan
		// can defeat the cheaper evaluation order chosen by DuckDB.
		auto type = expr.GetExpressionType();
		bool range = type == ExpressionType::COMPARE_BETWEEN || type == ExpressionType::COMPARE_LESSTHAN ||
		             type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
		             type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		idx_t columns = 0;
		optional_ptr<const BoundColumnRefExpression> column;
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
			if (child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				column = &child.Cast<BoundColumnRefExpression>();
				columns++;
			}
			range &= child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
			         child.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		});
		if (!range || columns != 1) {
			continue;
		}
		D_ASSERT(column->Binding().table_index == get.table_index);
		auto &column_type = column->GetReturnType();
		BoundReferenceExpression reference(column_type, storage_t(0));
		auto filter_expr = ExpressionIterator::ReplaceExpression(expr, *column, reference);
		get.table_filters.PushFilter(
		    column->Binding().column_index,
		    make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(filter_expr), column_type)));
	}
}

// Called on the GET/FILTER/MARK path that can expose a real base row ID.
static void KeepFilterColumns(LogicalOperator &plan, LogicalGet &get, const ScannerFilterSet &pending,
                              column_binding_set_t &required) {
	for (auto &filter : get.table_filters) {
		required.insert(ColumnBinding(get.table_index, filter.GetIndex()));
	}
	for (auto &filter : get.table_filters.GetMultiColumnFilters()) {
		for (auto idx : filter->Cast<ExpressionFilter>().column_indexes) {
			required.insert(ColumnBinding(get.table_index, idx));
		}
	}
	for (auto &filter : pending.Entries()) {
		for (auto &binding : filter.bindings) {
			required.insert(binding);
		}
	}
	for (auto *op = &plan; op != &get; op = op->children[0].get()) {
		LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr) {
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(**expr,
			    [&](const BoundColumnRefExpression &ref) { required.insert(ref.Binding()); });
		});
	}
}

// Compare widths of projected output columns. Predicate-only scan inputs do
// not occupy space in the materialized result.
static bool ShouldDeferPayload(ClientContext &context, LogicalOperator &plan, LogicalGet &get,
                               column_binding_set_t &required, optional_idx estimated_rows, bool log) {
	Value setting;
	context.TryGetCurrentSetting("bloom_late_min_byte_ratio", setting);
	plan.ResolveOperatorTypes();
	auto bindings = plan.GetColumnBindings();
	auto table = get.GetTable();
	double kept = 0;
	double deferred = 0;
	for (idx_t i = 0; i < bindings.size(); i++) {
		auto &type = plan.types[i];
		if (type.IsNested()) {
			return false;
		}
		double bytes = GetTypeIdSize(type.InternalType());
		// Keep every non-string output on the first scan, including fixed-width
		// payloads from scans that also project a deferred string.
		if (type.id() != LogicalTypeId::VARCHAR) {
			required.insert(bindings[i]);
		}
		bool retain = required.count(bindings[i]) != 0;
		if (bindings[i].table_index == get.table_index && bindings[i].column_index < get.GetColumnIds().size()) {
			const auto &column = get.GetColumnIds()[bindings[i].column_index];
			if (column.IsRowIdColumn()) {
				continue; // Keep the binding for fetch, but exclude it from the width estimate.
			}
			if (table && table->IsDuckTable() && !column.IsVirtualColumn() && !column.IsPushdownExtract() &&
			    type.InternalType() == PhysicalType::VARCHAR && get.returned_types[column.GetPrimaryIndex()] == type) {
				auto stats = table->GetStatistics(context, column.GetPrimaryIndex());
				auto rows = table->GetStorage().GetTotalRows();
				if (stats && rows) {
					auto length = StringStats::TotalStringLength(*stats);
					if (length.IsValid()) {
						// The slot already holds short inlined strings. Use average
						// logical length as a conservative payload-width estimate.
						bytes = MaxValue<double>(bytes, double(length.GetIndex()) / rows);
					}
				}
			}
		}
		(retain ? kept : deferred) += bytes;
	}
	auto threshold = setting.GetValue<double>();
	auto base_rows = table && table->IsDuckTable() ? table->GetStorage().GetTotalRows() : 0;
	auto selectivity = estimated_rows.IsValid() && base_rows
	                       ? MinValue(1.0, double(estimated_rows.GetIndex()) / base_rows)
	                       : 1.0;
	bool small_result = estimated_rows.IsValid() && estimated_rows.GetIndex() <= 8192 && selectivity <= 0.005;
	bool use_late = deferred > 0 && (small_result || deferred * (1.0 - selectivity) > kept * threshold);
	if (log && table) {
		double deferred_bytes = estimated_rows.IsValid() ? estimated_rows.GetIndex() * deferred : 0;
		std::cerr << "  [LateBytes] table=" << table->name << " deferred=" << deferred << " retained=" << kept
		          << " expected_rows=" << (estimated_rows.IsValid() ? double(estimated_rows.GetIndex()) : -1)
		          << " deferred_bytes=" << deferred_bytes
		          << " sel=" << selectivity << " base_rows=" << base_rows << " threshold=" << threshold << " eligible=" << use_late << '\n';
	}
	return use_late;
}

void TableMaterialization::Materialize(ScannerFilterSet &filters, optional_idx estimated_rows) {
	auto total_start = MaterializeClock::now();
	auto log_timing = LogEnabled();
	// Already-materialized CHUNK_GET leaves are set up in the constructor;
	// nothing to do here.
	if (materialized_) {
		return;
	}

	auto plan_copy = table_op_.Copy(context_);
	AddMaterializationRangePruning(*plan_copy);
	auto copy_end = MaterializeClock::now();
	if (log_timing) {
		LogMaterializePlan(*plan_copy, "before_rpt");
	}
	auto before_log_end = MaterializeClock::now();

	// Step 1: Push BF/Bitmap filters into a safely reachable LogicalGet whose
	// table_index matches a filter binding. Removes pushed entries from
	// the filter set; remaining entries stay and will be applied in-memory by
	// Scan() / Compact() after execution.
	TryInstallRPTIndexScan(context_, *plan_copy, filters.Entries(), estimated_rows);
	filters.Pushdown(*plan_copy);
	auto inject_end = MaterializeClock::now();

	// Step 2: Late materialization — only applies when the table operator
	// reduces to a single FILTER/PROJ/COMPARISON_JOIN chain over a single
	// LogicalGet. Other shapes (UNION, WINDOW, CROSS_PRODUCT, grouped AGG,
	// CHUNK_GET, multi-GET joins, …) skip this step entirely.
	auto leaf = enable_late_materialization_ && !required_bindings_.empty()
	                ? ScannerFilterSet::GetPushdownScan(*plan_copy) : nullptr;
	if (leaf && leaf->GetTable() && leaf->GetTable()->IsDuckTable() &&
	    leaf->virtual_columns.count(COLUMN_IDENTIFIER_ROW_ID)) {
		KeepFilterColumns(*plan_copy, *leaf, filters, required_bindings_);
	} else {
		leaf = nullptr;
	}

	if (leaf && ShouldDeferPayload(context_, *plan_copy, *leaf, required_bindings_, estimated_rows, log_timing)) {
		auto &get = *leaf;
		auto col_count_before = get.GetColumnIds().size();

		// Wrap in projection of required columns → RemoveUnusedColumns → unwrap
		plan_copy->ResolveOperatorTypes();
		auto bindings = plan_copy->GetColumnBindings();
		auto col_types = plan_copy->types;

		vector<ColumnBinding> original_keys;
		auto proj = make_uniq<LogicalProjection>(get.table_index, vector<unique_ptr<Expression>> {});
		for (auto &req : required_bindings_) {
			for (idx_t i = 0; i < bindings.size(); i++) {
				if (bindings[i] == req) {
					original_keys.push_back(req);
					proj->expressions.push_back(make_uniq<BoundColumnRefExpression>(col_types[i], bindings[i]));
					break;
				}
			}
		}

		proj->children.push_back(std::move(plan_copy));
		unique_ptr<LogicalOperator> wrapper = std::move(proj);

		RemoveUnusedColumns unused(optimizer_);
		unused.VisitOperator(wrapper);

		plan_copy = std::move(wrapper->children[0]);

		is_pruned_ = get.GetColumnIds().size() < col_count_before || plan_copy->GetColumnBindings() != bindings;
		if (is_pruned_ && ScannerFilterSet::GetPushdownScan(*plan_copy) == &get &&
		    get.virtual_columns.count(COLUMN_IDENTIFIER_ROW_ID) && get.GetTable()) {
			auto row_id = ExposeScanRowId(*plan_copy, get);
			output_bindings_ = plan_copy->GetColumnBindings();
			rowid_chunk_col_ = FindChunkCol(row_id);
			D_ASSERT(rowid_chunk_col_ != DConstants::INVALID_INDEX);
			if (LogEnabled()) {
				fprintf(stderr, "  [LateMat] %s: rowid at materialized output %lu\n",
				        get.GetTable()->name.c_str(), rowid_chunk_col_);
			}
		}
		if (is_pruned_) {
			// The optimizer rewrites references in our wrapper when it renumbers
			// columns. Publish original graph bindings at their new chunk positions.
			auto physical_bindings = plan_copy->GetColumnBindings();
			output_bindings_.assign(physical_bindings.size(), ColumnBinding());
			D_ASSERT(original_keys.size() == wrapper->expressions.size());
			for (idx_t i = 0; i < original_keys.size(); i++) {
				auto key = wrapper->expressions[i]->Cast<BoundColumnRefExpression>().Binding();
				auto pos = std::find(physical_bindings.begin(), physical_bindings.end(), key);
				if (pos == physical_bindings.end()) {
					throw InternalException("Late materialization lost a required transfer key");
				}
				output_bindings_[pos - physical_bindings.begin()] = original_keys[i];
			}
		}
	}

	plan_copy->ResolveOperatorTypes();
	if (!is_pruned_) {
		output_bindings_ = plan_copy->GetColumnBindings();
	}

	auto late_materialization_end = MaterializeClock::now();

	if (log_timing) {
		LogMaterializePlan(*plan_copy, "after_rpt");
	}
	auto after_log_end = MaterializeClock::now();

	// Step 3: Execute the (possibly modified) subtree.
	if (!ExecutePlan(std::move(plan_copy))) {
		return;
	}
	auto execute_plan_end = MaterializeClock::now();

	if (log_timing) {
		std::cerr << "      [Materialize] rows=" << (data_ ? data_->Count() : 0)
		          << " out_cols=" << (data_ ? data_->Types().size() : 0) << '\n';
	}
	auto result_log_end = MaterializeClock::now();

	// Filters that could not be pushed into a GET remain in-memory.
	filters.Resolve(output_bindings_);
	auto resolve_filters_end = MaterializeClock::now();
	if (log_timing) {
		std::cerr << "      [MaterializePhaseTiming] copy=" << MaterializeElapsedMs(total_start, copy_end)
		          << "ms log_before=" << MaterializeElapsedMs(copy_end, before_log_end)
		          << "ms inject_filters=" << MaterializeElapsedMs(before_log_end, inject_end)
		          << "ms late_materialization=" << MaterializeElapsedMs(inject_end, late_materialization_end)
		          << "ms log_after=" << MaterializeElapsedMs(late_materialization_end, after_log_end)
		          << "ms execute_plan=" << MaterializeElapsedMs(after_log_end, execute_plan_end)
		          << "ms result_log=" << MaterializeElapsedMs(execute_plan_end, result_log_end)
		          << "ms resolve_deferred=" << MaterializeElapsedMs(result_log_end, resolve_filters_end)
		          << "ms total=" << MaterializeElapsedMs(total_start, resolve_filters_end) << "ms\n";
	}
}

//===--------------------------------------------------------------------===//
// Column pruning
//===--------------------------------------------------------------------===//

void TableMaterialization::SetRequiredColumns(const column_binding_set_t &bindings) {
	required_bindings_ = bindings;
}

//===--------------------------------------------------------------------===//
// FindChunkCol — resolve ColumnBinding to chunk position via output_bindings_
//===--------------------------------------------------------------------===//

idx_t TableMaterialization::FindChunkCol(const ColumnBinding &binding) const {
	for (idx_t i = 0; i < output_bindings_.size(); i++) {
		if (output_bindings_[i] == binding) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// InitScanChunk
//===--------------------------------------------------------------------===//

void TableMaterialization::InitScanChunk(DataChunk &chunk) const {
	if (!data_) {
		return;
	}
	// When pending_expression_ narrows via projection_map, the caller-visible
	// chunk schema is narrower than the raw CDC. Initialize to table_op_ types
	// so FindChunkCol (which resolves against output_bindings_) stays correct.
	if (pending_expression_ && !pending_expression_->projection_map.empty()) {
		chunk.Initialize(BufferAllocator::Get(context_), table_op_.types);
	} else {
		data_->InitializeScanChunk(chunk);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

bool TableMaterialization::Scan(DataChunk &chunk) {
	if (!data_) {
		return false;
	}

	while (true) {
		chunk.Reset();

		if (pending_expression_) {
			// Scan into the wide scratch chunk, evaluate the composed filter
			// expression, then project/reference into the caller's narrow chunk.
			auto &pf = *pending_expression_;
			pf.scratch.Reset();
			if (!data_->Scan(scan_state_, pf.scratch) || pf.scratch.size() == 0) {
				return false;
			}

			idx_t count = pf.scratch.size();
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			bool sliced = false;
			if (pf.executor) {
				count = pf.executor->SelectExpression(pf.scratch, sel);
				sliced = count < pf.scratch.size();
			}
			if (count == 0) {
				continue;
			}

			chunk.SetCardinalityUnsafe(count);
			for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
				idx_t src = pf.projection_map.empty() ? i : pf.projection_map[i];
				if (sliced) {
					chunk.data[i].Slice(pf.scratch.data[src], sel, count);
				} else {
					chunk.data[i].Reference(pf.scratch.data[src]);
				}
			}
		} else {
			if (!data_->Scan(scan_state_, chunk) || chunk.size() == 0) {
				return false;
			}
		}

		return true;
	}
}

//===--------------------------------------------------------------------===//
// ResetScan / Count
//===--------------------------------------------------------------------===//

void TableMaterialization::ResetScan() {
	if (data_) {
		data_->InitializeScan(scan_state_);
	}
}

idx_t TableMaterialization::Count() const {
	return data_ ? data_->Count() : 0;
}

// A bitmap scan amortizes decoding over each visited row group. Count only
// distinct groups containing survivor row IDs, including partial groups.
static bool IsSparsePayloadFetch(DataTable &storage, ColumnDataCollection &data, idx_t rowid_column, bool log) {
	static constexpr idx_t SCAN_ROWS_PER_FETCH_ROW = 200; // At most 0.5% density.
	auto required_coverage = data.Count() * SCAN_ROWS_PER_FETCH_ROW;
	auto groups = storage.GetRowGroupCollection()->GetRowGroups();
	unordered_set<idx_t> seen;
	idx_t covered = 0, start = 0, end = 0;
	ColumnDataScanState scan;
	DataChunk input;
	data.InitializeScan(scan, vector<column_t> {rowid_column});
	data.InitializeScanChunk(scan, input);
	while (data.Scan(scan, input)) {
		auto &ids = input.data[0];
		ids.Flatten();
		auto rows = FlatVector::GetData<row_t>(ids);
		for (idx_t i = 0; i < input.size(); i++) {
			if (rows[i] < 0 || rows[i] >= MAX_ROW_ID) {
				return false;
			}
			auto id = NumericCast<idx_t>(rows[i]);
			if (id >= start && id < end) {
				continue;
			}
			auto lock = groups->Lock();
			idx_t index;
			if (!groups->TryGetSegmentIndex(lock, id, index)) {
				return false;
			}
			auto group = groups->GetSegmentByIndex(lock, NumericCast<int64_t>(index));
			start = group->GetRowStart();
			auto count = group->GetNode().count.load();
			end = start + count;
			if (seen.insert(index).second) {
				covered += count;
			}
		}
	}
	if (log) {
		std::cerr << "[PayloadFetchDensity] rows=" << data.Count() << " covered_rows=" << covered
		          << " fetch=" << (covered >= required_coverage) << '\n';
	}
	return covered >= required_coverage;
}

bool TableMaterialization::TryFetchDeferred(const std::function<bool(idx_t)> &reserve_bytes) {
	if (!is_pruned_ || !data_ || pending_expression_ || rowid_chunk_col_ == DConstants::INVALID_INDEX) {
		return false;
	}
	// Dense payloads are cheaper to recover through the row-id bitmap scan.
	// The caller has already compacted this table's final set of survivors.
	static constexpr idx_t MAX_PAYLOAD_FETCH_ROWS = 4096;
	if (data_->Count() > MAX_PAYLOAD_FETCH_ROWS) {
		if (LogEnabled()) {
			std::cerr << "[BatchPayloadFetchLimit] rows=" << data_->Count() << " limit=" << MAX_PAYLOAD_FETCH_ROWS
			          << '\n';
		}
		return false;
	}
	auto get = ScannerFilterSet::GetPushdownScan(table_op_);
	if (!get || !get->GetTable() || !get->GetTable()->IsDuckTable()) {
		return false;
	}
	auto &table = get->GetTable()->Cast<DuckTableEntry>();
	auto bindings = table_op_.GetColumnBindings();
	const auto &types = table_op_.types;
	if (types.size() != bindings.size()) {
		return false;
	}
	vector<StorageIndex> columns;
	vector<LogicalType> fetch_types;
	vector<idx_t> positions;
	const idx_t retained_width = data_->ColumnCount();
	for (idx_t i = 0; i < bindings.size(); i++) {
		auto pos = FindChunkCol(bindings[i]);
		if (pos < retained_width && pos != rowid_chunk_col_ && data_->Types()[pos] == types[i]) {
			positions.push_back(pos);
			continue;
		}
		const auto &binding = bindings[i];
		if (binding.table_index != get->table_index || binding.column_index >= get->GetColumnIds().size()) {
			return false;
		}
		const auto &column = get->GetColumnIds()[binding.column_index];
		if (column.IsVirtualColumn() || column.HasChildren() || column.HasType() ||
		    get->GetColumnType(column) != types[i]) {
			return false;
		}
		positions.push_back(retained_width + columns.size());
		columns.push_back(table.GetStorageIndex(column));
		fetch_types.push_back(types[i]);
	}
	if (columns.empty() || !IsSparsePayloadFetch(table.GetStorage(), *data_, rowid_chunk_col_, LogEnabled()) ||
	    !reserve_bytes(data_->AllocationSize())) {
		return false;
	}
	const auto start = MaterializeClock::now();
	auto full = make_uniq<ColumnDataCollection>(BufferAllocator::Get(context_), types);
	auto &transaction = DuckTransaction::Get(context_, table.catalog);
	DataChunk input, payload, output;
	data_->InitializeScanChunk(input);
	payload.Initialize(context_, fetch_types);
	output.Initialize(context_, types);
	ColumnDataScanState scan;
	data_->InitializeScan(scan);
	while (data_->Scan(scan, input)) {
		ColumnFetchState state; // Release pinned payload blocks between batches.
		auto &row_ids = input.data[rowid_chunk_col_];
		row_ids.Flatten();
		payload.Reset();
		table.GetStorage().Fetch(transaction, payload, columns, row_ids, input.size(), state);
		// Only positional alignment can pair fetched payloads with retained keys.
		if (payload.size() != input.size()) {
			return false;
		}
		output.Reset();
		for (idx_t i = 0; i < positions.size(); i++) {
			auto pos = positions[i];
			output.data[i].Reference(pos < retained_width ? input.data[pos] : payload.data[pos - retained_width]);
		}
		output.SetCardinalityUnsafe(input.size());
		if (!reserve_bytes(full->AllocationSize() + output.GetAllocationSize())) {
			return false;
		}
		full->Append(output);
	}
	if (!reserve_bytes(full->AllocationSize())) {
		return false;
	}
	if (LogEnabled()) {
		std::cerr << "[BatchPayloadFetch] table=" << table.name << " rows=" << full->Count()
		          << " ms=" << MaterializeElapsedMs(start, MaterializeClock::now()) << '\n';
	}
	output_bindings_ = std::move(bindings);
	is_pruned_ = false;
	rowid_chunk_col_ = DConstants::INVALID_INDEX;
	ReplaceData(std::move(full));
	return true;
}

void TableMaterialization::ReplaceData(unique_ptr<ColumnDataCollection> data) {
	D_ASSERT(data);
	data_ = shared_ptr<ColumnDataCollection>(data.release());
	pending_expression_.reset();
	materialized_ = true;
	ResetScan();
}

} // namespace duckdb
