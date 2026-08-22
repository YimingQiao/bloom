#include "predicate_transfer/table_scanner/materialization.hpp"
#include "predicate_transfer/rpt_result_collector.hpp"
#include "predicate_transfer/table_scanner/filter_set.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>

namespace duckdb {

using MaterializeClock = std::chrono::steady_clock;

static double MaterializeElapsedMs(MaterializeClock::time_point begin, MaterializeClock::time_point end) {
	return std::chrono::duration<double, std::milli>(end - begin).count();
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
	stmt_data.memory_type = QueryResultMemoryType::IN_MEMORY;
	stmt_data.output_type = QueryResultOutputType::FORCE_MATERIALIZED;

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

	while (executor.ExecuteTask() != PendingExecutionResult::EXECUTION_FINISHED) {
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
	auto &mat_result = result->Cast<MaterializedQueryResult>();
	data_ = shared_ptr<ColumnDataCollection>(mat_result.TakeCollection().release());
	if (!data_) {
		data_ = shared_ptr<ColumnDataCollection>(
		    make_uniq<ColumnDataCollection>(BufferAllocator::Get(context_), mat_result.types).release());
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
//   1. Filter pushdown — DFS the subtree, push each pending BF/Bitmap filter
//      into the matching LogicalGet's table_filters (matching by table_index).
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

void TableMaterialization::Materialize(ScannerFilterSet &filters) {
	auto total_start = MaterializeClock::now();
	auto log_timing = LogEnabled();
	// Already-materialized CHUNK_GET leaves are set up in the constructor;
	// nothing to do here.
	if (materialized_) {
		return;
	}

	auto plan_copy = table_op_.Copy(context_);
	auto copy_end = MaterializeClock::now();
	if (log_timing) {
		LogMaterializePlan(*plan_copy, "before_rpt");
	}
	auto before_log_end = MaterializeClock::now();

	// Step 1: Push BF/Bitmap filters into any LogicalGet in the subtree whose
	// table_index matches a filter binding. Removes pushed entries from
	// the filter set; remaining entries stay and will be applied in-memory by
	// Scan() / Compact() after execution.
	filters.Pushdown(*plan_copy);
	auto inject_end = MaterializeClock::now();

	// Step 2: Late materialization — only applies when the table operator
	// reduces to a single FILTER/PROJ/COMPARISON_JOIN chain over a single
	// LogicalGet. Other shapes (UNION, WINDOW, CROSS_PRODUCT, grouped AGG,
	// CHUNK_GET, multi-GET joins, …) skip this step entirely.
	LogicalOperator *leaf = plan_copy.get();
	while ((leaf->type == LogicalOperatorType::LOGICAL_FILTER ||
	        leaf->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	        leaf->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) &&
	       !leaf->children.empty()) {
		leaf = leaf->children[0].get();
	}

	idx_t rowid_col_ids_pos = DConstants::INVALID_INDEX;
	LogicalGet *late_mat_get = nullptr;
	vector<ColumnBinding> pre_prune_bindings;
	if (leaf->type == LogicalOperatorType::LOGICAL_GET && enable_late_materialization_ && !required_bindings_.empty()) {
		auto &get = leaf->Cast<LogicalGet>();
		late_mat_get = &get;
		auto col_count_before = get.GetColumnIds().size();

		// Wrap in projection of required columns → RemoveUnusedColumns → unwrap
		plan_copy->ResolveOperatorTypes();
		auto bindings = plan_copy->GetColumnBindings();
		pre_prune_bindings = bindings;
		auto col_types = plan_copy->types;

		auto proj = make_uniq<LogicalProjection>(get.table_index, vector<unique_ptr<Expression>> {});
		for (auto &req : required_bindings_) {
			for (idx_t i = 0; i < bindings.size(); i++) {
				if (bindings[i] == req) {
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

		auto &new_col_ids = get.GetMutableColumnIds();
		if (new_col_ids.size() < col_count_before) {
			is_pruned_ = true;
			new_col_ids.push_back(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID));
			rowid_col_ids_pos = new_col_ids.size() - 1;

			// Walk intermediate PROJECTIONs to add rowid expression
			LogicalOperator *cur = plan_copy.get();
			while (cur != &get) {
				if (cur->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &p = cur->Cast<LogicalProjection>();
					ColumnBinding rowid_bind(get.table_index, ProjectionIndex(rowid_col_ids_pos));
					p.expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::ROW_TYPE, rowid_bind));
				}
				cur = cur->children[0].get();
			}

			if (LogEnabled()) {
				fprintf(stderr, "  [LateMat] %s: pruned to %lu/%lu cols (+rowid)\n",
				        late_mat_get->GetTable() ? late_mat_get->GetTable()->name.c_str() : "?", new_col_ids.size(),
				        col_count_before);
			}
		}
	}

	plan_copy->ResolveOperatorTypes();
	output_bindings_ = plan_copy->GetColumnBindings();

	// RemoveUnusedColumns can also narrow the subtree through FILTER
	// projection_maps without shrinking the GET's column_ids; the column-count
	// check above misses that. Whenever the materialized output no longer
	// matches the original subtree's bindings, the data cannot back a
	// MemoryScan rewrite — mark pruned so GetTableResult takes DefaultScan.
	if (late_mat_get && !is_pruned_ && output_bindings_ != pre_prune_bindings) {
		is_pruned_ = true;
		if (LogEnabled()) {
			fprintf(stderr, "  [LateMat] %s: subtree narrowed via projection_map (%lu -> %lu bindings)\n",
			        late_mat_get->GetTable() ? late_mat_get->GetTable()->name.c_str() : "?", pre_prune_bindings.size(),
			        output_bindings_.size());
		}
	}

	if (late_mat_get && rowid_col_ids_pos != DConstants::INVALID_INDEX) {
		ColumnBinding rowid_binding(late_mat_get->table_index, ProjectionIndex(rowid_col_ids_pos));
		rowid_chunk_col_ = FindChunkCol(rowid_binding);
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

void TableMaterialization::ReplaceData(unique_ptr<ColumnDataCollection> data) {
	D_ASSERT(data);
	data_ = shared_ptr<ColumnDataCollection>(data.release());
	pending_expression_.reset();
	materialized_ = true;
	ResetScan();
}

} // namespace duckdb
