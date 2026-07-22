#include "predicate_transfer/table_scanner.hpp"
#include "predicate_transfer/filter/table_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
namespace {

class ParallelCompactScanTask : public BaseExecutorTask {
public:
	using ScanFunction = std::function<void(idx_t, DataChunk &)>;

	ParallelCompactScanTask(TaskExecutor &executor, const ColumnDataCollection &collection,
	                        ColumnDataParallelScanState &scan_state, idx_t task_id, ScanFunction &function)
	    : BaseExecutorTask(executor), collection(collection), scan_state(scan_state), task_id(task_id),
	      function(function) {
	}

	void ExecuteTask() override {
		ColumnDataLocalScanState local_state;
		DataChunk chunk;
		collection.InitializeScanChunk(chunk);
		while (collection.Scan(scan_state, local_state, chunk)) {
			if (chunk.size() > 0) {
				function(task_id, chunk);
			}
		}
	}

	string TaskType() const override {
		return "RPTParallelCompactScan";
	}

private:
	const ColumnDataCollection &collection;
	ColumnDataParallelScanState &scan_state;
	idx_t task_id;
	ScanFunction &function;
};

static void ParallelCompactScan(ClientContext &context, const ColumnDataCollection &collection, idx_t task_count,
                                ParallelCompactScanTask::ScanFunction function) {
	ColumnDataParallelScanState scan_state;
	collection.InitializeScan(scan_state);
	TaskExecutor executor(context);
	for (idx_t task_id = 0; task_id < task_count; task_id++) {
		executor.ScheduleTask(make_uniq<ParallelCompactScanTask>(executor, collection, scan_state, task_id, function));
	}
	executor.WorkOnTasks();
}

} // namespace

idx_t RPTScanTaskCount(ClientContext &context, const ColumnDataCollection &collection) {
	static constexpr idx_t CHUNKS_PER_TASK = 8;
	auto tasks = MaxValue<idx_t>(collection.ChunkCount() / CHUNKS_PER_TASK, 1);
	return MinValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), tasks);
}

bool TableScanner::RptLogEnabled() const {
	if (ClientConfig::GetConfig(context_).enable_profiler) {
		return true;
	}
	Value v;
	return context_.TryGetCurrentSetting("rpt_log_transfer_steps", v) && v.GetValue<bool>();
}

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//

TableScanner::TableScanner(Optimizer &optimizer, ClientContext &context, LogicalOperator &table_op,
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
			for (idx_t i = 0; i < current_bindings.size(); i++) {
				if (current_bindings[i] == cref.Binding()) {
					expr = make_uniq<BoundReferenceExpression>(cref.GetAlias(), cref.GetReturnType(),
					                                           current_positions[i]);
					return;
				}
			}
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

	pending_expr_filter_ = make_uniq<PendingExprFilter>();
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
		pending_expr_filter_->executor = make_uniq<ExpressionExecutor>(context_);
		pending_expr_filter_->executor->AddExpression(*final_expr);
		pending_expr_filter_->exprs_holder.push_back(std::move(final_expr));
	}
	if (needs_projection) {
		pending_expr_filter_->projection_map = std::move(current_positions);
	}
	pending_expr_filter_->scratch.Initialize(Allocator::DefaultAllocator(), data_->Types());
}

//===--------------------------------------------------------------------===//
// FindChunkCol — resolve ColumnBinding to chunk position via output_bindings_
//===--------------------------------------------------------------------===//

idx_t TableScanner::FindChunkCol(const ColumnBinding &binding) const {
	for (idx_t i = 0; i < output_bindings_.size(); i++) {
		if (output_bindings_[i] == binding) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// Materialize — load data via Executor with filters preserved
//===--------------------------------------------------------------------===//

//! Helper: execute a plan via Executor and store the result in data_.
bool TableScanner::ExecutePlan(unique_ptr<LogicalOperator> plan_copy) {
	auto col_types = plan_copy->types;
	if (col_types.empty()) {
		return false;
	}

	PhysicalPlanGenerator generator(context_);
	auto physical_plan = generator.Plan(std::move(plan_copy));

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

	auto &client_data = ClientData::Get(context_);
	auto saved_profiler = client_data.profiler;
	client_data.profiler = make_shared_ptr<QueryProfiler>(context_);

	Executor executor(context_);
	auto collector = PhysicalResultCollector::GetResultCollector(context_, stmt_data);
	executor.Initialize(std::move(collector));

	while (executor.ExecuteTask() != PendingExecutionResult::EXECUTION_FINISHED) {
	}

	auto result = executor.GetResult();
	D_ASSERT(result);
	D_ASSERT(!result->HasError());
	auto &mat_result = result->Cast<MaterializedQueryResult>();
	data_ = shared_ptr<ColumnDataCollection>(mat_result.TakeCollection().release());
	if (!data_) {
		data_ = shared_ptr<ColumnDataCollection>(make_uniq<ColumnDataCollection>(context_, mat_result.types).release());
	}

	client_data.profiler = std::move(saved_profiler);

	materialized_ = true;
	data_->InitializeScan(scan_state_);
	return true;
}

//===--------------------------------------------------------------------===//
// Materialize — unified entry point for all plan shapes
//===--------------------------------------------------------------------===//
//
// Three-step pipeline applied to every table operator subtree:
//   1. Filter pushdown — DFS the subtree, push each pending BF/Bitmap filter
//      into the matching LogicalGet's table_filters (matching by table_index).
//      Pushed filters are removed from filters_; un-pushed ones (e.g. on
//      grouped-AGG output, on CHUNK_GET, on virtual columns) stay and will
//      be applied in-memory by Scan() / Compact() after execution.
//   2. Late materialization — only when the leaf is a single LogicalGet:
//      column-prune via RemoveUnusedColumns and inject rowid for late
//      materialization in the join graph.
//   3. Execute — run the (possibly modified) subtree via Executor, store
//      the result in data_, then resolve chunk_col for any remaining
//      in-memory filters.

void TableScanner::Materialize() {
	// Already-materialized CHUNK_GET leaves are set up in the constructor;
	// nothing to do here.
	if (materialized_) {
		return;
	}

	auto plan_copy = table_op_.Copy(context_);

	// Step 1: Push BF/Bitmap filters into any LogicalGet in the subtree whose
	// table_index matches a filter binding. Removes pushed entries from
	// filters_; remaining entries stay and will be applied in-memory by
	// Scan() / Compact() after execution.
	InjectTableFilters(*plan_copy);

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

			if (RptLogEnabled()) {
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
		if (RptLogEnabled()) {
			fprintf(stderr, "  [LateMat] %s: subtree narrowed via projection_map (%lu -> %lu bindings)\n",
			        late_mat_get->GetTable() ? late_mat_get->GetTable()->name.c_str() : "?", pre_prune_bindings.size(),
			        output_bindings_.size());
		}
	}

	if (late_mat_get && rowid_col_ids_pos != DConstants::INVALID_INDEX) {
		ColumnBinding rowid_binding(late_mat_get->table_index, ProjectionIndex(rowid_col_ids_pos));
		rowid_chunk_col_ = FindChunkCol(rowid_binding);
	}

	// Step 3: Execute the (possibly modified) subtree.
	if (!ExecutePlan(std::move(plan_copy))) {
		return;
	}

	// Resolve chunk_col for any filters that did NOT get pushed into a GET.
	// They will be applied in-memory by Scan() / Compact().
	for (auto &entry : filters_) {
		for (idx_t i = 0; i < entry.bindings.size(); ++i) {
			entry.chunk_cols[i] = FindChunkCol(entry.bindings[i]);
		}
	}
}

//===--------------------------------------------------------------------===//
// Column pruning
//===--------------------------------------------------------------------===//

void TableScanner::SetRequiredColumns(const unordered_set<ColumnBinding, ColumnBindingHashFunc> &bindings) {
	required_bindings_ = bindings;
}

//===--------------------------------------------------------------------===//
// Filter management
//===--------------------------------------------------------------------===//

void TableScanner::AddFilter(ColumnBinding binding, shared_ptr<RPTFilter> filter, size_t identity_hash) {
	AddFilter(vector<ColumnBinding> {binding}, std::move(filter), identity_hash);
}

void TableScanner::AddFilter(const vector<ColumnBinding> &bindings, shared_ptr<RPTFilter> filter,
                             size_t identity_hash) {
	FilterEntry entry;
	entry.filter = std::move(filter);
	entry.bindings = bindings;
	entry.identity_hash = identity_hash;
	entry.chunk_cols.reserve(bindings.size());
	for (const auto &bind : entry.bindings) {
		entry.chunk_cols.push_back(materialized_ ? FindChunkCol(bind) : DConstants::INVALID_INDEX);
	}
	filters_.push_back(std::move(entry));
}

size_t TableScanner::FilterStateFingerprint() const {
	size_t fp = 0;
	for (auto &entry : filters_) {
		size_t h = entry.identity_hash;
		for (auto &b : entry.bindings) {
			h = h * 1315423911u + (std::hash<idx_t> {}(b.table_index.index) * 3 + std::hash<idx_t> {}(b.column_index));
		}
		fp ^= h; // XOR: attach order must not matter across runs
	}
	return fp;
}

//===--------------------------------------------------------------------===//
// InjectTableFilters — DFS the subtree, push filters into matching GETs
//===--------------------------------------------------------------------===//
//
// For each pending filter in filters_, find the LogicalGet in the subtree
// whose table_index matches the filter's binding and push the filter into
// that GET's table_filters. Filters that match are removed from filters_;
// filters that don't match (binding refers to a CHUNK_GET, a grouped-AGG
// output, a virtual column, or has no GET in the subtree) stay in filters_
// and will be applied in-memory by Scan() / Compact() after execution.

static void CollectGetsByTableIndex(LogicalOperator &op, unordered_map<idx_t, LogicalGet *> &out) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		out.emplace(get.table_index.index, &get);
	}
	for (auto &child : op.children) {
		CollectGetsByTableIndex(*child, out);
	}
}

void TableScanner::InjectTableFilters(LogicalOperator &plan_copy) {
	if (filters_.empty()) {
		return;
	}

	unordered_map<idx_t, LogicalGet *> gets_by_table_index;
	CollectGetsByTableIndex(plan_copy, gets_by_table_index);
	if (gets_by_table_index.empty()) {
		return;
	}

	auto pushed_end = std::remove_if(filters_.begin(), filters_.end(), [&](FilterEntry &entry) {
		if (!entry.filter) {
			return false;
		}
		// Composite-key filters cannot be pushed down as per-column TableFilters.
		if (entry.bindings.size() != 1) {
			return false;
		}
		auto &binding = entry.bindings.front();

		auto get_it = gets_by_table_index.find(binding.table_index.index);
		if (get_it == gets_by_table_index.end()) {
			return false;
		}
		auto &get = *get_it->second;
		auto &column_ids = get.GetMutableColumnIds();

		idx_t pos = binding.column_index;
		if (pos >= column_ids.size() || column_ids[pos].IsVirtualColumn()) {
			return false;
		}
		auto col_id = column_ids[pos].GetPrimaryIndex();
		auto key_type = (col_id < get.returned_types.size()) ? get.returned_types[col_id] : LogicalType::BIGINT;
		get.table_filters.PushFilter(ProjectionIndex(pos), RPTTableFilter::MakeOptional(entry.filter, key_type));
		return true;
	});
	filters_.erase(pushed_end, filters_.end());
}

//===--------------------------------------------------------------------===//
// InitScanChunk
//===--------------------------------------------------------------------===//

void TableScanner::InitScanChunk(DataChunk &chunk) const {
	if (!data_) {
		return;
	}
	// When pending_expr_filter_ narrows via projection_map, the caller-visible
	// chunk schema is narrower than the raw CDC. Initialize to table_op_ types
	// so FindChunkCol (which resolves against output_bindings_) stays correct.
	if (pending_expr_filter_ && !pending_expr_filter_->projection_map.empty()) {
		chunk.Initialize(Allocator::DefaultAllocator(), table_op_.types);
	} else {
		data_->InitializeScanChunk(chunk);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

bool TableScanner::Scan(DataChunk &chunk) {
	if (!data_) {
		return false;
	}

	while (true) {
		chunk.Reset();

		if (pending_expr_filter_) {
			// Scan into the wide scratch chunk, evaluate the composed filter
			// expression, then project/reference into the caller's narrow chunk.
			auto &pf = *pending_expr_filter_;
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

		if (!filters_.empty()) {
			ApplyFilters(chunk);
		}
		if (chunk.size() > 0) {
			return true;
		}
	}
}

//===--------------------------------------------------------------------===//
// ResetScan / Count
//===--------------------------------------------------------------------===//

void TableScanner::ResetScan() {
	if (data_) {
		data_->InitializeScan(scan_state_);
	}
}

idx_t TableScanner::Count() const {
	return data_ ? data_->Count() : 0;
}

//===--------------------------------------------------------------------===//
// ApplyFilters — BF filters on materialized data
//===--------------------------------------------------------------------===//

void TableScanner::ApplyFilters(DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	for (auto &entry : filters_) {
		if (chunk.size() == 0) {
			break;
		}
		if (!entry.filter || entry.chunk_cols.empty()) {
			continue;
		}
		const idx_t column_count = chunk.ColumnCount();
		if (std::any_of(entry.chunk_cols.begin(), entry.chunk_cols.end(),
		                [&](idx_t cc) { return cc >= column_count; })) {
			continue;
		}

		size_t count = chunk.size();
		entry.filter->Lookup(chunk, entry.chunk_cols, sel, count);

		if (count < chunk.size()) {
			chunk.Slice(sel, count);
			chunk.Flatten();
		}
	}
}

//===--------------------------------------------------------------------===//
// Compact — Evaluate deferred filters into a new flat collection
//===--------------------------------------------------------------------===//

TableScanner::CompactResult TableScanner::Compact(const vector<StatsRequest> &stats_requests) {
	CompactResult result;
	result.column_stats.resize(stats_requests.size());
	if (!data_) {
		return result;
	}
	if (filters_.empty() && !pending_expr_filter_ && stats_requests.empty()) {
		result.row_count = Count();
		return result;
	}

	ResetScan();
	DataChunk chunk;
	InitScanChunk(chunk);

	vector<idx_t> collected_request_indices;
	for (idx_t request_idx = 0; request_idx < stats_requests.size(); request_idx++) {
		auto &request = stats_requests[request_idx];
		if (request.chunk_col < chunk.ColumnCount() && request.type.IsIntegral() &&
		    request.type != LogicalType::HUGEINT && request.type != LogicalType::UHUGEINT) {
			collected_request_indices.push_back(request_idx);
		}
	}
	bool collect_min_max = !collected_request_indices.empty();

	vector<unique_ptr<Expression>> min_max_aggregates;
	unique_ptr<GlobalUngroupedAggregateState> global_aggregate_state;
	unique_ptr<LocalUngroupedAggregateState> local_aggregate_state;
	if (collect_min_max) {
		for (auto request_idx : collected_request_indices) {
			for (auto &aggr : {MinFunction::GetFunction(), MaxFunction::GetFunction()}) {
				FunctionBinder function_binder(context_);
				vector<unique_ptr<Expression>> aggr_children;
				aggr_children.push_back(make_uniq<BoundReferenceExpression>(stats_requests[request_idx].type,
				                                                            stats_requests[request_idx].chunk_col));
				auto aggr_expr = function_binder.BindAggregateFunction(aggr, std::move(aggr_children), nullptr,
				                                                       AggregateType::NON_DISTINCT);
				min_max_aggregates.push_back(std::move(aggr_expr));
			}
		}

		global_aggregate_state =
		    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context_), min_max_aggregates);
		local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*global_aggregate_state);
	}

	unique_ptr<ColumnDataCollection> new_data;
	if (!filters_.empty() || pending_expr_filter_) {
		// New CDC matches the (possibly narrowed) chunk schema that Scan emits.
		new_data = make_uniq<ColumnDataCollection>(context_, chunk.GetTypes());
	}

	auto task_count = RPTScanTaskCount(context_, *data_);
	if (task_count <= 1) {
		while (Scan(chunk)) {
			if (collect_min_max) {
				for (idx_t stats_idx = 0; stats_idx < collected_request_indices.size(); stats_idx++) {
					auto chunk_col = stats_requests[collected_request_indices[stats_idx]].chunk_col;
					local_aggregate_state->Sink(chunk, chunk_col, stats_idx * 2, chunk.size());
					local_aggregate_state->Sink(chunk, chunk_col, stats_idx * 2 + 1, chunk.size());
				}
			}
			if (new_data) {
				// Append flattens the chunk natively, preserving only valid rows.
				new_data->Append(chunk);
			}
		}
	} else {
		vector<unique_ptr<ExpressionExecutor>> local_expression_executors;
		vector<unique_ptr<DataChunk>> local_projection_chunks;
		if (pending_expr_filter_) {
			local_expression_executors.reserve(task_count);
			local_projection_chunks.reserve(task_count);
			for (idx_t task_id = 0; task_id < task_count; task_id++) {
				unique_ptr<ExpressionExecutor> executor;
				if (!pending_expr_filter_->exprs_holder.empty()) {
					executor = make_uniq<ExpressionExecutor>(context_, *pending_expr_filter_->exprs_holder.front());
				}
				local_expression_executors.push_back(std::move(executor));

				unique_ptr<DataChunk> projection_chunk;
				if (!pending_expr_filter_->projection_map.empty()) {
					projection_chunk = make_uniq<DataChunk>();
					projection_chunk->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
				}
				local_projection_chunks.push_back(std::move(projection_chunk));
			}
		}

		vector<unique_ptr<LocalUngroupedAggregateState>> local_aggregate_states;
		if (collect_min_max) {
			local_aggregate_state.reset();
			local_aggregate_states.reserve(task_count);
			for (idx_t task_id = 0; task_id < task_count; task_id++) {
				local_aggregate_states.push_back(make_uniq<LocalUngroupedAggregateState>(*global_aggregate_state));
			}
		}

		vector<unique_ptr<ColumnDataCollection>> local_collections;
		if (new_data) {
			new_data.reset();
			local_collections.reserve(task_count);
			for (idx_t task_id = 0; task_id < task_count; task_id++) {
				local_collections.push_back(make_uniq<ColumnDataCollection>(context_, chunk.GetTypes()));
			}
		}

		ParallelCompactScan(context_, *data_, task_count, [&](idx_t task_id, DataChunk &scan_chunk) {
			DataChunk *filtered_chunk = &scan_chunk;
			if (pending_expr_filter_) {
				auto &pending = *pending_expr_filter_;
				auto original_count = scan_chunk.size();
				auto selected_count = original_count;
				SelectionVector selected(STANDARD_VECTOR_SIZE);
				if (local_expression_executors[task_id]) {
					selected_count = local_expression_executors[task_id]->SelectExpression(scan_chunk, selected);
				}
				if (selected_count == 0) {
					return;
				}

				if (!pending.projection_map.empty()) {
					auto &projected = *local_projection_chunks[task_id];
					projected.Reset();
					projected.SetCardinalityUnsafe(selected_count);
					for (idx_t col_idx = 0; col_idx < projected.ColumnCount(); col_idx++) {
						auto source_idx = pending.projection_map[col_idx];
						if (selected_count < original_count) {
							projected.data[col_idx].Slice(scan_chunk.data[source_idx], selected, selected_count);
						} else {
							projected.data[col_idx].Reference(scan_chunk.data[source_idx]);
						}
					}
					filtered_chunk = &projected;
				} else if (selected_count < original_count) {
					scan_chunk.Slice(selected, selected_count);
					scan_chunk.Flatten();
				}
			}

			if (!filters_.empty()) {
				ApplyFilters(*filtered_chunk);
			}
			if (filtered_chunk->size() == 0) {
				return;
			}
			if (collect_min_max) {
				auto &aggregate_state = *local_aggregate_states[task_id];
				for (idx_t stats_idx = 0; stats_idx < collected_request_indices.size(); stats_idx++) {
					auto chunk_col = stats_requests[collected_request_indices[stats_idx]].chunk_col;
					aggregate_state.Sink(*filtered_chunk, chunk_col, stats_idx * 2, filtered_chunk->size());
					aggregate_state.Sink(*filtered_chunk, chunk_col, stats_idx * 2 + 1, filtered_chunk->size());
				}
			}
			if (!local_collections.empty()) {
				local_collections[task_id]->Append(*filtered_chunk);
			}
		});

		for (auto &aggregate_state : local_aggregate_states) {
			global_aggregate_state->Combine(*aggregate_state);
		}
		if (!local_collections.empty()) {
			new_data = make_uniq<ColumnDataCollection>(context_, chunk.GetTypes());
			for (auto &local_collection : local_collections) {
				new_data->Combine(*local_collection);
			}
		}
	}

	if (collect_min_max) {
		if (local_aggregate_state) {
			global_aggregate_state->Combine(*local_aggregate_state);
		}
		vector<LogicalType> min_max_types;
		for (auto &aggr_expr : min_max_aggregates) {
			min_max_types.push_back(aggr_expr->GetReturnType());
		}
		DataChunk final_min_max;
		final_min_max.Initialize(Allocator::DefaultAllocator(), min_max_types);
		global_aggregate_state->Finalize(final_min_max);

		for (idx_t stats_idx = 0; stats_idx < collected_request_indices.size(); stats_idx++) {
			auto min_val = final_min_max.data[stats_idx * 2].GetValue(0);
			auto max_val = final_min_max.data[stats_idx * 2 + 1].GetValue(0);
			if (!min_val.IsNull() && !max_val.IsNull() && min_val.type().IsIntegral() &&
			    max_val.type() == min_val.type()) {
				auto &stats = result.column_stats[collected_request_indices[stats_idx]];
				stats.has_min_max = true;
				stats.observed_min = static_cast<int64_t>(IntegralValue::Get(min_val).lower);
				stats.observed_max = static_cast<int64_t>(IntegralValue::Get(max_val).lower);
			}
		}
	}

	if (new_data) {
		data_ = shared_ptr<ColumnDataCollection>(new_data.release());
		filters_.clear();
		// pending_expr_filter_ has been materialized into data_; drop it so
		// subsequent Scan() sees the narrow schema directly.
		pending_expr_filter_.reset();
	}
	result.row_count = Count();
	ResetScan();
	return result;
}

} // namespace duckdb
