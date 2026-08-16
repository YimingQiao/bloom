#include "predicate_transfer/cardinality_estimation/sampling_estimator/table_sample_manager.hpp"
#include "predicate_transfer/cardinality_estimation/instant_sampler/instant_sampler.hpp"
#include "predicate_transfer/cardinality_estimation/prepared_sampler.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"

#include <chrono>
#include <iostream>
#include <random>

namespace duckdb {

bool TableSampleManager::LogEnabled() const {
	Value setting;
	return context_.TryGetCurrentSetting("rpt_log_transfer_steps", setting) && setting.GetValue<bool>();
}

//! True when a logical column reference could not be mapped to the sample
//! layout. Such a predicate cannot run in a bare ExpressionExecutor, so sample
//! evaluation conservatively leaves it unapplied.
static bool HasUnboundColumnRef(const Expression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		if (HasUnboundColumnRef(child)) {
			found = true;
		}
	});
	return found;
}

static void RewriteRefsToPositions(unique_ptr<Expression> &expression, const vector<ColumnBinding> &bindings,
                                   const vector<idx_t> &positions) {
	if (expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column_ref = expression->Cast<BoundColumnRefExpression>();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == column_ref.Binding()) {
				expression = make_uniq<BoundReferenceExpression>(column_ref.GetAlias(), column_ref.GetReturnType(),
				                                                 positions[i]);
				return;
			}
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expression, [&](unique_ptr<Expression> &child) { RewriteRefsToPositions(child, bindings, positions); });
}

//! Optional table filters are scan markers. Generic expression evaluation must
//! execute the predicate stored in their bind data instead of the wrapper.
static unique_ptr<Expression> UnwrapOptionalFilterExpressions(const Expression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expression.Cast<BoundFunctionExpression>();
		if (function.Function().GetName() == OptionalFilterScalarFun::NAME && function.BindInfo()) {
			auto &data = function.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? UnwrapOptionalFilterExpressions(*data.child_filter_expr)
			                              : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
		if (function.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && function.BindInfo()) {
			auto &data = function.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? UnwrapOptionalFilterExpressions(*data.child_filter_expr)
			                              : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
	}
	auto result = expression.Copy();
	ExpressionIterator::EnumerateChildren(
	    *result, [&](unique_ptr<Expression> &child) { child = UnwrapOptionalFilterExpressions(*child); });
	return result;
}

unique_ptr<Expression> TableSampleManager::BuildLocalPredicate(const Entry &sample, const LogicalOperator &op,
                                                               const vector<LogicalType> &sample_types,
                                                               idx_t &predicate_count, bool emit_log) const {
	vector<idx_t> narrow_positions;
	narrow_positions.reserve(sample.output_bindings.size());
	for (idx_t i = 0; i < sample.output_bindings.size(); i++) {
		narrow_positions.push_back(i);
	}

	vector<unique_ptr<Expression>> predicates;
	const LogicalGet *get = nullptr;
	const LogicalOperator *node = &op;
	if (emit_log) {
		std::cerr << "  [RPT-Sample] op=" << static_cast<int>(op.type) << " sample_rows=" << sample.sampled_rows
		          << " needed_storage_cols=(";
		for (idx_t i = 0; i < sample.needed_storage_columns.size(); i++) {
			if (i > 0) {
				std::cerr << ",";
			}
			std::cerr << sample.needed_storage_columns[i] << "->" << sample.output_bindings[i].ToString();
		}
		std::cerr << ")" << '\n';
	}
	while (node) {
		if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
			for (auto &expression : node->Cast<LogicalFilter>().expressions) {
				auto cloned = expression->Copy();
				auto original = cloned->ToString();
				RewriteRefsToPositions(cloned, sample.output_bindings, narrow_positions);
				bool unbound = HasUnboundColumnRef(*cloned);
				if (emit_log) {
					std::cerr << "    [LogicalFilter] " << original << " -> " << cloned->ToString()
					          << (unbound ? " SKIPPED(unbound)" : " APPLIED") << '\n';
				}
				if (!unbound) {
					predicates.push_back(std::move(cloned));
				}
			}
		} else if (node->type == LogicalOperatorType::LOGICAL_GET) {
			get = &node->Cast<LogicalGet>();
			break;
		}
		if (node->children.empty()) {
			break;
		}
		node = node->children[0].get();
	}

	if (get) {
		if (emit_log) {
			std::cerr << "    [Get] table_index=" << get->table_index.index
			          << " projected_cols=" << get->GetColumnIds().size()
			          << " table_filters=" << get->table_filters.FilterCount() << '\n';
		}
		for (auto &filter_entry : get->table_filters) {
			auto projection_column = filter_entry.GetIndex().GetIndex();
			if (projection_column >= get->GetColumnIds().size() ||
			    get->GetColumnIds()[projection_column].IsVirtualColumn()) {
				if (emit_log) {
					std::cerr << "      [TableFilter] projection=" << projection_column
					          << " SKIPPED(out-of-range/virtual)" << '\n';
				}
				continue;
			}
			idx_t storage_column = get->GetColumnIds()[projection_column].GetPrimaryIndex();
			idx_t chunk_column = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < sample.needed_storage_columns.size(); i++) {
				if (sample.needed_storage_columns[i] == storage_column) {
					chunk_column = i;
					break;
				}
			}
			if (chunk_column == DConstants::INVALID_INDEX || chunk_column >= sample_types.size()) {
				if (emit_log) {
					std::cerr << "      [TableFilter] projection=" << projection_column << " storage=" << storage_column
					          << " SKIPPED(not-in-sample)" << '\n';
				}
				continue;
			}
			BoundReferenceExpression column_ref(sample_types[chunk_column], chunk_column);
			auto &expression_filter = filter_entry.Filter().Cast<ExpressionFilter>();
			auto predicate = UnwrapOptionalFilterExpressions(*expression_filter.expr);
			ExpressionFilter::ReplaceExpressionRecursive(predicate, column_ref);
			if (emit_log) {
				std::cerr << "      [TableFilter] projection=" << projection_column << " storage=" << storage_column
				          << " sample=" << chunk_column << " filter=" << expression_filter.DebugToString()
				          << " expr=" << predicate->ToString() << " APPLIED" << '\n';
			}
			predicates.push_back(std::move(predicate));
		}
	}

	predicate_count = predicates.size();
	if (predicates.empty()) {
		return nullptr;
	}
	if (predicates.size() == 1) {
		return std::move(predicates[0]);
	}
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &predicate : predicates) {
		conjunction->GetChildrenMutable().push_back(std::move(predicate));
	}
	return std::move(conjunction);
}

void TableSampleManager::EnsureLocalFilter(Entry &sample, const LogicalOperator &op) {
	if (sample.local_filter_evaluated) {
		return;
	}
	auto evaluate_started = std::chrono::steady_clock::now();
	sample.local_filter_evaluated = true;

	// With no sampleable column (for example count(*) or a virtual-only
	// predicate), preserve the raw row count as a conservative upper bound.
	if (sample.sample_column_positions.empty()) {
		sample.locally_filtered = nullptr;
		sample.local_survivors = sample.sampled_rows;
		if (LogEnabled()) {
			auto elapsed =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - evaluate_started).count();
			std::cerr << "  [RPT-SampleEvaluate] table=" << sample.table_name
			          << " phase=local rows=" << sample.sampled_rows << " survivors=" << sample.local_survivors
			          << " elapsed=" << elapsed << "ms" << '\n';
		}
		return;
	}

	D_ASSERT(sample.sample);
	const auto &full_types = sample.sample->Types();
	vector<LogicalType> sample_types;
	sample_types.reserve(sample.sample_column_positions.size());
	for (auto sample_position : sample.sample_column_positions) {
		sample_types.push_back(full_types[sample_position]);
	}

	const bool log = LogEnabled();
	idx_t predicate_count = 0;
	auto final_expression = BuildLocalPredicate(sample, op, sample_types, predicate_count, log);
	unique_ptr<ExpressionExecutor> expression_executor;
	if (final_expression) {
		expression_executor = make_uniq<ExpressionExecutor>(context_);
		expression_executor->AddExpression(*final_expression);
	}

	auto locally_filtered = make_shared_ptr<ColumnDataCollection>(context_, sample_types);
	ColumnDataScanState scan_state;
	sample.sample->InitializeScan(scan_state, sample.sample_column_positions);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), sample_types);
	SelectionVector selection(STANDARD_VECTOR_SIZE);
	idx_t survivors = 0;
	while (true) {
		chunk.Reset();
		if (!sample.sample->Scan(scan_state, chunk) || chunk.size() == 0) {
			break;
		}
		idx_t count = chunk.size();
		if (expression_executor) {
			count = expression_executor->SelectExpression(chunk, selection);
			if (count == 0) {
				continue;
			}
			if (count < chunk.size()) {
				chunk.Slice(selection, count);
				chunk.Flatten();
			}
		}
		locally_filtered->Append(chunk);
		survivors += count;
	}
	sample.locally_filtered = std::move(locally_filtered);
	sample.local_survivors = survivors;
	if (log) {
		auto elapsed =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - evaluate_started).count();
		std::cerr << "    [RPT-Sample] predicates=" << predicate_count << " survivors=" << survivors << "/"
		          << sample.sampled_rows << '\n';
		std::cerr << "  [RPT-SampleEvaluate] table=" << sample.table_name << " phase=local rows=" << sample.sampled_rows
		          << " survivors=" << survivors << " elapsed=" << elapsed << "ms" << '\n';
	}
}

static idx_t CeilDivide(idx_t value, idx_t divisor) {
	D_ASSERT(divisor > 0);
	return value / divisor + (value % divisor != 0);
}

// ---------------------------------------------------------------------------
static const char *InstantSampleSourceName(InstantSampleSource source) {
	switch (source) {
	case InstantSampleSource::NATIVE_SCATTERED:
		return "instant_native_scattered";
	case InstantSampleSource::NATIVE_BLOCK:
		return "instant_native_block";
	case InstantSampleSource::PARQUET:
		return "instant_parquet";
	case InstantSampleSource::UNKNOWN:
		break;
	}
	D_ASSERT(false);
	return "instant_unknown";
}

void TableSampleManager::LogInstantSample(const Entry &sample, const InstantSampleResult &result, idx_t predicate_count,
                                          uint64_t table_seed) const {
	if (!LogEnabled()) {
		return;
	}
	idx_t target_accesses;
	idx_t rows_per_access;
	switch (result.source) {
	case InstantSampleSource::NATIVE_SCATTERED:
		target_accesses = config_.instant_access_points;
		rows_per_access = config_.instant_rows_per_access;
		break;
	case InstantSampleSource::NATIVE_BLOCK:
		target_accesses = config_.instant_block_windows;
		rows_per_access = STANDARD_VECTOR_SIZE;
		break;
	case InstantSampleSource::PARQUET:
		target_accesses = config_.instant_parquet_row_groups;
		rows_per_access = CeilDivide(config_.target_rows, MaxValue<idx_t>(result.selected_row_groups, 1));
		break;
	case InstantSampleSource::UNKNOWN:
		D_ASSERT(false);
		target_accesses = 0;
		rows_per_access = 0;
		break;
	}
	auto source = InstantSampleSourceName(result.source);
	std::cerr << "  [RPT-SampleBuild] table=" << sample.table_name << " mode=instant source=" << source
	          << " rows=" << sample.sampled_rows << "/" << sample.total_rows
	          << " accesses=" << sample.sample_access_points << "/" << target_accesses
	          << " rows_per_access=" << rows_per_access << " seed=" << config_.seed << " effective_seed=" << table_seed
	          << " metadata=" << result.metadata_ms << "ms scan=" << result.scan_ms << "ms";
	if (result.total_row_groups > 0) {
		std::cerr << " row_groups=" << result.selected_row_groups << "/" << result.total_row_groups
		          << " candidate_rows=" << result.candidate_rows;
	}
	std::cerr << '\n';
	std::cerr << "    [RPT-Sample] predicates=" << predicate_count << " survivors=" << sample.local_survivors << "/"
	          << sample.sampled_rows << '\n';
	std::cerr << "  [RPT-SamplePhysical] table=" << sample.table_name;
	if (result.source != InstantSampleSource::PARQUET) {
		std::cerr << " consistency=" << (config_.instant_snapshot ? "snapshot" : "storage_direct");
	}
	std::cerr << " decoded_rows=" << result.decoded_rows << " prefetched_blocks=" << result.prefetched_blocks
	          << " prefetch_tasks=" << result.prefetch_task_count << " prefetch=" << result.prefetch_ms << "ms"
	          << " tasks=" << result.task_count << " scheduler_setup=" << result.scheduler_setup_ms << "ms"
	          << " schedule=" << result.schedule_ms << "ms wait=" << result.wait_ms << "ms"
	          << " locate=" << result.locate_ms << "ms decode=" << result.decode_ms << "ms filter=" << result.filter_ms
	          << "ms append=" << result.append_ms << "ms combine=" << result.combine_ms
	          << "ms task_wall_sum=" << result.task_wall_ms << "ms total=" << result.scan_ms << "ms\n";
	if (!result.selected_row_offsets.empty()) {
		std::cerr << "  [RPT-SampleRanges] table=" << sample.table_name
		          << " selected=" << result.selected_row_offsets.size() << "/" << result.candidate_chunks
		          << " row_offsets=(";
		for (idx_t i = 0; i < result.selected_row_offsets.size(); i++) {
			if (i > 0) {
				std::cerr << ',';
			}
			std::cerr << result.selected_row_offsets[i];
		}
		std::cerr << ")\n";
	}
}

void TableSampleManager::AdoptInstantSample(Entry &sample, InstantSampleResult result, idx_t predicate_count,
                                            uint64_t table_seed) {
	D_ASSERT(result.source != InstantSampleSource::UNKNOWN);
	if (!result) {
		if (LogEnabled()) {
			std::cerr << "  [RPT-SampleUnavailable] table=" << sample.table_name
			          << " mode=instant reason=" << result.unavailable_reason << '\n';
		}
		return;
	}
	D_ASSERT(result.sample);
	if (!result.sample) {
		throw InternalException("Successful instant sample has no materialized collection");
	}
	sample.sampled_rows = result.sampled_rows;
	sample.sample_access_points =
	    result.selected_row_offsets.empty() ? result.selected_row_groups : result.selected_row_offsets.size();
	sample.sample = std::move(result.sample);
	sample.locally_filtered = sample.sample;
	sample.local_survivors = sample.sample->Count();
	D_ASSERT(sample.local_survivors <= sample.sampled_rows);
	sample.local_filter_evaluated = true;
	LogInstantSample(sample, result, predicate_count, table_seed);
}

void TableSampleManager::BuildInstantSample(Entry &sample, const LogicalOperator &op, LogicalGet &get,
                                            optional_ptr<TableCatalogEntry> table, uint64_t table_seed) {
	const bool log = LogEnabled();
	if (table) {
		vector<StorageIndex> column_ids;
		vector<LogicalType> column_types;
		for (auto &column_id : get.GetColumnIds()) {
			D_ASSERT(!column_id.IsVirtualColumn());
			D_ASSERT(column_id.GetPrimaryIndex() < get.returned_types.size());
			if (column_id.IsVirtualColumn() || column_id.GetPrimaryIndex() >= get.returned_types.size()) {
				throw InternalException("Unsupported column in native instant sample");
			}
			column_ids.emplace_back(column_id.GetPrimaryIndex());
			column_types.push_back(get.returned_types[column_id.GetPrimaryIndex()]);
		}
		idx_t predicate_count = 0;
		auto local_predicate = BuildLocalPredicate(sample, op, column_types, predicate_count, log);
		std::mt19937_64 random(table_seed);
		if (config_.instant_access == RPTInstantAccessMode::BLOCK) {
			auto result = BuildInstantBlockSample(context_, table->GetStorage(), column_ids, column_types,
			                                      config_.target_rows, config_.instant_block_windows, log,
			                                      config_.instant_snapshot, local_predicate.get(), random);
			AdoptInstantSample(sample, std::move(result), predicate_count, table_seed);
		} else {
			auto result = BuildInstantScatteredSample(context_, table->GetStorage(), column_ids, column_types,
			                                          config_.instant_access_points, config_.instant_rows_per_access,
			                                          log, config_.instant_snapshot, local_predicate.get(), random);
			AdoptInstantSample(sample, std::move(result), predicate_count, table_seed);
		}
		return;
	}

	if (!get.function.get_partition_stats) {
		InstantSampleResult unavailable;
		unavailable.source = InstantSampleSource::PARQUET;
		unavailable.unavailable_reason = "table function does not expose partition statistics";
		AdoptInstantSample(sample, std::move(unavailable), 0, table_seed);
		return;
	}
	auto metadata_started = std::chrono::steady_clock::now();
	auto plan =
	    PlanInstantParquetSample(context_, get, config_.target_rows, config_.instant_parquet_row_groups, table_seed);
	if (!plan || plan.output_types.empty()) {
		InstantSampleResult unavailable;
		unavailable.source = InstantSampleSource::PARQUET;
		unavailable.unavailable_reason = "Parquet scan cannot form a bounded file-aware row-number sample";
		AdoptInstantSample(sample, std::move(unavailable), 0, table_seed);
		return;
	}
	sample.total_rows = plan.source_rows;
	auto metadata_ms =
	    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - metadata_started).count();
	idx_t predicate_count = 0;
	auto local_predicate = BuildLocalPredicate(sample, op, plan.output_types, predicate_count, log);
	auto result = BuildInstantParquetSample(context_, get, plan, log, local_predicate.get());
	result.metadata_ms = metadata_ms;
	AdoptInstantSample(sample, std::move(result), predicate_count, table_seed);
}

// ---------------------------------------------------------------------------
// Entry lifecycle — metadata registration is I/O-free. Prepared or instant
// sample acquisition is deferred until a local or transfer predicate needs an
// estimate; a bare GET already has an exact catalog cardinality.
// ---------------------------------------------------------------------------

optional_idx TableSampleManager::TryGetExactLocalCardinality(const Entry &sample, const LogicalOperator &op) const {
	const LogicalOperator *node = &op;
	while (node) {
		if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
			return optional_idx();
		}
		if (node->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = node->Cast<LogicalGet>();
			auto table = get.GetTable();
			if (!table) {
				return optional_idx();
			}
			if (get.table_filters.FilterCount() == 0) {
				return sample.total_rows;
			}
			for (auto &filter_entry : get.table_filters) {
				auto projection_column = filter_entry.GetIndex().GetIndex();
				if (projection_column >= get.GetColumnIds().size()) {
					return optional_idx();
				}
				auto column_id = get.GetColumnIds()[projection_column];
				if (column_id.IsVirtualColumn()) {
					return optional_idx();
				}
				auto statistics =
				    table->GetStorage().GetStatistics(context_, StorageIndex(column_id.GetPrimaryIndex()));
				if (!statistics) {
					return optional_idx();
				}
				auto result = filter_entry.Filter().Cast<ExpressionFilter>().CheckStatistics(context_, *statistics);
				if (result == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
				    result == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
					return idx_t(0);
				}
				if (result != FilterPropagateResult::FILTER_ALWAYS_TRUE) {
					return optional_idx();
				}
			}
			return sample.total_rows;
		}
		if (node->children.empty()) {
			break;
		}
		node = node->children[0].get();
	}
	return optional_idx();
}

TableSampleManager::Entry &TableSampleManager::GetEntry(const LogicalOperator &op) {
	auto it = entries_.find(&op);
	if (it != entries_.end()) {
		return it->second;
	}
	auto &entry = entries_[&op];
	entry.total_rows = op.estimated_cardinality;

	// Find the underlying LogicalGet.
	const LogicalOperator *leaf = &op;
	while (leaf->type != LogicalOperatorType::LOGICAL_GET && !leaf->children.empty()) {
		leaf = leaf->children[0].get();
	}
	if (leaf->type != LogicalOperatorType::LOGICAL_GET) {
		return entry; // no base GET — leave empty, EstimateOnSample will fall back
	}
	auto &get = leaf->Cast<LogicalGet>();

	// Total rows from catalog.
	idx_t total_rows = op.estimated_cardinality;
	if (auto tbl = get.GetTable()) {
		total_rows = tbl->GetStorage().GetTotalRows();
	}

	const idx_t full_width = get.returned_types.size();

	// Per-query narrow view: storage column s is referenced by the query
	// binding whose column_ids[i] has storage id s. Virtual columns (rowid)
	// cannot be sampled; predicates on them are skipped downstream, which
	// conservatively over-counts survivors.
	vector<ColumnBinding> full_bindings(full_width); // default = invalid marker
	const auto &query_ids = get.GetColumnIds();
	for (idx_t i = 0; i < query_ids.size(); i++) {
		if (query_ids[i].IsVirtualColumn()) {
			continue;
		}
		idx_t s = query_ids[i].GetPrimaryIndex();
		if (s < full_width) {
			full_bindings[s] = ColumnBinding(get.table_index, ProjectionIndex(i));
		}
	}
	vector<column_t> needed_storage_columns;
	vector<ColumnBinding> narrow_bindings;
	for (idx_t s = 0; s < full_width; s++) {
		if (full_bindings[s].column_index.IsValid()) {
			needed_storage_columns.push_back(s);
			narrow_bindings.push_back(full_bindings[s]);
		}
	}

	entry.total_rows = total_rows;
	entry.needed_storage_columns = std::move(needed_storage_columns);
	entry.output_bindings = std::move(narrow_bindings);

	auto table = get.GetTable();
	entry.table_name = table ? table->name.GetIdentifierName() : get.GetName();
	return entry;
}

void TableSampleManager::EnsureSample(Entry &entry, const LogicalOperator &op) {
	if (entry.sample_acquisition_attempted) {
		return;
	}
	entry.sample_acquisition_attempted = true;

	const LogicalOperator *leaf = &op;
	while (leaf->type != LogicalOperatorType::LOGICAL_GET && !leaf->children.empty()) {
		leaf = leaf->children[0].get();
	}
	if (leaf->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	// Persistent reservoirs are full-width and query-independent. Instant
	// samples are narrowed to the columns referenced by this query. In both
	// cases local predicates are stripped from the scan and evaluated exactly
	// once by this manager.
	auto scan = leaf->Copy(context_);
	auto &scan_get = scan->Cast<LogicalGet>();
	scan_get.table_filters.ClearFilters();
	scan_get.projection_ids.clear();
	const idx_t full_width = scan_get.returned_types.size();
	auto table = scan_get.GetTable();

	// Storage sample placement must not depend on the database mount path. The
	// benchmark runner opens the same database through a temporary symlink; a
	// path-derived seed made an identical user seed select different chunks.
	// Keep the persistent-cache key path-specific, but derive sampling RNG from
	// stable table/schema/content-shape identity.
	string storage_seed_identity;
	if (table) {
		storage_seed_identity = table->schema.name + "|" + table->name;
	} else {
		storage_seed_identity = entry.table_name;
	}
	storage_seed_identity += "|rows=" + std::to_string(entry.total_rows) + "|types=";
	for (auto &type : scan_get.returned_types) {
		storage_seed_identity += type.ToString() + ",";
	}

	storage_seed_identity += "|seed=" + std::to_string(config_.seed);
	// DuckDB's reservoir implementation consumes an int64_t seed. Keep the value
	// in its non-negative range so the conversion is defined on every platform.
	uint64_t table_seed = Hash(storage_seed_identity.c_str(), storage_seed_identity.size()) & 0x7FFFFFFFFFFFFFFFULL;
	if (config_.mode == RPTSamplingMode::INSTANT) {
		auto &ids = scan_get.GetMutableColumnIds();
		ids.clear();
		for (auto storage_col : entry.needed_storage_columns) {
			ids.emplace_back(storage_col);
		}
		if (ids.empty() && full_width > 0) {
			ids.emplace_back(scan_get.GetAnyColumn());
		}
		entry.sample_column_positions.clear();
		for (idx_t i = 0; i < entry.needed_storage_columns.size(); i++) {
			entry.sample_column_positions.push_back(i);
		}
		BuildInstantSample(entry, op, scan_get, table, table_seed);
		return;
	}

	entry.sample_column_positions = entry.needed_storage_columns;
	entry.sample = GetOrCreatePreparedSample(context_, config_, std::move(scan), entry.total_rows, table_seed);
	entry.sampled_rows = entry.sample ? entry.sample->Count() : 0;
}

} // namespace duckdb
