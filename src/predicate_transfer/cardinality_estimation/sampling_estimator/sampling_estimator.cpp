#include "predicate_transfer/cardinality_estimation/sampling_estimator/sampling_estimator.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>

namespace duckdb {

SamplingCardinalityEstimator::SamplingCardinalityEstimator(ClientContext &context, RPTSamplingConfig config)
    : context_(context), config_(std::move(config)), samples_(context_, config_) {
	D_ASSERT(config_.intermediate_rate >= 0.0 && config_.intermediate_rate <= 1.0);
	D_ASSERT(config_.target_rows > 0);
	D_ASSERT(config_.instant_access_points > 0);
	D_ASSERT(config_.instant_rows_per_access > 0);
	D_ASSERT(config_.instant_block_windows > 0);
	D_ASSERT(config_.instant_parquet_row_groups > 0);
}

bool SamplingCardinalityEstimator::LogEnabled() const {
	Value setting;
	return context_.TryGetCurrentSetting("rpt_log_transfer_steps", setting) && setting.GetValue<bool>();
}

// Rewrite every BOUND_COLUMN_REF whose binding appears in `bindings` to a
// BoundReferenceExpression on the paired positions[i]; unmatched refs are left
// as-is.
static void RewriteRefsToPositions(unique_ptr<Expression> &expr, const vector<ColumnBinding> &bindings,
                                   const vector<idx_t> &positions) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &cref = expr->Cast<BoundColumnRefExpression>();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == cref.Binding()) {
				expr = make_uniq<BoundReferenceExpression>(cref.GetAlias(), cref.GetReturnType(), positions[i]);
				return;
			}
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { RewriteRefsToPositions(child, bindings, positions); });
}

// ---------------------------------------------------------------------------
// Chunk-level sampling on a ColumnDataCollection (for CDC / in-memory data)
// ---------------------------------------------------------------------------

static idx_t SampleChunks(const ColumnDataCollection &cdc, double sample_rate, ExpressionExecutor *expr_exec,
                          const vector<ScannerFilterSet::Entry> &filters) {
	idx_t total_chunks = cdc.ChunkCount();
	if (total_chunks == 0) {
		return 0;
	}

	idx_t target_chunks = total_chunks;
	if (sample_rate > 0.0 && sample_rate < 1.0) {
		target_chunks = MaxValue<idx_t>(
		    1, MinValue<idx_t>(total_chunks,
		                       static_cast<idx_t>(std::ceil(static_cast<double>(total_chunks) * sample_rate))));
	}

	DataChunk chunk;
	chunk.Initialize(cdc.GetAllocator(), cdc.Types());
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	idx_t sampled_chunks = 0;
	idx_t surviving_rows = 0;

	for (idx_t sample_idx = 0; sample_idx < target_chunks; sample_idx++) {
		// Spread the selected chunks over the collection. Since target_chunks is
		// never larger than total_chunks, this mapping produces unique indices.
		const idx_t ci = sample_idx * total_chunks / target_chunks;
		chunk.Reset();
		cdc.FetchChunk(ci, chunk);
		if (chunk.size() == 0) {
			sampled_chunks++;
			continue;
		}

		// Expression filter.
		if (expr_exec) {
			idx_t count = expr_exec->SelectExpression(chunk, sel);
			if (count == 0) {
				sampled_chunks++;
				continue;
			}
			if (count < chunk.size()) {
				chunk.Slice(sel, count);
				chunk.Flatten();
			}
		}

		// BF filters.
		for (auto &entry : filters) {
			if (chunk.size() == 0) {
				break;
			}
			if (!entry.filter || entry.chunk_cols.empty()) {
				continue;
			}
			idx_t col_count = chunk.ColumnCount();
			bool skip = false;
			for (auto cc : entry.chunk_cols) {
				if (cc >= col_count) {
					skip = true;
					break;
				}
			}
			if (skip) {
				continue;
			}

			size_t count = chunk.size();
			entry.filter->Lookup(chunk, entry.chunk_cols, sel, count);
			if (count < chunk.size()) {
				chunk.Slice(sel, count);
				chunk.Flatten();
			}
		}

		surviving_rows += chunk.size();
		sampled_chunks++;
	}

	if (sampled_chunks == 0) {
		return 0;
	}
	return static_cast<idx_t>(static_cast<double>(surviving_rows) * static_cast<double>(total_chunks) /
	                          static_cast<double>(sampled_chunks));
}

// ---------------------------------------------------------------------------
// SampleCDC — estimation for FILTER+CHUNK_GET (in-memory CDC tables)
// ---------------------------------------------------------------------------

idx_t SamplingCardinalityEstimator::SampleCDC(const LogicalOperator &op) {
	const LogicalOperator *node = &op;
	vector<const LogicalFilter *> filter_chain;
	while (node->type == LogicalOperatorType::LOGICAL_FILTER && node->children.size() == 1) {
		filter_chain.push_back(&node->Cast<LogicalFilter>());
		node = node->children[0].get();
	}
	auto &chunk_get = node->Cast<LogicalColumnDataGet>();
	if (!chunk_get.collection) {
		return 0;
	}
	auto &cdc = *chunk_get.collection;

	// Build expression filter (same rewrite as TableScanner constructor).
	auto chunk_bindings = LogicalOperator::GenerateColumnBindings(chunk_get.table_index, chunk_get.chunk_types.size());
	vector<ColumnBinding> current_bindings = chunk_bindings;
	vector<idx_t> current_positions;
	current_positions.reserve(chunk_bindings.size());
	for (idx_t i = 0; i < chunk_bindings.size(); i++) {
		current_positions.push_back(i);
	}

	vector<unique_ptr<Expression>> rewritten;
	for (auto it = filter_chain.rbegin(); it != filter_chain.rend(); ++it) {
		auto *f = *it;
		for (auto &e : f->expressions) {
			auto cloned = e->Copy();
			RewriteRefsToPositions(cloned, current_bindings, current_positions);
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

	// NOTE: ExpressionExecutor::AddExpression stores a *reference*, so final_expr
	// must outlive every SampleChunks call below — keep it at function scope.
	unique_ptr<Expression> final_expr;
	unique_ptr<ExpressionExecutor> expr_exec;
	if (!rewritten.empty()) {
		if (rewritten.size() == 1) {
			final_expr = std::move(rewritten[0]);
		} else {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &e : rewritten) {
				conj->GetChildrenMutable().push_back(std::move(e));
			}
			final_expr = std::move(conj);
		}
		expr_exec = make_uniq<ExpressionExecutor>(context_);
		expr_exec->AddExpression(*final_expr);
	}

	// No BF filters at init time.
	static const vector<ScannerFilterSet::Entry> empty_filters;
	idx_t v = SampleChunks(cdc, config_.intermediate_rate, expr_exec.get(), empty_filters);
	if (v == 0 && cdc.Count() > 0) {
		// A 1% chunk sample can miss a rare survivor; downstream treats 0 as
		// EmptyResult. In-memory data is cheap to rescan — confirm exactly.
		v = SampleChunks(cdc, 1.0, expr_exec.get(), empty_filters);
	}
	return v;
}

// ---------------------------------------------------------------------------
// EstimateOnSample — apply op's local predicate (LogicalFilter expressions +
// GET.table_filters) plus BF filters to the raw sample, count survivors, and
// extrapolate to the full table by total_rows / sampled_rows.
// ---------------------------------------------------------------------------

idx_t SamplingCardinalityEstimator::EstimateOnSample(TableSampleManager::Entry &sample, const LogicalOperator &op,
                                                     const vector<DirectFilterInfo> &filters) {
	if (!sample.sample || sample.sampled_rows == 0) {
		// Couldn't build a sample — fall back to DuckDB's own estimate. Never
		// report 0 here: a 0 becomes EmptyResult downstream, and without a sample
		// we have no evidence the table is actually empty.
		auto fallback = MaxValue<idx_t>(op.estimated_cardinality, 1);
		if (LogEnabled()) {
			std::cerr << "  [RPT-SampleEstimate] table=" << sample.table_name
			          << " sample_rows=0 local_survivors=0 transfer_survivors=0 estimate=" << fallback
			          << " filters=" << filters.size() << " status=fallback" << '\n';
		}
		return fallback;
	}
	samples_.EnsureLocalFilter(sample, op);

	// Resolve BF filters to sample chunk columns (same order in locally_filtered).
	vector<ScannerFilterSet::Entry> bf_filters;
	for (auto &bf : filters) {
		if (!bf.filter) {
			continue;
		}
		idx_t chunk_col = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < sample.output_bindings.size(); i++) {
			if (sample.output_bindings[i] == bf.binding) {
				chunk_col = i;
				break;
			}
		}
		if (chunk_col == DConstants::INVALID_INDEX) {
			continue;
		}
		ScannerFilterSet::Entry e;
		e.bindings.push_back(bf.binding);
		e.chunk_cols.push_back(chunk_col);
		e.filter = bf.filter;
		bf_filters.push_back(std::move(e));
	}

	// Survivors after local predicate (+ BF). BF pass scans only the small local set.
	idx_t survivors = sample.local_survivors;
	auto probe_started = std::chrono::steady_clock::now();
	if (!bf_filters.empty() && sample.locally_filtered && sample.local_survivors > 0) {
		survivors = SampleChunks(*sample.locally_filtered, 1.0, nullptr, bf_filters);
	}
	if (LogEnabled()) {
		auto elapsed =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - probe_started).count();
		std::cerr << "  [RPT-SampleEvaluate] table=" << sample.table_name
		          << " phase=transfer filters=" << bf_filters.size() << " input=" << sample.local_survivors
		          << " survivors=" << survivors << " elapsed=" << elapsed << "ms" << '\n';
	}

	// Extrapolate to the full table. A 0 estimate is special downstream (treated as
	// EmptyResult), so only return 0 when we sampled the WHOLE table — a partial
	// sample can miss a rare-but-present match, and reporting empty would wrongly
	// drop all rows. With 0 survivors in a partial sample, report the weight of a
	// single sample row (total/sample) rather than 1: a hard floor of 1 would tie
	// with genuinely single-row tables (whose full-sample estimate is exact) and
	// mislead the excitation ordering into flooding an uncertain table first.
	//
	// A 0 is only trustworthy for a pure LOCAL predicate on a fully materialized
	// sample: then the whole table was checked. With BF filters applied the
	// probe BF was itself built from the *source* table's sample, so it has
	// false negatives — 0 survivors here does not mean the join is empty, and
	// returning 0 would wrongly rewrite a non-empty table to EmptyResult.
	idx_t est;
	string estimate_status = "sampled";
	if (survivors == 0) {
		if (sample.sampled_rows >= sample.total_rows && bf_filters.empty()) {
			est = 0;
			estimate_status = "sampled_exact_zero";
		} else {
			est = MaxValue<idx_t>(sample.total_rows / MaxValue<idx_t>(sample.sampled_rows, 1), 1);
			estimate_status = "sampled_zero_floor";
		}
	} else {
		double frac = static_cast<double>(survivors) / static_cast<double>(sample.sampled_rows);
		est = static_cast<idx_t>(std::lround(frac * static_cast<double>(sample.total_rows)));
		if (est == 0) {
			est = 1;
		}
	}
	if (LogEnabled()) {
		std::cerr << "  [RPT-SampleEstimate] table=" << sample.table_name << " sample_rows=" << sample.sampled_rows
		          << " local_survivors=" << sample.local_survivors << " transfer_survivors=" << survivors
		          << " estimate=" << est << " filters=" << bf_filters.size() << " status=" << estimate_status << '\n';
	}
	return est;
}

// ---------------------------------------------------------------------------
// Public Estimate overloads
// ---------------------------------------------------------------------------

static bool IsCDC(const LogicalOperator &op) {
	const LogicalOperator *leaf = &op;
	while (leaf->type == LogicalOperatorType::LOGICAL_FILTER && leaf->children.size() == 1) {
		leaf = leaf->children[0].get();
	}
	return leaf->type == LogicalOperatorType::LOGICAL_CHUNK_GET;
}

idx_t SamplingCardinalityEstimator::Estimate(const LogicalOperator &op) {
	if (IsCDC(op)) {
		return SampleCDC(op);
	}
	auto &sample = samples_.GetEntry(op);
	if (config_.mode == RPTSamplingMode::INSTANT) {
		auto exact_cardinality = samples_.TryGetExactLocalCardinality(sample, op);
		if (exact_cardinality.IsValid()) {
			return exact_cardinality.GetIndex();
		}
	}
	samples_.EnsureSample(sample, op);
	return EstimateOnSample(sample, op, {});
}

idx_t SamplingCardinalityEstimator::Estimate(const LogicalOperator &op, const vector<DirectFilterInfo> &filters) {
	if (IsCDC(op)) {
		return SampleCDC(op);
	}
	if (filters.empty()) {
		return Estimate(op);
	}
	auto &sample = samples_.GetEntry(op);
	samples_.EnsureSample(sample, op);
	return EstimateOnSample(sample, op, filters);
}

idx_t SamplingCardinalityEstimator::Estimate(TableScanner &scanner, const vector<DirectFilterInfo> &) {
	auto *cdc = scanner.GetData();
	if (!cdc) {
		return 0;
	}
	auto *pf = scanner.GetPendingExprFilter();
	ExpressionExecutor *expr_exec = (pf && pf->executor) ? pf->executor.get() : nullptr;
	// SampleChunks fetches RAW chunks from the CDC, but the scanner's filter
	// chunk_cols were resolved against the (possibly narrower) post-projection
	// output bindings. When the pending filter carries a projection_map, remap
	// each chunk_col from its narrow output position to the raw CDC position.
	// (The pending expression executor itself is built against the raw wide
	// layout — see TableMaterialization::PendingExpression::scratch — so it
	// needs no remapping.)
	const vector<ScannerFilterSet::Entry> *filters = &scanner.GetFilters();
	vector<ScannerFilterSet::Entry> remapped_filters;
	if (pf && !pf->projection_map.empty()) {
		remapped_filters = scanner.GetFilters();
		for (auto &entry : remapped_filters) {
			for (auto &cc : entry.chunk_cols) {
				cc = cc < pf->projection_map.size() ? pf->projection_map[cc] : DConstants::INVALID_INDEX;
			}
		}
		filters = &remapped_filters;
	}
	idx_t v = SampleChunks(*cdc, config_.intermediate_rate, expr_exec, *filters);
	if (v == 0 && cdc->Count() > 0) {
		// A 1% chunk sample can miss a rare survivor while selective BF filters are
		// applied; a false 0 becomes EmptyResult downstream and wrongly empties the
		// query. In-memory data is cheap to rescan — confirm the 0 exactly.
		v = SampleChunks(*cdc, 1.0, expr_exec, *filters);
	}
	return v;
}

} // namespace duckdb
