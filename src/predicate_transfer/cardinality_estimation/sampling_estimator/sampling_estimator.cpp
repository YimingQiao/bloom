#include "predicate_transfer/cardinality_estimation/sampling_estimator/sampling_estimator.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "predicate_transfer/bloom_log.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <iostream>

namespace duckdb {

SamplingCardinalityEstimator::SamplingCardinalityEstimator(ClientContext &context, RPTSamplingConfig config)
    : context_(context), config_(std::move(config)), samples_(context_, config_) {
	D_ASSERT(config_.target_rows > 0);
	D_ASSERT(config_.in_memory_sample_size > 0);
	D_ASSERT(config_.instant_access_points > 0);
	D_ASSERT(config_.instant_rows_per_access > 0);
	D_ASSERT(config_.instant_block_windows > 0);
	D_ASSERT(config_.instant_parquet_row_groups > 0);
}

bool SamplingCardinalityEstimator::LogEnabled() const {
	Value setting;
	return context_.TryGetCurrentSetting("bloom_log_transfer_steps", setting) && setting.GetValue<bool>();
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
// Row sampling on a ColumnDataCollection (CDC / in-memory data)
// ---------------------------------------------------------------------------

static void ApplySampleFilters(DataChunk &chunk, ExpressionExecutor *expr_exec,
                               const vector<ScannerFilterSet::Entry> &filters) {
	if (expr_exec && chunk.size()) {
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		const auto count = expr_exec->SelectExpression(chunk, sel);
		if (count < chunk.size()) {
			chunk.Slice(sel, count);
		}
	}
	for (auto &entry : filters) {
		if (chunk.size() == 0) {
			break;
		}
		if (!entry.filter || entry.chunk_cols.empty() ||
		    std::any_of(entry.chunk_cols.begin(), entry.chunk_cols.end(),
		                [&](idx_t col) { return col >= chunk.ColumnCount(); })) {
			continue;
		}
		// Slice retains its selection; do not overwrite it in the next filter.
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		size_t count = chunk.size();
		entry.filter->Lookup(chunk, entry.chunk_cols, sel, count);
		if (count < chunk.size()) {
			chunk.Slice(sel, count);
		}
	}
}

static idx_t ScanCollection(const ColumnDataCollection &cdc, const vector<column_t> &columns,
                            ExpressionExecutor *executor, const vector<ScannerFilterSet::Entry> &filters) {
	vector<LogicalType> types;
	for (auto column : columns) {
		types.push_back(cdc.Types()[column]);
	}
	ChunkManagementState state;
	state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	DataChunk chunk;
	chunk.Initialize(cdc.GetAllocator(), types);
	idx_t survivors = 0;
	for (auto &segment : cdc.GetSegments()) {
		chunk.Reset();
		state.handles.clear();
		for (idx_t ci = 0; ci < segment->chunk_data.size(); ci++) {
			chunk.Reset();
			segment->ReadChunk(ci, state, chunk, columns);
			if (columns.empty()) {
				chunk.SetCardinalityUnsafe(segment->chunk_data[ci].count);
			}
			ApplySampleFilters(chunk, executor, filters);
			survivors += chunk.size();
		}
	}
	return survivors;
}

static bool CanGatherDirect(const ColumnDataCollection &cdc, const vector<column_t> &columns) {
	for (auto col : columns) {
		switch (cdc.Types()[col].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT128:
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
		case PhysicalType::INTERVAL:
			break;
		default:
			return false;
		}
	}
	for (auto &segment : cdc.GetSegments()) {
		if (segment->allocator->GetType() != ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
			return false;
		}
	}
	return true;
}

static void GatherDirect(ColumnDataCollectionSegment &segment, idx_t chunk_index, const vector<column_t> &columns,
                         const SelectionVector &selection, idx_t count, DataChunk &sample) {
	ChunkManagementState state;
	const auto output_offset = sample.size();
	for (idx_t col = 0; col < columns.size(); col++) {
		auto &meta = segment.GetVectorData(segment.chunk_data[chunk_index].vector_data[columns[col]]);
		D_ASSERT(!meta.next_data.IsValid());
		const auto width = GetTypeIdSize(sample.data[col].GetType().InternalType());
		auto data = segment.allocator->GetDataPointer(state, meta.block_id, meta.offset);
		// Inspect only selected validity bits, not the entire vector's mask.
		ValidityMask validity(ColumnDataCollectionSegment::GetValidityPointerForWriting(data, width),
		                      STANDARD_VECTOR_SIZE);
		auto output = FlatVector::GetDataMutable(sample.data[col]);
		auto &output_validity = FlatVector::ValidityMutable(sample.data[col]);
		for (idx_t row = 0; row < count; row++) {
			const auto source_row = selection.get_index(row);
			D_ASSERT(source_row < meta.count);
			if (validity.RowIsValid(source_row)) {
				memcpy(output + (output_offset + row) * width, data + source_row * width, width);
			} else {
				output_validity.SetInvalid(output_offset + row);
			}
		}
		FlatVector::SetSize(sample.data[col], count_t(output_offset + count));
	}
	sample.CheckCardinality(output_offset + count);
}

// Uniform sampling without replacement; retain raw samples in execution-sized
// chunks.
static idx_t SampleRows(const ColumnDataCollection &cdc, const vector<column_t> &column_ids, idx_t target_rows,
                        idx_t seed, ExpressionExecutor *expr_exec, const vector<ScannerFilterSet::Entry> &filters,
                        ClientContext *stats_context, const char *phase, idx_t table_index,
                        TableSampleManager::CDCEntry *cache) {
	const auto started = std::chrono::steady_clock::now();
	const idx_t n = cdc.Count(), k = MinValue(n, target_rows);
	if (k == 0) {
		return 0;
	}
	vector<LogicalType> types;
	for (auto col : column_ids)
		types.push_back(cdc.Types()[col]);
	DataChunk sample;
	sample.Initialize(cdc.GetAllocator(), types);
	const bool reused = cache && !cache->sample.empty() && cache->columns == column_ids;
	const char *cache_status = reused                   ? "hit"
	                           : !cache                 ? "unowned"
	                           : !cache->sample.empty() ? "columns_changed"
	                                                    : "new_snapshot";
	const bool direct = CanGatherDirect(cdc, column_ids);
	idx_t touched = 0;
	vector<unique_ptr<DataChunk>> acquired_chunks;
	auto save_chunk = [&]() {
		if (!sample.size()) {
			return;
		}
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(cdc.GetAllocator(), types);
		sample.Copy(*chunk);
		chunk->SetCardinalityUnsafe(sample.size());
		acquired_chunks.push_back(std::move(chunk));
		sample.Reset();
	};
	if (reused) {
		touched = cache->touched_chunks;
	} else {
		vector<idx_t> rows;
		rows.reserve(k);
		if (k == n) {
			for (idx_t i = 0; i < n; i++) {
				rows.push_back(i);
			}
		} else {
			RandomEngine rng(0);
			rng.SetSeed(seed ^ (uint64_t(table_index) * 0x9e3779b97f4a7c15ULL));
			// Fixed-capacity open addressing avoids a heap allocation per selected
			// row.
			idx_t capacity = 1;
			while (capacity < 2 * k) {
				capacity *= 2;
			}
			vector<idx_t> selected(capacity, DConstants::INVALID_INDEX);
			auto insert = [&](idx_t value) {
				idx_t slot = (value * 0x9e3779b97f4a7c15ULL) & (capacity - 1);
				while (selected[slot] != DConstants::INVALID_INDEX) {
					if (selected[slot] == value) {
						return false;
					}
					slot = (slot + 1) & (capacity - 1);
				}
				selected[slot] = value;
				return true;
			};
			// Floyd sampling, with rejection to avoid bias in bounded integer draws.
			for (idx_t j = n - k; j < n; j++) {
				const uint64_t bound = j + 1;
				const uint64_t threshold = -bound % bound;
				uint64_t draw;
				do {
					draw = rng.NextRandomInteger64();
				} while (draw < threshold);
				const idx_t candidate = draw % bound;
				if (insert(candidate)) {
					rows.push_back(candidate);
				} else {
					insert(j);
					rows.push_back(j);
				}
			}
			// Sort integer row positions by bytes; only bytes needed for this CDC are
			// visited. The ascending access order is identical to comparison sorting.
			vector<idx_t> scratch(k);
			for (idx_t remaining = n - 1, shift = 0; remaining; remaining >>= 8, shift += 8) {
				std::array<idx_t, 256> offsets {};
				for (auto row : rows) {
					offsets[(row >> shift) & 255]++;
				}
				idx_t offset = 0;
				for (auto &count : offsets) {
					const auto next = offset + count;
					count = offset;
					offset = next;
				}
				for (auto row : rows) {
					scratch[offsets[(row >> shift) & 255]++] = row;
				}
				rows.swap(scratch);
			}
		}
		ChunkManagementState read_state;
		read_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
		DataChunk input;
		if (!direct)
			input.Initialize(cdc.GetAllocator(), types);
		// Append copies the selected values; unlike Slice, it does not retain this
		// buffer.
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t cursor = 0, base = 0;
		for (auto &segment : cdc.GetSegments()) {
			if (cursor == k) {
				break;
			}
			if (!direct)
				input.Reset();
			read_state.handles.clear();
			if (rows[cursor] >= base + segment->count) {
				base += segment->count;
				continue;
			}
			for (idx_t ci = 0; ci < segment->chunk_data.size() && cursor < k; ci++) {
				const idx_t end = base + segment->chunk_data[ci].count;
				if (rows[cursor] < end) {
					if (!direct) {
						input.Reset();
						segment->ReadChunk(ci, read_state, input, column_ids);
					}
					touched++;
					while (cursor < k && rows[cursor] < end) {
						idx_t count = 0;
						const auto capacity = STANDARD_VECTOR_SIZE - sample.size();
						while (cursor < k && rows[cursor] < end && count < capacity) {
							sel.set_index(count++, rows[cursor++] - base);
						}
						if (column_ids.empty()) {
							sample.SetCardinalityUnsafe(sample.size() + count);
						} else if (direct) {
							GatherDirect(*segment, ci, column_ids, sel, count, sample);
						} else {
							sample.Append(input, sel, count);
						}
						if (sample.size() == STANDARD_VECTOR_SIZE) {
							save_chunk();
						}
					}
				}
				base = end;
			}
		}
		D_ASSERT(cursor == k);
		save_chunk();
		if (cache) {
			cache->sample = std::move(acquired_chunks);
			cache->columns = column_ids;
			cache->touched_chunks = touched;
		}
	}

	const auto acquired = std::chrono::steady_clock::now();
	const auto &chunks = cache ? cache->sample : acquired_chunks;
	idx_t survivors = 0;
	for (auto &raw : chunks) {
		sample.Reset();
		raw->Copy(sample);
		sample.SetCardinalityUnsafe(raw->size());
		ApplySampleFilters(sample, expr_exec, filters);
		survivors += sample.size();
	}
	const auto filtered = std::chrono::steady_clock::now();
	// No sampled survivor means low selectivity, not a proven empty table.
	// Use one sample row's weight without scanning additional source rows.
	const bool zero_floor = survivors == 0 && k < n;
	const auto estimate =
	    zero_floor ? n / k : static_cast<idx_t>(std::llround(double(n) * double(survivors) / double(k)));
	if (stats_context) {
		auto ms = [](auto a, auto b) {
			return StringUtil::Format("%.17g", std::chrono::duration<double, std::milli>(b - a).count());
		};
		auto number = [](double value) {
			return StringUtil::Format("%.17g", value);
		};
		const double p = double(survivors) / double(k);
		vector<pair<string, string>> info = {{"domain", "cdc"},
		                                     {"source", "cdc"},
		                                     {"mode", "in_memory"},
		                                     {"design", k == n ? "census" : "uniform_rows_without_replacement"},
		                                     {"seed", to_string(seed)},
		                                     {"sample_rng", "duckdb_random_engine"},
		                                     {"total_rows", to_string(n)},
		                                     {"sample_reused", reused ? "true" : "false"},
		                                     {"cache_status", cache_status},
		                                     {"sample_snapshot", cache ? to_string(cache->snapshot_id) : "NA"},
		                                     {"gather_path", reused   ? "cache"
		                                                     : direct ? "direct"
		                                                              : "read_chunk"},
		                                     {"read_chunks", to_string(reused ? 0 : touched)},
		                                     {"total_chunks", to_string(cdc.ChunkCount())},
		                                     {"sampled_chunks", to_string(touched)},
		                                     {"sample_rows", to_string(k)},
		                                     {"survivors", to_string(survivors)},
		                                     {"estimate", to_string(estimate)},
		                                     {"estimate_status", zero_floor ? "sampled_zero_floor"
		                                                         : k == n   ? "exact"
		                                                                    : "sampled"},
		                                     {"acquire_ms", ms(started, acquired)},
		                                     {"filter_ms", ms(acquired, filtered)},
		                                     {"sample_ms", ms(started, filtered)},
		                                     {"row_survival", number(p)},
		                                     {"sample_columns", to_string(column_ids.size())},
		                                     {"filter_count", to_string(filters.size())},
		                                     {"expression_filter", expr_exec ? "true" : "false"}};
		// This is an estimated standard error, not a confidence guarantee.
		info.emplace_back("sampling_se",
		                  k == n  ? "0"
		                  : k > 1 ? number(std::sqrt(p * (1 - p) * double(n - k) / (double(n) * double(k - 1))))
		                          : "NA");
		const bool audit = BloomSampleAuditEnabled(*stats_context);
		info.emplace_back("audit", audit ? "true" : "false");
		if (audit || k == n) {
			const auto audit_started = std::chrono::steady_clock::now();
			const auto exact = k == n ? survivors : ScanCollection(cdc, column_ids, expr_exec, filters);
			info.emplace_back("exact_rows", to_string(exact));
			info.emplace_back("audit_extra_rows", to_string(k == n ? 0 : n));
			info.emplace_back("audit_ms", ms(audit_started, std::chrono::steady_clock::now()));
			info.emplace_back("signed_error_rows", number(double(estimate) - double(exact)));
			info.emplace_back("absolute_error_fraction",
			                  number(std::abs(double(estimate) - double(exact)) / double(n)));
		}
		LogBloomCDCSample(*stats_context, table_index, phase, info);
	}
	return estimate;
}

// Evaluate only columns referenced by the current predicates and filters. Do
// not mutate the scanner's executor or filter bindings: they are reused for
// normal execution.
static idx_t SampleCollection(ClientContext &context, const ColumnDataCollection &cdc, const RPTSamplingConfig &config,
                              ExpressionExecutor *expr_exec, const vector<ScannerFilterSet::Entry> &filters,
                              const char *phase, idx_t table_index, TableSampleManager::CDCEntry *cache = nullptr) {
	vector<bool> used(cdc.Types().size(), false);
	if (expr_exec) {
		for (auto expr : expr_exec->expressions) {
			ExpressionIterator::VisitExpression<BoundReferenceExpression>(*expr,
			                                                              [&](const BoundReferenceExpression &ref) {
				                                                              D_ASSERT(ref.Index() < used.size());
				                                                              used[ref.Index()] = true;
			                                                              });
		}
	}
	for (auto &entry : filters) {
		if (entry.filter && !entry.chunk_cols.empty() &&
		    std::all_of(entry.chunk_cols.begin(), entry.chunk_cols.end(),
		                [&](idx_t col) { return col < used.size(); })) {
			for (auto col : entry.chunk_cols) {
				used[col] = true;
			}
		}
	}
	vector<column_t> columns;
	vector<idx_t> positions(used.size(), DConstants::INVALID_INDEX);
	for (idx_t col = 0; col < used.size(); col++) {
		if (used[col]) {
			positions[col] = columns.size();
			columns.push_back(col);
		}
	}
	vector<unique_ptr<Expression>> expressions;
	unique_ptr<ExpressionExecutor> projected_executor;
	vector<ScannerFilterSet::Entry> projected_filters;
	auto active_filters = &filters;
	if (columns.size() != used.size()) {
		if (expr_exec) {
			projected_executor = make_uniq<ExpressionExecutor>(context);
			for (auto expr : expr_exec->expressions) {
				auto copy = expr->Copy();
				ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
				    copy, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &) {
					    ref.IndexMutable() = positions[ref.Index()];
				    });
				projected_executor->AddExpression(*copy);
				expressions.push_back(std::move(copy));
			}
			expr_exec = projected_executor.get();
		}
		projected_filters = filters;
		for (auto &entry : projected_filters) {
			for (auto &col : entry.chunk_cols) {
				col = col < positions.size() ? positions[col] : DConstants::INVALID_INDEX;
			}
		}
		active_filters = &projected_filters;
	}
	const bool stats = BloomStructuredLoggingEnabled(context);
	auto stats_context = stats ? &context : nullptr;
	return SampleRows(cdc, columns, config.in_memory_sample_size, config.seed, expr_exec, *active_filters,
	                  stats_context, phase, table_index, cache);
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
	// must outlive every SampleCollection call below — keep it at function scope.
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
	return SampleCollection(context_, cdc, config_, expr_exec.get(), empty_filters, "init",
	                        chunk_get.table_index.index);
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
			std::cerr << "  [Bloom-SampleEstimate] table=" << sample.table_name
			          << " sample_rows=0 local_survivors=0 transfer_survivors=0 estimate=" << fallback
			          << " filters=" << filters.size() << " status=fallback" << '\n';
		}
		return fallback;
	}
	samples_.EnsureLocalFilter(sample, op);

	// Resolve BF filters to sample chunk columns (same order in
	// locally_filtered).
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

	// Survivors after local predicate (+ BF). BF pass scans only the small local
	// set.
	idx_t survivors = sample.local_survivors;
	auto probe_started = std::chrono::steady_clock::now();
	if (!bf_filters.empty() && sample.locally_filtered && sample.local_survivors > 0) {
		vector<column_t> columns;
		for (idx_t col = 0; col < sample.locally_filtered->Types().size(); col++) {
			columns.push_back(col);
		}
		survivors = ScanCollection(*sample.locally_filtered, columns, nullptr, bf_filters);
	}
	if (LogEnabled()) {
		auto elapsed =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - probe_started).count();
		std::cerr << "  [Bloom-SampleEvaluate] table=" << sample.table_name
		          << " phase=transfer filters=" << bf_filters.size() << " input=" << sample.local_survivors
		          << " survivors=" << survivors << " elapsed=" << elapsed << "ms" << '\n';
	}

	// Extrapolate to the full table. A 0 estimate is special downstream (treated
	// as EmptyResult), so only return 0 when we sampled the WHOLE table — a
	// partial sample can miss a rare-but-present match, and reporting empty would
	// wrongly drop all rows. With 0 survivors in a partial sample, report the
	// weight of a single sample row (total/sample) rather than 1: a hard floor of
	// 1 would tie with genuinely single-row tables (whose full-sample estimate is
	// exact) and mislead the excitation ordering into flooding an uncertain table
	// first.
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
		std::cerr << "  [Bloom-SampleEstimate] table=" << sample.table_name << " sample_rows=" << sample.sampled_rows
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
	// Sampling reads RAW chunks from the CDC, but the scanner's filter
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
	const auto &bindings = scanner.GetOutputBindings();
	const auto table_index = bindings.empty() ? DConstants::INVALID_INDEX : bindings[0].table_index.index;
	auto cache = &samples_.GetCDCEntry(scanner.GetDataReference(), table_index);
	return SampleCollection(context_, *cdc, config_, expr_exec, *filters, "transfer", table_index, cache);
}

} // namespace duckdb
