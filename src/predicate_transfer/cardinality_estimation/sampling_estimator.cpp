#include "predicate_transfer/cardinality_estimation/sampling_estimator.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <cmath>
#include <iostream>

namespace duckdb {

static bool RptSamplingLogEnabled(ClientContext &context) {
	Value setting;
	return context.TryGetCurrentSetting("rpt_log_transfer_steps", setting) && setting.GetValue<bool>();
}

// True if expr still references a column by binding (BOUND_COLUMN_REF) that was
// not rewritten to a sample-chunk BoundReferenceExpression. Such an expression
// cannot be evaluated by a bare ExpressionExecutor (no column resolution), so we
// skip it during estimation — conservatively over-counting survivors, which is
// always safe (the real filters are built from full data in Phase 2).
static bool HasUnboundColumnRef(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (HasUnboundColumnRef(child)) {
			found = true;
		}
	});
	return found;
}

// Rewrite every BOUND_COLUMN_REF whose binding appears in `bindings` to a
// BoundReferenceExpression on the paired positions[i]; unmatched refs are left
// as-is (callers detect them with HasUnboundColumnRef).
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

// Stock DuckDB represents optional table filters as scalar-function wrappers.
// Those wrappers are scan-only markers: the non-selectivity variant evaluates
// to TRUE in a generic ExpressionExecutor, while the real predicate lives in
// bind data. Sampling must execute that predicate, as the native fork's legacy
// OptionalFilter::ToExpression did.
static unique_ptr<Expression> UnwrapOptionalFilterExpressions(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? UnwrapOptionalFilterExpressions(*data.child_filter_expr)
			                              : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? UnwrapOptionalFilterExpressions(*data.child_filter_expr)
			                              : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
	}
	auto result = expr.Copy();
	ExpressionIterator::EnumerateChildren(
	    *result, [&](unique_ptr<Expression> &child) { child = UnwrapOptionalFilterExpressions(*child); });
	return result;
}

// ---------------------------------------------------------------------------
// Chunk-level sampling on a ColumnDataCollection (for CDC / in-memory data)
// ---------------------------------------------------------------------------

static idx_t SampleChunks(const ColumnDataCollection &cdc, double sample_rate, ExpressionExecutor *expr_exec,
                          const vector<TableScanner::FilterEntry> &filters) {
	idx_t total_chunks = cdc.ChunkCount();
	if (total_chunks == 0) {
		return 0;
	}

	idx_t target_chunks = total_chunks;
	if (sample_rate > 0.0 && sample_rate < 1.0) {
		target_chunks = MaxValue<idx_t>(
		    1, MinValue<idx_t>(total_chunks, static_cast<idx_t>(std::ceil(total_chunks * sample_rate))));
	}

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), cdc.Types());
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
	auto chunk_bindings = const_cast<LogicalColumnDataGet &>(chunk_get).GetColumnBindings();
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
	static const vector<TableScanner::FilterEntry> empty_filters;
	idx_t v = SampleChunks(cdc, sample_rate_, expr_exec.get(), empty_filters);
	if (v == 0 && cdc.Count() > 0) {
		// A 1% chunk sample can miss a rare survivor; downstream treats 0 as
		// EmptyResult. In-memory data is cheap to rescan — confirm exactly.
		v = SampleChunks(cdc, 1.0, expr_exec.get(), empty_filters);
	}
	return v;
}

// ---------------------------------------------------------------------------
// Per-table sample caches (in-memory + on-disk)
// ---------------------------------------------------------------------------
//
// A raw-data sample depends only on the table identity/schema fingerprint and
// (method, N) — it is query-independent, so it is persisted once and reused
// across queries and process invocations. Each sample is its own file, so
// concurrent processes sampling different tables never conflict; same-table
// races resolve idempotently (identical content).
//
// On top of the disk cache, loaded/generated samples are published to the
// DatabaseInstance ObjectCache so every later optimization in the same process
// (repeated PREPAREs, other queries touching the same tables) reuses the
// deserialized ColumnDataCollection directly. Sample CDCs are allocated from
// Allocator::DefaultAllocator() (both the deserialize and the collect path),
// so their lifetime is safe beyond the query; the ObjectCache accounts them
// against the buffer pool and can evict them under memory pressure.

namespace {
class RPTSampleObjectCacheEntry : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rpt_sample";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return cdc ? optional_idx(cdc->AllocationSize()) : optional_idx(0);
	}
	explicit RPTSampleObjectCacheEntry(shared_ptr<ColumnDataCollection> cdc_p) : cdc(std::move(cdc_p)) {
	}

	shared_ptr<ColumnDataCollection> cdc;
};
} // namespace

string SamplingCardinalityEstimator::SampleCacheKey(const LogicalGet &get) {
	auto tbl = get.GetTable();
	if (!tbl) {
		return ""; // not a base table (e.g. table function) — no stable key
	}
	// The sample always holds ALL table columns in storage order, so the key is
	// column-set independent: exactly one sample per (table, method, N).
	// v3 = v2 chunk stream plus stronger identity. The row count and logical
	// types prevent stale samples after common benchmark/database swaps or
	// ALTER TYPE operations; same-row-count content changes remain a cache
	// invalidation responsibility of the caller.
	auto &catalog = tbl->ParentCatalog();
	string key = "rpt_sample_v3|" + catalog.GetName();
	key += "|path=" + catalog.GetDBPath();
	key += "|" + tbl->schema.name + "|" + tbl->name;
	key += "|rows=" + std::to_string(tbl->GetStorage().GetTotalRows());
	key += "|types=";
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		if (i > 0) {
			key += ',';
		}
		key += get.returned_types[i].ToString();
	}
	key += "|n=" + std::to_string(sample_target_);
	return key;
}

string SamplingCardinalityEstimator::SampleCachePath(const LogicalGet &get, const string &key) {
	if (cache_dir_.empty() || key.empty()) {
		return "";
	}
	string cache_dir = cache_dir_;
	if (StringUtil::CIEquals(cache_dir, "auto")) {
		auto table = get.GetTable();
		if (!table) {
			return "";
		}
		auto database_path = table->ParentCatalog().GetDBPath();
		if (database_path.empty() || database_path == ":memory:") {
			return "";
		}
		cache_dir = database_path + ".rpt_samples";
	}
	auto h = std::hash<string> {}(key);
	auto &fs = FileSystem::GetFileSystem(context_);
	return fs.JoinPath(cache_dir, StringUtil::Format("%016llx.sample", static_cast<unsigned long long>(h)));
}

shared_ptr<ColumnDataCollection> SamplingCardinalityEstimator::GetFromMemoryCache(const string &key) {
	if (!memory_cache_ || key.empty()) {
		return nullptr;
	}
	auto cached = ObjectCache::GetObjectCache(context_).Get<RPTSampleObjectCacheEntry>(key);
	return cached ? cached->cdc : nullptr;
}

void SamplingCardinalityEstimator::PublishToMemoryCache(const string &key,
                                                        const shared_ptr<ColumnDataCollection> &cdc) {
	if (!memory_cache_ || key.empty() || !cdc) {
		return;
	}
	ObjectCache::GetObjectCache(context_).Put(key, make_shared_ptr<RPTSampleObjectCacheEntry>(cdc));
}

// Disk format: the sample's identity key, its types, and the chunk stream
// serialized with DataChunk::Serialize — DuckDB's flat columnar binary path
// (validity bitmap + data buffers). ColumnDataCollection::Serialize is
// deliberately NOT used: it round-trips every cell through a boxed Value
// (per-cell heap traffic), which made loading a 10k-row sample cost tens of
// milliseconds instead of ~memcpy speed.

shared_ptr<ColumnDataCollection> SamplingCardinalityEstimator::LoadSampleFromDisk(const string &key,
                                                                                  const string &path) {
	if (path.empty()) {
		return nullptr;
	}
	auto &fs = FileSystem::GetFileSystem(context_);
	if (!fs.FileExists(path)) {
		return nullptr;
	}
	try {
		BufferedFileReader reader(fs, path.c_str());
		if (reader.Finished()) {
			return nullptr;
		}
		BinaryDeserializer deserializer(reader);
		deserializer.Begin();
		auto disk_key = deserializer.ReadProperty<string>(100, "key");
		if (disk_key != key) {
			return nullptr;
		}
		auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
		auto chunk_count = deserializer.ReadProperty<idx_t>(102, "chunk_count");
		auto cdc = make_shared_ptr<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		deserializer.ReadList(103, "chunks", [&](Deserializer::List &list, idx_t i) {
			list.ReadObject([&](Deserializer &obj) {
				DataChunk chunk;
				chunk.Deserialize(obj);
				cdc->Append(chunk);
			});
		});
		deserializer.End();
		if (cdc->ChunkCount() != chunk_count) {
			return nullptr;
		}
		return cdc;
	} catch (...) {
		return nullptr; // corrupt/partial/old-format file — regenerate
	}
}

void SamplingCardinalityEstimator::SaveSampleToDisk(const string &key, const string &path,
                                                    const ColumnDataCollection &cdc) {
	if (path.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context_);
	try {
		auto cache_directory = path.substr(0, path.size() - fs.ExtractName(path).size());
		auto separator = fs.PathSeparator(cache_directory);
		if (StringUtil::EndsWith(cache_directory, separator)) {
			cache_directory.resize(cache_directory.size() - separator.size());
		}
		if (!cache_directory.empty() && !fs.DirectoryExists(cache_directory)) {
			fs.CreateDirectory(cache_directory);
		}
		// Write to a per-instance temp file, then publish via rename so a reader
		// never observes a half-written sample.
		string tmp = path + "." + StringUtil::GenerateRandomName(16) + ".tmp";
		{
			BufferedFileWriter writer(fs, tmp);
			BinarySerializer serializer(writer);
			serializer.Begin();
			serializer.WriteProperty(100, "key", key);
			serializer.WriteProperty(101, "types", cdc.Types());
			serializer.WriteProperty(102, "chunk_count", cdc.ChunkCount());
			DataChunk scratch;
			cdc.InitializeScanChunk(scratch);
			serializer.WriteList(103, "chunks", cdc.ChunkCount(), [&](Serializer::List &list, idx_t i) {
				scratch.Reset();
				cdc.FetchChunk(i, scratch);
				list.WriteObject([&](Serializer &obj) { scratch.Serialize(obj, false); });
			});
			serializer.End();
			writer.Flush();
		}
		if (fs.FileExists(path)) {
			if (LoadSampleFromDisk(key, path)) {
				fs.RemoveFile(tmp); // another process already published — drop ours
			} else {
				// Existing file is corrupt or belongs to a colliding/stale key.
				// Replace it so the cache heals instead of regenerating forever.
				fs.RemoveFile(path);
				fs.MoveFile(tmp, path);
			}
		} else {
			fs.MoveFile(tmp, path);
		}
	} catch (...) {
		// best-effort: a caching failure must never break estimation
	}
}

// ---------------------------------------------------------------------------
// GetOrCreateSample — materialize a table-level RAW-DATA sample for a disk
// table. The sample carries NO local predicate; local WHERE and BF filters are
// applied later in EstimateOnSample and extrapolated by total/sample.
// ---------------------------------------------------------------------------

SamplingCardinalityEstimator::SampleEntry &SamplingCardinalityEstimator::GetOrCreateSample(const LogicalOperator &op) {
	auto it = sample_cache_.find(&op);
	if (it != sample_cache_.end()) {
		return it->second;
	}
	auto &entry = sample_cache_[&op];

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

	// Raw FULL-COLUMN table scan: copy the GET, strip any pushed-down local
	// predicate (re-applied on the sample at estimation time), clear
	// projection_ids, and widen column_ids to every table column in storage
	// order. The stored sample is therefore query-independent — exactly one
	// per (table, method, N). Queries read it with column pruning (CDC subset
	// scan over needed_columns in EnsureLocalFiltered), so per-query cost does
	// not grow with table width.
	auto scan = leaf->Copy(context_);
	auto &scan_get = scan->Cast<LogicalGet>();
	scan_get.table_filters.ClearFilters();
	scan_get.projection_ids.clear();
	const idx_t full_width = scan_get.returned_types.size();
	{
		auto &ids = scan_get.GetMutableColumnIds();
		ids.clear();
		for (idx_t s = 0; s < full_width; s++) {
			ids.emplace_back(s);
		}
	}

	// Per-query narrow view: sample column s (storage order) is referenced by
	// the query binding whose column_ids[i] has storage id s. Unreferenced
	// columns are pruned at read time. Virtual columns (rowid) cannot be
	// sampled — predicates on them are skipped downstream, which over-counts
	// survivors and is correctness-safe.
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
	vector<column_t> needed;
	vector<ColumnBinding> narrow_bindings;
	for (idx_t s = 0; s < full_width; s++) {
		if (full_bindings[s].column_index.IsValid()) {
			needed.push_back(s);
			narrow_bindings.push_back(full_bindings[s]);
		}
	}

	string cache_key = SampleCacheKey(scan_get);
	string cache_path = SampleCachePath(scan_get, cache_key);

	entry.total_rows = total_rows;
	entry.needed_columns = std::move(needed);
	entry.output_bindings = std::move(narrow_bindings);

	// Fastest path: an earlier optimization in this process already holds the
	// deserialized sample.
	if (auto cached = GetFromMemoryCache(cache_key)) {
		entry.sample_cdc = std::move(cached);
		entry.sample_row_count = entry.sample_cdc->Count();
		return entry;
	}

	// Fast path: a matching sample was persisted by an earlier query/run.
	if (auto cached = LoadSampleFromDisk(cache_key, cache_path)) {
		entry.sample_cdc = std::move(cached);
		entry.sample_row_count = entry.sample_cdc->Count();
		PublishToMemoryCache(cache_key, entry.sample_cdc);
		return entry;
	}

	// Wrap to limit the sample to ~sample_target_ rows according to the method.
	unique_ptr<LogicalOperator> sample_plan;
	if (total_rows == 0 || total_rows <= sample_target_) {
		// Small table — the raw scan IS the sample (extrapolation factor 1).
		sample_plan = std::move(scan);
	} else {
		// Reservoir sample of ~sample_target_ random rows (unbiased).
		auto opts = make_uniq<SampleOptions>();
		opts->sample_size = Value::BIGINT(static_cast<int64_t>(sample_target_));
		opts->is_percentage = false;
		opts->method = SampleMethod::RESERVOIR_SAMPLE;
		sample_plan = make_uniq<LogicalSample>(std::move(opts), std::move(scan));
	}

	sample_plan->ResolveOperatorTypes();
	shared_ptr<ColumnDataCollection> sample_cdc;
	try {
		sample_cdc = ExecutePlanAndCollect(context_, std::move(sample_plan));
	} catch (...) {
		// A scan that cannot run with widened column_ids (exotic table
		// function). Leave the entry sample-less — EstimateOnSample falls back
		// to the operator's own estimate (floored at 1), which is safe.
	}

	entry.sample_cdc = std::move(sample_cdc);
	entry.sample_row_count = entry.sample_cdc ? entry.sample_cdc->Count() : 0;

	// Persist for future optimizations in this process and future runs.
	if (entry.sample_cdc && entry.sample_row_count > 0) {
		PublishToMemoryCache(cache_key, entry.sample_cdc);
		SaveSampleToDisk(cache_key, cache_path, *entry.sample_cdc);
	}
	return entry;
}

// ---------------------------------------------------------------------------
// EstimateOnSample — apply op's local predicate (LogicalFilter expressions +
// GET.table_filters) plus BF filters to the raw sample, count survivors, and
// extrapolate to the full table by total_rows / sample_row_count.
// ---------------------------------------------------------------------------

void SamplingCardinalityEstimator::EnsureLocalFiltered(SampleEntry &sample, const LogicalOperator &op) {
	if (sample.local_computed) {
		return;
	}
	sample.local_computed = true;

	// The query references no sampleable column (e.g. bare count(*) or
	// virtual-only) — nothing can be evaluated; keep the raw count (safe
	// over-count) and no BF-probe set.
	if (sample.needed_columns.empty()) {
		sample.local_cdc = nullptr;
		sample.local_survivors = sample.sample_row_count;
		return;
	}

	// Column-pruned view of the full-width sample: only the query's referenced
	// columns are read (CDC subset scan), so estimation cost matches a
	// per-query narrow sample. Narrow position k holds storage column
	// needed_columns[k]; output_bindings[k] is its query binding.
	const auto &full_types = sample.sample_cdc->Types();
	vector<LogicalType> sample_types;
	sample_types.reserve(sample.needed_columns.size());
	for (auto s : sample.needed_columns) {
		sample_types.push_back(full_types[s]);
	}

	// Narrow position i holds output_bindings[i], so a matching ref rewrites to
	// its own index into the pruned view.
	vector<idx_t> narrow_positions;
	narrow_positions.reserve(sample.output_bindings.size());
	for (idx_t i = 0; i < sample.output_bindings.size(); i++) {
		narrow_positions.push_back(i);
	}

	// 1. Collect local-predicate expressions on the sample columns.
	vector<unique_ptr<Expression>> preds;
	const LogicalGet *get = nullptr;
	const LogicalOperator *node = &op;
	const bool rpt_log = RptSamplingLogEnabled(context_);
	if (rpt_log) {
		std::cerr << "  [RPT-Sample] op=" << static_cast<int>(op.type) << " sample_rows=" << sample.sample_row_count
		          << " needed_storage_cols=(";
		for (idx_t i = 0; i < sample.needed_columns.size(); i++) {
			if (i > 0) {
				std::cerr << ",";
			}
			std::cerr << sample.needed_columns[i] << "->" << sample.output_bindings[i].ToString();
		}
		std::cerr << ")" << '\n';
	}
	while (node) {
		if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
			for (auto &e : node->Cast<LogicalFilter>().expressions) {
				auto cloned = e->Copy();
				auto original = cloned->ToString();
				RewriteRefsToPositions(cloned, sample.output_bindings, narrow_positions);
				// A predicate whose columns all resolve to this sample's table is
				// applied. A leftover BOUND_COLUMN_REF means the predicate references
				// a DIFFERENT input of a join-containing source op (e.g. a base table
				// joined with a materialized CHUNK_GET, with the filter on the other
				// side). Single-table sampling cannot evaluate it, so we skip it —
				// this over-counts survivors, which is always correctness-safe (the
				// real filters are built from full data in Phase 2). It never fires
				// for a clean single-table source op.
				bool unbound = HasUnboundColumnRef(*cloned);
				if (rpt_log) {
					std::cerr << "    [LogicalFilter] " << original << " -> " << cloned->ToString()
					          << (unbound ? " SKIPPED(unbound)" : " APPLIED") << '\n';
				}
				if (!unbound) {
					preds.push_back(std::move(cloned));
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

	// 1b. table_filters live on the GET keyed by projection index; map through
	// GET.column_ids to the storage column, then to the narrow sample view.
	// its narrow-view position (index into needed_columns).
	if (get) {
		if (rpt_log) {
			std::cerr << "    [Get] table_index=" << get->table_index.index
			          << " projected_cols=" << get->GetColumnIds().size()
			          << " table_filters=" << get->table_filters.FilterCount() << '\n';
		}
		for (auto &f : get->table_filters) {
			auto projection_col = f.GetIndex().GetIndex();
			if (projection_col >= get->GetColumnIds().size() || get->GetColumnIds()[projection_col].IsVirtualColumn()) {
				if (rpt_log) {
					std::cerr << "      [TableFilter] projection=" << projection_col << " SKIPPED(out-of-range/virtual)"
					          << '\n';
				}
				continue;
			}
			idx_t storage_col = get->GetColumnIds()[projection_col].GetPrimaryIndex();
			idx_t chunk_col = DConstants::INVALID_INDEX;
			for (idx_t k = 0; k < sample.needed_columns.size(); k++) {
				if (sample.needed_columns[k] == storage_col) {
					chunk_col = k;
					break;
				}
			}
			if (chunk_col == DConstants::INVALID_INDEX || chunk_col >= sample_types.size()) {
				if (rpt_log) {
					std::cerr << "      [TableFilter] projection=" << projection_col << " storage=" << storage_col
					          << " SKIPPED(not-in-sample)" << '\n';
				}
				continue;
			}
			BoundReferenceExpression colref(sample_types[chunk_col], chunk_col);
			auto &expr_filter = f.Filter().Cast<ExpressionFilter>();
			auto predicate = UnwrapOptionalFilterExpressions(*expr_filter.expr);
			ExpressionFilter::ReplaceExpressionRecursive(predicate, colref);
			if (rpt_log) {
				std::cerr << "      [TableFilter] projection=" << projection_col << " storage=" << storage_col
				          << " sample=" << chunk_col << " filter=" << expr_filter.DebugToString()
				          << " expr=" << predicate->ToString() << " APPLIED" << '\n';
			}
			preds.push_back(std::move(predicate));
		}
	}

	// Build a single conjunctive expression executor (kept alive for the scan).
	// With no local predicate the pruned view is still materialized: repeated
	// BF probes during flooding then scan only the narrow columns.
	unique_ptr<Expression> final_expr;
	unique_ptr<ExpressionExecutor> expr_exec;
	if (!preds.empty()) {
		if (preds.size() == 1) {
			final_expr = std::move(preds[0]);
		} else {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &e : preds) {
				conj->GetChildrenMutable().push_back(std::move(e));
			}
			final_expr = std::move(conj);
		}
		expr_exec = make_uniq<ExpressionExecutor>(context_);
		expr_exec->AddExpression(*final_expr);
	}

	// Materialize surviving rows once into the narrow local_cdc via a
	// column-pruned scan of the full-width sample.
	auto local = make_shared_ptr<ColumnDataCollection>(context_, sample_types);
	ColumnDataScanState scan_state;
	sample.sample_cdc->InitializeScan(scan_state, sample.needed_columns);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), sample_types);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t survivors = 0;
	while (true) {
		chunk.Reset();
		if (!sample.sample_cdc->Scan(scan_state, chunk) || chunk.size() == 0) {
			break;
		}
		idx_t count = chunk.size();
		if (expr_exec) {
			count = expr_exec->SelectExpression(chunk, sel);
			if (count == 0) {
				continue;
			}
			if (count < chunk.size()) {
				chunk.Slice(sel, count);
				chunk.Flatten();
			}
		}
		local->Append(chunk);
		survivors += count;
	}
	sample.local_cdc = std::move(local);
	sample.local_survivors = survivors;
	if (rpt_log) {
		std::cerr << "    [RPT-Sample] predicates=" << preds.size() << " survivors=" << survivors << "/"
		          << sample.sample_row_count << '\n';
	}
}

idx_t SamplingCardinalityEstimator::EstimateOnSample(SampleEntry &sample, const LogicalOperator &op,
                                                     const vector<DirectFilterInfo> &filters) {
	if (!sample.sample_cdc || sample.sample_row_count == 0) {
		// Couldn't build a sample — fall back to DuckDB's own estimate. Never
		// report 0 here: a 0 becomes EmptyResult downstream, and without a sample
		// we have no evidence the table is actually empty.
		return MaxValue<idx_t>(op.estimated_cardinality, 1);
	}
	EnsureLocalFiltered(sample, op);

	// Resolve BF filters to sample chunk columns (same order in local_cdc).
	vector<TableScanner::FilterEntry> bf_filters;
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
		TableScanner::FilterEntry e;
		e.bindings.push_back(bf.binding);
		e.chunk_cols.push_back(chunk_col);
		e.filter = bf.filter;
		bf_filters.push_back(std::move(e));
	}

	// Survivors after local predicate (+ BF). BF pass scans only the small local set.
	idx_t survivors = sample.local_survivors;
	if (!bf_filters.empty() && sample.local_cdc && sample.local_survivors > 0) {
		survivors = SampleChunks(*sample.local_cdc, 1.0, nullptr, bf_filters);
	}

	// Extrapolate to the full table. A 0 estimate is special downstream (treated as
	// EmptyResult), so only return 0 when we sampled the WHOLE table — a partial
	// sample can miss a rare-but-present match, and reporting empty would wrongly
	// drop all rows. With 0 survivors in a partial sample, report the weight of a
	// single sample row (total/sample) rather than 1: a hard floor of 1 would tie
	// with genuinely single-row tables (whose full-sample estimate is exact) and
	// mislead the excitation ordering into flooding an uncertain table first.
	idx_t est;
	if (survivors == 0) {
		if (sample.sample_row_count >= sample.total_rows) {
			est = 0;
		} else {
			est = MaxValue<idx_t>(sample.total_rows / sample.sample_row_count, 1);
		}
	} else {
		double frac = static_cast<double>(survivors) / static_cast<double>(sample.sample_row_count);
		est = static_cast<idx_t>(frac * static_cast<double>(sample.total_rows) + 0.5);
		if (est == 0) {
			est = 1;
		}
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
	auto &sample = GetOrCreateSample(op);
	return EstimateOnSample(sample, op, {});
}

idx_t SamplingCardinalityEstimator::Estimate(const LogicalOperator &op, const vector<DirectFilterInfo> &filters) {
	if (IsCDC(op)) {
		return SampleCDC(op);
	}
	auto &sample = GetOrCreateSample(op);
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
	// layout — see PendingExprFilter::scratch — so it needs no remapping.)
	const vector<TableScanner::FilterEntry> *filters = &scanner.GetFilters();
	vector<TableScanner::FilterEntry> remapped_filters;
	if (pf && !pf->projection_map.empty()) {
		remapped_filters = scanner.GetFilters();
		for (auto &entry : remapped_filters) {
			for (auto &cc : entry.chunk_cols) {
				cc = cc < pf->projection_map.size() ? pf->projection_map[cc] : DConstants::INVALID_INDEX;
			}
		}
		filters = &remapped_filters;
	}
	idx_t v = SampleChunks(*cdc, sample_rate_, expr_exec, *filters);
	if (v == 0 && cdc->Count() > 0) {
		// A 1% chunk sample can miss a rare survivor while selective BF filters are
		// applied; a false 0 becomes EmptyResult downstream and wrongly empties the
		// query. In-memory data is cheap to rescan — confirm the 0 exactly.
		v = SampleChunks(*cdc, 1.0, expr_exec, *filters);
	}
	return v;
}

} // namespace duckdb
