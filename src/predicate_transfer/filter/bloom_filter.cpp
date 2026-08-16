#include "predicate_transfer/filter/bloom_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cmath>
#include <limits>

namespace duckdb {
namespace {
static uint32_t CeilPowerOfTwo(uint32_t n) {
	if (n <= 1) {
		return 1;
	}
	n--;
	n |= n >> 1U;
	n |= n >> 2U;
	n |= n >> 4U;
	n |= n >> 8U;
	n |= n >> 16U;
	return n + 1;
}

static Vector HashColumns(DataChunk &chunk, const vector<idx_t> &cols) {
	auto count = chunk.size();
	Vector hashes(LogicalType::HASH);
	hashes.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	VectorOperations::Hash(chunk.data[cols[0]], hashes, count);
	for (size_t j = 1; j < cols.size(); j++) {
		VectorOperations::CombineHash(hashes, chunk.data[cols[j]], count);
	}
	hashes.Flatten();
	// Nested hash implementations flatten the result but do not set its
	// logical size. BloomFilter::InsertHashes iterates Vector::Values(), which
	// uses that size; without this, LIST/ARRAY keys insert zero hashes.
	FlatVector::SetSize(hashes, count_t(count));
	return hashes;
}
} // namespace

//===----------------------------------------------------------------------===//
// CacheSectorizedBF
//===----------------------------------------------------------------------===//

CacheSectorizedBF::CacheSectorizedBF(ClientContext &context_p, const BloomFilterConfig &config, uint32_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	uint32_t raw_min_bits = est_num_rows * config.bits_per_key;
	uint32_t min_bits = MIN_NUM_BITS > raw_min_bits ? MIN_NUM_BITS : raw_min_bits;
	uint32_t raw_sectors = CeilPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE;
	num_sectors = raw_sectors < MAX_NUM_SECTORS ? raw_sectors : MAX_NUM_SECTORS;
	num_sectors_log = static_cast<uint32_t>(std::log2(num_sectors));

	buf_ = buffer_manager->GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
	blocks = reinterpret_cast<uint32_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(blocks, num_sectors, 0);
}

int CacheSectorizedBF::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, SelectionVector &results,
                              size_t &result_count) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	auto hash_ptr = reinterpret_cast<const uint64_t *>(FlatVector::GetData(hashes));
	BloomFilterLookup(count, hash_ptr, blocks, results, result_count);
	return count;
}

int CacheSectorizedBF::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, Vector &results,
                              size_t &result_count) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	BloomFilterLookup(count, reinterpret_cast<const uint64_t *>(FlatVector::GetData(hashes)), blocks, results,
	                  result_count);
	return count;
}

void CacheSectorizedBF::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_built);
	auto hash_ptr = reinterpret_cast<const uint64_t *>(FlatVector::GetData(hashes));
	BloomFilterInsert(count, hash_ptr, blocks);
}

size_t CacheSectorizedBF::Hash() const {
	auto hash_combine = [](size_t h1, size_t h2) {
		return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + (h1 << 6U) + (h1 >> 2U));
	};
	size_t hash = std::hash<uint32_t> {}(num_sectors);
	hash = hash_combine(hash, std::hash<uint32_t> {}(num_sectors_log));
	hash = hash_combine(hash, std::hash<bool> {}(finalized_));
	for (idx_t i = 0; i < num_sectors; i++) {
		hash = hash_combine(hash, std::hash<uint32_t> {}(blocks[i]));
	}
	return hash;
}

//===----------------------------------------------------------------------===//
// DuckDBBloomFilterAdapter
//===----------------------------------------------------------------------===//

DuckDBBloomFilterAdapter::DuckDBBloomFilterAdapter(ClientContext &context_p, const BloomFilterConfig &config,
                                                   uint32_t est_num_rows) {
	bf_.Initialize(context_p, static_cast<idx_t>(est_num_rows));
}

int DuckDBBloomFilterAdapter::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied,
                                     SelectionVector &results, size_t &result_count) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	result_count = bf_.LookupHashes(hashes, results, count);
	return count;
}

int DuckDBBloomFilterAdapter::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, Vector &results,
                                     size_t &result_count) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);

	SelectionVector sel(count);
	idx_t found = bf_.LookupHashes(hashes, sel, count);

	auto result_data = FlatVector::GetDataMutable<bool>(results);
	std::fill_n(result_data, count, false);
	for (idx_t i = 0; i < found; i++) {
		result_data[sel.get_index(i)] = true;
	}
	result_count = found;
	return count;
}

void DuckDBBloomFilterAdapter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	Vector hashes = HashColumns(chunk, bound_cols_built);
	bf_.InsertHashes(hashes);
}

size_t DuckDBBloomFilterAdapter::Hash() const {
	return std::hash<string> {}("DuckDBBloomFilter");
}

DuckDBPrefixRangeFilterAdapter::DuckDBPrefixRangeFilterAdapter(ClientContext &context, const LogicalType &key_type,
                                                               int64_t lower_bound, int64_t upper_bound,
                                                               idx_t row_count)
    : context_(context), lower_bound_(lower_bound), upper_bound_(upper_bound),
      filter_(PrefixRangeFilter::CreatePrefixRangeFilter(key_type)) {
	if (!filter_) {
		throw InternalException("RPT: DuckDB prefix-range filter does not support type %s", key_type.ToString());
	}
	D_ASSERT(lower_bound <= upper_bound);
	auto min_value = Value::BIGINT(lower_bound).DefaultCastAs(key_type);
	auto max_value = Value::BIGINT(upper_bound).DefaultCastAs(key_type);
	const auto span = static_cast<uint64_t>(upper_bound) - static_cast<uint64_t>(lower_bound);
	const auto max_width = std::numeric_limits<idx_t>::max();
	D_ASSERT(span < max_width);
	auto width = static_cast<idx_t>(span + 1);
	filter_->Initialize(context, row_count, min_value, max_value, width);
}

int DuckDBPrefixRangeFilterAdapter::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols, SelectionVector &results,
                                           size_t &result_count) const {
	if (bound_cols.size() != 1) {
		throw InternalException("RPT: prefix-range filter requires one key column");
	}
	result_count = filter_->LookupKeys(chunk.data[bound_cols[0]], results, chunk.size());
	return static_cast<int>(chunk.size());
}

int DuckDBPrefixRangeFilterAdapter::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols, Vector &results,
                                           size_t &result_count) const {
	SelectionVector selected(chunk.size());
	Lookup(chunk, bound_cols, selected, result_count);
	auto result_data = FlatVector::GetDataMutable<bool>(results);
	std::fill_n(result_data, chunk.size(), false);
	for (idx_t i = 0; i < result_count; i++) {
		result_data[selected.get_index(i)] = true;
	}
	return static_cast<int>(chunk.size());
}

void DuckDBPrefixRangeFilterAdapter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols) {
	if (bound_cols.size() != 1) {
		throw InternalException("RPT: prefix-range filter requires one key column");
	}
	if (!build_state_) {
		build_state_ = filter_->InitializeBuildState(context_);
	}
	filter_->InsertKeys(chunk.data[bound_cols[0]], *build_state_);
}

unique_ptr<PrefixRangeFilter::BuildState>
DuckDBPrefixRangeFilterAdapter::InitializeBuildState(ClientContext &context) const {
	return filter_->InitializeBuildState(context);
}

idx_t DuckDBPrefixRangeFilterAdapter::GetBuildStateSize() const {
	return filter_->GetBuildStateSize();
}

void DuckDBPrefixRangeFilterAdapter::InsertBuildState(DataChunk &chunk, const vector<idx_t> &bound_cols,
                                                      PrefixRangeFilter::BuildState &state, bool parallel) const {
	if (bound_cols.size() != 1) {
		throw InternalException("RPT: prefix-range filter requires one key column");
	}
	if (parallel) {
		filter_->InsertKeysParallel(chunk.data[bound_cols[0]], state);
	} else {
		filter_->InsertKeys(chunk.data[bound_cols[0]], state);
	}
}

void DuckDBPrefixRangeFilterAdapter::MergeBuildState(PrefixRangeFilter::BuildState &state) {
	filter_->MergeBuildState(state);
}

void DuckDBPrefixRangeFilterAdapter::SetValid() {
	if (build_state_) {
		filter_->MergeBuildState(*build_state_);
		build_state_.reset();
	}
	RPTFilter::SetValid();
}

size_t DuckDBPrefixRangeFilterAdapter::Hash() const {
	auto hash = std::hash<int64_t> {}(lower_bound_);
	return hash ^ (std::hash<int64_t> {}(upper_bound_) + 0x9e3779b97f4a7c15ULL + (hash << 6U) + (hash >> 2U));
}

string DuckDBPrefixRangeFilterAdapter::ToString() const {
	return StringUtil::Format("DuckDBPrefixRangeFilter[%lld..%lld]", lower_bound_, upper_bound_);
}

} // namespace duckdb
