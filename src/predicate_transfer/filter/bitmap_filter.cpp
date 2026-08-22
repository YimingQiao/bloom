#include "predicate_transfer/filter/bitmap_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <atomic>

namespace duckdb {

BitmapFilter::BitmapFilter(ClientContext &context_p, const BitmapFilterConfig &config) : config_(config) {
	auto block_count = BlockCount();

	auto &buffer_manager = BufferManager::GetBufferManager(context_p);
	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + block_count * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	blocks = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(blocks, block_count, 0);
}

template <typename T, bool PARALLEL>
void BitmapFilter::BitmapFilterInsert(int num, const_data_ptr_t BF_RESTRICT keys_ori, const ValidityMask &validity,
                                      uint64_t *BF_RESTRICT bf) const {
	const T *keys = reinterpret_cast<const T *>(keys_ori);
	for (int i = 0; i < num; i++) {
		if (!validity.RowIsValid(static_cast<idx_t>(i)) || keys[i] < config_.lower_bound ||
		    keys[i] > config_.upper_bound) {
			continue;
		}
		uint64_t bit_id = static_cast<uint64_t>(keys[i] - config_.lower_bound);
		auto block_idx = bit_id >> 6U;
		auto mask = 1ULL << (bit_id & 63ULL);
		if (PARALLEL) {
			auto &block = *reinterpret_cast<std::atomic<uint64_t> *>(&bf[block_idx]);
			block.fetch_or(mask, std::memory_order_relaxed);
		} else {
			bf[block_idx] |= mask;
		}
	}
}

size_t BitmapFilter::Hash() const {
	auto hash_combine = [](size_t h1, size_t h2) {
		return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + (h1 << 6U) + (h1 >> 2U));
	};
	size_t hash = std::hash<int64_t> {}(config_.lower_bound);
	hash = hash_combine(hash, std::hash<int64_t> {}(config_.upper_bound));
	hash = hash_combine(hash, std::hash<bool> {}(finalized_));

	for (idx_t i = 0; i < BlockCount(); i++) {
		hash = hash_combine(hash, std::hash<uint64_t> {}(blocks[i]));
	}
	return hash;
}

optional_idx BitmapFilter::ExactDistinctCount() const {
	idx_t distinct = 0;
	for (idx_t block_idx = 0; block_idx < BlockCount(); block_idx++) {
#if defined(__GNUC__) || defined(__clang__)
		distinct += static_cast<idx_t>(__builtin_popcountll(blocks[block_idx]));
#else
		auto block = blocks[block_idx];
		while (block != 0) {
			block &= block - 1;
			distinct++;
		}
#endif
	}
	return distinct;
}

idx_t BitmapFilter::BlockCount() const {
	uint64_t bits = config_.lower_bound <= config_.upper_bound ? config_.upper_bound - config_.lower_bound + 1 : 1;
	return static_cast<idx_t>(std::max<uint64_t>((bits + 63) / 64, 1));
}

template <typename T>
int BitmapFilter::BitmapFilterLookup(int num, const_data_ptr_t BF_RESTRICT keys_ori, const uint64_t *BF_RESTRICT bf,
                                     const ValidityMask &validity, SelectionVector &results,
                                     size_t &result_count) const {
	const T *keys = reinterpret_cast<const T *>(keys_ori);
	result_count = 0;
	for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
		bool outs[SIMD_BATCH_SIZE];
		for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
			bool flag = validity.RowIsValid(static_cast<idx_t>(i + j)) && keys[i + j] >= config_.lower_bound &&
			            keys[i + j] <= config_.upper_bound;
			uint64_t bit_id = flag ? keys[i + j] - config_.lower_bound : 0;
			outs[j] = flag ? (blocks[bit_id >> 6U] >> (bit_id & 63ULL) & 1ULL) : false;
		}

		for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
			bool out = outs[j];
			results.set_index(result_count, i + j);
			result_count += out;
		}
	}

	// unaligned tail
	for (int i = num / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE; i < num; i++) {
		bool flag = validity.RowIsValid(static_cast<idx_t>(i)) && keys[i] >= config_.lower_bound &&
		            keys[i] <= config_.upper_bound;
		uint64_t bit_id = flag ? keys[i] - config_.lower_bound : 0;
		bool out = flag ? (blocks[bit_id >> 6U] >> (bit_id & 63ULL) & 1ULL) : false;
		results.set_index(result_count, i);
		result_count += out;
	}
	return num;
}

template <typename T>
int BitmapFilter::BitmapFilterLookup(int num, const_data_ptr_t BF_RESTRICT keys_ori, const uint64_t *BF_RESTRICT bf,
                                     const ValidityMask &validity, Vector &results, size_t &result_count) const {
	const T *keys = reinterpret_cast<const T *>(keys_ori);
	result_count = 0;
	auto result_data = FlatVector::GetDataMutable<bool>(results);
	for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
		bool outs[SIMD_BATCH_SIZE];
		for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
			bool flag = validity.RowIsValid(static_cast<idx_t>(i + j)) && keys[i + j] >= config_.lower_bound &&
			            keys[i + j] <= config_.upper_bound;
			uint64_t bit_id = flag ? keys[i + j] - config_.lower_bound : 0;
			outs[j] = flag ? (blocks[bit_id >> 6U] >> (bit_id & 63ULL) & 1ULL) : false;
		}

		for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
			bool out = outs[j];
			result_data[i + j] = out;
			result_count += out;
		}
	}

	// unaligned tail
	for (int i = num / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE; i < num; i++) {
		bool flag = validity.RowIsValid(static_cast<idx_t>(i)) && keys[i] >= config_.lower_bound &&
		            keys[i] <= config_.upper_bound;
		uint64_t bit_id = flag ? keys[i] - config_.lower_bound : 0;
		bool out = flag ? (blocks[bit_id >> 6U] >> (bit_id & 63ULL) & 1ULL) : false;
		result_data[i] = out;
		result_count += out;
	}
	return num;
}

int BitmapFilter::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, SelectionVector &results,
                         size_t &result_count) const {
	int count = static_cast<int>(chunk.size());
	if (bound_cols_applied.size() != 1) {
		throw InternalException("Bloom: Bitmap Filter needs bound_cols_applied.size() == 1!");
	}
	auto &v = chunk.data[bound_cols_applied[0]];
	v.Flatten();
	const auto &validity = FlatVector::Validity(v);

	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
		BitmapFilterLookup<bool>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT8:
		BitmapFilterLookup<int8_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT16:
		BitmapFilterLookup<int16_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT32:
		BitmapFilterLookup<int32_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT64:
		BitmapFilterLookup<int64_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::UINT8:
		BitmapFilterLookup<uint8_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::UINT16:
		BitmapFilterLookup<uint16_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::UINT32:
		BitmapFilterLookup<uint32_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	default:
		throw InternalException("Bloom: Cannot support this vector type in bitmap filter! column index=%d, type=%s",
		                        bound_cols_applied[0], v.GetType().ToString());
	}

	return count;
}

int BitmapFilter::Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, Vector &results,
                         size_t &result_count) const {
	int count = static_cast<int>(chunk.size());
	if (bound_cols_applied.size() != 1) {
		throw InternalException("Bloom: Bitmap Filter needs bound_cols_applied.size() == 1!");
	}
	auto &v = chunk.data[bound_cols_applied[0]];
	v.Flatten();
	const auto &validity = FlatVector::Validity(v);

	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
		BitmapFilterLookup<bool>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT8:
		BitmapFilterLookup<int8_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT16:
		BitmapFilterLookup<int16_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT32:
		BitmapFilterLookup<int32_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::INT64:
		BitmapFilterLookup<int64_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::UINT8:
		BitmapFilterLookup<uint8_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::UINT16:
		BitmapFilterLookup<uint16_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	case PhysicalType::UINT32:
		BitmapFilterLookup<uint32_t>(count, FlatVector::GetData(v), blocks, validity, results, result_count);
		break;
	default:
		throw InternalException("Bloom: Cannot support this vector type in bitmap filter! column index=%d, type=%s",
		                        bound_cols_applied[0], v.GetType().ToString());
	}

	return count;
}

template <bool PARALLEL>
void BitmapFilter::InsertInternal(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	int count = static_cast<int>(chunk.size());
	if (bound_cols_built.size() != 1) {
		throw InternalException("Bloom: Bitmap Filter needs bound_cols_applied.size() == 1!");
	}

	auto &v = chunk.data[bound_cols_built[0]];
	v.Flatten();
	auto &validity = FlatVector::Validity(v);

	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
		BitmapFilterInsert<bool, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::INT8:
		BitmapFilterInsert<int8_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::INT16:
		BitmapFilterInsert<int16_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::INT32:
		BitmapFilterInsert<int32_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::INT64:
		BitmapFilterInsert<int64_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::UINT8:
		BitmapFilterInsert<uint8_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::UINT16:
		BitmapFilterInsert<uint16_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	case PhysicalType::UINT32:
		BitmapFilterInsert<uint32_t, PARALLEL>(count, FlatVector::GetData(v), validity, blocks);
		break;
	default:
		throw InternalException("Bloom: Cannot support this vector type in bitmap filter!");
	}
}

void BitmapFilter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	InsertInternal<true>(chunk, bound_cols_built);
}

void BitmapFilter::InsertTaskLocal(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	InsertInternal<false>(chunk, bound_cols_built);
}

void BitmapFilter::MergeTaskLocal(const BitmapFilter &other) {
	D_ASSERT(config_.lower_bound == other.config_.lower_bound);
	D_ASSERT(config_.upper_bound == other.config_.upper_bound);
	D_ASSERT(BlockCount() == other.BlockCount());
	for (idx_t block_idx = 0; block_idx < BlockCount(); block_idx++) {
		blocks[block_idx] |= other.blocks[block_idx];
	}
}

template <typename T>
FilterPropagateResult BitmapFilter::BitmapFilterRangeLookup(const BaseStatistics &stats,
                                                            const uint64_t *BF_RESTRICT bf) const {
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	T min_value = NumericStats::GetMin<T>(stats);
	T max_value = NumericStats::GetMax<T>(stats);
	if (min_value > max_value) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Invalid stats
	}
	if (max_value < config_.lower_bound || min_value > config_.upper_bound) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	uint64_t L = min_value < config_.lower_bound ? 0 : min_value - config_.lower_bound;
	uint64_t R = std::min<uint64_t>(config_.upper_bound - config_.lower_bound, max_value - config_.lower_bound);
	for (uint64_t i = L; i < (L + 63) && i <= R; i++) {
		if (bf[i >> 6U] >> (i & 63ULL) & 1ULL) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}
	for (uint64_t i = R; i > (R - 63) && i >= L; i--) {
		if (bf[i >> 6U] >> (i & 63ULL) & 1ULL) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}

	L = (L / 64 + 1) * 64;
	R = R / 64 * 64;

	for (uint64_t i = L; i < R; i += 64) {
		if (bf[i >> 6U]) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}

	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

FilterPropagateResult BitmapFilter::CheckStatistics(const BaseStatistics &stats) {
	switch (stats.GetType().InternalType()) {
	case PhysicalType::INT8:
		return BitmapFilterRangeLookup<int8_t>(stats, blocks);
	case PhysicalType::INT16:
		return BitmapFilterRangeLookup<int16_t>(stats, blocks);
	case PhysicalType::INT32:
		return BitmapFilterRangeLookup<int32_t>(stats, blocks);
	case PhysicalType::INT64:
		return BitmapFilterRangeLookup<int64_t>(stats, blocks);
	case PhysicalType::UINT8:
		return BitmapFilterRangeLookup<uint8_t>(stats, blocks);
	case PhysicalType::UINT16:
		return BitmapFilterRangeLookup<uint16_t>(stats, blocks);
	case PhysicalType::UINT32:
		return BitmapFilterRangeLookup<uint32_t>(stats, blocks);
	// case PhysicalType::INT128:
	// 	return BitmapFilterRangeLookup<hugeint_t>(stats, blocks);
	// case PhysicalType::UINT128:
	// 	return BitmapFilterRangeLookup<uhugeint_t>(stats, blocks);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

string BitmapFilter::ToString() const {
	idx_t set_count = 0;
	idx_t width = static_cast<idx_t>(config_.upper_bound - config_.lower_bound + 1);
	for (idx_t id = 0; id < width; id++) {
		if (blocks[id >> 6U] >> (id & 63ULL) & 1ULL) {
			set_count++;
		}
	}
	stringstream ss;
	ss << "BitmapFilter[range=" << config_.lower_bound << ".." << config_.upper_bound << ", width=" << width
	   << ", set=" << set_count << "]";
	return ss.str();
}

} // namespace duckdb
