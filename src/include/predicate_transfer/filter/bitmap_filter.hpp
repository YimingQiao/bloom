//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/filter/bitmap_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstdint>

#include "filter.hpp"

#ifndef BF_RESTRICT
#if defined(_MSC_VER)
#define BF_RESTRICT __restrict
#elif defined(__GNUC__) || defined(__clang__)
#define BF_RESTRICT __restrict__
#else
// Fallback: just return the pointer as-is
#define BF_RESTRICT
#endif
#endif

namespace duckdb {

class BitmapFilterConfig {
public:
	int64_t lower_bound = 0;
	int64_t upper_bound = 0;

	BitmapFilterConfig(int64_t lower_bound, int64_t upper_bound) : lower_bound(lower_bound), upper_bound(upper_bound) {
	}

	BitmapFilterConfig() = default;
};

class BitmapFilter : public RPTFilter {
public:
	BitmapFilter(ClientContext &context_p, const BitmapFilterConfig &config);

	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, SelectionVector &results,
	           size_t &result_count) const override;

	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, Vector &results,
	           size_t &result_count) const override;

	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) override;
	void InsertTaskLocal(DataChunk &chunk, const vector<idx_t> &bound_cols_built);
	void MergeTaskLocal(const BitmapFilter &other);

	FilterPropagateResult CheckStatistics(const BaseStatistics &stats) override;
	size_t Hash() const override;
	idx_t MemoryUsage() const override {
		return 64 + BlockCount() * sizeof(uint64_t);
	}
	optional_idx ExactDistinctCount() const override;
	string ToString() const override;

private:
	static constexpr int32_t SIMD_BATCH_SIZE = 16;

	template <typename T>
	int BitmapFilterLookup(int num, const_data_ptr_t BF_RESTRICT keys_ori, const uint64_t *BF_RESTRICT bf,
	                       const ValidityMask &validity, SelectionVector &results, size_t &result_count) const;

	template <typename T>
	int BitmapFilterLookup(int num, const_data_ptr_t BF_RESTRICT keys_ori, const uint64_t *BF_RESTRICT bf,
	                       const ValidityMask &validity, Vector &results, size_t &result_count) const;

	template <typename T, bool PARALLEL>
	void BitmapFilterInsert(int num, const_data_ptr_t BF_RESTRICT keys_ori, const ValidityMask &validity,
	                        uint64_t *BF_RESTRICT bf) const;
	template <bool PARALLEL>
	void InsertInternal(DataChunk &chunk, const vector<idx_t> &bound_cols_built);

	template <typename T>
	FilterPropagateResult BitmapFilterRangeLookup(const BaseStatistics &stats, const uint64_t *BF_RESTRICT bf) const;

	idx_t BlockCount() const;

	uint64_t *blocks = nullptr;
	AllocatedData buf_;
	BitmapFilterConfig config_;
};

} // namespace duckdb
