//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp" // DuckDB's built-in BloomFilter
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <cstdint>
#include <atomic>

#include "filter.hpp"

#ifndef BF_RESTRICT
#if defined(_MSC_VER)
#define BF_RESTRICT __restrict
#elif defined(__GNUC__) || defined(__clang__)
#define BF_RESTRICT __restrict__
#else
#define BF_RESTRICT
#endif
#endif

// Define RPT_USE_DUCKDB_BF to use DuckDB's built-in bloom filter.
// Undefine to use CacheSectorizedBF (RPT's split-sector bloom filter).
#define RPT_USE_DUCKDB_BF

namespace duckdb {

class BloomFilterConfig {
public:
	uint32_t bits_per_key = 24;
};

//===----------------------------------------------------------------------===//
// CacheSectorizedBF — RPT's split-sector BF (uint32_t, dual-sector, 7 bits/hash)
//===----------------------------------------------------------------------===//
class CacheSectorizedBF : public RPTFilter {
public:
	CacheSectorizedBF(ClientContext &context_p, const BloomFilterConfig &config, uint32_t est_num_rows);

	ClientContext *context;
	BufferManager *buffer_manager;

public:
	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, SelectionVector &results,
	           size_t &result_count) const override;
	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, Vector &results,
	           size_t &result_count) const override;
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) override;
	size_t Hash() const override;
	string ToString() const override {
		return "CacheSectorizedBF";
	}

	uint32_t num_sectors;
	uint32_t num_sectors_log;
	uint32_t *blocks;

private:
	static constexpr uint32_t MAX_NUM_SECTORS = 1U << 26U;
	static constexpr const int32_t SIMD_BATCH_SIZE = 16;
	static constexpr const uint32_t MIN_NUM_BITS = 512;
	static constexpr const uint32_t LOG_SECTOR_SIZE = 5;

	inline uint32_t GetMask1(uint32_t key_lo) const {
		return (1U << ((key_lo >> 17U) & 31U)) | (1U << ((key_lo >> 22U) & 31U)) | (1U << ((key_lo >> 27U) & 31U));
	}
	inline uint32_t GetMask2(uint32_t key_hi) const {
		return (1U << ((key_hi >> 12U) & 31U)) | (1U << ((key_hi >> 17U) & 31U)) | (1U << ((key_hi >> 22U) & 31U)) |
		       (1U << ((key_hi >> 27U) & 31U));
	}

	inline uint32_t GetSector1(uint32_t key_lo, uint32_t key_hi) const {
		return ((key_lo & ((1U << 17U) - 1U)) + ((key_hi << 14U) & (((1U << 9U) - 1U) << 17U))) & (num_sectors - 1U);
	}
	inline uint32_t GetSector2(uint32_t key_hi, uint32_t block1) const {
		return block1 ^ (8U + (key_hi & 7U));
	}

	inline void InsertOne(uint32_t key_lo, uint32_t key_hi, uint32_t *BF_RESTRICT bf) const {
		uint32_t sector1 = GetSector1(key_lo, key_hi);
		uint32_t mask1 = GetMask1(key_lo);
		uint32_t sector2 = GetSector2(key_hi, sector1);
		uint32_t mask2 = GetMask2(key_hi);

		std::atomic<uint32_t> &atomic_bf1 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[sector1]);
		std::atomic<uint32_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[sector2]);

		atomic_bf1.fetch_or(mask1, std::memory_order_relaxed);
		atomic_bf2.fetch_or(mask2, std::memory_order_relaxed);
	}
	inline bool LookupOne(uint32_t key_lo, uint32_t key_hi, const uint32_t *BF_RESTRICT bf) const {
		uint32_t sector1 = GetSector1(key_lo, key_hi);
		uint32_t mask1 = GetMask1(key_lo);
		uint32_t sector2 = GetSector2(key_hi, sector1);
		uint32_t mask2 = GetMask2(key_hi);
		return ((bf[sector1] & mask1) == mask1) & ((bf[sector2] & mask2) == mask2);
	}

	int BloomFilterLookup(int num, const uint64_t *BF_RESTRICT key64, const uint32_t *BF_RESTRICT bf,
	                      SelectionVector &results, size_t &result_count) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
		result_count = 0;
		for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				bool out = ((bf[block1[j]] & mask1[j]) == mask1[j]) & ((bf[block2[j]] & mask2[j]) == mask2[j]);
				results.set_index(result_count, i + j);
				result_count += out;
			}
		}

		for (int i = num / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE; i < num; i++) {
			bool out = LookupOne(key[i + i], key[i + i + 1], bf);
			results.set_index(result_count, i);
			result_count += out;
		}
		return num;
	}

	int BloomFilterLookup(int num, const uint64_t *BF_RESTRICT key64, const uint32_t *BF_RESTRICT bf, Vector &results,
	                      size_t &result_count) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
		result_count = 0;
		auto result_data = FlatVector::GetDataMutable<bool>(results);
		for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				bool out = ((bf[block1[j]] & mask1[j]) == mask1[j]) & ((bf[block2[j]] & mask2[j]) == mask2[j]);
				result_data[i + j] = out;
				result_count += out;
			}
		}

		for (int i = num / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE; i < num; i++) {
			bool out = LookupOne(key[i + i], key[i + i + 1], bf);
			result_data[i] = out;
			result_count += out;
		}
		return num;
	}

	void BloomFilterInsert(int num, const uint64_t *BF_RESTRICT key64, uint32_t *BF_RESTRICT bf) const {
		const uint32_t *BF_RESTRICT key = reinterpret_cast<const uint32_t * BF_RESTRICT>(key64);
		for (int i = 0; i + SIMD_BATCH_SIZE <= num; i += SIMD_BATCH_SIZE) {
			uint32_t block1[SIMD_BATCH_SIZE], mask1[SIMD_BATCH_SIZE];
			uint32_t block2[SIMD_BATCH_SIZE], mask2[SIMD_BATCH_SIZE];

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				int p = i + j;
				uint32_t key_lo = key[p + p];
				uint32_t key_hi = key[p + p + 1];
				block1[j] = GetSector1(key_lo, key_hi);
				mask1[j] = GetMask1(key_lo);
				block2[j] = GetSector2(key_hi, block1[j]);
				mask2[j] = GetMask2(key_hi);
			}

			for (int j = 0; j < SIMD_BATCH_SIZE; j++) {
				std::atomic<uint32_t> &atomic_bf1 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[block1[j]]);
				std::atomic<uint32_t> &atomic_bf2 = *reinterpret_cast<std::atomic<uint32_t> *>(&bf[block2[j]]);

				atomic_bf1.fetch_or(mask1[j], std::memory_order_relaxed);
				atomic_bf2.fetch_or(mask2[j], std::memory_order_relaxed);
			}
		}

		for (int i = num / SIMD_BATCH_SIZE * SIMD_BATCH_SIZE; i < num; i++) {
			InsertOne(key[i + i], key[i + i + 1], bf);
		}
	}

	AllocatedData buf_;
};

//===----------------------------------------------------------------------===//
// DuckDBBloomFilterAdapter — wraps DuckDB's built-in BloomFilter as RPTFilter
//===----------------------------------------------------------------------===//
class DuckDBBloomFilterAdapter : public RPTFilter {
public:
	DuckDBBloomFilterAdapter(ClientContext &context_p, const BloomFilterConfig &config, uint32_t est_num_rows);

public:
	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, SelectionVector &results,
	           size_t &result_count) const override;
	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, Vector &results,
	           size_t &result_count) const override;
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) override;
	size_t Hash() const override;
	string ToString() const override {
		return "DuckDBBloomFilter";
	}
	BloomFilter &GetBloomFilter() {
		return bf_;
	}

private:
	BloomFilter bf_; // DuckDB's built-in BloomFilter
};

//! Exact integral bitmap implemented through DuckDB's built-in prefix-range
//! filter interface so ExpressionFilter can use its native fast executor.
class DuckDBPrefixRangeFilterAdapter : public RPTFilter {
public:
	DuckDBPrefixRangeFilterAdapter(ClientContext &context, const LogicalType &key_type, int64_t lower_bound,
	                               int64_t upper_bound, idx_t row_count);

	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols, SelectionVector &results,
	           size_t &result_count) const override;
	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols, Vector &results, size_t &result_count) const override;
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols) override;
	unique_ptr<PrefixRangeFilter::BuildState> InitializeLocalBuildState(ClientContext &context) const;
	idx_t GetLocalBuildStateSize() const;
	void InsertLocal(DataChunk &chunk, const vector<idx_t> &bound_cols, PrefixRangeFilter::BuildState &state) const;
	void MergeLocalBuildState(PrefixRangeFilter::BuildState &state);
	void SetValid() override;
	size_t Hash() const override;
	string ToString() const override;

	PrefixRangeFilter &GetPrefixRangeFilter() {
		return *filter_;
	}

private:
	ClientContext &context_;
	int64_t lower_bound_;
	int64_t upper_bound_;
	unique_ptr<PrefixRangeFilter> filter_;
	unique_ptr<PrefixRangeFilter::BuildState> build_state_;
};

//===----------------------------------------------------------------------===//
// Macro switch: select active BF implementation
//===----------------------------------------------------------------------===//
#ifdef RPT_USE_DUCKDB_BF
using ActiveBloomFilter = DuckDBBloomFilterAdapter;
#else
using ActiveBloomFilter = CacheSectorizedBF;
#endif

} // namespace duckdb
