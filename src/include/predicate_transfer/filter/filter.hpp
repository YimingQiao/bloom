//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/filter/filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class RPTFilter {
public:
	virtual ~RPTFilter() = default;

	virtual int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols, SelectionVector &results,
	                   size_t &result_count) const = 0;
	virtual int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols, Vector &results,
	                   size_t &result_count) const = 0;
	virtual void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols) = 0;
	virtual size_t Hash() const = 0;
	//! Bytes retained by the finalized filter itself. Task-local build state is
	//! temporary and is accounted separately by transfer admission.
	virtual idx_t MemoryUsage() const = 0;
	//! Exact number of non-NULL keys represented by this filter when its
	//! physical representation can prove it. Approximate filters return invalid.
	virtual optional_idx ExactDistinctCount() const {
		return optional_idx();
	}

	virtual FilterPropagateResult CheckStatistics(const BaseStatistics &stats) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	virtual string ToString() const {
		return "RPTFilter";
	}

	bool IsValid() const {
		return finalized_;
	}
	virtual void SetValid() {
		finalized_ = true;
	}

protected:
	bool finalized_ = false;
};

class RPTFilterWrapper {
public:
	shared_ptr<RPTFilter> filter;
};

class RPTFilterUsage {
public:
	explicit RPTFilterUsage(shared_ptr<RPTFilterWrapper> filter) : filter(std::move(filter)) {
	}

	bool IsValid() const {
		return filter->filter && filter->filter->IsValid();
	}

	void SetValid() {
		filter->filter->SetValid();
	}

	shared_ptr<RPTFilter> GetFilter() {
		return filter->filter;
	}

public:
	int Lookup(DataChunk &chunk, const vector<idx_t> &bound_cols_applied, SelectionVector &results,
	           size_t &result_count) const {
		return filter->filter->Lookup(chunk, bound_cols_applied, results, result_count);
	}
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_applied) const {
		return filter->filter->Insert(chunk, bound_cols_applied);
	}

private:
	shared_ptr<RPTFilterWrapper> filter;
};

} // namespace duckdb
