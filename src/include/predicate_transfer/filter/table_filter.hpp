#pragma once

#include "predicate_transfer/filter/filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

//! Adapts an RPT Bloom/bitmap filter to DuckDB's stock expression table-filter API.
class RPTTableFilter {
public:
	RPTTableFilter(shared_ptr<RPTFilter> filter, LogicalType input_type)
	    : filter_(std::move(filter)), input_type_(std::move(input_type)) {
	}

	unique_ptr<ExpressionFilter> GetFilter(float selectivity_threshold = 1.0f,
	                                      idx_t n_vectors_to_check = 6) const;

	static unique_ptr<TableFilter> MakeOptional(shared_ptr<RPTFilter> filter, LogicalType input_type,
	                                            float selectivity_threshold = 1.0f,
	                                            idx_t n_vectors_to_check = 6) {
		return RPTTableFilter(std::move(filter), std::move(input_type))
		    .GetFilter(selectivity_threshold, n_vectors_to_check);
	}

private:
	shared_ptr<RPTFilter> filter_;
	LogicalType input_type_;
};

} // namespace duckdb
