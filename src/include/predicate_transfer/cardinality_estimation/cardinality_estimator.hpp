#pragma once

#include "predicate_transfer/filter/filter.hpp"
#include "predicate_transfer/table_scanner/table_scanner.hpp"

#include <unordered_set>

namespace duckdb {

struct DirectFilterInfo {
	ColumnBinding binding;
	shared_ptr<RPTFilter> filter;
	unordered_set<idx_t> lineage;
};

class RPTCardinalityEstimator {
public:
	virtual ~RPTCardinalityEstimator() = default;
	virtual idx_t Estimate(const LogicalOperator &op) = 0;
	virtual idx_t Estimate(const LogicalOperator &op, const vector<DirectFilterInfo> &filters) = 0;
	virtual idx_t Estimate(TableScanner &scanner, const vector<DirectFilterInfo> &filters) = 0;
	//! Memory retained by query-local samples and their filtered views.
	virtual idx_t MemoryUsage() const = 0;
};

shared_ptr<ColumnDataCollection> ExecutePlanAndCollect(ClientContext &context, unique_ptr<LogicalOperator> plan);

} // namespace duckdb
