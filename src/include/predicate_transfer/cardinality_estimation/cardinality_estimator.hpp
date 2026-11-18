#pragma once

#include "predicate_transfer/filter/filter.hpp"
#include "predicate_transfer/table_scanner/table_scanner.hpp"

namespace duckdb {

struct DirectFilterInfo {
	ColumnBinding binding;
	shared_ptr<RPTFilter> filter;
};

class RPTCardinalityEstimator {
public:
	virtual ~RPTCardinalityEstimator() = default;
	virtual idx_t Estimate(const LogicalOperator &op) = 0;
	virtual idx_t Estimate(const LogicalOperator &op, const vector<DirectFilterInfo> &filters) = 0;
	virtual idx_t Estimate(TableScanner &scanner, const vector<DirectFilterInfo> &filters) = 0;
	//! Filter time extrapolated by the most recent Estimate call, in milliseconds.
	virtual double FilterTimeMS() const {
		return 0;
	}
	//! Discard base-snapshot observations after actual materialization.
	virtual void Forget(const LogicalOperator &op) {
	}
	//! Memory retained by query-local samples and their filtered views.
	virtual idx_t MemoryUsage() const = 0;
};

shared_ptr<ColumnDataCollection> ExecutePlanAndCollect(ClientContext &context, unique_ptr<LogicalOperator> plan);

} // namespace duckdb
