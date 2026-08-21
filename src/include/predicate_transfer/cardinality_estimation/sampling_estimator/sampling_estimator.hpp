#pragma once

#include "predicate_transfer/cardinality_estimation/cardinality_estimator.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator/table_sample_manager.hpp"
#include "predicate_transfer/config.hpp"

namespace duckdb {

class SamplingCardinalityEstimator : public RPTCardinalityEstimator {
public:
	explicit SamplingCardinalityEstimator(ClientContext &context, RPTSamplingConfig config);

	idx_t Estimate(const LogicalOperator &op) override;
	idx_t Estimate(const LogicalOperator &op, const vector<DirectFilterInfo> &filters) override;
	idx_t Estimate(TableScanner &scanner, const vector<DirectFilterInfo> &filters) override;
	idx_t MemoryUsage() const override {
		return samples_.MemoryUsage();
	}

private:
	bool LogEnabled() const;
	//! Apply BF filters (if any) on top of the local-filtered sample, count
	//! survivors, extrapolate to the full table.
	idx_t EstimateOnSample(TableSampleManager::Entry &sample, const LogicalOperator &op,
	                       const vector<DirectFilterInfo> &filters);
	idx_t SampleCDC(const LogicalOperator &op);

	ClientContext &context_;
	RPTSamplingConfig config_;
	TableSampleManager samples_;
};

} // namespace duckdb
