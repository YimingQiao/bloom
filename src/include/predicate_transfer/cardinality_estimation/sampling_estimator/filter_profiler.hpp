#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "predicate_transfer/table_scanner/filter_set.hpp"

namespace duckdb {

class ClientContext;
class Expression;

//! Per-sample filter observations. Every bit addresses the same unfiltered row.
//! Recreate this object when the sample or its column layout changes.
class SampleFilterProfiler {
public:
	struct Observation {
		string description;
		unique_ptr<Expression> expression;
		vector<uint64_t> passes;
		idx_t passed = 0;
		double evaluation_ns = 0;
		//! Weak identity: do not retain obsolete transfer filters just for profiling.
		weak_ptr<RPTFilter> runtime_filter;
		vector<idx_t> columns;
	};

	SampleFilterProfiler(ClientContext &context, const ColumnDataCollection &sample, vector<column_t> columns);
	//! Split AND into independently evaluated predicates; retain OR as one filter.
	void AddLocal(const Expression &expression);
	//! Change the active local predicate set without discarding cached results.
	void ClearLocal() {
		local_filters_.clear();
	}
	//! Finalized transfer filters are immutable. Reuse an observation only when
	//! both its live filter object and bound columns match.
	idx_t AddRuntime(const ScannerFilterSet::Entry &filter);
	idx_t Count(const vector<idx_t> &filters) const;
	double EstimateLocalEvaluationNS(const vector<idx_t> &filters) const;
	//! Estimate pending filter evaluation from cached timings and pass bitmaps.
	double EstimateEvaluationNS(const vector<idx_t> &filters, vector<idx_t> *order = nullptr) const;
	void Log(const string &table, const vector<idx_t> &filters) const;
	idx_t MemoryUsage() const;
	const vector<idx_t> &LocalFilters() const {
		return local_filters_;
	}
	const vector<Observation> &Observations() const {
		return observations_;
	}
	idx_t SampleSize() const {
		return sample_.Count();
	}

private:
	idx_t Evaluate(const Expression *expression, const ScannerFilterSet::Entry *filter);
	ClientContext &context_;
	const ColumnDataCollection &sample_;
	vector<column_t> columns_;
	vector<idx_t> local_filters_;
	vector<Observation> observations_;
};

} // namespace duckdb
