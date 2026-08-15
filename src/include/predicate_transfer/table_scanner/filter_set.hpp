#pragma once

#include "predicate_transfer/filter/filter.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalOperator;

//! Owns every transfer filter attached to a scanner. It is responsible for
//! filter identity, logical-GET pushdown, output-binding resolution, and
//! in-memory application.
class ScannerFilterSet {
public:
	struct Entry {
		vector<ColumnBinding> bindings;
		vector<idx_t> chunk_cols;
		shared_ptr<RPTFilter> filter;
		size_t identity_hash = 0;
	};

	void Add(ColumnBinding binding, shared_ptr<RPTFilter> filter, size_t identity_hash);
	void Add(const vector<ColumnBinding> &bindings, shared_ptr<RPTFilter> filter, size_t identity_hash);

	size_t Fingerprint() const;
	void Pushdown(LogicalOperator &plan);
	void Resolve(const vector<ColumnBinding> &output_bindings);
	void Apply(DataChunk &chunk) const;

	bool Empty() const {
		return entries_.empty();
	}
	void Clear() {
		entries_.clear();
	}
	const vector<Entry> &Entries() const {
		return entries_;
	}

private:
	vector<Entry> entries_;
};

} // namespace duckdb
