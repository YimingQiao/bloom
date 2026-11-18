#pragma once

#include "predicate_transfer/table_scanner/filter_set.hpp"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb {

//! Collect and sort ART hits, then reuse native Fetch/local-storage scanning.
//! The caller must evaluate all predicates above this scan, including key membership.
unique_ptr<GlobalTableFunctionState> InitializeRPTIndexScan(ClientContext &context, TableFunctionInitInput &input,
                                                            const Identifier &index_name, const vector<Value> &keys);

//! Install an exact-key ART access path for a small estimated materialization.
bool TryInstallRPTIndexScan(ClientContext &context, LogicalOperator &plan,
                            const vector<ScannerFilterSet::Entry> &filters, optional_idx estimated_rows);

} // namespace duckdb
