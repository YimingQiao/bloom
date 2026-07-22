#include "predicate_transfer/filter/table_filter.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

struct RPTFilterFunctionData final : FunctionData {
	RPTFilterFunctionData(shared_ptr<RPTFilter> filter_p, LogicalType input_type_p)
	    : filter(std::move(filter_p)), input_type(std::move(input_type_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RPTFilterFunctionData>(filter, input_type);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RPTFilterFunctionData>();
		return filter.get() == other.filter.get() && input_type == other.input_type;
	}

	shared_ptr<RPTFilter> filter;
	LogicalType input_type;
};

struct OwnedPrefixRangeFunctionData final : PrefixRangeFunctionData {
	OwnedPrefixRangeFunctionData(shared_ptr<RPTFilter> owner_p, PrefixRangeFilter &filter_p,
	                             const LogicalType &input_type_p)
	    : PrefixRangeFunctionData(&filter_p, true, string(), input_type_p, 0.0f, idx_t(0)), owner(std::move(owner_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<OwnedPrefixRangeFunctionData>(owner, const_cast<PrefixRangeFilter &>(*filter), key_type);
	}

	bool Equals(const FunctionData &other) const override {
		return PrefixRangeFunctionData::Equals(other);
	}

	shared_ptr<RPTFilter> owner;
};

struct OwnedBloomFilterFunctionData final : BloomFilterFunctionData {
	OwnedBloomFilterFunctionData(shared_ptr<RPTFilter> owner_p, BloomFilter &filter_p, const LogicalType &input_type_p)
	    : BloomFilterFunctionData(&filter_p, true, string(), input_type_p, 0.0f, idx_t(0)), owner(std::move(owner_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<OwnedBloomFilterFunctionData>(owner, const_cast<BloomFilter &>(*filter), key_type);
	}

	bool Equals(const FunctionData &other) const override {
		return BloomFilterFunctionData::Equals(other);
	}

	shared_ptr<RPTFilter> owner;
};

static void RPTFilterExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &expression = state.expr.Cast<BoundFunctionExpression>();
	auto &data = expression.BindInfo()->Cast<RPTFilterFunctionData>();
	if (!data.filter || !data.filter->IsValid()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<bool>(result)[0] = true;
		ConstantVector::SetNull(result, false);
		return;
	}
	size_t result_count = 0;
	data.filter->Lookup(args, {0}, result, result_count);
}

static FilterPropagateResult RPTFilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<RPTFilterFunctionData>();
	if (!data.filter || !data.filter->IsValid()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto stats = input.ChildStats(0);
	return stats ? data.filter->CheckStatistics(*stats) : FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

unique_ptr<ExpressionFilter> RPTTableFilter::GetFilter(float selectivity_threshold, idx_t n_vectors_to_check) const {
	if (auto prefix_filter = dynamic_cast<DuckDBPrefixRangeFilterAdapter *>(filter_.get())) {
		auto function = PrefixRangeScalarFun::GetFunction(input_type_);
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundReferenceExpression>(input_type_, storage_t(0)));
		auto bind_data =
		    make_uniq<OwnedPrefixRangeFunctionData>(filter_, prefix_filter->GetPrefixRangeFilter(), input_type_);
		unique_ptr<Expression> expression = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(function), std::move(arguments), std::move(bind_data));
		if (selectivity_threshold < 1.0f) {
			expression = CreateSelectivityOptionalFilterExpression(std::move(expression), input_type_,
			                                                       selectivity_threshold, n_vectors_to_check);
		}
		return make_uniq<ExpressionFilter>(std::move(expression));
	}
	if (auto duckdb_filter = dynamic_cast<DuckDBBloomFilterAdapter *>(filter_.get())) {
		auto function = BloomFilterScalarFun::GetFunction(input_type_);
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(make_uniq<BoundReferenceExpression>(input_type_, storage_t(0)));
		auto bind_data = make_uniq<OwnedBloomFilterFunctionData>(filter_, duckdb_filter->GetBloomFilter(), input_type_);
		unique_ptr<Expression> expression = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(function), std::move(arguments), std::move(bind_data));
		if (selectivity_threshold < 1.0f) {
			expression = CreateSelectivityOptionalFilterExpression(std::move(expression), input_type_,
			                                                       selectivity_threshold, n_vectors_to_check);
		}
		return make_uniq<ExpressionFilter>(std::move(expression));
	}

	ScalarFunction function("__internal_bloom_rpt_filter", {input_type_}, LogicalType::BOOLEAN, RPTFilterExecute);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetFilterPruneCallback(RPTFilterPrune);

	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(input_type_, storage_t(0)));
	auto bind_data = make_uniq<RPTFilterFunctionData>(filter_, input_type_);
	unique_ptr<Expression> expression =
	    make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments), std::move(bind_data));

	if (selectivity_threshold < 1.0f) {
		expression = CreateSelectivityOptionalFilterExpression(std::move(expression), input_type_,
		                                                       selectivity_threshold, n_vectors_to_check);
	}
	return make_uniq<ExpressionFilter>(std::move(expression));
}

} // namespace duckdb
