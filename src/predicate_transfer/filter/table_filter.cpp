#include "predicate_transfer/filter/table_filter.hpp"
#include "predicate_transfer/filter/bloom_filter.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
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
		auto &adapter = static_cast<DuckDBPrefixRangeFilterAdapter &>(*owner);
		return make_uniq<OwnedPrefixRangeFunctionData>(owner, adapter.GetPrefixRangeFilter(), key_type);
	}

	bool Equals(const FunctionData &other) const override {
		return PrefixRangeFunctionData::Equals(other);
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

static unique_ptr<FunctionData> RPTFilterBind(BindScalarFunctionInput &) {
	throw BinderException("Bloom transfer filters are for internal use only");
}

static void RPTFilterSerialize(Serializer &, const optional_ptr<FunctionData>, const BoundScalarFunction &) {
	// Runtime filter state belongs to this process and is not serialized.
}

static unique_ptr<FunctionData> RPTFilterDeserialize(Deserializer &, BoundScalarFunction &function) {
	// Match DuckDB's runtime table filters: a deserialized plan does not retain
	// process-local filter state. The original joins still enforce membership.
	D_ASSERT(function.GetArguments().size() == 1);
	return make_uniq<RPTFilterFunctionData>(nullptr, function.GetArguments()[0]);
}

static ScalarFunction RPTFilterScalarFunction(const LogicalType &input_type) {
	ScalarFunction function("__internal_bloom_rpt_filter", {input_type}, LogicalType::BOOLEAN, RPTFilterExecute,
	                        RPTFilterBind);
	function.GetSignature().GetParameter(0).SetName("col");
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetFilterPruneCallback(RPTFilterPrune);
	function.SetSerializeCallback(RPTFilterSerialize);
	function.SetDeserializeCallback(RPTFilterDeserialize);
	return function;
}

void RPTTableFilter::RegisterFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(RPTFilterScalarFunction(LogicalType::ANY));
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
		// DuckDB 2.0 retains a shared filter. Alias the adapter's ownership so
		// native FunctionData copies keep its embedded BloomFilter alive too.
		auto bind_data =
		    make_uniq<BloomFilterFunctionData>(shared_ptr<const BloomFilter>(filter_, &duckdb_filter->GetBloomFilter()),
		                                       true, string(), input_type_, 0.0f, idx_t(0));
		unique_ptr<Expression> expression = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(function), std::move(arguments), std::move(bind_data));
		if (selectivity_threshold < 1.0f) {
			expression = CreateSelectivityOptionalFilterExpression(std::move(expression), input_type_,
			                                                       selectivity_threshold, n_vectors_to_check);
		}
		return make_uniq<ExpressionFilter>(std::move(expression));
	}

	auto function = RPTFilterScalarFunction(input_type_);

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
