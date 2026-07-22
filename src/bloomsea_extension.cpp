#define DUCKDB_EXTENSION_MAIN

#include "bloomsea_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

// Placeholder smoke-test function; the real BloomSea registers an optimizer
// extension for predicate transfer (to be ported from the research prototype).
inline void BloomseaScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "🌊🌸 BloomSea: " + name.GetString());
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	auto bloomsea_scalar_function =
	    ScalarFunction("bloomsea", {LogicalType::VARCHAR}, LogicalType::VARCHAR, BloomseaScalarFun);
	loader.RegisterFunction(bloomsea_scalar_function);
}

void BloomseaExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string BloomseaExtension::Name() {
	return "bloomsea";
}

std::string BloomseaExtension::Version() const {
#ifdef EXT_VERSION_BLOOMSEA
	return EXT_VERSION_BLOOMSEA;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(bloomsea, loader) {
	duckdb::LoadInternal(loader);
}
}
