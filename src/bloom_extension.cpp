#define DUCKDB_EXTENSION_MAIN

#include "bloom_extension.hpp"
#include "predicate_transfer/cardinality_estimation/prepared_sampler.hpp"
#include "predicate_transfer/config.hpp"
#include "predicate_transfer/predicate_transfer_optimizer.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>

namespace duckdb {

static bool EnvFlagDefault(const char *name, bool fallback) {
	const char *value = std::getenv(name);
	if (!value) {
		return fallback;
	}
	if (StringUtil::CIEquals(value, "1") || StringUtil::CIEquals(value, "true")) {
		return true;
	}
	if (StringUtil::CIEquals(value, "0") || StringUtil::CIEquals(value, "false")) {
		return false;
	}
	throw InvalidInputException("Environment variable %s must be 'true', 'false', '1', or '0'", name);
}

static string EnvStringDefault(const char *name, const char *fallback) {
	const char *value = std::getenv(name);
	return value ? string(value) : string(fallback);
}

static idx_t EnvUBigIntDefault(const char *name, idx_t fallback) {
	const char *value = std::getenv(name);
	if (!value || !*value) {
		return fallback;
	}
	if (*value == '-') {
		throw InvalidInputException("Environment variable %s is not a valid unsigned integer", name);
	}
	errno = 0;
	char *end = nullptr;
	auto parsed = std::strtoull(value, &end, 10);
	if (errno == ERANGE || end == value || *end != '\0' || parsed > std::numeric_limits<idx_t>::max()) {
		throw InvalidInputException("Environment variable %s is not a valid unsigned integer", name);
	}
	return static_cast<idx_t>(parsed);
}

static double EnvDoubleDefault(const char *name, double fallback) {
	const char *value = std::getenv(name);
	if (!value || !*value) {
		return fallback;
	}
	errno = 0;
	char *end = nullptr;
	auto parsed = std::strtod(value, &end);
	if (errno == ERANGE || end == value || *end != '\0' || !std::isfinite(parsed)) {
		throw InvalidInputException("Environment variable %s is not a valid finite number", name);
	}
	return parsed;
}

static string ReadChoice(const Value &value, const char *setting, const char *left, const char *right) {
	auto choice = StringUtil::Lower(value.GetValue<string>());
	if (choice != left && choice != right) {
		throw InvalidInputException("%s must be '%s' or '%s'", setting, left, right);
	}
	return choice;
}

static RPTSamplingMode ReadSampleMode(const Value &value) {
	return ReadChoice(value, "rpt_sample_mode", "prepared", "instant") == "prepared" ? RPTSamplingMode::PREPARED
	                                                                                 : RPTSamplingMode::INSTANT;
}

static RPTInstantAccessMode ReadInstantAccess(const Value &value) {
	return ReadChoice(value, "rpt_instant_access", "scattered", "block") == "scattered"
	           ? RPTInstantAccessMode::SCATTERED
	           : RPTInstantAccessMode::BLOCK;
}

static void ValidateSampleMode(ClientContext &, SetScope, Value &value) {
	ReadSampleMode(value);
}

static void ValidateInstantAccess(ClientContext &, SetScope, Value &value) {
	ReadInstantAccess(value);
}

static void ValidateUnsignedRange(Value &value, const char *setting, idx_t maximum) {
	auto number = value.GetValue<idx_t>();
	if (number == 0 || number > maximum) {
		throw InvalidInputException("%s must be between 1 and %llu", setting, maximum);
	}
}

static void ValidateSampleSize(ClientContext &, SetScope, Value &value) {
	ValidateUnsignedRange(value, "rpt_sample_size", SampleOptions::MAX_SAMPLE_ROWS);
}

static void ValidateInstantAccessPoints(ClientContext &, SetScope, Value &value) {
	ValidateUnsignedRange(value, "rpt_instant_access_points", 1000000);
}

static void ValidateInstantRowsPerAccess(ClientContext &, SetScope, Value &value) {
	ValidateUnsignedRange(value, "rpt_instant_rows_per_access", 1000000);
}

static void ValidateInstantBlockWindows(ClientContext &, SetScope, Value &value) {
	ValidateUnsignedRange(value, "rpt_instant_block_windows", 1000000);
}

static void ValidateInstantParquetRowGroups(ClientContext &, SetScope, Value &value) {
	ValidateUnsignedRange(value, "rpt_instant_parquet_row_groups", 1000000);
}

static void ValidateUnitInterval(Value &value, const char *setting) {
	auto number = value.GetValue<double>();
	if (!std::isfinite(number) || number < 0.0 || number > 1.0) {
		throw InvalidInputException("%s must be finite and between 0 and 1", setting);
	}
}

static void ValidateSampleRate(ClientContext &, SetScope, Value &value) {
	ValidateUnitInterval(value, "rpt_sample_rate");
}

static void ValidateExcitationThreshold(ClientContext &, SetScope, Value &value) {
	ValidateUnitInterval(value, "rpt_excitation_threshold");
}

BloomOptimizerExtension::BloomOptimizerExtension() {
	optimize_function = Optimize;
}

struct RPTSamplePreloadGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> RPTSamplePreloadBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("loaded_samples");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> RPTSamplePreloadInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<RPTSamplePreloadGlobalState>();
}

static void RPTSamplePreloadFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (!input.global_state) {
		input.async_result = AsyncResultType::FINISHED;
		return;
	}
	auto &state = input.global_state->Cast<RPTSamplePreloadGlobalState>();
	if (state.finished) {
		input.async_result = AsyncResultType::FINISHED;
		return;
	}
	Value setting;
	RPTSamplingConfig sampling;
	if (context.TryGetCurrentSetting("rpt_sample_size", setting)) {
		sampling.target_rows = setting.GetValue<idx_t>();
	}
	if (context.TryGetCurrentSetting("rpt_sample_cache_dir", setting)) {
		sampling.prepared_cache_dir = setting.ToString();
	}
	if (context.TryGetCurrentSetting("rpt_sample_seed", setting)) {
		sampling.seed = setting.GetValue<idx_t>();
	}
	auto loaded = PreloadPreparedSamples(context, sampling);
	output.data[0].SetValue(0, Value::UBIGINT(loaded));
	output.SetChildCardinality(1);
	state.finished = true;
	input.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
}

void BloomOptimizerExtension::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	Value setting;
	if (input.context.TryGetCurrentSetting("enable_rpt", setting) && !setting.GetValue<bool>()) {
		return;
	}

	RPTOptimizerConfig config;
	if (input.context.TryGetCurrentSetting("rpt_sample_cache_dir", setting)) {
		config.sampling.prepared_cache_dir = setting.ToString();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_size", setting)) {
		ValidateUnsignedRange(setting, "rpt_sample_size", SampleOptions::MAX_SAMPLE_ROWS);
		config.sampling.target_rows = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_rate", setting)) {
		ValidateUnitInterval(setting, "rpt_sample_rate");
		config.sampling.intermediate_rate = setting.GetValue<double>();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_mode", setting)) {
		config.sampling.mode = ReadSampleMode(setting);
	}
	if (input.context.TryGetCurrentSetting("rpt_instant_access", setting)) {
		config.sampling.instant_access = ReadInstantAccess(setting);
	}
	if (input.context.TryGetCurrentSetting("rpt_instant_snapshot", setting)) {
		config.sampling.instant_snapshot = setting.GetValue<bool>();
	}
	if (input.context.TryGetCurrentSetting("rpt_instant_access_points", setting)) {
		ValidateUnsignedRange(setting, "rpt_instant_access_points", 1000000);
		config.sampling.instant_access_points = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_instant_rows_per_access", setting)) {
		ValidateUnsignedRange(setting, "rpt_instant_rows_per_access", 1000000);
		config.sampling.instant_rows_per_access = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_instant_block_windows", setting)) {
		ValidateUnsignedRange(setting, "rpt_instant_block_windows", 1000000);
		config.sampling.instant_block_windows = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_instant_parquet_row_groups", setting)) {
		ValidateUnsignedRange(setting, "rpt_instant_parquet_row_groups", 1000000);
		config.sampling.instant_parquet_row_groups = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_seed", setting)) {
		config.sampling.seed = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_memory_cache", setting)) {
		config.sampling.prepared_memory_cache = setting.GetValue<bool>();
	}
	if (input.context.TryGetCurrentSetting("rpt_log_transfer_steps", setting)) {
		config.log_transfer_steps = setting.GetValue<bool>();
	}
	if (input.context.TryGetCurrentSetting("rpt_excitation_threshold", setting)) {
		ValidateUnitInterval(setting, "rpt_excitation_threshold");
		config.excitation_threshold = setting.GetValue<double>();
	}
	if (input.context.TryGetCurrentSetting("rpt_late_materialize", setting)) {
		config.late_materialize_flag = setting.GetValue<bool>();
	}

	PredicateTransferOptimizer optimizer(input.optimizer, input.context, config);
	plan = optimizer.Optimize(std::move(plan));
	if (input.context.config.enable_profiler || config.log_transfer_steps) {
		auto debug_info = optimizer.GetDebugInfo();
		if (!debug_info.empty()) {
			std::cerr << debug_info << std::endl;
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("enable_rpt", "Enable the Robust Predicate Transfer optimizer", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(EnvFlagDefault("RPT_ENABLE", true)));
	config.AddExtensionOption("rpt_sample_cache_dir",
	                          "Prepared-sample cache directory ('auto' stores it beside the database)",
	                          LogicalType::VARCHAR, Value(EnvStringDefault("RPT_SAMPLE_CACHE_DIR", "auto")));
	config.AddExtensionOption("rpt_sample_size", "Target rows per sampled base table", LogicalType::UBIGINT,
	                          Value::UBIGINT(EnvUBigIntDefault("RPT_SAMPLE_SIZE", 10000)), ValidateSampleSize);
	config.AddExtensionOption("rpt_sample_rate",
	                          "Chunk-sampling fraction for estimating cardinalities of already-materialized "
	                          "intermediate data",
	                          LogicalType::DOUBLE, Value::DOUBLE(EnvDoubleDefault("RPT_SAMPLE_RATE", 0.01)),
	                          ValidateSampleRate);
	config.AddExtensionOption("rpt_sample_mode", "Base-table sampling mode: 'prepared' or 'instant'",
	                          LogicalType::VARCHAR, Value(EnvStringDefault("RPT_SAMPLE_MODE", "prepared")),
	                          ValidateSampleMode);
	config.AddExtensionOption(
	    "rpt_instant_access",
	    "Native-storage instant access policy: 'scattered' (resident data) or 'block' (cold data)",
	    LogicalType::VARCHAR, Value(EnvStringDefault("RPT_INSTANT_ACCESS", "scattered")), ValidateInstantAccess);
	config.AddExtensionOption(
	    "rpt_instant_snapshot",
	    "Use the active transaction snapshot for native instant samples instead of direct base-storage reads",
	    LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_INSTANT_SNAPSHOT", false)));
	config.AddExtensionOption("rpt_instant_access_points",
	                          "Stratified access points used by scattered instant sampling", LogicalType::UBIGINT,
	                          Value::UBIGINT(EnvUBigIntDefault("RPT_INSTANT_ACCESS_POINTS", 256)),
	                          ValidateInstantAccessPoints);
	config.AddExtensionOption(
	    "rpt_instant_rows_per_access", "Contiguous rows decoded at each scattered access point", LogicalType::UBIGINT,
	    Value::UBIGINT(EnvUBigIntDefault("RPT_INSTANT_ROWS_PER_ACCESS", 32)), ValidateInstantRowsPerAccess);
	config.AddExtensionOption("rpt_instant_block_windows", "Block-aligned windows used by cold instant sampling",
	                          LogicalType::UBIGINT, Value::UBIGINT(EnvUBigIntDefault("RPT_INSTANT_BLOCK_WINDOWS", 16)),
	                          ValidateInstantBlockWindows);
	config.AddExtensionOption(
	    "rpt_instant_parquet_row_groups", "Stratified Parquet row groups sampled per table", LogicalType::UBIGINT,
	    Value::UBIGINT(EnvUBigIntDefault("RPT_INSTANT_PARQUET_ROW_GROUPS", 8)), ValidateInstantParquetRowGroups);
	config.AddExtensionOption("rpt_sample_seed", "Reproducible base-table sample seed", LogicalType::UBIGINT,
	                          Value::UBIGINT(EnvUBigIntDefault("RPT_SAMPLE_SEED", 2)));
	config.AddExtensionOption("rpt_sample_memory_cache",
	                          "Cache prepared samples in the process object cache to skip disk reloads across queries "
	                          "(advanced)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_SAMPLE_MEMORY_CACHE", true)));
	config.AddExtensionOption("rpt_log_transfer_steps", "Log the transfer plan to stderr for debugging",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_LOG_TRANSFER_STEPS", false)));
	config.AddExtensionOption("rpt_excitation_threshold", "Re-excite below this fraction of the preceding cardinality",
	                          LogicalType::DOUBLE, Value::DOUBLE(EnvDoubleDefault("RPT_EXCITATION_THRESHOLD", 1.0)),
	                          ValidateExcitationThreshold);
	config.AddExtensionOption("rpt_late_materialize",
	                          "Experimental: rowid-based late materialization (currently a net slowdown; off)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_LATE_MATERIALIZE", false)));

	TableFunction preload_samples("rpt_preload_samples", {}, RPTSamplePreloadFunction, RPTSamplePreloadBind,
	                              RPTSamplePreloadInit);
	loader.RegisterFunction(preload_samples);

	config.GetCallbackManager().Register(BloomOptimizerExtension());
}

void BloomExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

string BloomExtension::Name() {
	return "bloom";
}

string BloomExtension::Version() const {
#ifdef EXT_VERSION_BLOOM
	return EXT_VERSION_BLOOM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(bloom, loader) {
	duckdb::LoadInternal(loader);
}
}
