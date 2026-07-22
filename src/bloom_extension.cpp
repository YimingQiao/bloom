#define DUCKDB_EXTENSION_MAIN

#include "bloom_extension.hpp"
#include "predicate_transfer/config.hpp"
#include "predicate_transfer/operator/logical_create_bf.hpp"
#include "predicate_transfer/operator/logical_use_bf.hpp"
#include "predicate_transfer/predicate_transfer_optimizer.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

#include <cstdlib>
#include <iostream>

namespace duckdb {

static bool EnvFlagDefault(const char *name, bool fallback) {
	const char *value = std::getenv(name);
	if (!value) {
		return fallback;
	}
	return !(StringUtil::CIEquals(value, "0") || StringUtil::CIEquals(value, "false"));
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
	char *end = nullptr;
	auto parsed = std::strtoull(value, &end, 10);
	return end == value ? fallback : static_cast<idx_t>(parsed);
}

static double EnvDoubleDefault(const char *name, double fallback) {
	const char *value = std::getenv(name);
	if (!value || !*value) {
		return fallback;
	}
	char *end = nullptr;
	auto parsed = std::strtod(value, &end);
	return end == value ? fallback : parsed;
}

BloomOptimizerExtension::BloomOptimizerExtension() {
	optimize_function = Optimize;
}

void BloomOptimizerExtension::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	Value setting;
	if (input.context.TryGetCurrentSetting("enable_rpt", setting) && !setting.GetValue<bool>()) {
		return;
	}

	RPTOptimizerConfig config;
	if (input.context.TryGetCurrentSetting("rpt_sample_cache_dir", setting)) {
		config.sample_cache_dir = setting.ToString();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_size", setting)) {
		config.sample_materialization_size = setting.GetValue<idx_t>();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_rate", setting)) {
		config.sample_rate = setting.GetValue<double>();
	}
	if (input.context.TryGetCurrentSetting("rpt_sample_memory_cache", setting)) {
		config.sample_memory_cache = setting.GetValue<bool>();
	}
	if (input.context.TryGetCurrentSetting("rpt_log_transfer_steps", setting)) {
		config.log_transfer_steps = setting.GetValue<bool>();
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
	                          "RPT sampling cache directory ('auto' stores it beside the database)",
	                          LogicalType::VARCHAR, Value(EnvStringDefault("RPT_SAMPLE_CACHE_DIR", "auto")));
	config.AddExtensionOption("rpt_sample_size", "RPT per-table sample target row count", LogicalType::UBIGINT,
	                          Value::UBIGINT(EnvUBigIntDefault("RPT_SAMPLE_SIZE", 10000)));
	config.AddExtensionOption("rpt_sample_rate", "RPT sample rate for in-memory materialized data", LogicalType::DOUBLE,
	                          Value::DOUBLE(EnvDoubleDefault("RPT_SAMPLE_RATE", 0.01)));
	config.AddExtensionOption("rpt_sample_memory_cache", "Keep RPT samples in the process object cache",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_SAMPLE_MEMORY_CACHE", true)));
	config.AddExtensionOption("rpt_log_transfer_steps", "Log RPT transfer-plan generation to stderr",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_LOG_TRANSFER_STEPS", false)));
	config.AddExtensionOption("rpt_late_materialize", "Use rowid-based late materialization for RPT",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(EnvFlagDefault("RPT_LATE_MATERIALIZE", false)));

	config.GetCallbackManager().Register(BloomOptimizerExtension());
	config.GetCallbackManager().Register(make_uniq<LogicalCreateBF::OperatorExtension>());
	config.GetCallbackManager().Register(make_uniq<LogicalUseBF::OperatorExtension>());
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
