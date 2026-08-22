#include "predicate_transfer/cardinality_estimation/prepared_sampler.hpp"
#include "predicate_transfer/cardinality_estimation/cardinality_estimator.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <chrono>
#include <iostream>

namespace duckdb {

namespace {

class RPTSampleObjectCacheEntry : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rpt_sample";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		// The collection itself is already hard-accounted by BufferAllocator.
		// Keep the full ObjectCache estimate as a deliberately conservative
		// eviction reservation: DuckDB's current ObjectCache API cannot mark a
		// payload as externally accounted, and understating this value can make
		// pressure-driven eviction ineffective. Users with a tight limit can
		// disable the optional process cache and retain only the disk cache.
		return cdc ? optional_idx(cdc->AllocationSize()) : optional_idx(0);
	}

	explicit RPTSampleObjectCacheEntry(shared_ptr<ColumnDataCollection> cdc_p) : cdc(std::move(cdc_p)) {
	}

	shared_ptr<ColumnDataCollection> cdc;
};

static bool SamplingLogEnabled(ClientContext &context) {
	Value setting;
	return context.TryGetCurrentSetting("bloom_log_transfer_steps", setting) && setting.GetValue<bool>();
}

static string SampleCacheKey(TableCatalogEntry &table, const RPTSamplingConfig &config) {
	// Prepared samples contain every physical column in storage order, so the
	// identity is independent of any one query's projected column set.
	auto &catalog = table.ParentCatalog();
	string key = "bloom_sample_v4|" + catalog.GetName();
	key += "|path=" + catalog.GetDBPath();
	key += "|" + table.schema.name + "|" + table.name;
	key += "|rows=" + std::to_string(table.GetStorage().GetTotalRows());
	key += "|types=";
	auto types = table.GetTypes();
	for (idx_t i = 0; i < types.size(); i++) {
		if (i > 0) {
			key += ',';
		}
		key += types[i].ToString();
	}
	key += "|n=" + std::to_string(config.target_rows);
	key += "|seed=" + std::to_string(config.seed);
	return key;
}

static string SampleCachePath(ClientContext &context, TableCatalogEntry &table, const RPTSamplingConfig &config,
                              const string &key) {
	if (config.prepared_cache_dir.empty() || key.empty()) {
		return "";
	}
	string cache_dir = config.prepared_cache_dir;
	if (StringUtil::CIEquals(cache_dir, "auto")) {
		auto database_path = table.ParentCatalog().GetDBPath();
		if (database_path.empty() || database_path == ":memory:") {
			return "";
		}
		cache_dir = database_path + ".bloom_samples";
	}
	auto hash = Hash(key.c_str(), key.size());
	auto &fs = FileSystem::GetFileSystem(context);
	return fs.JoinPath(cache_dir, StringUtil::Format("%016llx.sample", static_cast<uint64_t>(hash)));
}

static shared_ptr<ColumnDataCollection> GetFromMemoryCache(ClientContext &context, const RPTSamplingConfig &config,
                                                           const string &key) {
	if (!config.prepared_memory_cache || key.empty()) {
		return nullptr;
	}
	auto cached = ObjectCache::GetObjectCache(context).Get<RPTSampleObjectCacheEntry>(key);
	return cached ? cached->cdc : nullptr;
}

static void PublishToMemoryCache(ClientContext &context, const string &key,
                                 const shared_ptr<ColumnDataCollection> &sample) {
	if (key.empty() || !sample) {
		return;
	}
	ObjectCache::GetObjectCache(context).Put(key, make_shared_ptr<RPTSampleObjectCacheEntry>(sample));
}

// Disk format: identity key, logical types, then DuckDB's flat DataChunk stream.
// ColumnDataCollection::Serialize boxes every cell as a Value and is much slower
// for these small, frequently loaded optimizer samples.
static shared_ptr<ColumnDataCollection> LoadSampleFromDisk(ClientContext &context, const string &key,
                                                           const string &path) {
	if (path.empty()) {
		return nullptr;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.FileExists(path)) {
		return nullptr;
	}
	BufferedFileReader reader(fs, path.c_str());
	if (reader.Finished()) {
		throw SerializationException("Prepared sample cache is empty: %s", path);
	}
	BinaryDeserializer deserializer(reader);
	deserializer.Begin();
	auto disk_key = deserializer.ReadProperty<string>(100, "key");
	if (disk_key != key) {
		throw SerializationException("Prepared sample cache key mismatch: %s", path);
	}
	auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
	auto chunk_count = deserializer.ReadProperty<idx_t>(102, "chunk_count");
	auto sample = make_shared_ptr<ColumnDataCollection>(BufferAllocator::Get(context), types);
	deserializer.ReadList(103, "chunks", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &object) {
			DataChunk chunk;
			chunk.Deserialize(object);
			sample->Append(chunk);
		});
	});
	deserializer.End();
	if (sample->ChunkCount() != chunk_count) {
		throw SerializationException("Prepared sample cache chunk count mismatch: %s", path);
	}
	return sample;
}

static void SaveSampleToDisk(ClientContext &context, const string &key, const string &path,
                             const ColumnDataCollection &sample) {
	if (path.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto cache_directory = path.substr(0, path.size() - fs.ExtractName(path).size());
	auto separator = fs.PathSeparator(cache_directory);
	if (StringUtil::EndsWith(cache_directory, separator)) {
		cache_directory.resize(cache_directory.size() - separator.size());
	}
	if (!cache_directory.empty() && !fs.DirectoryExists(cache_directory)) {
		fs.CreateDirectory(cache_directory);
	}

	// Publish through a per-instance temporary file so readers never see a
	// partially serialized sample.
	string temporary_path = path + "." + StringUtil::GenerateRandomName(16) + ".tmp";
	{
		BufferedFileWriter writer(fs, temporary_path);
		BinarySerializer serializer(writer);
		serializer.Begin();
		serializer.WriteProperty(100, "key", key);
		serializer.WriteProperty(101, "types", sample.Types());
		serializer.WriteProperty(102, "chunk_count", sample.ChunkCount());
		DataChunk scratch;
		sample.InitializeScanChunk(scratch);
		serializer.WriteList(103, "chunks", sample.ChunkCount(), [&](Serializer::List &list, idx_t i) {
			scratch.Reset();
			sample.FetchChunk(i, scratch);
			list.WriteObject([&](Serializer &object) { scratch.Serialize(object, false); });
		});
		serializer.End();
		writer.Flush();
	}
	if (fs.FileExists(path)) {
		// A concurrent writer won. Validate its publication before discarding ours.
		LoadSampleFromDisk(context, key, path);
		fs.RemoveFile(temporary_path);
	} else {
		fs.MoveFile(temporary_path, path);
	}
}

static void LogPreparedSample(ClientContext &context, const RPTSamplingConfig &config, const string &table_name,
                              const char *source, idx_t sample_rows, idx_t total_rows, uint64_t table_seed,
                              double elapsed_ms) {
	if (!SamplingLogEnabled(context)) {
		return;
	}
	D_ASSERT(STANDARD_VECTOR_SIZE > 0);
	auto chunks = sample_rows / STANDARD_VECTOR_SIZE + (sample_rows % STANDARD_VECTOR_SIZE != 0);
	std::cerr << "  [Bloom-SampleBuild] table=" << table_name << " mode=prepared source=" << source
	          << " rows=" << sample_rows << "/" << total_rows << " chunks=" << chunks << " seed=" << config.seed
	          << " effective_seed=" << table_seed << " elapsed=" << elapsed_ms << "ms\n";
}

} // namespace

shared_ptr<ColumnDataCollection> GetOrCreatePreparedSample(ClientContext &context, const RPTSamplingConfig &config,
                                                           unique_ptr<LogicalOperator> scan, idx_t total_rows,
                                                           uint64_t table_seed) {
	D_ASSERT(scan && scan->type == LogicalOperatorType::LOGICAL_GET);
	D_ASSERT(config.target_rows > 0);
	if (!scan || scan->type != LogicalOperatorType::LOGICAL_GET) {
		throw InternalException("Prepared sampler requires a LogicalGet");
	}

	auto &get = scan->Cast<LogicalGet>();
	get.table_filters.ClearFilters();
	get.projection_ids.clear();
	auto &column_ids = get.GetMutableColumnIds();
	column_ids.clear();
	for (idx_t column = 0; column < get.returned_types.size(); column++) {
		column_ids.emplace_back(column);
	}

	auto table = get.GetTable();
	auto table_name = table ? table->name.GetIdentifierName() : get.GetName();
	auto key = table ? SampleCacheKey(*table, config) : string();
	auto path = table ? SampleCachePath(context, *table, config, key) : string();

	auto cache_started = std::chrono::steady_clock::now();
	if (auto cached = GetFromMemoryCache(context, config, key)) {
		auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - cache_started);
		LogPreparedSample(context, config, table_name, "prepared_memory_cache", cached->Count(), total_rows, table_seed,
		                  elapsed.count());
		return cached;
	}
	if (auto cached = LoadSampleFromDisk(context, key, path)) {
		if (config.prepared_memory_cache) {
			PublishToMemoryCache(context, key, cached);
		}
		auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - cache_started);
		LogPreparedSample(context, config, table_name, "prepared_disk_cache", cached->Count(), total_rows, table_seed,
		                  elapsed.count());
		return cached;
	}

	unique_ptr<LogicalOperator> sample_plan;
	const char *source;
	if (total_rows == 0 || total_rows <= config.target_rows) {
		sample_plan = std::move(scan);
		source = "prepared_full_table";
	} else {
		auto options = make_uniq<SampleOptions>();
		options->sample_size = Value::BIGINT(static_cast<int64_t>(config.target_rows));
		options->is_percentage = false;
		options->method = SampleMethod::RESERVOIR_SAMPLE;
		options->SetSeed(static_cast<idx_t>(table_seed));
		sample_plan = make_uniq<LogicalSample>(std::move(options), std::move(scan));
		source = "prepared_reservoir_build";
	}

	sample_plan->ResolveOperatorTypes();
	auto started = std::chrono::steady_clock::now();
	auto sample = ExecutePlanAndCollect(context, std::move(sample_plan));
	auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started);
	auto sample_rows = sample ? sample->Count() : 0;
	LogPreparedSample(context, config, table_name, source, sample_rows, total_rows, table_seed, elapsed.count());

	if (sample && sample_rows > 0) {
		if (config.prepared_memory_cache) {
			PublishToMemoryCache(context, key, sample);
		}
		auto persist_started = std::chrono::steady_clock::now();
		SaveSampleToDisk(context, key, path, *sample);
		if (SamplingLogEnabled(context)) {
			auto persist_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - persist_started);
			std::cerr << "  [Bloom-SamplePersist] table=" << table_name << " elapsed=" << persist_ms.count() << "ms\n";
		}
	}
	return sample;
}

idx_t PreloadPreparedSamples(ClientContext &context, const RPTSamplingConfig &config) {
	auto preload_config = config;
	preload_config.prepared_memory_cache = true;
	idx_t loaded = 0;
	auto entries = Catalog::GetAllEntries(context, CatalogType::TABLE_ENTRY);
	for (auto &entry : entries) {
		auto &catalog_entry = entry.get();
		if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
			continue;
		}
		auto &table = catalog_entry.Cast<TableCatalogEntry>();
		if (!table.IsDuckTable()) {
			continue;
		}
		auto database_path = table.ParentCatalog().GetDBPath();
		if (database_path.empty() || database_path == ":memory:") {
			continue;
		}
		auto key = SampleCacheKey(table, preload_config);
		if (GetFromMemoryCache(context, preload_config, key)) {
			loaded++;
			continue;
		}
		auto path = SampleCachePath(context, table, preload_config, key);
		auto sample = LoadSampleFromDisk(context, key, path);
		if (!sample) {
			continue;
		}
		PublishToMemoryCache(context, key, sample);
		loaded++;
	}
	return loaded;
}

} // namespace duckdb
