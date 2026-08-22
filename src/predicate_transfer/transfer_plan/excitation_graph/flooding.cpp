#include "predicate_transfer/transfer_plan/excitation_graph/excitation_graph_manager.hpp"
#include "predicate_transfer/cardinality_estimation/sampling_estimator/sampling_estimator.hpp"
#include "predicate_transfer/bloom_log.hpp"
#include "predicate_transfer/table_operator_manager.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <numeric>
#include <sstream>

namespace duckdb {

idx_t ExcitationGraphManager::SaturatingAdd(idx_t left, idx_t right) {
	return right > std::numeric_limits<idx_t>::max() - left ? std::numeric_limits<idx_t>::max() : left + right;
}

idx_t ExcitationGraphManager::SaturatingMultiply(idx_t left, idx_t right) {
	return left != 0 && right > std::numeric_limits<idx_t>::max() / left ? std::numeric_limits<idx_t>::max()
	                                                                     : left * right;
}

template <class T>
static shared_ptr<T> HoldMemoryState(shared_ptr<T> value, const shared_ptr<TemporaryMemoryState> &memory_state) {
	if (!value || !memory_state) {
		return value;
	}
	auto *raw = value.get();
	auto retained_state = memory_state;
	// The deleter releases the payload first, so BufferAllocator returns its
	// bytes before the last execution object unregisters the reservation.
	return shared_ptr<T>(raw, [value = std::move(value), retained_state = std::move(retained_state)](T *) mutable {
		value.reset();
		retained_state.reset();
	});
}

void ExcitationGraphManager::InitializeMemoryBudget() {
	if (memory_state_) {
		memory_state_->SetZero();
		memory_state_.reset();
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto limit = buffer_manager.GetOperatorMemoryLimit();
	auto used = buffer_manager.GetUsedMemory();
	auto available = used < limit ? limit - used : 0;
	memory_budget_ =
	    config.memory_limit.IsValid() ? MinValue(config.memory_limit.GetIndex(), available) : available / 4;
	if (memory_budget_ == 0) {
		return;
	}
	auto state = TemporaryMemoryManager::Get(context).Register(context);
	memory_state_ = shared_ptr<TemporaryMemoryState>(std::move(state));
	// Bloom cannot make progress on a partial reservation because it deliberately
	// has no spill path. Do not claim DuckDB's per-operator minimum as usable;
	// every phase below must obtain its complete projected peak.
	memory_state_->SetMinimumReservation(0);
	ResizeMemoryReservation(memory_budget_);
}

idx_t ExcitationGraphManager::EstimateCollectionMemory(idx_t rows, const vector<LogicalType> &types) const {
	idx_t row_width = 0;
	for (auto &type : types) {
		idx_t width;
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR:
			// string_t plus a conservative allowance for non-inlined payload.
			width = 64;
			break;
		case PhysicalType::LIST:
		case PhysicalType::STRUCT:
		case PhysicalType::ARRAY:
			width = 128;
			break;
		default:
			width = MaxValue<idx_t>(GetTypeIdSize(type.InternalType()), 8);
			break;
		}
		// Include validity and per-vector bookkeeping conservatively per row.
		row_width = SaturatingAdd(row_width, SaturatingAdd(width, 1));
	}
	return SaturatingAdd(SaturatingMultiply(rows, row_width), SaturatingMultiply(types.size(), 4096));
}

idx_t ExcitationGraphManager::EstimateInitialSampleMemory() const {
	idx_t total = 0;
	unordered_set<const TableCatalogEntry *> seen_tables;
	for (auto &entry : table_operator_manager.GetAllTableOperators()) {
		auto &op = entry.second.get();
		const LogicalOperator *leaf = &op;
		while (leaf->type != LogicalOperatorType::LOGICAL_GET && leaf->type != LogicalOperatorType::LOGICAL_CHUNK_GET &&
		       !leaf->children.empty()) {
			leaf = leaf->children[0].get();
		}
		if (leaf->type != LogicalOperatorType::LOGICAL_GET) {
			continue;
		}
		auto &get = leaf->Cast<LogicalGet>();
		auto table = get.GetTable();
		auto input_rows = MaxValue<idx_t>(ComputeBaseTableRows(op), op.estimated_cardinality);
		auto target_rows = config.sampling.target_rows;
		if (config.sampling.mode == RPTSamplingMode::INSTANT &&
		    config.sampling.instant_access == RPTInstantAccessMode::SCATTERED) {
			target_rows =
			    SaturatingMultiply(config.sampling.instant_access_points, config.sampling.instant_rows_per_access);
		}
		auto rows = MinValue<idx_t>(input_rows, target_rows);
		// Use the full storage schema for admission. Instant sampling normally
		// narrows this set, but op.types can omit filter-only columns and would
		// underestimate the allocation in exactly the cases where safety matters.
		auto collection_memory = EstimateCollectionMemory(rows, get.returned_types);
		// Only memory-cached prepared samples are shared across self-join
		// references. Instant samples and non-memory-cached prepared samples are
		// acquired independently. Every logical reference can additionally own a
		// different local-predicate view.
		auto shares_raw_sample =
		    table && config.sampling.mode == RPTSamplingMode::PREPARED && config.sampling.prepared_memory_cache;
		if (!shares_raw_sample || seen_tables.insert(table.get()).second) {
			total = SaturatingAdd(total, collection_memory);
		}
		total = SaturatingAdd(total, collection_memory);
	}
	return total;
}

void ExcitationGraphManager::EstimateFilterMemory(idx_t rows, const vector<shared_ptr<GraphEdge>> &edges,
                                                  idx_t &persistent, idx_t &temporary) const {
	persistent = 0;
	temporary = 0;
	vector<vector<ColumnBinding>> spec_bindings;
	vector<vector<LogicalType>> spec_types;
	vector<bool> spec_tracks_exact_domain;
	for (auto &edge : edges) {
		vector<idx_t> order(edge->source_columns.size());
		std::iota(order.begin(), order.end(), idx_t {0});
		std::sort(order.begin(), order.end(), [&](idx_t left_idx, idx_t right_idx) {
			const auto &left = edge->source_columns[left_idx];
			const auto &right = edge->source_columns[right_idx];
			return left.table_index.index == right.table_index.index ? left.column_index < right.column_index
			                                                         : left.table_index.index < right.table_index.index;
		});
		vector<ColumnBinding> sorted_bindings;
		vector<LogicalType> sorted_types;
		for (auto index : order) {
			sorted_bindings.push_back(edge->source_columns[index]);
			sorted_types.push_back(edge->return_types[index]);
		}
		if (config.enable_filter_cache) {
			auto source_lineage_id = state_.Lineage().LineageOf(edge->source);
			auto snapshot = state_.Lineage().MakeSnapshot(edge->source, sorted_bindings);
			if (filter_cache_.Lookup(source_lineage_id, snapshot)) {
				continue;
			}
		}
		bool track_exact_domain = config.excitation_mode == RPTExcitationMode::JOIN_KEY_NDV &&
		                          sorted_bindings.size() == 1 && edge->dest_columns.size() == 1 &&
		                          equality_domains_.Tracks(sorted_bindings.front(), edge->dest_columns.front());
		idx_t existing = DConstants::INVALID_INDEX;
		for (idx_t spec_idx = 0; spec_idx < spec_bindings.size(); spec_idx++) {
			if (spec_bindings[spec_idx] == sorted_bindings && spec_types[spec_idx] == sorted_types) {
				existing = spec_idx;
				break;
			}
		}
		if (existing != DConstants::INVALID_INDEX) {
			spec_tracks_exact_domain[existing] = spec_tracks_exact_domain[existing] || track_exact_domain;
			continue;
		}
		spec_bindings.push_back(std::move(sorted_bindings));
		spec_types.push_back(std::move(sorted_types));
		spec_tracks_exact_domain.push_back(track_exact_domain);
	}

	auto thread_count = MaxValue<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads(), 1);
	for (idx_t spec_idx = 0; spec_idx < spec_bindings.size(); spec_idx++) {
		auto &types = spec_types[spec_idx];
		bool prefix_candidate = types.size() == 1 && types.front().IsIntegral() &&
		                        types.front() != LogicalType::UBIGINT && types.front() != LogicalType::HUGEINT &&
		                        types.front() != LogicalType::UHUGEINT;
		idx_t filter_bytes;
		idx_t build_bytes = 0;
		if (prefix_candidate) {
			// Prefix filters are chosen for a span of at most max(rows * 128,
			// 8M) bits. Account for the largest possible final bitmap and every
			// task-private build lane before construction starts.
			filter_bytes = SaturatingAdd(MaxValue<idx_t>(1024ULL * 1024ULL, SaturatingMultiply(rows, 16)), 64);
			auto private_bytes = SaturatingMultiply(filter_bytes, thread_count);
			build_bytes = private_bytes <= 16ULL * 1024ULL * 1024ULL ? private_bytes : filter_bytes;
			if (spec_tracks_exact_domain[spec_idx]) {
				build_bytes = SaturatingAdd(build_bytes, SaturatingMultiply(thread_count, 1024ULL * 1024ULL + 64));
			}
		} else {
			// DuckDB's Bloom filter uses 12 bits/key rounded to a power of two.
			// Four bytes/key safely covers rounding and the aligned allocation.
			filter_bytes = SaturatingAdd(MaxValue<idx_t>(512, SaturatingMultiply(rows, 4)), 64);
		}
		persistent = SaturatingAdd(persistent, filter_bytes);
		temporary = SaturatingAdd(temporary, build_bytes);
	}
}

idx_t ExcitationGraphManager::RetainedMemoryUsage() const {
	idx_t retained = executor_.MaterializedMemoryUsage();
	retained = SaturatingAdd(retained, filter_memory_used_);
	if (estimator_) {
		retained = SaturatingAdd(retained, estimator_->MemoryUsage());
	}
	return retained;
}

void ExcitationGraphManager::AccountFilterMemory(const shared_ptr<RPTFilter> &filter) {
	if (!filter || !accounted_filters_.insert(filter.get()).second) {
		return;
	}
	filter_memory_used_ = SaturatingAdd(filter_memory_used_, filter->MemoryUsage());
}

void ExcitationGraphManager::ResizeMemoryReservation(idx_t size) {
	if (!memory_state_) {
		return;
	}
	if (size == 0) {
		memory_state_->SetZero();
	} else {
		memory_state_->SetRemainingSizeAndUpdateReservation(context, size);
	}
}

bool ExcitationGraphManager::FitsMemoryBudget(idx_t retained, idx_t additional) {
	if (retained > memory_budget_ || additional > memory_budget_ - retained) {
		return false;
	}
	auto required = retained + additional;
	ResizeMemoryReservation(required);
	if (config.log_transfer_steps) {
		std::cerr << "[Bloom-Memory] admission required=" << required
		          << " reservation=" << (memory_state_ ? memory_state_->GetReservation() : 0)
		          << " budget=" << memory_budget_ << '\n';
	}
	if (required > 0 && (!memory_state_ || memory_state_->GetReservation() < required)) {
		return false;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto limit = buffer_manager.GetOperatorMemoryLimit();
	auto used = buffer_manager.GetUsedMemory();
	auto available = used < limit ? limit - used : 0;
	return additional <= available;
}

shared_ptr<ColumnDataCollection>
ExcitationGraphManager::RetainMaterializedDataForExecution(shared_ptr<ColumnDataCollection> data,
                                                           bool owns_materialized_data) {
	if (!data || !owns_materialized_data) {
		return data;
	}
	if (execution_collections_.insert(data.get()).second) {
		execution_memory_used_ = SaturatingAdd(execution_memory_used_, data->AllocationSize());
	}
	return HoldMemoryState(std::move(data), memory_state_);
}

shared_ptr<RPTFilter> ExcitationGraphManager::RetainFilterForExecution(shared_ptr<RPTFilter> filter) {
	if (!filter) {
		return filter;
	}
	if (execution_filters_.insert(filter.get()).second) {
		execution_memory_used_ = SaturatingAdd(execution_memory_used_, filter->MemoryUsage());
	}
	return HoldMemoryState(std::move(filter), memory_state_);
}

void ExcitationGraphManager::FinalizeMemoryReservationForExecution() {
	D_ASSERT(execution_memory_used_ <= memory_budget_);
	if (execution_memory_used_ > memory_budget_) {
		throw OutOfMemoryException("Bloom retained %s for execution, exceeding its %s memory budget",
		                           StringUtil::BytesToHumanReadableString(execution_memory_used_),
		                           StringUtil::BytesToHumanReadableString(memory_budget_));
	}
	if (execution_memory_used_ > 0) {
		D_ASSERT(memory_state_);
		if (!memory_state_) {
			throw InternalException("Bloom retained execution memory without a temporary-memory state");
		}
		memory_state_->SetMinimumReservation(execution_memory_used_);
	}
	ResizeMemoryReservation(execution_memory_used_);
	if (execution_memory_used_ > 0 && memory_state_->GetReservation() < execution_memory_used_) {
		throw OutOfMemoryException("DuckDB could reserve only %s of the %s retained by Bloom for query execution",
		                           StringUtil::BytesToHumanReadableString(memory_state_->GetReservation()),
		                           StringUtil::BytesToHumanReadableString(execution_memory_used_));
	}
}

void ExcitationGraphManager::StopForMemory(const char *phase, idx_t requested, idx_t retained) {
	memory_stopped_ = true;
	auto whole_query_fallback = materialized_source_count_ == 0;
	if (ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps) {
		std::cerr << "[Bloom-Memory] transfer stopped phase=" << phase << " requested=" << requested
		          << " retained=" << retained << " budget=" << memory_budget_
		          << " completed_sources=" << materialized_source_count_ << '\n';
	}
	if (whole_query_fallback) {
		result_transfer_steps.clear();
		executor_.Clear();
		cascade_filters_.clear();
		row_id_filters_.clear();
		direct_filters_.clear();
		filter_cache_ = RPTFilterCache {};
		filter_memory_used_ = 0;
		accounted_filters_.clear();
		estimator_.reset();
		ResizeMemoryReservation(0);
	}
	// Release abandoned query-local state before asking the log storage for
	// memory on the whole-query fallback path.
	LogBloomMemoryStopped(context, ++structured_log_sequence_, phase, whole_query_fallback, requested, retained,
	                      memory_budget_, materialized_source_count_);
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Initialization
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::InitEstimator() {
	estimator_ = make_uniq<SamplingCardinalityEstimator>(context, config.sampling);
}

idx_t ExcitationGraphManager::ComputeBaseTableRows(const LogicalOperator &op) const {
	// Walk through the operator tree to find the underlying LogicalGet.
	const LogicalOperator *leaf = &op;
	while (leaf->type != LogicalOperatorType::LOGICAL_GET && !leaf->children.empty()) {
		leaf = leaf->children[0].get();
	}

	idx_t base_rows = op.estimated_cardinality;
	if (leaf->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = leaf->Cast<LogicalGet>();
		if (auto entry = get.GetTable()) {
			base_rows = entry->GetStorage().GetTotalRows();
		}
	}
	return base_rows;
}

void ExcitationGraphManager::InitializeWorkingSet() {
	state_.Reset();
	filter_cache_ = RPTFilterCache {};
	if (config.excitation_mode == RPTExcitationMode::JOIN_KEY_NDV) {
		equality_domains_.Reset(table_groups);
	} else {
		equality_domains_.Reset({});
	}

	const auto &all_tables = table_operator_manager.GetAllTableOperators();

	// Register CDC-alias groups before any SeedColumn so CTE mirrors share
	// lineage from the start (prevents self-to-self BF transfer).
	for (auto &entry : all_tables) {
		auto &aliases = table_operator_manager.GetCDCAliases(entry.first);
		if (aliases.size() >= 2) {
			state_.Lineage().RegisterAliasGroup(aliases);
		}
	}

	// Seed only join-key columns — those are the only ones flooding reads.
	auto seed_column_lineage = [this](idx_t table_id) {
		auto nm_it = neighbor_matrix.find(table_id);
		if (nm_it == neighbor_matrix.end()) {
			return;
		}
		for (auto &neighbor_entry : nm_it->second) {
			for (auto &edge : neighbor_entry.second) {
				for (auto &b : edge.left_bindings) {
					state_.Lineage().SeedColumn(b);
				}
			}
		}
	};

	auto init_lineage = [&](idx_t table_id) {
		state_.Lineage().InitTable(table_id);
		seed_column_lineage(table_id);
	};

	for (auto &entry : all_tables) {
		idx_t table_id = entry.first;
		auto &op = entry.second.get();

		// Protected (e.g. below TOP_N/LIMIT): seed lineage only — do not excite.
		if (protected_tables_.count(table_id)) {
			init_lineage(table_id);
			continue;
		}

		// Lifted CTE (bare or FILTER+CHUNK_GET): baseline = CDC size from the
		// leaf; pre-register so RewriteQueryPlan can take MemoryScan.
		const LogicalOperator *leaf = &op;
		while (leaf->type == LogicalOperatorType::LOGICAL_FILTER && leaf->children.size() == 1) {
			leaf = leaf->children[0].get();
		}
		if (leaf->type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
			auto &chunk_get = leaf->Cast<LogicalColumnDataGet>();
			double base_rows = chunk_get.collection ? static_cast<double>(chunk_get.collection->Count()) : 0;
			// If a FILTER sits above CHUNK_GET we need the estimator for the
			// post-filter cardinality; for bare CHUNK_GET card == baseline.
			double row_count = (leaf == &op) ? base_rows : static_cast<double>(estimator_->Estimate(op));
			init_lineage(table_id);
			state_.AddTable(table_id, row_count, base_rows, true);
			executor_.Register(op);
			continue;
		}

		double true_card = static_cast<double>(estimator_->Estimate(op));
		double base_rows = static_cast<double>(ComputeBaseTableRows(op));
		// Only excite if local filters reduce cardinality enough.
		bool will_be_active = true_card < base_rows * config.excitation_threshold;
		init_lineage(table_id);
		state_.AddTable(table_id, true_card, base_rows, will_be_active);
	}

	// Reverse adjacency for O(neighbors) incoming-edge lookup in PrepareSourceTable.
	for (auto &[src_id, neighbors] : neighbor_matrix) {
		for (auto &[dst_id, edges] : neighbors) {
			for (auto &edge : edges) {
				state_.AddIncomingEdge(dst_id, src_id, edge);
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Edge construction
//===--------------------------------------------------------------------===//

shared_ptr<GraphEdge> ExcitationGraphManager::MakeBloomFilterEdge(idx_t source_id, idx_t dest_id,
                                                                  const EdgeInfo &edge) const {
	auto bf_edge = make_shared_ptr<GraphEdge>(source_id, dest_id);
	bf_edge->return_types = edge.return_types;

	// Because EdgeInfo in neighbor_matrix is symmetrically flipped,
	// edge.left_bindings strictly correspond to the source_id, and
	// edge.right_bindings correspond to the dest_id.
	D_ASSERT(!edge.left_bindings.empty());
	D_ASSERT(edge.left_bindings.size() == edge.right_bindings.size());
	D_ASSERT(edge.left_bindings.front().table_index.index == source_id);
	D_ASSERT(edge.right_bindings.front().table_index.index == dest_id);

	bf_edge->source_columns = edge.left_bindings;
	bf_edge->dest_columns = edge.right_bindings;
	return bf_edge;
}

vector<shared_ptr<GraphEdge>> ExcitationGraphManager::CollectOutgoingEdges(idx_t table_id) const {
	vector<shared_ptr<GraphEdge>> result;
	auto it = neighbor_matrix.find(table_id);
	if (it == neighbor_matrix.end()) {
		return result;
	}

	for (auto &neighbor_pair : it->second) {
		idx_t neighbor_id = neighbor_pair.first;
		auto &edges = neighbor_pair.second;
		if (!state_.HasCardinality(neighbor_id)) {
			continue;
		}
		auto neighbor_cardinality = state_.Cardinality(neighbor_id);
		if (neighbor_cardinality == 0.0) {
			continue;
		}
		// Skip sending filters to tables that are already small enough —
		// filtering them further has negligible benefit but costs materialization.
		if (neighbor_cardinality <= static_cast<double>(config.small_table_threshold)) {
			continue;
		}

		for (auto &edge : edges) {
			if (edge.protect_right) {
				continue;
			}
			if (!state_.Lineage().EdgeCarriesNewInfo(edge)) {
				continue;
			}
			result.push_back(MakeBloomFilterEdge(table_id, neighbor_id, edge));
		}
	}
	std::sort(result.begin(), result.end(), [](const shared_ptr<GraphEdge> &left, const shared_ptr<GraphEdge> &right) {
		auto edge_key = [](const GraphEdge &edge) {
			std::stringstream key;
			key << edge.destination << ":";
			vector<idx_t> order(edge.source_columns.size());
			std::iota(order.begin(), order.end(), idx_t {0});
			std::sort(order.begin(), order.end(), [&](idx_t a, idx_t b) {
				const auto &left_binding = edge.source_columns[a];
				const auto &right_binding = edge.source_columns[b];
				if (left_binding.table_index.index != right_binding.table_index.index) {
					return left_binding.table_index.index < right_binding.table_index.index;
				}
				if (left_binding.column_index != right_binding.column_index) {
					return left_binding.column_index < right_binding.column_index;
				}
				return edge.dest_columns[a].column_index < edge.dest_columns[b].column_index;
			});
			for (auto index : order) {
				key << edge.source_columns[index].column_index << ">" << edge.dest_columns[index].column_index << ",";
			}
			return key.str();
		};
		return edge_key(*left) < edge_key(*right);
	});
	return result;
}

//===--------------------------------------------------------------------===//
// ExcitationGraphManager — Cardinality update
//===--------------------------------------------------------------------===//

void ExcitationGraphManager::UpdateNeighborCardinalities(const vector<shared_ptr<GraphEdge>> &candidates) {
	unordered_set<idx_t> seen_neighbors;
	for (auto &edge : candidates) {
		idx_t neighbor_id = edge->destination;
		if (!seen_neighbors.insert(neighbor_id).second) {
			continue;
		}
		if (!state_.HasCardinality(neighbor_id)) {
			continue;
		}
		auto prior_card = state_.Cardinality(neighbor_id);

		auto &dest_filters = cascade_filters_[neighbor_id];
		auto *dest_op = table_operator_manager.GetTableOperator(neighbor_id);
		if (!dest_op) {
			continue;
		}
		// Single-column cascade filters become direct filters for the
		// cardinality oracle. Composite-key filters cannot be expressed as
		// per-column TableFilters, so we skip them here — they only
		// participate via the materialized scanner path.
		vector<DirectFilterInfo> direct_filters;
		direct_filters.reserve(dest_filters.size());
		for (auto &dest_filter : dest_filters) {
			if (dest_filter.bindings.size() != 1) {
				continue;
			}
			direct_filters.push_back({dest_filter.bindings.front(), dest_filter.filter, dest_filter.lineage.lineages});
		}

		auto *dest_scanner = executor_.Find(*dest_op);
		double current_card = dest_scanner ? static_cast<double>(estimator_->Estimate(*dest_scanner, direct_filters))
		                                   : static_cast<double>(estimator_->Estimate(*dest_op, direct_filters));

		// Update both table-level and per-column lineage (the latter is read
		// by the next round's CollectOutgoingEdges and ActivateTable for
		// subsumption, in column-granular mode).
		state_.Lineage().Propagate(*edge);

		state_.AddDependency(neighbor_id, edge->source);

		auto last_card = state_.Baseline(neighbor_id);
		bool reactivated = state_.UpdateCardinality(neighbor_id, current_card, config.excitation_threshold);
		if (ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps) {
			auto dest_name = TableOperatorManager::GetTableName(*dest_op);
			std::cerr << "  [Bloom-Estimate] dest=[" << neighbor_id << "] name=" << dest_name
			          << " prior=" << static_cast<idx_t>(prior_card) << " baseline=" << static_cast<idx_t>(last_card)
			          << " estimate=" << static_cast<idx_t>(current_card)
			          << " decision_boundary=" << static_cast<idx_t>(last_card * config.excitation_threshold)
			          << " filters=" << direct_filters.size() << " source=" << (dest_scanner ? "materialized" : "base")
			          << " reactivate=" << (reactivated ? 1 : 0) << '\n';
		}
	}
}

void ExcitationGraphManager::PropagateZeroCardinality(idx_t table_id, const vector<shared_ptr<GraphEdge>> &candidates) {
	unordered_set<idx_t> seen_neighbors;

	for (auto &edge : candidates) {
		idx_t neighbor_id = edge->destination;
		if (!seen_neighbors.insert(neighbor_id).second) {
			continue;
		}

		if (!state_.HasCardinality(neighbor_id) || state_.Cardinality(neighbor_id) == 0.0) {
			continue;
		}

		state_.MarkZero(neighbor_id);

		// Destination inherits the source lineage even though the filter
		// itself is just an empty-result marker.
		state_.Lineage().Propagate(*edge);

		state_.AddDependency(neighbor_id, table_id);
		cascade_filters_[neighbor_id].clear();
		direct_filters_[neighbor_id].clear();
		row_id_filters_.erase(neighbor_id);
	}
}

void ExcitationGraphManager::ExecuteTransfer() {
	result_transfer_steps.clear();
	execution_history_.clear();
	execution_action_count_ = 0;
	execution_round_count_ = 0;
	filter_memory_used_ = 0;
	accounted_filters_.clear();
	execution_memory_used_ = 0;
	execution_collections_.clear();
	execution_filters_.clear();
	materialized_source_count_ = 0;
	memory_stopped_ = false;
	executor_.Clear();
	cascade_filters_.clear();
	row_id_filters_.clear();
	direct_filters_.clear();

	bool bloom_diagnostic_log = ClientConfig::GetConfig(context).enable_profiler || config.log_transfer_steps;
	bool bloom_structured_log = BloomStructuredLoggingEnabled(context);
	bool bloom_timing = bloom_diagnostic_log || bloom_structured_log;

	// Per-phase wall-clock accounting, printed with the transfer log. All
	// timestamps are taken only when logging is enabled.
	using SteadyClock = std::chrono::steady_clock;
	auto elapsed_ms = [](SteadyClock::time_point a, SteadyClock::time_point b) {
		return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(b - a).count();
	};
	double init_ms = 0, mat_ms = 0, bf_ms = 0, est_ms = 0, finalize_ms = 0;

	auto t_init = bloom_timing ? SteadyClock::now() : SteadyClock::time_point {};
	InitializeMemoryBudget();
	auto estimated_sample_memory = EstimateInitialSampleMemory();
	if (bloom_structured_log) {
		LogBloomStarted(context, ++structured_log_sequence_,
		                config.sampling.mode == RPTSamplingMode::PREPARED ? "prepared" : "instant",
		                config.excitation_mode == RPTExcitationMode::TABLE_SIZE ? "table_size" : "join_key_ndv",
		                config.sampling.target_rows, estimated_sample_memory, memory_budget_);
	}
	if (memory_budget_ == 0 || !FitsMemoryBudget(0, estimated_sample_memory)) {
		StopForMemory("sample_admission", estimated_sample_memory, 0);
		return;
	}
	InitEstimator();
	InitializeWorkingSet();
	auto sampled_memory = estimator_->MemoryUsage();
	if (!FitsMemoryBudget(sampled_memory, 0)) {
		StopForMemory("sample_actual", 0, sampled_memory);
		return;
	}
	if (bloom_timing) {
		init_ms = elapsed_ms(t_init, SteadyClock::now());
	}

	if (bloom_diagnostic_log) {
		std::cerr << "[Bloom-SamplingConfig] mode="
		          << (config.sampling.mode == RPTSamplingMode::PREPARED ? "prepared" : "instant")
		          << " target_rows=" << config.sampling.target_rows;
		if (config.sampling.mode == RPTSamplingMode::INSTANT) {
			std::cerr << " access="
			          << (config.sampling.instant_access == RPTInstantAccessMode::SCATTERED ? "scattered" : "block")
			          << " consistency=" << (config.sampling.instant_snapshot ? "snapshot" : "storage_direct")
			          << " access_points=" << config.sampling.instant_access_points
			          << " rows_per_access=" << config.sampling.instant_rows_per_access
			          << " block_windows=" << config.sampling.instant_block_windows
			          << " parquet_row_groups=" << config.sampling.instant_parquet_row_groups
			          << " seed=" << config.sampling.seed;
		}
		std::cerr << " excitation="
		          << (config.excitation_mode == RPTExcitationMode::TABLE_SIZE ? "table_size" : "join_key_ndv");
		if (config.excitation_mode == RPTExcitationMode::JOIN_KEY_NDV) {
			std::cerr << " exact_domains=" << equality_domains_.TrackedDomainCount();
		}
		std::cerr << '\n';
		std::cerr << "[Bloom-Excitation] === Initial table cardinalities ===" << '\n';
		vector<idx_t> table_ids;
		table_ids.reserve(state_.Cardinalities().size());
		for (auto &entry : state_.Cardinalities()) {
			table_ids.push_back(entry.first);
		}
		std::sort(table_ids.begin(), table_ids.end());
		for (auto tid : table_ids) {
			auto cardinality = state_.Cardinality(tid);
			auto *op = table_operator_manager.GetTableOperator(tid);
			std::string name = op ? TableOperatorManager::GetTableName(*op) : "?";
			bool active = state_.IsActive(tid);
			std::cerr << "  [" << tid << "] " << name << "  card=" << static_cast<idx_t>(cardinality)
			          << "  baseline=" << static_cast<idx_t>(state_.Baseline(tid)) << (active ? "  ACTIVE" : "")
			          << '\n';
		}
		std::cerr << "[Bloom-Excitation] active_nodes count: " << state_.ActiveTableCount() << '\n';
	}

	idx_t excitation_round = 0;
	idx_t structured_transfer_count = 0;
	std::stringstream execution_history;

	// Main adaptive-excitation loop:
	// 1. pick the smallest active source table A
	// 2. materialize A once (with any filters it has already received)
	// 3. determine which outgoing edges from A still carry new information
	// 4. build/apply A's outgoing filters to the destination tables B
	// 5. re-estimate each affected B after those filters are attached
	while (state_.HasActiveTables()) {
		idx_t table_id = state_.PopSmallestActiveTable();

		excitation_round++;
		if (bloom_diagnostic_log) {
			execution_history << "R" << excitation_round << ":S" << table_id;
		}
		if (bloom_diagnostic_log) {
			auto *src_op = table_operator_manager.GetTableOperator(table_id);
			std::string src_name = src_op ? TableOperatorManager::GetTableName(*src_op) : "?";
			std::cerr << "[Bloom-Excitation] --- Round " << excitation_round << ": source=[" << table_id << "] "
			          << src_name << "  card=" << static_cast<idx_t>(state_.Cardinality(table_id))
			          << "  remaining_active=" << state_.ActiveTableCount() << '\n';
		}

		auto informative_candidates = CollectOutgoingEdges(table_id);
		if (informative_candidates.empty()) {
			if (bloom_diagnostic_log) {
				execution_history << ":no_edges;";
			}
			continue;
		}

		if (state_.Cardinality(table_id) == 0.0) {
			if (bloom_diagnostic_log) {
				execution_history << ":zero{";
				for (auto &edge : informative_candidates) {
					AppendEdgeSignature(execution_history, table_id, *edge);
					execution_action_count_++;
				}
				execution_history << "};";
			}
			TransferStep step;
			step.id = state_.NextStepId();
			step.table = table_operator_manager.GetTableOperator(table_id);
			step.depends_on = state_.Dependencies(table_id);
			step.create_bf = informative_candidates;

			unordered_map<idx_t, idx_t> destination_rows_before;
			if (bloom_structured_log) {
				for (auto &edge : informative_candidates) {
					destination_rows_before.emplace(edge->destination,
					                                static_cast<idx_t>(state_.Cardinality(edge->destination)));
				}
			}
			PropagateZeroCardinality(table_id, informative_candidates);
			if (bloom_structured_log) {
				auto *source_op = table_operator_manager.GetTableOperator(table_id);
				auto source_name = source_op ? TableOperatorManager::GetTableName(*source_op) : string("?");
				for (auto &edge : informative_candidates) {
					auto *destination_op = table_operator_manager.GetTableOperator(edge->destination);
					auto destination_name =
					    destination_op ? TableOperatorManager::GetTableName(*destination_op) : string("?");
					LogBloomTransfer(context, ++structured_log_sequence_, excitation_round, step.id, table_id,
					                 source_name, edge->destination, destination_name, 0,
					                 destination_rows_before.at(edge->destination), 0, edge->source_columns.size(),
					                 false, 0);
					structured_transfer_count++;
				}
			}
			result_transfer_steps.push_back(std::move(step));
			continue;
		}

		auto *source_op = table_operator_manager.GetTableOperator(table_id);
		if (!source_op) {
			continue;
		}
		idx_t persistent_filter_memory, temporary_filter_memory;
		EstimateFilterMemory(static_cast<idx_t>(state_.Cardinality(table_id)), informative_candidates,
		                     persistent_filter_memory, temporary_filter_memory);
		auto maximum_source_rows = MaxValue<idx_t>(ComputeBaseTableRows(*source_op), source_op->estimated_cardinality);
		auto *registered_scanner = executor_.Find(*source_op);
		idx_t collection_memory = 0;
		idx_t collection_copies = 0;
		if (!executor_.IsMaterialized(*source_op)) {
			collection_memory = EstimateCollectionMemory(maximum_source_rows, source_op->types);
			// A disk source can briefly hold its initial materialization and its
			// filtered compacted replacement at the same time.
			collection_copies = 2;
		} else if (registered_scanner && !executor_.OwnsMaterializedData(*source_op) &&
		           registered_scanner->NeedsCompaction()) {
			// CHUNK_GET is already in memory and remains owned by DuckDB. Applying
			// pending filters creates exactly one Bloom-owned compacted collection.
			auto *borrowed = registered_scanner->GetData();
			collection_memory = borrowed ? borrowed->AllocationSize() : 0;
			collection_copies = 1;
		}
		// Filters and their task-local build states coexist with any new source
		// collections accounted above.
		auto requested_memory = SaturatingMultiply(collection_memory, collection_copies);
		requested_memory = SaturatingAdd(requested_memory, persistent_filter_memory);
		requested_memory = SaturatingAdd(requested_memory, temporary_filter_memory);
		auto retained_memory = RetainedMemoryUsage();
		if (!FitsMemoryBudget(retained_memory, requested_memory)) {
			StopForMemory("materialize_admission", requested_memory, retained_memory);
			break;
		}

		auto t_mat = bloom_timing ? SteadyClock::now() : SteadyClock::time_point {};
		PrepareSourceTable(table_id);
		double round_mat = bloom_timing ? elapsed_ms(t_mat, SteadyClock::now()) : 0;
		auto estimated_source_card = state_.Cardinality(table_id);
		mat_ms += round_mat;
		auto *source_scanner = executor_.Find(*source_op);
		// The current collection is already part of retained memory. Only
		// Compact() needs another collection, and only while filters remain.
		auto compaction_memory = source_scanner && source_scanner->NeedsCompaction() && source_scanner->GetData()
		                             ? source_scanner->GetData()->AllocationSize()
		                             : 0;
		if (source_scanner) {
			EstimateFilterMemory(source_scanner->Count(), informative_candidates, persistent_filter_memory,
			                     temporary_filter_memory);
		}
		retained_memory = RetainedMemoryUsage();
		requested_memory = SaturatingAdd(compaction_memory, persistent_filter_memory);
		requested_memory = SaturatingAdd(requested_memory, temporary_filter_memory);
		if (!FitsMemoryBudget(retained_memory, requested_memory)) {
			executor_.Remove(*source_op);
			StopForMemory("filter_admission", requested_memory, RetainedMemoryUsage());
			break;
		}
		materialized_source_count_++;
		state_.CommitBaseline(table_id);
		if (bloom_diagnostic_log && source_op) {
			std::cerr << "  [Bloom-Materialized] source=[" << table_id
			          << "] name=" << TableOperatorManager::GetTableName(*source_op)
			          << " estimated=" << static_cast<idx_t>(estimated_source_card)
			          << " actual=" << (source_scanner ? source_scanner->Count() : 0) << '\n';
		}

		TransferStep step;
		step.id = state_.NextStepId();
		step.table = source_op;
		step.depends_on = state_.Dependencies(table_id);

		auto t_bf = bloom_timing ? SteadyClock::now() : SteadyClock::time_point {};
		auto effective_candidates = ActivateTables(table_id, informative_candidates);
		double round_bf = bloom_timing ? elapsed_ms(t_bf, SteadyClock::now()) : 0;
		bf_ms += round_bf;
		if (effective_candidates.empty()) {
			if (bloom_diagnostic_log) {
				execution_history << ":materialized_no_effective;";
			}
			if (bloom_diagnostic_log) {
				std::cerr << "    [timing] materialize=" << round_mat << "ms build_bf=" << round_bf << "ms" << '\n';
			}
			continue;
		}
		step.create_bf = effective_candidates;
		if (bloom_diagnostic_log) {
			execution_history << ":actions{";
			for (auto &edge : effective_candidates) {
				AppendEdgeSignature(execution_history, table_id, *edge);
				execution_action_count_++;
			}
			execution_history << "};";
		}

		unordered_map<idx_t, idx_t> destination_rows_before;
		if (bloom_structured_log) {
			for (auto &edge : effective_candidates) {
				destination_rows_before.emplace(edge->destination,
				                                static_cast<idx_t>(state_.Cardinality(edge->destination)));
			}
		}
		auto t_est = bloom_timing ? SteadyClock::now() : SteadyClock::time_point {};
		UpdateNeighborCardinalities(effective_candidates);
		double round_est = bloom_timing ? elapsed_ms(t_est, SteadyClock::now()) : 0;
		est_ms += round_est;
		if (bloom_structured_log) {
			auto source_name = TableOperatorManager::GetTableName(*source_op);
			auto source_rows = source_scanner ? source_scanner->Count() : 0;
			for (auto &edge : effective_candidates) {
				auto *destination_op = table_operator_manager.GetTableOperator(edge->destination);
				auto destination_name =
				    destination_op ? TableOperatorManager::GetTableName(*destination_op) : string("?");
				LogBloomTransfer(context, ++structured_log_sequence_, excitation_round, step.id, table_id, source_name,
				                 edge->destination, destination_name, source_rows,
				                 destination_rows_before.at(edge->destination),
				                 static_cast<idx_t>(state_.Cardinality(edge->destination)), edge->source_columns.size(),
				                 state_.IsActive(edge->destination), round_mat + round_bf + round_est);
				structured_transfer_count++;
			}
		}
		if (bloom_diagnostic_log) {
			std::cerr << "    [timing] materialize=" << round_mat << "ms build_bf=" << round_bf
			          << "ms re_estimate=" << round_est << "ms" << '\n';
		}

		if (bloom_diagnostic_log) {
			for (auto &edge : effective_candidates) {
				auto *dest_op = table_operator_manager.GetTableOperator(edge->destination);
				std::string dest_name = dest_op ? TableOperatorManager::GetTableName(*dest_op) : "?";
				bool reactivated = state_.IsActive(edge->destination);
				std::cerr << "    -> [" << edge->destination << "] " << dest_name
				          << "  card=" << static_cast<idx_t>(state_.Cardinality(edge->destination))
				          << (reactivated ? "  RE-ACTIVATED" : "") << '\n';
			}
		}

		result_transfer_steps.push_back(std::move(step));
	}
	if (bloom_diagnostic_log) {
		execution_history_ = execution_history.str();
		execution_round_count_ = excitation_round;
	}
	if (memory_stopped_ && materialized_source_count_ == 0) {
		return;
	}

	auto t_fin = bloom_timing ? SteadyClock::now() : SteadyClock::time_point {};
	GenerateJoinStageExecutionPlan();
	if (bloom_timing) {
		finalize_ms = elapsed_ms(t_fin, SteadyClock::now());
	}
	if (bloom_diagnostic_log) {
		std::cerr << "[Bloom-Timing] init_estimates=" << init_ms << "ms materialize=" << mat_ms
		          << "ms build_bf=" << bf_ms << "ms re_estimate=" << est_ms << "ms finalize=" << finalize_ms
		          << "ms total=" << (init_ms + mat_ms + bf_ms + est_ms + finalize_ms) << "ms" << '\n';
	}
	if (bloom_structured_log) {
		LogBloomTransferCompleted(context, ++structured_log_sequence_, memory_stopped_, excitation_round,
		                          structured_transfer_count, materialized_source_count_, RetainedMemoryUsage(),
		                          memory_budget_, init_ms, mat_ms, bf_ms, est_ms, finalize_ms);
	}
}

} // namespace duckdb
