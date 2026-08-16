#pragma once

#include "predicate_transfer/cardinality_estimation/instant_sampler/instant_sampler.hpp"

namespace duckdb {

class BlockHandle;
class BufferManager;
class RowGroupCollection;
class RowGroupSegmentTree;
struct TransactionData;

namespace instant_sampler_internal {

// These provisional native-storage limits preserve the rollout values. They
// are engineering guardrails, not benchmark-proven optima; revisit 8/4 with a
// dedicated warm/cold fan-out sweep across work-unit counts and pool sizes.
static constexpr idx_t INSTANT_SCATTERED_TASK_LIMIT = 8;
static constexpr idx_t INSTANT_BLOCK_TASK_LIMIT = 4;

struct PhysicalRowGroupSpan {
	idx_t index;
	idx_t row_start;
	idx_t row_count;
	idx_t access_count;
};

struct SelectedStorageVector {
	idx_t row_start;
	idx_t row_count;
	vector<idx_t> sample_offsets;
};

struct DirectStorageScanTiming {
	double locate_ms = 0;
	double decode_ms = 0;
	double filter_ms = 0;
	double append_ms = 0;
	double task_wall_ms = 0;
};

bool SupportsBasePhysicalScan(const LogicalType &type);
idx_t CeilDivide(idx_t value, idx_t divisor);
vector<idx_t> SelectStratifiedExactlyK(idx_t population_size, idx_t target_size, std::mt19937_64 &random);
vector<idx_t> SelectWindowRows(idx_t window_size, idx_t target_size, std::mt19937_64 &random);
vector<idx_t> AllocateProportionalQuotas(const vector<idx_t> &capacities, idx_t target_size);

void PrefetchStorageBlocks(ClientContext &context, BufferManager &buffer_manager,
                           const vector<shared_ptr<BlockHandle>> &blocks, InstantSampleResult &result);

void ExecuteStorageScan(ClientContext &context, RowGroupCollection &collection, RowGroupSegmentTree &row_group_tree,
                        const TransactionData &transaction, const vector<StorageIndex> &column_ids,
                        const vector<LogicalType> &column_types, const vector<SelectedStorageVector> &sample_vectors,
                        idx_t task_limit, bool collect_timing, bool snapshot_safe, const Expression *local_predicate,
                        InstantSampleResult &result);

} // namespace instant_sampler_internal
} // namespace duckdb
