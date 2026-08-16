#pragma once

#include "duckdb/common/common.hpp"

#include <string>

namespace duckdb {

enum class RPTSamplingMode : uint8_t { PREPARED, INSTANT };

//! Criterion used after a transfer changes a table. TABLE_SIZE preserves the
//! original row-cardinality rule. JOIN_KEY_NDV additionally suppresses an
//! equality-domain transfer when its exact join-key domain has not shrunk.
enum class RPTExcitationMode : uint8_t { TABLE_SIZE, JOIN_KEY_NDV };

enum class RPTInstantAccessMode : uint8_t {
	//! Many small, stratified reads. Best when the base table is already resident.
	SCATTERED,
	//! A few block-aligned windows with explicit prefetch. Best for cold storage.
	BLOCK
};

struct RPTSamplingConfig {
	//! Sampling fraction for already-materialized intermediate relations.
	double intermediate_rate = 0.01;
	//! Target rows for prepared, block, and Parquet samples. Scattered instant
	//! sampling uses its explicit access-point shape below.
	idx_t target_rows = 10000;
	//! Prepared samples are the stable default. Instant sampling is explicitly
	//! selected when sample maintenance is undesirable.
	RPTSamplingMode mode = RPTSamplingMode::PREPARED;

	//! Prepared-sample persistence. "auto" stores the cache beside the database.
	std::string prepared_cache_dir = "auto";
	//! Keep prepared samples in DuckDB's ObjectCache after loading them.
	bool prepared_memory_cache = true;

	//! Physical access policy used by instant sampling for native DuckDB storage.
	RPTInstantAccessMode instant_access = RPTInstantAccessMode::SCATTERED;
	//! Read the active transaction snapshot instead of narrow base-column ranges.
	//! Direct reads deliberately ignore update/delete overlays and transaction-local
	//! rows because samples guide only optimization; formal execution still uses MVCC.
	bool instant_snapshot = false;
	//! The scattered policy reads 256 stratified points x 32 contiguous rows.
	idx_t instant_access_points = 256;
	idx_t instant_rows_per_access = 32;
	//! The block policy reads a small number of aligned windows and samples rows
	//! inside each window after I/O. Parquet is always sampled by row group.
	idx_t instant_block_windows = 16;
	idx_t instant_parquet_row_groups = 8;
	//! Reproducible seed mixed with stable table identity. Seed 2 is the
	//! cross-workload-validated release configuration.
	idx_t seed = 2;
};

class RPTOptimizerConfig {
public:
	bool late_materialize_flag = false;
	//! Protect the left-leaf table below TOP_N / LIMIT / MARK-join from excitation.
	//! These tables benefit from early termination; RPT materialisation would defeat it.
	bool enable_table_protection = false;
	//! Skip RPT when the plan contains a pure range-inequality join
	//! (<, >, <=, >=) with no equality key. Mixed joins that also contain an
	//! equality condition remain eligible for RPT on their equality graph.
	bool skip_on_inequality_join = true;
	//! Skip RPT entirely when the plan contains any set operator
	//! (UNION / UNION ALL / INTERSECT / EXCEPT). Those plans split into
	//! independent sub-branches that don't benefit from cross-branch BF transfer.
	bool skip_on_set_operator = true;
	//! Do not optimizer-time lift a CTE definition when it contains a filter
	//! above an aggregation. Such CTEs are self-selective; eager execution and
	//! RPT materialisation of their internals rarely pay off.
	bool skip_cte_with_filter_agg = true;
	//! Do not optimizer-time lift any CTE definition that contains an
	//! aggregation, even without a filter above it. Stronger than
	//! skip_cte_with_filter_agg: large aggregate CTEs can be safe to execute
	//! but much more expensive when eagerly materialized.
	bool skip_cte_with_agg = true;
	//! Execute and lift optimizer-time-safe MATERIALIZED CTE definitions.
	//! Definitions whose physical plans may schedule tasks through DuckDB's
	//! not-yet-installed outer query Executor remain intact for runtime.
	bool enable_materialized_cte_lifting = true;
	//! Skip RPT when the scope's join tree is fully left-deep (every join's
	//! right input is a base table / CTE scan). Such plans already get most of
	//! their benefit from join-side filter pushdown; RPT's materialization
	//! overhead rarely pays off.
	bool skip_left_deep_join_tree = true;

	//! Bundle transitive column pairs linking the same table pair into a single
	//! composite edge (multi-key BF) instead of emitting independent single-key
	//! edges. Composite BFs are strictly more selective but may hurt queries
	//! where the extra key columns add materialization overhead without payoff
	//! (observed: IMDB 21a/21c/27a/27c regress ~2x).
	bool bundle_composite_edges = true;

	//! Enable cross-source RPT filter cache. Two sources with equal lineage
	//! snapshots on the same canonical columns (e.g. the two aliases of a
	//! lifted CTE) share a single built BF instead of each rebuilding it.
	bool enable_filter_cache = true;

	RPTSamplingConfig sampling;
	//! Log transfer-plan generation to stderr: initial cardinalities, each
	//! flooding round (source pick, estimates, filter-cache hits) and the final
	//! ExcitationTimeline. Works for both the oracle and the sampling estimator
	//! and does not require the profiler (SET rpt_log_transfer_steps=true or
	//! env RPT_LOG_TRANSFER_STEPS=1). Used to diff oracle vs sampling paths.
	bool log_transfer_steps = false;
	//! Excitation threshold: re-excite if cardinality drops below this ratio of baseline
	double excitation_threshold = 1.0;
	//! How a re-excited source decides whether it carries new information.
	RPTExcitationMode excitation_mode = RPTExcitationMode::TABLE_SIZE;
	//! Tables with cardinality at or below this threshold are "small" and will
	//! only send filters, never receive them (no benefit from further filtering).
	idx_t small_table_threshold = 0;
};

} // namespace duckdb
