#pragma once

#include <string>

namespace duckdb {

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

	//! Sampling rate for adaptive excitation (default 1%) — used for CDC chunk sampling
	double sample_rate = 0.01;
	//! Target row count for the per-table raw-data sample (disk tables).
	idx_t sample_materialization_size = 10000;
	//! Directory for the on-disk per-table sample cache. Samples are keyed by
	//! database path, table identity, schema fingerprint, row count, method, and
	//! N, then reused across queries/runs so the (potentially full-table)
	//! generation cost is paid only once. Empty string disables disk persistence
	//! (samples are then rebuilt per query).
	std::string sample_cache_dir = "auto";
	//! Also keep loaded/generated samples in the DatabaseInstance ObjectCache
	//! (LRU, buffer-pool accounted) so later optimizations in the same process
	//! skip the per-PREPARE disk deserialization entirely.
	bool sample_memory_cache = true;
	//! Log transfer-plan generation to stderr: initial cardinalities, each
	//! flooding round (source pick, estimates, filter-cache hits) and the final
	//! ExcitationTimeline. Works for both the oracle and the sampling estimator
	//! and does not require the profiler (SET rpt_log_transfer_steps=true or
	//! env RPT_LOG_TRANSFER_STEPS=1). Used to diff oracle vs sampling paths.
	bool log_transfer_steps = false;
	//! Excitation threshold: re-excite if cardinality drops below this ratio of baseline
	double excitation_threshold = 1.0;
	//! Tables with cardinality at or below this threshold are "small" and will
	//! only send filters, never receive them (no benefit from further filtering).
	idx_t small_table_threshold = 0;
};

} // namespace duckdb
