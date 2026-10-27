#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"
#include "predicate_transfer/transfer_plan/transfer_types.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include <iostream>
#include <stdexcept>
using namespace duckdb;
static void Check(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}
static vector<ColumnBinding> Key(idx_t table, std::initializer_list<idx_t> cols) {
	vector<ColumnBinding> result;
	for (auto col : cols) {
		result.emplace_back(TableIndex(table), ProjectionIndex(col));
	}
	return result;
}
int main() {
	LineageTracker tracker;
	const auto a = Key(0, {0}), ab = Key(0, {0, 1}), bc = Key(0, {1, 2});
	for (auto key : {a, ab, bc, Key(1, {0}), Key(2, {0})}) {
		tracker.SeedKey(key);
	}
	const auto original_a = tracker.MakeSnapshot(a);
	const auto original_ab = tracker.MakeSnapshot(ab);
	const auto beta = tracker.MakeSnapshot(Key(1, {0}));
	LogicalDummyScan src(TableIndex(1)), dst(TableIndex(0));
	EdgeInfo edge({LogicalType::INTEGER}, src, Key(1, {0}), dst, a);
	Check(tracker.EdgeCarriesNewInfo(edge), "a new constraint carries information");
	tracker.ReceiveFilters(0, {{a, beta}}, false);
	Check(!tracker.EdgeCarriesNewInfo(edge), "receipt records C even when the estimate predicts no shrink");
	Check(tracker.MakeSnapshot(a).lineages == original_a.lineages, "no shrink must preserve L");
	Check(tracker.MakeSnapshot(ab).lineages == original_ab.lineages, "no shrink must preserve lifted L");
	const auto gamma = tracker.MakeSnapshot(Key(2, {0}));
	tracker.ReceiveFilters(0, {{a, gamma}}, true);
	Check(!tracker.EdgeCarriesNewInfo(edge), "old satisfied constraints survive shrink");
	const auto now_a = tracker.MakeSnapshot(a);
	const auto now_ab = tracker.MakeSnapshot(ab);
	Check(LineageTracker::IsSubset(original_a, now_a), "same key must retain result lineage");
	Check(LineageTracker::IsSubset(gamma, now_a), "same key must incorporate filter");
	Check(!LineageTracker::IsSubset(beta, now_a), "C must not be merged into L");
	Check(LineageTracker::IsSubset(original_ab, now_ab) && LineageTracker::IsSubset(gamma, now_ab), "a -> ab lift");
	Check(!LineageTracker::IsSubset(gamma, tracker.MakeSnapshot(bc)), "a -> bc needs fresh domain");
	Check(original_a.lineages.size() == 1, "snapshot must be immutable");
	tracker.ReceiveFilters(0, {{ab, now_ab}}, true);
	const auto after_composite = tracker.MakeSnapshot(a);
	Check(LineageTracker::IsSubset(now_a, after_composite), "cross-key updates retain previous result tokens");
	Check(after_composite.lineages.size() == now_a.lineages.size() + 1, "cross-key shrink adds one version");
	Check(!LineageTracker::IsSubset(now_ab, after_composite), "a composite predicate cannot be lifted to a subset key");
	EdgeInfo self_edge({LogicalType::INTEGER}, dst, a, dst, a);
	Check(!tracker.EdgeCarriesNewInfo(self_edge), "C contains the same projection version appended to L");
	const auto acceptance = tracker.NewPredicate(ab);
	Check(!LineageTracker::IsSubset(acceptance, now_ab), "Bloom acceptance differs from exact source domain");
	Check(tracker.MakeSnapshot(Key(0, {1, 0})).lineages == tracker.MakeSnapshot(ab).lineages, "key set ordering");
	// Aliases can compact independently: never share mutable L/C slots.
	tracker.SeedKey(Key(10, {0}));
	tracker.SeedKey(Key(11, {0}));
	const auto alias = tracker.MakeSnapshot(Key(11, {0}));
	Check(tracker.MakeSnapshot(Key(10, {0})).lineages != alias.lineages, "independent alias projections");
	tracker.ReceiveFilters(10, {{Key(10, {0}), gamma}}, true);
	Check(tracker.MakeSnapshot(Key(11, {0})).lineages == alias.lineages, "independent alias constraints");
	// A no-reduction cycle records constraints without creating new versions.
	LineageTracker cycle;
	for (idx_t i = 0; i < 3; i++) {
		cycle.SeedKey(Key(i, {0}));
	}
	for (idx_t i = 0; i < 3; i++) {
		const auto sender = cycle.MakeSnapshot(Key(i, {0}));
		for (idx_t j = 0; j < 3; j++) {
			if (i != j) {
				cycle.ReceiveFilters(j, {{Key(j, {0}), sender}}, false);
			}
		}
	}
	for (idx_t i = 0; i < 3; i++) {
		Check(cycle.MakeSnapshot(Key(i, {0})).lineages.size() == 1, "no-reduction cycle preserves L");
		for (idx_t j = 0; j < 3; j++) {
			EdgeInfo cycle_edge({LogicalType::INTEGER}, src, Key(i, {0}), dst, Key(j, {0}));
			Check(!cycle.EdgeCarriesNewInfo(cycle_edge), "no-reduction cycle stops after recording constraints");
		}
	}
	std::cout << "key lineage invariants passed\n";
}
