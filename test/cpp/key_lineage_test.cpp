#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"
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
	tracker.EnableKeyLineage();
	const auto a = Key(0, {0}), ab = Key(0, {0, 1}), bc = Key(0, {1, 2});
	for (auto key : {a, ab, bc, Key(1, {0}), Key(2, {0})}) {
		tracker.SeedKey(key);
	}
	const auto original_a = tracker.MakeSnapshot(0, a);
	const auto original_ab = tracker.MakeSnapshot(0, ab);
	const auto beta = tracker.MakeSnapshot(1, Key(1, {0}));
	LogicalDummyScan src(TableIndex(1)), dst(TableIndex(0));
	EdgeInfo edge({LogicalType::INTEGER}, src, Key(1, {0}), dst, a);
	Check(tracker.EdgeCarriesNewInfo(edge), "pending is not yet satisfied");
	tracker.ApplyKeyFilters(0, {{a, beta}}, false, false);
	Check(!tracker.EdgeCarriesNewInfo(edge), "C must retain a no-shrink predicate");
	Check(tracker.MakeSnapshot(0, a).lineages == original_a.lineages, "no shrink must preserve L");
	Check(tracker.MakeSnapshot(0, ab).lineages == original_ab.lineages, "no shrink must preserve lifted L");
	const auto gamma = tracker.MakeSnapshot(2, Key(2, {0}));
	tracker.ApplyKeyFilters(0, {{a, gamma}}, true, false);
	Check(!tracker.EdgeCarriesNewInfo(edge), "old satisfied constraints survive shrink");
	const auto now_a = tracker.MakeSnapshot(0, a);
	const auto now_ab = tracker.MakeSnapshot(0, ab);
	Check(LineageTracker::IsSubset(original_a, now_a), "same key must retain result lineage");
	Check(LineageTracker::IsSubset(gamma, now_a), "same key must incorporate filter");
	Check(!LineageTracker::IsSubset(beta, now_a), "C must not be merged into L");
	Check(LineageTracker::IsSubset(original_ab, now_ab) && LineageTracker::IsSubset(gamma, now_ab), "a -> ab lift");
	Check(!LineageTracker::IsSubset(gamma, tracker.MakeSnapshot(0, bc)), "a -> bc needs fresh domain");
	Check(original_a.lineages.size() == 1, "snapshot must be immutable");
	tracker.ApplyKeyFilters(0, {{ab, now_ab}}, true, false);
	Check(!LineageTracker::IsSubset(now_a, tracker.MakeSnapshot(0, a)), "ab -> a is not a lift");
	const auto acceptance = tracker.NewPredicate(ab);
	Check(!LineageTracker::IsSubset(acceptance, now_ab), "Bloom acceptance differs from exact source domain");
	Check(tracker.MakeSnapshot(0, Key(0, {1, 0})).lineages == tracker.MakeSnapshot(0, ab).lineages, "key set ordering");
	// Aliases can compact independently: never share mutable L/C slots.
	tracker.RegisterAliasGroup({10, 11});
	tracker.SeedKey(Key(10, {0}));
	tracker.SeedKey(Key(11, {0}));
	Check(tracker.LineageOf(10) != tracker.LineageOf(11), "independent alias state");
	std::cout << "key lineage invariants passed\n";
}
