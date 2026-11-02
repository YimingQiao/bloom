#include "predicate_transfer/transfer_plan/lineage_tracker.hpp"
#include "predicate_transfer/transfer_plan/transfer_types.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include <iostream>
#include <random>
#include <set>
#include <stdexcept>
using namespace duckdb;
using E = shared_ptr<const LineageExpression>;
static size_t checks = 0;
static void Check(bool condition, const char *message) {
	++checks;
	if (!condition) {
		throw std::runtime_error(message);
	}
}
static bool Implies(const E &a, const E &b) {
	return LineageExpression::Implies(*a, *b);
}
static void Equivalent(const E &a, const E &b) {
	Check(Implies(a, b) && Implies(b, a), "expected equivalent expressions");
}
static vector<ColumnBinding> Key(idx_t table, std::initializer_list<idx_t> cols) {
	vector<ColumnBinding> result;
	for (auto col : cols) {
		result.emplace_back(TableIndex(table), ProjectionIndex(col));
	}
	return result;
}
static bool Eligible(LineageTracker &tracker, const vector<ColumnBinding> &source, const vector<ColumnBinding> &dest) {
	LogicalDummyScan src(source.front().table_index), dst(dest.front().table_index);
	EdgeInfo edge(vector<LogicalType>(source.size(), LogicalType::INTEGER), src, source, dst, dest);
	return tracker.EdgeCarriesNewInfo(edge);
}
static void Algebra() {
	auto s = LineageExpression::Relation(1, 2);
	auto r = LineageExpression::Relation(2, 2);
	auto sa = s->Project({0}), sb = s->Project({1});
	auto separate = LineageExpression::Conjoin(r, sa, {0});
	separate = LineageExpression::Conjoin(separate, sb, {1});
	auto joint = LineageExpression::Conjoin(r, s, {0, 1});
	Check(Implies(joint, separate), "a composite condition implies its marginal conditions");
	Check(!Implies(separate, joint), "independent witnesses must not imply a composite key");
	Equivalent(LineageExpression::Conjoin(joint, s, {0, 1}), joint);
	Check(joint->AtomCount() == 2, "redundant projected atoms should simplify");
	auto distributed = LineageExpression::Conjoin(r->Project({0}), sa, {0});
	Check(!Implies(distributed, joint->Project({0})), "projection must not distribute through conjunction");
	Check(Implies(joint->Project({0}), distributed), "whole projection implies the marginal conjunction");
	Equivalent(s->Project({1, 0})->Project({1, 0}), s);
	Check(!Implies(s, s->Project({1, 0})), "column order is semantic");
	auto diagonal = sa->Project({0, 0});
	auto equal = LineageExpression::Conjoin(r, diagonal, {0, 1});
	Check(Implies(equal, diagonal), "repeated output variables impose destination equality");
	Check(!Implies(joint, diagonal), "an ordinary composite key does not impose equality");
	Equivalent(LineageExpression::Conjoin(equal, joint, {0, 1}), LineageExpression::Conjoin(joint, equal, {0, 1}));
	Equivalent(LineageExpression::Conjoin(s, s, {1, 0}), LineageExpression::Conjoin(s->Project({1, 0}), s, {0, 1}));
}
static void PaperExample(bool separated) {
	LineageTracker tracker;
	auto rab = Key(0, {0, 1}), rb = Key(0, {1});
	auto sab = Key(1, {0, 1}), sbc = Key(1, {1, 2}), sabc = Key(1, {0, 1, 2});
	auto tb = Key(2, {1}), tbc = Key(2, {1, 2});
	for (auto key : {rab, rb, sab, sbc, tb, tbc}) {
		tracker.SeedKey(key);
	}
	auto initial_s = tracker.MakeSnapshot(sabc);
	// Step 1: S -> R and T. All keys belong to a single table expression.
	auto s_to_r = tracker.MakeSnapshot(sab), s_to_t = tracker.MakeSnapshot(sbc);
	tracker.ReceiveFilters(0, {{rab, s_to_r}}, true);
	tracker.ReceiveFilters(2, {{tbc, s_to_t}}, true);
	Check(Eligible(tracker, rab, sab), "S has not yet received R's original ab constraint");
	Check(Eligible(tracker, tbc, sbc), "T can still reduce S on the full bc key");
	// Step 2: T -> R and S, freezing both outgoing projections first.
	auto t_to_r = tracker.MakeSnapshot(tb), t_to_s = tracker.MakeSnapshot(tbc);
	tracker.ReceiveFilters(0, {{rb, t_to_r}}, true);
	tracker.ReceiveFilters(1, {{sbc, t_to_s}}, true);
	Check(Eligible(tracker, rab, sab), "R now has an informative composite projection");
	// Step 3: R -> S and T. T's rowset is unchanged in the paper example.
	auto r_to_s = tracker.MakeSnapshot(rab), r_to_t = tracker.MakeSnapshot(rb);
	tracker.ReceiveFilters(1, {{sab, r_to_s}}, true);
	tracker.ReceiveFilters(2, {{tb, r_to_t}}, !separated);
	Check(!Eligible(tracker, tbc, sbc), "whole-expression implication proves T -> S redundant");
	Check(Eligible(tracker, sbc, tbc), "S can still reduce T on bc");
	// Final S broadcast reaches full reduction / ready on every edge.
	s_to_r = tracker.MakeSnapshot(sab);
	s_to_t = tracker.MakeSnapshot(sbc);
	tracker.ReceiveFilters(0, {{rab, s_to_r}}, !separated);
	tracker.ReceiveFilters(2, {{tbc, s_to_t}}, true);
	for (auto edge : vector<pair<vector<ColumnBinding>, vector<ColumnBinding>>> {
	         {rab, sab}, {sab, rab}, {rb, tb}, {tb, rb}, {sbc, tbc}, {tbc, sbc}}) {
		Check(!Eligible(tracker, edge.first, edge.second), "terminal state must cover both directions on every edge");
	}
	Check(initial_s.expression->AtomCount() == 1, "old snapshots must remain immutable");
}
static void Separated() {
	LineageTracker tracker;
	auto ra = Key(0, {0}), sa = Key(1, {0}), sb = Key(1, {1}), sab = Key(1, {0, 1}), tb = Key(2, {1});
	for (auto key : {ra, sa, sb, tb}) {
		tracker.SeedKey(key);
	}
	auto initial_s = tracker.MakeSnapshot(sab);
	tracker.ReceiveFilters(0, {{ra, tracker.MakeSnapshot(sa)}}, true);
	tracker.ReceiveFilters(2, {{tb, tracker.MakeSnapshot(sb)}}, true);
	tracker.ReceiveFilters(1, {{sa, tracker.MakeSnapshot(ra)}}, false);
	Check(tracker.MakeSnapshot(sab).expression == initial_s.expression,
	      "no-op preserves the immutable result projection");
	Check(!Eligible(tracker, sb, tb), "no-op filter on a must not retrigger outgoing b");
	Check(Eligible(tracker, tb, sb), "T remains able to reduce S");
	Check(!Eligible(tracker, ra, sa), "C remembers the satisfied R condition");
	tracker.ReceiveFilters(1, {{sb, tracker.MakeSnapshot(tb)}}, true);
	Check(!LineageTracker::Implies(tracker.MakeSnapshot(sa), tracker.MakeSnapshot(ra)),
	      "C must not leak into L on later shrink");
	Check(!Eligible(tracker, ra, sa), "old satisfied constraints survive a later shrink");
	const auto bloom = tracker.NewPredicate(ra);
	Check(!LineageTracker::Implies(bloom, tracker.MakeSnapshot(ra)),
	      "Bloom acceptance does not prove exact source membership");
	tracker.SeedKey(Key(10, {0}));
	tracker.SeedKey(Key(11, {0}));
	Check(!LineageTracker::Implies(tracker.MakeSnapshot(Key(10, {0})), tracker.MakeSnapshot(Key(11, {0}))),
	      "different aliases have distinct relation symbols");
	tracker.Clear();
	tracker.SeedKey(ra);
	Check(tracker.MakeSnapshot(ra).expression->AtomCount() == 1, "reset clears query-local expressions");
}
static void CachedImplications() {
	LineageTracker tracker;
	auto a = Key(0, {0}), b = Key(1, {0});
	tracker.SeedKey(a);
	tracker.SeedKey(b);
	auto before = tracker.MakeSnapshot(a);
	auto filter = tracker.MakeSnapshot(b);
	Check(tracker.MakeConstraintSnapshot(a).expression == before.expression, "equal L and C share projected snapshots");
	Check(!tracker.ImpliesCached(before, filter), "cache remembers a false implication");
	tracker.ReceiveFilters(0, {{a, filter}}, true);
	auto after = tracker.MakeSnapshot(a);
	Check(tracker.MakeConstraintSnapshot(a).expression == after.expression, "equal L and C remain shared after shrink");
	Check(tracker.ImpliesCached(after, filter), "updated lineage does not reuse a stale negative result");
	Check(!tracker.ImpliesCached(before, filter), "old snapshots retain their original implication");
	for (idx_t i = 0; i < 600; i++) {
		auto predicate = tracker.NewPredicate(a);
		Check(!tracker.ImpliesCached(after, predicate), "fresh predicates must not alias evicted cache keys");
		Check(tracker.ImpliesCached(after, filter), "eviction preserves true implications");
	}
	tracker.Clear();
	tracker.SeedKey(a);
	Check(!tracker.ImpliesCached(tracker.MakeSnapshot(a), filter), "reset clears memoized coverage");
}
// Independent relational evaluation over ALL 256 instances of two binary
// relations on domain {0,1}. Exercise expression simplification and containment
// against concrete projection and semijoin, not another homomorphism solver.
using Rows = std::set<std::vector<int>>;
struct Model {
	E expression;
	std::vector<Rows> rows;
};
static void ExhaustiveFiniteModels() {
	std::vector<Model> models;
	for (int relation = 0; relation < 2; relation++) {
		Model m {LineageExpression::Relation(relation, 2), {}};
		for (int db = 0; db < 256; db++) {
			Rows rows;
			int mask = (db >> (4 * relation)) & 15;
			for (int value = 0; value < 4; value++) {
				if (mask & (1 << value)) {
					rows.insert({value / 2, value % 2});
				}
			}
			m.rows.push_back(rows);
		}
		models.push_back(std::move(m));
	}
	std::mt19937 rng(42);
	for (int round = 0; round < 70; round++) {
		auto &a = models[rng() % models.size()];
		Model m;
		if (round % 3 == 0) {
			vector<idx_t> positions = {rng() % a.expression->Arity()};
			if (round % 2) {
				positions.push_back(rng() % a.expression->Arity());
			}
			m.expression = a.expression->Project(positions);
			for (auto &rows : a.rows) {
				Rows out;
				for (auto &row : rows) {
					std::vector<int> projected;
					for (auto p : positions) {
						projected.push_back(row[p]);
					}
					out.insert(projected);
				}
				m.rows.push_back(out);
			}
		} else {
			auto &b = models[rng() % models.size()];
			vector<idx_t> positions;
			for (idx_t i = 0; i < b.expression->Arity(); i++) {
				positions.push_back(rng() % a.expression->Arity());
			}
			m.expression = LineageExpression::Conjoin(a.expression, b.expression, positions);
			for (idx_t db = 0; db < 256; db++) {
				Rows out;
				for (auto &row : a.rows[db]) {
					std::vector<int> projected;
					for (auto p : positions) {
						projected.push_back(row[p]);
					}
					if (b.rows[db].count(projected)) {
						out.insert(row);
					}
				}
				m.rows.push_back(out);
			}
		}
		models.push_back(std::move(m));
	}
	for (auto &a : models) {
		for (auto &b : models) {
			if (a.expression->Arity() != b.expression->Arity() || !Implies(a.expression, b.expression)) {
				continue;
			}
			for (idx_t db = 0; db < 256; db++) {
				Check(std::includes(b.rows[db].begin(), b.rows[db].end(), a.rows[db].begin(), a.rows[db].end()),
				      "proven containment must hold on every finite model");
			}
		}
	}
}
int main() {
	Algebra();
	PaperExample(false);
	PaperExample(true);
	Separated();
	CachedImplications();
	ExhaustiveFiniteModels();
	std::cout << "paper lineage: " << checks << " checks passed\n";
}
