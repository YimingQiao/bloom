#include "predicate_transfer/transfer_plan/lineage_expression.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <algorithm>
#include <functional>
#include <numeric>
#include <sstream>

namespace duckdb {

shared_ptr<const LineageExpression> LineageExpression::Relation(idx_t symbol, idx_t arity) {
	auto result = make_shared_ptr<LineageExpression>();
	result->head_.resize(arity);
	std::iota(result->head_.begin(), result->head_.end(), idx_t {0});
	result->atoms_.push_back({symbol, result->head_});
	result->Normalize();
	return result;
}

void LineageExpression::Normalize() {
	// Stable alpha-renaming and syntactic duplicate elimination. Equivalent
	// expressions need not get the same signature; equal signatures are exact.
	std::sort(atoms_.begin(), atoms_.end());
	atoms_.erase(std::unique(atoms_.begin(), atoms_.end()), atoms_.end());
	unordered_map<idx_t, idx_t> names;
	auto rename = [&](idx_t &variable) {
		auto entry = names.emplace(variable, names.size());
		variable = entry.first->second;
	};
	for (auto &variable : head_) {
		rename(variable);
	}
	for (auto &atom : atoms_) {
		for (auto &variable : atom.variables) {
			rename(variable);
		}
	}
	variable_count_ = names.size();
	std::sort(atoms_.begin(), atoms_.end());
	std::ostringstream out;
	out << '(';
	for (auto variable : head_) {
		out << variable << ',';
	}
	out << ')';
	for (auto &atom : atoms_) {
		out << atom.symbol << '(';
		for (auto variable : atom.variables) {
			out << variable << ',';
		}
		out << ')';
	}
	signature_ = out.str();
}

bool LineageExpression::HasHomomorphism(const LineageExpression &pattern, const LineageExpression &target) {
	if (pattern.head_.size() != target.head_.size()) {
		return false;
	}
	vector<idx_t> mapping(pattern.variable_count_, DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < pattern.head_.size(); i++) {
		auto &mapped = mapping[pattern.head_[i]];
		if (mapped != DConstants::INVALID_INDEX && mapped != target.head_[i]) {
			return false;
		}
		mapped = target.head_[i];
	}
	unordered_map<idx_t, vector<idx_t>> candidates;
	for (idx_t i = 0; i < target.atoms_.size(); i++) {
		candidates[target.atoms_[i].symbol].push_back(i);
	}
	vector<idx_t> order(pattern.atoms_.size());
	std::iota(order.begin(), order.end(), idx_t {0});
	for (auto &atom : pattern.atoms_) {
		if (!candidates.count(atom.symbol)) {
			return false;
		}
	}
	// Start with the most selective relation symbol. A homomorphism is not
	// injective: multiple occurrences can legitimately map to one target atom.
	std::stable_sort(order.begin(), order.end(), [&](idx_t a, idx_t b) {
		return candidates.at(pattern.atoms_[a].symbol).size() < candidates.at(pattern.atoms_[b].symbol).size();
	});
	std::function<bool(idx_t)> search = [&](idx_t depth) {
		if (depth == order.size()) {
			return true;
		}
		auto &atom = pattern.atoms_[order[depth]];
		for (auto candidate : candidates.at(atom.symbol)) {
			auto &other = target.atoms_[candidate];
			if (atom.variables.size() != other.variables.size()) {
				continue;
			}
			vector<idx_t> assigned;
			bool compatible = true;
			for (idx_t i = 0; i < atom.variables.size(); i++) {
				const auto variable = atom.variables[i];
				if (mapping[variable] == DConstants::INVALID_INDEX) {
					mapping[variable] = other.variables[i];
					assigned.push_back(variable);
				} else if (mapping[variable] != other.variables[i]) {
					compatible = false;
					break;
				}
			}
			if (compatible && search(depth + 1)) {
				return true;
			}
			for (auto variable : assigned) {
				mapping[variable] = DConstants::INVALID_INDEX;
			}
		}
		return false;
	};
	return search(0);
}

bool LineageExpression::Implies(const LineageExpression &premise, const LineageExpression &condition) {
	if (premise.signature_ == condition.signature_) {
		return true;
	}
	return HasHomomorphism(condition, premise);
}

void LineageExpression::Simplify() {
	Normalize();
	// A uniquely occurring relation symbol cannot be inferred from other
	// symbols. Only repeated occurrences need a containment check for removal.
	for (idx_t i = atoms_.size(); i > 0;) {
		--i;
		const auto symbol = atoms_[i].symbol;
		if (std::count_if(atoms_.begin(), atoms_.end(), [&](const Atom &atom) { return atom.symbol == symbol; }) < 2) {
			continue;
		}
		auto candidate = *this;
		candidate.atoms_.erase(candidate.atoms_.begin() + i);
		// Keep variable numbering while checking; Normalize renumbers afterward.
		if (HasHomomorphism(*this, candidate)) {
			atoms_.erase(atoms_.begin() + i);
		}
	}
	Normalize();
}

shared_ptr<const LineageExpression> LineageExpression::Project(const vector<idx_t> &positions) const {
	auto result = make_shared_ptr<LineageExpression>(*this);
	result->head_.clear();
	for (auto position : positions) {
		D_ASSERT(position < head_.size());
		result->head_.push_back(head_[position]);
	}
	result->Simplify();
	return result;
}

shared_ptr<const LineageExpression> LineageExpression::Conjoin(const shared_ptr<const LineageExpression> &left,
                                                               const shared_ptr<const LineageExpression> &right,
                                                               const vector<idx_t> &left_positions) {
	D_ASSERT(left_positions.size() == right->head_.size());
	// Preserve the old immutable result when the new condition is already true.
	auto projected = left->Project(left_positions);
	if (Implies(*projected, *right)) {
		return left;
	}
	auto result = make_shared_ptr<LineageExpression>(*left);
	vector<idx_t> mapping(right->variable_count_, DConstants::INVALID_INDEX);
	vector<idx_t> parent(left->variable_count_);
	std::iota(parent.begin(), parent.end(), idx_t {0});
	auto root = [&](idx_t variable) {
		while (parent[variable] != variable) {
			variable = parent[variable];
		}
		return variable;
	};
	for (idx_t i = 0; i < left_positions.size(); i++) {
		D_ASSERT(left_positions[i] < left->head_.size());
		const auto destination = left->head_[left_positions[i]];
		auto &mapped = mapping[right->head_[i]];
		if (mapped == DConstants::INVALID_INDEX) {
			mapped = destination;
		} else {
			// A repeated output variable imposes equality on destination columns.
			parent[root(destination)] = root(mapped);
		}
	}
	for (auto &variable : result->head_) {
		variable = root(variable);
	}
	for (auto &atom : result->atoms_) {
		for (auto &variable : atom.variables) {
			variable = root(variable);
		}
	}
	idx_t next_variable = left->variable_count_;
	for (auto &variable : mapping) {
		variable = variable == DConstants::INVALID_INDEX ? next_variable++ : root(variable);
	}
	for (auto &atom : right->atoms_) {
		Atom copy {atom.symbol, {}};
		for (auto variable : atom.variables) {
			copy.variables.push_back(mapping[variable]);
		}
		result->atoms_.push_back(std::move(copy));
	}
	// Freshly renamed existential variables keep different projection witnesses
	// separate; e.g. pi_a(S_ab) AND pi_b(S_ab) does not become S_ab.
	result->Simplify();
	return result;
}

} // namespace duckdb
