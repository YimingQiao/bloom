#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! An immutable conjunctive query. Head variables are the visible attributes;
//! body-only variables are existentially quantified. Projection changes the
//! head, never distributes across conjunction or forgets join correlations.
class LineageExpression {
public:
	static shared_ptr<const LineageExpression> Relation(idx_t symbol, idx_t arity);
	shared_ptr<const LineageExpression> Project(const vector<idx_t> &positions) const;
	static shared_ptr<const LineageExpression> Conjoin(const shared_ptr<const LineageExpression> &left,
	                                                   const shared_ptr<const LineageExpression> &right,
	                                                   const vector<idx_t> &left_positions);
	//! premise => condition: map condition's body into premise's body, fixing
	//! corresponding head positions. This is conjunctive-query containment.
	static bool Implies(const LineageExpression &premise, const LineageExpression &condition);

	const string &Signature() const {
		return signature_;
	}
	size_t SignatureHash() const {
		return signature_hash_;
	}
	idx_t Arity() const {
		return head_.size();
	}
	idx_t AtomCount() const {
		return atoms_.size();
	}

private:
	struct Atom {
		idx_t symbol;
		vector<idx_t> variables;
		bool operator==(const Atom &other) const {
			return symbol == other.symbol && variables == other.variables;
		}
		bool operator<(const Atom &other) const {
			return symbol < other.symbol || (symbol == other.symbol && variables < other.variables);
		}
	};
	void Normalize();
	void IndexAtoms();
	void Simplify();
	static bool HasHomomorphism(const LineageExpression &pattern, const LineageExpression &target,
	                            const vector<idx_t> &target_head);
	vector<idx_t> head_;
	vector<Atom> atoms_;
	idx_t variable_count_ = 0;
	string signature_;
	size_t signature_hash_ = 0;
	unordered_map<idx_t, vector<idx_t>> atom_index_;
};

} // namespace duckdb
