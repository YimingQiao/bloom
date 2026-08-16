#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "predicate_transfer/transfer_plan/transfer_types.hpp"

namespace duckdb {

//! Tracks exact NDV versions for unambiguous single-column integral equality
//! classes. A domain size identifies equality only after a relation has consumed
//! the current version: subsequent materializations of that relation are
//! monotone subsets, so an unchanged exact NDV means an unchanged key domain.
class EqualityDomainTracker {
public:
	enum class ObservationKind : uint8_t { UNTRACKED, INITIALIZED, SHRUNK, UNCHANGED, UNCONTAINED };

	struct Observation {
		idx_t domain_id = DConstants::INVALID_INDEX;
		idx_t version = 0;
		idx_t previous_ndv = 0;
		idx_t current_ndv = 0;
		ObservationKind kind = ObservationKind::UNTRACKED;

		bool IsContained() const {
			return version != 0;
		}
	};

	void Reset(const BindingGroupMap &groups);
	bool Tracks(const ColumnBinding &source, const ColumnBinding &destination) const;
	Observation Observe(const ColumnBinding &source, idx_t exact_ndv);
	bool Suppresses(const ColumnBinding &source, const ColumnBinding &destination,
	                const Observation &observation) const;
	void MarkApplied(const ColumnBinding &destination, const Observation &observation);

	idx_t TrackedDomainCount() const {
		return domains_.size();
	}

private:
	struct Domain {
		idx_t ndv = 0;
		idx_t version = 0;
		unordered_map<idx_t, idx_t> applied_versions;
	};

	optional_idx FindDomain(const ColumnBinding &binding) const;

	unordered_map<ColumnBinding, idx_t, ColumnBindingHashFunc> binding_domains_;
	vector<Domain> domains_;
};

} // namespace duckdb
