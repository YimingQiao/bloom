#include "predicate_transfer/transfer_plan/excitation_graph/equality_domain_tracker.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/unordered_set.hpp"

#include <algorithm>

namespace duckdb {

optional_idx EqualityDomainTracker::FindDomain(const ColumnBinding &binding) const {
	auto entry = binding_domains_.find(binding);
	if (entry == binding_domains_.end()) {
		return optional_idx();
	}
	return entry->second;
}

void EqualityDomainTracker::Reset(const BindingGroupMap &groups) {
	binding_domains_.clear();
	domains_.clear();

	unordered_map<const JoinKeyTableGroup *, idx_t> group_indices;
	vector<pair<const JoinKeyTableGroup *, vector<ColumnBinding>>> grouped_members;
	for (auto &entry : groups) {
		if (!entry.second) {
			continue;
		}
		auto *group = entry.second.get();
		auto inserted = group_indices.emplace(group, grouped_members.size());
		if (inserted.second) {
			grouped_members.emplace_back(group, vector<ColumnBinding> {});
		}
		grouped_members[inserted.first->second].second.push_back(entry.first);
	}

	auto binding_less = [](const ColumnBinding &left, const ColumnBinding &right) {
		return left.table_index.index == right.table_index.index ? left.column_index < right.column_index
		                                                         : left.table_index.index < right.table_index.index;
	};
	for (auto &entry : grouped_members) {
		std::sort(entry.second.begin(), entry.second.end(), binding_less);
	}
	std::sort(grouped_members.begin(), grouped_members.end(), [&](const auto &left, const auto &right) {
		D_ASSERT(!left.second.empty());
		D_ASSERT(!right.second.empty());
		return binding_less(left.second.front(), right.second.front());
	});

	for (auto &entry : grouped_members) {
		auto &group = *entry.first;
		auto &members = entry.second;
		if (members.size() < 2 || !group.return_type.IsIntegral() || group.return_type == LogicalType::UBIGINT ||
		    group.return_type == LogicalType::HUGEINT || group.return_type == LogicalType::UHUGEINT) {
			continue;
		}

		unordered_set<idx_t> relations;
		bool unambiguous = true;
		for (auto &binding : members) {
			if (!relations.insert(binding.table_index.index).second) {
				unambiguous = false;
				break;
			}
		}
		if (!unambiguous) {
			continue;
		}

		auto domain_id = domains_.size();
		domains_.emplace_back();
		for (auto &binding : members) {
			binding_domains_.emplace(binding, domain_id);
		}
	}
}

bool EqualityDomainTracker::Tracks(const ColumnBinding &source, const ColumnBinding &destination) const {
	auto source_domain = FindDomain(source);
	auto destination_domain = FindDomain(destination);
	return source_domain.IsValid() && destination_domain.IsValid() &&
	       source_domain.GetIndex() == destination_domain.GetIndex();
}

EqualityDomainTracker::Observation EqualityDomainTracker::Observe(const ColumnBinding &source, idx_t exact_ndv) {
	auto domain_id = FindDomain(source);
	if (!domain_id.IsValid()) {
		return {};
	}

	auto &domain = domains_[domain_id.GetIndex()];
	Observation result;
	result.domain_id = domain_id.GetIndex();
	result.previous_ndv = domain.ndv;
	result.current_ndv = exact_ndv;

	if (domain.version == 0) {
		domain.ndv = exact_ndv;
		domain.version = 1;
		domain.applied_versions[source.table_index.index] = domain.version;
		result.version = domain.version;
		result.kind = ObservationKind::INITIALIZED;
		return result;
	}

	auto applied = domain.applied_versions.find(source.table_index.index);
	if (applied == domain.applied_versions.end() || applied->second != domain.version) {
		result.kind = ObservationKind::UNCONTAINED;
		return result;
	}

	D_ASSERT(exact_ndv <= domain.ndv);
	if (exact_ndv > domain.ndv) {
		result.kind = ObservationKind::UNCONTAINED;
		return result;
	}
	if (exact_ndv < domain.ndv) {
		domain.ndv = exact_ndv;
		domain.version++;
		domain.applied_versions[source.table_index.index] = domain.version;
		result.kind = ObservationKind::SHRUNK;
	} else {
		result.kind = ObservationKind::UNCHANGED;
	}
	result.version = domain.version;
	return result;
}

bool EqualityDomainTracker::Suppresses(const ColumnBinding &source, const ColumnBinding &destination,
                                       const Observation &observation) const {
	if (!observation.IsContained()) {
		return false;
	}
	auto source_domain = FindDomain(source);
	auto destination_domain = FindDomain(destination);
	if (!source_domain.IsValid() || !destination_domain.IsValid() ||
	    source_domain.GetIndex() != observation.domain_id || destination_domain.GetIndex() != observation.domain_id) {
		return false;
	}
	auto &domain = domains_[observation.domain_id];
	if (domain.version != observation.version) {
		return false;
	}
	auto applied = domain.applied_versions.find(destination.table_index.index);
	return applied != domain.applied_versions.end() && applied->second >= observation.version;
}

void EqualityDomainTracker::MarkApplied(const ColumnBinding &destination, const Observation &observation) {
	if (!observation.IsContained()) {
		return;
	}
	auto domain_id = FindDomain(destination);
	if (!domain_id.IsValid() || domain_id.GetIndex() != observation.domain_id) {
		return;
	}
	auto &domain = domains_[observation.domain_id];
	if (domain.version == observation.version) {
		domain.applied_versions[destination.table_index.index] = observation.version;
	}
}

} // namespace duckdb
