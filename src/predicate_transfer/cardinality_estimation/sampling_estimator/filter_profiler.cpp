#include "predicate_transfer/cardinality_estimation/sampling_estimator/filter_profiler.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include <chrono>
#include <algorithm>
#include <limits>
#include <iomanip>
#include <iostream>

namespace duckdb {

SampleFilterProfiler::SampleFilterProfiler(ClientContext &context, const ColumnDataCollection &sample,
                                           vector<column_t> columns)
    : context_(context), sample_(sample), columns_(std::move(columns)) {
}

void SampleFilterProfiler::AddLocal(const Expression &expression) {
	if (expression.CanThrow() || expression.IsVolatile()) {
		throw InvalidInputException("Cannot independently profile a throwing or volatile predicate");
	}
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
			AddLocal(*child);
		}
		return;
	}
	for (idx_t i = 0; i < observations_.size(); i++) {
		if (observations_[i].expression && observations_[i].expression->Equals(expression)) {
			local_filters_.push_back(i);
			return;
		}
	}
	local_filters_.push_back(Evaluate(&expression, nullptr));
}

idx_t SampleFilterProfiler::AddRuntime(const ScannerFilterSet::Entry &filter) {
	D_ASSERT(filter.filter && filter.filter->IsValid());
	for (idx_t i = 0; i < observations_.size(); i++) {
		auto &observation = observations_[i];
		if (observation.columns == filter.chunk_cols && observation.runtime_filter.lock() == filter.filter) {
			return i;
		}
	}
	return Evaluate(nullptr, &filter);
}

idx_t SampleFilterProfiler::Evaluate(const Expression *expression, const ScannerFilterSet::Entry *filter) {
	D_ASSERT(bool(expression) != bool(filter));
	Observation observation;
	observation.description = expression ? expression->ToString() : filter->filter->ToString();
	observation.passes.resize((SampleSize() + 63) / 64, 0);
	unique_ptr<ExpressionExecutor> executor;
	if (expression) {
		observation.expression = expression->Copy();
		executor = make_uniq<ExpressionExecutor>(context_, *expression);
	} else {
		observation.runtime_filter = filter->filter;
		observation.columns = filter->chunk_cols;
	}

	vector<LogicalType> types;
	for (auto column : columns_) {
		types.push_back(sample_.Types()[column]);
	}
	ColumnDataScanState scan;
	sample_.InitializeScan(scan, columns_);
	DataChunk chunk;
	chunk.Initialize(BufferAllocator::Get(context_), types);
	SelectionVector selected(STANDARD_VECTOR_SIZE);
	idx_t offset = 0;
	while (true) {
		chunk.Reset();
		if (!sample_.Scan(scan, chunk) || !chunk.size()) {
			break;
		}
		size_t count = chunk.size();
		auto started = std::chrono::steady_clock::now();
		if (executor) {
			count = executor->SelectExpression(chunk, selected);
		} else {
			filter->filter->Lookup(chunk, filter->chunk_cols, selected, count);
		}
		observation.evaluation_ns +=
		    std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - started).count();
		for (idx_t i = 0; i < count; i++) {
			auto row = offset + (count == chunk.size() ? i : selected.get_index(i));
			D_ASSERT(row < SampleSize());
			observation.passes[row / 64] |= uint64_t(1) << (row % 64);
		}
		observation.passed += count;
		offset += chunk.size();
	}
	D_ASSERT(offset == SampleSize());
	observations_.push_back(std::move(observation));
	return observations_.size() - 1;
}

double SampleFilterProfiler::EstimateLocalEvaluationNS(const vector<idx_t> &filters) const {
	if (!SampleSize()) {
		return 0;
	}
	vector<idx_t> preceding, local;
	for (auto id : filters) {
		D_ASSERT(id < observations_.size());
		(observations_[id].expression ? local : preceding).push_back(id);
	}
	if (local.empty()) {
		return 0;
	}
	// Model cheap received filters before local predicates. Approximate the
	// native adaptive local order; never execute predicates to obtain this cost.
	auto rank = [&](idx_t id) {
		auto &observation = observations_[id];
		auto rejected = SampleSize() - observation.passed;
		return rejected ? observation.evaluation_ns / rejected : std::numeric_limits<double>::infinity();
	};
	std::stable_sort(local.begin(), local.end(), [&](idx_t a, idx_t b) { return rank(a) < rank(b); });
	// Accumulate each prefix once instead of recomputing its full conjunction
	// for every local predicate. Only integer counts are accumulated here.
	vector<idx_t> input_rows(local.size(), 0);
	for (idx_t offset = 0; offset < SampleSize(); offset += ValidityMask::BITS_PER_VALUE) {
		auto word = offset / ValidityMask::BITS_PER_VALUE;
		auto width = MinValue<idx_t>(ValidityMask::BITS_PER_VALUE, SampleSize() - offset);
		uint64_t bits = ValidityMask::EntryWithValidBits(width);
		for (auto id : preceding) {
			bits &= observations_[id].passes[word];
		}
		for (idx_t i = 0; i < local.size() && bits; i++) {
			input_rows[i] += ValidityMask(&bits, width).CountValid(width);
			bits &= observations_[local[i]].passes[word];
		}
	}
	double elapsed = 0;
	for (idx_t i = 0; i < local.size(); i++) {
		elapsed += observations_[local[i]].evaluation_ns * double(input_rows[i]) / double(SampleSize());
	}
	return elapsed;
}

idx_t SampleFilterProfiler::Count(const vector<idx_t> &filters) const {
	if (filters.empty()) {
		return SampleSize();
	}
	if (filters.size() == 1) {
		D_ASSERT(filters[0] < observations_.size());
		return observations_[filters[0]].passed;
	}
	idx_t count = 0;
	for (idx_t word = 0; word < (SampleSize() + 63) / 64; word++) {
		uint64_t bits = ~uint64_t(0);
		for (auto id : filters) {
			D_ASSERT(id < observations_.size());
			bits &= observations_[id].passes[word];
		}
		count += ValidityMask(&bits, ValidityMask::BITS_PER_VALUE).CountValid(ValidityMask::BITS_PER_VALUE);
	}
	return count;
}

void SampleFilterProfiler::Log(const string &table, const vector<idx_t> &filters) const {
	auto joint = Count(filters);
	std::cerr << "  [Bloom-FilterProfile] table=" << std::quoted(table) << " rows=" << SampleSize()
	          << " filters=" << filters.size() << " joint_pass=" << joint << '\n';
	for (idx_t i = 0; i < filters.size(); i++) {
		vector<idx_t> others;
		for (idx_t j = 0; j < filters.size(); j++) {
			if (j != i) {
				others.push_back(filters[j]);
			}
		}
		auto input_after_others = Count(others);
		auto &observation = observations_[filters[i]];
		std::cerr << "    [Bloom-FilterProfileItem] id=" << filters[i]
		          << " kind=" << (observation.columns.empty() ? "local" : "transfer") << " pass=" << observation.passed
		          << " input_after_others=" << input_after_others << " additional_reject=" << input_after_others - joint
		          << " eval_us=" << observation.evaluation_ns / 1000.0
		          << " ns_per_row=" << (SampleSize() ? observation.evaluation_ns / SampleSize() : 0)
		          << " expression=" << std::quoted(observation.description) << '\n';
	}
}

idx_t SampleFilterProfiler::MemoryUsage() const {
	idx_t bytes = sizeof(*this) + columns_.capacity() * sizeof(column_t) + local_filters_.capacity() * sizeof(idx_t);
	bytes += observations_.capacity() * sizeof(Observation);
	for (auto &observation : observations_) {
		bytes += observation.description.capacity() + observation.passes.capacity() * sizeof(uint64_t) +
		         observation.columns.capacity() * sizeof(idx_t);
	}
	return bytes;
}

} // namespace duckdb
