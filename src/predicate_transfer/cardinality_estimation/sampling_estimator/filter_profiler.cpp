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

double SampleFilterProfiler::EstimateEvaluationNS(const vector<idx_t> &filters, vector<idx_t> *order) const {
	if (order) {
		order->clear();
	}
	const auto rows = SampleSize();
	if (filters.empty() || !rows) {
		return 0;
	}
	const auto n = filters.size();
	if (n == 1) {
		if (order) {
			order->push_back(filters.front());
		}
		return observations_[filters.front()].evaluation_ns;
	}

	// For a prefix S, only rows passing every filter in S reach the next
	// filter. Their number depends on S, independently of its ordering.
	constexpr idx_t max_exact_filters = 16;
	if (n <= max_exact_filters) {
		const idx_t states = idx_t(1) << n;
		const idx_t full = states - 1;
		vector<idx_t> survivors(states, 0);
		// Histogram the observed pass patterns, then sum over supersets.
		for (idx_t row = 0; row < rows; row++) {
			idx_t pattern = 0;
			const auto bit = uint64_t(1) << (row % 64);
			for (idx_t i = 0; i < n; i++) {
				if (observations_[filters[i]].passes[row / 64] & bit) {
					pattern |= idx_t(1) << i;
				}
			}
			survivors[pattern]++;
		}
		for (idx_t bit = 1; bit < states; bit <<= 1) {
			for (idx_t base = 0; base < states; base += 2 * bit) {
				for (idx_t i = base; i < base + bit; i++) {
					survivors[i] += survivors[i + bit];
				}
			}
		}
		vector<double> best(states, 0);
		vector<uint8_t> next(order ? states : 0);
		for (idx_t mask = full; mask-- > 0;) {
			best[mask] = std::numeric_limits<double>::infinity();
			for (idx_t i = 0; i < n; i++) {
				auto bit = idx_t(1) << i;
				if (mask & bit) {
					continue;
				}
				auto cost = observations_[filters[i]].evaluation_ns * double(survivors[mask]) / rows +
				            best[mask | bit];
				if (cost < best[mask]) {
					best[mask] = cost;
					if (order) {
						next[mask] = static_cast<uint8_t>(i);
					}
				}
			}
		}
		if (order) {
			for (idx_t mask = 0; mask != full;) {
				auto i = next[mask];
				order->push_back(filters[i]);
				mask |= idx_t(1) << i;
			}
		}
		return best[0];
	}

	// Bound optimizer work for unusually large conjunctions. Greedily rank
	// each next filter by cost per newly rejected row of the current prefix.
	vector<idx_t> remaining = filters;
	vector<idx_t> prefix;
	double total = 0;
	idx_t input = rows;
	while (!remaining.empty()) {
		idx_t selected = 0, output = input;
		double lowest = std::numeric_limits<double>::infinity();
		prefix.push_back(remaining.front());
		for (idx_t i = 0; i < remaining.size(); i++) {
			prefix.back() = remaining[i];
			auto passed = Count(prefix);
			auto rank = input > passed ? observations_[remaining[i]].evaluation_ns / (input - passed)
			                           : std::numeric_limits<double>::infinity();
			if (!i || rank < lowest) {
				selected = i;
				output = passed;
				lowest = rank;
			}
		}
		prefix.back() = remaining[selected];
		total += observations_[prefix.back()].evaluation_ns * double(input) / rows;
		input = output;
		remaining.erase(remaining.begin() + selected);
	}
	if (order) {
		*order = std::move(prefix);
	}
	return total;
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
	vector<idx_t> order;
	auto cost = EstimateEvaluationNS(filters, &order);
	std::cerr << "  [Bloom-FilterCostOrder] table=" << std::quoted(table) << " sample_ns=" << cost << " ids=";
	for (auto id : order) {
		std::cerr << id << " ";
	}
	std::cerr << '\n';
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
