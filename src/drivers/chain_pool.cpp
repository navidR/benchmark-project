#include "bbp/drivers/chain_pool.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace bbp {
namespace {
void Add(std::uint64_t& sum, std::uint64_t value) {
  if (value > std::numeric_limits<std::uint64_t>::max() - sum)
    throw std::runtime_error("pool aggregate overflow");
  sum += value;
}
ChainPoolSizeStatistics Statistics(
    const std::vector<ChainPoolTransaction>& entries,
    std::optional<std::uint64_t> ChainPoolTransaction::* member) {
  ChainPoolSizeStatistics result;
  std::uint64_t total = 0, minimum = UINT64_MAX, maximum = 0;
  for (const auto& entry : entries) {
    const auto value = entry.*member;
    if (!value) return {};
    Add(total, *value);
    minimum = std::min(minimum, *value);
    maximum = std::max(maximum, *value);
  }
  result.total = total;
  if (!entries.empty()) {
    result.minimum = minimum;
    result.maximum = maximum;
    result.average =
        static_cast<double>(total) / static_cast<double>(entries.size());
  }
  return result;
}
}  // namespace

void CalculatePoolSummary(ChainPoolSnapshot& snapshot) {
  if (snapshot.transactions.size() > ChainPoolSnapshot::kMaximumTransactions)
    throw std::runtime_error("pool exceeds 65536 transaction display bound");
  auto& summary = snapshot.summary;
  const auto& entries = snapshot.transactions;
  summary.transaction_count = entries.size();
  summary.size = Statistics(entries, &ChainPoolTransaction::serialized_size);
  summary.weight = Statistics(entries, &ChainPoolTransaction::weight);
  summary.total_fees = Statistics(entries, &ChainPoolTransaction::fee).total;
  summary.oldest_first_seen.reset();
  summary.minimum_fee_rate.reset();
  summary.maximum_fee_rate.reset();
  summary.average_fee_rate.reset();
  if (!entries.empty() &&
      std::all_of(entries.begin(), entries.end(),
                  [](const auto& e) { return e.first_seen.has_value(); })) {
    summary.oldest_first_seen =
        (*std::min_element(entries.begin(), entries.end(),
                           [](const auto& a, const auto& b) {
                             return a.first_seen < b.first_seen;
                           }))
            .first_seen;
  }
  if (!entries.empty() &&
      std::all_of(entries.begin(), entries.end(),
                  [](const auto& e) { return e.fee_rate.has_value(); })) {
    double total = 0, minimum = std::numeric_limits<double>::max(), maximum = 0;
    for (const auto& entry : entries) {
      total += *entry.fee_rate;
      minimum = std::min(minimum, *entry.fee_rate);
      maximum = std::max(maximum, *entry.fee_rate);
    }
    summary.minimum_fee_rate = minimum;
    summary.maximum_fee_rate = maximum;
    summary.average_fee_rate = total / static_cast<double>(entries.size());
  }
}
}  // namespace bbp
