#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace bbp {

// All absent values mean unknown/inapplicable. Fee integers use the snapshot's
// explicitly named atomic unit; rate denominators are driver-defined.
struct ChainPoolTransaction {
  std::string id;
  std::optional<std::uint64_t> first_seen;
  std::optional<std::uint64_t> serialized_size, weight, metadata_size;
  std::optional<std::uint64_t> fee;
  std::optional<double> fee_rate;
  std::optional<std::uint64_t> input_count, output_count;
  std::optional<std::vector<std::string>> dependencies;
  std::optional<std::uint64_t> ancestor_count, ancestor_size;
  std::optional<std::uint64_t> descendant_count, descendant_size;
  std::optional<bool> replaceable, relayed, do_not_relay;
  std::optional<std::string> validation;
};

struct ChainPoolSizeStatistics {
  std::optional<std::uint64_t> total, minimum, maximum;
  std::optional<double> average;
};

struct ChainPoolSummary {
  std::uint64_t transaction_count = 0;
  ChainPoolSizeStatistics size, weight;
  std::optional<std::uint64_t> total_fees, oldest_first_seen, memory_usage;
  std::optional<double> minimum_fee_rate, maximum_fee_rate, average_fee_rate;
  std::string fee_unit, fee_rate_unit, ancestor_size_unit, byte_definition;
};

struct ChainPoolSnapshot {
  static constexpr std::size_t kMaximumTransactions = 65536;
  ChainPoolSummary summary;
  std::vector<ChainPoolTransaction> transactions;
};

// Aggregate only complete known columns from a single driver pool sample.
void CalculatePoolSummary(ChainPoolSnapshot& snapshot);

}  // namespace bbp
