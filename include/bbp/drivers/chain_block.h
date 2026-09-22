#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace bbp {

// Sizes are bytes, weights retain the native chain's units. Absent values
// are unknown/inapplicable, never zero estimates. See CHAIN_VIEW.md.
struct ChainBlockSummary {
  std::uint64_t height = 0;
  std::string hash;
  std::optional<std::string> previous_hash;
  std::optional<std::string> next_hash;
  std::optional<std::uint64_t> timestamp;
  std::optional<std::int64_t> confirmations;
  std::optional<std::uint64_t> serialized_size;
  std::optional<std::uint64_t> weight;
  std::optional<std::uint64_t> header_size;
  std::optional<std::uint64_t> transaction_count;
};

struct ChainBlockTransaction {
  std::string id;
  std::optional<std::uint64_t> serialized_size;
  std::optional<std::uint64_t> weight;
  std::optional<std::uint64_t> metadata_size;
  std::optional<std::string> fee;
  std::optional<std::uint64_t> input_count;
  std::optional<std::uint64_t> output_count;
  std::optional<bool> coinbase;
  std::string error;
};

struct ChainBlockDetail {
  ChainBlockSummary block;
  std::vector<ChainBlockTransaction> transactions;
  std::optional<std::uint64_t> transaction_bytes;
  std::optional<std::uint64_t> minimum_transaction_size;
  std::optional<std::uint64_t> maximum_transaction_size;
  std::optional<double> average_transaction_size;
  std::optional<std::uint64_t> miner_transaction_size;
  std::optional<std::uint64_t> metadata_bytes;
  std::string byte_definition;
};

// Populate aggregates only when every transaction size is known.
void CalculateBlockTransactionSizes(ChainBlockDetail& detail);

}  // namespace bbp
