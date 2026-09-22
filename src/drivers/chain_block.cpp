#include "bbp/drivers/chain_block.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace bbp {
void CalculateBlockTransactionSizes(ChainBlockDetail& detail) {
  detail.transaction_bytes.reset();
  detail.minimum_transaction_size.reset();
  detail.maximum_transaction_size.reset();
  detail.average_transaction_size.reset();
  detail.miner_transaction_size.reset();
  for (const auto& tx : detail.transactions) {
    if (tx.coinbase.value_or(false))
      detail.miner_transaction_size = tx.serialized_size;
  }
  if (detail.transactions.empty() ||
      std::any_of(detail.transactions.begin(), detail.transactions.end(),
                  [](const auto& tx) { return !tx.serialized_size; }))
    return;
  std::uint64_t total = 0;
  std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t maximum = 0;
  for (const auto& tx : detail.transactions) {
    const auto size = *tx.serialized_size;
    if (size > std::numeric_limits<std::uint64_t>::max() - total)
      throw std::runtime_error("block transaction byte count overflow");
    total += size;
    minimum = std::min(minimum, size);
    maximum = std::max(maximum, size);
  }
  detail.transaction_bytes = total;
  detail.minimum_transaction_size = minimum;
  detail.maximum_transaction_size = maximum;
  detail.average_transaction_size =
      static_cast<double>(total) /
      static_cast<double>(detail.transactions.size());
}
}  // namespace bbp
