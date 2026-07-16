#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "bbp/simulator/wallet_transactions_workload.h"

namespace bbp {

struct WalletTransactionPlanEntry {
  std::size_t sender_index = 0;
  std::size_t receiver_index = 0;
  std::uint64_t amount_satoshis = 0;
  std::chrono::milliseconds interval_before{0};

  bool operator==(const WalletTransactionPlanEntry&) const = default;
};

std::vector<WalletTransactionPlanEntry> BuildWalletTransactionPlan(
    std::size_t wallet_count, const WalletTransactionsWorkload& workload);

}  // namespace bbp
