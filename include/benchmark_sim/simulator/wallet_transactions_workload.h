#pragma once

#include <cstdint>

#include "benchmark_sim/simulator/constants.h"
#include "benchmark_sim/simulator/wallet_transfer_strategy.h"

namespace bsim {

struct WalletTransactionsWorkload {
  WalletTransferStrategy strategy = WalletTransferStrategy::kRoundRobin;
  std::uint32_t funding_blocks_per_wallet = kFiroCoinbaseSpendableConfirmations;
  std::uint64_t readiness_confirmations = kFiroCoinbaseSpendableConfirmations;
  std::uint32_t transaction_count = 0;
  std::uint64_t amount_satoshis = 0;
  std::uint64_t fee_satoshis = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bsim
