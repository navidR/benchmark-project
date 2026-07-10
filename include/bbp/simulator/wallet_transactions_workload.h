#pragma once

#include <cstdint>

#include "bbp/simulator/constants.h"
#include "bbp/simulator/wallet_transfer_strategy.h"

namespace bbp {

struct WalletTransactionsWorkload {
  WalletTransferStrategy strategy = WalletTransferStrategy::kRoundRobin;
  std::uint32_t funding_blocks_per_wallet =
      kDefaultCoinbaseSpendableConfirmations;
  std::uint64_t readiness_confirmations =
      kDefaultCoinbaseSpendableConfirmations;
  std::uint32_t transaction_count = 0;
  std::uint64_t amount_satoshis = 0;
  std::uint64_t fee_satoshis = 0;
  std::uint64_t random_seed = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
