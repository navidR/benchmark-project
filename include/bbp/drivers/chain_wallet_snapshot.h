#pragma once

#include <cstdint>
#include <vector>

#include "bbp/drivers/chain_wallet_transaction.h"

namespace bbp {

struct ChainWalletSnapshot {
  std::uint64_t available_balance_satoshis = 0;
  std::uint64_t unconfirmed_balance_satoshis = 0;
  std::uint64_t immature_balance_satoshis = 0;
  std::uint64_t transaction_count = 0;
  bool transaction_history_truncated = false;
  std::vector<ChainWalletTransaction> transactions;
};

}  // namespace bbp
