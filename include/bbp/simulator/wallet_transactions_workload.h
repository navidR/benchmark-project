#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "bbp/simulator/constants.h"
#include "bbp/simulator/wallet_funding_strategy.h"
#include "bbp/simulator/wallet_transfer_strategy.h"

namespace bbp {

enum class ValueDistributionKind {
  kFixed,
  kUniform,
};

constexpr std::string_view ValueDistributionKindName(
    ValueDistributionKind kind) {
  switch (kind) {
    case ValueDistributionKind::kFixed:
      return "fixed";
    case ValueDistributionKind::kUniform:
      return "uniform";
  }
  return "unknown";
}

struct AmountDistribution {
  ValueDistributionKind kind = ValueDistributionKind::kFixed;
  std::uint64_t minimum_satoshis = 0;
  std::uint64_t maximum_satoshis = 0;
};

struct IntervalDistribution {
  ValueDistributionKind kind = ValueDistributionKind::kFixed;
  std::chrono::milliseconds minimum{0};
  std::chrono::milliseconds maximum{0};
};

class WalletTransactionRate {
 public:
  static WalletTransactionRate FromDouble(double value);

  [[nodiscard]] double value() const;
  [[nodiscard]] std::uint64_t millionths() const;
  [[nodiscard]] std::chrono::milliseconds SimulationElapsedBefore(
      std::uint64_t zero_based_transaction_index) const;

 private:
  explicit WalletTransactionRate(std::uint64_t millionths);

  std::uint64_t millionths_ = 1U;
};

struct WalletTransactionsWorkload {
  WalletFundingStrategy funding_strategy = WalletFundingStrategy::kRoundRobin;
  WalletTransferStrategy strategy = WalletTransferStrategy::kRoundRobin;
  std::uint32_t funding_blocks_per_wallet =
      kDefaultCoinbaseSpendableConfirmations;
  std::uint64_t readiness_confirmations =
      kDefaultCoinbaseSpendableConfirmations;
  std::uint64_t funding_threshold_satoshis = 0;
  std::uint32_t transaction_count = 0;
  std::optional<WalletTransactionRate> transaction_rate;
  AmountDistribution amount;
  IntervalDistribution interval;
  std::uint64_t fee_satoshis = 0;
  std::uint64_t random_seed = 0;
  std::vector<std::uint32_t> sender_wallets;
  std::vector<std::uint32_t> receiver_wallets;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
