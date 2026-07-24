#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <random>
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

class WalletTransactionPlanner {
 public:
  WalletTransactionPlanner(std::size_t wallet_count,
                           const WalletTransactionsWorkload& workload,
                           std::uint64_t sequence_offset = 0U);

  WalletTransactionPlanEntry Next();

 private:
  std::size_t wallet_count_ = 0;
  WalletTransactionsWorkload workload_;
  std::vector<std::size_t> senders_;
  std::vector<std::size_t> receivers_;
  std::vector<std::size_t> shuffled_wallets_;
  std::mt19937_64 rng_;
  std::uint64_t transaction_index_ = 0;
};

class WalletTransactionLoadPlanner {
 public:
  WalletTransactionLoadPlanner(std::size_t wallet_count,
                               const WalletTransactionsWorkload& workload,
                               std::uint64_t sequence_offset = 0U);

  std::optional<std::vector<WalletTransactionPlanEntry>> NextBatch(
      std::vector<std::uint64_t>* available_balances,
      std::size_t maximum_entries = std::numeric_limits<std::size_t>::max());
  [[nodiscard]] std::size_t batch_size() const;

 private:
  WalletTransactionsWorkload workload_;
  std::size_t wallet_count_ = 0U;
  std::vector<std::size_t> senders_;
  std::vector<std::size_t> receivers_;
  std::vector<std::size_t> equal_sender_order_;
  std::mt19937_64 rng_;
  std::size_t equal_sender_cursor_ = 0U;
};

std::uint64_t WalletTransactionDurationAttemptLimit(
    std::chrono::milliseconds duration, const WalletTransactionRate& rate);

std::optional<std::uint64_t> ExplicitWalletTransactionAttemptLimit(
    const WalletTransactionsWorkload& workload);

std::vector<WalletTransactionPlanEntry> BuildWalletTransactionPlan(
    std::size_t wallet_count, const WalletTransactionsWorkload& workload);

}  // namespace bbp
