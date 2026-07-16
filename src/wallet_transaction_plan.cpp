#include "bbp/simulator/wallet_transaction_plan.h"

#include <algorithm>
#include <limits>
#include <numeric>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace bbp {
namespace {

std::vector<std::size_t> ResolveSelectedWallets(
    const std::vector<std::uint32_t>& one_based_wallets,
    std::size_t wallet_count, std::string_view field) {
  std::vector<std::size_t> result;
  result.reserve(one_based_wallets.size());
  std::set<std::uint32_t> unique;
  for (const std::uint32_t wallet : one_based_wallets) {
    if (wallet == 0U || static_cast<std::size_t>(wallet) > wallet_count) {
      throw std::runtime_error(std::string(field) +
                               " wallet index is out of range");
    }
    if (!unique.insert(wallet).second) {
      throw std::runtime_error(std::string(field) +
                               " contains a duplicate wallet index");
    }
    result.push_back(static_cast<std::size_t>(wallet - 1U));
  }
  return result;
}

std::vector<std::size_t> ComplementWallets(
    std::size_t wallet_count, const std::vector<std::size_t>& selected) {
  std::vector<std::size_t> result;
  for (std::size_t wallet = 0; wallet < wallet_count; ++wallet) {
    if (std::find(selected.begin(), selected.end(), wallet) == selected.end()) {
      result.push_back(wallet);
    }
  }
  return result;
}

std::uint64_t SampleInclusive(std::mt19937_64* rng, std::uint64_t minimum,
                              std::uint64_t maximum) {
  if (minimum == maximum) {
    return minimum;
  }
  const std::uint64_t range = maximum - minimum + 1U;
  if (range == 0U) {
    return (*rng)();
  }
  const std::uint64_t rejection_threshold =
      static_cast<std::uint64_t>(0U - range) % range;
  std::uint64_t value = 0;
  do {
    value = (*rng)();
  } while (value < rejection_threshold);
  return minimum + value % range;
}

std::uint64_t MillisecondsCount(std::chrono::milliseconds value) {
  if (value.count() < 0) {
    throw std::runtime_error(
        "wallet transaction interval must not be negative");
  }
  return static_cast<std::uint64_t>(value.count());
}

}  // namespace

std::vector<WalletTransactionPlanEntry> BuildWalletTransactionPlan(
    std::size_t wallet_count, const WalletTransactionsWorkload& workload) {
  if (wallet_count < 2U) {
    throw std::runtime_error(
        "wallet transaction plan requires at least two wallets");
  }
  if (workload.transaction_count == 0U) {
    throw std::runtime_error(
        "wallet transaction plan requires at least one transaction");
  }
  if (workload.amount.minimum_satoshis == 0U ||
      workload.amount.minimum_satoshis > workload.amount.maximum_satoshis) {
    throw std::runtime_error("wallet transaction amount range is invalid");
  }
  if (workload.amount.kind == ValueDistributionKind::kFixed &&
      workload.amount.minimum_satoshis != workload.amount.maximum_satoshis) {
    throw std::runtime_error(
        "fixed wallet transaction amount range must have equal bounds");
  }
  const std::uint64_t minimum_interval =
      MillisecondsCount(workload.interval.minimum);
  const std::uint64_t maximum_interval =
      MillisecondsCount(workload.interval.maximum);
  if (minimum_interval > maximum_interval) {
    throw std::runtime_error("wallet transaction interval range is invalid");
  }
  if (workload.interval.kind == ValueDistributionKind::kFixed &&
      minimum_interval != maximum_interval) {
    throw std::runtime_error(
        "fixed wallet transaction interval range must have equal bounds");
  }

  std::vector<std::size_t> senders;
  std::vector<std::size_t> receivers;
  std::vector<std::size_t> shuffled_wallets(wallet_count);
  std::iota(shuffled_wallets.begin(), shuffled_wallets.end(), 0U);
  std::mt19937_64 rng(workload.random_seed);
  switch (workload.strategy) {
    case WalletTransferStrategy::kRoundRobin:
      if (!workload.sender_wallets.empty() ||
          !workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "round_robin wallet transaction plan does not accept wallet "
            "selectors");
      }
      break;
    case WalletTransferStrategy::kRandom:
      if (!workload.sender_wallets.empty() ||
          !workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "random wallet transaction plan does not accept wallet selectors");
      }
      std::shuffle(shuffled_wallets.begin(), shuffled_wallets.end(), rng);
      break;
    case WalletTransferStrategy::kFanout:
      if (!workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "fanout wallet transaction plan does not accept receiver_wallets");
      }
      senders = ResolveSelectedWallets(workload.sender_wallets, wallet_count,
                                       "sender_wallets");
      receivers = ComplementWallets(wallet_count, senders);
      if (senders.empty() || receivers.empty()) {
        throw std::runtime_error(
            "fanout wallet transaction plan requires sender and receiver "
            "wallets");
      }
      break;
    case WalletTransferStrategy::kHotspot:
      if (!workload.sender_wallets.empty()) {
        throw std::runtime_error(
            "hotspot wallet transaction plan does not accept sender_wallets");
      }
      receivers = ResolveSelectedWallets(workload.receiver_wallets,
                                         wallet_count, "receiver_wallets");
      senders = ComplementWallets(wallet_count, receivers);
      if (senders.empty() || receivers.empty()) {
        throw std::runtime_error(
            "hotspot wallet transaction plan requires sender and receiver "
            "wallets");
      }
      break;
  }

  std::vector<WalletTransactionPlanEntry> plan;
  plan.reserve(workload.transaction_count);
  for (std::uint32_t transaction_index = 0;
       transaction_index < workload.transaction_count; ++transaction_index) {
    std::size_t sender = 0;
    std::size_t receiver = 0;
    switch (workload.strategy) {
      case WalletTransferStrategy::kRoundRobin:
      case WalletTransferStrategy::kRandom: {
        const std::size_t sender_position =
            static_cast<std::size_t>(transaction_index) % wallet_count;
        const std::size_t receiver_position =
            (sender_position + 1U) % wallet_count;
        sender = shuffled_wallets[sender_position];
        receiver = shuffled_wallets[receiver_position];
        break;
      }
      case WalletTransferStrategy::kFanout:
        receiver = receivers[static_cast<std::size_t>(transaction_index) %
                             receivers.size()];
        sender = senders[(static_cast<std::size_t>(transaction_index) /
                          receivers.size()) %
                         senders.size()];
        break;
      case WalletTransferStrategy::kHotspot:
        sender = senders[static_cast<std::size_t>(transaction_index) %
                         senders.size()];
        receiver = receivers[(static_cast<std::size_t>(transaction_index) /
                              senders.size()) %
                             receivers.size()];
        break;
    }
    const std::uint64_t amount =
        workload.amount.kind == ValueDistributionKind::kFixed
            ? workload.amount.minimum_satoshis
            : SampleInclusive(&rng, workload.amount.minimum_satoshis,
                              workload.amount.maximum_satoshis);
    std::chrono::milliseconds interval_before{0};
    if (transaction_index != 0U) {
      const std::uint64_t interval =
          workload.interval.kind == ValueDistributionKind::kFixed
              ? minimum_interval
              : SampleInclusive(&rng, minimum_interval, maximum_interval);
      if (interval >
          static_cast<std::uint64_t>(
              std::numeric_limits<std::chrono::milliseconds::rep>::max())) {
        throw std::runtime_error(
            "wallet transaction interval exceeds milliseconds range");
      }
      interval_before = std::chrono::milliseconds(
          static_cast<std::chrono::milliseconds::rep>(interval));
    }
    plan.push_back(WalletTransactionPlanEntry{
        .sender_index = sender,
        .receiver_index = receiver,
        .amount_satoshis = amount,
        .interval_before = interval_before,
    });
  }
  return plan;
}

}  // namespace bbp
