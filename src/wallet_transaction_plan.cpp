#include "bbp/simulator/wallet_transaction_plan.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace bbp {
namespace {

constexpr std::uint64_t kTransactionRateUnitsPerOne = 1'000'000U;
constexpr std::uint64_t kMaximumTransactionRateUnits =
    1'000U * kTransactionRateUnitsPerOne;
constexpr std::uint64_t kMillisecondsTimesRateUnits =
    1'000U * kTransactionRateUnitsPerOne;

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

std::size_t CycleIndex(std::uint64_t value, std::size_t size) {
  if (size == 0U) {
    throw std::runtime_error("wallet transaction cycle must not be empty");
  }
  return static_cast<std::size_t>(value % static_cast<std::uint64_t>(size));
}

}  // namespace

WalletTransactionRate WalletTransactionRate::FromDouble(double value) {
  if (!std::isfinite(value) || value < 0.000001 || value > 1'000.0) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_rate must be finite and in "
        "0.000001..1000");
  }
  const double scaled =
      value * static_cast<double>(kTransactionRateUnitsPerOne);
  const double rounded = std::round(scaled);
  if (std::fabs(scaled - rounded) > 1e-6) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_rate supports at most six "
        "decimal places");
  }
  const auto millionths = static_cast<std::uint64_t>(rounded);
  if (millionths == 0U || millionths > kMaximumTransactionRateUnits) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_rate is outside its range");
  }
  return WalletTransactionRate(millionths);
}

double WalletTransactionRate::value() const {
  return static_cast<double>(millionths_) /
         static_cast<double>(kTransactionRateUnitsPerOne);
}

std::uint64_t WalletTransactionRate::millionths() const { return millionths_; }

std::chrono::milliseconds WalletTransactionRate::SimulationElapsedBefore(
    std::uint64_t zero_based_transaction_index) const {
  const std::uint64_t whole = zero_based_transaction_index / millionths_;
  const std::uint64_t remainder = zero_based_transaction_index % millionths_;
  const std::uint64_t maximum = static_cast<std::uint64_t>(
      std::numeric_limits<std::chrono::milliseconds::rep>::max());
  if (whole > maximum / kMillisecondsTimesRateUnits) {
    throw std::runtime_error(
        "wallet transaction rate schedule exceeds milliseconds range");
  }
  std::uint64_t elapsed_ms = whole * kMillisecondsTimesRateUnits;
  if (remainder != 0U) {
    const std::uint64_t partial_numerator =
        remainder * kMillisecondsTimesRateUnits;
    const std::uint64_t partial =
        partial_numerator / millionths_ +
        (partial_numerator % millionths_ == 0U ? 0U : 1U);
    if (elapsed_ms > maximum - partial) {
      throw std::runtime_error(
          "wallet transaction rate schedule exceeds milliseconds range");
    }
    elapsed_ms += partial;
  }
  return std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(elapsed_ms));
}

WalletTransactionRate::WalletTransactionRate(std::uint64_t millionths)
    : millionths_(millionths) {}

WalletTransactionPlanner::WalletTransactionPlanner(
    std::size_t wallet_count, const WalletTransactionsWorkload& workload)
    : wallet_count_(wallet_count),
      workload_(workload),
      shuffled_wallets_(wallet_count),
      rng_(workload.random_seed) {
  if (wallet_count_ < 2U) {
    throw std::runtime_error(
        "wallet transaction plan requires at least two wallets");
  }
  if (workload_.amount.minimum_satoshis == 0U ||
      workload_.amount.minimum_satoshis > workload_.amount.maximum_satoshis) {
    throw std::runtime_error("wallet transaction amount range is invalid");
  }
  if (workload_.amount.kind == ValueDistributionKind::kFixed &&
      workload_.amount.minimum_satoshis != workload_.amount.maximum_satoshis) {
    throw std::runtime_error(
        "fixed wallet transaction amount range must have equal bounds");
  }
  const std::uint64_t minimum_interval =
      MillisecondsCount(workload_.interval.minimum);
  const std::uint64_t maximum_interval =
      MillisecondsCount(workload_.interval.maximum);
  if (minimum_interval > maximum_interval) {
    throw std::runtime_error("wallet transaction interval range is invalid");
  }
  if (workload_.interval.kind == ValueDistributionKind::kFixed &&
      minimum_interval != maximum_interval) {
    throw std::runtime_error(
        "fixed wallet transaction interval range must have equal bounds");
  }

  std::iota(shuffled_wallets_.begin(), shuffled_wallets_.end(), 0U);
  switch (workload_.strategy) {
    case WalletTransferStrategy::kRoundRobin:
      if (!workload_.sender_wallets.empty() ||
          !workload_.receiver_wallets.empty()) {
        throw std::runtime_error(
            "round_robin wallet transaction plan does not accept wallet "
            "selectors");
      }
      break;
    case WalletTransferStrategy::kRandom:
      if (!workload_.sender_wallets.empty() ||
          !workload_.receiver_wallets.empty()) {
        throw std::runtime_error(
            "random wallet transaction plan does not accept wallet selectors");
      }
      std::shuffle(shuffled_wallets_.begin(), shuffled_wallets_.end(), rng_);
      break;
    case WalletTransferStrategy::kFanout:
      if (!workload_.receiver_wallets.empty()) {
        throw std::runtime_error(
            "fanout wallet transaction plan does not accept receiver_wallets");
      }
      senders_ = ResolveSelectedWallets(workload_.sender_wallets, wallet_count_,
                                        "sender_wallets");
      receivers_ = ComplementWallets(wallet_count_, senders_);
      if (senders_.empty() || receivers_.empty()) {
        throw std::runtime_error(
            "fanout wallet transaction plan requires sender and receiver "
            "wallets");
      }
      break;
    case WalletTransferStrategy::kHotspot:
      if (!workload_.sender_wallets.empty()) {
        throw std::runtime_error(
            "hotspot wallet transaction plan does not accept sender_wallets");
      }
      receivers_ = ResolveSelectedWallets(workload_.receiver_wallets,
                                          wallet_count_, "receiver_wallets");
      senders_ = ComplementWallets(wallet_count_, receivers_);
      if (senders_.empty() || receivers_.empty()) {
        throw std::runtime_error(
            "hotspot wallet transaction plan requires sender and receiver "
            "wallets");
      }
      break;
  }
}

WalletTransactionPlanEntry WalletTransactionPlanner::Next() {
  const std::uint64_t transaction_index = transaction_index_;
  if (transaction_index_ == std::numeric_limits<std::uint64_t>::max()) {
    throw std::runtime_error("wallet transaction index exceeds uint64");
  }
  ++transaction_index_;

  std::size_t sender = 0;
  std::size_t receiver = 0;
  switch (workload_.strategy) {
    case WalletTransferStrategy::kRoundRobin:
    case WalletTransferStrategy::kRandom: {
      const std::size_t sender_position =
          CycleIndex(transaction_index, wallet_count_);
      const std::size_t receiver_position =
          (sender_position + 1U) % wallet_count_;
      sender = shuffled_wallets_[sender_position];
      receiver = shuffled_wallets_[receiver_position];
      break;
    }
    case WalletTransferStrategy::kFanout:
      receiver = receivers_[CycleIndex(transaction_index, receivers_.size())];
      sender = senders_[CycleIndex(
          transaction_index / static_cast<std::uint64_t>(receivers_.size()),
          senders_.size())];
      break;
    case WalletTransferStrategy::kHotspot:
      sender = senders_[CycleIndex(transaction_index, senders_.size())];
      receiver = receivers_[CycleIndex(
          transaction_index / static_cast<std::uint64_t>(senders_.size()),
          receivers_.size())];
      break;
  }
  const std::uint64_t amount =
      workload_.amount.kind == ValueDistributionKind::kFixed
          ? workload_.amount.minimum_satoshis
          : SampleInclusive(&rng_, workload_.amount.minimum_satoshis,
                            workload_.amount.maximum_satoshis);
  std::chrono::milliseconds interval_before{0};
  if (transaction_index != 0U) {
    const std::uint64_t minimum_interval =
        MillisecondsCount(workload_.interval.minimum);
    const std::uint64_t maximum_interval =
        MillisecondsCount(workload_.interval.maximum);
    const std::uint64_t interval =
        workload_.interval.kind == ValueDistributionKind::kFixed
            ? minimum_interval
            : SampleInclusive(&rng_, minimum_interval, maximum_interval);
    if (interval >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::chrono::milliseconds::rep>::max())) {
      throw std::runtime_error(
          "wallet transaction interval exceeds milliseconds range");
    }
    interval_before = std::chrono::milliseconds(
        static_cast<std::chrono::milliseconds::rep>(interval));
  }
  return WalletTransactionPlanEntry{
      .sender_index = sender,
      .receiver_index = receiver,
      .amount_satoshis = amount,
      .interval_before = interval_before,
  };
}

std::vector<WalletTransactionPlanEntry> BuildWalletTransactionPlan(
    std::size_t wallet_count, const WalletTransactionsWorkload& workload) {
  if (workload.transaction_count == 0U) {
    throw std::runtime_error(
        "wallet transaction plan requires at least one transaction");
  }
  WalletTransactionPlanner planner(wallet_count, workload);
  std::vector<WalletTransactionPlanEntry> plan;
  plan.reserve(workload.transaction_count);
  for (std::uint32_t transaction_index = 0;
       transaction_index < workload.transaction_count; ++transaction_index) {
    plan.push_back(planner.Next());
  }
  return plan;
}

}  // namespace bbp
