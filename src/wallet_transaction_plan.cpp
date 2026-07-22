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

std::size_t SampleIndex(std::mt19937_64* rng, std::size_t size) {
  if (size == 0U) {
    throw std::runtime_error("wallet transaction sample must not be empty");
  }
  return static_cast<std::size_t>(
      SampleInclusive(rng, 0U, static_cast<std::uint64_t>(size - 1U)));
}

void RequireDisjointWalletSets(const std::vector<std::size_t>& senders,
                               const std::vector<std::size_t>& receivers) {
  for (const std::size_t sender : senders) {
    if (std::find(receivers.begin(), receivers.end(), sender) !=
        receivers.end()) {
      throw std::runtime_error(
          "transaction load sender_wallets and receiver_wallets must be "
          "disjoint");
    }
  }
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
    case WalletTransferStrategy::kRandomBruteforce:
    case WalletTransferStrategy::kEqualFanout:
      throw std::runtime_error(
          "balance-aware transaction load strategy requires the load "
          "planner");
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
    case WalletTransferStrategy::kRandomBruteforce:
    case WalletTransferStrategy::kEqualFanout:
      throw std::runtime_error(
          "balance-aware transaction load strategy requires the load "
          "planner");
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

WalletTransactionLoadPlanner::WalletTransactionLoadPlanner(
    std::size_t wallet_count, const WalletTransactionsWorkload& workload)
    : workload_(workload), rng_(workload.random_seed) {
  if (!IsTransactionLoadStrategy(workload_.strategy)) {
    throw std::runtime_error(
        "transaction load planner requires a load strategy");
  }
  if (wallet_count < 2U) {
    throw std::runtime_error(
        "transaction load planner requires at least two wallets");
  }
  if (workload_.amount.minimum_satoshis == 0U ||
      workload_.amount.minimum_satoshis > workload_.amount.maximum_satoshis) {
    throw std::runtime_error("transaction load amount range is invalid");
  }
  if (workload_.fee_satoshis == 0U) {
    throw std::runtime_error("transaction load fee must be greater than zero");
  }
  senders_ = ResolveSelectedWallets(workload_.sender_wallets, wallet_count,
                                    "sender_wallets");
  receivers_ = ResolveSelectedWallets(workload_.receiver_wallets, wallet_count,
                                      "receiver_wallets");
  if (senders_.empty() || receivers_.empty()) {
    throw std::runtime_error(
        "transaction load requires explicit sender and receiver wallets");
  }
  RequireDisjointWalletSets(senders_, receivers_);
  equal_sender_order_ = senders_;
  if (workload_.strategy == WalletTransferStrategy::kEqualFanout) {
    std::shuffle(equal_sender_order_.begin(), equal_sender_order_.end(), rng_);
  }
}

std::optional<std::vector<WalletTransactionPlanEntry>>
WalletTransactionLoadPlanner::NextBatch(
    std::vector<std::uint64_t>* available_balances) {
  if (available_balances == nullptr) {
    throw std::runtime_error(
        "transaction load planner requires an available-balance ledger");
  }
  for (const std::size_t sender : senders_) {
    if (sender >= available_balances->size()) {
      throw std::runtime_error(
          "transaction load balance ledger does not cover every sender");
    }
  }
  const std::uint64_t fee_reserve_satoshis =
      EffectiveWalletTransactionFeeReserveSatoshis(workload_);

  if (workload_.strategy == WalletTransferStrategy::kRandomBruteforce) {
    std::vector<std::size_t> eligible_senders;
    eligible_senders.reserve(senders_.size());
    for (const std::size_t sender : senders_) {
      const std::uint64_t balance = available_balances->at(sender);
      if (balance <= fee_reserve_satoshis) {
        continue;
      }
      const std::uint64_t maximum =
          std::min({workload_.amount.maximum_satoshis, balance / 5U,
                    balance - fee_reserve_satoshis});
      if (maximum >= workload_.amount.minimum_satoshis) {
        eligible_senders.push_back(sender);
      }
    }
    if (eligible_senders.empty()) {
      return std::nullopt;
    }
    const std::size_t sender =
        eligible_senders[SampleIndex(&rng_, eligible_senders.size())];
    const std::size_t receiver =
        receivers_[SampleIndex(&rng_, receivers_.size())];
    const std::uint64_t balance = available_balances->at(sender);
    const std::uint64_t maximum =
        std::min({workload_.amount.maximum_satoshis, balance / 5U,
                  balance - fee_reserve_satoshis});
    const std::uint64_t amount =
        SampleInclusive(&rng_, workload_.amount.minimum_satoshis, maximum);
    available_balances->at(sender) = balance - amount - fee_reserve_satoshis;
    return std::vector<WalletTransactionPlanEntry>{WalletTransactionPlanEntry{
        .sender_index = sender,
        .receiver_index = receiver,
        .amount_satoshis = amount,
        .interval_before = std::chrono::milliseconds(0)}};
  }

  if (workload_.strategy != WalletTransferStrategy::kEqualFanout) {
    throw std::runtime_error("unknown transaction load strategy");
  }
  const std::uint64_t receiver_count =
      static_cast<std::uint64_t>(receivers_.size());
  if (fee_reserve_satoshis >
      std::numeric_limits<std::uint64_t>::max() / receiver_count) {
    throw std::runtime_error("equal fan-out fee total overflows uint64");
  }
  const std::uint64_t total_fees = fee_reserve_satoshis * receiver_count;
  for (std::size_t offset = 0U; offset < equal_sender_order_.size(); ++offset) {
    const std::size_t position =
        (equal_sender_cursor_ + offset) % equal_sender_order_.size();
    const std::size_t sender = equal_sender_order_[position];
    const std::uint64_t balance = available_balances->at(sender);
    if (balance <= total_fees) {
      continue;
    }
    const std::uint64_t fee_safe_budget = balance - total_fees;
    const std::uint64_t amount = std::min(workload_.amount.maximum_satoshis,
                                          fee_safe_budget / receiver_count);
    if (amount < workload_.amount.minimum_satoshis) {
      continue;
    }
    if (amount > (std::numeric_limits<std::uint64_t>::max() - total_fees) /
                     receiver_count) {
      throw std::runtime_error("equal fan-out amount total overflows uint64");
    }
    const std::uint64_t total_amount = amount * receiver_count;
    available_balances->at(sender) = balance - total_fees - total_amount;
    equal_sender_cursor_ = (position + 1U) % equal_sender_order_.size();
    std::vector<WalletTransactionPlanEntry> batch;
    batch.reserve(receivers_.size());
    for (const std::size_t receiver : receivers_) {
      batch.push_back(WalletTransactionPlanEntry{
          .sender_index = sender,
          .receiver_index = receiver,
          .amount_satoshis = amount,
          .interval_before = std::chrono::milliseconds(0),
      });
    }
    return batch;
  }
  return std::nullopt;
}

std::size_t WalletTransactionLoadPlanner::batch_size() const {
  return workload_.strategy == WalletTransferStrategy::kEqualFanout
             ? receivers_.size()
             : 1U;
}

std::uint64_t WalletTransactionDurationAttemptLimit(
    std::chrono::milliseconds duration, const WalletTransactionRate& rate) {
  if (duration.count() <= 0) {
    throw std::runtime_error(
        "transaction load duration must be greater than zero");
  }
  const std::uint64_t duration_ms =
      static_cast<std::uint64_t>(duration.count());
  if (duration_ms >
      std::numeric_limits<std::uint64_t>::max() / rate.millionths()) {
    throw std::runtime_error(
        "transaction load duration and rate attempt count overflows uint64");
  }
  constexpr std::uint64_t kMillisecondsPerRateUnit = 1'000'000'000U;
  const std::uint64_t numerator = duration_ms * rate.millionths();
  const std::uint64_t quotient = numerator / kMillisecondsPerRateUnit;
  const std::uint64_t remainder = numerator % kMillisecondsPerRateUnit;
  if (quotient == std::numeric_limits<std::uint64_t>::max() &&
      remainder != 0U) {
    throw std::runtime_error(
        "transaction load duration attempt count exceeds uint64");
  }
  return quotient + (remainder == 0U ? 0U : 1U);
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
