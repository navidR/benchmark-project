#include "simulator_wallet_transaction_validation.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/wallet_transaction_plan.h"

namespace bbp::simulator_app_internal {

WalletFundingStrategy ParseWalletFundingStrategy(std::string_view value) {
  const std::optional<WalletFundingStrategy> strategy =
      WalletFundingStrategyFromName(value);
  if (strategy) {
    return *strategy;
  }
  throw std::runtime_error(
      "scenario wallet_transactions funding_strategy must be round_robin or "
      "random");
}

WalletTransferStrategy ParseWalletTransferStrategy(std::string_view value) {
  const std::optional<WalletTransferStrategy> strategy =
      WalletTransferStrategyFromName(value);
  if (strategy) {
    return *strategy;
  }
  throw std::runtime_error(
      "scenario wallet_transactions strategy must be round_robin, random, "
      "fanout, hotspot, random_bruteforce, or equal_fanout");
}

WalletPrivacyMode ParseWalletTransactionMode(std::string_view value) {
  const std::optional<WalletPrivacyMode> mode =
      WalletPrivacyModeFromName(value);
  if (mode) {
    return *mode;
  }
  throw std::runtime_error(
      "scenario wallet_transactions mode must be public or private");
}

WalletTransactionFeePolicy ParseWalletTransactionFeePolicy(
    std::string_view value) {
  const std::optional<WalletTransactionFeePolicy> policy =
      WalletTransactionFeePolicyFromName(value);
  if (policy) {
    return *policy;
  }
  throw std::runtime_error(
      "scenario wallet_transactions fee_policy must be fixed");
}

ChainWalletMode ToChainWalletMode(WalletPrivacyMode mode) {
  switch (mode) {
    case WalletPrivacyMode::kPublic:
      return ChainWalletMode::kPublic;
    case WalletPrivacyMode::kPrivate:
      return ChainWalletMode::kPrivate;
  }
  throw std::runtime_error("unknown wallet privacy mode");
}

ChainWalletMode ToChainWalletMode(const WalletInitialization& initialization) {
  return ToChainWalletMode(initialization.mode);
}

void ValidateWalletTransactionsWorkload(
    const WalletTransactionsWorkload& workload, const Options& options) {
  if (!options.topology.configured) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires topology");
  }
  if (options.topology.miner_nodes.empty()) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires at least one "
        "MinerNode");
  }
  const size_t wallet_count = options.topology.wallet_nodes.size();
  if (wallet_count < 2U) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires at least two "
        "WalletNode roles");
  }
  const bool has_transaction_count = workload.transaction_count != 0U;
  const bool has_transaction_rate = workload.transaction_rate.has_value();
  const bool has_duration = workload.duration.has_value();
  const bool load_strategy = IsTransactionLoadStrategy(workload.strategy);
  if (load_strategy) {
    if (has_transaction_count && has_duration) {
      throw std::runtime_error(
          "scenario transaction load transaction_count and duration are "
          "mutually exclusive");
    }
    if (has_duration && !has_transaction_rate) {
      throw std::runtime_error(
          "scenario transaction load duration requires transaction_rate");
    }
  } else if (has_transaction_count && has_transaction_rate) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_count and transaction_rate "
        "are mutually exclusive");
  }
  const std::uint64_t coinbase_confirmations =
      ChainDriverSpecFor(options.chain).coinbase_spendable_confirmations;
  if (workload.funding_blocks_per_wallet < coinbase_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_blocks_per_wallet must be at "
        "least " +
        std::to_string(coinbase_confirmations));
  }
  if (workload.readiness_confirmations < coinbase_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions readiness_confirmations must be at "
        "least " +
        std::to_string(coinbase_confirmations));
  }
  if (workload.funding_blocks_per_wallet < workload.readiness_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_blocks_per_wallet must be >= "
        "readiness_confirmations");
  }
  if (workload.amount.minimum_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions amount must be greater than zero");
  }
  if (workload.amount.minimum_satoshis > workload.amount.maximum_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions amount distribution min must be <= max");
  }
  if (workload.amount.kind == ValueDistributionKind::kFixed &&
      workload.amount.minimum_satoshis != workload.amount.maximum_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions fixed amount distribution requires "
        "equal min and max");
  }
  if (workload.interval.minimum.count() < 0 ||
      workload.interval.minimum > workload.interval.maximum) {
    throw std::runtime_error(
        "scenario wallet_transactions interval distribution min must be <= "
        "max");
  }
  if (workload.interval.kind == ValueDistributionKind::kFixed &&
      workload.interval.minimum != workload.interval.maximum) {
    throw std::runtime_error(
        "scenario wallet_transactions fixed interval distribution requires "
        "equal min and max");
  }
  if (workload.fee_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions fee must be greater than zero");
  }
  const std::uint64_t fee_reserve_satoshis =
      EffectiveWalletTransactionFeeReserveSatoshis(workload);
  if (fee_reserve_satoshis < workload.fee_satoshis) {
    throw std::runtime_error(
        "scenario transaction load fee reserve must cover the requested "
        "fee rate");
  }
  if (workload.amount.maximum_satoshis >
      std::numeric_limits<uint64_t>::max() - fee_reserve_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions amount plus fee overflows uint64");
  }
  const std::uint64_t minimum_funding_threshold =
      workload.amount.maximum_satoshis + fee_reserve_satoshis;
  if (workload.funding_threshold_satoshis < minimum_funding_threshold) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_threshold must cover amount "
        "plus fee");
  }
  if (workload.timeout_sec == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions timeout_sec must be greater than zero");
  }

  if (load_strategy) {
    if (workload.concurrency == 0U ||
        workload.concurrency > kMaximumWalletTransactionLoadConcurrency) {
      throw std::runtime_error(
          "scenario transaction load concurrency must be in 1.." +
          std::to_string(kMaximumWalletTransactionLoadConcurrency));
    }
    if (workload.queue_capacity == 0U ||
        workload.queue_capacity > kMaximumWalletTransactionLoadQueueCapacity) {
      throw std::runtime_error(
          "scenario transaction load queue_capacity must be in 1.." +
          std::to_string(kMaximumWalletTransactionLoadQueueCapacity));
    }
    if (workload.interval.minimum != std::chrono::milliseconds(0) ||
        workload.interval.maximum != std::chrono::milliseconds(0)) {
      throw std::runtime_error(
          "scenario transaction load does not accept interval");
    }
    if (workload.mode != options.wallet_initialization.mode) {
      throw std::runtime_error(
          "scenario transaction load mode must match "
          "topology.wallet_initialization.mode");
    }
    const std::unique_ptr<ChainDriver> driver =
        CreateChainDriver(options.chain);
    if (!driver->SupportsWalletTransactionMode(
            ToChainWalletMode(options.wallet_initialization))) {
      throw std::runtime_error(
          "selected chain driver does not support the requested transaction "
          "load mode");
    }
    if (has_duration) {
      static_cast<void>(WalletTransactionDurationAttemptLimit(
          *workload.duration, *workload.transaction_rate));
    }
  }

  switch (workload.strategy) {
    case WalletTransferStrategy::kRoundRobin:
    case WalletTransferStrategy::kRandom:
      if (!workload.sender_wallets.empty() ||
          !workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions wallet selectors require fanout "
            "or hotspot strategy");
      }
      break;
    case WalletTransferStrategy::kFanout:
      if (workload.sender_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout requires sender_wallets");
      }
      if (!workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout does not accept "
            "receiver_wallets");
      }
      if (workload.sender_wallets.size() >= wallet_count) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout sender_wallets must leave "
            "at least one receiver");
      }
      break;
    case WalletTransferStrategy::kHotspot:
      if (workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot requires receiver_wallets");
      }
      if (!workload.sender_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot does not accept "
            "sender_wallets");
      }
      if (workload.receiver_wallets.size() >= wallet_count) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot receiver_wallets must "
            "leave at least one sender");
      }
      break;
    case WalletTransferStrategy::kRandomBruteforce:
      if (!workload.sender_wallets.empty() ||
          !workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario random_bruteforce uses every wallet and does not "
            "accept sender_wallets or receiver_wallets");
      }
      if (!workload.retained_balance_basis_points ||
          *workload.retained_balance_basis_points >= 10'000U) {
        throw std::runtime_error(
            "scenario random_bruteforce requires "
            "retained_balance_percentage in 0..99.99");
      }
      if (workload.queue_capacity < wallet_count) {
        throw std::runtime_error(
            "scenario random_bruteforce queue_capacity must fit one "
            "all-wallet scheduling cycle");
      }
      break;
    case WalletTransferStrategy::kEqualFanout: {
      if (workload.retained_balance_basis_points) {
        throw std::runtime_error(
            "scenario equal_fanout does not accept "
            "retained_balance_percentage");
      }
      if (workload.sender_wallets.empty() ||
          workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario transaction load requires nonempty sender_wallets and "
            "receiver_wallets");
      }
      for (const std::uint32_t sender : workload.sender_wallets) {
        if (std::find(workload.receiver_wallets.begin(),
                      workload.receiver_wallets.end(),
                      sender) != workload.receiver_wallets.end()) {
          throw std::runtime_error(
              "scenario transaction load sender_wallets and receiver_wallets "
              "must be disjoint");
        }
      }
      if (workload.strategy == WalletTransferStrategy::kEqualFanout) {
        const std::uint64_t receiver_count =
            static_cast<std::uint64_t>(workload.receiver_wallets.size());
        if (fee_reserve_satoshis >
            std::numeric_limits<std::uint64_t>::max() / receiver_count) {
          throw std::runtime_error(
              "scenario equal_fanout fee total overflows uint64");
        }
        if (workload.amount.minimum_satoshis >
            (std::numeric_limits<std::uint64_t>::max() -
             fee_reserve_satoshis * receiver_count) /
                receiver_count) {
          throw std::runtime_error(
              "scenario equal_fanout minimum amount total overflows uint64");
        }
        if (workload.queue_capacity < workload.receiver_wallets.size()) {
          throw std::runtime_error(
              "scenario equal_fanout queue_capacity must fit one complete "
              "receiver batch");
        }
        const std::optional<std::uint64_t> attempt_limit =
            ExplicitWalletTransactionAttemptLimit(workload);
        if (attempt_limit && *attempt_limit % receiver_count != 0U) {
          throw std::runtime_error(
              "scenario equal_fanout attempt limit must contain complete "
              "receiver batches");
        }
      }
      break;
    }
  }

  if (load_strategy) {
    static_cast<void>(WalletTransactionLoadPlanner(wallet_count, workload));
  } else if (has_transaction_count) {
    static_cast<void>(BuildWalletTransactionPlan(wallet_count, workload));
  } else {
    static_cast<void>(WalletTransactionPlanner(wallet_count, workload));
  }

  SimulationRegistry::FromTopology(options.topology,
                                   options.wallet_initialization);
}

}  // namespace bbp::simulator_app_internal
