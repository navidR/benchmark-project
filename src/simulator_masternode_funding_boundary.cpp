#include "simulator_masternode_funding_boundary.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "bbp/drivers/chain_driver.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_node_process_state.h"

namespace bbp::simulator_app_internal {
namespace {

void WaitForRuntimeNodeHeight(const ChainDriver& driver,
                              const RuntimeNodePointers& nodes,
                              std::uint64_t height,
                              std::chrono::seconds timeout,
                              std::stop_token stop_token) {
  for (NodeRuntime* node : nodes) {
    if (node == nullptr || !node->AllowsChainMetrics()) {
      continue;
    }
    driver.WaitForHeight(node->config, height, timeout, stop_token);
  }
}

std::uint64_t GenerateAndSynchronizeMasternodeBlocks(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    NodeRuntime& miner, const RuntimeNodePointers& nodes, std::uint32_t count,
    const std::string& reward_address, std::chrono::seconds timeout,
    std::stop_token stop_token) {
  if (count == 0U) {
    return driver.ReadMetrics(miner.config, stop_token).height;
  }
  RequireNodeRunning(miner, "masternode block generation");
  const std::uint64_t start_height =
      driver.ReadMetrics(miner.config, stop_token).height;
  const std::vector<std::string> hashes =
      GenerateBlocksSerialized(block_generation_mutex, driver, miner.config,
                               count, reward_address, stop_token);
  if (hashes.size() != count ||
      start_height >
          std::numeric_limits<std::uint64_t>::max() - hashes.size()) {
    throw std::runtime_error(
        "masternode block generation returned an invalid block count");
  }
  RecordGeneratedBlocks(driver, miner, hashes, stop_token);
  const std::uint64_t target_height = start_height + hashes.size();
  WaitForRuntimeNodeHeight(driver, nodes, target_height, timeout, stop_token);
  return target_height;
}

}  // namespace

std::uint64_t PrepareMasternodeFunding(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    NodeRuntime& miner, NodeRuntime& funding_wallet,
    const std::string& funding_address, const RuntimeNodePointers& nodes,
    const ChainMasternodeFundingRequirements& requirements,
    std::chrono::seconds timeout, std::stop_token stop_token) {
  if (funding_address.empty() || requirements.minimum_balance_satoshis == 0U ||
      requirements.minimum_chain_height == 0U) {
    throw std::logic_error("masternode funding requirements are incomplete");
  }
  RequireNodeRunning(funding_wallet, "masternode funding");
  if (!funding_wallet.config.wallet_enabled) {
    throw std::runtime_error(
        "masternode funding requires a wallet-enabled node");
  }
  constexpr std::uint32_t kFundingBatchBlocks = 100U;
  for (;;) {
    ThrowIfStopRequested(stop_token);
    const std::uint64_t height =
        driver.ReadMetrics(miner.config, stop_token).height;
    const ChainWalletSnapshot balance = driver.ReadWalletSnapshot(
        funding_wallet.config, ChainWalletMode::kPublic, 1U, stop_token);
    if (height >= requirements.minimum_chain_height &&
        balance.available_balance_satoshis >=
            requirements.minimum_balance_satoshis) {
      return balance.available_balance_satoshis;
    }
    std::uint32_t block_count = kFundingBatchBlocks;
    if (height < requirements.minimum_chain_height) {
      block_count = static_cast<std::uint32_t>(std::min<std::uint64_t>(
          kFundingBatchBlocks, requirements.minimum_chain_height - height));
    }
    static_cast<void>(GenerateAndSynchronizeMasternodeBlocks(
        driver, block_generation_mutex, miner, nodes, block_count,
        funding_address, timeout, stop_token));
  }
}

void ConfirmMasternodeTransactions(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    NodeRuntime& miner, const std::string& reward_address,
    const RuntimeNodePointers& nodes,
    const std::vector<MasternodeTransactionConfirmation>& transactions,
    std::uint32_t confirmation_blocks, std::chrono::seconds timeout,
    std::stop_token stop_token) {
  if (transactions.empty()) {
    return;
  }
  bool requires_confirmation_blocks = false;
  for (const MasternodeTransactionConfirmation& transaction : transactions) {
    if (transaction.funding_wallet == nullptr ||
        transaction.transaction_id.empty()) {
      throw std::runtime_error(
          "masternode transaction confirmation is incomplete");
    }
    bool funding_wallet_observed = false;
    for (NodeRuntime* node : nodes) {
      if (node == nullptr || !node->AllowsChainMetrics()) {
        continue;
      }
      const ChainTransactionObservation observation = driver.WaitForTransaction(
          node->config, transaction.transaction_id, timeout, stop_token);
      requires_confirmation_blocks =
          requires_confirmation_blocks ||
          observation.state != ChainTransactionState::kConfirmed ||
          observation.confirmations == 0U;
      if (node == transaction.funding_wallet) {
        funding_wallet_observed = true;
      }
    }
    if (!funding_wallet_observed) {
      throw std::runtime_error(
          "masternode transaction funding wallet is not active");
    }
  }
  if (requires_confirmation_blocks) {
    static_cast<void>(GenerateAndSynchronizeMasternodeBlocks(
        driver, block_generation_mutex, miner, nodes,
        std::max<std::uint32_t>(1U, confirmation_blocks), reward_address,
        timeout, stop_token));
  }
  for (const MasternodeTransactionConfirmation& transaction : transactions) {
    const ChainTransactionObservation observation = driver.WaitForTransaction(
        transaction.funding_wallet->config, transaction.transaction_id, timeout,
        stop_token);
    if (observation.state != ChainTransactionState::kConfirmed ||
        observation.confirmations == 0U) {
      throw std::runtime_error(
          "masternode transaction did not reach confirmed state");
    }
  }
}

MasternodeIdentity RegisteredMasternodeIdentity(
    std::uint32_t node, std::string node_id, std::string funding_wallet_node_id,
    const ChainMasternodeRegistration& registration,
    const ChainMasternodeStatus& status) {
  if (!status.ready() || status.pro_tx_hash != registration.pro_tx_hash ||
      status.service != registration.service) {
    throw std::runtime_error(
        "masternode readiness identity does not match its registration");
  }
  return MasternodeIdentity{
      .node = node,
      .node_id = std::move(node_id),
      .funding_wallet_node_id = std::move(funding_wallet_node_id),
      .pro_tx_hash = registration.pro_tx_hash,
      .service = registration.service,
      .collateral_address = registration.collateral_address,
      .owner_address = registration.owner_address,
      .operator_public_key = registration.operator_public_key,
      .operator_secret_key = registration.operator_secret_key,
      .voting_address = registration.voting_address,
      .payout_address = registration.payout_address,
      .collateral_hash = status.collateral_hash,
      .collateral_index = status.collateral_index,
      .state = status.state,
      .status = status.status,
  };
}

}  // namespace bbp::simulator_app_internal
