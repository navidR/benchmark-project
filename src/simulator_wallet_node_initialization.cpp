#include "simulator_wallet_node_initialization.h"

#include <chrono>
#include <cstddef>
#include <stdexcept>

#include "bbp/drivers/chain_driver.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {

void InitializeWalletNodes(const Options& options,
                           const std::filesystem::path& events_path,
                           const ChainDriver& driver,
                           const RuntimeNodeSnapshot& nodes,
                           SimulationRegistry& registry,
                           std::stop_token stop_token) {
  if (!options.wallet_backed_workload_requested) {
    return;
  }
  if (registry.wallet_initialization().strategy !=
      WalletInitializationStrategy::kDriverRpc) {
    throw std::runtime_error(
        "wallet-backed workload requires driver_rpc wallet initialization");
  }

  for (std::size_t wallet_index = 0; wallet_index < registry.wallets().size();
       ++wallet_index) {
    ThrowIfStopRequested(stop_token);
    WalletIdentity& wallet = registry.MutableWalletByIndex(wallet_index);
    if (wallet.node == 0U || wallet.node > nodes.size()) {
      throw std::runtime_error("wallet node is out of range");
    }
    NodeRuntime& node = nodes[wallet.node - 1U];
    wallet.node_id = node.config.id;
    while (!node.AllowsChainMetrics()) {
      ThrowIfStopRequested(stop_token);
      if (node.DeclarativeStopApplied() ||
          node.Lifecycle() == NodeRuntimeLifecycle::kStopped ||
          node.Lifecycle() == NodeRuntimeLifecycle::kFailed ||
          node.Lifecycle() == NodeRuntimeLifecycle::kCleaned) {
        throw std::runtime_error(
            "wallet node did not reach Running before initialization: " +
            node.config.id);
      }
      WaitForDuration(std::chrono::milliseconds(20), stop_token);
    }
    if (!node.config.wallet_enabled) {
      throw std::runtime_error(
          "wallet node was not started with wallet support enabled");
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kWalletAddressRequested,
               WalletAddressDetail(wallet, registry.wallet_initialization()));
    wallet.address = driver.CreateWalletAddress(
        node.config, ToChainWalletMode(registry.wallet_initialization()),
        stop_token);
    if (wallet.address.empty()) {
      throw std::runtime_error("chain wallet RPC returned an empty address");
    }
    wallet.funding_address = driver.CreateWalletFundingAddress(
        node.config, ToChainWalletMode(registry.wallet_initialization()),
        wallet.address, stop_token);
    if (wallet.funding_address.empty()) {
      throw std::runtime_error(
          "chain wallet RPC returned an empty funding address");
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kWalletAddressCreated,
               WalletAddressDetail(wallet, registry.wallet_initialization()));
  }
}

}  // namespace bbp::simulator_app_internal
