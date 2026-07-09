#include "benchmark_sim/simulation_registry.h"

#include <cstdint>
#include <utility>

namespace bsim {

Result<SimulationRegistry> SimulationRegistry::FromTopology(
    const NodeRoleTopology& topology,
    const WalletInitialization& wallet_initialization) {
  SimulationRegistry registry;
  registry.topology_ = topology;
  registry.wallet_initialization_ = wallet_initialization;
  if (!topology.configured) {
    return Ok(std::move(registry));
  }
  if (topology.wallet_node_count != topology.wallet_nodes.size()) {
    return Error<SimulationRegistry>(
        "resolved topology wallet_nodes size must match wallet_node_count");
  }
  registry.wallets_.reserve(topology.wallet_nodes.size());
  for (size_t i = 0; i < topology.wallet_nodes.size(); ++i) {
    WalletIdentity wallet;
    wallet.wallet_index = static_cast<uint32_t>(i + 1U);
    wallet.node = topology.wallet_nodes[i] + 1U;
    registry.wallets_.push_back(std::move(wallet));
  }
  return Ok(std::move(registry));
}

void SimulationRegistry::AddWallet(WalletIdentity wallet) {
  wallet.wallet_index = static_cast<uint32_t>(wallets_.size() + 1U);
  wallets_.push_back(std::move(wallet));
}

Result<WalletIdentity*> SimulationRegistry::MutableWalletByIndex(
    size_t wallet_index) {
  if (wallet_index >= wallets_.size()) {
    return Error<WalletIdentity*>("wallet index is out of range");
  }
  return Ok(&wallets_[wallet_index]);
}

Result<const WalletIdentity*> SimulationRegistry::WalletByIndex(
    size_t wallet_index) const {
  if (wallet_index >= wallets_.size()) {
    return Error<const WalletIdentity*>("wallet index is out of range");
  }
  return Ok(&wallets_[wallet_index]);
}

Result<uint32_t> SimulationRegistry::MinerNodeForWalletIndex(
    size_t wallet_index) const {
  if (topology_.miner_nodes.empty()) {
    return Error<uint32_t>("simulation registry has no miner nodes");
  }
  return Ok(topology_.miner_nodes[wallet_index % topology_.miner_nodes.size()] +
            1U);
}

}  // namespace bsim
