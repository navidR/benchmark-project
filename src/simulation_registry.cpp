#include "bbp/simulation_registry.h"

#include <cstdint>
#include <stdexcept>

namespace bbp {

SimulationRegistry SimulationRegistry::FromTopology(
    const NodeRoleTopology& topology,
    const WalletInitialization& wallet_initialization) {
  SimulationRegistry registry;
  registry.topology_ = topology;
  registry.wallet_initialization_ = wallet_initialization;
  if (!topology.configured) {
    return registry;
  }
  if (topology.wallet_node_count != topology.wallet_nodes.size()) {
    throw std::runtime_error(
        "resolved topology wallet_nodes size must match wallet_node_count");
  }
  registry.wallets_.reserve(topology.wallet_nodes.size());
  for (size_t i = 0; i < topology.wallet_nodes.size(); ++i) {
    WalletIdentity wallet;
    wallet.wallet_index = static_cast<uint32_t>(i + 1U);
    wallet.node = topology.wallet_nodes[i] + 1U;
    registry.wallets_.push_back(std::move(wallet));
  }
  return registry;
}

void SimulationRegistry::AddWallet(WalletIdentity wallet) {
  wallet.wallet_index = static_cast<uint32_t>(wallets_.size() + 1U);
  wallets_.push_back(std::move(wallet));
}

WalletIdentity& SimulationRegistry::MutableWalletByIndex(size_t wallet_index) {
  if (wallet_index >= wallets_.size()) {
    throw std::runtime_error("wallet index is out of range");
  }
  return wallets_[wallet_index];
}

const WalletIdentity& SimulationRegistry::WalletByIndex(
    size_t wallet_index) const {
  if (wallet_index >= wallets_.size()) {
    throw std::runtime_error("wallet index is out of range");
  }
  return wallets_[wallet_index];
}

uint32_t SimulationRegistry::MinerNodeForWalletIndex(
    size_t wallet_index) const {
  if (topology_.miner_nodes.empty()) {
    throw std::runtime_error("simulation registry has no miner nodes");
  }
  return topology_.miner_nodes[wallet_index % topology_.miner_nodes.size()] +
         1U;
}

}  // namespace bbp
