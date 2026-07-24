#include "bbp/simulation_registry.h"

#include <algorithm>
#include <cstdint>
#include <limits>
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
  if (wallet.node == 0U || wallet.node > topology_.node_count) {
    throw std::runtime_error("wallet backing node is out of range");
  }
  if (wallets_.size() >= std::numeric_limits<uint32_t>::max()) {
    throw std::overflow_error("wallet index exceeds uint32");
  }
  wallet.wallet_index = static_cast<uint32_t>(wallets_.size() + 1U);
  const uint32_t node_index = wallet.node - 1U;
  if (std::find(topology_.wallet_nodes.begin(), topology_.wallet_nodes.end(),
                node_index) == topology_.wallet_nodes.end()) {
    topology_.wallet_nodes.push_back(node_index);
    std::sort(topology_.wallet_nodes.begin(), topology_.wallet_nodes.end());
  }
  topology_.configured = true;
  topology_.wallet_node_count =
      static_cast<uint32_t>(topology_.wallet_nodes.size());
  wallets_.push_back(std::move(wallet));
}

void SimulationRegistry::SetRuntimeNodeCount(uint32_t node_count) {
  if (node_count < topology_.node_count) {
    throw std::runtime_error(
        "runtime node count cannot shrink through wallet publication");
  }
  topology_.node_count = node_count;
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

}  // namespace bbp
