#include "bbp/simulation_registry.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <set>
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
  if (topology.masternode_node_count != topology.masternode_nodes.size()) {
    throw std::runtime_error(
        "resolved topology masternode_nodes size must match "
        "masternode_node_count");
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
  if (!topology_.allow_miner_wallet_overlap &&
      std::find(topology_.miner_nodes.begin(), topology_.miner_nodes.end(),
                node_index) != topology_.miner_nodes.end()) {
    throw std::runtime_error(
        "wallet node overlaps a miner node without overlap permission");
  }
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

void SimulationRegistry::RemoveWalletNode(uint32_t node_index) {
  const auto role = std::find(topology_.wallet_nodes.begin(),
                              topology_.wallet_nodes.end(), node_index);
  if (role == topology_.wallet_nodes.end()) {
    throw std::runtime_error("wallet removal references an unregistered node");
  }
  const auto removed_begin = std::remove_if(
      wallets_.begin(), wallets_.end(),
      [node_index](const WalletIdentity& wallet) {
        return wallet.node != 0U && wallet.node - 1U == node_index;
      });
  if (removed_begin == wallets_.end()) {
    throw std::runtime_error("wallet role has no registered wallet identity");
  }
  wallets_.erase(removed_begin, wallets_.end());
  topology_.wallet_nodes.erase(role);
  topology_.wallet_node_count =
      static_cast<uint32_t>(topology_.wallet_nodes.size());
  for (std::size_t index = 0U; index < wallets_.size(); ++index) {
    wallets_[index].wallet_index = static_cast<uint32_t>(index + 1U);
  }
}

void SimulationRegistry::AddMinerNode(uint32_t node_index) {
  if (node_index >= topology_.node_count) {
    throw std::runtime_error("miner node is out of range");
  }
  if (std::find(topology_.miner_nodes.begin(), topology_.miner_nodes.end(),
                node_index) != topology_.miner_nodes.end()) {
    throw std::runtime_error("miner node is already registered");
  }
  if (!topology_.allow_miner_wallet_overlap &&
      std::find(topology_.wallet_nodes.begin(), topology_.wallet_nodes.end(),
                node_index) != topology_.wallet_nodes.end()) {
    throw std::runtime_error(
        "miner node overlaps a wallet node without overlap permission");
  }
  topology_.miner_nodes.push_back(node_index);
  std::sort(topology_.miner_nodes.begin(), topology_.miner_nodes.end());
  topology_.configured = true;
  topology_.miner_node_count =
      static_cast<uint32_t>(topology_.miner_nodes.size());
}

void SimulationRegistry::RemoveMinerNodes(
    const std::vector<uint32_t>& node_indexes) {
  std::set<uint32_t> requested(node_indexes.begin(), node_indexes.end());
  if (requested.size() != node_indexes.size()) {
    throw std::runtime_error("miner removal contains duplicate node indexes");
  }
  for (const uint32_t node_index : requested) {
    if (std::find(topology_.miner_nodes.begin(), topology_.miner_nodes.end(),
                  node_index) == topology_.miner_nodes.end()) {
      throw std::runtime_error("miner removal references an unregistered node");
    }
  }
  std::erase_if(topology_.miner_nodes,
                [&](uint32_t node) { return requested.contains(node); });
  topology_.miner_node_count =
      static_cast<uint32_t>(topology_.miner_nodes.size());
}

void SimulationRegistry::AddMasternode(MasternodeIdentity masternode) {
  if (masternode.node == 0U || masternode.node > topology_.node_count) {
    throw std::runtime_error("masternode backing node is out of range");
  }
  if (masternode.node_id.empty() || masternode.funding_wallet_node_id.empty() ||
      masternode.pro_tx_hash.empty() || masternode.service.empty() ||
      masternode.collateral_address.empty() ||
      masternode.owner_address.empty() ||
      masternode.operator_public_key.empty() ||
      masternode.operator_secret_key.empty() ||
      masternode.voting_address.empty() || masternode.payout_address.empty() ||
      masternode.state != "READY") {
    throw std::runtime_error("masternode identity is incomplete");
  }
  const uint32_t node_index = masternode.node - 1U;
  if (std::find(topology_.masternode_nodes.begin(),
                topology_.masternode_nodes.end(),
                node_index) != topology_.masternode_nodes.end()) {
    throw std::runtime_error("masternode node is already registered");
  }
  const auto duplicate_identity = [&](const MasternodeIdentity& existing) {
    return existing.pro_tx_hash == masternode.pro_tx_hash ||
           existing.service == masternode.service;
  };
  if (std::any_of(masternodes_.begin(), masternodes_.end(),
                  duplicate_identity)) {
    throw std::runtime_error(
        "masternode ProTx hash or service is already registered");
  }
  topology_.masternode_nodes.push_back(node_index);
  std::sort(topology_.masternode_nodes.begin(),
            topology_.masternode_nodes.end());
  topology_.configured = true;
  topology_.masternode_node_count =
      static_cast<uint32_t>(topology_.masternode_nodes.size());
  masternodes_.push_back(std::move(masternode));
  std::sort(
      masternodes_.begin(), masternodes_.end(),
      [](const MasternodeIdentity& left, const MasternodeIdentity& right) {
        return left.node < right.node;
      });
}

void SimulationRegistry::RemoveMasternodeNodes(
    const std::vector<uint32_t>& node_indexes) {
  std::set<uint32_t> requested(node_indexes.begin(), node_indexes.end());
  if (requested.size() != node_indexes.size()) {
    throw std::runtime_error(
        "masternode removal contains duplicate node indexes");
  }
  for (const uint32_t node_index : requested) {
    if (std::find(topology_.masternode_nodes.begin(),
                  topology_.masternode_nodes.end(),
                  node_index) == topology_.masternode_nodes.end()) {
      throw std::runtime_error(
          "masternode removal references an unregistered node");
    }
  }
  std::erase_if(topology_.masternode_nodes,
                [&](uint32_t node) { return requested.contains(node); });
  std::erase_if(masternodes_, [&](const MasternodeIdentity& masternode) {
    return masternode.node != 0U && requested.contains(masternode.node - 1U);
  });
  topology_.masternode_node_count =
      static_cast<uint32_t>(topology_.masternode_nodes.size());
}

void SimulationRegistry::SetRuntimeNodeCount(uint32_t node_count) {
  if (node_count < topology_.node_count) {
    throw std::runtime_error(
        "runtime node count cannot shrink through wallet publication");
  }
  topology_.node_count = node_count;
}

SimulationRegistry SimulationRegistry::RemapRuntimeNodes(
    const std::vector<std::optional<std::uint32_t>>& old_to_new,
    PeerTopologyConfig peer_topology) const {
  if (old_to_new.size() != topology_.node_count) {
    throw std::invalid_argument(
        "runtime role node remap size must match the current node count");
  }
  std::vector<bool> seen(old_to_new.size(), false);
  std::uint32_t next_node_count = 0U;
  for (const std::optional<std::uint32_t> mapped : old_to_new) {
    if (!mapped) {
      continue;
    }
    if (*mapped >= old_to_new.size() || seen[*mapped]) {
      throw std::invalid_argument(
          "runtime role node remap must contain unique compact indexes");
    }
    seen[*mapped] = true;
    next_node_count = std::max(next_node_count, *mapped + 1U);
  }
  if (std::find(seen.begin(), seen.begin() + next_node_count, false) !=
      seen.begin() + next_node_count) {
    throw std::invalid_argument(
        "runtime role node remap must contain unique compact indexes");
  }

  SimulationRegistry next = *this;
  const auto remap_role_nodes = [&](const std::vector<std::uint32_t>& nodes,
                                    std::string_view role) {
    std::vector<std::uint32_t> remapped;
    remapped.reserve(nodes.size());
    for (const std::uint32_t node : nodes) {
      if (node >= old_to_new.size() || !old_to_new[node]) {
        throw std::runtime_error("cannot remove a node with the " +
                                 std::string(role) + " role");
      }
      remapped.push_back(*old_to_new[node]);
    }
    return remapped;
  };
  next.topology_.node_count = next_node_count;
  next.topology_.wallet_nodes =
      remap_role_nodes(topology_.wallet_nodes, "wallet");
  next.topology_.miner_nodes = remap_role_nodes(topology_.miner_nodes, "miner");
  next.topology_.masternode_nodes =
      remap_role_nodes(topology_.masternode_nodes, "masternode");
  next.topology_.wallet_node_count =
      static_cast<std::uint32_t>(next.topology_.wallet_nodes.size());
  next.topology_.miner_node_count =
      static_cast<std::uint32_t>(next.topology_.miner_nodes.size());
  next.topology_.masternode_node_count =
      static_cast<std::uint32_t>(next.topology_.masternode_nodes.size());
  next.topology_.peer_topology = std::move(peer_topology);
  next.topology_.peer_connectivity.clear();
  next.topology_.peer_connectivity.reserve(topology_.peer_connectivity.size());
  for (const PeerConnectivityPolicy& policy : topology_.peer_connectivity) {
    if (policy.node >= old_to_new.size()) {
      throw std::runtime_error(
          "runtime peer connectivity node is out of range");
    }
    if (!old_to_new[policy.node]) {
      continue;
    }
    PeerConnectivityPolicy remapped = policy;
    remapped.node = *old_to_new[policy.node];
    const std::vector<std::uint32_t> eligible = ResolvePeerTopologyPeerIndexes(
        next.topology_.peer_topology, next_node_count, remapped.node);
    if (remapped.mode == PeerConnectivityMode::kAllPeers) {
      if (eligible.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error(
            "runtime all-peers eligibility exceeds uint32");
      }
      const std::uint32_t count = static_cast<std::uint32_t>(eligible.size());
      remapped.peer_count = PeerCountPolicy(count, count);
    } else {
      const std::size_t maximum_possible =
          next_node_count == 0U ? 0U : next_node_count - 1U;
      if (remapped.peer_count.maximum() > maximum_possible ||
          remapped.peer_count.minimum() > eligible.size()) {
        throw std::runtime_error(
            "runtime peer connectivity policy is incompatible with the "
            "remapped node set");
      }
    }
    next.topology_.peer_connectivity.push_back(std::move(remapped));
  }
  for (WalletIdentity& wallet : next.wallets_) {
    if (wallet.node == 0U || wallet.node > old_to_new.size() ||
        !old_to_new[wallet.node - 1U]) {
      throw std::runtime_error(
          "cannot remove a node backing a registered wallet");
    }
    wallet.node = *old_to_new[wallet.node - 1U] + 1U;
  }
  for (MasternodeIdentity& masternode : next.masternodes_) {
    if (masternode.node == 0U || masternode.node > old_to_new.size() ||
        !old_to_new[masternode.node - 1U]) {
      throw std::runtime_error(
          "cannot remove a node backing a registered masternode");
    }
    masternode.node = *old_to_new[masternode.node - 1U] + 1U;
  }
  return next;
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
