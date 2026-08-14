#include "simulator_runtime_identity_details.h"

#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <cstddef>
#include <set>
#include <stdexcept>
#include <utility>

#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/options.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {

std::string NodeRoleName(const NodeRoleTopology& topology,
                         std::uint32_t node_index) {
  const bool wallet = NodeListContains(topology.wallet_nodes, node_index);
  const bool miner = NodeListContains(topology.miner_nodes, node_index);
  const bool masternode =
      NodeListContains(topology.masternode_nodes, node_index);
  return wallet && miner && masternode ? "wallet_miner_masternode"
         : wallet && miner             ? "wallet_miner"
         : wallet && masternode        ? "wallet_masternode"
         : miner && masternode         ? "miner_masternode"
         : wallet                      ? "wallet"
         : miner                       ? "miner"
         : masternode                  ? "masternode"
                                       : "base";
}

std::string NodeRoleName(const Options& options, std::uint32_t node_index,
                         const NodeRoleTopology* runtime_topology) {
  if (runtime_topology == nullptr && node_index < options.node_roles.size()) {
    return options.node_roles[node_index];
  }
  return NodeRoleName(
      runtime_topology != nullptr ? *runtime_topology : options.topology,
      node_index);
}

std::string WalletAddressDetail(const WalletIdentity& wallet,
                                const WalletInitialization& initialization) {
  boost::json::object detail;
  detail["wallet_index"] = wallet.wallet_index;
  detail["node"] = wallet.node;
  detail["strategy"] =
      std::string(WalletInitializationStrategyName(initialization.strategy));
  detail["mode"] = std::string(WalletPrivacyModeName(initialization.mode));
  if (!wallet.address.empty()) {
    detail["address"] = wallet.address;
  }
  if (!wallet.funding_address.empty()) {
    detail["funding_address"] = wallet.funding_address;
  }
  return boost::json::serialize(detail);
}

boost::json::object RuntimeWalletIdentityJson(
    const WalletIdentity& wallet, const WalletInitialization& initialization) {
  return boost::json::object{
      {"wallet_index", wallet.wallet_index},
      {"node", wallet.node},
      {"node_id", wallet.node_id},
      {"mode", WalletPrivacyModeName(initialization.mode)},
      {"address", wallet.address},
      {"funding_address", wallet.funding_address},
  };
}

boost::json::object RuntimeMasternodeIdentityJson(
    const MasternodeIdentity& masternode) {
  return boost::json::object{
      {"node", masternode.node},
      {"node_id", masternode.node_id},
      {"funding_wallet_node_id", masternode.funding_wallet_node_id},
      {"pro_tx_hash", masternode.pro_tx_hash},
      {"service", masternode.service},
      {"collateral_address", masternode.collateral_address},
      {"owner_address", masternode.owner_address},
      {"operator_public_key", masternode.operator_public_key},
      {"voting_address", masternode.voting_address},
      {"payout_address", masternode.payout_address},
      {"collateral_hash", masternode.collateral_hash},
      {"collateral_index", masternode.collateral_index},
      {"state", masternode.state},
      {"status", masternode.status},
  };
}

boost::json::object RuntimeWalletGenerationDetail(
    const RuntimeWalletSnapshot& snapshot,
    std::span<const WalletIdentity> added_wallets,
    std::span<const WalletIdentity> removed_wallets) {
  boost::json::array added;
  added.reserve(added_wallets.size());
  boost::json::array removed;
  removed.reserve(removed_wallets.size());
  boost::json::array current;
  current.reserve(snapshot.wallets().size());
  std::set<std::string> affected_node_ids;
  boost::json::array node_ids;
  node_ids.reserve(added_wallets.size() + removed_wallets.size());
  for (const WalletIdentity& wallet : added_wallets) {
    added.push_back(RuntimeWalletIdentityJson(
        wallet, snapshot.registry().wallet_initialization()));
    if (affected_node_ids.insert(wallet.node_id).second) {
      node_ids.emplace_back(wallet.node_id);
    }
  }
  for (const WalletIdentity& wallet : removed_wallets) {
    removed.push_back(RuntimeWalletIdentityJson(
        wallet, snapshot.registry().wallet_initialization()));
    if (affected_node_ids.insert(wallet.node_id).second) {
      node_ids.emplace_back(wallet.node_id);
    }
  }
  for (const WalletIdentity& wallet : snapshot.wallets()) {
    current.push_back(RuntimeWalletIdentityJson(
        wallet, snapshot.registry().wallet_initialization()));
  }
  return boost::json::object{
      {"generation", snapshot.generation()},
      {"wallet_count", snapshot.wallets().size()},
      {"node_ids", std::move(node_ids)},
      {"wallets", std::move(added)},
      {"removed_wallets", std::move(removed)},
      {"current_wallets", std::move(current)},
  };
}

boost::json::object RuntimeRoleGenerationDetail(
    const RuntimeWalletSnapshot& snapshot, const RuntimeNodeSnapshot& nodes) {
  const NodeRoleTopology& topology = snapshot.registry().topology();
  if (topology.node_count != nodes.size()) {
    throw std::logic_error(
        "runtime role generation does not match node inventory");
  }
  boost::json::array miner_nodes;
  boost::json::array miner_node_ids;
  miner_nodes.reserve(topology.miner_nodes.size());
  miner_node_ids.reserve(topology.miner_nodes.size());
  for (const std::uint32_t node_index : topology.miner_nodes) {
    if (node_index >= nodes.size()) {
      throw std::logic_error(
          "runtime role generation contains an invalid miner node");
    }
    miner_nodes.emplace_back(node_index + 1U);
    miner_node_ids.emplace_back(nodes[node_index].config.id);
  }
  boost::json::array masternode_nodes;
  boost::json::array masternode_node_ids;
  boost::json::array masternodes;
  masternode_nodes.reserve(topology.masternode_nodes.size());
  masternode_node_ids.reserve(topology.masternode_nodes.size());
  masternodes.reserve(snapshot.masternodes().size());
  for (const std::uint32_t node_index : topology.masternode_nodes) {
    if (node_index >= nodes.size()) {
      throw std::logic_error(
          "runtime role generation contains an invalid masternode node");
    }
    masternode_nodes.emplace_back(node_index + 1U);
    masternode_node_ids.emplace_back(nodes[node_index].config.id);
  }
  for (const MasternodeIdentity& masternode : snapshot.masternodes()) {
    if (masternode.node == 0U || masternode.node > nodes.size() ||
        masternode.node_id != nodes[masternode.node - 1U].config.id) {
      throw std::logic_error(
          "runtime role generation contains an invalid masternode identity");
    }
    masternodes.push_back(RuntimeMasternodeIdentityJson(masternode));
  }
  boost::json::array node_roles;
  node_roles.reserve(nodes.size());
  for (std::size_t index = 0U; index < nodes.size(); ++index) {
    node_roles.emplace_back(boost::json::object{
        {"node", index + 1U},
        {"node_id", nodes[index].config.id},
        {"role", NodeRoleName(topology, static_cast<std::uint32_t>(index))},
    });
  }
  return boost::json::object{
      {"generation", snapshot.generation()},
      {"node_count", nodes.size()},
      {"miner_count", topology.miner_nodes.size()},
      {"miner_nodes", std::move(miner_nodes)},
      {"miner_node_ids", std::move(miner_node_ids)},
      {"masternode_count", topology.masternode_nodes.size()},
      {"masternode_nodes", std::move(masternode_nodes)},
      {"masternode_node_ids", std::move(masternode_node_ids)},
      {"masternodes", std::move(masternodes)},
      {"node_roles", std::move(node_roles)},
  };
}

}  // namespace bbp::simulator_app_internal
