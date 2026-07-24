#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/peer_connectivity_policy.h"

namespace bbp {

struct NodeRoleTopology {
  bool configured = false;
  uint32_t node_count = 0;
  uint32_t wallet_node_count = 0;
  uint32_t miner_node_count = 0;
  uint32_t masternode_node_count = 0;
  bool allow_miner_wallet_overlap = false;
  std::vector<uint32_t> wallet_nodes;
  std::vector<uint32_t> miner_nodes;
  std::vector<uint32_t> masternode_nodes;
  PeerTopologyConfig peer_topology;
  std::vector<PeerConnectivityPolicy> peer_connectivity;
};

enum class WalletInitializationStrategy {
  kDriverRpc,
};

constexpr std::string_view WalletInitializationStrategyName(
    WalletInitializationStrategy strategy) {
  switch (strategy) {
    case WalletInitializationStrategy::kDriverRpc:
      return "driver_rpc";
  }
  return "unknown";
}

constexpr std::optional<WalletInitializationStrategy>
WalletInitializationStrategyFromName(std::string_view name) {
  if (name == "driver_rpc") {
    return WalletInitializationStrategy::kDriverRpc;
  }
  return std::nullopt;
}

enum class WalletPrivacyMode {
  kPublic,
  kPrivate,
};

constexpr std::string_view WalletPrivacyModeName(WalletPrivacyMode mode) {
  switch (mode) {
    case WalletPrivacyMode::kPublic:
      return "public";
    case WalletPrivacyMode::kPrivate:
      return "private";
  }
  return "unknown";
}

constexpr std::optional<WalletPrivacyMode> WalletPrivacyModeFromName(
    std::string_view name) {
  if (name == "public") {
    return WalletPrivacyMode::kPublic;
  }
  if (name == "private") {
    return WalletPrivacyMode::kPrivate;
  }
  return std::nullopt;
}

struct WalletInitialization {
  WalletInitializationStrategy strategy =
      WalletInitializationStrategy::kDriverRpc;
  WalletPrivacyMode mode = WalletPrivacyMode::kPublic;
};

struct WalletIdentity {
  uint32_t wallet_index = 0;
  uint32_t node = 1;
  std::string node_id;
  std::string address;
  std::string funding_address;
};

struct MasternodeIdentity {
  uint32_t node = 1U;
  std::string node_id;
  std::string funding_wallet_node_id;
  std::string pro_tx_hash;
  std::string service;
  std::string collateral_address;
  std::string owner_address;
  std::string operator_public_key;
  std::string operator_secret_key;
  std::string voting_address;
  std::string payout_address;
  std::string collateral_hash;
  uint32_t collateral_index = 0U;
  std::string state;
  std::string status;
};

class SimulationRegistry {
 public:
  static SimulationRegistry FromTopology(
      const NodeRoleTopology& topology,
      const WalletInitialization& wallet_initialization);

  const NodeRoleTopology& topology() const { return topology_; }
  const WalletInitialization& wallet_initialization() const {
    return wallet_initialization_;
  }
  const std::vector<WalletIdentity>& wallets() const { return wallets_; }
  const std::vector<uint32_t>& miner_nodes() const {
    return topology_.miner_nodes;
  }
  const std::vector<MasternodeIdentity>& masternodes() const {
    return masternodes_;
  }

  void AddWallet(WalletIdentity wallet);
  void AddMinerNode(uint32_t node_index);
  void RemoveMinerNodes(const std::vector<uint32_t>& node_indexes);
  void AddMasternode(MasternodeIdentity masternode);
  void RemoveMasternodeNodes(const std::vector<uint32_t>& node_indexes);
  void SetRuntimeNodeCount(uint32_t node_count);
  SimulationRegistry RemapRuntimeNodes(
      const std::vector<std::optional<std::uint32_t>>& old_to_new,
      PeerTopologyConfig peer_topology) const;
  WalletIdentity& MutableWalletByIndex(size_t wallet_index);
  const WalletIdentity& WalletByIndex(size_t wallet_index) const;

 private:
  NodeRoleTopology topology_;
  WalletInitialization wallet_initialization_;
  std::vector<WalletIdentity> wallets_;
  std::vector<MasternodeIdentity> masternodes_;
};

}  // namespace bbp
