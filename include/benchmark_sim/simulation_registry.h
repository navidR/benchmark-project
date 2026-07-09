#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace bsim {

enum class PeerConnectivityMode {
  kFixedCount,
  kAllPeers,
};

struct PeerConnectivityPolicy {
  uint32_t node = 0;
  PeerConnectivityMode mode = PeerConnectivityMode::kFixedCount;
  uint32_t max_peer_count = 1;
};

struct NodeRoleTopology {
  bool configured = false;
  uint32_t node_count = 0;
  uint32_t wallet_node_count = 0;
  uint32_t miner_node_count = 0;
  bool allow_miner_wallet_overlap = false;
  std::vector<uint32_t> wallet_nodes;
  std::vector<uint32_t> miner_nodes;
  std::vector<PeerConnectivityPolicy> peer_connectivity;
};

enum class WalletInitializationStrategy {
  kDriverRpc,
};

enum class WalletPrivacyMode {
  kPublic,
  kPrivate,
};

struct WalletInitialization {
  WalletInitializationStrategy strategy =
      WalletInitializationStrategy::kDriverRpc;
  WalletPrivacyMode mode = WalletPrivacyMode::kPublic;
  std::string seed;
};

struct WalletIdentity {
  uint32_t wallet_index = 0;
  uint32_t node = 1;
  std::string address;
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

  uint32_t MinerNodeForWalletIndex(size_t wallet_index) const;

  void AddWallet(WalletIdentity wallet);
  WalletIdentity& MutableWalletByIndex(size_t wallet_index);
  const WalletIdentity& WalletByIndex(size_t wallet_index) const;

 private:
  NodeRoleTopology topology_;
  WalletInitialization wallet_initialization_;
  std::vector<WalletIdentity> wallets_;
};

}  // namespace bsim
