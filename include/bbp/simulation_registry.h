#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/peer_connectivity_policy.h"

namespace bbp {

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

  void AddWallet(WalletIdentity wallet);
  WalletIdentity& MutableWalletByIndex(size_t wallet_index);
  const WalletIdentity& WalletByIndex(size_t wallet_index) const;

 private:
  NodeRoleTopology topology_;
  WalletInitialization wallet_initialization_;
  std::vector<WalletIdentity> wallets_;
};

}  // namespace bbp
