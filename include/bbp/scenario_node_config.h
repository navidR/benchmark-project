#pragma once

#include <filesystem>
#include <optional>

#include "bbp/chain_extra_args.h"
#include "bbp/chain_network.h"
#include "bbp/node_lifecycle_policy.h"
#include "bbp/simulation_registry.h"

namespace bbp {

struct ScenarioNodeWalletConfig {
  bool enabled = false;
  WalletInitializationStrategy strategy =
      WalletInitializationStrategy::kDriverRpc;
  WalletPrivacyMode mode = WalletPrivacyMode::kPublic;
};

struct ScenarioNodeConfig {
  std::optional<std::filesystem::path> binary;
  std::optional<std::filesystem::path> data_dir;
  NodeLifecyclePolicy lifecycle;
  ChainNetwork network = ChainNetwork::kRegtest;
  ChainExtraArgs extra_args;
  std::optional<ScenarioNodeWalletConfig> wallet;
};

}  // namespace bbp
