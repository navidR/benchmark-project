#pragma once

#include <filesystem>
#include <optional>

#include "bbp/chain_network.h"

namespace bbp {

struct ScenarioNodeConfig {
  std::optional<std::filesystem::path> binary;
  std::optional<std::filesystem::path> data_dir;
  ChainNetwork network = ChainNetwork::kRegtest;
};

}  // namespace bbp
