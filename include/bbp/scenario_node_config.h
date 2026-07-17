#pragma once

#include <filesystem>
#include <optional>

namespace bbp {

struct ScenarioNodeConfig {
  std::optional<std::filesystem::path> binary;
  std::optional<std::filesystem::path> data_dir;
};

}  // namespace bbp
