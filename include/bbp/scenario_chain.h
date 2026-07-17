#pragma once

#include <filesystem>

#include "bbp/chain_kind.h"

namespace bbp {

struct ScenarioChain {
  ChainKind driver = ChainKind::kFiro;
  std::filesystem::path default_binary;
};

}  // namespace bbp
