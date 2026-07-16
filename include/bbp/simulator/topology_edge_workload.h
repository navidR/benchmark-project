#pragma once

#include <cstdint>
#include <optional>

#include "bbp/network.h"

namespace bbp {

struct TopologyEdgeWorkload {
  std::uint32_t from = 0;
  std::uint32_t to = 0;
  std::optional<NetworkCondition> condition = std::nullopt;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
