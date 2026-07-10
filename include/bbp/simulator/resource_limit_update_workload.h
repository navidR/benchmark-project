#pragma once

#include <cstdint>

#include "bbp/simulator/resource_limit_patch.h"

namespace bbp {

struct ResourceLimitUpdateWorkload {
  std::uint32_t node = 1;
  ResourceLimitPatch patch;
};

}  // namespace bbp
