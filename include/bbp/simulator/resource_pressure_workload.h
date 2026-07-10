#pragma once

#include <cstdint>

#include "bbp/simulator/resource_limit_patch.h"

namespace bbp {

struct ResourcePressureWorkload {
  std::uint32_t node = 1;
  ResourceLimitPatch patch;
  std::uint32_t duration_ms = 0;
};

}  // namespace bbp
