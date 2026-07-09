#pragma once

#include <cstdint>

#include "benchmark_sim/simulator/resource_limit_patch.h"

namespace bsim {

struct ResourceLimitUpdateWorkload {
  std::uint32_t node = 1;
  ResourceLimitPatch patch;
};

}  // namespace bsim
