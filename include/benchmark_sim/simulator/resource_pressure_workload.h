#pragma once

#include <cstdint>

#include "benchmark_sim/simulator/resource_limit_patch.h"

namespace bsim {

struct ResourcePressureWorkload {
  std::uint32_t node = 1;
  ResourceLimitPatch patch;
  std::uint32_t duration_ms = 0;
};

}  // namespace bsim
