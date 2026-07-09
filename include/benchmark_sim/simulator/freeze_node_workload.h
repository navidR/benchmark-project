#pragma once

#include <cstdint>

namespace bsim {

struct FreezeNodeWorkload {
  std::uint32_t node = 1;
  std::uint32_t duration_ms = 0;
};

}  // namespace bsim
