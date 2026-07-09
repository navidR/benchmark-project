#pragma once

#include <cstdint>

namespace bsim {

struct FreezeRequest {
  std::uint32_t node_index = 0;
  std::uint32_t duration_ms = 0;
};

}  // namespace bsim
