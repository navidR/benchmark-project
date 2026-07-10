#pragma once

#include <cstdint>

namespace bbp {

struct FreezeRequest {
  std::uint32_t node_index = 0;
  std::uint32_t duration_ms = 0;
};

}  // namespace bbp
