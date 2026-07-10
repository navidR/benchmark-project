#pragma once

#include <cstdint>

namespace bbp {

struct BlockGenerationWorkload {
  std::uint32_t node = 1;
  std::uint32_t count = 0;
  std::uint32_t sync_timeout_sec = 30;
};

}  // namespace bbp
