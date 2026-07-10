#pragma once

#include <cstdint>

namespace bbp {

struct WaitUntilHeightWorkload {
  std::uint32_t node = 1;
  std::uint64_t height = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
