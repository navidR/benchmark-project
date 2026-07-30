#pragma once

#include <cstdint>
#include <string>

namespace bbp {

struct WaitUntilHeightWorkload {
  std::uint32_t node = 1;
  std::string node_id;
  std::uint64_t height = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
