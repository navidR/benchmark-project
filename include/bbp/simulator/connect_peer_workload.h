#pragma once

#include <cstdint>

namespace bbp {

struct ConnectPeerWorkload {
  std::uint32_t node = 1;
  std::uint32_t peer = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
