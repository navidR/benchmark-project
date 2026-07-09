#pragma once

#include <cstdint>

namespace bsim {

struct DisconnectPeerWorkload {
  std::uint32_t node = 1;
  std::uint32_t peer = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bsim
