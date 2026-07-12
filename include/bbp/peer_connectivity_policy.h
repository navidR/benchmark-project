#pragma once

#include <cstdint>

#include "bbp/peer_count_policy.h"

namespace bbp {

enum class PeerConnectivityMode {
  kFixedCount,
  kAllPeers,
};

struct PeerConnectivityPolicy {
  std::uint32_t node = 0;
  PeerConnectivityMode mode = PeerConnectivityMode::kFixedCount;
  PeerCountPolicy peer_count{1U, 1U};
};

}  // namespace bbp
