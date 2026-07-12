#include "bbp/peer_count_policy.h"

#include <stdexcept>

namespace bbp {

PeerCountPolicy::PeerCountPolicy(std::uint32_t minimum, std::uint32_t maximum)
    : minimum_(minimum), maximum_(maximum) {
  if (minimum > maximum) {
    throw std::runtime_error(
        "minimum peer count must not exceed maximum peer count");
  }
}

}  // namespace bbp
