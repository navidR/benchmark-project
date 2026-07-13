#include "bbp/default_peer_topology.h"

#include <stdexcept>

namespace bbp {

std::vector<std::uint32_t> DefaultStartupPeerIndexes(std::uint32_t node_count,
                                                     std::uint32_t node_index) {
  if (node_count == 0U || node_index >= node_count) {
    throw std::runtime_error("default peer topology node is out of range");
  }

  std::vector<std::uint32_t> peers;
  peers.reserve(node_count - 1U);
  for (std::uint32_t peer_index = 0; peer_index < node_count; ++peer_index) {
    if (peer_index != node_index) {
      peers.push_back(peer_index);
    }
  }
  return peers;
}

}  // namespace bbp
