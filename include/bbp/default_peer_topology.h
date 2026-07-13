#pragma once

#include <cstdint>
#include <vector>

namespace bbp {

std::vector<std::uint32_t> DefaultStartupPeerIndexes(std::uint32_t node_count,
                                                     std::uint32_t node_index);

}  // namespace bbp
