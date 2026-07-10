#pragma once

#include <cstdint>
#include <vector>

namespace bbp {

struct NetworkPartitionRule {
  std::vector<std::uint32_t> group_a;
  std::vector<std::uint32_t> group_b;
};

}  // namespace bbp
