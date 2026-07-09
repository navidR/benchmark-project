#pragma once

#include <cstdint>
#include <vector>

namespace bsim {

struct NetworkPartitionRule {
  std::vector<std::uint32_t> group_a;
  std::vector<std::uint32_t> group_b;
};

}  // namespace bsim
