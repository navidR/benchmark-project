#pragma once

#include <cstdint>
#include <string>

namespace bbp {

struct RestartNodeWorkload {
  std::uint32_t node = 1;
  std::string node_id;
};

}  // namespace bbp
