#pragma once

#include <cstdint>
#include <string>

namespace bbp {

struct NetworkBlockRule {
  std::uint32_t node_index = 0;
  std::string src_address;
  std::string dst_address;
  std::uint16_t dst_port = 0;
  std::uint32_t handle = 0;
};

}  // namespace bbp
