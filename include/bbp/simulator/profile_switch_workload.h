#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace bbp {

struct ProfileSwitchWorkload {
  std::vector<std::uint32_t> nodes;
  std::vector<std::string> node_ids;
  std::string profile;
};

}  // namespace bbp
