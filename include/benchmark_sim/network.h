#pragma once

#include <string>
#include <vector>

namespace bsim {

struct LinkInfo {
  int index = 0;
  std::string name;
  bool up = false;
};

std::vector<LinkInfo> ListNetworkLinks();

}  // namespace bsim
