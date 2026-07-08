#pragma once

#include <string>
#include <vector>

#include <sys/types.h>

namespace bsim {

struct LinkInfo {
  int index = 0;
  std::string name;
  bool up = false;
};

struct NetworkNamespaceProbe {
  pid_t helper_pid = -1;
  std::vector<LinkInfo> parent_links;
  std::vector<LinkInfo> namespace_links;
};

std::vector<LinkInfo> ListNetworkLinks();
std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd);
NetworkNamespaceProbe ProbeIsolatedNetworkNamespace();

}  // namespace bsim
