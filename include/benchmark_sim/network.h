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

struct VethProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::vector<LinkInfo> parent_before;
  std::vector<LinkInfo> parent_after_create;
  std::vector<LinkInfo> parent_after_move;
  std::vector<LinkInfo> namespace_after_move;
  std::vector<LinkInfo> parent_after_delete;
};

std::vector<LinkInfo> ListNetworkLinks();
std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd);
NetworkNamespaceProbe ProbeIsolatedNetworkNamespace();
void CreateVethPair(const std::string& host_name, const std::string& peer_name);
void DeleteLink(const std::string& name);
void MoveLinkToNamespace(const std::string& name, int netns_fd);
void SetLinkUp(const std::string& name, bool up);
VethProbe ProbeVethPair();

}  // namespace bsim
