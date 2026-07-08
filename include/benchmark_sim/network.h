#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <sys/types.h>

namespace bsim {

struct LinkInfo {
  int index = 0;
  std::string name;
  bool up = false;
};

struct AddressInfo {
  int if_index = 0;
  std::string if_name;
  std::string address;
  std::uint8_t prefix_len = 0;
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

struct AddressProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::string assigned_address;
  std::uint8_t assigned_prefix_len = 0;
  std::vector<LinkInfo> parent_after_move;
  std::vector<LinkInfo> namespace_links_after_address;
  std::vector<AddressInfo> namespace_addresses;
  std::vector<LinkInfo> parent_after_delete;
};

std::vector<LinkInfo> ListNetworkLinks();
std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd);
std::vector<AddressInfo> ListIpv4Addresses();
std::vector<AddressInfo> ListIpv4AddressesInNamespace(int netns_fd);
NetworkNamespaceProbe ProbeIsolatedNetworkNamespace();
void CreateVethPair(const std::string& host_name, const std::string& peer_name);
void DeleteLink(const std::string& name);
void MoveLinkToNamespace(const std::string& name, int netns_fd);
void SetLinkUp(const std::string& name, bool up);
void AddIpv4Address(const std::string& if_name, const std::string& address,
                    std::uint8_t prefix_len);
VethProbe ProbeVethPair();
AddressProbe ProbeIpv4AddressAssignment();

}  // namespace bsim
