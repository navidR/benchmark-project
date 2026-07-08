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

struct RouteInfo {
  std::string destination;
  std::uint8_t prefix_len = 0;
  int oif_index = 0;
  std::string oif_name;
  std::string gateway;
  std::uint32_t table = 0;
  std::uint8_t protocol = 0;
  std::uint8_t scope = 0;
  std::uint8_t type = 0;
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
  std::vector<AddressInfo> namespace_addresses_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct RouteProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::string assigned_address;
  std::uint8_t assigned_prefix_len = 0;
  std::string route_destination;
  std::uint8_t route_prefix_len = 0;
  std::vector<LinkInfo> namespace_links_after_route;
  std::vector<AddressInfo> namespace_addresses;
  std::vector<RouteInfo> namespace_routes;
  std::vector<AddressInfo> namespace_addresses_after_delete;
  std::vector<RouteInfo> namespace_routes_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

std::vector<LinkInfo> ListNetworkLinks();
std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd);
std::vector<AddressInfo> ListIpv4Addresses();
std::vector<AddressInfo> ListIpv4AddressesInNamespace(int netns_fd);
std::vector<RouteInfo> ListIpv4Routes();
std::vector<RouteInfo> ListIpv4RoutesInNamespace(int netns_fd);
NetworkNamespaceProbe ProbeIsolatedNetworkNamespace();
void CreateVethPair(const std::string& host_name, const std::string& peer_name);
void DeleteLink(const std::string& name);
void MoveLinkToNamespace(const std::string& name, int netns_fd);
void SetLinkUp(const std::string& name, bool up);
void AddIpv4Address(const std::string& if_name, const std::string& address,
                    std::uint8_t prefix_len);
void DeleteIpv4Address(const std::string& if_name, const std::string& address,
                       std::uint8_t prefix_len);
void AddIpv4Route(const std::string& if_name, const std::string& destination,
                  std::uint8_t prefix_len, const std::string& gateway = "");
void DeleteIpv4Route(const std::string& if_name, const std::string& destination,
                     std::uint8_t prefix_len,
                     const std::string& gateway = "");
VethProbe ProbeVethPair();
AddressProbe ProbeIpv4AddressAssignment();
RouteProbe ProbeIpv4RouteAssignment();

}  // namespace bsim
