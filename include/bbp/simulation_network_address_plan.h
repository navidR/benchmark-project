#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/network.h"

namespace bbp {

struct PeerTopologyConfig;

class SimulationNetworkAddressPlan {
 public:
  static SimulationNetworkAddressPlan Allocate(
      std::string_view run_id, std::uint32_t node_count,
      const std::vector<RouteInfo>& routes);
  static SimulationNetworkAddressPlan FromCidr(std::string_view cidr,
                                               std::uint32_t node_count);

  std::string Cidr() const;
  std::string HostAddress(std::uint32_t node_index) const;
  std::string NodeAddress(std::uint32_t node_index) const;
  std::uint8_t NodePrefixLength() const;

 private:
  SimulationNetworkAddressPlan(std::uint32_t base_address,
                               std::uint32_t node_count);

  std::uint32_t base_address_ = 0;
  std::uint32_t node_count_ = 0;
};

std::vector<DirectionalNetworkPolicy> ResolveDirectionalNetworkPolicies(
    const PeerTopologyConfig& topology,
    const SimulationNetworkAddressPlan& address_plan, std::uint32_t node_count,
    std::uint32_t node_index);

}  // namespace bbp
