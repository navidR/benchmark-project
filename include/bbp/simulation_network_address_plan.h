#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/network.h"

namespace bbp {

struct PeerTopologyConfig;

class SimulationNetworkAddressPlan {
 public:
  static std::string CanonicalPoolCidr(std::string_view pool_cidr);
  static std::uint32_t PoolLinkCapacity(std::string_view pool_cidr);
  static SimulationNetworkAddressPlan Allocate(
      std::string_view run_id, std::uint32_t node_count,
      const std::vector<RouteInfo>& routes,
      const std::vector<AddressInfo>& addresses = {},
      std::string_view pool_cidr = "10.0.0.0/8");
  // Constructs the first node_count /31 links without kernel collision checks.
  static SimulationNetworkAddressPlan FromCidr(std::string_view pool_cidr,
                                               std::uint32_t node_count);
  static SimulationNetworkAddressPlan FromSerialized(
      const boost::json::object& allocation);

  SimulationNetworkAddressPlan Expanded(
      std::uint32_t new_capacity, const std::vector<RouteInfo>& routes,
      const std::vector<AddressInfo>& addresses = {}) const;
  std::uint32_t capacity() const;
  std::string PoolCidr() const;
  std::vector<std::string> LinkCidrs() const;
  boost::json::object ToSerialized() const;
  // Compatibility spelling: a non-contiguous plan has a pool, not a run range.
  std::string Cidr() const;
  std::string HostAddress(std::uint32_t node_index) const;
  std::string NodeAddress(std::uint32_t node_index) const;
  std::uint8_t NodePrefixLength() const;
  void RequireNodeSlotsAvailable(const std::vector<std::uint32_t>& node_slots,
                                 const std::vector<RouteInfo>& routes) const;
  void RequireNodeSlotsAvailable(
      const std::vector<std::uint32_t>& node_slots,
      const std::vector<RouteInfo>& routes,
      const std::vector<AddressInfo>& addresses) const;

  bool operator==(const SimulationNetworkAddressPlan&) const = default;

 private:
  SimulationNetworkAddressPlan(std::uint32_t pool_base,
                               std::uint8_t pool_prefix_length);
  SimulationNetworkAddressPlan ExpandedFrom(
      std::uint32_t new_capacity, const std::vector<RouteInfo>& routes,
      const std::vector<AddressInfo>& addresses,
      std::uint64_t first_slot) const;

  std::uint32_t pool_base_ = 0U;
  std::uint8_t pool_prefix_length_ = 0U;
  std::vector<std::uint32_t> link_bases_;
};

std::vector<DirectionalNetworkPolicy> ResolveDirectionalNetworkPolicies(
    const PeerTopologyConfig& topology,
    const SimulationNetworkAddressPlan& address_plan, std::uint32_t node_count,
    std::uint32_t node_index);

}  // namespace bbp
