#include "bbp/simulation_network_address_plan.h"

#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/network_v4.hpp>
#include <boost/system/error_code.hpp>
#include <limits>
#include <stdexcept>

#include "bbp/default_peer_topology.h"

namespace bbp {
namespace {

constexpr std::uint32_t kPoolBase = 0x0AD20000U;
constexpr std::uint32_t kPoolPrefixLength = 16U;
constexpr std::uint32_t kRunPrefixLength = 26U;
constexpr std::uint32_t kNodePrefixLength = 30U;
constexpr std::uint32_t kAddressesPerRun = 1U << (32U - kRunPrefixLength);
constexpr std::uint32_t kRunSlotCount =
    1U << (kRunPrefixLength - kPoolPrefixLength);
constexpr std::uint32_t kAddressesPerNode = 1U << (32U - kNodePrefixLength);
constexpr std::uint32_t kMaximumNodeCount =
    kAddressesPerRun / kAddressesPerNode;

std::uint32_t StableRunHash(std::string_view run_id) {
  std::uint32_t hash = 2166136261U;
  for (const unsigned char character : run_id) {
    hash ^= character;
    hash *= 16777619U;
  }
  return hash;
}

std::uint32_t PrefixMask(std::uint32_t prefix_length) {
  if (prefix_length == 0U) {
    return 0U;
  }
  return std::numeric_limits<std::uint32_t>::max() << (32U - prefix_length);
}

bool RangeOverlapsRoute(std::uint32_t range_base, const RouteInfo& route) {
  if (route.prefix_len == 0U || route.prefix_len > 32U) {
    return false;
  }
  boost::system::error_code error;
  const boost::asio::ip::address_v4 route_address =
      boost::asio::ip::make_address_v4(route.destination, error);
  if (error) {
    throw std::runtime_error("invalid IPv4 route destination from rtnetlink: " +
                             route.destination);
  }
  const std::uint32_t route_mask = PrefixMask(route.prefix_len);
  const std::uint32_t route_start = route_address.to_uint() & route_mask;
  const std::uint32_t route_end = route_start | ~route_mask;
  const std::uint32_t range_end = range_base + kAddressesPerRun - 1U;
  return range_base <= route_end && route_start <= range_end;
}

bool RangeIsAvailable(std::uint32_t range_base,
                      const std::vector<RouteInfo>& routes) {
  for (const RouteInfo& route : routes) {
    if (RangeOverlapsRoute(range_base, route)) {
      return false;
    }
  }
  return true;
}

void RequireNodeCount(std::uint32_t node_count) {
  if (node_count == 0U || node_count > kMaximumNodeCount) {
    throw std::runtime_error("isolated network address plan supports 1.." +
                             std::to_string(kMaximumNodeCount) + " nodes");
  }
}

}  // namespace

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::Allocate(
    std::string_view run_id, std::uint32_t node_count,
    const std::vector<RouteInfo>& routes) {
  RequireNodeCount(node_count);
  const std::uint32_t first_slot = StableRunHash(run_id) % kRunSlotCount;
  for (std::uint32_t offset = 0; offset < kRunSlotCount; ++offset) {
    const std::uint32_t slot = (first_slot + offset) % kRunSlotCount;
    const std::uint32_t base = kPoolBase + slot * kAddressesPerRun;
    if (RangeIsAvailable(base, routes)) {
      return SimulationNetworkAddressPlan(base, node_count);
    }
  }
  throw std::runtime_error(
      "no non-overlapping isolated simulation network range is available in "
      "10.210.0.0/16");
}

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::FromCidr(
    std::string_view cidr, std::uint32_t node_count) {
  RequireNodeCount(node_count);
  boost::system::error_code error;
  const boost::asio::ip::network_v4 network =
      boost::asio::ip::make_network_v4(cidr, error);
  if (error || network.prefix_length() != kRunPrefixLength ||
      network.address() != network.network() ||
      network.network().to_uint() < kPoolBase ||
      network.network().to_uint() >= kPoolBase + (1U << 16U)) {
    throw std::runtime_error(
        "isolated simulation network range must be an aligned "
        "10.210.0.0/16 subnet with /26 prefix");
  }
  return SimulationNetworkAddressPlan(network.network().to_uint(), node_count);
}

SimulationNetworkAddressPlan::SimulationNetworkAddressPlan(
    std::uint32_t base_address, std::uint32_t node_count)
    : base_address_(base_address), node_count_(node_count) {}

std::string SimulationNetworkAddressPlan::Cidr() const {
  return boost::asio::ip::network_v4(boost::asio::ip::address_v4(base_address_),
                                     kRunPrefixLength)
      .to_string();
}

std::string SimulationNetworkAddressPlan::HostAddress(
    std::uint32_t node_index) const {
  if (node_index >= node_count_) {
    throw std::out_of_range("network address plan node index is out of range");
  }
  return boost::asio::ip::address_v4(base_address_ +
                                     node_index * kAddressesPerNode + 1U)
      .to_string();
}

std::string SimulationNetworkAddressPlan::NodeAddress(
    std::uint32_t node_index) const {
  if (node_index >= node_count_) {
    throw std::out_of_range("network address plan node index is out of range");
  }
  return boost::asio::ip::address_v4(base_address_ +
                                     node_index * kAddressesPerNode + 2U)
      .to_string();
}

std::uint8_t SimulationNetworkAddressPlan::NodePrefixLength() const {
  return static_cast<std::uint8_t>(kNodePrefixLength);
}

std::vector<DirectionalNetworkPolicy> ResolveDirectionalNetworkPolicies(
    const PeerTopologyConfig& topology,
    const SimulationNetworkAddressPlan& address_plan, std::uint32_t node_count,
    std::uint32_t node_index) {
  if (node_index >= node_count) {
    throw std::out_of_range("directional policy node index is out of range");
  }

  std::vector<DirectionalNetworkPolicy> policies;
  std::uint32_t outgoing_band = 0U;
  for (const ResolvedPeerTopologyEdge& edge :
       ResolvePeerTopologyEdges(topology, node_count)) {
    if (edge.from != node_index) {
      continue;
    }
    ++outgoing_band;
    if (outgoing_band > 15U) {
      throw std::runtime_error(
          "directional topology policy exceeds the 15-peer band limit");
    }
    if (!edge.condition) {
      continue;
    }
    policies.push_back(DirectionalNetworkPolicy{
        .band = outgoing_band,
        .destination_address = address_plan.NodeAddress(edge.to),
        .condition = *edge.condition,
    });
  }
  return policies;
}

}  // namespace bbp
