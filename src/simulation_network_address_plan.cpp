#include "bbp/simulation_network_address_plan.h"

#include <algorithm>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/network_v4.hpp>
#include <boost/json/array.hpp>
#include <boost/system/error_code.hpp>
#include <iterator>
#include <set>
#include <stdexcept>
#include <utility>

#include "bbp/default_peer_topology.h"
#include "bbp/runtime_peer_topology.h"

namespace bbp {
namespace {

constexpr std::uint8_t kNodePrefixLength = 31U;
constexpr std::uint64_t kAddressesPerNode = 2U;
using Interval = std::pair<std::uint64_t, std::uint64_t>;

std::uint32_t StableRunHash(std::string_view run_id) {
  std::uint32_t hash = 2166136261U;
  for (const unsigned char character : run_id) {
    hash ^= character;
    hash *= 16777619U;
  }
  return hash;
}

boost::asio::ip::network_v4 ParseNetwork(std::string_view cidr) {
  boost::system::error_code error;
  const auto network = boost::asio::ip::make_network_v4(cidr, error);
  if (error || network.address() != network.network()) {
    throw std::runtime_error(
        "isolated network CIDR must be an aligned IPv4 network: " +
        std::string(cidr));
  }
  return network;
}

boost::asio::ip::network_v4 ParsePool(std::string_view cidr) {
  boost::system::error_code error;
  const auto network = boost::asio::ip::make_network_v4(cidr, error);
  if (error) {
    throw std::runtime_error("isolated network pool must be an IPv4 CIDR: " +
                             std::string(cidr));
  }
  if (network.prefix_length() > kNodePrefixLength) {
    throw std::runtime_error("isolated network pool has no /31 node links: " +
                             std::string(cidr));
  }
  return network.canonical();
}

std::uint32_t ParseKernelAddress(std::string_view address,
                                 std::string_view source) {
  boost::system::error_code error;
  const auto parsed = boost::asio::ip::make_address_v4(address, error);
  if (error) {
    throw std::runtime_error("invalid IPv4 " + std::string(source) +
                             " from rtnetlink: " + std::string(address));
  }
  return parsed.to_uint();
}

std::uint64_t AddressCount(std::uint8_t prefix_length) {
  return std::uint64_t{1U} << (32U - prefix_length);
}

void AddBlockedRange(std::vector<Interval>& blocked, std::uint64_t first,
                     std::uint64_t end, std::uint64_t pool_first,
                     std::uint64_t pool_end) {
  first = std::max(first, pool_first);
  end = std::min(end, pool_end);
  if (first < end) {
    blocked.emplace_back(
        (first - pool_first) / kAddressesPerNode,
        (end - pool_first + kAddressesPerNode - 1U) / kAddressesPerNode);
  }
}

std::vector<Interval> BlockedLinks(std::uint32_t pool_base,
                                   std::uint8_t pool_prefix,
                                   const std::vector<RouteInfo>& routes,
                                   const std::vector<AddressInfo>& addresses,
                                   const std::vector<std::uint32_t>& reserved) {
  const std::uint64_t pool_end =
      std::uint64_t{pool_base} + AddressCount(pool_prefix);
  std::vector<Interval> blocked;
  for (const RouteInfo& route : routes) {
    // A default route does not reserve every address reachable through it.
    if (route.prefix_len == 0U || route.prefix_len > 32U) {
      continue;
    }
    const std::uint64_t size = AddressCount(route.prefix_len);
    const std::uint64_t first = std::uint64_t{ParseKernelAddress(
                                    route.destination, "route destination")} &
                                ~(size - 1U);
    AddBlockedRange(blocked, first, first + size, pool_base, pool_end);
  }
  for (const AddressInfo& address : addresses) {
    const std::uint64_t first = ParseKernelAddress(address.address, "address");
    AddBlockedRange(blocked, first, first + 1U, pool_base, pool_end);
  }
  for (const std::uint32_t first : reserved) {
    AddBlockedRange(blocked, first, std::uint64_t{first} + kAddressesPerNode,
                    pool_base, pool_end);
  }
  std::sort(blocked.begin(), blocked.end());
  std::vector<Interval> merged;
  for (const auto& interval : blocked) {
    if (!merged.empty() && interval.first <= merged.back().second) {
      merged.back().second = std::max(merged.back().second, interval.second);
    } else {
      merged.push_back(interval);
    }
  }
  return merged;
}

bool IsBlocked(std::uint64_t slot, const std::vector<Interval>& blocked) {
  const auto next =
      std::upper_bound(blocked.begin(), blocked.end(), slot,
                       [](std::uint64_t value, const Interval& interval) {
                         return value < interval.first;
                       });
  return next != blocked.begin() && slot < std::prev(next)->second;
}

}  // namespace

std::string SimulationNetworkAddressPlan::CanonicalPoolCidr(
    std::string_view pool_cidr) {
  return ParsePool(pool_cidr).to_string();
}

std::uint32_t SimulationNetworkAddressPlan::PoolLinkCapacity(
    std::string_view pool_cidr) {
  return static_cast<std::uint32_t>(AddressCount(static_cast<std::uint8_t>(
                                        ParsePool(pool_cidr).prefix_length())) /
                                    kAddressesPerNode);
}

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::Allocate(
    std::string_view run_id, std::uint32_t node_count,
    const std::vector<RouteInfo>& routes,
    const std::vector<AddressInfo>& addresses, std::string_view pool_cidr) {
  const auto pool = ParsePool(pool_cidr);
  const SimulationNetworkAddressPlan empty(
      pool.network().to_uint(),
      static_cast<std::uint8_t>(pool.prefix_length()));
  const std::uint64_t slot_count =
      AddressCount(empty.pool_prefix_length_) / kAddressesPerNode;
  return empty.ExpandedFrom(node_count, routes, addresses,
                            StableRunHash(run_id) % slot_count);
}

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::FromCidr(
    std::string_view pool_cidr, std::uint32_t node_count) {
  const auto pool = ParsePool(pool_cidr);
  return SimulationNetworkAddressPlan(
             pool.network().to_uint(),
             static_cast<std::uint8_t>(pool.prefix_length()))
      .ExpandedFrom(node_count, {}, {}, 0U);
}

SimulationNetworkAddressPlan::SimulationNetworkAddressPlan(
    std::uint32_t pool_base, std::uint8_t pool_prefix_length)
    : pool_base_(pool_base), pool_prefix_length_(pool_prefix_length) {}

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::Expanded(
    std::uint32_t new_capacity, const std::vector<RouteInfo>& routes,
    const std::vector<AddressInfo>& addresses) const {
  const std::uint64_t slot_count =
      AddressCount(pool_prefix_length_) / kAddressesPerNode;
  const std::uint64_t first_slot =
      link_bases_.empty() ? 0U
                          : ((std::uint64_t{link_bases_.back()} - pool_base_) /
                                 kAddressesPerNode +
                             1U) %
                                slot_count;
  return ExpandedFrom(new_capacity, routes, addresses, first_slot);
}

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::ExpandedFrom(
    std::uint32_t new_capacity, const std::vector<RouteInfo>& routes,
    const std::vector<AddressInfo>& addresses, std::uint64_t first_slot) const {
  if (new_capacity < capacity()) {
    throw std::runtime_error("isolated network reservations cannot shrink");
  }
  if (new_capacity == capacity()) {
    return *this;
  }
  const std::uint64_t slot_count =
      AddressCount(pool_prefix_length_) / kAddressesPerNode;
  const std::vector<Interval> blocked = BlockedLinks(
      pool_base_, pool_prefix_length_, routes, addresses, link_bases_);
  std::uint64_t available = slot_count;
  for (const auto& [first, end] : blocked) {
    available -= end - first;
  }
  const std::uint64_t requested = std::uint64_t{new_capacity} - capacity();
  if (requested > available) {
    throw std::runtime_error("isolated network /31 link pool exhausted: pool " +
                             PoolCidr() + ", requested " +
                             std::to_string(requested) + ", available " +
                             std::to_string(available));
  }
  SimulationNetworkAddressPlan result = *this;
  result.link_bases_.reserve(new_capacity);
  const auto append_available = [&](std::uint64_t first, std::uint64_t end) {
    auto interval =
        std::lower_bound(blocked.begin(), blocked.end(), first,
                         [](const Interval& item, std::uint64_t value) {
                           return item.second <= value;
                         });
    while (first < end && result.capacity() < new_capacity) {
      if (interval != blocked.end() && interval->first <= first) {
        first = interval->second;
        ++interval;
        continue;
      }
      const std::uint64_t free_end =
          interval == blocked.end() ? end : std::min(end, interval->first);
      while (first < free_end && result.capacity() < new_capacity) {
        result.link_bases_.push_back(static_cast<std::uint32_t>(
            std::uint64_t{pool_base_} + first * kAddressesPerNode));
        ++first;
      }
    }
  };
  append_available(first_slot, slot_count);
  append_available(0U, first_slot);
  return result;
}

std::uint32_t SimulationNetworkAddressPlan::capacity() const {
  return static_cast<std::uint32_t>(link_bases_.size());
}

std::string SimulationNetworkAddressPlan::PoolCidr() const {
  return boost::asio::ip::network_v4(boost::asio::ip::address_v4(pool_base_),
                                     pool_prefix_length_)
      .to_string();
}

std::string SimulationNetworkAddressPlan::Cidr() const { return PoolCidr(); }

std::vector<std::string> SimulationNetworkAddressPlan::LinkCidrs() const {
  std::vector<std::string> links;
  links.reserve(link_bases_.size());
  for (const std::uint32_t base : link_bases_) {
    links.push_back(boost::asio::ip::network_v4(
                        boost::asio::ip::address_v4(base), kNodePrefixLength)
                        .to_string());
  }
  return links;
}

boost::json::object SimulationNetworkAddressPlan::ToSerialized() const {
  boost::json::array links;
  links.reserve(link_bases_.size());
  for (std::string& cidr : LinkCidrs()) {
    links.emplace_back(std::move(cidr));
  }
  return {{"pool_cidr", PoolCidr()}, {"link_cidrs", std::move(links)}};
}

SimulationNetworkAddressPlan SimulationNetworkAddressPlan::FromSerialized(
    const boost::json::object& allocation) {
  const boost::json::value* pool = allocation.if_contains("pool_cidr");
  const boost::json::value* links = allocation.if_contains("link_cidrs");
  if (allocation.size() != 2U || pool == nullptr || !pool->is_string() ||
      links == nullptr || !links->is_array()) {
    throw std::runtime_error(
        "isolated network allocation requires only pool_cidr and link_cidrs");
  }
  SimulationNetworkAddressPlan result = FromCidr(pool->as_string(), 0U);
  const std::uint64_t pool_end = std::uint64_t{result.pool_base_} +
                                 AddressCount(result.pool_prefix_length_);
  const std::uint64_t slot_count =
      AddressCount(result.pool_prefix_length_) / kAddressesPerNode;
  if (links->as_array().size() > slot_count) {
    throw std::runtime_error(
        "isolated network allocation exceeds its pool capacity");
  }
  std::set<std::uint32_t> seen;
  result.link_bases_.reserve(links->as_array().size());
  for (const boost::json::value& value : links->as_array()) {
    if (!value.is_string()) {
      throw std::runtime_error("isolated network link CIDR must be a string");
    }
    const auto link = ParseNetwork(value.as_string());
    const std::uint32_t base = link.network().to_uint();
    if (link.prefix_length() != kNodePrefixLength || base < result.pool_base_ ||
        std::uint64_t{base} + kAddressesPerNode > pool_end ||
        !seen.insert(base).second) {
      throw std::runtime_error(
          "isolated network links must be distinct /31 subnets inside the "
          "pool");
    }
    result.link_bases_.push_back(base);
  }
  return result;
}

std::string SimulationNetworkAddressPlan::HostAddress(
    std::uint32_t node_index) const {
  if (node_index >= capacity()) {
    throw std::out_of_range("network address plan node index is out of range");
  }
  return boost::asio::ip::address_v4(link_bases_[node_index]).to_string();
}

std::string SimulationNetworkAddressPlan::NodeAddress(
    std::uint32_t node_index) const {
  if (node_index >= capacity()) {
    throw std::out_of_range("network address plan node index is out of range");
  }
  return boost::asio::ip::address_v4(link_bases_[node_index] + 1U).to_string();
}

std::uint8_t SimulationNetworkAddressPlan::NodePrefixLength() const {
  return kNodePrefixLength;
}

void SimulationNetworkAddressPlan::RequireNodeSlotsAvailable(
    const std::vector<std::uint32_t>& node_slots,
    const std::vector<RouteInfo>& routes) const {
  RequireNodeSlotsAvailable(node_slots, routes, {});
}

void SimulationNetworkAddressPlan::RequireNodeSlotsAvailable(
    const std::vector<std::uint32_t>& node_slots,
    const std::vector<RouteInfo>& routes,
    const std::vector<AddressInfo>& addresses) const {
  const std::vector<Interval> blocked =
      BlockedLinks(pool_base_, pool_prefix_length_, routes, addresses, {});
  for (const std::uint32_t node_slot : node_slots) {
    if (node_slot >= capacity()) {
      throw std::out_of_range("network address plan node slot is out of range");
    }
    const std::uint64_t link =
        (std::uint64_t{link_bases_[node_slot]} - pool_base_) /
        kAddressesPerNode;
    if (IsBlocked(link, blocked)) {
      throw std::runtime_error("isolated simulation node /31 link for slot " +
                               std::to_string(node_slot) +
                               " overlaps a kernel route or assigned address");
    }
  }
}

std::vector<DirectionalNetworkPolicy> ResolveDirectionalNetworkPolicies(
    const PeerTopologyConfig& topology,
    const SimulationNetworkAddressPlan& address_plan, std::uint32_t node_count,
    std::uint32_t node_index) {
  return RuntimePeerTopology(topology, node_count)
      .DirectionalPolicies(address_plan, node_index);
}

}  // namespace bbp
