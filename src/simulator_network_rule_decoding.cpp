#include "simulator_network_rule_decoding.h"

#include <boost/json/value.hpp>
#include <limits>
#include <stdexcept>
#include <string>

#include "simulator_json_field_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

using simulator_app_internal::JsonOptionalUint32Field;
using simulator_app_internal::JsonPercentBasisPoints;
using simulator_app_internal::JsonStringField;
using simulator_app_internal::JsonUint32Field;
using simulator_app_internal::JsonUint64Value;

}  // namespace

std::vector<uint32_t> JsonNodeGroupField(const boost::json::object& object,
                                         const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("missing or invalid node group JSON field: " +
                             std::string(field));
  }
  std::vector<uint32_t> nodes;
  for (const boost::json::value& node_value : value->as_array()) {
    const uint64_t raw_node = JsonUint64Value(node_value, field);
    if (raw_node > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error("partition node value exceeds uint32");
    }
    const uint32_t node = static_cast<uint32_t>(raw_node);
    if (node == 0U) {
      throw std::runtime_error(
          "partition node values must be greater than zero");
    }
    for (uint32_t existing : nodes) {
      if (existing == node - 1U) {
        throw std::runtime_error("partition node group contains a duplicate");
      }
    }
    nodes.push_back(node - 1U);
  }
  if (nodes.empty()) {
    throw std::runtime_error("partition node groups must not be empty");
  }
  return nodes;
}

NetworkCondition ParseNetworkConditionObject(
    const boost::json::object& object) {
  NetworkCondition condition;
  condition.bandwidth_kbps = JsonOptionalUint32Field(object, "bandwidth_kbps",
                                                     condition.bandwidth_kbps);
  condition.delay_ms =
      JsonOptionalUint32Field(object, "delay_ms", condition.delay_ms);
  condition.jitter_ms =
      JsonOptionalUint32Field(object, "jitter_ms", condition.jitter_ms);
  const bool loss_basis_points_present =
      object.if_contains("loss_basis_points") != nullptr;
  const bool loss_percent_present =
      object.if_contains("loss_percent") != nullptr;
  if (loss_basis_points_present && loss_percent_present) {
    throw std::runtime_error(
        "network condition loss_percent and loss_basis_points must not both "
        "be specified");
  }
  condition.loss_basis_points =
      loss_percent_present
          ? JsonPercentBasisPoints(object, "loss_percent")
          : JsonOptionalUint32Field(object, "loss_basis_points",
                                    condition.loss_basis_points);
  condition.duplicate_basis_points = JsonOptionalUint32Field(
      object, "duplicate_basis_points", condition.duplicate_basis_points);
  condition.corrupt_basis_points = JsonOptionalUint32Field(
      object, "corrupt_basis_points", condition.corrupt_basis_points);
  condition.reorder_basis_points = JsonOptionalUint32Field(
      object, "reorder_basis_points", condition.reorder_basis_points);
  condition.limit_packets =
      JsonOptionalUint32Field(object, "limit_packets", condition.limit_packets);
  return condition;
}

uint32_t StableRuleHandle(const NetworkBlockRule& rule) {
  uint32_t hash = 2166136261U;
  const auto mix_byte = [&hash](unsigned char value) {
    hash ^= value;
    hash *= 16777619U;
  };
  const auto mix_uint32 = [&mix_byte](uint32_t value) {
    for (uint32_t shift = 0; shift < 32U; shift += 8U) {
      mix_byte(static_cast<unsigned char>((value >> shift) & 0xFFU));
    }
  };
  mix_uint32(rule.node_index + 1U);
  for (const unsigned char c : rule.src_address) {
    mix_byte(c);
  }
  if (rule.src_port != 0U) {
    mix_byte(0xFFU);
    mix_uint32(rule.src_port);
  }
  for (const unsigned char c : rule.dst_address) {
    mix_byte(c);
  }
  mix_uint32(rule.dst_port);
  hash &= 0x00FFFFFFU;
  return hash == 0U ? 1U : hash;
}

NetworkBlockRule ParseNetworkBlockRuleObject(
    const boost::json::object& object) {
  const uint32_t node = JsonUint32Field(object, "node");
  if (node == 0U) {
    throw std::runtime_error(
        "network block rule node must be greater than zero");
  }
  const uint32_t dst_port = JsonUint32Field(object, "dst_port");
  if (dst_port == 0U || dst_port > 65535U) {
    throw std::runtime_error("network block rule dst_port must be 1..65535");
  }

  NetworkBlockRule rule;
  rule.node_index = node - 1U;
  const boost::json::value* src_address = object.if_contains("src_address");
  if (src_address != nullptr) {
    if (!src_address->is_string()) {
      throw std::runtime_error(
          "network block rule src_address must be a string");
    }
    rule.src_address = std::string(src_address->as_string());
  }
  const bool src_port_present = object.if_contains("src_port") != nullptr;
  const uint32_t src_port = JsonOptionalUint32Field(object, "src_port", 0U);
  if (src_port_present && (src_port == 0U || src_port > 65535U)) {
    throw std::runtime_error("network block rule src_port must be 1..65535");
  }
  rule.dst_address = JsonStringField(object, "dst_address");
  ValidateIpv4Address(rule.dst_address, "network block destination");
  if (!rule.src_address.empty()) {
    ValidateIpv4Address(rule.src_address, "network block source");
  }
  rule.src_port = static_cast<uint16_t>(src_port);
  rule.dst_port = static_cast<uint16_t>(dst_port);
  rule.handle = JsonOptionalUint32Field(object, "handle", 0U);
  if (rule.handle == 0U) {
    rule.handle = StableRuleHandle(rule);
  }
  return rule;
}

NetworkPartitionRule ParseNetworkPartitionRuleObject(
    const boost::json::object& object) {
  NetworkPartitionRule rule;
  rule.group_a = JsonNodeGroupField(object, "group_a");
  rule.group_b = JsonNodeGroupField(object, "group_b");
  for (uint32_t a : rule.group_a) {
    for (uint32_t b : rule.group_b) {
      if (a == b) {
        throw std::runtime_error(
            "partition groups must not contain the same node");
      }
    }
  }
  return rule;
}

void ValidateNetworkPartitionRule(const NetworkPartitionRule& rule,
                                  uint32_t nodes, std::string_view source) {
  for (uint32_t node_index : rule.group_a) {
    if (node_index >= nodes) {
      throw std::runtime_error(std::string(source) +
                               " group_a node must be in 1.." +
                               std::to_string(nodes));
    }
  }
  for (uint32_t node_index : rule.group_b) {
    if (node_index >= nodes) {
      throw std::runtime_error(std::string(source) +
                               " group_b node must be in 1.." +
                               std::to_string(nodes));
    }
  }
}

}  // namespace bbp::simulator_app_internal
