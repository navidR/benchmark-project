#include "benchmark_sim/simulator_app.h"

#include <linux/capability.h>
#include <yaml.h>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/program_options.hpp>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "benchmark_sim/capability.h"
#include "benchmark_sim/cgroup.h"
#include "benchmark_sim/drivers/firo_driver.h"
#include "benchmark_sim/log_tail.h"
#include "benchmark_sim/logging.h"
#include "benchmark_sim/network.h"
#include "benchmark_sim/process.h"
#include "benchmark_sim/run_report.h"
#include "benchmark_sim/simulation_registry.h"
#include "benchmark_sim/simulator/constants.h"
#include "benchmark_sim/simulator/node_runtime.h"
#include "benchmark_sim/simulator/options.h"
#include "benchmark_sim/simulator/yaml_helpers.h"
#include "benchmark_sim/tui.h"
#include "benchmark_sim/util.h"

namespace bsim {
namespace {

uint32_t JsonUint32Field(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint32 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint32 JSON field: " +
                           std::string(field));
}

uint32_t JsonOptionalUint32Field(const boost::json::object& object,
                                 const char* field, uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint32 JSON field: " + std::string(field));
}

uint32_t JsonOptionalNullableUint32Field(const boost::json::object& object,
                                         const char* field,
                                         uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_null()) {
    return default_value;
  }
  return JsonOptionalUint32Field(object, field, default_value);
}

uint64_t JsonOptionalUint64Field(const boost::json::object& object,
                                 const char* field, uint64_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint64 JSON field: " + std::string(field));
}

uint64_t JsonUint64Field(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint64 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint64 JSON field: " +
                           std::string(field));
}

uint64_t JsonUint64Value(const boost::json::value& value,
                         std::string_view field) {
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<uint64_t>(value.as_int64());
  }
  throw std::runtime_error("invalid uint64 JSON field: " + std::string(field));
}

std::optional<uint64_t> JsonOptionalUint64FieldValue(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  return JsonUint64Value(*value, field);
}

bool JsonOptionalBoolField(const boost::json::object& object, const char* field,
                           bool default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (!value->is_bool()) {
    throw std::runtime_error("invalid bool JSON field: " + std::string(field));
  }
  return value->as_bool();
}

std::filesystem::path JsonOptionalPathField(
    const boost::json::object& object, const char* field,
    const std::filesystem::path& default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (!value->is_string()) {
    throw std::runtime_error("invalid path JSON field: " + std::string(field));
  }
  return std::filesystem::path(std::string(value->as_string()));
}

std::string JsonStringField(const boost::json::object& object,
                            const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    throw std::runtime_error("missing or invalid string JSON field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

std::string JsonOptionalStringField(const boost::json::object& object,
                                    const char* field,
                                    std::string_view default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::string(default_value);
  }
  if (!value->is_string()) {
    throw std::runtime_error("invalid string JSON field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

uint64_t JsonAmountField(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid fixed-8 amount JSON field: " +
                             std::string(field));
  }
  return JsonFixed8Amount(*value, field);
}

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

std::optional<std::vector<uint32_t>> JsonOptionalNodeIndexListField(
    const boost::json::object& object, const char* field,
    std::string_view source) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (!value->is_array()) {
    throw std::runtime_error(std::string(source) + " must be a JSON array");
  }

  std::vector<uint32_t> nodes;
  for (const boost::json::value& node_value : value->as_array()) {
    const uint64_t raw_node = JsonUint64Value(node_value, field);
    if (raw_node > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error(std::string(source) +
                               " node value exceeds uint32");
    }
    const uint32_t node = static_cast<uint32_t>(raw_node);
    if (node == 0U) {
      throw std::runtime_error(std::string(source) +
                               " node values must be greater than zero");
    }
    for (uint32_t existing : nodes) {
      if (existing == node - 1U) {
        throw std::runtime_error(std::string(source) + " contains a duplicate");
      }
    }
    nodes.push_back(node - 1U);
  }
  return nodes;
}

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

std::string YamlScalarText(const yaml_event_t& event) {
  return std::string(reinterpret_cast<const char*>(event.data.scalar.value),
                     event.data.scalar.length);
}

std::string LowerAscii(std::string_view text) {
  std::string lower;
  lower.reserve(text.size());
  for (const char c : text) {
    if (c >= 'A' && c <= 'Z') {
      lower.push_back(static_cast<char>(c - 'A' + 'a'));
    } else {
      lower.push_back(c);
    }
  }
  return lower;
}

bool IsDecimalInteger(std::string_view text) {
  if (text.empty()) {
    return false;
  }
  size_t offset = 0;
  if (text.front() == '-') {
    if (text.size() == 1U) {
      return false;
    }
    offset = 1U;
  }
  for (size_t i = offset; i < text.size(); ++i) {
    if (text[i] < '0' || text[i] > '9') {
      return false;
    }
  }
  return true;
}

boost::json::value ParseYamlPlainScalar(std::string_view text) {
  const std::string lower = LowerAscii(text);
  if (text.empty() || text == "~" || lower == "null") {
    return nullptr;
  }
  if (lower == "true") {
    return true;
  }
  if (lower == "false") {
    return false;
  }
  if (!IsDecimalInteger(text)) {
    return boost::json::string(text);
  }
  if (text.front() == '-') {
    int64_t value = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size()) {
      return value;
    }
  } else {
    uint64_t value = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size()) {
      return value;
    }
  }
  return boost::json::string(text);
}

boost::json::value ParseYamlValue(YamlParser* parser, YamlEvent event);

boost::json::array ParseYamlSequence(YamlParser* parser) {
  boost::json::array array;
  while (true) {
    YamlEvent event = parser->Next();
    if (event.Type() == YAML_SEQUENCE_END_EVENT) {
      break;
    }
    array.push_back(ParseYamlValue(parser, std::move(event)));
  }
  return array;
}

boost::json::object ParseYamlMapping(YamlParser* parser) {
  boost::json::object object;
  while (true) {
    YamlEvent key_event = parser->Next();
    if (key_event.Type() == YAML_MAPPING_END_EVENT) {
      break;
    }
    if (key_event.Type() != YAML_SCALAR_EVENT) {
      throw std::runtime_error("YAML mapping keys must be scalars");
    }
    const std::string key = YamlScalarText(key_event.Raw());
    if (object.if_contains(key) != nullptr) {
      throw std::runtime_error("duplicate YAML mapping key: " + key);
    }
    object[key] = ParseYamlValue(parser, parser->Next());
  }
  return object;
}

boost::json::value ParseYamlValue(YamlParser* parser, YamlEvent event) {
  if (event.Type() == YAML_SCALAR_EVENT) {
    const std::string text = YamlScalarText(event.Raw());
    if (event.Raw().data.scalar.style != YAML_PLAIN_SCALAR_STYLE) {
      return boost::json::string(text);
    }
    return ParseYamlPlainScalar(text);
  }
  if (event.Type() == YAML_SEQUENCE_START_EVENT) {
    return ParseYamlSequence(parser);
  }
  if (event.Type() == YAML_MAPPING_START_EVENT) {
    return ParseYamlMapping(parser);
  }
  if (event.Type() == YAML_ALIAS_EVENT) {
    throw std::runtime_error("YAML aliases are not supported");
  }
  throw std::runtime_error("unexpected YAML event while parsing scenario");
}

void RequireYamlEvent(const YamlEvent& event, yaml_event_type_t expected,
                      std::string_view message) {
  if (event.Type() != expected) {
    throw std::runtime_error(std::string(message));
  }
}

boost::json::value ParseYamlDocument(std::string input,
                                     const std::filesystem::path& source) {
  YamlParser parser(std::move(input), source.string());
  RequireYamlEvent(parser.Next(), YAML_STREAM_START_EVENT,
                   "YAML stream did not start");
  YamlEvent document_start = parser.Next();
  if (document_start.Type() == YAML_STREAM_END_EVENT) {
    return nullptr;
  }
  RequireYamlEvent(document_start, YAML_DOCUMENT_START_EVENT,
                   "YAML document did not start");

  boost::json::value root;
  YamlEvent root_event = parser.Next();
  if (root_event.Type() == YAML_DOCUMENT_END_EVENT) {
    root = nullptr;
  } else {
    root = ParseYamlValue(&parser, std::move(root_event));
    RequireYamlEvent(parser.Next(), YAML_DOCUMENT_END_EVENT,
                     "YAML document did not end");
  }
  RequireYamlEvent(parser.Next(), YAML_STREAM_END_EVENT,
                   "YAML scenario must contain exactly one document");
  return root;
}

NetworkCondition ParseNetworkConditionObject(
    const boost::json::object& object) {
  NetworkCondition condition;
  condition.bandwidth_mbps = JsonOptionalUint32Field(object, "bandwidth_mbps",
                                                     condition.bandwidth_mbps);
  condition.delay_ms =
      JsonOptionalUint32Field(object, "delay_ms", condition.delay_ms);
  condition.jitter_ms =
      JsonOptionalUint32Field(object, "jitter_ms", condition.jitter_ms);
  condition.loss_basis_points = JsonOptionalUint32Field(
      object, "loss_basis_points", condition.loss_basis_points);
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
  rule.dst_address = JsonStringField(object, "dst_address");
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

std::vector<uint32_t> ConsecutiveNodeIndexes(uint32_t first_index,
                                             uint32_t count) {
  std::vector<uint32_t> nodes;
  nodes.reserve(count);
  for (uint32_t offset = 0; offset < count; ++offset) {
    nodes.push_back(first_index + offset);
  }
  return nodes;
}

void ValidateRoleNodeList(const std::vector<uint32_t>& role_nodes,
                          uint32_t node_count, std::string_view source) {
  for (uint32_t node_index : role_nodes) {
    if (node_index >= node_count) {
      throw std::runtime_error(std::string(source) + " must be in 1.." +
                               std::to_string(node_count));
    }
  }
}

bool NodeListsOverlap(const std::vector<uint32_t>& left,
                      const std::vector<uint32_t>& right) {
  for (uint32_t left_node : left) {
    for (uint32_t right_node : right) {
      if (left_node == right_node) {
        return true;
      }
    }
  }
  return false;
}

bool NodeListContains(const std::vector<uint32_t>& nodes, uint32_t node_index) {
  for (uint32_t candidate : nodes) {
    if (candidate == node_index) {
      return true;
    }
  }
  return false;
}

WalletInitializationStrategy ParseWalletInitializationStrategy(
    std::string_view value) {
  if (value == "driver_rpc") {
    return WalletInitializationStrategy::kDriverRpc;
  }
  throw std::runtime_error(
      "scenario topology.wallet_initialization strategy must be driver_rpc");
}

WalletPrivacyMode ParseWalletPrivacyMode(std::string_view value) {
  if (value == "public") {
    return WalletPrivacyMode::kPublic;
  }
  if (value == "private") {
    return WalletPrivacyMode::kPrivate;
  }
  throw std::runtime_error(
      "scenario topology.wallet_initialization mode must be public or private");
}

WalletTransferStrategy ParseWalletTransferStrategy(std::string_view value) {
  if (value == "round_robin") {
    return WalletTransferStrategy::kRoundRobin;
  }
  throw std::runtime_error(
      "scenario wallet_transactions strategy must be round_robin");
}

std::string_view WalletInitializationStrategyName(
    WalletInitializationStrategy strategy) {
  switch (strategy) {
    case WalletInitializationStrategy::kDriverRpc:
      return "driver_rpc";
  }
  throw std::runtime_error("unknown wallet initialization strategy");
}

std::string_view WalletPrivacyModeName(WalletPrivacyMode mode) {
  switch (mode) {
    case WalletPrivacyMode::kPublic:
      return "public";
    case WalletPrivacyMode::kPrivate:
      return "private";
  }
  throw std::runtime_error("unknown wallet privacy mode");
}

std::string_view WalletTransferStrategyName(WalletTransferStrategy strategy) {
  switch (strategy) {
    case WalletTransferStrategy::kRoundRobin:
      return "round_robin";
  }
  throw std::runtime_error("unknown wallet transfer strategy");
}

WalletMode ToFiroWalletMode(const WalletInitialization& initialization) {
  switch (initialization.mode) {
    case WalletPrivacyMode::kPublic:
      return WalletMode::kPublic;
    case WalletPrivacyMode::kPrivate:
      return WalletMode::kPrivate;
  }
  throw std::runtime_error("unknown wallet privacy mode");
}

NodeRoleTopology ParseNodeRoleTopologyObject(const boost::json::object& object,
                                             uint32_t nodes) {
  NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = JsonOptionalUint32Field(object, "node_count", nodes);
  if (topology.node_count != nodes) {
    throw std::runtime_error(
        "scenario topology.node_count must match node count");
  }

  std::optional<std::vector<uint32_t>> wallet_nodes =
      JsonOptionalNodeIndexListField(object, "wallet_nodes",
                                     "scenario topology.wallet_nodes");
  std::optional<std::vector<uint32_t>> miner_nodes =
      JsonOptionalNodeIndexListField(object, "miner_nodes",
                                     "scenario topology.miner_nodes");
  const bool wallet_count_present =
      object.if_contains("wallet_node_count") != nullptr;
  const bool miner_count_present =
      object.if_contains("miner_node_count") != nullptr;
  const uint32_t wallet_nodes_size =
      wallet_nodes ? static_cast<uint32_t>(wallet_nodes->size()) : 0U;
  const uint32_t miner_nodes_size =
      miner_nodes ? static_cast<uint32_t>(miner_nodes->size()) : 0U;

  topology.wallet_node_count = JsonOptionalUint32Field(
      object, "wallet_node_count", wallet_nodes ? wallet_nodes_size : 0U);
  topology.miner_node_count = JsonOptionalUint32Field(
      object, "miner_node_count", miner_nodes ? miner_nodes_size : 0U);
  topology.allow_miner_wallet_overlap =
      JsonOptionalBoolField(object, "allow_miner_wallet_overlap",
                            topology.allow_miner_wallet_overlap);

  if (wallet_nodes && wallet_count_present &&
      topology.wallet_node_count != wallet_nodes_size) {
    throw std::runtime_error(
        "scenario topology wallet_node_count must match wallet_nodes size");
  }
  if (miner_nodes && miner_count_present &&
      topology.miner_node_count != miner_nodes_size) {
    throw std::runtime_error(
        "scenario topology miner_node_count must match miner_nodes size");
  }
  if (topology.wallet_node_count > topology.node_count) {
    throw std::runtime_error(
        "scenario topology wallet_node_count must be <= node_count");
  }
  if (topology.miner_node_count > topology.node_count) {
    throw std::runtime_error(
        "scenario topology miner_node_count must be <= node_count");
  }
  if (!topology.allow_miner_wallet_overlap &&
      topology.wallet_node_count >
          topology.node_count - topology.miner_node_count) {
    throw std::runtime_error(
        "scenario topology wallet_node_count plus miner_node_count must be <= "
        "node_count when overlap is disabled");
  }

  topology.wallet_nodes =
      wallet_nodes ? *wallet_nodes
                   : ConsecutiveNodeIndexes(0U, topology.wallet_node_count);
  const uint32_t first_miner_index =
      topology.allow_miner_wallet_overlap ? 0U : topology.wallet_node_count;
  topology.miner_nodes =
      miner_nodes ? *miner_nodes
                  : ConsecutiveNodeIndexes(first_miner_index,
                                           topology.miner_node_count);

  ValidateRoleNodeList(topology.wallet_nodes, topology.node_count,
                       "scenario topology.wallet_nodes");
  ValidateRoleNodeList(topology.miner_nodes, topology.node_count,
                       "scenario topology.miner_nodes");
  if (!topology.allow_miner_wallet_overlap &&
      NodeListsOverlap(topology.wallet_nodes, topology.miner_nodes)) {
    throw std::runtime_error(
        "scenario topology wallet_nodes and miner_nodes overlap but "
        "allow_miner_wallet_overlap is false");
  }
  return topology;
}

WalletInitialization ParseWalletInitializationObject(
    const boost::json::object& topology) {
  WalletInitialization initialization;
  const boost::json::value* value =
      topology.if_contains("wallet_initialization");
  if (value == nullptr) {
    return initialization;
  }
  if (!value->is_object()) {
    throw std::runtime_error(
        "scenario topology.wallet_initialization must be a JSON object");
  }
  const boost::json::object& object = value->as_object();
  initialization.strategy =
      ParseWalletInitializationStrategy(JsonOptionalStringField(
          object, "strategy",
          WalletInitializationStrategyName(initialization.strategy)));
  initialization.mode = ParseWalletPrivacyMode(JsonOptionalStringField(
      object, "mode", WalletPrivacyModeName(initialization.mode)));
  initialization.seed =
      JsonOptionalStringField(object, "seed", initialization.seed);
  return initialization;
}

void ValidateWalletTransactionsWorkload(
    const WalletTransactionsWorkload& workload, const Options& options) {
  if (!options.topology.configured) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires topology");
  }
  if (options.topology.miner_nodes.empty()) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires at least one "
        "MinerNode");
  }
  const size_t wallet_count = options.topology.wallet_nodes.size();
  if (wallet_count < 2U) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires at least two "
        "WalletNode roles");
  }
  if (workload.transaction_count == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_count must be greater than "
        "zero");
  }
  if (workload.transaction_count > static_cast<uint32_t>(wallet_count)) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_count currently must be <= "
        "wallet node count");
  }
  if (workload.funding_blocks_per_wallet <
      kFiroCoinbaseSpendableConfirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_blocks_per_wallet must be at "
        "least " +
        std::to_string(kFiroCoinbaseSpendableConfirmations));
  }
  if (workload.readiness_confirmations < kFiroCoinbaseSpendableConfirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions readiness_confirmations must be at "
        "least " +
        std::to_string(kFiroCoinbaseSpendableConfirmations));
  }
  if (workload.funding_blocks_per_wallet < workload.readiness_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_blocks_per_wallet must be >= "
        "readiness_confirmations");
  }
  if (workload.amount_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions amount must be greater than zero");
  }
  if (workload.fee_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions fee must be greater than zero");
  }
  if (workload.amount_satoshis >
      std::numeric_limits<uint64_t>::max() - workload.fee_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions amount plus fee overflows uint64");
  }
  if (workload.timeout_sec == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions timeout_sec must be greater than zero");
  }

  SimulationRegistry::FromTopology(options.topology,
                                   options.wallet_initialization);
}

bool ResourceLimitPatchEmpty(const ResourceLimitPatch& patch) {
  return !patch.memory_high_bytes && !patch.memory_max_bytes &&
         !patch.cpu_quota_present && !patch.cpu_period_us && !patch.pids_max;
}

void RequireNonZero(uint64_t value, std::string_view field) {
  if (value == 0U) {
    throw std::runtime_error(std::string(field) + " must be greater than zero");
  }
}

ResourceLimitPatch ParseResourceLimitPatchObject(
    const boost::json::object& object) {
  ResourceLimitPatch patch;
  patch.memory_high_bytes =
      JsonOptionalUint64FieldValue(object, "memory_high_bytes");
  patch.memory_max_bytes =
      JsonOptionalUint64FieldValue(object, "memory_max_bytes");
  const boost::json::value* quota = object.if_contains("cpu_quota_us");
  if (quota != nullptr) {
    patch.cpu_quota_present = true;
    if (!quota->is_null()) {
      patch.cpu_quota_us = JsonUint64Value(*quota, "cpu_quota_us");
    }
  }
  patch.cpu_period_us = JsonOptionalUint64FieldValue(object, "cpu_period_us");
  patch.pids_max = JsonOptionalUint64FieldValue(object, "pids_max");

  if (ResourceLimitPatchEmpty(patch)) {
    throw std::runtime_error("runtime resource update has no limit fields");
  }
  if (patch.memory_max_bytes) {
    RequireNonZero(*patch.memory_max_bytes, "memory_max_bytes");
  }
  if (patch.cpu_quota_us) {
    RequireNonZero(*patch.cpu_quota_us, "cpu_quota_us");
  }
  if (patch.cpu_period_us) {
    RequireNonZero(*patch.cpu_period_us, "cpu_period_us");
  }
  if (patch.pids_max) {
    RequireNonZero(*patch.pids_max, "pids_max");
  }
  if (patch.memory_high_bytes && patch.memory_max_bytes &&
      *patch.memory_high_bytes > *patch.memory_max_bytes) {
    throw std::runtime_error(
        "memory_high_bytes must be less than or equal to memory_max_bytes");
  }
  return patch;
}

void ApplyNodeConditions(const boost::json::array& conditions, uint32_t nodes,
                         std::string_view source,
                         std::map<uint32_t, NetworkCondition>& output) {
  for (const boost::json::value& value : conditions) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output[node - 1U] = ParseNetworkConditionObject(object);
  }
}

void ApplyNetworkBlockRules(const boost::json::array& rules, uint32_t nodes,
                            std::string_view source,
                            std::vector<NetworkBlockRule>& output) {
  for (const boost::json::value& value : rules) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    NetworkBlockRule rule = ParseNetworkBlockRuleObject(value.as_object());
    if (rule.node_index >= nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output.push_back(std::move(rule));
  }
}

void ApplyNetworkPartitionRules(const boost::json::array& rules, uint32_t nodes,
                                std::string_view source,
                                std::vector<NetworkPartitionRule>& output) {
  for (const boost::json::value& value : rules) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    NetworkPartitionRule rule =
        ParseNetworkPartitionRuleObject(value.as_object());
    ValidateNetworkPartitionRule(rule, nodes, source);
    output.push_back(std::move(rule));
  }
}

void ApplyResourceLimitPatches(const boost::json::array& updates,
                               uint32_t nodes, std::string_view source,
                               std::map<uint32_t, ResourceLimitPatch>& output) {
  for (const boost::json::value& value : updates) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output[node - 1U] = ParseResourceLimitPatchObject(object);
  }
}

void ApplyScenarioWorkloads(const boost::json::array& workloads,
                            const boost::program_options::variables_map& vm,
                            Options& options) {
  for (const boost::json::value& value : workloads) {
    if (!value.is_object()) {
      throw std::runtime_error(
          "scenario workloads entries must be JSON objects");
    }
    const boost::json::object& workload = value.as_object();
    const std::string type = JsonStringField(workload, "type");
    if (type == "block_generation") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP block_generation workload uses node, not nodes");
      }
      BlockGenerationWorkload block_generation;
      block_generation.count =
          OptionProvided(vm, "generate-blocks")
              ? options.generate_blocks
              : JsonOptionalUint32Field(workload, "count",
                                        options.generate_blocks);
      block_generation.node =
          OptionProvided(vm, "generate-node")
              ? options.generate_node
              : JsonOptionalUint32Field(workload, "node",
                                        options.generate_node);
      block_generation.sync_timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "sync_timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kBlockGeneration;
      scenario_workload.block_generation = block_generation;
      options.workloads.push_back(scenario_workload);
    } else if (type == "wait_until_height") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP wait_until_height workload uses node, not nodes");
      }
      WaitUntilHeightWorkload wait;
      wait.node = JsonOptionalUint32Field(workload, "node", wait.node);
      wait.height = JsonUint64Field(workload, "height");
      wait.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWaitUntilHeight;
      scenario_workload.wait_until_height = wait;
      options.workloads.push_back(scenario_workload);
    } else if (type == "wait_for_peers") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP wait_for_peers workload uses node, not nodes");
      }
      WaitForPeersWorkload wait;
      wait.node = JsonOptionalUint32Field(workload, "node", wait.node);
      wait.peer_count = JsonUint64Field(workload, "peer_count");
      wait.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWaitForPeers;
      scenario_workload.wait_for_peers = wait;
      options.workloads.push_back(scenario_workload);
    } else if (type == "connect_peer") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP connect_peer workload uses node, not nodes");
      }
      ConnectPeerWorkload connect;
      connect.node = JsonOptionalUint32Field(workload, "node", connect.node);
      connect.peer = JsonUint32Field(workload, "peer");
      connect.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kConnectPeer;
      scenario_workload.connect_peer = connect;
      options.workloads.push_back(scenario_workload);
    } else if (type == "disconnect_peer") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP disconnect_peer workload uses node, not nodes");
      }
      DisconnectPeerWorkload disconnect;
      disconnect.node =
          JsonOptionalUint32Field(workload, "node", disconnect.node);
      disconnect.peer = JsonUint32Field(workload, "peer");
      disconnect.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kDisconnectPeer;
      scenario_workload.disconnect_peer = disconnect;
      options.workloads.push_back(scenario_workload);
    } else if (type == "restart_node") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP restart_node workload uses node, not nodes");
      }
      RestartNodeWorkload restart;
      restart.node = JsonOptionalUint32Field(workload, "node", restart.node);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kRestartNode;
      scenario_workload.restart_node = restart;
      options.workloads.push_back(scenario_workload);
    } else if (type == "freeze_node") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP freeze_node workload uses node, not nodes");
      }
      FreezeNodeWorkload freeze;
      freeze.node = JsonOptionalUint32Field(workload, "node", freeze.node);
      freeze.duration_ms = JsonUint32Field(workload, "duration_ms");
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kFreezeNode;
      scenario_workload.freeze_node = freeze;
      options.workloads.push_back(scenario_workload);
    } else if (type == "update_resource_limits") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP update_resource_limits workload uses node, not "
            "nodes");
      }
      ResourceLimitUpdateWorkload update;
      update.node = JsonOptionalUint32Field(workload, "node", update.node);
      update.patch = ParseResourceLimitPatchObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kUpdateResourceLimits;
      scenario_workload.update_resource_limits = update;
      options.workloads.push_back(scenario_workload);
    } else if (type == "partition_nodes") {
      NetworkPartitionWorkload partition;
      partition.partition = ParseNetworkPartitionRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kPartitionNodes;
      scenario_workload.network_partition = partition;
      options.workloads.push_back(scenario_workload);
    } else if (type == "heal_partition") {
      NetworkPartitionWorkload partition;
      partition.partition = ParseNetworkPartitionRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kHealPartition;
      scenario_workload.network_partition = partition;
      options.workloads.push_back(scenario_workload);
    } else if (type == "send_raw_transaction") {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current Firo MVP send_raw_transaction workload uses "
            "funding_node and submit_node, not nodes");
      }
      SendRawTransactionWorkload transaction;
      transaction.funding_node = JsonOptionalUint32Field(
          workload, "funding_node", transaction.funding_node);
      transaction.submit_node = JsonOptionalUint32Field(
          workload, "submit_node", transaction.submit_node);
      transaction.source_address = JsonStringField(workload, "source_address");
      transaction.source_private_key =
          JsonStringField(workload, "source_private_key");
      transaction.destination_address =
          JsonStringField(workload, "destination_address");
      transaction.funding_blocks = JsonOptionalUint32Field(
          workload, "funding_blocks", transaction.funding_blocks);
      transaction.amount_satoshis = JsonAmountField(workload, "amount");
      transaction.fee_satoshis = JsonAmountField(workload, "fee");
      transaction.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kSendRawTransaction;
      scenario_workload.send_raw_transaction = transaction;
      options.workloads.push_back(scenario_workload);
    } else if (type == "wallet_transactions") {
      if (workload.if_contains("wallets") != nullptr ||
          workload.if_contains("private_key") != nullptr ||
          workload.if_contains("source_private_key") != nullptr ||
          workload.if_contains("address") != nullptr ||
          workload.if_contains("source_address") != nullptr ||
          workload.if_contains("destination_address") != nullptr) {
        throw std::runtime_error(
            "scenario wallet_transactions workload must use "
            "topology.wallet_initialization instead of wallet keys or "
            "addresses");
      }
      WalletTransactionsWorkload transactions;
      transactions.strategy =
          ParseWalletTransferStrategy(JsonOptionalStringField(
              workload, "strategy",
              WalletTransferStrategyName(transactions.strategy)));
      transactions.funding_blocks_per_wallet =
          JsonOptionalUint32Field(workload, "funding_blocks_per_wallet",
                                  transactions.funding_blocks_per_wallet);
      transactions.readiness_confirmations =
          JsonOptionalUint64Field(workload, "readiness_confirmations",
                                  transactions.readiness_confirmations);
      transactions.transaction_count = JsonOptionalUint32Field(
          workload, "transaction_count",
          options.topology.configured
              ? static_cast<uint32_t>(options.topology.wallet_nodes.size())
              : 0U);
      transactions.amount_satoshis = JsonAmountField(workload, "amount");
      transactions.fee_satoshis = JsonAmountField(workload, "fee");
      transactions.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWalletTransactions;
      scenario_workload.wallet_transactions = std::move(transactions);
      options.workloads.push_back(std::move(scenario_workload));
      options.wallet_backed_workload_requested = true;
    } else {
      throw std::runtime_error("unsupported scenario workload type: " + type);
    }
  }
}

void ApplyScenarioJson(const boost::json::object& scenario,
                       const boost::program_options::variables_map& vm,
                       Options& options) {
  if (!OptionProvided(vm, "firod")) {
    options.firod = JsonOptionalPathField(scenario, "firod", options.firod);
  }
  if (!OptionProvided(vm, "benchmark-root") &&
      !OptionProvided(vm, "output-dir")) {
    options.output_dir =
        JsonOptionalPathField(scenario, "output_dir", options.output_dir);
  }
  if (!OptionProvided(vm, "run-id")) {
    const boost::json::value* run_id = scenario.if_contains("run_id");
    if (run_id != nullptr) {
      if (!run_id->is_string()) {
        throw std::runtime_error("scenario run_id must be a string");
      }
      options.run_id = std::string(run_id->as_string());
    }
  }
  if (!OptionProvided(vm, "nodes")) {
    const bool nodes_present = scenario.if_contains("nodes") != nullptr;
    const bool node_count_present =
        scenario.if_contains("node_count") != nullptr;
    const uint32_t scenario_nodes =
        JsonOptionalUint32Field(scenario, "nodes", options.nodes);
    const uint32_t scenario_node_count =
        JsonOptionalUint32Field(scenario, "node_count", scenario_nodes);
    if (nodes_present && node_count_present &&
        scenario_nodes != scenario_node_count) {
      throw std::runtime_error("scenario nodes and node_count must match");
    }
    options.nodes = node_count_present ? scenario_node_count : scenario_nodes;
  }
  const boost::json::value* topology = scenario.if_contains("topology");
  if (topology != nullptr) {
    if (!topology->is_object()) {
      throw std::runtime_error("scenario topology must be a JSON object");
    }
    options.topology =
        ParseNodeRoleTopologyObject(topology->as_object(), options.nodes);
    options.wallet_initialization =
        ParseWalletInitializationObject(topology->as_object());
    if (!OptionProvided(vm, "generate-node") &&
        !options.topology.miner_nodes.empty()) {
      options.generate_node = options.topology.miner_nodes.front() + 1U;
    }
  }
  if (!OptionProvided(vm, "generate-blocks")) {
    options.generate_blocks = JsonOptionalUint32Field(
        scenario, "generate_blocks", options.generate_blocks);
  }
  if (!OptionProvided(vm, "generate-node")) {
    options.generate_node = JsonOptionalNullableUint32Field(
        scenario, "generate_node", options.generate_node);
  }
  if (!OptionProvided(vm, "ready-timeout-sec")) {
    options.ready_timeout_sec = JsonOptionalUint32Field(
        scenario, "ready_timeout_sec", options.ready_timeout_sec);
  }
  if (!OptionProvided(vm, "sync-timeout-sec")) {
    options.sync_timeout_sec = JsonOptionalNullableUint32Field(
        scenario, "sync_timeout_sec", options.sync_timeout_sec);
  }
  if (!OptionProvided(vm, "metrics-sample-count")) {
    options.metrics_sample_count = JsonOptionalUint32Field(
        scenario, "metrics_sample_count", options.metrics_sample_count);
  }
  if (!OptionProvided(vm, "metrics-interval-ms")) {
    options.metrics_interval_ms = JsonOptionalUint32Field(
        scenario, "metrics_interval_ms", options.metrics_interval_ms);
  }
  if (!OptionProvided(vm, "isolate-network")) {
    options.isolate_network = JsonOptionalBoolField(
        scenario, "isolated_network", options.isolate_network);
  }
  const boost::json::value* workloads = scenario.if_contains("workloads");
  if (workloads != nullptr) {
    if (!workloads->is_array()) {
      throw std::runtime_error("scenario workloads must be a JSON array");
    }
    options.workloads_configured = true;
    ApplyScenarioWorkloads(workloads->as_array(), vm, options);
  }

  const boost::json::value* resources = scenario.if_contains("resources");
  if (resources != nullptr) {
    if (!resources->is_object()) {
      throw std::runtime_error("scenario resources must be a JSON object");
    }
    const boost::json::object& object = resources->as_object();
    if (!OptionProvided(vm, "memory-high-bytes")) {
      options.memory_high_bytes = JsonOptionalUint64Field(
          object, "memory_high_bytes", options.memory_high_bytes);
    }
    if (!OptionProvided(vm, "memory-max-bytes")) {
      options.memory_max_bytes = JsonOptionalUint64Field(
          object, "memory_max_bytes", options.memory_max_bytes);
    }
    if (!OptionProvided(vm, "cpu-period-us")) {
      options.cpu_period_us = JsonOptionalUint64Field(object, "cpu_period_us",
                                                      options.cpu_period_us);
    }
    if (!OptionProvided(vm, "pids-max")) {
      options.pids_max =
          JsonOptionalUint64Field(object, "pids_max", options.pids_max);
    }
    if (!OptionProvided(vm, "cpu-quota-us")) {
      const boost::json::value* quota = object.if_contains("cpu_quota_us");
      if (quota != nullptr && !quota->is_null()) {
        if (!quota->is_uint64() &&
            !(quota->is_int64() && quota->as_int64() >= 0)) {
          throw std::runtime_error(
              "scenario resources.cpu_quota_us must be uint or null");
        }
        options.cpu_quota_us = quota->is_uint64()
                                   ? quota->as_uint64()
                                   : static_cast<uint64_t>(quota->as_int64());
        options.cpu_quota_requested = true;
      }
    }
    const boost::json::value* runtime_node_limits =
        object.if_contains("runtime_node_limits");
    if (runtime_node_limits != nullptr) {
      if (!runtime_node_limits->is_array()) {
        throw std::runtime_error(
            "scenario resources.runtime_node_limits must be a JSON array");
      }
      ApplyResourceLimitPatches(runtime_node_limits->as_array(), options.nodes,
                                "scenario resources.runtime_node_limits",
                                options.runtime_node_resource_updates);
    }
  }

  const boost::json::value* process = scenario.if_contains("process");
  if (process != nullptr) {
    if (!process->is_object()) {
      throw std::runtime_error("scenario process must be a JSON object");
    }
    const boost::json::object& object = process->as_object();
    const boost::json::value* runtime_node_restarts =
        object.if_contains("runtime_node_restarts");
    if (runtime_node_restarts != nullptr) {
      if (!runtime_node_restarts->is_array()) {
        throw std::runtime_error(
            "scenario process.runtime_node_restarts must be a JSON array");
      }
      for (const boost::json::value& value :
           runtime_node_restarts->as_array()) {
        if (!value.is_object()) {
          throw std::runtime_error(
              "scenario process.runtime_node_restarts entries must be JSON "
              "objects");
        }
        const uint32_t node = JsonUint32Field(value.as_object(), "node");
        if (node == 0 || node > options.nodes) {
          throw std::runtime_error(
              "scenario process.runtime_node_restarts node must be in 1.." +
              std::to_string(options.nodes));
        }
        options.runtime_node_restarts.push_back(node - 1U);
      }
    }
    const boost::json::value* runtime_node_freezes =
        object.if_contains("runtime_node_freezes");
    if (runtime_node_freezes != nullptr) {
      if (!runtime_node_freezes->is_array()) {
        throw std::runtime_error(
            "scenario process.runtime_node_freezes must be a JSON array");
      }
      for (const boost::json::value& value : runtime_node_freezes->as_array()) {
        if (!value.is_object()) {
          throw std::runtime_error(
              "scenario process.runtime_node_freezes entries must be JSON "
              "objects");
        }
        const boost::json::object& freeze = value.as_object();
        const uint32_t node = JsonUint32Field(freeze, "node");
        if (node == 0 || node > options.nodes) {
          throw std::runtime_error(
              "scenario process.runtime_node_freezes node must be in 1.." +
              std::to_string(options.nodes));
        }
        const uint32_t duration_ms = JsonUint32Field(freeze, "duration_ms");
        if (duration_ms == 0U) {
          throw std::runtime_error(
              "scenario process.runtime_node_freezes duration_ms must be "
              "greater than zero");
        }
        options.runtime_node_freezes.push_back(
            FreezeRequest{.node_index = node - 1U, .duration_ms = duration_ms});
      }
    }
  }

  const boost::json::value* network = scenario.if_contains("network");
  if (network != nullptr) {
    if (!network->is_object()) {
      throw std::runtime_error("scenario network must be a JSON object");
    }
    const boost::json::object& object = network->as_object();
    if (!OptionProvided(vm, "isolate-network")) {
      options.isolate_network =
          JsonOptionalBoolField(object, "isolated", options.isolate_network);
    }
    const boost::json::value* default_condition =
        object.if_contains("default_condition");
    if (default_condition != nullptr) {
      if (!default_condition->is_object()) {
        throw std::runtime_error(
            "scenario network.default_condition must be a JSON object");
      }
      const NetworkCondition scenario_condition =
          ParseNetworkConditionObject(default_condition->as_object());
      if (!OptionProvided(vm, "network-bandwidth-mbps")) {
        options.network_condition.bandwidth_mbps =
            scenario_condition.bandwidth_mbps;
      }
      if (!OptionProvided(vm, "network-delay-ms")) {
        options.network_condition.delay_ms = scenario_condition.delay_ms;
      }
      if (!OptionProvided(vm, "network-jitter-ms")) {
        options.network_condition.jitter_ms = scenario_condition.jitter_ms;
      }
      if (!OptionProvided(vm, "network-loss-bps")) {
        options.network_condition.loss_basis_points =
            scenario_condition.loss_basis_points;
      }
      if (!OptionProvided(vm, "network-duplicate-bps")) {
        options.network_condition.duplicate_basis_points =
            scenario_condition.duplicate_basis_points;
      }
      if (!OptionProvided(vm, "network-corrupt-bps")) {
        options.network_condition.corrupt_basis_points =
            scenario_condition.corrupt_basis_points;
      }
      if (!OptionProvided(vm, "network-reorder-bps")) {
        options.network_condition.reorder_basis_points =
            scenario_condition.reorder_basis_points;
      }
      if (!OptionProvided(vm, "network-limit-packets")) {
        options.network_condition.limit_packets =
            scenario_condition.limit_packets;
      }
      options.network_condition_requested = true;
    }
    const boost::json::value* node_conditions =
        object.if_contains("node_conditions");
    if (node_conditions != nullptr) {
      if (!node_conditions->is_array()) {
        throw std::runtime_error(
            "scenario network.node_conditions must be a JSON array");
      }
      ApplyNodeConditions(node_conditions->as_array(), options.nodes,
                          "scenario network.node_conditions",
                          options.node_network_conditions);
    }
    const boost::json::value* runtime_node_conditions =
        object.if_contains("runtime_node_conditions");
    if (runtime_node_conditions != nullptr) {
      if (!runtime_node_conditions->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_conditions must be a JSON array");
      }
      ApplyNodeConditions(runtime_node_conditions->as_array(), options.nodes,
                          "scenario network.runtime_node_conditions",
                          options.runtime_node_network_conditions);
    }
    const boost::json::value* runtime_node_blocks =
        object.if_contains("runtime_node_blocks");
    if (runtime_node_blocks != nullptr) {
      if (!runtime_node_blocks->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_blocks must be a JSON array");
      }
      ApplyNetworkBlockRules(runtime_node_blocks->as_array(), options.nodes,
                             "scenario network.runtime_node_blocks",
                             options.runtime_node_blocks);
    }
    const boost::json::value* runtime_node_unblocks =
        object.if_contains("runtime_node_unblocks");
    if (runtime_node_unblocks != nullptr) {
      if (!runtime_node_unblocks->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_unblocks must be a JSON array");
      }
      ApplyNetworkBlockRules(runtime_node_unblocks->as_array(), options.nodes,
                             "scenario network.runtime_node_unblocks",
                             options.runtime_node_unblocks);
    }
    const boost::json::value* runtime_partitions =
        object.if_contains("runtime_partitions");
    if (runtime_partitions != nullptr) {
      if (!runtime_partitions->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_partitions must be a JSON array");
      }
      ApplyNetworkPartitionRules(runtime_partitions->as_array(), options.nodes,
                                 "scenario network.runtime_partitions",
                                 options.runtime_partitions);
    }
    const boost::json::value* runtime_partition_heals =
        object.if_contains("runtime_partition_heals");
    if (runtime_partition_heals != nullptr) {
      if (!runtime_partition_heals->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_partition_heals must be a JSON array");
      }
      ApplyNetworkPartitionRules(runtime_partition_heals->as_array(),
                                 options.nodes,
                                 "scenario network.runtime_partition_heals",
                                 options.runtime_partition_heals);
    }
  }
}

bool WorkloadsRequireIsolatedNetwork(const Options& options) {
  for (const ScenarioWorkload& workload : options.workloads) {
    if (workload.kind == WorkloadKind::kPartitionNodes ||
        workload.kind == WorkloadKind::kHealPartition) {
      return true;
    }
  }
  return false;
}

void ParseNodeNetworkConditionTexts(
    const std::vector<std::string>& texts, uint32_t nodes,
    std::string_view option_name,
    std::map<uint32_t, NetworkCondition>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(option_name) +
                               " node must be in 1..--nodes");
    }
    output[node - 1U] = ParseNetworkConditionObject(object);
  }
}

void ParseRuntimeNodeResourceTexts(
    const std::vector<std::string>& texts, uint32_t nodes,
    std::map<uint32_t, ResourceLimitPatch>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-resource-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-resource-json node must be in 1..--nodes");
    }
    output[node - 1U] = ParseResourceLimitPatchObject(object);
  }
}

void ParseRuntimeNodeBlockTexts(const std::vector<std::string>& texts,
                                uint32_t nodes, std::string_view option_name,
                                std::vector<NetworkBlockRule>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    NetworkBlockRule rule = ParseNetworkBlockRuleObject(value.as_object());
    if (rule.node_index >= nodes) {
      throw std::runtime_error(std::string(option_name) +
                               " node must be in 1..--nodes");
    }
    output.push_back(std::move(rule));
  }
}

void ParseRuntimePartitionTexts(const std::vector<std::string>& texts,
                                uint32_t nodes, std::string_view option_name,
                                std::vector<NetworkPartitionRule>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    NetworkPartitionRule rule =
        ParseNetworkPartitionRuleObject(value.as_object());
    for (uint32_t node_index : rule.group_a) {
      if (node_index >= nodes) {
        throw std::runtime_error(std::string(option_name) +
                                 " group_a node must be in 1..--nodes");
      }
    }
    for (uint32_t node_index : rule.group_b) {
      if (node_index >= nodes) {
        throw std::runtime_error(std::string(option_name) +
                                 " group_b node must be in 1..--nodes");
      }
    }
    output.push_back(std::move(rule));
  }
}

void ParseRuntimeNodeRestartTexts(const std::vector<std::string>& texts,
                                  uint32_t nodes,
                                  std::vector<uint32_t>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-restart-json must be a JSON object");
    }
    const uint32_t node = JsonUint32Field(value.as_object(), "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-restart-json node must be in 1..--nodes");
    }
    output.push_back(node - 1U);
  }
}

void ParseRuntimeNodeFreezeTexts(const std::vector<std::string>& texts,
                                 uint32_t nodes,
                                 std::vector<FreezeRequest>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-freeze-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-freeze-json node must be in 1..--nodes");
    }
    const uint32_t duration_ms = JsonUint32Field(object, "duration_ms");
    if (duration_ms == 0U) {
      throw std::runtime_error(
          "--runtime-node-freeze-json duration_ms must be greater than zero");
    }
    output.push_back(
        FreezeRequest{.node_index = node - 1U, .duration_ms = duration_ms});
  }
}

void ParseNodeNetworkConditions(Options& options) {
  ParseNodeNetworkConditionTexts(options.node_network_condition_json,
                                 options.nodes, "--node-network-condition-json",
                                 options.node_network_conditions);
  ParseNodeNetworkConditionTexts(options.runtime_node_network_condition_json,
                                 options.nodes,
                                 "--runtime-node-network-condition-json",
                                 options.runtime_node_network_conditions);
  ParseRuntimeNodeBlockTexts(options.runtime_node_block_json, options.nodes,
                             "--runtime-node-block-json",
                             options.runtime_node_blocks);
  ParseRuntimeNodeBlockTexts(options.runtime_node_unblock_json, options.nodes,
                             "--runtime-node-unblock-json",
                             options.runtime_node_unblocks);
  ParseRuntimePartitionTexts(options.runtime_partition_json, options.nodes,
                             "--runtime-partition-json",
                             options.runtime_partitions);
  ParseRuntimePartitionTexts(options.runtime_heal_partition_json, options.nodes,
                             "--runtime-heal-partition-json",
                             options.runtime_partition_heals);
  ParseRuntimeNodeResourceTexts(options.runtime_node_resource_json,
                                options.nodes,
                                options.runtime_node_resource_updates);
  ParseRuntimeNodeRestartTexts(options.runtime_node_restart_json, options.nodes,
                               options.runtime_node_restarts);
  ParseRuntimeNodeFreezeTexts(options.runtime_node_freeze_json, options.nodes,
                              options.runtime_node_freezes);
}

Options ParseOptions(int argc, char** argv) {
  namespace po = boost::program_options;
  Options options;

  const std::string nodes_help =
      "Firo regtest nodes, 1.." + std::to_string(kMaxFiroNodes);
  po::options_description desc("Allowed options");
  desc.add_options()("help", "show this help")(
      "scenario-json", po::value<std::filesystem::path>(&options.scenario_json),
      "Boost.JSON scenario file for the Firo MVP")(
      "scenario-yaml", po::value<std::filesystem::path>(&options.scenario_yaml),
      "libyaml scenario file for the Firo MVP")(
      "firod", po::value<std::filesystem::path>(&options.firod),
      "explicit firod binary")(
      "benchmark-root", po::value<std::filesystem::path>(&options.output_dir),
      "root directory for run data, node directories, metrics, events, and "
      "logs")("output-dir",
              po::value<std::filesystem::path>(&options.output_dir),
              "legacy alias for --benchmark-root")(
      "run-id", po::value<std::string>(&options.run_id), "safe run id")(
      "report-run", po::value<std::filesystem::path>(&options.report_run),
      "summarize an existing run directory as JSON and exit")(
      "run", po::value<std::filesystem::path>(&options.tui_run),
      "view an existing run directory in the integrated ncurses TUI")(
      "once", po::bool_switch(&options.tui_once),
      "render one integrated TUI frame and exit; requires --run")(
      "refresh-ms", po::value<std::uint32_t>(&options.tui_refresh_ms),
      "milliseconds between integrated TUI report refreshes")(
      "nodes", po::value<uint32_t>(&options.nodes), nodes_help.c_str())(
      "generate-blocks", po::value<uint32_t>(&options.generate_blocks),
      "blocks generated on --generate-node")(
      "generate-node", po::value<uint32_t>(&options.generate_node),
      "1-based node number that generates blocks")(
      "ready-timeout-sec", po::value<uint32_t>(&options.ready_timeout_sec),
      "RPC startup timeout")("sync-timeout-sec",
                             po::value<uint32_t>(&options.sync_timeout_sec),
                             "block propagation timeout")(
      "metrics-sample-count",
      po::value<uint32_t>(&options.metrics_sample_count),
      "extra metric samples to collect after runtime events and before "
      "workload generation")("metrics-interval-ms",
                             po::value<uint32_t>(&options.metrics_interval_ms),
                             "milliseconds between extra metric samples")(
      "memory-high-bytes", po::value<uint64_t>(&options.memory_high_bytes),
      "cgroup memory.high soft pressure threshold in bytes")(
      "memory-max-bytes", po::value<uint64_t>(&options.memory_max_bytes),
      "cgroup memory.max hard limit in bytes")(
      "cpu-quota-us", po::value<uint64_t>(&options.cpu_quota_us),
      "optional cgroup cpu.max quota in microseconds per period")(
      "cpu-period-us", po::value<uint64_t>(&options.cpu_period_us),
      "cgroup cpu.max period in microseconds")(
      "pids-max", po::value<uint64_t>(&options.pids_max),
      "cgroup pids.max process limit")(
      "keep-cgroups", po::bool_switch(&options.keep_cgroups),
      "leave cgroups after exit for inspection")(
      "cleanup-run", po::bool_switch(&options.cleanup_run),
      "remove stale simulator-owned veth and cgroup objects for --run-id and "
      "exit")("isolate-network", po::bool_switch(&options.isolate_network),
              "run each Firo node in its own network namespace and veth link")(
      "network-bandwidth-mbps",
      po::value<uint32_t>(&options.network_condition.bandwidth_mbps),
      "TBF bandwidth limit in megabits per second for each isolated node "
      "host-side veth")(
      "network-delay-ms",
      po::value<uint32_t>(&options.network_condition.delay_ms),
      "netem delay applied to each isolated node host-side veth")(
      "network-jitter-ms",
      po::value<uint32_t>(&options.network_condition.jitter_ms),
      "netem jitter applied to each isolated node host-side veth")(
      "network-loss-bps",
      po::value<uint32_t>(&options.network_condition.loss_basis_points),
      "netem packet loss in basis points, 10000 = 100%")(
      "network-duplicate-bps",
      po::value<uint32_t>(&options.network_condition.duplicate_basis_points),
      "netem packet duplication in basis points, 10000 = 100%")(
      "network-corrupt-bps",
      po::value<uint32_t>(&options.network_condition.corrupt_basis_points),
      "netem packet corruption in basis points, 10000 = 100%")(
      "network-reorder-bps",
      po::value<uint32_t>(&options.network_condition.reorder_basis_points),
      "netem packet reordering in basis points, 10000 = 100%")(
      "network-limit-packets",
      po::value<uint32_t>(&options.network_condition.limit_packets),
      "netem queue limit applied to each isolated node host-side veth")(
      "node-network-condition-json",
      po::value<std::vector<std::string>>(&options.node_network_condition_json)
          ->composing(),
      "repeatable JSON object with node plus network condition fields for one "
      "isolated "
      "node")(
      "runtime-node-network-condition-json",
      po::value<std::vector<std::string>>(
          &options.runtime_node_network_condition_json)
          ->composing(),
      "repeatable JSON object with node plus live network condition fields to "
      "apply after isolated nodes are running")(
      "runtime-node-block-json",
      po::value<std::vector<std::string>>(&options.runtime_node_block_json)
          ->composing(),
      "repeatable JSON object with node, optional src_address, dst_address, "
      "dst_port, and optional handle for one live host-side TCP drop filter")(
      "runtime-node-unblock-json",
      po::value<std::vector<std::string>>(&options.runtime_node_unblock_json)
          ->composing(),
      "repeatable JSON object with node, optional src_address, dst_address, "
      "dst_port, and optional handle for one live host-side TCP drop filter "
      "removal")(
      "runtime-partition-json",
      po::value<std::vector<std::string>>(&options.runtime_partition_json)
          ->composing(),
      "repeatable JSON object with group_a and group_b arrays for one live "
      "source-aware group partition")(
      "runtime-heal-partition-json",
      po::value<std::vector<std::string>>(&options.runtime_heal_partition_json)
          ->composing(),
      "repeatable JSON object with group_a and group_b arrays for one live "
      "source-aware group partition heal")(
      "runtime-node-resource-json",
      po::value<std::vector<std::string>>(&options.runtime_node_resource_json)
          ->composing(),
      "repeatable JSON object with node plus live cgroup limit fields to apply "
      "after nodes are running")(
      "runtime-node-restart-json",
      po::value<std::vector<std::string>>(&options.runtime_node_restart_json)
          ->composing(),
      "repeatable JSON object with node field for one live node restart after "
      "nodes are running")(
      "runtime-node-freeze-json",
      po::value<std::vector<std::string>>(&options.runtime_node_freeze_json)
          ->composing(),
      "repeatable JSON object with node and duration_ms for one live cgroup "
      "freeze/thaw after nodes are running")(
      "replace-run", po::bool_switch(&options.replace_run),
      "remove an existing run directory first")(
      "probe-address", po::bool_switch(&options.probe_address),
      "assign and inspect an IPv4 address inside a temporary netns through "
      "libmnl")(
      "probe-bandwidth-limit", po::bool_switch(&options.probe_bandwidth_limit),
      "apply and remove a TBF bandwidth limit on a temporary veth peer through "
      "libmnl")(
      "probe-capabilities", po::bool_switch(&options.probe_capabilities),
      "report effective Linux capabilities needed by privileged simulator "
      "paths")(
      "probe-cgroup-freeze", po::bool_switch(&options.probe_cgroup_freeze),
      "attach a child process to a cgroup and verify cgroup.freeze/thaw "
      "paths")(
      "probe-drop-filter", po::bool_switch(&options.probe_drop_filter),
      "apply and remove a flower/gact TCP drop filter on a temporary veth "
      "through libmnl")("probe-netns", po::bool_switch(&options.probe_netns),
                        "create a temporary network namespace and inspect it "
                        "through setns/libmnl")(
      "probe-network-condition",
      po::bool_switch(&options.probe_network_condition),
      "apply and remove a netem network condition on a temporary veth peer "
      "through libmnl")(
      "probe-combined-network-condition",
      po::bool_switch(&options.probe_combined_network_condition),
      "apply and remove a combined TBF/netem condition on a temporary veth "
      "peer through libmnl")(
      "probe-network-condition-update",
      po::bool_switch(&options.probe_network_condition_update),
      "replace a live host-side netem network condition on a temporary veth "
      "through libmnl")(
      "probe-qdisc", po::bool_switch(&options.probe_qdisc),
      "dump qdisc state for a temporary veth peer through libmnl")(
      "probe-qdisc-mutation", po::bool_switch(&options.probe_qdisc_mutation),
      "replace and delete a root pfifo qdisc on a temporary veth peer through "
      "libmnl")("probe-route", po::bool_switch(&options.probe_route),
                "assign and inspect an IPv4 route inside a temporary netns "
                "through libmnl")(
      "probe-veth", po::bool_switch(&options.probe_veth),
      "create, move, inspect, and delete a temporary veth pair through libmnl")(
      "probe-network", po::bool_switch(&options.probe_network),
      "list links through rtnetlink/libmnl and exit");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") != 0U) {
    BSIM_LOG(info) << "Usage: " << argv[0] << " [options]\n" << desc;
    std::exit(0);
  }
  if (vm.count("scenario-json") != 0U && vm.count("scenario-yaml") != 0U) {
    throw std::runtime_error(
        "--scenario-json and --scenario-yaml are mutually exclusive");
  }
  if (OptionProvided(vm, "benchmark-root") &&
      OptionProvided(vm, "output-dir")) {
    throw std::runtime_error(
        "--benchmark-root and --output-dir are aliases and must not both be "
        "provided");
  }
  if (vm.count("run") != 0U && vm.count("report-run") != 0U) {
    throw std::runtime_error("--run and --report-run are mutually exclusive");
  }
  if (vm.count("run") == 0U &&
      (OptionProvided(vm, "once") || OptionProvided(vm, "refresh-ms"))) {
    throw std::runtime_error("--once and --refresh-ms require --run");
  }
  if (vm.count("run") != 0U && options.tui_refresh_ms == 0U) {
    throw std::runtime_error("--refresh-ms must be greater than zero");
  }
  if (vm.count("scenario-json") != 0U) {
    const boost::json::value scenario =
        boost::json::parse(ReadText(options.scenario_json));
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-json root must be a JSON object");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
  }
  if (vm.count("scenario-yaml") != 0U) {
    const boost::json::value scenario = ParseYamlDocument(
        ReadText(options.scenario_yaml), options.scenario_yaml);
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-yaml root must be a YAML mapping");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
  }
  options.network_condition_requested =
      options.network_condition_requested ||
      vm.count("network-bandwidth-mbps") != 0U ||
      vm.count("network-delay-ms") != 0U ||
      vm.count("network-jitter-ms") != 0U ||
      vm.count("network-loss-bps") != 0U ||
      vm.count("network-duplicate-bps") != 0U ||
      vm.count("network-corrupt-bps") != 0U ||
      vm.count("network-reorder-bps") != 0U ||
      vm.count("network-limit-packets") != 0U;
  options.cpu_quota_requested =
      options.cpu_quota_requested || vm.count("cpu-quota-us") != 0U;
  if (options.memory_high_bytes > options.memory_max_bytes) {
    throw std::runtime_error(
        "--memory-high-bytes must be less than or equal to --memory-max-bytes");
  }
  if (options.cpu_period_us == 0U) {
    throw std::runtime_error("--cpu-period-us must be greater than zero");
  }
  if (options.cpu_quota_requested && options.cpu_quota_us == 0U) {
    throw std::runtime_error("--cpu-quota-us must be greater than zero");
  }
  if (options.pids_max == 0U) {
    throw std::runtime_error("--pids-max must be greater than zero");
  }
  if (options.metrics_sample_count > 0U && options.metrics_interval_ms == 0U) {
    throw std::runtime_error(
        "--metrics-interval-ms must be greater than zero when sampling");
  }
  if ((options.network_condition_requested ||
       !options.node_network_condition_json.empty() ||
       !options.node_network_conditions.empty() ||
       !options.runtime_node_network_condition_json.empty() ||
       !options.runtime_node_network_conditions.empty() ||
       !options.runtime_node_block_json.empty() ||
       !options.runtime_node_blocks.empty() ||
       !options.runtime_node_unblock_json.empty() ||
       !options.runtime_node_unblocks.empty() ||
       !options.runtime_partition_json.empty() ||
       !options.runtime_partitions.empty() ||
       !options.runtime_heal_partition_json.empty() ||
       !options.runtime_partition_heals.empty() ||
       WorkloadsRequireIsolatedNetwork(options)) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network runtime options require --isolate-network");
  }
  if (options.nodes < 1 || options.nodes > kMaxFiroNodes) {
    throw std::runtime_error("--nodes currently supports 1.." +
                             std::to_string(kMaxFiroNodes) +
                             " for Firo smoke runs");
  }
  if (options.generate_node == 0U || options.generate_node > options.nodes) {
    throw std::runtime_error("--generate-node must be in 1..--nodes");
  }
  for (const ScenarioWorkload& workload : options.workloads) {
    if (workload.kind == WorkloadKind::kBlockGeneration) {
      if (workload.block_generation.node == 0U ||
          workload.block_generation.node > options.nodes) {
        throw std::runtime_error(
            "scenario block_generation workload node must be in 1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kWaitUntilHeight) {
      if (workload.wait_until_height.node == 0U ||
          workload.wait_until_height.node > options.nodes) {
        throw std::runtime_error(
            "scenario wait_until_height workload node must be in 1..--nodes");
      }
      if (workload.wait_until_height.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario wait_until_height timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kWaitForPeers) {
      if (workload.wait_for_peers.node == 0U ||
          workload.wait_for_peers.node > options.nodes) {
        throw std::runtime_error(
            "scenario wait_for_peers workload node must be in 1..--nodes");
      }
      if (workload.wait_for_peers.peer_count == 0U) {
        throw std::runtime_error(
            "scenario wait_for_peers peer_count must be greater than zero");
      }
      if (workload.wait_for_peers.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario wait_for_peers timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kConnectPeer) {
      if (workload.connect_peer.node == 0U ||
          workload.connect_peer.node > options.nodes) {
        throw std::runtime_error(
            "scenario connect_peer workload node must be in 1..--nodes");
      }
      if (workload.connect_peer.peer == 0U ||
          workload.connect_peer.peer > options.nodes) {
        throw std::runtime_error(
            "scenario connect_peer workload peer must be in 1..--nodes");
      }
      if (workload.connect_peer.node == workload.connect_peer.peer) {
        throw std::runtime_error(
            "scenario connect_peer workload node and peer must differ");
      }
      if (workload.connect_peer.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario connect_peer timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kDisconnectPeer) {
      if (workload.disconnect_peer.node == 0U ||
          workload.disconnect_peer.node > options.nodes) {
        throw std::runtime_error(
            "scenario disconnect_peer workload node must be in 1..--nodes");
      }
      if (workload.disconnect_peer.peer == 0U ||
          workload.disconnect_peer.peer > options.nodes) {
        throw std::runtime_error(
            "scenario disconnect_peer workload peer must be in 1..--nodes");
      }
      if (workload.disconnect_peer.node == workload.disconnect_peer.peer) {
        throw std::runtime_error(
            "scenario disconnect_peer workload node and peer must differ");
      }
      if (workload.disconnect_peer.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario disconnect_peer timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kRestartNode) {
      if (workload.restart_node.node == 0U ||
          workload.restart_node.node > options.nodes) {
        throw std::runtime_error(
            "scenario restart_node workload node must be in 1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kFreezeNode) {
      if (workload.freeze_node.node == 0U ||
          workload.freeze_node.node > options.nodes) {
        throw std::runtime_error(
            "scenario freeze_node workload node must be in 1..--nodes");
      }
      if (workload.freeze_node.duration_ms == 0U) {
        throw std::runtime_error(
            "scenario freeze_node duration_ms must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kUpdateResourceLimits) {
      if (workload.update_resource_limits.node == 0U ||
          workload.update_resource_limits.node > options.nodes) {
        throw std::runtime_error(
            "scenario update_resource_limits workload node must be in "
            "1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kPartitionNodes) {
      ValidateNetworkPartitionRule(workload.network_partition.partition,
                                   options.nodes,
                                   "scenario partition_nodes workload");
    } else if (workload.kind == WorkloadKind::kHealPartition) {
      ValidateNetworkPartitionRule(workload.network_partition.partition,
                                   options.nodes,
                                   "scenario heal_partition workload");
    } else if (workload.kind == WorkloadKind::kSendRawTransaction) {
      const SendRawTransactionWorkload& transaction =
          workload.send_raw_transaction;
      if (transaction.funding_node == 0U ||
          transaction.funding_node > options.nodes) {
        throw std::runtime_error(
            "scenario send_raw_transaction funding_node must be in "
            "1..--nodes");
      }
      if (transaction.submit_node == 0U ||
          transaction.submit_node > options.nodes) {
        throw std::runtime_error(
            "scenario send_raw_transaction submit_node must be in 1..--nodes");
      }
      if (transaction.source_address == transaction.destination_address) {
        throw std::runtime_error(
            "scenario send_raw_transaction source_address and "
            "destination_address must differ");
      }
      if (transaction.funding_blocks < kFiroCoinbaseSpendableConfirmations) {
        throw std::runtime_error(
            "scenario send_raw_transaction funding_blocks must be at least " +
            std::to_string(kFiroCoinbaseSpendableConfirmations));
      }
      if (transaction.amount_satoshis == 0U) {
        throw std::runtime_error(
            "scenario send_raw_transaction amount must be greater than zero");
      }
      if (transaction.fee_satoshis == 0U) {
        throw std::runtime_error(
            "scenario send_raw_transaction fee must be greater than zero");
      }
      if (transaction.amount_satoshis >
          std::numeric_limits<uint64_t>::max() - transaction.fee_satoshis) {
        throw std::runtime_error(
            "scenario send_raw_transaction amount plus fee overflows uint64");
      }
      if (transaction.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario send_raw_transaction timeout_sec must be greater than "
            "zero");
      }
    } else if (workload.kind == WorkloadKind::kWalletTransactions) {
      ValidateWalletTransactionsWorkload(workload.wallet_transactions, options);
    }
  }
  if (options.topology.configured) {
    SimulationRegistry::FromTopology(options.topology,
                                     options.wallet_initialization);
  }
  ParseNodeNetworkConditions(options);
  RequireSafeRunId(options.run_id);
  const bool needs_firod =
      !options.probe_network && options.report_run.empty() &&
      options.tui_run.empty() && !options.probe_bandwidth_limit &&
      !options.probe_capabilities && !options.probe_cgroup_freeze &&
      !options.probe_drop_filter && !options.probe_netns &&
      !options.probe_veth && !options.probe_address && !options.probe_route &&
      !options.probe_qdisc && !options.probe_qdisc_mutation &&
      !options.probe_network_condition &&
      !options.probe_combined_network_condition &&
      !options.probe_network_condition_update && !options.cleanup_run;
  if (needs_firod && options.firod.empty()) {
    throw std::runtime_error(
        "Firo runs require an explicit --firod path or scenario firod field");
  }
  if (needs_firod) {
    RequireExecutable(options.firod);
  }
  return options;
}

boost::json::array LinksJson(const std::vector<LinkInfo>& links) {
  boost::json::array links_json;
  for (const LinkInfo& link : links) {
    boost::json::object link_json;
    link_json["index"] = link.index;
    link_json["name"] = link.name;
    link_json["up"] = link.up;
    link_json["has_stats"] = link.has_stats;
    link_json["rx_bytes"] = link.rx_bytes;
    link_json["tx_bytes"] = link.tx_bytes;
    link_json["rx_packets"] = link.rx_packets;
    link_json["tx_packets"] = link.tx_packets;
    link_json["rx_dropped"] = link.rx_dropped;
    link_json["tx_dropped"] = link.tx_dropped;
    link_json["rx_errors"] = link.rx_errors;
    link_json["tx_errors"] = link.tx_errors;
    links_json.push_back(std::move(link_json));
  }
  return links_json;
}

boost::json::array AddressesJson(const std::vector<AddressInfo>& addresses) {
  boost::json::array addresses_json;
  for (const AddressInfo& address : addresses) {
    boost::json::object address_json;
    address_json["if_index"] = address.if_index;
    address_json["if_name"] = address.if_name;
    address_json["address"] = address.address;
    address_json["prefix_len"] = address.prefix_len;
    addresses_json.push_back(std::move(address_json));
  }
  return addresses_json;
}

boost::json::array RoutesJson(const std::vector<RouteInfo>& routes) {
  boost::json::array routes_json;
  for (const RouteInfo& route : routes) {
    boost::json::object route_json;
    route_json["destination"] = route.destination;
    route_json["prefix_len"] = route.prefix_len;
    route_json["oif_index"] = route.oif_index;
    route_json["oif_name"] = route.oif_name;
    route_json["gateway"] = route.gateway;
    route_json["table"] = route.table;
    route_json["protocol"] = route.protocol;
    route_json["scope"] = route.scope;
    route_json["type"] = route.type;
    routes_json.push_back(std::move(route_json));
  }
  return routes_json;
}

boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs) {
  boost::json::array qdiscs_json;
  for (const QdiscInfo& qdisc : qdiscs) {
    boost::json::object qdisc_json;
    qdisc_json["if_index"] = qdisc.if_index;
    qdisc_json["if_name"] = qdisc.if_name;
    qdisc_json["kind"] = qdisc.kind;
    qdisc_json["handle"] = qdisc.handle;
    qdisc_json["parent"] = qdisc.parent;
    qdisc_json["info"] = qdisc.info;
    qdisc_json["has_stats"] = qdisc.has_stats;
    qdisc_json["bytes"] = qdisc.bytes;
    qdisc_json["packets"] = qdisc.packets;
    qdisc_json["drops"] = qdisc.drops;
    qdisc_json["overlimits"] = qdisc.overlimits;
    qdisc_json["qlen"] = qdisc.qlen;
    qdisc_json["backlog"] = qdisc.backlog;
    qdisc_json["requeues"] = qdisc.requeues;
    qdisc_json["has_netem_options"] = qdisc.has_netem_options;
    qdisc_json["netem_latency_us"] = qdisc.netem_latency_us;
    qdisc_json["netem_jitter_us"] = qdisc.netem_jitter_us;
    qdisc_json["netem_loss"] = qdisc.netem_loss;
    qdisc_json["netem_duplicate"] = qdisc.netem_duplicate;
    qdisc_json["netem_corrupt"] = qdisc.netem_corrupt;
    qdisc_json["netem_reorder"] = qdisc.netem_reorder;
    qdisc_json["netem_limit_packets"] = qdisc.netem_limit_packets;
    qdisc_json["has_tbf_options"] = qdisc.has_tbf_options;
    qdisc_json["tbf_rate_bytes_per_sec"] = qdisc.tbf_rate_bytes_per_sec;
    qdisc_json["tbf_limit_bytes"] = qdisc.tbf_limit_bytes;
    qdisc_json["tbf_buffer_ticks"] = qdisc.tbf_buffer_ticks;
    qdisc_json["tbf_mtu_ticks"] = qdisc.tbf_mtu_ticks;
    qdiscs_json.push_back(std::move(qdisc_json));
  }
  return qdiscs_json;
}

boost::json::array TcFiltersJson(const std::vector<TcFilterInfo>& filters) {
  boost::json::array filters_json;
  for (const TcFilterInfo& filter : filters) {
    boost::json::object filter_json;
    filter_json["if_index"] = filter.if_index;
    filter_json["if_name"] = filter.if_name;
    filter_json["kind"] = filter.kind;
    filter_json["handle"] = filter.handle;
    filter_json["parent"] = filter.parent;
    filter_json["priority"] = filter.priority;
    filter_json["protocol"] = filter.protocol;
    filter_json["egress"] = filter.egress;
    filter_json["ingress"] = filter.ingress;
    filter_json["has_eth_type"] = filter.has_eth_type;
    filter_json["eth_type"] = filter.eth_type;
    filter_json["has_ip_proto"] = filter.has_ip_proto;
    filter_json["ip_proto"] = filter.ip_proto;
    filter_json["has_ipv4_src"] = filter.has_ipv4_src;
    filter_json["ipv4_src"] = filter.ipv4_src;
    filter_json["has_ipv4_src_mask"] = filter.has_ipv4_src_mask;
    filter_json["ipv4_src_mask"] = filter.ipv4_src_mask;
    filter_json["has_ipv4_dst"] = filter.has_ipv4_dst;
    filter_json["ipv4_dst"] = filter.ipv4_dst;
    filter_json["has_ipv4_dst_mask"] = filter.has_ipv4_dst_mask;
    filter_json["ipv4_dst_mask"] = filter.ipv4_dst_mask;
    filter_json["has_tcp_dst"] = filter.has_tcp_dst;
    filter_json["tcp_dst"] = filter.tcp_dst;
    filter_json["has_tcp_dst_mask"] = filter.has_tcp_dst_mask;
    filter_json["tcp_dst_mask"] = filter.tcp_dst_mask;
    filter_json["has_drop_action"] = filter.has_drop_action;
    filters_json.push_back(std::move(filter_json));
  }
  return filters_json;
}

boost::json::object NetworkConditionJson(const NetworkCondition& condition) {
  boost::json::object object;
  object["bandwidth_mbps"] = condition.bandwidth_mbps;
  object["delay_ms"] = condition.delay_ms;
  object["jitter_ms"] = condition.jitter_ms;
  object["loss_basis_points"] = condition.loss_basis_points;
  object["duplicate_basis_points"] = condition.duplicate_basis_points;
  object["corrupt_basis_points"] = condition.corrupt_basis_points;
  object["reorder_basis_points"] = condition.reorder_basis_points;
  object["limit_packets"] = condition.limit_packets;
  return object;
}

boost::json::object NetworkBlockRuleJson(const NetworkBlockRule& rule) {
  boost::json::object object;
  object["node"] = rule.node_index + 1U;
  if (!rule.src_address.empty()) {
    object["src_address"] = rule.src_address;
  }
  object["dst_address"] = rule.dst_address;
  object["dst_port"] = rule.dst_port;
  object["handle"] = rule.handle;
  return object;
}

boost::json::array NodeGroupJson(const std::vector<uint32_t>& nodes) {
  boost::json::array array;
  for (uint32_t node_index : nodes) {
    array.push_back(node_index + 1U);
  }
  return array;
}

boost::json::object NetworkPartitionRuleJson(const NetworkPartitionRule& rule) {
  boost::json::object object;
  object["group_a"] = NodeGroupJson(rule.group_a);
  object["group_b"] = NodeGroupJson(rule.group_b);
  return object;
}

boost::json::object WalletInitializationJson(
    const WalletInitialization& initialization) {
  boost::json::object object;
  object["strategy"] =
      std::string(WalletInitializationStrategyName(initialization.strategy));
  object["mode"] = std::string(WalletPrivacyModeName(initialization.mode));
  if (!initialization.seed.empty()) {
    object["seed"] = initialization.seed;
  }
  return object;
}

boost::json::object NodeRoleTopologyJson(
    const NodeRoleTopology& topology,
    const WalletInitialization& wallet_initialization) {
  boost::json::object object;
  object["node_count"] = topology.node_count;
  object["wallet_node_count"] = topology.wallet_node_count;
  object["miner_node_count"] = topology.miner_node_count;
  object["allow_miner_wallet_overlap"] = topology.allow_miner_wallet_overlap;
  object["wallet_nodes"] = NodeGroupJson(topology.wallet_nodes);
  object["miner_nodes"] = NodeGroupJson(topology.miner_nodes);
  object["wallet_initialization"] =
      WalletInitializationJson(wallet_initialization);
  return object;
}

boost::json::object ResourceLimitsJson(const ResourceLimits& limits) {
  boost::json::object object;
  object["memory_high_bytes"] = limits.memory_high_bytes;
  object["memory_max_bytes"] = limits.memory_max_bytes;
  if (limits.cpu_quota_us) {
    object["cpu_quota_us"] = *limits.cpu_quota_us;
  } else {
    object["cpu_quota_us"] = nullptr;
  }
  object["cpu_period_us"] = limits.cpu_period_us;
  object["pids_max"] = limits.pids_max;
  return object;
}

boost::json::object ResourceLimitPatchJson(const ResourceLimitPatch& patch) {
  boost::json::object object;
  if (patch.memory_high_bytes) {
    object["memory_high_bytes"] = *patch.memory_high_bytes;
  }
  if (patch.memory_max_bytes) {
    object["memory_max_bytes"] = *patch.memory_max_bytes;
  }
  if (patch.cpu_quota_present) {
    if (patch.cpu_quota_us) {
      object["cpu_quota_us"] = *patch.cpu_quota_us;
    } else {
      object["cpu_quota_us"] = nullptr;
    }
  }
  if (patch.cpu_period_us) {
    object["cpu_period_us"] = *patch.cpu_period_us;
  }
  if (patch.pids_max) {
    object["pids_max"] = *patch.pids_max;
  }
  return object;
}

ResourceLimits InitialResourceLimits(const Options& options) {
  return ResourceLimits{
      .memory_high_bytes = options.memory_high_bytes,
      .memory_max_bytes = options.memory_max_bytes,
      .cpu_quota_us = options.cpu_quota_requested
                          ? std::optional<uint64_t>(options.cpu_quota_us)
                          : std::nullopt,
      .cpu_period_us = options.cpu_period_us,
      .pids_max = options.pids_max,
  };
}

ResourceLimits ApplyResourceLimitPatch(const ResourceLimits& current,
                                       const ResourceLimitPatch& patch,
                                       const std::string& node_id) {
  ResourceLimits next = current;
  if (patch.memory_high_bytes) {
    next.memory_high_bytes = *patch.memory_high_bytes;
  }
  if (patch.memory_max_bytes) {
    next.memory_max_bytes = *patch.memory_max_bytes;
  }
  if (patch.cpu_quota_present) {
    next.cpu_quota_us = patch.cpu_quota_us;
  }
  if (patch.cpu_period_us) {
    next.cpu_period_us = *patch.cpu_period_us;
  }
  if (patch.pids_max) {
    next.pids_max = *patch.pids_max;
  }
  if (next.memory_high_bytes > next.memory_max_bytes) {
    throw std::runtime_error("runtime resource update for " + node_id +
                             " would make memory_high_bytes exceed "
                             "memory_max_bytes");
  }
  RequireNonZero(next.memory_max_bytes, "memory_max_bytes");
  RequireNonZero(next.cpu_period_us, "cpu_period_us");
  if (next.cpu_quota_us) {
    RequireNonZero(*next.cpu_quota_us, "cpu_quota_us");
  }
  RequireNonZero(next.pids_max, "pids_max");
  return next;
}

void WriteResourceLimits(const Cgroup& cgroup, const ResourceLimits& previous,
                         const ResourceLimits& next) {
  if (next.memory_max_bytes != previous.memory_max_bytes &&
      next.memory_max_bytes > previous.memory_max_bytes) {
    cgroup.SetMemoryMax(next.memory_max_bytes);
  }
  if (next.memory_high_bytes != previous.memory_high_bytes) {
    cgroup.SetMemoryHigh(next.memory_high_bytes);
  }
  if (next.memory_max_bytes != previous.memory_max_bytes &&
      next.memory_max_bytes <= previous.memory_max_bytes) {
    cgroup.SetMemoryMax(next.memory_max_bytes);
  }
  if (next.cpu_quota_us != previous.cpu_quota_us ||
      next.cpu_period_us != previous.cpu_period_us) {
    cgroup.SetCpuMax(next.cpu_quota_us, next.cpu_period_us);
  }
  if (next.pids_max != previous.pids_max) {
    cgroup.SetPidsMax(next.pids_max);
  }
}

std::string NetworkConditionVerificationDetail(const NodeVethConfig& config,
                                               const QdiscInfo& qdisc) {
  boost::json::object detail;
  detail["host_if"] = config.host_name;
  detail["condition"] = NetworkConditionJson(config.condition);
  detail["qdisc_kind"] = qdisc.kind;
  detail["qdisc_handle"] = qdisc.handle;
  detail["qdisc_parent"] = qdisc.parent;
  return boost::json::serialize(detail);
}

std::string NodeHostAddress(uint32_t node_index) {
  return "10.210." + std::to_string(node_index + 1U) + ".1";
}

std::string NodeAddress(uint32_t node_index) {
  return "10.210." + std::to_string(node_index + 1U) + ".2";
}

uint32_t StableRunHash(std::string_view run_id) {
  uint32_t hash = 2166136261U;
  for (const unsigned char c : run_id) {
    hash ^= c;
    hash *= 16777619U;
  }
  return hash;
}

std::string RunInterfaceToken(std::string_view run_id) {
  constexpr char kHex[] = "0123456789abcdef";
  uint32_t value = StableRunHash(run_id) & 0x00FFFFFFU;
  std::string token(6, '0');
  for (int i = 5; i >= 0; --i) {
    token[static_cast<size_t>(i)] = kHex[value & 0x0FU];
    value >>= 4U;
  }
  return token;
}

std::string NodeInterfaceName(std::string_view run_id, uint32_t node_index,
                              char suffix) {
  return "bs" + RunInterfaceToken(run_id) + "n" +
         std::to_string(node_index + 1U) + suffix;
}

NodeVethConfig MakeNodeVethConfig(const Options& options, uint32_t node_index) {
  NodeVethConfig config;
  config.host_name = NodeInterfaceName(options.run_id, node_index, 'h');
  config.peer_name = NodeInterfaceName(options.run_id, node_index, 'p');
  config.host_address = NodeHostAddress(node_index);
  config.node_address = NodeAddress(node_index);
  config.prefix_len = 30;
  const auto node_condition = options.node_network_conditions.find(node_index);
  if (node_condition != options.node_network_conditions.end()) {
    config.apply_condition = true;
    config.condition = node_condition->second;
  } else {
    config.apply_condition = options.network_condition_requested;
    config.condition = options.network_condition;
  }
  return config;
}

std::string FiroPeerHost(const Options& options, uint32_t node_index) {
  if (options.isolate_network) {
    return NodeAddress(node_index);
  }
  return "127.0.0.1";
}

bool HostIpv4ForwardingEnabled() {
  const std::string value = ReadText("/proc/sys/net/ipv4/ip_forward");
  return !value.empty() && value.front() == '1';
}

void RequireSafeOutputDirectory(const std::filesystem::path& output_dir) {
  if (output_dir.empty()) {
    throw std::runtime_error("output directory must not be empty");
  }
  const std::filesystem::path absolute = std::filesystem::absolute(output_dir);
  if (absolute == absolute.root_path()) {
    throw std::runtime_error("output directory must not be filesystem root");
  }
}

bool IsOwnedRunDirectory(const std::filesystem::path& run_root) {
  return std::filesystem::exists(run_root / kRunMarkerFile) ||
         std::filesystem::exists(run_root / "resolved-scenario.json");
}

void RequireOwnedRunDirectory(const std::filesystem::path& run_root) {
  if (!std::filesystem::is_directory(run_root)) {
    throw std::runtime_error("run path exists but is not a directory: " +
                             run_root.string());
  }
  if (!IsOwnedRunDirectory(run_root)) {
    throw std::runtime_error(
        "refusing to remove directory without simulator marker: " +
        run_root.string());
  }
}

const LinkInfo* FindLinkByName(const std::vector<LinkInfo>& links,
                               std::string_view name) {
  for (const LinkInfo& link : links) {
    if (link.name == name) {
      return &link;
    }
  }
  return nullptr;
}

const QdiscInfo* FindQdiscByInterfaceName(const std::vector<QdiscInfo>& qdiscs,
                                          std::string_view name) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == name) {
      return &qdisc;
    }
  }
  return nullptr;
}

QdiscInfo VerifyNodeNetworkCondition(const NodeVethConfig& config) {
  const std::vector<QdiscInfo> qdiscs = ListQdiscs();
  QdiscInfo summary;
  if (!QdiscsMatchNetworkCondition(qdiscs, config.host_name, config.condition,
                                   &summary)) {
    throw std::runtime_error(
        "host-side qdisc does not match requested network condition: " +
        config.host_name);
  }
  return summary;
}

std::string NetworkProbeJson() {
  boost::json::object result;
  result["links"] = LinksJson(ListNetworkLinks());
  result["ipv4_addresses"] = AddressesJson(ListIpv4Addresses());
  result["ipv4_routes"] = RoutesJson(ListIpv4Routes());
  result["qdiscs"] = QdiscsJson(ListQdiscs());
  result["tc_filters"] = TcFiltersJson(ListTcFilters());
  return boost::json::serialize(result);
}

std::string CapabilityProbeJson() {
  boost::json::object result;
  result["cap_sys_admin"] = HasEffectiveCapability(CAP_SYS_ADMIN);
  result["cap_net_admin"] = HasEffectiveCapability(CAP_NET_ADMIN);
  result["cap_sys_resource"] = HasEffectiveCapability(CAP_SYS_RESOURCE);
  return boost::json::serialize(result);
}

std::string CgroupFreezeProbeJson() {
  CgroupFreezeProbe probe = Cgroup::ProbeFreezeThaw();
  boost::json::object result;
  result["run_id"] = probe.run_id;
  result["node_id"] = probe.node_id;
  result["child_pid"] = probe.child_pid;
  result["frozen_after_freeze"] = probe.frozen_after_freeze;
  result["frozen_after_thaw"] = probe.frozen_after_thaw;
  return boost::json::serialize(result);
}

std::string NetworkNamespaceProbeJson() {
  NetworkNamespaceProbe probe = ProbeIsolatedNetworkNamespace();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["parent_links"] = LinksJson(probe.parent_links);
  result["namespace_links"] = LinksJson(probe.namespace_links);
  return boost::json::serialize(result);
}

std::string VethProbeJson() {
  VethProbe probe = ProbeVethPair();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["parent_before"] = LinksJson(probe.parent_before);
  result["parent_after_create"] = LinksJson(probe.parent_after_create);
  result["parent_after_move"] = LinksJson(probe.parent_after_move);
  result["namespace_after_move"] = LinksJson(probe.namespace_after_move);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string NetworkConditionProbeJson() {
  NetworkConditionProbe probe = ProbeNetworkCondition();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string CombinedNetworkConditionProbeJson() {
  NetworkConditionProbe probe = ProbeCombinedNetworkCondition();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string BandwidthLimitProbeJson() {
  BandwidthLimitProbe probe = ProbeBandwidthLimit();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string NetworkConditionUpdateProbeJson() {
  NetworkConditionUpdateProbe probe = ProbeNetworkConditionUpdate();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["initial_condition"] = NetworkConditionJson(probe.initial_condition);
  result["updated_condition"] = NetworkConditionJson(probe.updated_condition);
  result["parent_qdiscs_after_initial"] =
      QdiscsJson(probe.parent_qdiscs_after_initial);
  result["parent_qdiscs_after_update"] =
      QdiscsJson(probe.parent_qdiscs_after_update);
  result["parent_qdiscs_after_delete"] =
      QdiscsJson(probe.parent_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string DropFilterProbeJson() {
  DropFilterProbe probe = ProbeDropFilter();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["dst_address"] = probe.dst_address;
  result["dst_port"] = probe.dst_port;
  result["handle"] = probe.handle;
  result["parent_filters_before"] = TcFiltersJson(probe.parent_filters_before);
  result["parent_filters_after_apply"] =
      TcFiltersJson(probe.parent_filters_after_apply);
  result["parent_filters_after_delete"] =
      TcFiltersJson(probe.parent_filters_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string QdiscProbeJson() {
  QdiscProbe probe = ProbeQdiscDump();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["namespace_links"] = LinksJson(probe.namespace_links);
  result["namespace_qdiscs"] = QdiscsJson(probe.namespace_qdiscs);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string QdiscMutationProbeJson() {
  QdiscMutationProbe probe = ProbeQdiscMutation();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["pfifo_limit_packets"] = probe.pfifo_limit_packets;
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_replace"] =
      QdiscsJson(probe.namespace_qdiscs_after_replace);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string RouteProbeJson() {
  RouteProbe probe = ProbeIpv4RouteAssignment();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["assigned_address"] = probe.assigned_address;
  result["assigned_prefix_len"] = probe.assigned_prefix_len;
  result["route_destination"] = probe.route_destination;
  result["route_prefix_len"] = probe.route_prefix_len;
  result["namespace_links_after_route"] =
      LinksJson(probe.namespace_links_after_route);
  result["namespace_addresses"] = AddressesJson(probe.namespace_addresses);
  result["namespace_routes"] = RoutesJson(probe.namespace_routes);
  result["namespace_addresses_after_delete"] =
      AddressesJson(probe.namespace_addresses_after_delete);
  result["namespace_routes_after_delete"] =
      RoutesJson(probe.namespace_routes_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string AddressProbeJson() {
  AddressProbe probe = ProbeIpv4AddressAssignment();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["assigned_address"] = probe.assigned_address;
  result["assigned_prefix_len"] = probe.assigned_prefix_len;
  result["parent_after_move"] = LinksJson(probe.parent_after_move);
  result["namespace_links_after_address"] =
      LinksJson(probe.namespace_links_after_address);
  result["namespace_addresses"] = AddressesJson(probe.namespace_addresses);
  result["namespace_addresses_after_delete"] =
      AddressesJson(probe.namespace_addresses_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string MetricsJson(const std::string& run_id, const std::string& node_id,
                        const FiroMetrics& chain,
                        uint64_t generated_block_count, uint64_t restart_count,
                        const CgroupMetrics* cgroup, const LinkInfo* link,
                        const QdiscInfo* qdisc) {
  boost::json::object object;
  object["timestamp_ms"] = NowUnixMillis();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["chain_version"] = chain.version;
  object["chain_protocol_version"] = chain.protocol_version;
  object["chain_subversion"] = chain.subversion;
  object["height"] = chain.height;
  object["best_hash"] = chain.best_hash;
  object["peer_count"] = chain.peer_count;
  object["mempool_tx_count"] = chain.mempool_tx_count;
  object["mempool_bytes"] = chain.mempool_bytes;
  object["generated_block_count"] = generated_block_count;
  object["restart_count"] = restart_count;
  if (chain.initial_block_download) {
    object["initial_block_download"] = *chain.initial_block_download;
  } else {
    object["initial_block_download"] = nullptr;
  }
  if (chain.difficulty) {
    object["difficulty"] = *chain.difficulty;
  } else {
    object["difficulty"] = nullptr;
  }
  object["rpc_latency_ms"] = chain.rpc_latency_ms;
  if (cgroup != nullptr) {
    object["cpu_usage_usec"] = cgroup->cpu_usage_usec;
    object["cpu_throttled_usec"] = cgroup->cpu_throttled_usec;
    object["cpu_pressure_some_total_usec"] =
        cgroup->cpu_pressure_some_total_usec;
    object["cpu_pressure_full_total_usec"] =
        cgroup->cpu_pressure_full_total_usec;
    object["memory_current"] = cgroup->memory_current;
    object["memory_peak"] = cgroup->memory_peak;
    if (cgroup->memory_high_limit_bytes) {
      object["memory_high_limit_bytes"] = *cgroup->memory_high_limit_bytes;
    } else {
      object["memory_high_limit_bytes"] = nullptr;
    }
    if (cgroup->memory_max_limit_bytes) {
      object["memory_max_limit_bytes"] = *cgroup->memory_max_limit_bytes;
    } else {
      object["memory_max_limit_bytes"] = nullptr;
    }
    if (cgroup->cpu_quota_us) {
      object["cpu_quota_us"] = *cgroup->cpu_quota_us;
    } else {
      object["cpu_quota_us"] = nullptr;
    }
    object["cpu_period_us"] = cgroup->cpu_period_us;
    object["io_read_bytes"] = cgroup->io_read_bytes;
    object["io_write_bytes"] = cgroup->io_write_bytes;
    object["io_pressure_some_total_usec"] = cgroup->io_pressure_some_total_usec;
    object["io_pressure_full_total_usec"] = cgroup->io_pressure_full_total_usec;
    object["pids_current"] = cgroup->pids_current;
    if (cgroup->pids_max_limit) {
      object["pids_max_limit"] = *cgroup->pids_max_limit;
    } else {
      object["pids_max_limit"] = nullptr;
    }
    object["pids_max_events"] = cgroup->pids_max_events;
    object["cgroup_populated"] = cgroup->cgroup_populated;
    object["cgroup_frozen"] = cgroup->cgroup_frozen;
    object["memory_low"] = cgroup->memory_low;
    object["memory_high"] = cgroup->memory_high;
    object["memory_max"] = cgroup->memory_max;
    object["oom"] = cgroup->oom;
    object["oom_kill"] = cgroup->oom_kill;
    object["oom_group_kill"] = cgroup->oom_group_kill;
  }
  if (link != nullptr) {
    object["network_has_stats"] = link->has_stats;
    object["network_rx_bytes"] = link->rx_bytes;
    object["network_tx_bytes"] = link->tx_bytes;
    object["network_rx_packets"] = link->rx_packets;
    object["network_tx_packets"] = link->tx_packets;
    object["network_rx_dropped"] = link->rx_dropped;
    object["network_tx_dropped"] = link->tx_dropped;
    object["network_rx_errors"] = link->rx_errors;
    object["network_tx_errors"] = link->tx_errors;
  }
  if (qdisc != nullptr) {
    object["qdisc_kind"] = qdisc->kind;
    object["qdisc_handle"] = qdisc->handle;
    object["qdisc_parent"] = qdisc->parent;
    object["qdisc_has_stats"] = qdisc->has_stats;
    object["qdisc_bytes"] = qdisc->bytes;
    object["qdisc_packets"] = qdisc->packets;
    object["qdisc_drops"] = qdisc->drops;
    object["qdisc_overlimits"] = qdisc->overlimits;
    object["qdisc_qlen"] = qdisc->qlen;
    object["qdisc_backlog"] = qdisc->backlog;
    object["qdisc_requeues"] = qdisc->requeues;
    object["qdisc_has_netem_options"] = qdisc->has_netem_options;
    object["qdisc_netem_latency_us"] = qdisc->netem_latency_us;
    object["qdisc_netem_jitter_us"] = qdisc->netem_jitter_us;
    object["qdisc_netem_loss"] = qdisc->netem_loss;
    object["qdisc_netem_duplicate"] = qdisc->netem_duplicate;
    object["qdisc_netem_corrupt"] = qdisc->netem_corrupt;
    object["qdisc_netem_reorder"] = qdisc->netem_reorder;
    object["qdisc_netem_limit_packets"] = qdisc->netem_limit_packets;
    object["qdisc_has_tbf_options"] = qdisc->has_tbf_options;
    object["qdisc_tbf_rate_bytes_per_sec"] = qdisc->tbf_rate_bytes_per_sec;
    object["qdisc_tbf_limit_bytes"] = qdisc->tbf_limit_bytes;
    object["qdisc_tbf_buffer_ticks"] = qdisc->tbf_buffer_ticks;
    object["qdisc_tbf_mtu_ticks"] = qdisc->tbf_mtu_ticks;
  }
  return boost::json::serialize(object);
}

void WriteEvent(const std::filesystem::path& events_path,
                const std::string& run_id, const std::string& node_id,
                std::string_view event, std::string_view detail = "") {
  boost::json::object object;
  object["timestamp"] = NowIso8601();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["event"] = event;
  object["detail"] = detail;
  AppendLine(events_path, boost::json::serialize(object));
}

void WriteNodeState(const std::filesystem::path& events_path,
                    const std::string& run_id, const std::string& node_id,
                    std::string_view state) {
  WriteEvent(events_path, run_id, node_id, "state", state);
}

std::string GeneratedBlocksDetail(uint32_t workload_index,
                                  uint32_t workload_count,
                                  uint32_t generator_node,
                                  uint64_t start_height, uint64_t target_height,
                                  const std::vector<std::string>& hashes) {
  boost::json::array hash_array;
  for (const std::string& hash : hashes) {
    hash_array.emplace_back(hash);
  }
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["generator_node"] = generator_node;
  detail["count"] = static_cast<uint64_t>(hashes.size());
  detail["start_height"] = start_height;
  detail["target_height"] = target_height;
  detail["reward_address"] = kDefaultRewardAddress;
  detail["hashes"] = std::move(hash_array);
  return boost::json::serialize(detail);
}

std::string HeightWaitDetail(uint32_t workload_index, uint32_t workload_count,
                             uint32_t node, uint64_t target_height,
                             uint64_t observed_height) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["target_height"] = target_height;
  detail["observed_height"] = observed_height;
  return boost::json::serialize(detail);
}

std::string PeerCountWaitDetail(uint32_t workload_index,
                                uint32_t workload_count, uint32_t node,
                                uint64_t target_peer_count,
                                uint64_t observed_peer_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["target_peer_count"] = target_peer_count;
  detail["observed_peer_count"] = observed_peer_count;
  return boost::json::serialize(detail);
}

bool ContainsPeerAddress(const std::vector<std::string>& addresses,
                         const std::string& address) {
  for (const std::string& candidate : addresses) {
    if (candidate == address) {
      return true;
    }
  }
  return false;
}

std::string PeerAddress(const Options& options,
                        const std::vector<NodeRuntime>& nodes, uint32_t node) {
  const uint32_t node_index = node - 1U;
  return FiroPeerHost(options, node_index) + ":" +
         std::to_string(nodes[node_index].config.p2p_port);
}

std::string PeerChurnDetail(uint32_t workload_index, uint32_t workload_count,
                            uint32_t node, uint32_t peer,
                            const std::string& address,
                            uint64_t before_peer_count,
                            uint64_t after_peer_count, bool connected_before,
                            bool connected_after,
                            std::optional<uint32_t> timeout_sec) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["peer"] = peer;
  detail["address"] = address;
  detail["before_peer_count"] = before_peer_count;
  detail["after_peer_count"] = after_peer_count;
  detail["connected_before"] = connected_before;
  detail["connected_after"] = connected_after;
  if (timeout_sec) {
    detail["timeout_sec"] = *timeout_sec;
  }
  return boost::json::serialize(detail);
}

std::string RawTransactionDetail(uint32_t workload_index,
                                 uint32_t workload_count,
                                 const SendRawTransactionWorkload& workload,
                                 uint64_t start_height, uint64_t target_height,
                                 const std::vector<std::string>& funding_hashes,
                                 const FiroRawTransactionResult& transaction) {
  boost::json::object utxo;
  utxo["txid"] = transaction.utxo.txid;
  utxo["vout"] = transaction.utxo.vout;
  utxo["amount"] = transaction.utxo.amount;
  utxo["block_hash"] = transaction.utxo.block_hash;
  utxo["confirmations"] = transaction.utxo.confirmations;

  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["funding_node"] = workload.funding_node;
  detail["submit_node"] = workload.submit_node;
  detail["source_address"] = workload.source_address;
  detail["destination_address"] = workload.destination_address;
  detail["funding_blocks"] = workload.funding_blocks;
  detail["funding_hash_count"] = static_cast<uint64_t>(funding_hashes.size());
  detail["funding_start_height"] = start_height;
  detail["funding_target_height"] = target_height;
  detail["selected_utxo"] = std::move(utxo);
  detail["amount"] = transaction.destination_amount;
  detail["fee"] = transaction.fee;
  detail["change_amount"] = transaction.change_amount;
  detail["txid"] = transaction.txid;
  detail["mempool_size"] = transaction.mempool_size;
  detail["timeout_sec"] = workload.timeout_sec;
  return boost::json::serialize(detail);
}

boost::json::array TxIdsJson(const std::vector<std::string>& txids) {
  boost::json::array array;
  for (const std::string& txid : txids) {
    array.emplace_back(txid);
  }
  return array;
}

std::string WalletTransactionDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload, uint32_t transaction_index,
    const WalletIdentity& sender, const WalletIdentity& receiver,
    uint32_t funding_miner_node, uint64_t funding_start_height,
    uint64_t funding_target_height, uint64_t funding_hash_count,
    const FiroWalletTransactionResult& transaction) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["transaction_index"] = transaction_index;
  detail["transaction_count"] = workload.transaction_count;
  detail["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  detail["sender_wallet_index"] = sender.wallet_index;
  detail["receiver_wallet_index"] = receiver.wallet_index;
  detail["sender_node"] = sender.node;
  detail["receiver_node"] = receiver.node;
  detail["sender_address"] = sender.address;
  detail["receiver_address"] = receiver.address;
  detail["funding_miner_node"] = funding_miner_node;
  detail["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  detail["funding_hash_count"] = funding_hash_count;
  detail["funding_start_height"] = funding_start_height;
  detail["funding_target_height"] = funding_target_height;
  detail["readiness_confirmations"] = workload.readiness_confirmations;
  detail["amount"] = transaction.destination_amount;
  detail["requested_fee_rate"] = transaction.requested_fee_rate;
  detail["txids"] = TxIdsJson(transaction.txids);
  detail["mempool_size"] = transaction.mempool_size;
  detail["timeout_sec"] = workload.timeout_sec;
  return boost::json::serialize(detail);
}

std::string WalletAddressDetail(const WalletIdentity& wallet,
                                const WalletInitialization& initialization) {
  boost::json::object detail;
  detail["wallet_index"] = wallet.wallet_index;
  detail["node"] = wallet.node;
  detail["strategy"] =
      std::string(WalletInitializationStrategyName(initialization.strategy));
  detail["mode"] = std::string(WalletPrivacyModeName(initialization.mode));
  if (!wallet.address.empty()) {
    detail["address"] = wallet.address;
  }
  return boost::json::serialize(detail);
}

std::string RestartNodeWorkloadDetail(uint32_t workload_index,
                                      uint32_t workload_count, uint32_t node,
                                      uint64_t restart_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

std::string FreezeNodeWorkloadDetail(uint32_t workload_index,
                                     uint32_t workload_count, uint32_t node,
                                     uint32_t duration_ms) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["duration_ms"] = duration_ms;
  return boost::json::serialize(detail);
}

void WriteMetricsSnapshot(const std::filesystem::path& metrics_path,
                          const Options& options, const FiroDriver& driver,
                          std::vector<NodeRuntime>& nodes) {
  const std::vector<LinkInfo> links = ListNetworkLinks();
  const std::vector<QdiscInfo> qdiscs = ListQdiscs();
  for (auto& node : nodes) {
    FiroMetrics chain = driver.ReadMetrics(node.config);
    CgroupMetrics cg = node.cgroup->ReadMetrics();
    const LinkInfo* link =
        node.network ? FindLinkByName(links, node.network->host_name) : nullptr;
    std::optional<QdiscInfo> qdisc_summary;
    const QdiscInfo* qdisc = nullptr;
    if (node.network) {
      if (node.network->apply_condition) {
        QdiscInfo candidate;
        if (QdiscsMatchNetworkCondition(qdiscs, node.network->host_name,
                                        node.network->condition, &candidate)) {
          qdisc_summary = candidate;
          qdisc = &*qdisc_summary;
        }
      }
      if (qdisc == nullptr) {
        qdisc = FindQdiscByInterfaceName(qdiscs, node.network->host_name);
      }
    }
    AppendLine(metrics_path, MetricsJson(options.run_id, node.config.id, chain,
                                         node.generated_block_count,
                                         node.restart_count, &cg, link, qdisc));
  }
}

void WritePeriodicMetrics(const std::filesystem::path& events_path,
                          const std::filesystem::path& metrics_path,
                          const Options& options, const FiroDriver& driver,
                          std::vector<NodeRuntime>& nodes) {
  for (uint32_t sample = 0; sample < options.metrics_sample_count; ++sample) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(options.metrics_interval_ms));
    WriteMetricsSnapshot(metrics_path, options, driver, nodes);
    boost::json::object detail;
    detail["sample"] = sample + 1U;
    detail["sample_count"] = options.metrics_sample_count;
    detail["interval_ms"] = options.metrics_interval_ms;
    WriteEvent(events_path, options.run_id, "sim", "metrics_sample",
               boost::json::serialize(detail));
  }
}

std::string LogTailDetail(std::string_view kind, const LogTailChunk& chunk) {
  boost::json::object detail;
  detail["kind"] = kind;
  detail["start_offset"] = chunk.start_offset;
  detail["next_offset"] = chunk.next_offset;
  detail["truncated"] = chunk.truncated;
  detail["offset_reset"] = chunk.offset_reset;
  detail["text"] = chunk.text;
  return boost::json::serialize(detail);
}

void WriteLogTailEvent(const std::filesystem::path& events_path,
                       const Options& options, const NodeRuntime& node,
                       std::string_view kind, const std::filesystem::path& path,
                       uint64_t* offset) {
  if (!std::filesystem::exists(path)) {
    return;
  }
  const LogTailChunk chunk = TailLogFile(path, *offset, kMaxLogTailBytes);
  *offset = chunk.next_offset;
  if (chunk.text.empty() && !chunk.truncated && !chunk.offset_reset) {
    return;
  }
  WriteEvent(events_path, options.run_id, node.config.id,
             std::string(kind) + "_tail", LogTailDetail(kind, chunk));
}

void WriteNodeLogTail(const std::filesystem::path& events_path,
                      const Options& options, const FiroDriver& driver,
                      NodeRuntime& node) {
  WriteLogTailEvent(events_path, options, node, "stdout",
                    node.config.log_dir / "stdout.log", &node.stdout_offset);
  WriteLogTailEvent(events_path, options, node, "stderr",
                    node.config.log_dir / "stderr.log", &node.stderr_offset);
  WriteLogTailEvent(events_path, options, node, "daemon_log",
                    driver.LogPath(node.config), &node.daemon_log_offset);
}

void WriteNodeLogTails(const std::filesystem::path& events_path,
                       const Options& options, const FiroDriver& driver,
                       std::vector<NodeRuntime>& nodes) {
  for (NodeRuntime& node : nodes) {
    WriteNodeLogTail(events_path, options, driver, node);
  }
}

void EmitYamlScalar(YamlEmitter* emitter, std::string_view value,
                    yaml_scalar_style_t style) {
  if (value.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    throw std::runtime_error("YAML scalar is too large");
  }
  yaml_event_t event;
  yaml_scalar_event_initialize(
      &event, nullptr, nullptr,
      const_cast<yaml_char_t*>(
          reinterpret_cast<const yaml_char_t*>(value.data())),
      static_cast<int>(value.size()), 1, 1, style);
  emitter->Emit(&event);
}

void EmitYamlJsonValue(YamlEmitter* emitter, const boost::json::value& value);

void EmitYamlJsonObject(YamlEmitter* emitter,
                        const boost::json::object& object) {
  yaml_event_t event;
  yaml_mapping_start_event_initialize(&event, nullptr, nullptr, 1,
                                      YAML_BLOCK_MAPPING_STYLE);
  emitter->Emit(&event);
  for (const auto& item : object) {
    EmitYamlScalar(emitter, item.key(), YAML_PLAIN_SCALAR_STYLE);
    EmitYamlJsonValue(emitter, item.value());
  }
  yaml_mapping_end_event_initialize(&event);
  emitter->Emit(&event);
}

void EmitYamlJsonArray(YamlEmitter* emitter, const boost::json::array& array) {
  yaml_event_t event;
  yaml_sequence_start_event_initialize(&event, nullptr, nullptr, 1,
                                       YAML_BLOCK_SEQUENCE_STYLE);
  emitter->Emit(&event);
  for (const boost::json::value& value : array) {
    EmitYamlJsonValue(emitter, value);
  }
  yaml_sequence_end_event_initialize(&event);
  emitter->Emit(&event);
}

void EmitYamlJsonValue(YamlEmitter* emitter, const boost::json::value& value) {
  if (value.is_object()) {
    EmitYamlJsonObject(emitter, value.as_object());
  } else if (value.is_array()) {
    EmitYamlJsonArray(emitter, value.as_array());
  } else if (value.is_string()) {
    EmitYamlScalar(emitter, std::string_view(value.as_string()),
                   YAML_DOUBLE_QUOTED_SCALAR_STYLE);
  } else if (value.is_bool()) {
    EmitYamlScalar(emitter, value.as_bool() ? "true" : "false",
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_int64()) {
    EmitYamlScalar(emitter, std::to_string(value.as_int64()),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_uint64()) {
    EmitYamlScalar(emitter, std::to_string(value.as_uint64()),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_double()) {
    EmitYamlScalar(emitter, boost::json::serialize(value),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_null()) {
    EmitYamlScalar(emitter, "null", YAML_PLAIN_SCALAR_STYLE);
  }
}

std::string YamlFromJson(const boost::json::value& value) {
  YamlEmitter emitter;
  yaml_event_t event;
  yaml_stream_start_event_initialize(&event, YAML_UTF8_ENCODING);
  emitter.Emit(&event);
  yaml_document_start_event_initialize(&event, nullptr, nullptr, nullptr, 0);
  emitter.Emit(&event);
  EmitYamlJsonValue(&emitter, value);
  yaml_document_end_event_initialize(&event, 0);
  emitter.Emit(&event);
  yaml_stream_end_event_initialize(&event);
  emitter.Emit(&event);
  return emitter.Output();
}

std::vector<ScenarioWorkload> EffectiveWorkloads(const Options& options) {
  if (options.workloads_configured) {
    return options.workloads;
  }
  BlockGenerationWorkload workload;
  workload.node = options.generate_node;
  workload.count = options.generate_blocks;
  workload.sync_timeout_sec = options.sync_timeout_sec;
  ScenarioWorkload scenario_workload;
  scenario_workload.kind = WorkloadKind::kBlockGeneration;
  scenario_workload.block_generation = workload;
  return {scenario_workload};
}

uint64_t TotalBlockGenerationCount(
    const std::vector<ScenarioWorkload>& workloads) {
  uint64_t total = 0;
  for (const ScenarioWorkload& workload : workloads) {
    if (workload.kind == WorkloadKind::kBlockGeneration) {
      total += workload.block_generation.count;
    }
  }
  return total;
}

std::optional<uint32_t> CommonBlockGenerationNode(
    const std::vector<ScenarioWorkload>& workloads) {
  std::optional<uint32_t> node;
  for (const ScenarioWorkload& workload : workloads) {
    if (workload.kind != WorkloadKind::kBlockGeneration) {
      continue;
    }
    if (!node) {
      node = workload.block_generation.node;
    } else if (*node != workload.block_generation.node) {
      return std::nullopt;
    }
  }
  return node;
}

std::optional<uint32_t> CommonBlockGenerationSyncTimeout(
    const std::vector<ScenarioWorkload>& workloads) {
  std::optional<uint32_t> sync_timeout_sec;
  for (const ScenarioWorkload& workload : workloads) {
    if (workload.kind != WorkloadKind::kBlockGeneration) {
      continue;
    }
    if (!sync_timeout_sec) {
      sync_timeout_sec = workload.block_generation.sync_timeout_sec;
    } else if (*sync_timeout_sec !=
               workload.block_generation.sync_timeout_sec) {
      return std::nullopt;
    }
  }
  return sync_timeout_sec;
}

boost::json::object BlockGenerationWorkloadJson(
    const BlockGenerationWorkload& workload) {
  boost::json::object object;
  object["type"] = "block_generation";
  object["node"] = workload.node;
  object["count"] = workload.count;
  object["sync_timeout_sec"] = workload.sync_timeout_sec;
  return object;
}

boost::json::object WaitUntilHeightWorkloadJson(
    const WaitUntilHeightWorkload& workload) {
  boost::json::object object;
  object["type"] = "wait_until_height";
  object["node"] = workload.node;
  object["height"] = workload.height;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WaitForPeersWorkloadJson(
    const WaitForPeersWorkload& workload) {
  boost::json::object object;
  object["type"] = "wait_for_peers";
  object["node"] = workload.node;
  object["peer_count"] = workload.peer_count;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object ConnectPeerWorkloadJson(
    const ConnectPeerWorkload& workload) {
  boost::json::object object;
  object["type"] = "connect_peer";
  object["node"] = workload.node;
  object["peer"] = workload.peer;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object DisconnectPeerWorkloadJson(
    const DisconnectPeerWorkload& workload) {
  boost::json::object object;
  object["type"] = "disconnect_peer";
  object["node"] = workload.node;
  object["peer"] = workload.peer;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object RestartNodeWorkloadJson(
    const RestartNodeWorkload& workload) {
  boost::json::object object;
  object["type"] = "restart_node";
  object["node"] = workload.node;
  return object;
}

boost::json::object FreezeNodeWorkloadJson(const FreezeNodeWorkload& workload) {
  boost::json::object object;
  object["type"] = "freeze_node";
  object["node"] = workload.node;
  object["duration_ms"] = workload.duration_ms;
  return object;
}

boost::json::object ResourceLimitUpdateWorkloadJson(
    const ResourceLimitUpdateWorkload& workload) {
  boost::json::object object = ResourceLimitPatchJson(workload.patch);
  object["type"] = "update_resource_limits";
  object["node"] = workload.node;
  return object;
}

boost::json::object NetworkPartitionWorkloadJson(
    const NetworkPartitionWorkload& workload, std::string_view type) {
  boost::json::object object = NetworkPartitionRuleJson(workload.partition);
  object["type"] = std::string(type);
  return object;
}

boost::json::object SendRawTransactionWorkloadJson(
    const SendRawTransactionWorkload& workload) {
  boost::json::object object;
  object["type"] = "send_raw_transaction";
  object["funding_node"] = workload.funding_node;
  object["submit_node"] = workload.submit_node;
  object["source_address"] = workload.source_address;
  object["source_private_key"] = workload.source_private_key;
  object["destination_address"] = workload.destination_address;
  object["funding_blocks"] = workload.funding_blocks;
  object["amount"] = FormatFixed8Amount(workload.amount_satoshis);
  object["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WalletTransactionsWorkloadJson(
    const WalletTransactionsWorkload& workload) {
  boost::json::object object;
  object["type"] = "wallet_transactions";
  object["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  object["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  object["readiness_confirmations"] = workload.readiness_confirmations;
  object["transaction_count"] = workload.transaction_count;
  object["amount"] = FormatFixed8Amount(workload.amount_satoshis);
  object["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WorkloadJson(const ScenarioWorkload& workload) {
  if (workload.kind == WorkloadKind::kBlockGeneration) {
    return BlockGenerationWorkloadJson(workload.block_generation);
  }
  if (workload.kind == WorkloadKind::kWaitUntilHeight) {
    return WaitUntilHeightWorkloadJson(workload.wait_until_height);
  }
  if (workload.kind == WorkloadKind::kWaitForPeers) {
    return WaitForPeersWorkloadJson(workload.wait_for_peers);
  }
  if (workload.kind == WorkloadKind::kConnectPeer) {
    return ConnectPeerWorkloadJson(workload.connect_peer);
  }
  if (workload.kind == WorkloadKind::kDisconnectPeer) {
    return DisconnectPeerWorkloadJson(workload.disconnect_peer);
  }
  if (workload.kind == WorkloadKind::kRestartNode) {
    return RestartNodeWorkloadJson(workload.restart_node);
  }
  if (workload.kind == WorkloadKind::kFreezeNode) {
    return FreezeNodeWorkloadJson(workload.freeze_node);
  }
  if (workload.kind == WorkloadKind::kUpdateResourceLimits) {
    return ResourceLimitUpdateWorkloadJson(workload.update_resource_limits);
  }
  if (workload.kind == WorkloadKind::kPartitionNodes) {
    return NetworkPartitionWorkloadJson(workload.network_partition,
                                        "partition_nodes");
  }
  if (workload.kind == WorkloadKind::kHealPartition) {
    return NetworkPartitionWorkloadJson(workload.network_partition,
                                        "heal_partition");
  }
  if (workload.kind == WorkloadKind::kSendRawTransaction) {
    return SendRawTransactionWorkloadJson(workload.send_raw_transaction);
  }
  if (workload.kind == WorkloadKind::kWalletTransactions) {
    return WalletTransactionsWorkloadJson(workload.wallet_transactions);
  }
  throw std::runtime_error("unknown scenario workload kind");
}

void WriteScenarioFiles(const Options& options,
                        const std::filesystem::path& run_root) {
  const std::vector<ScenarioWorkload> workloads = EffectiveWorkloads(options);
  boost::json::object resolved;
  resolved["run_id"] = options.run_id;
  resolved["chain"] = "firo";
  resolved["nodes"] = options.nodes;
  resolved["generate_blocks"] = TotalBlockGenerationCount(workloads);
  if (const std::optional<uint32_t> generate_node =
          CommonBlockGenerationNode(workloads)) {
    resolved["generate_node"] = *generate_node;
  } else {
    resolved["generate_node"] = nullptr;
  }
  resolved["firod"] = options.firod.string();
  if (!options.scenario_json.empty()) {
    resolved["scenario_json"] = options.scenario_json.string();
  }
  if (!options.scenario_yaml.empty()) {
    resolved["scenario_yaml"] = options.scenario_yaml.string();
  }
  resolved["isolated_network"] = options.isolate_network;
  if (const std::optional<uint32_t> sync_timeout_sec =
          CommonBlockGenerationSyncTimeout(workloads)) {
    resolved["sync_timeout_sec"] = *sync_timeout_sec;
  } else {
    resolved["sync_timeout_sec"] = nullptr;
  }
  resolved["metrics_sample_count"] = options.metrics_sample_count;
  resolved["metrics_interval_ms"] = options.metrics_interval_ms;
  if (options.topology.configured) {
    resolved["topology"] =
        NodeRoleTopologyJson(options.topology, options.wallet_initialization);
  }
  boost::json::array workload_array;
  for (const ScenarioWorkload& workload : workloads) {
    workload_array.push_back(WorkloadJson(workload));
  }
  resolved["workloads"] = std::move(workload_array);
  boost::json::object resources;
  resources["memory_high_bytes"] = options.memory_high_bytes;
  resources["memory_max_bytes"] = options.memory_max_bytes;
  if (options.cpu_quota_requested) {
    resources["cpu_quota_us"] = options.cpu_quota_us;
  } else {
    resources["cpu_quota_us"] = nullptr;
  }
  resources["cpu_period_us"] = options.cpu_period_us;
  resources["pids_max"] = options.pids_max;
  resolved["resources"] = std::move(resources);
  if (options.network_condition_requested) {
    resolved["default_network_condition"] =
        NetworkConditionJson(options.network_condition);
  }
  if (!options.node_network_conditions.empty()) {
    boost::json::array node_conditions;
    for (const auto& [node_index, condition] :
         options.node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      node_conditions.push_back(std::move(node_condition));
    }
    resolved["node_network_conditions"] = std::move(node_conditions);
  }
  if (!options.runtime_node_network_conditions.empty()) {
    boost::json::array runtime_node_conditions;
    for (const auto& [node_index, condition] :
         options.runtime_node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      runtime_node_conditions.push_back(std::move(node_condition));
    }
    resolved["runtime_node_network_conditions"] =
        std::move(runtime_node_conditions);
  }
  if (!options.runtime_node_blocks.empty()) {
    boost::json::array runtime_node_blocks;
    for (const NetworkBlockRule& rule : options.runtime_node_blocks) {
      runtime_node_blocks.push_back(NetworkBlockRuleJson(rule));
    }
    resolved["runtime_node_blocks"] = std::move(runtime_node_blocks);
  }
  if (!options.runtime_node_unblocks.empty()) {
    boost::json::array runtime_node_unblocks;
    for (const NetworkBlockRule& rule : options.runtime_node_unblocks) {
      runtime_node_unblocks.push_back(NetworkBlockRuleJson(rule));
    }
    resolved["runtime_node_unblocks"] = std::move(runtime_node_unblocks);
  }
  if (!options.runtime_partitions.empty()) {
    boost::json::array runtime_partitions;
    for (const NetworkPartitionRule& rule : options.runtime_partitions) {
      runtime_partitions.push_back(NetworkPartitionRuleJson(rule));
    }
    resolved["runtime_partitions"] = std::move(runtime_partitions);
  }
  if (!options.runtime_partition_heals.empty()) {
    boost::json::array runtime_partition_heals;
    for (const NetworkPartitionRule& rule : options.runtime_partition_heals) {
      runtime_partition_heals.push_back(NetworkPartitionRuleJson(rule));
    }
    resolved["runtime_partition_heals"] = std::move(runtime_partition_heals);
  }
  if (!options.runtime_node_resource_updates.empty()) {
    boost::json::array runtime_node_limits;
    for (const auto& [node_index, patch] :
         options.runtime_node_resource_updates) {
      boost::json::object node_limits;
      node_limits["node"] = node_index + 1U;
      node_limits["limits"] = ResourceLimitPatchJson(patch);
      runtime_node_limits.push_back(std::move(node_limits));
    }
    resolved["runtime_node_resource_limits"] = std::move(runtime_node_limits);
  }
  if (!options.runtime_node_restarts.empty()) {
    boost::json::array runtime_node_restarts;
    for (uint32_t node_index : options.runtime_node_restarts) {
      boost::json::object restart;
      restart["node"] = node_index + 1U;
      runtime_node_restarts.push_back(std::move(restart));
    }
    resolved["runtime_node_restarts"] = std::move(runtime_node_restarts);
  }
  if (!options.runtime_node_freezes.empty()) {
    boost::json::array runtime_node_freezes;
    for (const FreezeRequest& freeze : options.runtime_node_freezes) {
      boost::json::object object;
      object["node"] = freeze.node_index + 1U;
      object["duration_ms"] = freeze.duration_ms;
      runtime_node_freezes.push_back(std::move(object));
    }
    resolved["runtime_node_freezes"] = std::move(runtime_node_freezes);
  }
  WriteText(run_root / "resolved-scenario.json",
            boost::json::serialize(resolved) + "\n");
  WriteText(run_root / "scenario.yaml", YamlFromJson(resolved));
}

void LoadCleanupMetadata(const std::filesystem::path& run_root,
                         Options* options) {
  const std::filesystem::path resolved_path =
      run_root / "resolved-scenario.json";
  if (!std::filesystem::exists(resolved_path)) {
    return;
  }

  const boost::json::value value = boost::json::parse(ReadText(resolved_path));
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             resolved_path.string());
  }
  const boost::json::object& object = value.as_object();
  options->nodes = JsonOptionalUint32Field(object, "nodes", options->nodes);
  const boost::json::value* isolated = object.if_contains("isolated_network");
  if (isolated != nullptr) {
    if (!isolated->is_bool()) {
      throw std::runtime_error(
          "resolved scenario isolated_network is not a boolean");
    }
    options->isolate_network = isolated->as_bool();
  }
  if (options->nodes < 1 || options->nodes > kMaxFiroNodes) {
    throw std::runtime_error(
        "cleanup currently supports resolved node counts in 1.." +
        std::to_string(kMaxFiroNodes));
  }
}

void CleanupRun(Options options) {
  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  LoadCleanupMetadata(run_root, &options);
  RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");

  for (uint32_t i = 0; i < options.nodes; ++i) {
    DeleteNodeVethNetwork(MakeNodeVethConfig(options, i));
  }
  Cgroup::RemoveRun(options.run_id);

  BSIM_LOG(info) << "cleanup_run=" << options.run_id << "\n"
                 << "nodes=" << options.nodes << "\n"
                 << "run_dir=" << run_root;
}

void StartNodes(const Options& options, const std::filesystem::path& run_root,
                const std::filesystem::path& events_path,
                const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  if (options.isolate_network) {
    RequireNetworkSetupCapabilities();
  }
  if (options.isolate_network && options.nodes > 1 &&
      !HostIpv4ForwardingEnabled()) {
    throw std::runtime_error(
        "isolated multi-node Firo runs require IPv4 forwarding in the parent "
        "network namespace");
  }
  nodes.reserve(options.nodes);
  for (uint32_t i = 0; i < options.nodes; ++i) {
    const std::string node_id = "firo-" + std::to_string(i + 1);
    const auto node_root = run_root / "nodes" / node_id;

    FiroNodeConfig config;
    config.id = node_id;
    config.binary = options.firod;
    config.data_dir = node_root / "data";
    config.log_dir = node_root;
    config.p2p_port = static_cast<uint16_t>(18168 + i);
    config.rpc_port = static_cast<uint16_t>(18888 + i);
    config.rpc_user = "sim-" + options.run_id;
    config.rpc_password = "pass-" + options.run_id + "-" + std::to_string(i);
    config.listen = true;
    config.wallet_enabled = options.wallet_backed_workload_requested &&
                            NodeListContains(options.topology.wallet_nodes, i);
    if (i > 0) {
      config.connect_peers.push_back(FiroPeerHost(options, 0) + ":18168");
    }

    NodeRuntime runtime;
    try {
      runtime.config = config;
      WriteNodeState(events_path, options.run_id, node_id, "Preparing");
      if (options.isolate_network) {
        runtime.network_namespace = NetworkNamespace::Create();
        runtime.network = MakeNodeVethConfig(options, i);
        SetupNodeVethNetwork(runtime.network_namespace->fd(), *runtime.network);
        if (runtime.network->apply_condition) {
          const QdiscInfo qdisc = VerifyNodeNetworkCondition(*runtime.network);
          WriteEvent(
              events_path, options.run_id, node_id,
              "network_condition_verified",
              NetworkConditionVerificationDetail(*runtime.network, qdisc));
        }
        runtime.config.rpc_host = runtime.network->node_address;
        runtime.config.rpc_bind = runtime.network->node_address;
        runtime.config.rpc_allow_ips = {runtime.network->host_address};
        runtime.config.p2p_bind = runtime.network->node_address;
        WriteEvent(events_path, options.run_id, node_id, "network_ready",
                   "node_ip=" + runtime.network->node_address +
                       " host_if=" + runtime.network->host_name +
                       " peer_if=" + runtime.network->peer_name);
        WriteNodeState(events_path, options.run_id, node_id, "NetnsReady");
      }

      runtime.cgroup = Cgroup::Create(options.run_id, node_id);
      runtime.resources = InitialResourceLimits(options);
      runtime.cgroup->SetMemoryHigh(runtime.resources.memory_high_bytes);
      runtime.cgroup->SetMemoryMax(runtime.resources.memory_max_bytes);
      runtime.cgroup->SetCpuMax(runtime.resources.cpu_quota_us,
                                runtime.resources.cpu_period_us);
      runtime.cgroup->SetPidsMax(runtime.resources.pids_max);
      WriteNodeState(events_path, options.run_id, node_id, "CgroupReady");

      ProcessSpec process = driver.RenderProcess(runtime.config);
      if (runtime.network_namespace) {
        process.network_namespace_fd = runtime.network_namespace->fd();
      }
      WriteNodeState(events_path, options.run_id, node_id, "Starting");
      runtime.process = ChildProcess::Spawn(process, runtime.cgroup->path());
      BSIM_LOG(info) << "started " << node_id
                     << " pid=" << runtime.process.pid();
      WriteEvent(events_path, options.run_id, node_id, "process_started",
                 "pid=" + std::to_string(runtime.process.pid()));
      nodes.push_back(std::move(runtime));
    } catch (...) {
      WriteNodeState(events_path, options.run_id, node_id, "Failed");
      runtime.process.Kill();
      if (runtime.cgroup) {
        try {
          runtime.cgroup->KillAll();
          runtime.cgroup->Remove();
        } catch (const std::exception&) {
        }
      }
      if (runtime.network) {
        DeleteNodeVethNetwork(*runtime.network);
      }
      throw;
    }
  }

  for (auto& node : nodes) {
    driver.WaitReady(node.config,
                     std::chrono::seconds(options.ready_timeout_sec));
    WriteEvent(events_path, options.run_id, node.config.id, "rpc_ready");
    WriteNodeState(events_path, options.run_id, node.config.id, "Running");
  }
}

void InitializeWalletNodes(const Options& options,
                           const std::filesystem::path& events_path,
                           const FiroDriver& driver,
                           std::vector<NodeRuntime>& nodes,
                           SimulationRegistry& registry) {
  if (!options.wallet_backed_workload_requested) {
    return;
  }
  if (registry.wallet_initialization().strategy !=
      WalletInitializationStrategy::kDriverRpc) {
    throw std::runtime_error(
        "wallet-backed workload requires driver_rpc wallet initialization");
  }

  for (size_t wallet_index = 0; wallet_index < registry.wallets().size();
       ++wallet_index) {
    WalletIdentity& wallet = registry.MutableWalletByIndex(wallet_index);
    if (wallet.node == 0U || wallet.node > nodes.size()) {
      throw std::runtime_error("wallet node is out of range");
    }
    NodeRuntime& node = nodes[wallet.node - 1U];
    if (!node.config.wallet_enabled) {
      throw std::runtime_error(
          "wallet node was not started with wallet support enabled");
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               "wallet_address_requested",
               WalletAddressDetail(wallet, registry.wallet_initialization()));
    wallet.address = driver.CreateWalletAddress(
        node.config, ToFiroWalletMode(registry.wallet_initialization()));
    if (wallet.address.empty()) {
      throw std::runtime_error("Firo wallet RPC returned an empty address");
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               "wallet_address_created",
               WalletAddressDetail(wallet, registry.wallet_initialization()));
  }
}

std::string ResourceLimitUpdateDetail(
    const ResourceLimitPatch& patch, const ResourceLimits& previous,
    const ResourceLimits& current,
    std::optional<uint32_t> workload_index = std::nullopt,
    std::optional<uint32_t> workload_count = std::nullopt,
    std::optional<uint32_t> node = std::nullopt) {
  boost::json::object detail;
  if (workload_index) {
    detail["workload_index"] = *workload_index;
  }
  if (workload_count) {
    detail["workload_count"] = *workload_count;
  }
  if (node) {
    detail["node"] = *node;
  }
  detail["requested"] = ResourceLimitPatchJson(patch);
  detail["previous"] = ResourceLimitsJson(previous);
  detail["current"] = ResourceLimitsJson(current);
  return boost::json::serialize(detail);
}

void ApplyResourceLimitUpdate(
    const Options& options, const std::filesystem::path& events_path,
    NodeRuntime& node, const ResourceLimitPatch& patch,
    std::optional<uint32_t> workload_index = std::nullopt,
    std::optional<uint32_t> workload_count = std::nullopt,
    std::optional<uint32_t> workload_node = std::nullopt) {
  if (!node.cgroup) {
    throw std::runtime_error("resource update requires a node cgroup");
  }
  const ResourceLimits previous = node.resources;
  const ResourceLimits next =
      ApplyResourceLimitPatch(previous, patch, node.config.id);
  WriteResourceLimits(*node.cgroup, previous, next);
  node.resources = next;
  WriteEvent(events_path, options.run_id, node.config.id,
             "resource_limits_updated",
             ResourceLimitUpdateDetail(patch, previous, next, workload_index,
                                       workload_count, workload_node));
}

void ApplyRuntimeResourceLimitUpdates(const Options& options,
                                      const std::filesystem::path& events_path,
                                      std::vector<NodeRuntime>& nodes) {
  for (const auto& [node_index, patch] :
       options.runtime_node_resource_updates) {
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime resource update node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    ApplyResourceLimitUpdate(options, events_path, node, patch);
  }
}

void ApplyConnectPeerWorkload(const Options& options,
                              const std::filesystem::path& events_path,
                              const FiroDriver& driver,
                              std::vector<NodeRuntime>& nodes,
                              const ConnectPeerWorkload& workload,
                              uint32_t workload_index,
                              uint32_t workload_count) {
  NodeRuntime& node = nodes[workload.node - 1U];
  const std::string address = PeerAddress(options, nodes, workload.peer);
  const std::vector<std::string> before_addresses =
      driver.PeerAddresses(node.config);
  const bool connected_before = ContainsPeerAddress(before_addresses, address);
  driver.ConnectPeer(node.config, address);
  driver.WaitForPeerAddress(node.config, address,
                            std::chrono::seconds(workload.timeout_sec));
  const std::vector<std::string> after_addresses =
      driver.PeerAddresses(node.config);
  const bool connected_after = ContainsPeerAddress(after_addresses, address);
  WriteEvent(events_path, options.run_id, node.config.id, "peer_connected",
             PeerChurnDetail(
                 workload_index, workload_count, workload.node, workload.peer,
                 address, static_cast<uint64_t>(before_addresses.size()),
                 static_cast<uint64_t>(after_addresses.size()),
                 connected_before, connected_after, workload.timeout_sec));
}

void ApplyDisconnectPeerWorkload(const Options& options,
                                 const std::filesystem::path& events_path,
                                 const FiroDriver& driver,
                                 std::vector<NodeRuntime>& nodes,
                                 const DisconnectPeerWorkload& workload,
                                 uint32_t workload_index,
                                 uint32_t workload_count) {
  NodeRuntime& node = nodes[workload.node - 1U];
  const std::string address = PeerAddress(options, nodes, workload.peer);
  const std::vector<std::string> before_addresses =
      driver.PeerAddresses(node.config);
  const bool connected_before = ContainsPeerAddress(before_addresses, address);
  driver.DisconnectPeer(node.config, address);
  driver.WaitForPeerAddressAbsent(node.config, address,
                                  std::chrono::seconds(workload.timeout_sec));
  const std::vector<std::string> after_addresses =
      driver.PeerAddresses(node.config);
  const bool connected_after = ContainsPeerAddress(after_addresses, address);
  WriteEvent(events_path, options.run_id, node.config.id, "peer_disconnected",
             PeerChurnDetail(
                 workload_index, workload_count, workload.node, workload.peer,
                 address, static_cast<uint64_t>(before_addresses.size()),
                 static_cast<uint64_t>(after_addresses.size()),
                 connected_before, connected_after, workload.timeout_sec));
}

void ApplySendRawTransactionWorkload(const Options& options,
                                     const std::filesystem::path& events_path,
                                     const FiroDriver& driver,
                                     std::vector<NodeRuntime>& nodes,
                                     const SendRawTransactionWorkload& workload,
                                     uint32_t workload_index,
                                     uint32_t workload_count) {
  NodeRuntime& funder = nodes[workload.funding_node - 1U];
  NodeRuntime& submitter = nodes[workload.submit_node - 1U];
  const uint64_t start_height = driver.ReadMetrics(funder.config).height;
  std::vector<std::string> funding_hashes = driver.GenerateBlocks(
      funder.config, workload.funding_blocks, workload.source_address);
  funder.generated_block_count += funding_hashes.size();
  const uint64_t target_height =
      start_height + static_cast<uint64_t>(funding_hashes.size());
  for (auto& node : nodes) {
    driver.WaitForHeight(node.config, target_height,
                         std::chrono::seconds(workload.timeout_sec));
  }

  const uint64_t minimum_amount =
      workload.amount_satoshis + workload.fee_satoshis;
  const FiroUtxo utxo = driver.FindSpendableOutput(
      funder.config, funding_hashes, workload.source_address, minimum_amount,
      kFiroCoinbaseSpendableConfirmations);
  const FiroRawTransactionResult transaction = driver.SendRawTransaction(
      submitter.config, utxo, workload.source_address,
      workload.source_private_key, workload.destination_address,
      workload.amount_satoshis, workload.fee_satoshis,
      std::chrono::seconds(workload.timeout_sec));
  WriteEvent(events_path, options.run_id, submitter.config.id,
             "raw_transaction_submitted",
             RawTransactionDetail(workload_index, workload_count, workload,
                                  start_height, target_height, funding_hashes,
                                  transaction));
}

void ApplyWalletTransactionsWorkload(const Options& options,
                                     const std::filesystem::path& events_path,
                                     const FiroDriver& driver,
                                     std::vector<NodeRuntime>& nodes,
                                     const SimulationRegistry& registry,
                                     const WalletTransactionsWorkload& workload,
                                     uint32_t workload_index,
                                     uint32_t workload_count) {
  struct FundingState {
    uint32_t miner_node = 1;
    uint64_t start_height = 0;
    uint64_t target_height = 0;
    std::vector<std::string> hashes;
  };

  const std::vector<WalletIdentity>& wallets = registry.wallets();
  std::vector<FundingState> funding;
  funding.reserve(wallets.size());
  for (size_t wallet_index = 0; wallet_index < wallets.size(); ++wallet_index) {
    const WalletIdentity& wallet = wallets[wallet_index];
    if (wallet.address.empty()) {
      throw std::runtime_error(
          "wallet-backed workload requires initialized WalletNode addresses");
    }
    const uint32_t miner_node = registry.MinerNodeForWalletIndex(wallet_index);
    NodeRuntime& miner = nodes[miner_node - 1U];
    FundingState state;
    state.miner_node = miner_node;
    state.start_height = driver.ReadMetrics(miner.config).height;
    state.hashes = driver.GenerateBlocks(
        miner.config, workload.funding_blocks_per_wallet, wallet.address);
    miner.generated_block_count += state.hashes.size();
    state.target_height =
        state.start_height + static_cast<uint64_t>(state.hashes.size());
    for (NodeRuntime& node : nodes) {
      driver.WaitForHeight(node.config, state.target_height,
                           std::chrono::seconds(workload.timeout_sec));
    }
    funding.push_back(std::move(state));
  }

  for (uint32_t transaction_index = 0;
       transaction_index < workload.transaction_count; ++transaction_index) {
    const size_t sender_index = transaction_index % wallets.size();
    const size_t receiver_index = (sender_index + 1U) % wallets.size();
    const WalletIdentity& sender = wallets[sender_index];
    const WalletIdentity& receiver = wallets[receiver_index];
    const FundingState& sender_funding = funding[sender_index];
    NodeRuntime& sender_node = nodes[sender.node - 1U];

    const FiroWalletTransactionResult transaction =
        driver.SendWalletTransaction(
            sender_node.config,
            ToFiroWalletMode(registry.wallet_initialization()),
            receiver.address, workload.amount_satoshis, workload.fee_satoshis,
            std::chrono::seconds(workload.timeout_sec));
    for (NodeRuntime& node : nodes) {
      for (const std::string& txid : transaction.txids) {
        driver.WaitForMempoolTransaction(
            node.config, txid, std::chrono::seconds(workload.timeout_sec));
      }
    }
    WriteEvent(
        events_path, options.run_id, sender_node.config.id,
        "wallet_transaction_submitted",
        WalletTransactionDetail(
            workload_index, workload_count, workload, transaction_index + 1U,
            sender, receiver, sender_funding.miner_node,
            sender_funding.start_height, sender_funding.target_height,
            static_cast<uint64_t>(sender_funding.hashes.size()), transaction));
  }
}

void ApplyRuntimeNetworkConditionUpdates(
    const Options& options, const std::filesystem::path& events_path,
    std::vector<NodeRuntime>& nodes) {
  for (const auto& [node_index, condition] :
       options.runtime_node_network_conditions) {
    if (node_index >= nodes.size()) {
      throw std::runtime_error(
          "runtime network condition node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    if (!node.network) {
      throw std::runtime_error(
          "runtime network condition requires isolated networking");
    }
    node.network->apply_condition = true;
    node.network->condition = condition;
    ReplaceNetworkConditionQdisc(node.network->host_name,
                                 node.network->condition);
    const QdiscInfo qdisc = VerifyNodeNetworkCondition(*node.network);
    WriteEvent(events_path, options.run_id, node.config.id,
               "network_condition_updated",
               NetworkConditionVerificationDetail(*node.network, qdisc));
  }
}

bool NetworkBlockRulePresent(const NodeRuntime& node,
                             const NetworkBlockRule& rule) {
  if (!node.network) {
    return false;
  }
  const std::vector<TcFilterInfo> filters = ListTcFilters();
  for (const TcFilterInfo& filter : filters) {
    if (TcFilterMatchesEgressIpv4TcpDrop(filter, node.network->host_name,
                                         rule.src_address, rule.dst_address,
                                         rule.dst_port, rule.handle)) {
      return true;
    }
  }
  return false;
}

std::string NetworkBlockRuleDetail(const NodeRuntime& node,
                                   const NetworkBlockRule& rule,
                                   bool existed_before, bool present_after) {
  boost::json::object detail = NetworkBlockRuleJson(rule);
  if (node.network) {
    detail["host_if"] = node.network->host_name;
  } else {
    detail["host_if"] = nullptr;
  }
  detail["existed_before"] = existed_before;
  detail["present_after"] = present_after;
  return boost::json::serialize(detail);
}

void RequireNetworkBlockNode(const NodeRuntime& node) {
  if (!node.network) {
    throw std::runtime_error(
        "runtime network block rule requires isolated networking");
  }
}

void ApplyRuntimeNetworkBlockRules(const Options& options,
                                   const std::filesystem::path& events_path,
                                   std::vector<NodeRuntime>& nodes) {
  for (const NetworkBlockRule& rule : options.runtime_node_blocks) {
    if (rule.node_index >= nodes.size()) {
      throw std::runtime_error("runtime network block node is out of range");
    }
    NodeRuntime& node = nodes[rule.node_index];
    RequireNetworkBlockNode(node);
    const bool existed_before = NetworkBlockRulePresent(node, rule);
    ReplaceEgressIpv4TcpDropFilter(node.network->host_name, rule.src_address,
                                   rule.dst_address, rule.dst_port,
                                   rule.handle);
    const bool present_after = NetworkBlockRulePresent(node, rule);
    if (!present_after) {
      throw std::runtime_error(
          "runtime network block rule was not visible after apply");
    }
    WriteEvent(
        events_path, options.run_id, node.config.id, "network_block_applied",
        NetworkBlockRuleDetail(node, rule, existed_before, present_after));
  }
}

void ApplyRuntimeNetworkUnblockRules(const Options& options,
                                     const std::filesystem::path& events_path,
                                     std::vector<NodeRuntime>& nodes) {
  for (const NetworkBlockRule& rule : options.runtime_node_unblocks) {
    if (rule.node_index >= nodes.size()) {
      throw std::runtime_error("runtime network unblock node is out of range");
    }
    NodeRuntime& node = nodes[rule.node_index];
    RequireNetworkBlockNode(node);
    const bool existed_before = NetworkBlockRulePresent(node, rule);
    if (existed_before) {
      DeleteEgressIpv4TcpDropFilter(node.network->host_name, rule.handle);
    }
    const bool present_after = NetworkBlockRulePresent(node, rule);
    if (present_after) {
      throw std::runtime_error(
          "runtime network block rule remained after unblock");
    }
    WriteEvent(
        events_path, options.run_id, node.config.id, "network_block_removed",
        NetworkBlockRuleDetail(node, rule, existed_before, present_after));
  }
}

NetworkBlockRule MakeP2pBlockRule(uint32_t src_node_index,
                                  uint32_t dst_node_index,
                                  const std::vector<NodeRuntime>& nodes) {
  if (src_node_index >= nodes.size() || dst_node_index >= nodes.size()) {
    throw std::runtime_error("partition node is out of range");
  }
  NetworkBlockRule rule;
  rule.node_index = dst_node_index;
  rule.src_address = NodeAddress(src_node_index);
  rule.dst_address = NodeAddress(dst_node_index);
  rule.dst_port = nodes[dst_node_index].config.p2p_port;
  rule.handle = StableRuleHandle(rule);
  return rule;
}

std::vector<NetworkBlockRule> PartitionBlockRules(
    const NetworkPartitionRule& partition,
    const std::vector<NodeRuntime>& nodes) {
  std::vector<NetworkBlockRule> rules;
  rules.reserve((partition.group_a.size() * partition.group_b.size()) * 2U);
  for (uint32_t node_a : partition.group_a) {
    for (uint32_t node_b : partition.group_b) {
      rules.push_back(MakeP2pBlockRule(node_a, node_b, nodes));
      rules.push_back(MakeP2pBlockRule(node_b, node_a, nodes));
    }
  }
  return rules;
}

boost::json::object PartitionRuleResultJson(const NodeRuntime& node,
                                            const NetworkBlockRule& rule,
                                            bool existed_before,
                                            bool present_after) {
  boost::json::object object = NetworkBlockRuleJson(rule);
  object["node_id"] = node.config.id;
  if (node.network) {
    object["host_if"] = node.network->host_name;
  } else {
    object["host_if"] = nullptr;
  }
  object["existed_before"] = existed_before;
  object["present_after"] = present_after;
  return object;
}

std::string NetworkPartitionDetail(const NetworkPartitionRule& partition,
                                   const boost::json::array& rule_results,
                                   uint32_t workload_index = 0,
                                   uint32_t workload_count = 0) {
  boost::json::object detail = NetworkPartitionRuleJson(partition);
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  detail["rules"] = rule_results;
  detail["scope"] = "source_aware_group";
  return boost::json::serialize(detail);
}

void ApplyRuntimeNetworkPartition(const Options& options,
                                  const std::filesystem::path& events_path,
                                  std::vector<NodeRuntime>& nodes,
                                  const NetworkPartitionRule& partition,
                                  bool heal, uint32_t workload_index = 0,
                                  uint32_t workload_count = 0) {
  boost::json::array rule_results;
  for (const NetworkBlockRule& rule : PartitionBlockRules(partition, nodes)) {
    NodeRuntime& node = nodes[rule.node_index];
    RequireNetworkBlockNode(node);
    const bool existed_before = NetworkBlockRulePresent(node, rule);
    if (heal) {
      if (existed_before) {
        DeleteEgressIpv4TcpDropFilter(node.network->host_name, rule.handle);
      }
    } else {
      ReplaceEgressIpv4TcpDropFilter(node.network->host_name, rule.src_address,
                                     rule.dst_address, rule.dst_port,
                                     rule.handle);
    }
    const bool present_after = NetworkBlockRulePresent(node, rule);
    if (!heal && !present_after) {
      throw std::runtime_error(
          "runtime network partition rule was not visible after apply");
    }
    if (heal && present_after) {
      throw std::runtime_error(
          "runtime network partition rule remained after heal");
    }
    rule_results.push_back(
        PartitionRuleResultJson(node, rule, existed_before, present_after));
  }

  WriteEvent(events_path, options.run_id, "sim",
             heal ? "network_partition_healed" : "network_partition_applied",
             NetworkPartitionDetail(partition, rule_results, workload_index,
                                    workload_count));
}

void ApplyRuntimeNetworkPartitions(const Options& options,
                                   const std::filesystem::path& events_path,
                                   std::vector<NodeRuntime>& nodes) {
  for (const NetworkPartitionRule& partition : options.runtime_partitions) {
    ApplyRuntimeNetworkPartition(options, events_path, nodes, partition, false);
  }
}

void ApplyRuntimeNetworkPartitionHeals(const Options& options,
                                       const std::filesystem::path& events_path,
                                       std::vector<NodeRuntime>& nodes) {
  for (const NetworkPartitionRule& partition :
       options.runtime_partition_heals) {
    ApplyRuntimeNetworkPartition(options, events_path, nodes, partition, true);
  }
}

std::string RestartDetail(pid_t pid, uint64_t restart_count) {
  boost::json::object detail;
  detail["pid"] = pid;
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

void RestartNode(const Options& options,
                 const std::filesystem::path& events_path,
                 const FiroDriver& driver, NodeRuntime& node) {
  if (!node.cgroup) {
    throw std::runtime_error("node restart requires a node cgroup");
  }

  WriteNodeState(events_path, options.run_id, node.config.id, "Restarting");
  WriteEvent(events_path, options.run_id, node.config.id, "restart_requested",
             "restart_count=" + std::to_string(node.restart_count + 1U));
  driver.Stop(node.config);
  if (!node.process.WaitForExit(std::chrono::seconds(15))) {
    WriteEvent(events_path, options.run_id, node.config.id, "sigterm");
    node.process.Terminate(std::chrono::seconds(5));
  }
  WriteNodeLogTail(events_path, options, driver, node);

  ProcessSpec process = driver.RenderProcess(node.config);
  if (node.network_namespace) {
    process.network_namespace_fd = node.network_namespace->fd();
  }
  WriteNodeState(events_path, options.run_id, node.config.id, "Starting");
  node.process = ChildProcess::Spawn(process, node.cgroup->path());
  ++node.restart_count;
  WriteEvent(events_path, options.run_id, node.config.id, "process_restarted",
             RestartDetail(node.process.pid(), node.restart_count));
  driver.WaitReady(node.config,
                   std::chrono::seconds(options.ready_timeout_sec));
  WriteEvent(events_path, options.run_id, node.config.id, "rpc_ready");
  WriteNodeState(events_path, options.run_id, node.config.id, "Running");
  WriteNodeLogTail(events_path, options, driver, node);
}

void ApplyRuntimeNodeRestarts(const Options& options,
                              const std::filesystem::path& events_path,
                              const FiroDriver& driver,
                              std::vector<NodeRuntime>& nodes) {
  for (uint32_t node_index : options.runtime_node_restarts) {
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime restart node is out of range");
    }
    RestartNode(options, events_path, driver, nodes[node_index]);
  }
}

bool WaitForNodeFrozenState(const Cgroup& cgroup, bool expected) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    if (cgroup.Frozen() == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

std::string FreezeDetail(uint32_t duration_ms, bool frozen) {
  boost::json::object detail;
  detail["duration_ms"] = duration_ms;
  detail["frozen"] = frozen;
  return boost::json::serialize(detail);
}

void FreezeNodeForDuration(const Options& options,
                           const std::filesystem::path& events_path,
                           NodeRuntime& node, uint32_t duration_ms) {
  if (!node.cgroup) {
    throw std::runtime_error("node freeze requires a node cgroup");
  }

  node.cgroup->Freeze();
  try {
    if (!WaitForNodeFrozenState(*node.cgroup, true)) {
      throw std::runtime_error("node cgroup did not report frozen: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id, "cgroup_frozen",
               FreezeDetail(duration_ms, true));
    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
    node.cgroup->Thaw();
    if (!WaitForNodeFrozenState(*node.cgroup, false)) {
      throw std::runtime_error("node cgroup did not report thawed: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id, "cgroup_thawed",
               FreezeDetail(duration_ms, false));
  } catch (...) {
    try {
      node.cgroup->Thaw();
    } catch (const std::exception&) {
    }
    throw;
  }
}

void ApplyRuntimeNodeFreezes(const Options& options,
                             const std::filesystem::path& events_path,
                             std::vector<NodeRuntime>& nodes) {
  for (const FreezeRequest& freeze : options.runtime_node_freezes) {
    if (freeze.node_index >= nodes.size()) {
      throw std::runtime_error("runtime freeze node is out of range");
    }
    FreezeNodeForDuration(options, events_path, nodes[freeze.node_index],
                          freeze.duration_ms);
  }
}

void StopNodes(const Options& options, const std::filesystem::path& events_path,
               const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  for (auto& node : nodes) {
    WriteNodeState(events_path, options.run_id, node.config.id, "Stopping");
    WriteEvent(events_path, options.run_id, node.config.id, "rpc_stop");
    driver.Stop(node.config);
  }
  for (auto& node : nodes) {
    if (!node.process.WaitForExit(std::chrono::seconds(15))) {
      WriteEvent(events_path, options.run_id, node.config.id, "sigterm");
      node.process.Terminate(std::chrono::seconds(5));
    }
    WriteNodeState(events_path, options.run_id, node.config.id, "Stopped");
    WriteNodeState(events_path, options.run_id, node.config.id, "Cleaning");
    if (node.cgroup && !options.keep_cgroups) {
      node.cgroup->KillAll();
      try {
        node.cgroup->Remove();
      } catch (const std::exception& e) {
        WriteEvent(events_path, options.run_id, node.config.id,
                   "cgroup_remove_failed", e.what());
      }
    }
    if (node.network) {
      DeleteNodeVethNetwork(*node.network);
      WriteEvent(events_path, options.run_id, node.config.id,
                 "network_removed");
    }
    if (node.network_namespace) {
      node.network_namespace->Stop();
    }
    WriteNodeState(events_path, options.run_id, node.config.id, "Cleaned");
  }
  if (!options.keep_cgroups) {
    try {
      Cgroup::RemoveRun(options.run_id);
    } catch (const std::exception& e) {
      WriteEvent(events_path, options.run_id, "sim", "run_cgroup_remove_failed",
                 e.what());
    }
  }
}

}  // namespace

int SimulatorApp::Run(int argc, char** argv) {
  Options options = ParseOptions(argc, argv);
  RequireSafeOutputDirectory(options.output_dir);
  if (options.probe_network) {
    BSIM_LOG(info) << NetworkProbeJson();
    return 0;
  }
  if (!options.report_run.empty()) {
    BSIM_LOG(info) << BuildRunReportJson(options.report_run);
    return 0;
  }
  if (!options.tui_run.empty()) {
    return RunTuiReport(options.tui_run, options.tui_once,
                        options.tui_refresh_ms);
  }
  if (options.probe_capabilities) {
    BSIM_LOG(info) << CapabilityProbeJson();
    return 0;
  }
  if (options.probe_cgroup_freeze) {
    BSIM_LOG(info) << CgroupFreezeProbeJson();
    return 0;
  }
  if (options.probe_drop_filter) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << DropFilterProbeJson();
    return 0;
  }
  if (options.probe_netns) {
    RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
    BSIM_LOG(info) << NetworkNamespaceProbeJson();
    return 0;
  }
  if (options.probe_veth) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << VethProbeJson();
    return 0;
  }
  if (options.probe_bandwidth_limit) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << BandwidthLimitProbeJson();
    return 0;
  }
  if (options.probe_network_condition) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << NetworkConditionProbeJson();
    return 0;
  }
  if (options.probe_combined_network_condition) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << CombinedNetworkConditionProbeJson();
    return 0;
  }
  if (options.probe_network_condition_update) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << NetworkConditionUpdateProbeJson();
    return 0;
  }
  if (options.probe_address) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << AddressProbeJson();
    return 0;
  }
  if (options.probe_route) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << RouteProbeJson();
    return 0;
  }
  if (options.probe_qdisc) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << QdiscProbeJson();
    return 0;
  }
  if (options.probe_qdisc_mutation) {
    RequireNetworkSetupCapabilities();
    BSIM_LOG(info) << QdiscMutationProbeJson();
    return 0;
  }
  if (options.cleanup_run) {
    CleanupRun(options);
    return 0;
  }

  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  if (std::filesystem::exists(run_root)) {
    if (!options.replace_run) {
      throw std::runtime_error(
          "run directory already exists: " + run_root.string() +
          " (use --replace-run to remove it)");
    }
    RequireOwnedRunDirectory(run_root);
    std::error_code ec;
    std::filesystem::remove_all(run_root, ec);
    if (ec) {
      throw std::runtime_error("remove existing run directory failed: " +
                               ec.message());
    }
    Cgroup::RemoveRun(options.run_id);
  }
  EnsureDirectory(run_root);
  AttachRunLogFile(run_root);
  BSIM_LOG(info) << "starting run " << options.run_id;
  WriteText(run_root / kRunMarkerFile, "benchmark-sim run\n");
  EnsureDirectory(run_root / "nodes");
  SimulationRegistry simulation_registry = SimulationRegistry::FromTopology(
      options.topology, options.wallet_initialization);
  WriteScenarioFiles(options, run_root);

  const auto events_path = run_root / "events.jsonl";
  const auto metrics_path = run_root / "metrics.jsonl";
  WriteEvent(events_path, options.run_id, "sim", "run_started");

  FiroDriver driver(std::chrono::seconds(5));
  std::vector<NodeRuntime> nodes;
  const auto handle_run_failure = [&](std::string_view detail) {
    for (auto& node : nodes) {
      WriteNodeState(events_path, options.run_id, node.config.id, "Failed");
    }
    WriteEvent(events_path, options.run_id, "sim", "run_failed", detail);
    WriteNodeLogTails(events_path, options, driver, nodes);
    StopNodes(options, events_path, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);
  };
  try {
    StartNodes(options, run_root, events_path, driver, nodes);
    InitializeWalletNodes(options, events_path, driver, nodes,
                          simulation_registry);
    WriteNodeLogTails(events_path, options, driver, nodes);

    WriteMetricsSnapshot(metrics_path, options, driver, nodes);

    ApplyRuntimeResourceLimitUpdates(options, events_path, nodes);
    ApplyRuntimeNetworkConditionUpdates(options, events_path, nodes);
    ApplyRuntimeNetworkBlockRules(options, events_path, nodes);
    ApplyRuntimeNetworkPartitions(options, events_path, nodes);
    ApplyRuntimeNetworkPartitionHeals(options, events_path, nodes);
    ApplyRuntimeNetworkUnblockRules(options, events_path, nodes);
    ApplyRuntimeNodeRestarts(options, events_path, driver, nodes);
    ApplyRuntimeNodeFreezes(options, events_path, nodes);
    WritePeriodicMetrics(events_path, metrics_path, options, driver, nodes);

    const std::vector<ScenarioWorkload> workloads = EffectiveWorkloads(options);
    for (size_t workload_index = 0; workload_index < workloads.size();
         ++workload_index) {
      const ScenarioWorkload& scenario_workload = workloads[workload_index];
      if (scenario_workload.kind == WorkloadKind::kBlockGeneration) {
        const BlockGenerationWorkload& workload =
            scenario_workload.block_generation;
        if (workload.count == 0U) {
          continue;
        }
        NodeRuntime& generator = nodes[workload.node - 1U];
        const uint64_t start_height =
            driver.ReadMetrics(generator.config).height;
        std::vector<std::string> hashes = driver.GenerateBlocks(
            generator.config, workload.count, kDefaultRewardAddress);
        generator.generated_block_count += hashes.size();
        const uint64_t target_height =
            start_height + static_cast<uint64_t>(hashes.size());
        WriteEvent(events_path, options.run_id, generator.config.id,
                   "generated_blocks",
                   GeneratedBlocksDetail(
                       static_cast<uint32_t>(workload_index + 1U),
                       static_cast<uint32_t>(workloads.size()), workload.node,
                       start_height, target_height, hashes));
        for (auto& node : nodes) {
          driver.WaitForHeight(node.config, target_height,
                               std::chrono::seconds(workload.sync_timeout_sec));
          WriteEvent(events_path, options.run_id, node.config.id,
                     "height_reached", std::to_string(target_height));
        }
      } else if (scenario_workload.kind == WorkloadKind::kWaitUntilHeight) {
        const WaitUntilHeightWorkload& workload =
            scenario_workload.wait_until_height;
        NodeRuntime& node = nodes[workload.node - 1U];
        driver.WaitForHeight(node.config, workload.height,
                             std::chrono::seconds(workload.timeout_sec));
        const uint64_t observed_height = driver.ReadMetrics(node.config).height;
        WriteEvent(
            events_path, options.run_id, node.config.id, "height_wait_reached",
            HeightWaitDetail(static_cast<uint32_t>(workload_index + 1U),
                             static_cast<uint32_t>(workloads.size()),
                             workload.node, workload.height, observed_height));
      } else if (scenario_workload.kind == WorkloadKind::kWaitForPeers) {
        const WaitForPeersWorkload& workload = scenario_workload.wait_for_peers;
        NodeRuntime& node = nodes[workload.node - 1U];
        driver.WaitForPeerCount(node.config, workload.peer_count,
                                std::chrono::seconds(workload.timeout_sec));
        const uint64_t observed_peer_count =
            driver.ReadMetrics(node.config).peer_count;
        WriteEvent(
            events_path, options.run_id, node.config.id, "peer_count_reached",
            PeerCountWaitDetail(static_cast<uint32_t>(workload_index + 1U),
                                static_cast<uint32_t>(workloads.size()),
                                workload.node, workload.peer_count,
                                observed_peer_count));
      } else if (scenario_workload.kind == WorkloadKind::kConnectPeer) {
        ApplyConnectPeerWorkload(options, events_path, driver, nodes,
                                 scenario_workload.connect_peer,
                                 static_cast<uint32_t>(workload_index + 1U),
                                 static_cast<uint32_t>(workloads.size()));
      } else if (scenario_workload.kind == WorkloadKind::kDisconnectPeer) {
        ApplyDisconnectPeerWorkload(options, events_path, driver, nodes,
                                    scenario_workload.disconnect_peer,
                                    static_cast<uint32_t>(workload_index + 1U),
                                    static_cast<uint32_t>(workloads.size()));
      } else if (scenario_workload.kind == WorkloadKind::kSendRawTransaction) {
        ApplySendRawTransactionWorkload(
            options, events_path, driver, nodes,
            scenario_workload.send_raw_transaction,
            static_cast<uint32_t>(workload_index + 1U),
            static_cast<uint32_t>(workloads.size()));
      } else if (scenario_workload.kind == WorkloadKind::kWalletTransactions) {
        ApplyWalletTransactionsWorkload(
            options, events_path, driver, nodes, simulation_registry,
            scenario_workload.wallet_transactions,
            static_cast<uint32_t>(workload_index + 1U),
            static_cast<uint32_t>(workloads.size()));
      } else if (scenario_workload.kind == WorkloadKind::kRestartNode) {
        const RestartNodeWorkload& workload = scenario_workload.restart_node;
        NodeRuntime& node = nodes[workload.node - 1U];
        RestartNode(options, events_path, driver, node);
        WriteEvent(events_path, options.run_id, node.config.id,
                   "node_restarted",
                   RestartNodeWorkloadDetail(
                       static_cast<uint32_t>(workload_index + 1U),
                       static_cast<uint32_t>(workloads.size()), workload.node,
                       node.restart_count));
      } else if (scenario_workload.kind == WorkloadKind::kFreezeNode) {
        const FreezeNodeWorkload& workload = scenario_workload.freeze_node;
        NodeRuntime& node = nodes[workload.node - 1U];
        FreezeNodeForDuration(options, events_path, node, workload.duration_ms);
        WriteEvent(
            events_path, options.run_id, node.config.id,
            "node_freeze_completed",
            FreezeNodeWorkloadDetail(static_cast<uint32_t>(workload_index + 1U),
                                     static_cast<uint32_t>(workloads.size()),
                                     workload.node, workload.duration_ms));
      } else if (scenario_workload.kind ==
                 WorkloadKind::kUpdateResourceLimits) {
        const ResourceLimitUpdateWorkload& workload =
            scenario_workload.update_resource_limits;
        NodeRuntime& node = nodes[workload.node - 1U];
        ApplyResourceLimitUpdate(options, events_path, node, workload.patch,
                                 static_cast<uint32_t>(workload_index + 1U),
                                 static_cast<uint32_t>(workloads.size()),
                                 workload.node);
      } else if (scenario_workload.kind == WorkloadKind::kPartitionNodes) {
        ApplyRuntimeNetworkPartition(
            options, events_path, nodes,
            scenario_workload.network_partition.partition, false,
            static_cast<uint32_t>(workload_index + 1U),
            static_cast<uint32_t>(workloads.size()));
      } else if (scenario_workload.kind == WorkloadKind::kHealPartition) {
        ApplyRuntimeNetworkPartition(
            options, events_path, nodes,
            scenario_workload.network_partition.partition, true,
            static_cast<uint32_t>(workload_index + 1U),
            static_cast<uint32_t>(workloads.size()));
      }
    }

    WriteMetricsSnapshot(metrics_path, options, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);

    StopNodes(options, events_path, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);
    WriteEvent(events_path, options.run_id, "sim", "run_finished");
    BSIM_LOG(info) << "finished run " << options.run_id;
  } catch (const std::exception& e) {
    handle_run_failure(e.what());
    throw;
  } catch (...) {
    handle_run_failure("unknown exception");
    throw;
  }

  BSIM_LOG(info) << "run_id=" << options.run_id << "\n"
                 << "output_dir=" << run_root << "\n"
                 << "metrics=" << metrics_path << "\n"
                 << "events=" << events_path;
  return 0;
}

}  // namespace bsim
