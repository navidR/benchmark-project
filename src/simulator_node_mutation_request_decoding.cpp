#include <array>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/network.h"
#include "bbp/scenario_fields.h"
#include "bbp/scenario_service.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_peer_topology_decoding.h"
#include "simulator_resource_limit_decoding.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_scenario_identifier.h"

namespace bbp {

using simulator_app_internal::ApplyResourceLimitPatch;
using simulator_app_internal::InitialResourceLimits;
using simulator_app_internal::JsonOptionalUint32Field;
using simulator_app_internal::JsonStringField;
using simulator_app_internal::JsonUint32Field;
using simulator_app_internal::ParseNetworkConditionObject;
using simulator_app_internal::ParsePeerTopologyConfig;
using simulator_app_internal::ParseResourceLimitPatchObject;
using simulator_app_internal::RejectUnsupportedFields;
using simulator_app_internal::RequireSafeScenarioIdentifier;

SimulationNodeAddRequest ParseAndValidateSimulationNodeAddRequest(
    const boost::json::object& request, const Options& options) {
  static constexpr std::array<std::string_view, 9U> kFields{
      "chain",           "count",     "node_ids", "binary",
      "topology",        "resources", "network",  "ready_timeout_sec",
      "sync_timeout_sec"};
  RejectUnsupportedFields(request, kFields, "node.add request");

  SimulationNodeAddRequest result;
  result.chain = ParseChainKind(JsonStringField(request, "chain"));
  if (result.chain != options.chain) {
    throw std::runtime_error(
        "node.add chain must match the active simulation chain");
  }
  result.count = JsonUint32Field(request, "count");
  result.ready_timeout_sec = JsonOptionalUint32Field(
      request, "ready_timeout_sec", result.ready_timeout_sec);
  result.sync_timeout_sec = JsonOptionalUint32Field(request, "sync_timeout_sec",
                                                    result.sync_timeout_sec);
  if (result.count == 0U || result.count > kSimulationNodeAddMaximumCount ||
      result.ready_timeout_sec == 0U ||
      result.ready_timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds ||
      result.sync_timeout_sec == 0U ||
      result.sync_timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds) {
    throw std::runtime_error(
        "node.add count must be in 1..16 and timeout fields must be in "
        "1..600");
  }
  if (options.nodes > options.node_capacity ||
      result.count > options.node_capacity - options.nodes) {
    throw std::runtime_error(
        "node.add request exceeds the configured node capacity");
  }
  const std::uint32_t final_node_count = options.nodes + result.count;

  if (const boost::json::value* node_ids = request.if_contains("node_ids")) {
    if (!node_ids->is_array()) {
      throw std::runtime_error("node.add node_ids must be an array");
    }
    std::set<std::string> unique;
    for (const boost::json::value& value : node_ids->as_array()) {
      if (!value.is_string()) {
        throw std::runtime_error("node.add node_ids must contain strings");
      }
      std::string node_id(value.as_string());
      RequireSafeScenarioIdentifier(node_id, "node.add node id");
      if (!unique.insert(node_id).second) {
        throw std::runtime_error(
            "node.add node_ids must contain unique values");
      }
      result.node_ids.push_back(std::move(node_id));
    }
    if (result.node_ids.size() != result.count) {
      throw std::runtime_error(
          "node.add node_ids must match the requested count");
    }
  }

  if (const boost::json::value* binary = request.if_contains("binary")) {
    if (!binary->is_string()) {
      throw std::runtime_error(
          "node.add binary must be a non-empty path without NUL");
    }
    const std::string binary_text(binary->as_string());
    if (binary_text.empty() || binary_text.find('\0') != std::string::npos) {
      throw std::runtime_error(
          "node.add binary must be a non-empty path without NUL");
    }
    result.binary = binary_text;
  }
  if (const boost::json::value* topology = request.if_contains("topology")) {
    if (!topology->is_object()) {
      throw std::runtime_error("node.add topology must be an object");
    }
    static constexpr std::array<std::string_view, 8U> kIgnoredTopologyFields{
        "node_count",
        "wallet_node_count",
        "miner_node_count",
        "wallet_nodes",
        "miner_nodes",
        "allow_miner_wallet_overlap",
        "wallet_initialization",
        "peer_connectivity"};
    for (const std::string_view field : kIgnoredTopologyFields) {
      if (topology->as_object().contains(field)) {
        throw std::runtime_error(
            "node.add topology does not accept role or node-count field: " +
            std::string(field));
      }
    }
    result.topology = ParsePeerTopologyConfig(
        topology->as_object(), final_node_count, options.simulation_seed);
  }
  if (const boost::json::value* resources = request.if_contains("resources")) {
    if (!resources->is_object()) {
      throw std::runtime_error("node.add resources must be an object");
    }
    RejectUnsupportedFields(
        resources->as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kResourceLimits),
        "node.add resources");
    result.resources = ApplyResourceLimitPatch(
        InitialResourceLimits(options),
        ParseResourceLimitPatchObject(resources->as_object()), "node.add");
  }
  if (const boost::json::value* network = request.if_contains("network")) {
    if (!network->is_object()) {
      throw std::runtime_error("node.add network must be an object");
    }
    RejectUnsupportedFields(
        network->as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition),
        "node.add network");
    result.network = ParseNetworkConditionObject(network->as_object());
    ValidateNetworkCondition(*result.network);
  }
  return result;
}

SimulationNodeReplaceRequest ParseAndValidateSimulationNodeReplaceRequest(
    const boost::json::object& request, std::string_view node_id,
    const Options& options) {
  static constexpr std::array<std::string_view, 8U> kFields{
      "chain",     "count",   "node_ids",          "binary",
      "resources", "network", "ready_timeout_sec", "sync_timeout_sec"};
  RejectUnsupportedFields(request, kFields, "node.replace request");
  RequireSafeScenarioIdentifier(node_id, "node.replace node id");

  std::set<std::string> active_ids;
  if (options.node_ids.empty()) {
    const std::string& prefix =
        ChainDriverSpecFor(options.chain).node_id_prefix;
    for (std::uint32_t index = 0U; index < options.nodes; ++index) {
      active_ids.insert(prefix + "-" + std::to_string(index + 1U));
    }
  } else {
    active_ids.insert(options.node_ids.begin(), options.node_ids.end());
  }
  if (!active_ids.contains(std::string(node_id))) {
    throw std::runtime_error("node.replace references an inactive node id: " +
                             std::string(node_id));
  }

  SimulationNodeReplaceRequest result;
  result.chain = ParseChainKind(JsonStringField(request, "chain"));
  if (result.chain != options.chain) {
    throw std::runtime_error(
        "node.replace chain must match the active simulation chain");
  }
  result.count = JsonUint32Field(request, "count");
  result.ready_timeout_sec = JsonOptionalUint32Field(
      request, "ready_timeout_sec", result.ready_timeout_sec);
  result.sync_timeout_sec = JsonOptionalUint32Field(request, "sync_timeout_sec",
                                                    result.sync_timeout_sec);
  if (result.count != 1U) {
    throw std::runtime_error("node.replace count must equal one");
  }
  if (result.ready_timeout_sec == 0U ||
      result.ready_timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds ||
      result.sync_timeout_sec == 0U ||
      result.sync_timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds) {
    throw std::runtime_error("node.replace timeout fields must be in 1..600");
  }
  if (const boost::json::value* node_ids = request.if_contains("node_ids")) {
    if (!node_ids->is_array() || node_ids->as_array().size() != 1U ||
        !node_ids->as_array().front().is_string()) {
      throw std::runtime_error(
          "node.replace node_ids must contain the selected node");
    }
    std::string requested_id(node_ids->as_array().front().as_string());
    RequireSafeScenarioIdentifier(requested_id, "node.replace node id");
    result.node_ids.push_back(std::move(requested_id));
  }
  if (!result.node_ids.empty() &&
      (result.node_ids.size() != 1U || result.node_ids.front() != node_id)) {
    throw std::runtime_error(
        "node.replace explicit node id must equal the selected node");
  }
  if (const boost::json::value* binary = request.if_contains("binary")) {
    if (!binary->is_string()) {
      throw std::runtime_error(
          "node.replace binary must be a non-empty path without NUL");
    }
    const std::string binary_text(binary->as_string());
    if (binary_text.empty() || binary_text.find('\0') != std::string::npos) {
      throw std::runtime_error(
          "node.replace binary must be a non-empty path without NUL");
    }
    result.binary = binary_text;
  }
  if (const boost::json::value* resources = request.if_contains("resources")) {
    if (!resources->is_object()) {
      throw std::runtime_error("node.replace resources must be an object");
    }
    RejectUnsupportedFields(
        resources->as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kResourceLimits),
        "node.replace resources");
    result.resources = ParseResourceLimitPatchObject(resources->as_object());
  }
  if (const boost::json::value* network = request.if_contains("network")) {
    if (!network->is_object()) {
      throw std::runtime_error("node.replace network must be an object");
    }
    RejectUnsupportedFields(
        network->as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition),
        "node.replace network");
    result.network = ParseNetworkConditionObject(network->as_object());
    ValidateNetworkCondition(*result.network);
  }
  return result;
}

SimulationNodeRemoveRequest ParseAndValidateSimulationNodeRemoveRequest(
    const boost::json::object& request, const Options& options) {
  constexpr std::array<std::string_view, 2U> kFields = {"node_ids",
                                                        "timeout_sec"};
  RejectUnsupportedFields(request, kFields, "node.remove request");
  SimulationNodeRemoveRequest result;
  const boost::json::value* node_ids = request.if_contains("node_ids");
  if (node_ids == nullptr || !node_ids->is_array()) {
    throw std::runtime_error("node.remove node_ids must be an array");
  }
  if (node_ids->as_array().empty() ||
      node_ids->as_array().size() > kSimulationNodeRemoveMaximumCount) {
    throw std::runtime_error("node.remove node_ids size must be in 1.." +
                             std::to_string(kSimulationNodeRemoveMaximumCount));
  }
  std::set<std::string> unique;
  std::set<std::string> active_ids;
  if (options.node_ids.empty()) {
    const std::string& prefix =
        ChainDriverSpecFor(options.chain).node_id_prefix;
    for (std::uint32_t index = 0U; index < options.nodes; ++index) {
      active_ids.insert(prefix + "-" + std::to_string(index + 1U));
    }
  } else {
    active_ids.insert(options.node_ids.begin(), options.node_ids.end());
  }
  result.node_ids.reserve(node_ids->as_array().size());
  for (const boost::json::value& value : node_ids->as_array()) {
    if (!value.is_string()) {
      throw std::runtime_error("node.remove node_ids must contain strings");
    }
    std::string node_id(value.as_string());
    RequireSafeScenarioIdentifier(node_id, "node.remove node id");
    if (!unique.insert(node_id).second) {
      throw std::runtime_error("node.remove node_ids must be unique");
    }
    if (!active_ids.contains(node_id)) {
      throw std::runtime_error("node.remove references an inactive node id: " +
                               node_id);
    }
    result.node_ids.push_back(std::move(node_id));
  }
  result.timeout_sec = JsonOptionalUint32Field(request, "timeout_sec", 30U);
  if (result.timeout_sec == 0U ||
      result.timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds) {
    throw std::runtime_error(
        "node.remove timeout_sec must be in 1.." +
        std::to_string(kSimulationNodeAddMaximumTimeoutSeconds));
  }
  return result;
}

}  // namespace bbp
