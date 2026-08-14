#include "simulator_profile_assignment.h"

#include <boost/json/value.hpp>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "bbp/network.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/scenario_workload.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {
namespace {

bool NodeHasScenarioRole(const Options& options, uint32_t node_index,
                         std::string_view role) {
  if (role == "miner") {
    return NodeListContains(options.topology.miner_nodes, node_index);
  }
  if (role == "wallet") {
    return NodeListContains(options.topology.wallet_nodes, node_index);
  }
  if (role == "base" || role == "node") {
    return !NodeListContains(options.topology.miner_nodes, node_index) &&
           !NodeListContains(options.topology.wallet_nodes, node_index);
  }
  throw std::runtime_error(
      "profile switch role selector must be role:base, role:node, "
      "role:wallet, or role:miner");
}

}  // namespace

ProfileSwitchWorkload ParseProfileSwitchWorkload(
    const boost::json::object& object, const Options& options,
    WorkloadKind kind) {
  ProfileSwitchWorkload workload;
  workload.profile = JsonStringField(object, "profile");
  RequireSafeScenarioIdentifier(workload.profile, "profile switch name");
  const boost::json::value* targets = object.if_contains("nodes");
  if (targets == nullptr) {
    throw std::runtime_error("scenario " + std::string(WorkloadKindName(kind)) +
                             " requires nodes");
  }

  std::vector<std::string> requested_ids;
  if (targets->is_array()) {
    if (targets->as_array().empty()) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " nodes must not be empty");
    }
    for (const boost::json::value& target : targets->as_array()) {
      if (!target.is_string()) {
        throw std::runtime_error("scenario " +
                                 std::string(WorkloadKindName(kind)) +
                                 " nodes entries must be node ID strings");
      }
      requested_ids.emplace_back(target.as_string());
    }
  } else if (targets->is_string()) {
    const std::string selector(targets->as_string());
    constexpr std::string_view kRolePrefix = "role:";
    if (!std::string_view(selector).starts_with(kRolePrefix)) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " string nodes selector must start with role:");
    }
    const std::string_view role =
        std::string_view(selector).substr(kRolePrefix.size());
    for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
      if (NodeHasScenarioRole(options, node_index, role)) {
        requested_ids.push_back(ScenarioNodeId(options, node_index));
      }
    }
    if (requested_ids.empty()) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " role selector resolved no nodes");
    }
  } else {
    throw std::runtime_error("scenario " + std::string(WorkloadKindName(kind)) +
                             " nodes must be an ID array or role selector");
  }

  std::set<std::string> unique_ids;
  for (const std::string& requested_id : requested_ids) {
    if (!unique_ids.insert(requested_id).second) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " nodes contains duplicate ID: " + requested_id);
    }
    bool found = false;
    for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
      if (ScenarioNodeId(options, node_index) == requested_id) {
        workload.nodes.push_back(node_index + 1U);
        workload.node_ids.push_back(requested_id);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " references unknown node ID: " + requested_id);
    }
  }
  return workload;
}

void ParseNetworkProfiles(const boost::json::object& scenario,
                          Options* options) {
  const boost::json::value* value = scenario.if_contains("network_profiles");
  if (value == nullptr) {
    return;
  }
  if (!value->is_object()) {
    throw std::runtime_error("scenario network_profiles must be an object");
  }
  for (const auto& [name_json, profile_value] : value->as_object()) {
    const std::string name(name_json);
    RequireSafeScenarioIdentifier(name, "network profile name");
    if (!profile_value.is_object()) {
      throw std::runtime_error("scenario network profile " + name +
                               " must be an object");
    }
    RejectUnsupportedFields(
        profile_value.as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition),
        "scenario network profile " + name);
    const NetworkCondition condition =
        ParseNetworkConditionObject(profile_value.as_object());
    ValidateNetworkCondition(condition);
    options->network_profiles.emplace(name, condition);
  }
}

void ResolveNodeProfileAssignments(Options* options) {
  for (const auto& [node_index, profile_name] :
       options->node_resource_profiles) {
    const auto profile = options->resource_profiles.find(profile_name);
    if (profile == options->resource_profiles.end()) {
      throw std::runtime_error(
          "scenario node " + options->node_ids.at(node_index) +
          " references unknown resource profile: " + profile_name);
    }
    options->node_resource_limits.emplace(node_index, profile->second);
  }
  for (const auto& [node_index, profile_name] :
       options->node_network_profiles) {
    const auto profile = options->network_profiles.find(profile_name);
    if (profile == options->network_profiles.end()) {
      throw std::runtime_error(
          "scenario node " + options->node_ids.at(node_index) +
          " references unknown network profile: " + profile_name);
    }
    if (options->node_network_conditions.contains(node_index)) {
      throw std::runtime_error(
          "scenario node " + options->node_ids.at(node_index) +
          " cannot combine network.profile with network.node_conditions");
    }
    options->node_network_conditions.emplace(node_index, profile->second);
  }
}

void ValidateProfileSwitchReferences(Options* options) {
  const auto validate = [&](const ScenarioWorkload& workload) {
    if (workload.kind == WorkloadKind::kSetResourceProfile) {
      if (!options->resource_profiles.contains(
              workload.profile_switch.profile)) {
        throw std::runtime_error(
            "scenario set_resource_profile references unknown profile: " +
            workload.profile_switch.profile);
      }
    } else if (workload.kind == WorkloadKind::kSetNetworkProfile) {
      if (!options->network_profiles.contains(
              workload.profile_switch.profile)) {
        throw std::runtime_error(
            "scenario set_network_profile references unknown profile: " +
            workload.profile_switch.profile);
      }
    }
  };
  for (const ScenarioWorkload& workload : options->workloads) {
    validate(workload);
  }
  for (const ScheduledScenarioEvent& event : options->scheduled_events) {
    if (const auto* workload = std::get_if<ScenarioWorkload>(&event.action)) {
      validate(*workload);
      continue;
    }
    const SimulationCommand& command =
        std::get<SimulationCommand>(event.action);
    if (command.kind == SimulationCommandKind::kSetResourceProfile &&
        (!command.profile ||
         !options->resource_profiles.contains(*command.profile))) {
      throw std::runtime_error(
          "scenario scheduled command references unknown resource profile: " +
          (command.profile ? *command.profile : std::string("<missing>")));
    }
    if (command.kind == SimulationCommandKind::kSetNetworkProfile &&
        (!command.profile ||
         !options->network_profiles.contains(*command.profile))) {
      throw std::runtime_error(
          "scenario scheduled command references unknown network profile: " +
          (command.profile ? *command.profile : std::string("<missing>")));
    }
  }
}

}  // namespace bbp::simulator_app_internal
