#include "simulator_profile_assignment.h"

#include <boost/json/value.hpp>
#include <stdexcept>
#include <string>
#include <variant>

#include "bbp/network.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/scenario_workload.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_resource_profile_decoding.h"

namespace bbp::simulator_app_internal {

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
