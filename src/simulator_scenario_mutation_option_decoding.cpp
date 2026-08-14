#include "simulator_scenario_mutation_option_decoding.h"

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/scenario_fields.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_resource_limit_decoding.h"

namespace bbp::simulator_app_internal {

void ApplyNodeConditions(const boost::json::array& conditions, uint32_t nodes,
                         std::string_view source,
                         std::map<uint32_t, NetworkCondition>& output) {
  for (const boost::json::value& value : conditions) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kNodeNetworkCondition),
        std::string(source) + " entry");
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
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kNetworkBlockRule),
        std::string(source) + " entry");
    NetworkBlockRule rule = ParseNetworkBlockRuleObject(object);
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
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kNetworkPartition),
        std::string(source) + " entry");
    NetworkPartitionRule rule = ParseNetworkPartitionRuleObject(object);
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
    RejectUnsupportedFields(
        object,
        ScenarioObjectFields(ScenarioObjectKind::kRuntimeResourceLimits),
        std::string(source) + " entry");
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output[node - 1U] = ParseResourceLimitPatchObject(object);
  }
}

}  // namespace bbp::simulator_app_internal
