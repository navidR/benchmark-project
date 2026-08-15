#include "simulator_scenario_node_decoding.h"

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/chain_kind.h"
#include "bbp/positive_duration.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulator/options.h"
#include "simulator_json_field_decoding.h"
#include "simulator_scenario_identifier.h"
#include "simulator_wallet_configuration_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

std::filesystem::path ParseScenarioNodePath(const boost::json::object& node,
                                            const char* field,
                                            std::string_view node_id) {
  const std::string text = JsonStringField(node, field);
  if (text.empty()) {
    throw std::runtime_error("scenario node " + std::string(node_id) + " " +
                             field + " must not be empty");
  }
  if (text.size() > 4096U || text.find('\0') != std::string::npos) {
    throw std::runtime_error("scenario node " + std::string(node_id) + " " +
                             field + " is not a safe path");
  }
  return std::filesystem::path(text);
}

void ValidateScenarioNodeDataDirectory(const std::filesystem::path& data_dir,
                                       std::string_view node_id) {
  if (data_dir.string().size() > 1024U) {
    throw std::runtime_error("scenario node " + std::string(node_id) +
                             " data_dir is not a safe path");
  }
  if (data_dir.is_absolute() || data_dir.has_root_path()) {
    throw std::runtime_error("scenario node " + std::string(node_id) +
                             " data_dir must be run-relative");
  }
  std::vector<std::string> components;
  for (const std::filesystem::path& component : data_dir) {
    const std::string text = component.string();
    bool safe =
        !text.empty() && text.size() <= 64U && text != "." && text != "..";
    for (const char character : text) {
      safe = safe && ((character >= 'a' && character <= 'z') ||
                      (character >= 'A' && character <= 'Z') ||
                      (character >= '0' && character <= '9') ||
                      character == '-' || character == '_' || character == '.');
    }
    if (!safe) {
      throw std::runtime_error("scenario node " + std::string(node_id) +
                               " data_dir contains an unsafe component");
    }
    components.push_back(text);
  }
  if (components.size() < 3U || components[0] != "nodes" ||
      components[1] != node_id) {
    throw std::runtime_error(
        "scenario node " + std::string(node_id) +
        " data_dir must be below its owned nodes/<id> directory");
  }
}

}  // namespace

ScenarioNodeRoles ParseScenarioNodes(
    const boost::json::object& scenario,
    const boost::program_options::variables_map& vm, Options* options) {
  ScenarioNodeRoles roles;
  const boost::json::value* nodes_value = scenario.if_contains("nodes");
  if (nodes_value == nullptr || !nodes_value->is_array()) {
    if (!OptionProvided(vm, "nodes")) {
      const bool nodes_present = nodes_value != nullptr;
      const bool node_count_present =
          scenario.if_contains("node_count") != nullptr;
      const uint32_t scenario_nodes =
          JsonOptionalUint32Field(scenario, "nodes", options->nodes);
      const uint32_t scenario_node_count =
          JsonOptionalUint32Field(scenario, "node_count", scenario_nodes);
      if (nodes_present && node_count_present &&
          scenario_nodes != scenario_node_count) {
        throw std::runtime_error("scenario nodes and node_count must match");
      }
      options->nodes =
          node_count_present ? scenario_node_count : scenario_nodes;
    }
    return roles;
  }

  const boost::json::array& nodes = nodes_value->as_array();
  if (nodes.empty()) {
    throw std::runtime_error("scenario nodes array must not be empty");
  }
  if (nodes.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("scenario nodes array exceeds uint32 size");
  }
  const uint32_t node_count = static_cast<uint32_t>(nodes.size());
  if (OptionProvided(vm, "nodes") && options->nodes != node_count) {
    throw std::runtime_error("--nodes must match scenario nodes array size");
  }
  if (scenario.if_contains("node_count") != nullptr &&
      JsonUint32Field(scenario, "node_count") != node_count) {
    throw std::runtime_error(
        "scenario node_count must match scenario nodes array size");
  }
  options->nodes = node_count;
  options->node_ids.reserve(nodes.size());
  options->node_roles.reserve(nodes.size());
  options->scenario_node_configs.reserve(nodes.size());
  std::set<std::string> node_ids;
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    const boost::json::value& node_value = nodes[index];
    if (!node_value.is_object()) {
      throw std::runtime_error("scenario nodes entries must be objects");
    }
    const boost::json::object& node = node_value.as_object();
    const std::string id = JsonStringField(node, "id");
    RequireSafeScenarioIdentifier(id, "scenario node id");
    if (!node_ids.insert(id).second) {
      throw std::runtime_error("scenario nodes contains duplicate id: " + id);
    }
    for (const auto& member : node) {
      if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kNode,
                                      member.key())) {
        throw std::runtime_error(
            "scenario node " + id +
            " has unsupported field: " + std::string(member.key()));
      }
    }
    if (ParseChainKind(JsonStringField(node, "chain")) != options->chain) {
      throw std::runtime_error("scenario node " + id +
                               " chain must match the active chain");
    }
    std::string role = JsonStringField(node, "role");
    if (role == "node") {
      role = "base";
    }
    const bool wallet_role = role == "wallet" || role == "wallet_miner";
    const bool miner_role = role == "miner" || role == "wallet_miner";
    if (wallet_role) {
      roles.wallet_nodes.push_back(static_cast<uint32_t>(index));
    }
    if (miner_role) {
      roles.miner_nodes.push_back(static_cast<uint32_t>(index));
    }
    if (!wallet_role && !miner_role && role != "base") {
      throw std::runtime_error("scenario node " + id +
                               " role must be base, node, wallet, miner, or "
                               "wallet_miner");
    }

    ScenarioNodeConfig node_config;
    const auto read_lifecycle_time = [&](const char* field) {
      const boost::json::value* value = node.if_contains(field);
      if (value == nullptr) {
        return std::optional<std::chrono::milliseconds>{};
      }
      if (!value->is_string()) {
        throw std::runtime_error("scenario node " + id + " " + field +
                                 " must be a duration string");
      }
      return std::optional<std::chrono::milliseconds>(
          PositiveDuration::Parse(std::string_view(value->as_string()))
              .value());
    };
    node_config.lifecycle.start_time = read_lifecycle_time("start_time");
    node_config.lifecycle.stop_time = read_lifecycle_time("stop_time");
    const boost::json::value* restart_policy =
        node.if_contains("restart_policy");
    if (restart_policy != nullptr) {
      if (!restart_policy->is_string()) {
        throw std::runtime_error("scenario node " + id +
                                 " restart_policy must be a string");
      }
      const std::optional<NodeRestartPolicy> parsed =
          NodeRestartPolicyFromName(restart_policy->as_string());
      if (!parsed) {
        throw std::runtime_error(
            "scenario node " + id +
            " restart_policy must be never, on_failure, or always");
      }
      node_config.lifecycle.restart_policy = *parsed;
    }
    try {
      ValidateNodeLifecyclePolicy(node_config.lifecycle,
                                  options->simulation_duration);
      if (node_config.lifecycle.start_time) {
        static_cast<void>(options->time_scale.WallDuration(
            *node_config.lifecycle.start_time));
      }
      if (node_config.lifecycle.stop_time) {
        static_cast<void>(
            options->time_scale.WallDuration(*node_config.lifecycle.stop_time));
      }
    } catch (const std::exception& error) {
      throw std::runtime_error("scenario node " + id +
                               " lifecycle: " + error.what());
    }
    node_config.wallet = ParseScenarioNodeWalletConfig(
        node, id, wallet_role, options->wallet_initialization);
    if (node.if_contains("binary") != nullptr) {
      node_config.binary = ParseScenarioNodePath(node, "binary", id);
    }
    if (node.if_contains("data_dir") != nullptr) {
      node_config.data_dir = ParseScenarioNodePath(node, "data_dir", id);
      ValidateScenarioNodeDataDirectory(*node_config.data_dir, id);
      node_config.data_dir = node_config.data_dir->lexically_normal();
    }
    const boost::json::value* chain_config_value =
        node.if_contains("chain_config");
    if (chain_config_value != nullptr) {
      if (!chain_config_value->is_object()) {
        throw std::runtime_error("scenario node " + id +
                                 " chain_config must be an object");
      }
      const boost::json::object& chain_config = chain_config_value->as_object();
      for (const auto& member : chain_config) {
        if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kNodeChainConfig,
                                        member.key())) {
          throw std::runtime_error("scenario node " + id +
                                   " has unsupported chain_config field: " +
                                   std::string(member.key()));
        }
      }
      const boost::json::value* network = chain_config.if_contains("network");
      if (network != nullptr) {
        if (!network->is_string()) {
          throw std::runtime_error("scenario node " + id +
                                   " chain_config.network must be a string");
        }
        node_config.network = ParseChainNetwork(network->as_string());
      }
      const boost::json::value* extra_args =
          chain_config.if_contains("extra_args");
      if (extra_args != nullptr) {
        if (!extra_args->is_array()) {
          throw std::runtime_error("scenario node " + id +
                                   " chain_config.extra_args must be an array");
        }
        std::vector<std::string> arguments;
        arguments.reserve(extra_args->as_array().size());
        for (const boost::json::value& argument : extra_args->as_array()) {
          if (!argument.is_string()) {
            throw std::runtime_error(
                "scenario node " + id +
                " chain_config.extra_args entries must be strings");
          }
          arguments.emplace_back(argument.as_string());
        }
        node_config.extra_args = ChainExtraArgs(std::move(arguments));
      }
    }

    const auto read_profile = [&](const char* section,
                                  std::map<uint32_t, std::string>* output) {
      const boost::json::value* section_value = node.if_contains(section);
      if (section_value == nullptr) {
        return;
      }
      if (!section_value->is_object()) {
        throw std::runtime_error("scenario node " + id + " " + section +
                                 " must be an object");
      }
      for (const auto& member : section_value->as_object()) {
        if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kNodeProfile,
                                        member.key())) {
          throw std::runtime_error("scenario node " + id + " has unsupported " +
                                   section +
                                   " field: " + std::string(member.key()));
        }
      }
      const std::string profile =
          JsonStringField(section_value->as_object(), "profile");
      RequireSafeScenarioIdentifier(profile,
                                    std::string(section) + " profile name");
      output->emplace(static_cast<uint32_t>(index), profile);
    };
    read_profile("resources", &options->node_resource_profiles);
    read_profile("network", &options->node_network_profiles);
    options->node_ids.push_back(id);
    options->node_roles.push_back(role);
    options->scenario_node_configs.push_back(std::move(node_config));
  }
  roles.configured = true;
  return roles;
}

}  // namespace bbp::simulator_app_internal
