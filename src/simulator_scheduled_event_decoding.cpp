#include "simulator_scheduled_event_decoding.h"

#include <algorithm>
#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/positive_duration.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/options.h"
#include "simulator_json_field_decoding.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_scenario_workload_decoding.h"
#include "simulator_scheduled_command_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

bool UsesScenarioCommandSchema(
    const boost::json::object& event,
    const std::optional<WorkloadKind>& workload_kind,
    const std::optional<SimulationCommandKind>& command_kind) {
  if (!command_kind) {
    return false;
  }
  if (!workload_kind) {
    return true;
  }
  const boost::json::value* node = event.if_contains("node");
  if (node != nullptr && node->is_string()) {
    return true;
  }
  return std::any_of(event.begin(), event.end(), [&](const auto& member) {
    return member.key() != "at" && member.key() != "action" &&
           ScenarioCommandFieldAllowed(*command_kind, member.key()) &&
           !ScenarioWorkloadFieldAllowed(*workload_kind, member.key());
  });
}

}  // namespace

void ApplyScheduledScenarioEvents(
    const boost::json::array& events,
    const boost::program_options::variables_map& vm, Options& options) {
  if (options.workloads.size() > kMaximumScenarioActionCount ||
      options.scheduled_events.size() >
          kMaximumScenarioActionCount - options.workloads.size() ||
      events.size() > kMaximumScenarioActionCount - options.workloads.size() -
                          options.scheduled_events.size()) {
    throw std::runtime_error("scenario action count exceeds retained limit " +
                             std::to_string(kMaximumScenarioActionCount));
  }
  struct ScheduledInput {
    std::size_t source_index;
    std::chrono::milliseconds at;
  };
  std::vector<ScheduledInput> ordered_inputs;
  ordered_inputs.reserve(events.size());
  for (std::size_t index = 0; index < events.size(); ++index) {
    const boost::json::value& value = events[index];
    if (!value.is_object()) {
      throw std::runtime_error("scenario events entries must be JSON objects");
    }
    const std::chrono::milliseconds scheduled_at =
        PositiveDuration::Parse(JsonStringField(value.as_object(), "at"))
            .value();
    const std::chrono::milliseconds scheduled_wall_at =
        options.time_scale.WallDuration(scheduled_at);
    const auto maximum_monotonic_delay =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::duration::max());
    if (scheduled_wall_at > maximum_monotonic_delay) {
      throw std::runtime_error(
          "scheduled event time exceeds monotonic clock range");
    }
    if (options.simulation_duration &&
        scheduled_at >= *options.simulation_duration) {
      throw std::runtime_error(
          "scheduled event time must be less than simulation duration");
    }
    ordered_inputs.push_back(
        ScheduledInput{.source_index = index, .at = scheduled_at});
  }
  std::stable_sort(ordered_inputs.begin(), ordered_inputs.end(),
                   [](const ScheduledInput& lhs, const ScheduledInput& rhs) {
                     if (lhs.at != rhs.at) {
                       return lhs.at < rhs.at;
                     }
                     return lhs.source_index < rhs.source_index;
                   });

  std::uint32_t planned_node_count = options.nodes;
  std::vector<std::string> planned_node_ids = options.node_ids;
  if (planned_node_ids.empty()) {
    planned_node_ids.reserve(options.node_capacity);
    const std::string& prefix =
        ChainDriverSpecFor(options.chain).node_id_prefix;
    for (std::uint32_t index = 0U; index < options.nodes; ++index) {
      planned_node_ids.push_back(prefix + "-" + std::to_string(index + 1U));
    }
  }
  std::set<std::string> planned_node_id_set(planned_node_ids.begin(),
                                            planned_node_ids.end());
  std::set<std::string> planned_used_node_id_set = planned_node_id_set;
  std::set<std::string> planned_wallet_node_ids;
  for (const std::uint32_t node : options.topology.wallet_nodes) {
    planned_wallet_node_ids.insert(ScenarioNodeId(options, node));
  }
  std::set<std::string> planned_miner_node_ids;
  for (const std::uint32_t node : options.topology.miner_nodes) {
    planned_miner_node_ids.insert(ScenarioNodeId(options, node));
  }
  std::set<std::string> planned_masternode_node_ids;
  for (const std::uint32_t node : options.topology.masternode_nodes) {
    planned_masternode_node_ids.insert(ScenarioNodeId(options, node));
  }
  for (const ScheduledInput& input : ordered_inputs) {
    const boost::json::object& event = events[input.source_index].as_object();
    const std::string action_name = JsonStringField(event, "action");
    const std::optional<WorkloadKind> workload_kind =
        ParseWorkloadKind(action_name);
    const std::optional<SimulationCommandKind> command_kind =
        SimulationCommandKindFromName(action_name);
    if (!workload_kind && !command_kind) {
      throw std::runtime_error("unsupported scheduled event action: " +
                               action_name);
    }
    if (event.if_contains("type") != nullptr) {
      throw std::runtime_error("scenario events entries use action, not type");
    }
    const std::uint32_t sequence =
        static_cast<std::uint32_t>(input.source_index + 1U);
    if (UsesScenarioCommandSchema(event, workload_kind, command_kind)) {
      Options validation_options = options;
      validation_options.nodes = planned_node_count;
      validation_options.node_ids = planned_node_ids;
      SimulationCommand command = ParseScheduledSimulationCommand(
          event, *command_kind, validation_options);
      command.scheduled_event_sequence = sequence;
      if (command.kind == SimulationCommandKind::kSendWalletTransaction) {
        options.wallet_backed_workload_requested = true;
      }
      if (command.kind == SimulationCommandKind::kAddNodes) {
        std::vector<std::string> reserved_ids = command.node_add->node_ids;
        if (reserved_ids.empty()) {
          const std::string& prefix =
              ChainDriverSpecFor(options.chain).node_id_prefix;
          reserved_ids.reserve(command.node_add->count);
          for (std::uint64_t suffix = 1U;
               suffix <= std::numeric_limits<std::uint32_t>::max() &&
               reserved_ids.size() < command.node_add->count;
               ++suffix) {
            const std::string candidate = prefix + "-" + std::to_string(suffix);
            if (!planned_used_node_id_set.insert(candidate).second) {
              continue;
            }
            planned_node_id_set.insert(candidate);
            reserved_ids.push_back(candidate);
          }
          if (reserved_ids.size() != command.node_add->count) {
            throw std::runtime_error(
                "scheduled node.add could not reserve canonical node ids");
          }
        } else {
          for (const std::string& node_id : reserved_ids) {
            if (!planned_used_node_id_set.insert(node_id).second) {
              throw std::runtime_error(
                  "scheduled node.add node id is already reserved: " + node_id);
            }
            planned_node_id_set.insert(node_id);
          }
        }
        planned_node_ids.insert(planned_node_ids.end(), reserved_ids.begin(),
                                reserved_ids.end());
        command.node_add->node_ids = std::move(reserved_ids);
        planned_node_count += command.node_add->count;
      } else if (command.kind == SimulationCommandKind::kAssignRole ||
                 command.kind == SimulationCommandKind::kRemoveRole) {
        if (!command.role_mutation) {
          throw std::logic_error(
              "scheduled role mutation omitted its typed request");
        }
        const SimulationRoleMutationRequest& mutation = *command.role_mutation;
        std::set<std::string>* planned_roles = nullptr;
        switch (mutation.role) {
          case SimulationRoleKind::kWallet:
            planned_roles = &planned_wallet_node_ids;
            break;
          case SimulationRoleKind::kMiner:
            planned_roles = &planned_miner_node_ids;
            break;
          case SimulationRoleKind::kMasternode:
            planned_roles = &planned_masternode_node_ids;
            break;
          case SimulationRoleKind::kCount:
            throw std::logic_error("scheduled role mutation has unknown role");
        }
        const std::string role_name(SimulationRoleKindName(mutation.role));
        if (command.kind == SimulationCommandKind::kAssignRole) {
          if (mutation.role == SimulationRoleKind::kWallet &&
              (!mutation.mode ||
               *mutation.mode != options.wallet_initialization.mode)) {
            throw std::runtime_error(
                "scheduled wallet assignment mode must match the active run "
                "wallet mode");
          }
          if (mutation.role == SimulationRoleKind::kMasternode &&
              (!mutation.funding_wallet_id ||
               !planned_wallet_node_ids.contains(
                   *mutation.funding_wallet_id))) {
            throw std::runtime_error(
                "scheduled masternode assignment requires a planned funding "
                "wallet role");
          }
          for (const std::string& node_id : mutation.node_ids) {
            if (mutation.role == SimulationRoleKind::kWallet &&
                !options.topology.allow_miner_wallet_overlap &&
                planned_miner_node_ids.contains(node_id)) {
              throw std::runtime_error(
                  "scheduled wallet assignment conflicts with planned miner "
                  "role on " +
                  node_id);
            }
            if (mutation.role == SimulationRoleKind::kMiner &&
                !options.topology.allow_miner_wallet_overlap &&
                planned_wallet_node_ids.contains(node_id)) {
              throw std::runtime_error(
                  "scheduled miner assignment conflicts with planned wallet "
                  "role on " +
                  node_id);
            }
            if (!planned_roles->insert(node_id).second) {
              throw std::runtime_error("scheduled " + role_name +
                                       " role is already assigned to " +
                                       node_id);
            }
          }
        } else {
          for (const std::string& node_id : mutation.node_ids) {
            if (planned_roles->erase(node_id) != 1U) {
              throw std::runtime_error("scheduled " + role_name +
                                       " role is not assigned to " + node_id);
            }
          }
        }
      } else if (command.kind == SimulationCommandKind::kRemoveNodes) {
        for (const std::string& node_id : command.node_remove->node_ids) {
          if (planned_wallet_node_ids.contains(node_id)) {
            throw std::runtime_error(
                "scheduled node.remove requires wallet.remove before "
                "removing wallet node " +
                node_id);
          }
          if (planned_miner_node_ids.contains(node_id)) {
            throw std::runtime_error(
                "scheduled node.remove requires miner.remove before removing "
                "miner node " +
                node_id);
          }
          if (planned_masternode_node_ids.contains(node_id)) {
            throw std::runtime_error(
                "scheduled node.remove requires masternode.remove before "
                "removing masternode node " +
                node_id);
          }
          if (!planned_node_id_set.erase(node_id)) {
            throw std::runtime_error(
                "scheduled node.remove references an inactive node id: " +
                node_id);
          }
          const auto planned = std::find(planned_node_ids.begin(),
                                         planned_node_ids.end(), node_id);
          if (planned == planned_node_ids.end()) {
            throw std::logic_error(
                "scheduled node.remove planned identity state diverged");
          }
          planned_node_ids.erase(planned);
        }
        const std::uint32_t removed_count =
            static_cast<std::uint32_t>(command.node_remove->node_ids.size());
        if (removed_count > planned_node_count) {
          throw std::logic_error(
              "scheduled node.remove planned count would underflow");
        }
        planned_node_count -= removed_count;
      }
      options.scheduled_events.emplace_back(input.at, sequence,
                                            std::move(command));
      continue;
    }

    RejectUnsupportedScenarioActionFields(event, *workload_kind, true);
    boost::json::object action_object = event;
    action_object.erase("at");
    action_object.erase("action");
    action_object["type"] = action_name;
    boost::json::array action_array;
    action_array.push_back(std::move(action_object));
    Options workload_options = options;
    workload_options.nodes = planned_node_count;
    workload_options.node_ids = planned_node_ids;
    const std::size_t workload_count = workload_options.workloads.size();
    ApplyScenarioWorkloads(action_array, vm, workload_options);
    if (workload_options.workloads.size() != workload_count + 1U) {
      throw std::runtime_error("scheduled event did not produce one action");
    }
    options.wallet_backed_workload_requested =
        options.wallet_backed_workload_requested ||
        workload_options.wallet_backed_workload_requested;
    ScenarioWorkload workload = std::move(workload_options.workloads.back());
    options.scheduled_events.emplace_back(input.at, sequence,
                                          std::move(workload));
  }
}

}  // namespace bbp::simulator_app_internal
