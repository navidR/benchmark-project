#include "simulator_network_partition_application.h"

#include <boost/json/array.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/network_partition_rule.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_block_application.h"
#include "simulator_network_event_details.h"
#include "simulator_network_partition_planning.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

void ApplyRuntimeNetworkPartition(
    const Options& options, const std::filesystem::path& events_path,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_network_state_mutex,
    const NetworkPartitionRule& partition, bool heal,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::stop_token stop_token,
    std::optional<std::uint64_t> operator_sequence) {
  struct PartitionRuleState {
    NetworkBlockRule rule;
    bool existed_before = false;
    bool present_after = false;
  };
  const std::vector<NetworkBlockRule> rules =
      PartitionBlockRules(partition, nodes);
  std::vector<PartitionRuleState> states;
  states.reserve(rules.size());
  boost::json::array rule_results;
  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    std::set<std::pair<std::uint32_t, std::uint32_t>> planned_handles;
    for (const NetworkBlockRule& rule : rules) {
      ThrowIfStopRequested(stop_token);
      NodeRuntime& node = nodes[rule.node_index];
      RequireNetworkBlockNode(node);
      if (!planned_handles.emplace(rule.node_index, rule.handle).second) {
        throw std::runtime_error(
            "network partition produced a duplicate rule handle: " +
            std::to_string(rule.handle));
      }
      RequireNetworkBlockHandleAvailable(node, rule);
      states.push_back(PartitionRuleState{
          .rule = rule,
          .existed_before = NetworkBlockRulePresent(node, rule),
          .present_after = false,
      });
    }

    std::vector<std::size_t> attempted;
    try {
      for (std::size_t index = 0; index < states.size(); ++index) {
        ThrowIfStopRequested(stop_token);
        attempted.push_back(index);
        PartitionRuleState& state = states[index];
        NodeRuntime& node = nodes[state.rule.node_index];
        if (heal) {
          if (state.existed_before) {
            DeleteEgressIpv4TcpDropFilter(node.network->host_name,
                                          state.rule.handle);
          }
        } else {
          ReplaceEgressIpv4TcpDropFilter(
              node.network->host_name, state.rule.src_address,
              state.rule.src_port, state.rule.dst_address, state.rule.dst_port,
              state.rule.handle);
        }
        ThrowIfStopRequested(stop_token);
        state.present_after = NetworkBlockRulePresent(node, state.rule);
        if (!heal && !state.present_after) {
          throw std::runtime_error(
              "runtime network partition rule was not visible after apply");
        }
        if (heal && state.present_after) {
          throw std::runtime_error(
              "runtime network partition rule remained after heal");
        }
      }
    } catch (...) {
      const std::exception_ptr original_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      for (auto iter = attempted.rbegin(); iter != attempted.rend(); ++iter) {
        const PartitionRuleState& state = states[*iter];
        try {
          RestoreNetworkBlockRule(nodes[state.rule.node_index], state.rule,
                                  state.existed_before);
        } catch (const std::exception& error) {
          rollback_errors.push_back(std::to_string(state.rule.handle) + " on " +
                                    nodes[state.rule.node_index].config.id +
                                    ": " + error.what());
          BBP_LOG(error) << "failed to roll back partition rule "
                         << state.rule.handle << " on "
                         << nodes[state.rule.node_index].config.id << ": "
                         << error.what();
        } catch (...) {
          rollback_errors.push_back(std::to_string(state.rule.handle) + " on " +
                                    nodes[state.rule.node_index].config.id +
                                    ": unknown exception");
          BBP_LOG(error) << "failed to roll back partition rule "
                         << state.rule.handle << " on "
                         << nodes[state.rule.node_index].config.id
                         << ": unknown exception";
        }
      }
      if (!rollback_errors.empty()) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "network partition mutation outcome is unconfirmed", original_error,
            rollback_errors);
      }
      std::rethrow_exception(original_error);
    }
  }

  try {
    for (const PartitionRuleState& state : states) {
      const NodeRuntime& node = nodes[state.rule.node_index];
      rule_results.push_back(PartitionRuleResultJson(
          node, state.rule, state.existed_before, state.present_after));
    }

    const SimulationEventKind event_kind =
        heal ? SimulationEventKind::kNetworkPartitionHealed
             : SimulationEventKind::kNetworkPartitionApplied;
    WriteEvent(events_path, options.run_id, "sim", event_kind,
               NetworkPartitionDetail(partition, rule_results, workload_index,
                                      workload_count, operator_sequence));
  } catch (...) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "network partition completed without a publishable outcome",
        std::current_exception());
  }
}

}  // namespace bbp::simulator_app_internal
