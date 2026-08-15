#include "simulator_runtime_network_block_rules.h"

#include <exception>
#include <mutex>
#include <stdexcept>

#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_block_application.h"
#include "simulator_network_event_details.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

void ApplyRuntimeNetworkBlockRules(const Options& options,
                                   const std::filesystem::path& events_path,
                                   const RuntimeNodeSnapshot& nodes,
                                   std::mutex& node_network_state_mutex,
                                   std::stop_token stop_token) {
  for (const NetworkBlockRule& rule : options.runtime_node_blocks) {
    ThrowIfStopRequested(stop_token);
    if (rule.node_index >= nodes.size()) {
      throw std::runtime_error("runtime network block node is out of range");
    }
    NodeRuntime& node = nodes[rule.node_index];
    NetworkBlockMutationResult result;
    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      result =
          MutateNetworkBlockRuleTransactional(node, rule, false, stop_token);
    }
    try {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kNetworkBlockApplied,
                 NetworkBlockRuleDetail(node, rule, result.existed_before,
                                        result.present_after));
    } catch (...) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "network block completed without a publishable outcome",
          std::current_exception());
    }
  }
}

void ApplyRuntimeNetworkUnblockRules(const Options& options,
                                     const std::filesystem::path& events_path,
                                     const RuntimeNodeSnapshot& nodes,
                                     std::mutex& node_network_state_mutex,
                                     std::stop_token stop_token) {
  for (const NetworkBlockRule& rule : options.runtime_node_unblocks) {
    ThrowIfStopRequested(stop_token);
    if (rule.node_index >= nodes.size()) {
      throw std::runtime_error("runtime network unblock node is out of range");
    }
    NodeRuntime& node = nodes[rule.node_index];
    NetworkBlockMutationResult result;
    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      result =
          MutateNetworkBlockRuleTransactional(node, rule, true, stop_token);
    }
    try {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kNetworkBlockRemoved,
                 NetworkBlockRuleDetail(node, rule, result.existed_before,
                                        result.present_after));
    } catch (...) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "network unblock completed without a publishable outcome",
          std::current_exception());
    }
  }
}

}  // namespace bbp::simulator_app_internal
