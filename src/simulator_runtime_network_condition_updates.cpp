#include "simulator_runtime_network_condition_updates.h"

#include <exception>
#include <mutex>
#include <stdexcept>

#include "bbp/network.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

void ApplyRuntimeNetworkConditionUpdates(
    const Options& options, const std::filesystem::path& events_path,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_network_state_mutex,
    std::stop_token stop_token) {
  for (const auto& [node_index, condition] :
       options.runtime_node_network_conditions) {
    ThrowIfStopRequested(stop_token);
    if (node_index >= nodes.size()) {
      throw std::runtime_error(
          "runtime network condition node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    QdiscInfo qdisc;
    NodeVethConfig updated_network;
    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      qdisc = ReplaceNodeNetworkConditionTransactional(&node, condition,
                                                       stop_token);
      try {
        updated_network = *node.network;
      } catch (...) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "network condition update completed without coherent runtime "
            "evidence",
            std::current_exception());
      }
    }
    try {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kNetworkConditionUpdated,
                 NetworkConditionVerificationDetail(updated_network, qdisc));
    } catch (...) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "network condition update completed without a publishable outcome",
          std::current_exception());
    }
  }
}

}  // namespace bbp::simulator_app_internal
