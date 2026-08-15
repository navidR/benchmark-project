#include "simulator_runtime_network_partition_rules.h"

#include "bbp/simulator/network_partition_rule.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_network_partition_application.h"

namespace bbp::simulator_app_internal {

void ApplyRuntimeNetworkPartitions(const Options& options,
                                   const std::filesystem::path& events_path,
                                   const RuntimeNodeSnapshot& nodes,
                                   std::mutex& node_network_state_mutex,
                                   std::stop_token stop_token) {
  for (const NetworkPartitionRule& partition : options.runtime_partitions) {
    ThrowIfStopRequested(stop_token);
    ApplyRuntimeNetworkPartition(options, events_path, nodes,
                                 node_network_state_mutex, partition, false, 0U,
                                 0U, stop_token);
  }
}

void ApplyRuntimeNetworkPartitionHeals(const Options& options,
                                       const std::filesystem::path& events_path,
                                       const RuntimeNodeSnapshot& nodes,
                                       std::mutex& node_network_state_mutex,
                                       std::stop_token stop_token) {
  for (const NetworkPartitionRule& partition :
       options.runtime_partition_heals) {
    ThrowIfStopRequested(stop_token);
    ApplyRuntimeNetworkPartition(options, events_path, nodes,
                                 node_network_state_mutex, partition, true, 0U,
                                 0U, stop_token);
  }
}

}  // namespace bbp::simulator_app_internal
