#include "simulator_runtime_node_preparation.h"

#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/run_ownership.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_event_writing.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_network_launch_planning.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

RuntimeNodeResourceEntry RuntimeNodeResourceEntryFor(
    const Options& options, const ChainNodeConfig& config,
    std::uint32_t resource_slot, RuntimeNodeResourceState state) {
  const RunOwnership& ownership = RequireRunOwnership(options);
  return RuntimeNodeResourceEntry{
      .node_id = config.id,
      .slot = resource_slot,
      .chain = options.chain,
      .data_dir = config.data_dir.lexically_relative(ownership.run_root),
      .root_name = std::nullopt,
      .state = state,
  };
}

RuntimeNodeResourceManifest RuntimeNodeResourceManifestFor(
    const Options& options, const RuntimeNodeSnapshot& nodes,
    RuntimeNodeResourceState state) {
  RuntimeNodeResourceManifest manifest{
      .ownership = RequireRunOwnership(options),
      .isolated_network = options.isolate_network,
      .nodes = {},
  };
  manifest.nodes.reserve(nodes.size());
  for (std::size_t index = 0U; index < nodes.size(); ++index) {
    manifest.nodes.push_back(RuntimeNodeResourceEntryFor(
        options, nodes[index].config, nodes.slot(index), state));
  }
  return manifest;
}

void PrepareNodeRuntime(
    const Options& options, const std::filesystem::path& events_path,
    NodeRuntime& runtime, ChainNodeConfig config, std::uint32_t resource_slot,
    ResourceLimits resources, std::string resource_profile,
    std::string network_profile,
    std::vector<DirectionalNetworkPolicy> directional_network_policies,
    std::optional<NetworkCondition> network_condition,
    RunProcessState& run_process_state, std::stop_token stop_token,
    bool* runtime_root_acquired) {
  const std::string node_id = config.id;
  runtime.config = std::move(config);
  runtime.run_process_state = &run_process_state;
  runtime.perf_counter_target_kind = PerfCounterTargetKind::kNode;
  runtime.perf_counter_target_id = node_id;
  runtime.resource_profile = std::move(resource_profile);
  runtime.network_profile = std::move(network_profile);
  PrepareRuntimeNodeRoot(
      RequireRunOwnership(options),
      RuntimeNodeResourceEntryFor(options, runtime.config, resource_slot,
                                  RuntimeNodeResourceState::kLive),
      runtime_root_acquired);
  TransitionNodeState(events_path, options.run_id, runtime,
                      NodeRuntimeLifecycle::kDefined);
  TransitionNodeState(events_path, options.run_id, runtime,
                      NodeRuntimeLifecycle::kPreparing);
  runtime.cgroup = std::make_shared<Cgroup>(
      Cgroup::Create(RequireRunOwnership(options).cgroup_name, node_id));
  runtime.resources = std::move(resources);
  runtime.cgroup->SetMemoryHigh(runtime.resources.memory_high_bytes);
  runtime.cgroup->SetMemoryMax(runtime.resources.memory_max_bytes);
  runtime.cgroup->SetCpuMax(runtime.resources.cpu_quota_us,
                            runtime.resources.cpu_period_us);
  runtime.cgroup->SetCpuWeight(runtime.resources.cpu_weight);
  runtime.cgroup->SetIoWeight(runtime.resources.io_weight);
  for (const IoLimit& io_limit : runtime.resources.io_limits) {
    runtime.cgroup->SetIoMax(io_limit);
  }
  runtime.cgroup->SetPidsMax(runtime.resources.pids_max);

  if (options.isolate_network) {
    runtime.network_namespace = std::make_shared<NetworkNamespace>(
        NetworkNamespace::Create(runtime.cgroup->access_path()));
    runtime.network = MakeNodeVethConfig(options, resource_slot);
    if (network_condition) {
      runtime.network->apply_condition = true;
      runtime.network->condition = *network_condition;
    }
    std::optional<NodeVethIdentity> acquired_veth;
    try {
      SetupNodeVethNetwork(runtime.network_namespace->fd(), *runtime.network,
                           &acquired_veth, stop_token);
    } catch (...) {
      if (acquired_veth) {
        runtime.network_namespace->SetNodeVethIdentity(
            std::move(*acquired_veth));
      }
      throw;
    }
    if (!acquired_veth) {
      throw std::runtime_error(
          "node veth setup returned without an acquired identity");
    }
    runtime.network_namespace->SetNodeVethIdentity(std::move(*acquired_veth));
    if (runtime.network->apply_condition) {
      const QdiscInfo qdisc =
          VerifyNodeNetworkCondition(*runtime.network, stop_token);
      WriteEvent(events_path, options.run_id, node_id,
                 SimulationEventKind::kNetworkConditionVerified,
                 NetworkConditionVerificationDetail(*runtime.network, qdisc));
    }
    runtime.directional_network_policies =
        std::move(directional_network_policies);
    if (!runtime.directional_network_policies.empty()) {
      UpdateDirectionalNetworkPoliciesInNamespace(
          runtime.network_namespace->fd(), runtime.network->peer_name, {},
          runtime.directional_network_policies, stop_token);
      boost::json::object detail;
      detail["source_node"] = resource_slot + 1U;
      detail["peer_if"] = runtime.network->peer_name;
      detail["verified"] = true;
      detail["policies"] =
          DirectionalNetworkPoliciesJson(runtime.directional_network_policies);
      WriteEvent(events_path, options.run_id, node_id,
                 SimulationEventKind::kDirectionalNetworkPoliciesVerified,
                 boost::json::serialize(detail));
    }
    runtime.config.rpc_host = runtime.network->node_address;
    runtime.config.rpc_bind = runtime.network->node_address;
    runtime.config.rpc_allow_ips = {runtime.network->host_address};
    runtime.config.p2p_host = runtime.network->node_address;
    runtime.config.p2p_bind = runtime.network->node_address;
    WriteEvent(events_path, options.run_id, node_id,
               SimulationEventKind::kNetworkReady,
               "node_ip=" + runtime.network->node_address +
                   " host_if=" + runtime.network->host_name +
                   " peer_if=" + runtime.network->peer_name);
    TransitionNodeState(events_path, options.run_id, runtime,
                        NodeRuntimeLifecycle::kNetworkNamespaceReady);
  }
  TransitionNodeState(events_path, options.run_id, runtime,
                      NodeRuntimeLifecycle::kCgroupReady);
}

}  // namespace bbp::simulator_app_internal
