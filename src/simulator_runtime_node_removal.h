#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime.h"

namespace bbp {

class ChainDriver;
class PeerConnectivityController;
class RuntimeNodeInventory;
class RuntimeNodeSnapshot;
class RuntimePeerTopology;
class RuntimeWalletRegistry;
class SimulationNetworkAddressPlan;
struct Options;
struct PeerTopologyConfig;
struct SimulationCommandControl;
struct SimulationNodeRemoveRequest;

namespace simulator_app_internal {

class TransactionObservationTracker;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWalletWorkloadRegistry;

struct RuntimeNodeRemoveResult {
  std::vector<std::string> removed_node_ids;
  std::uint64_t inventory_generation = 0U;
  std::uint32_t final_node_count = 0U;
};

struct RuntimeNodeRemovalDependencies {
  std::mutex& network_state_mutex;
  std::function<RuntimeNodeResourceManifest(const Options&,
                                            const RuntimeNodeSnapshot&)>
      resource_manifest;
  std::function<std::vector<DirectionalNetworkPolicy>(
      const RuntimePeerTopology&, const SimulationNetworkAddressPlan&,
      const std::vector<std::uint32_t>&, std::uint32_t)>
      directional_network_policies;
  std::function<std::vector<std::string>(const RuntimePeerTopology&,
                                         const std::vector<ChainNodeConfig>&,
                                         std::uint32_t)>
      topology_peer_ids;
  std::function<std::vector<std::string>(
      const NodeRoleTopology&, const RuntimePeerTopology&,
      const std::vector<ChainNodeConfig>&, std::uint32_t)>
      restart_peer_endpoints;
  std::function<std::vector<std::string>(const RuntimePeerTopology&,
                                         const std::vector<ChainNodeConfig>&,
                                         std::uint32_t)>
      physical_peer_endpoints;
  std::function<std::vector<bool>(const Options&, const std::filesystem::path&,
                                  const ChainDriver&, std::vector<NodeRuntime>&,
                                  const std::vector<std::uint32_t>&,
                                  std::chrono::steady_clock::time_point)>
      stop_retired_nodes;
  std::function<std::unique_lock<std::timed_mutex>(std::stop_token)>
      acquire_publication_lock;
};

RuntimeNodeRemoveResult RemoveRuntimeNodesTransactional(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, RuntimeNodeInventory& inventory,
    RuntimeWalletRegistry& runtime_registry,
    PeerConnectivityController& peer_controller,
    std::unique_ptr<RuntimePeerTopology>* runtime_topology,
    PeerTopologyConfig* live_topology_config,
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    const TransactionObservationTracker& transaction_tracker,
    const SimulationNodeRemoveRequest& request,
    SimulationCommandControl* operation_control, std::stop_token stop_token,
    const RuntimeNodeRemovalDependencies& dependencies);

}  // namespace simulator_app_internal
}  // namespace bbp
