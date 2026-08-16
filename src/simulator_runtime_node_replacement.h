#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string_view>

#include "bbp/runtime_node_resource_manifest.h"

namespace bbp {

class Cgroup;
class ChainDriver;
class PeerConnectivityController;
class RunProcessState;
class RuntimeNodeInventory;
class RuntimeNodeSnapshot;
class RuntimePeerTopology;
class RuntimeWalletRegistry;
struct NodeRuntime;
struct Options;
struct PeerTopologyConfig;
struct SimulationCommandControl;
struct SimulationNodeReplaceRequest;

namespace simulator_app_internal {

class TransactionObservationTracker;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWalletWorkloadRegistry;

struct RuntimeNodeReplaceResult {
  std::uint64_t inventory_generation = 0U;
  std::uint32_t final_node_count = 0U;
};

struct RuntimeNodeReplacementDependencies {
  std::mutex& network_state_mutex;
  std::mutex& resource_state_mutex;
  std::function<RuntimeNodeResourceManifest(const Options&,
                                            const RuntimeNodeSnapshot&)>
      resource_manifest;
  std::function<bool(const Cgroup&, bool, std::stop_token)>
      wait_for_frozen_state;
  std::function<void(const Options&, const std::filesystem::path&,
                     const ChainDriver&, NodeRuntime&, std::stop_token, bool)>
      stop_node;
  std::function<bool(const Options&, const std::filesystem::path&,
                     const ChainDriver&, NodeRuntime&, std::string_view,
                     std::chrono::steady_clock::time_point, bool, bool,
                     std::stop_token)>
      start_node;
  std::function<std::unique_lock<std::timed_mutex>(std::stop_token)>
      acquire_publication_lock;
};

RuntimeNodeReplaceResult ReplaceRuntimeNodeTransactional(
    const Options& options, const std::filesystem::path& run_root,
    const std::filesystem::path& events_path, const ChainDriver& driver,
    RuntimeNodeInventory& inventory, RuntimeWalletRegistry& runtime_registry,
    PeerConnectivityController& peer_controller,
    const RuntimePeerTopology& runtime_topology,
    const PeerTopologyConfig& live_topology_config,
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    const TransactionObservationTracker& transaction_tracker,
    RunProcessState& run_process_state,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::string_view node_id, const SimulationNodeReplaceRequest& request,
    bool restore_native_mining, std::string_view native_mining_reward_address,
    SimulationCommandControl* operation_control, std::stop_token stop_token,
    const RuntimeNodeReplacementDependencies& dependencies);

}  // namespace simulator_app_internal
}  // namespace bbp
