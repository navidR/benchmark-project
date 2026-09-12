#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>

#include "simulator_live_wallet_workload_launcher.h"
#include "simulator_one_shot_workload_invocation.h"
#include "simulator_runtime_node_restart.h"
#include "simulator_stop_coordination.h"

namespace bbp {

class McpLiveApplication;
class PeerConnectivityController;
class RunProcessState;
class RuntimePeerTopology;
struct ChainDriverSpec;
struct PeerTopologyConfig;
struct ScenarioWorkload;

namespace simulator_app_internal {

// Retained callbacks copy this context and borrow the outer run state through
// the caller's workload-service cancellation and drain.
struct LiveWorkloadServiceBindingContext {
  const Options& options;
  const std::filesystem::path& events_path;
  const std::filesystem::path& metrics_path;
  const std::filesystem::path& wallet_metrics_path;
  const ChainDriverSpec& chain_spec;
  const ChainDriver& driver;
  const RuntimeNodeInventory& node_inventory;
  RuntimeWalletRegistry& runtime_wallet_registry;
  TransactionObservationTracker& transaction_tracker;
  RunProcessState& run_process_state;
  const std::unique_ptr<PeerConnectivityController>&
      peer_connectivity_controller;
  const std::unique_ptr<RuntimePeerTopology>& runtime_topology;
  const PeerTopologyConfig& live_topology_config;
  McpLiveApplication& mcp_application;
  std::timed_mutex& node_mutation_mutex;
  std::timed_mutex& one_shot_workload_mutex;
  std::timed_mutex& block_generation_mutex;
  std::mutex& node_network_state_mutex;
  std::mutex& node_resource_state_mutex;
  std::mutex& runtime_topology_mutex;
  const std::chrono::steady_clock::time_point& lifecycle_epoch;
  std::uint64_t& next_one_shot_invocation;
  std::atomic<RunStopTick>& run_stop_tick;
  std::shared_ptr<LiveWalletWorkloadRegistry> wallet_workloads;
  std::shared_ptr<LiveBlockGenerationWorkloadRegistry>
      block_generation_workloads;
  std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>
      wait_until_height_workloads;
  std::shared_ptr<LiveWaitForPeersWorkloadRegistry> wait_for_peers_workloads;
  std::function<bool()> request_simulation_stop;
  OneShotWorkloadMutationLock acquire_node_mutation_lock;
  RuntimeNodeStartWithPolicy start_node;
  std::stop_token stop_token;
};

struct LiveWorkloadServiceBinding {
  std::shared_ptr<McpLiveWorkloadService> service;
  LiveWalletWorkloadLauncher launch_wallet_workload;
  std::function<void(const ScenarioWorkload&, std::uint32_t, std::uint32_t,
                     std::stop_token)>
      execute_one_shot_workload;
};

LiveWorkloadServiceBinding MakeLiveWorkloadServiceBinding(
    const LiveWorkloadServiceBindingContext& context);

}  // namespace simulator_app_internal
}  // namespace bbp
