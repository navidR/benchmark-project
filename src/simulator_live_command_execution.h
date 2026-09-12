#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "simulator_runtime_node_restart.h"

namespace bbp {

class ChainCommandExecutor;
class ChainDriver;
class McpLiveApplication;
class PeerConnectivityController;
class ProbabilisticBlockScheduler;
class RunProcessState;
class RuntimeNodeInventory;
class RuntimePeerTopology;
class RuntimeWalletRegistry;
class SimulationCommandProcessor;
class SimulationCommandQueue;
struct ChainDriverSpec;
struct McpLiveRoleService;
struct Options;
struct PeerTopologyConfig;

namespace simulator_app_internal {

class TransactionObservationTracker;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveInstrumentationRegistry;
struct LiveWaitForPeersWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWalletWorkloadRegistry;
struct RuntimeNodeAdditionDependencies;
struct RuntimeNodeRemovalDependencies;
struct RuntimeNodeReplacementDependencies;

// Run-owned references remain valid until the caller drains the processor.
// Retained closures copy this context, including its callback values.
struct LiveCommandExecutionContext {
  const Options& options;
  const std::filesystem::path& run_root;
  const std::filesystem::path& events_path;
  const ChainDriverSpec& chain_spec;
  const ChainDriver& driver;
  McpLiveApplication& mcp_application;
  RuntimeNodeInventory& node_inventory;
  RuntimeWalletRegistry& runtime_wallet_registry;
  const std::unique_ptr<ProbabilisticBlockScheduler>& block_scheduler;
  const std::unique_ptr<PeerConnectivityController>&
      peer_connectivity_controller;
  const std::unique_ptr<ChainCommandExecutor>& chain_command_executor;
  std::mutex& configured_miner_node_ids_mutex;
  std::vector<std::string>& miner_node_ids;
  std::timed_mutex& node_mutation_mutex;
  std::timed_mutex& block_generation_mutex;
  std::mutex& runtime_topology_mutex;
  std::mutex& node_network_state_mutex;
  std::mutex& node_resource_state_mutex;
  std::unique_ptr<RuntimePeerTopology>& runtime_topology;
  PeerTopologyConfig& live_topology_config;
  RunProcessState& run_process_state;
  const std::chrono::steady_clock::time_point& lifecycle_epoch;
  const std::stop_source& command_rpc_stop_source;
  const std::atomic<bool>& wallets_initialized;
  TransactionObservationTracker& transaction_tracker;
  const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads;
  const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
      block_generation_workloads;
  const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
      wait_until_height_workloads;
  const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
      wait_for_peers_workloads;
  const std::shared_ptr<LiveInstrumentationRegistry>& live_instrumentation;
  const RuntimeNodeAdditionDependencies& runtime_node_addition_dependencies;
  const RuntimeNodeRemovalDependencies& runtime_node_removal_dependencies;
  const RuntimeNodeReplacementDependencies&
      runtime_node_replacement_dependencies;
  const std::atomic<std::shared_ptr<McpLiveRoleService>>& command_role_service;
  std::function<bool(std::string_view)> is_configured_miner;
  std::function<bool()> request_simulation_stop;
  std::function<void(const SimulationCommand&, std::optional<std::string_view>)>
      record_scheduled_command_outcome;
  std::unique_lock<std::timed_mutex> (*acquire_node_mutation_lock)(
      std::timed_mutex&, std::stop_token);
  std::unique_lock<std::timed_mutex> (*acquire_runtime_publication_lock)(
      std::stop_token);
  NodeRuntime& (*find_node_runtime_by_id)(const RuntimeNodeSnapshot&,
                                          const std::string&);
  std::string (*exception_message)(const std::exception_ptr&);
  RuntimeNodeStartWithPolicy start_node;
};

std::unique_ptr<ChainCommandExecutor> MakeLiveChainCommandExecutor(
    const LiveCommandExecutionContext& context);
std::unique_ptr<SimulationCommandProcessor> MakeLiveSimulationCommandProcessor(
    SimulationCommandQueue& command_queue,
    const LiveCommandExecutionContext& context);

}  // namespace simulator_app_internal
}  // namespace bbp
