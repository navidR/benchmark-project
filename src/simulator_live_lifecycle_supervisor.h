#pragma once

#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string_view>

#include "simulator_runtime_node_restart.h"

namespace bbp {
class ChainDriver;
class McpLiveApplication;
class PeerConnectivityController;
class ProbabilisticBlockScheduler;
class RunProcessState;
class RuntimeNodeInventory;
struct ChainDriverSpec;
struct Options;

namespace simulator_app_internal {

// The thread owns this context; the caller joins it before destroying run
// state.
struct LiveLifecycleSupervisorContext {
  const Options& options;
  const std::filesystem::path& run_root;
  const std::filesystem::path& events_path;
  const ChainDriverSpec& chain_spec;
  const ChainDriver& driver;
  RuntimeNodeInventory& node_inventory;
  RunProcessState& run_process_state;
  std::timed_mutex& node_mutation_mutex;
  std::mutex& node_network_state_mutex;
  const std::chrono::steady_clock::time_point& lifecycle_epoch;
  bool& operator_connection_resolved;
  std::mutex& lifecycle_failure_mutex;
  std::exception_ptr& lifecycle_failure;
  McpLiveApplication& mcp_application;
  const std::unique_ptr<ProbabilisticBlockScheduler>& block_scheduler;
  const std::unique_ptr<PeerConnectivityController>&
      peer_connectivity_controller;
  std::function<bool(std::string_view)> is_configured_miner;
  std::function<bool()> request_simulation_stop;
  std::stop_token stop_token;
  std::unique_lock<std::timed_mutex> (*acquire_node_mutation_lock)(
      std::timed_mutex&, std::stop_token);
  RuntimeNodeStartWithPolicy start_node;
};

void RunLiveLifecycleSupervisor(std::stop_token supervisor_stop_token,
                                LiveLifecycleSupervisorContext context);

}  // namespace simulator_app_internal
}  // namespace bbp
