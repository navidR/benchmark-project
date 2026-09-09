#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stop_token>

#include "simulator_runtime_node_restart.h"

namespace bbp {

class ChainDriver;
class PeerConnectivityController;
class RunProcessState;
class RuntimeNodeSnapshot;
class RuntimePeerTopology;
class RuntimeWalletRegistry;
struct ChainDriverSpec;
struct Options;
struct ScenarioWorkload;

namespace simulator_app_internal {

class TransactionObservationTracker;

struct OneShotWorkloadContext {
  const Options& options;
  const std::filesystem::path& events_path;
  const std::filesystem::path& metrics_path;
  const std::filesystem::path& wallet_metrics_path;
  const ChainDriverSpec& chain_spec;
  const ChainDriver& driver;
  const std::unique_ptr<PeerConnectivityController>&
      peer_connectivity_controller;
  const std::unique_ptr<RuntimePeerTopology>& runtime_topology;
  RuntimeWalletRegistry& runtime_wallet_registry;
  TransactionObservationTracker& transaction_tracker;
  RunProcessState& run_process_state;
  std::mutex& node_network_state_mutex;
  std::mutex& node_resource_state_mutex;
  std::mutex& runtime_topology_mutex;
  std::timed_mutex& block_generation_mutex;
  const std::chrono::steady_clock::time_point& lifecycle_epoch;
  RuntimeNodeStartWithPolicy start_node;
  const std::stop_token& stop_token;
};

void DispatchOneShotWorkload(
    const OneShotWorkloadContext& context,
    const ScenarioWorkload& scenario_workload, const RuntimeNodeSnapshot& nodes,
    std::uint32_t action_index, std::uint32_t action_count,
    std::stop_token operation_stop_token,
    SimulationCommandControl* cancellation_commit_control = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
