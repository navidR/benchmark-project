#pragma once

#include <boost/json/object.hpp>
#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <vector>

#include "simulator_runtime_node_restart.h"

namespace bbp {

enum class McpOperationKind;

class ChainDriver;
class McpLiveApplication;
class PeerConnectivityController;
class ProbabilisticBlockScheduler;
class RunProcessState;
class RuntimeNodeInventory;
class RuntimePeerTopology;
class RuntimeWalletRegistry;
struct ChainDriverSpec;
struct Options;
struct PeerTopologyConfig;

namespace simulator_app_internal {

struct RuntimeNodeAdditionDependencies;

struct LiveMasternodeOperationContext {
  const Options& options;
  const std::filesystem::path& run_root;
  const std::filesystem::path& events_path;
  const ChainDriverSpec& chain_spec;
  const ChainDriver& driver;
  McpLiveApplication& mcp_application;
  RuntimeNodeInventory& node_inventory;
  RuntimeWalletRegistry& runtime_wallet_registry;
  const std::unique_ptr<ProbabilisticBlockScheduler>& block_scheduler;
  std::mutex& configured_miner_node_ids_mutex;
  std::vector<std::string>& miner_node_ids;
  std::timed_mutex& node_mutation_mutex;
  std::timed_mutex& block_generation_mutex;
  std::mutex& runtime_topology_mutex;
  const std::unique_ptr<PeerConnectivityController>&
      peer_connectivity_controller;
  std::unique_ptr<RuntimePeerTopology>& runtime_topology;
  PeerTopologyConfig& live_topology_config;
  RunProcessState& run_process_state;
  const std::chrono::steady_clock::time_point& lifecycle_epoch;
  const RuntimeNodeAdditionDependencies& runtime_node_addition_dependencies;
  RuntimeNodeStartWithPolicy start_node;
  std::function<bool()> request_simulation_stop;
  std::unique_lock<std::timed_mutex> (*acquire_node_mutation_lock)(
      std::timed_mutex&, std::stop_token);
  std::unique_lock<std::timed_mutex> (*acquire_runtime_publication_lock)(
      std::stop_token);
  std::string (*exception_message)(const std::exception_ptr&);
};

boost::json::object ExecuteLiveMasternodeOperation(
    const LiveMasternodeOperationContext& context, McpOperationKind kind,
    const boost::json::object& arguments, std::stop_token operation_stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
