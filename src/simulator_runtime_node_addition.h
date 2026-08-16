#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

class ChainDriver;
class PeerConnectivityController;
class ProbabilisticBlockScheduler;
class RunProcessState;
class RuntimeNodeInventory;
class RuntimeNodeSnapshot;
class RuntimePeerTopology;
class RuntimeWalletRegistry;
class SimulationNetworkAddressPlan;
struct ChainDriverSpec;
struct NodeRoleTopology;
struct Options;
struct PeerTopologyConfig;
struct SimulationCommandControl;
struct SimulationNodeAddRequest;

namespace simulator_app_internal {

struct RuntimeNodeAddResult {
  std::vector<std::string> added_node_ids;
  std::uint64_t inventory_generation = 0U;
  std::uint32_t final_node_count = 0U;
  std::vector<WalletIdentity> added_wallets;
  std::optional<std::uint64_t> wallet_generation;
  std::optional<std::size_t> final_wallet_count;
  std::optional<std::size_t> final_wallet_node_count;
  std::optional<std::uint64_t> role_generation;
  std::optional<std::size_t> final_miner_count;
  std::vector<MasternodeIdentity> added_masternodes;
  std::optional<std::size_t> final_masternode_count;
};

enum class RuntimeNodeAdditionRole {
  kBase,
  kWallet,
  kMiner,
  kMasternode,
};

struct RuntimeMasternodeAddContext {
  std::string funding_wallet_node_id;
  std::timed_mutex* block_generation_mutex = nullptr;
};

struct RuntimeNodeAdditionDependencies {
  std::mutex& network_state_mutex;
  std::function<RuntimeNodeResourceEntry(const Options&, const ChainNodeConfig&,
                                         std::uint32_t,
                                         RuntimeNodeResourceState)>
      resource_entry;
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
  std::function<bool(const RuntimePeerTopology&, std::uint32_t, std::uint32_t)>
      physical_peer_required;
  std::function<std::vector<std::string>(const RuntimePeerTopology&,
                                         const std::vector<ChainNodeConfig>&,
                                         std::uint32_t)>
      physical_peer_endpoints;
  std::function<void(
      const Options&, const std::filesystem::path&, NodeRuntime&,
      ChainNodeConfig, std::uint32_t, ResourceLimits, std::string, std::string,
      std::vector<DirectionalNetworkPolicy>, std::optional<NetworkCondition>,
      RunProcessState&, std::stop_token, bool*)>
      prepare_node;
  std::function<bool(const Options&, const std::filesystem::path&,
                     const ChainDriver&, NodeRuntime&, std::string_view,
                     std::chrono::steady_clock::time_point, std::stop_token)>
      start_node;
  std::function<bool(const Options&, const std::filesystem::path&,
                     const ChainDriver&, PeerConnectivityController&,
                     NodeRuntime&, std::chrono::steady_clock::time_point,
                     std::stop_token, std::string_view)>
      restart_masternode;
  std::function<std::vector<bool>(
      const Options&, const std::filesystem::path&, const ChainDriver&,
      std::vector<NodeRuntime>&, const std::vector<std::uint32_t>&,
      std::chrono::steady_clock::time_point, std::stop_token)>
      stop_candidates;
  std::function<std::unique_lock<std::timed_mutex>(std::stop_token)>
      acquire_publication_lock;
};

RuntimeNodeAddResult AddRuntimeNodesTransactional(
    const Options& options, const std::filesystem::path& run_root,
    const std::filesystem::path& events_path, const ChainDriverSpec& chain_spec,
    const ChainDriver& driver, RuntimeNodeInventory& inventory,
    RuntimeWalletRegistry& runtime_registry, RuntimeNodeAdditionRole added_role,
    ProbabilisticBlockScheduler* block_scheduler,
    std::vector<std::string>* configured_miner_node_ids,
    std::mutex* configured_miner_node_ids_mutex,
    const RuntimeMasternodeAddContext* masternode_context,
    PeerConnectivityController& peer_controller,
    std::unique_ptr<RuntimePeerTopology>* runtime_topology,
    PeerTopologyConfig* live_topology_config,
    RunProcessState& run_process_state,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    const SimulationNodeAddRequest& request,
    SimulationCommandControl* operation_control, std::stop_token stop_token,
    const RuntimeNodeAdditionDependencies& dependencies);

}  // namespace simulator_app_internal
}  // namespace bbp
