#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <functional>
#include <mutex>
#include <stop_token>

namespace bbp {

class McpLiveApplication;
class RuntimeNodeInventory;
class RuntimeNodeSnapshot;
class RuntimeWalletRegistry;
struct Options;
struct PeerTopologyConfig;
struct ScenarioWorkload;
struct SimulationCommandControl;

namespace simulator_app_internal {

using OneShotWorkloadDispatcher = std::function<void(
    const ScenarioWorkload&, const RuntimeNodeSnapshot&, std::uint32_t,
    std::uint32_t, std::stop_token, SimulationCommandControl*)>;
using OneShotWorkloadMutationLock =
    std::unique_lock<std::timed_mutex> (*)(std::timed_mutex&, std::stop_token);
using OneShotWorkloadInvoker = std::function<boost::json::object(
    const boost::json::object&, std::stop_token)>;

OneShotWorkloadInvoker MakeOneShotWorkloadInvoker(
    const Options& options, const RuntimeNodeInventory& node_inventory,
    const RuntimeWalletRegistry& runtime_wallet_registry,
    const PeerTopologyConfig& live_topology_config,
    std::timed_mutex& one_shot_workload_mutex,
    std::timed_mutex& node_mutation_mutex,
    std::uint64_t& next_one_shot_invocation,
    McpLiveApplication& mcp_application,
    std::function<bool()> request_simulation_stop,
    OneShotWorkloadMutationLock acquire_node_mutation_lock,
    OneShotWorkloadDispatcher dispatch_one_shot_workload,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
