#pragma once

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "bbp/network.h"
#include "bbp/perf_counter.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/scenario_node_config.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulation_time_scale.h"
#include "bbp/simulator/block_generation_workload.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/network_partition_rule.h"
#include "bbp/simulator/resource_limit_patch.h"
#include "bbp/simulator/resource_limits.h"
#include "bbp/simulator/scenario_workload.h"
#include "bbp/simulator/scheduled_scenario_event.h"
#include "bbp/simulator/wait_for_peers_workload.h"
#include "bbp/simulator/wait_until_height_workload.h"
#include "bbp/simulator/wallet_transactions_workload.h"

namespace bbp::simulator_app_internal {

boost::json::object NetworkConditionJson(const NetworkCondition& condition);
boost::json::array DirectionalNetworkPoliciesJson(
    const std::vector<DirectionalNetworkPolicy>& policies);
boost::json::object NetworkBlockRuleJson(const NetworkBlockRule& rule);
boost::json::object NetworkPartitionRuleJson(const NetworkPartitionRule& rule);
boost::json::object ScenarioNodeWalletConfigJson(
    const ScenarioNodeWalletConfig& config);
boost::json::object RuntimePeerTopologyEdgeJson(
    const RuntimePeerTopologyEdge& edge);
boost::json::array RuntimePeerTopologyEdgesJson(
    const RuntimePeerTopology& topology);
void AddPeerTopologyJson(const PeerTopologyConfig& topology,
                         uint32_t node_count, boost::json::object* object);
boost::json::object NodeRoleTopologyJson(
    const NodeRoleTopology& topology,
    const WalletInitialization& wallet_initialization);
boost::json::array IoLimitsJson(const std::vector<IoLimit>& io_limits);
boost::json::object ResourceLimitsJson(const ResourceLimits& limits);
boost::json::object ResourceLimitPatchJson(const ResourceLimitPatch& patch);
boost::json::array PerfCounterNamesJson(
    const std::vector<PerfCounterKind>& kinds);

std::string YamlFromJson(const boost::json::value& value);
boost::json::object BlockGenerationWorkloadJson(
    const BlockGenerationWorkload& workload);
boost::json::object WaitUntilHeightWorkloadJson(
    const WaitUntilHeightWorkload& workload);
boost::json::object WaitForPeersWorkloadJson(
    const WaitForPeersWorkload& workload);
boost::json::object WalletTransactionsWorkloadJson(
    const WalletTransactionsWorkload& workload);
boost::json::object WorkloadJson(const ScenarioWorkload& workload);
boost::json::object ScheduledScenarioEventJson(
    const ScheduledScenarioEvent& event, const SimulationTimeScale& time_scale);

}  // namespace bbp::simulator_app_internal
