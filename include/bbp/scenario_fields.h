#pragma once

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/workload_kind.h"

namespace bbp {

enum class ScenarioObjectKind {
  kRoot,
  kSimulation,
  kChainDefinition,
  kNode,
  kNodeWallet,
  kWalletInitialization,
  kNodeChainConfig,
  kNodeProfile,
  kPeerConnectivity,
  kTopologyEdge,
  kTopologyRegionEdge,
  kDistribution,
  kIoLimit,
  kResourceLimits,
  kResourceProfile,
  kRuntimeResourceLimits,
  kNetworkCondition,
  kNodeNetworkCondition,
  kNetworkBlockRule,
  kNetworkFlow,
  kNetworkPartition,
  kSimulationPartitionGroup,
  kSimulationPartition,
  kPerfTarget,
  kWalletSend,
  kBlockProduction,
  kResources,
  kNetwork,
  kProcess,
  kProcessRestart,
  kProcessFreeze,
  kCount,
};

std::string_view ScenarioObjectKindName(ScenarioObjectKind kind);
std::span<const std::string_view> ScenarioObjectFields(ScenarioObjectKind kind);
bool ScenarioObjectFieldAllowed(ScenarioObjectKind kind,
                                std::string_view field);

std::span<const std::string_view> ScenarioTopologyCommonFields();
std::span<const std::string_view> ScenarioTopologyKindFields(
    PeerTopologyKind kind);
bool ScenarioTopologyFieldAllowed(PeerTopologyKind kind,
                                  std::string_view field);

std::span<const std::string_view> ScenarioWorkloadFields(WorkloadKind kind);
bool ScenarioWorkloadFieldAllowed(WorkloadKind kind, std::string_view field);

std::span<const std::string_view> ScenarioCommandFields(
    SimulationCommandKind kind);
bool ScenarioCommandFieldAllowed(SimulationCommandKind kind,
                                 std::string_view field);

// A deterministic, descriptor-derived discovery list. Entries use parser
// contexts rather than attempting to duplicate the JSON Schema tree by hand.
std::vector<std::string> BuildScenarioMemberRegistry();

}  // namespace bbp
