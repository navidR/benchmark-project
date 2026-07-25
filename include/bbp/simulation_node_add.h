#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/default_peer_topology.h"
#include "bbp/network.h"
#include "bbp/simulator/resource_limit_patch.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

inline constexpr std::uint32_t kSimulationNodeAddMaximumCount = 16U;
inline constexpr std::uint32_t kSimulationNodeAddMaximumTimeoutSeconds = 600U;
inline constexpr std::uint32_t kSimulationNodeRemoveMaximumCount = 16U;
inline constexpr auto kSimulationNodeReplaceSwitchoverAllowance =
    std::chrono::seconds(30);
inline constexpr auto kSimulationNodeReplaceRollbackTimeout =
    std::chrono::seconds(60);
inline constexpr auto kSimulationNodeReplaceCancellationReconciliation =
    std::chrono::seconds(65);

struct SimulationNodeResourceFailure {
  std::string resource_kind;
  std::string node_id;
  std::string address;
  std::uint16_t port = 0U;
  std::string purpose;
  bool mutation_started = false;
};

class SimulationNodeResourceUnavailable final : public std::runtime_error {
 public:
  SimulationNodeResourceUnavailable(std::string message,
                                    SimulationNodeResourceFailure failure)
      : std::runtime_error(std::move(message)), failure_(std::move(failure)) {}

  const SimulationNodeResourceFailure& failure() const noexcept {
    return failure_;
  }

 private:
  SimulationNodeResourceFailure failure_;
};

struct SimulationNodeAddRequest {
  ChainKind chain = ChainKind::kFiro;
  std::uint32_t count = 0U;
  std::vector<std::string> node_ids;
  std::optional<std::string> binary;
  std::optional<PeerTopologyConfig> topology;
  std::optional<ResourceLimits> resources;
  std::optional<NetworkCondition> network;
  std::uint32_t ready_timeout_sec = 30U;
  std::uint32_t sync_timeout_sec = 30U;
};

struct SimulationNodeReplaceRequest {
  ChainKind chain = ChainKind::kFiro;
  std::uint32_t count = 0U;
  std::vector<std::string> node_ids;
  std::optional<std::string> binary;
  std::optional<ResourceLimitPatch> resources;
  std::optional<NetworkCondition> network;
  std::uint32_t ready_timeout_sec = 30U;
  std::uint32_t sync_timeout_sec = 30U;
};

constexpr std::chrono::seconds SimulationNodeReplaceDefaultExecutionTimeout(
    const SimulationNodeReplaceRequest& request) {
  return std::chrono::seconds(
             static_cast<std::chrono::seconds::rep>(request.ready_timeout_sec) *
             3) +
         std::chrono::seconds(
             static_cast<std::chrono::seconds::rep>(request.sync_timeout_sec) *
             2) +
         kSimulationNodeReplaceSwitchoverAllowance;
}

struct SimulationNodeRemoveRequest {
  std::vector<std::string> node_ids;
  std::uint32_t timeout_sec = 30U;
};

}  // namespace bbp
