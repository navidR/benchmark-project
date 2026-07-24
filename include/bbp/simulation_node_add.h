#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/default_peer_topology.h"
#include "bbp/network.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

inline constexpr std::uint32_t kSimulationNodeAddMaximumCount = 16U;
inline constexpr std::uint32_t kSimulationNodeAddMaximumTimeoutSeconds = 600U;

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

}  // namespace bbp
