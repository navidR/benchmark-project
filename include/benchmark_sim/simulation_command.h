#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace bsim {

enum class SimulationCommandKind {
  kIncreaseLogVerbosity,
  kDecreaseLogVerbosity,
  kStopMining,
  kDisconnectNode,
  kKillNode,
};

struct SimulationCommand {
  std::uint64_t sequence = 0;
  SimulationCommandKind kind = SimulationCommandKind::kIncreaseLogVerbosity;
  std::string node_id;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);

}  // namespace bsim
