#include "benchmark_sim/simulation_command.h"

#include <stdexcept>

namespace bsim {

std::string_view SimulationCommandKindName(SimulationCommandKind kind) {
  switch (kind) {
    case SimulationCommandKind::kIncreaseLogVerbosity:
      return "increase_log_verbosity";
    case SimulationCommandKind::kDecreaseLogVerbosity:
      return "decrease_log_verbosity";
    case SimulationCommandKind::kStopMining:
      return "stop_mining";
    case SimulationCommandKind::kDisconnectNode:
      return "disconnect_node";
    case SimulationCommandKind::kKillNode:
      return "kill_node";
  }
  throw std::runtime_error("unknown simulation command kind");
}

}  // namespace bsim
