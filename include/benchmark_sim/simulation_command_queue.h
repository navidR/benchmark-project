#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>

#include "benchmark_sim/simulation_command.h"

namespace bsim {

class SimulationCommandQueue {
 public:
  std::uint64_t Push(SimulationCommandKind kind, std::string node_id);
  std::optional<SimulationCommand> TryPop();
  std::optional<SimulationCommand> WaitPop();
  void Close();
  [[nodiscard]] bool IsClosed() const;

 private:
  mutable std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<SimulationCommand> commands_;
  std::uint64_t next_sequence_ = 1;
  bool closed_ = false;
};

}  // namespace bsim
