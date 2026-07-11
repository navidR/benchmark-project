#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>

#include "bbp/simulation_command.h"

namespace bbp {

class SimulationCommandQueue {
 public:
  std::uint64_t Push(SimulationCommandKind kind, std::string node_id);
  std::uint64_t PushBlockProductionPolicy(BlockProductionPolicy policy);
  std::uint64_t PushMiningDifficulty(std::string node_id,
                                     MiningDifficulty difficulty);
  std::uint64_t PushPeerCommand(SimulationCommandKind kind, std::string node_id,
                                std::string peer_node_id);
  std::optional<SimulationCommand> TryPop();
  std::optional<SimulationCommand> WaitPop();
  void Close();
  void Cancel();
  [[nodiscard]] bool IsClosed() const;

 private:
  std::uint64_t PushCommand(SimulationCommand command);

  mutable std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<SimulationCommand> commands_;
  std::uint64_t next_sequence_ = 1;
  bool closed_ = false;
};

}  // namespace bbp
