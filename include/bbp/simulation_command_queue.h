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
  std::uint64_t Push(SimulationCommandKind kind, std::string node_id,
                     bool confirmed = false);
  std::uint64_t PushBlockProductionPolicy(BlockProductionPolicy policy);
  std::uint64_t PushMiningDifficulty(std::string node_id,
                                     MiningDifficulty difficulty);
  std::uint64_t PushPeerCommand(SimulationCommandKind kind, std::string node_id,
                                std::string peer_node_id,
                                bool confirmed = false);
  std::uint64_t PushPeerCountPolicy(std::string node_id, PeerCountPolicy policy,
                                    bool confirmed = false);
  std::uint64_t PushGenerateBlocks(std::string node_id,
                                   std::uint32_t block_count);
  std::uint64_t PushProfileCommand(SimulationCommandKind kind,
                                   std::string node_id, std::string profile,
                                   bool confirmed = false);
  std::uint64_t PushResourceLimits(std::string node_id,
                                   ResourceLimitPatch patch,
                                   bool confirmed = false);
  std::uint64_t PushNetworkCondition(std::string node_id,
                                     NetworkCondition condition,
                                     bool confirmed = false);
  std::uint64_t PushNetworkFlowCommand(SimulationCommandKind kind,
                                       std::string node_id,
                                       SimulationNetworkFlow flow,
                                       bool confirmed = false);
  std::uint64_t PushPartitionCommand(SimulationCommandKind kind,
                                     SimulationPartition partition,
                                     bool confirmed = false);
  std::uint64_t PushPerfCounters(PerfCounterTarget target,
                                 std::vector<PerfCounterKind> kinds);
  std::uint64_t PushWalletSend(std::string sender_node_id,
                               SimulationWalletSend send,
                               bool confirmed = false);
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
