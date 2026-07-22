#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "bbp/simulation_command.h"

namespace bbp {

constexpr std::size_t kDefaultSimulationCommandQueueCapacity = 256U;

struct SimulationCommandQueueStats {
  std::size_t size = 0U;
  std::size_t capacity = 0U;
  std::size_t maximum_size = 0U;
  std::uint64_t rejected = 0U;
};

class SimulationCommandQueue {
 public:
  explicit SimulationCommandQueue(
      std::size_t capacity = kDefaultSimulationCommandQueueCapacity);

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
                                   std::uint32_t block_count,
                                   bool confirmed = false);
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
  std::uint64_t PushRuntimeCommand(SimulationCommand command);
  std::uint64_t PushScenarioCommand(SimulationCommand command);
  std::optional<SimulationCommand> TryPop();
  std::optional<SimulationCommand> WaitPop();
  void Close();
  std::vector<SimulationCommand> Cancel();
  [[nodiscard]] bool IsClosed() const;
  [[nodiscard]] SimulationCommandQueueStats Stats() const;

 private:
  std::uint64_t PushCommand(SimulationCommand command);

  mutable std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<SimulationCommand> commands_;
  const std::size_t capacity_;
  std::size_t maximum_size_ = 0U;
  std::uint64_t rejected_ = 0U;
  std::uint64_t next_sequence_ = 1;
  bool closed_ = false;
};

}  // namespace bbp
