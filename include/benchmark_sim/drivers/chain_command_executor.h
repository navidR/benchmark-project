#pragma once

#include <functional>
#include <vector>

#include "benchmark_sim/drivers/chain_driver.h"
#include "benchmark_sim/simulation_command.h"

namespace bsim {

class ChainCommandExecutor {
 public:
  using StopMiningHandler = std::function<void(const ChainNodeConfig& config,
                                               std::stop_token stop_token)>;
  using BlockProductionPolicyHandler =
      std::function<void(BlockProductionPolicy policy)>;
  using MiningDifficultyHandler = std::function<void(
      const ChainNodeConfig& config, MiningDifficulty difficulty,
      std::stop_token stop_token)>;

  ChainCommandExecutor(const ChainDriver& driver,
                       std::vector<ChainNodeConfig> nodes,
                       StopMiningHandler stop_mining_handler,
                       BlockProductionPolicyHandler policy_handler,
                       MiningDifficultyHandler difficulty_handler);

  void Execute(const SimulationCommand& command,
               std::stop_token stop_token = {}) const;

 private:
  const ChainNodeConfig& FindNode(const std::string& node_id) const;

  const ChainDriver& driver_;
  std::vector<ChainNodeConfig> nodes_;
  StopMiningHandler stop_mining_handler_;
  BlockProductionPolicyHandler policy_handler_;
  MiningDifficultyHandler difficulty_handler_;
};

}  // namespace bsim
