#pragma once

#include <functional>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/simulation_command.h"

namespace bbp {

class ChainCommandExecutor {
 public:
  using StopMiningHandler = std::function<void(const ChainNodeConfig& config,
                                               std::stop_token stop_token)>;
  using BlockProductionPolicyHandler =
      std::function<void(BlockProductionPolicy policy)>;
  using MiningDifficultyHandler = std::function<void(
      const ChainNodeConfig& config, MiningDifficulty difficulty,
      std::stop_token stop_token)>;
  using PeerHandler = std::function<void(const ChainNodeConfig& config,
                                         const ChainNodeConfig& peer,
                                         std::stop_token stop_token)>;
  using PeerCountPolicyHandler = std::function<void(
      const ChainNodeConfig& config, PeerCountPolicy policy)>;

  ChainCommandExecutor(const ChainDriver& driver,
                       std::vector<ChainNodeConfig> nodes,
                       StopMiningHandler stop_mining_handler,
                       BlockProductionPolicyHandler policy_handler,
                       MiningDifficultyHandler difficulty_handler,
                       PeerHandler connect_peer_handler,
                       PeerHandler disconnect_peer_handler,
                       PeerCountPolicyHandler peer_count_policy_handler);

  void Execute(const SimulationCommand& command,
               std::stop_token stop_token = {}) const;

 private:
  const ChainNodeConfig& FindNode(const std::string& node_id) const;

  const ChainDriver& driver_;
  std::vector<ChainNodeConfig> nodes_;
  StopMiningHandler stop_mining_handler_;
  BlockProductionPolicyHandler policy_handler_;
  MiningDifficultyHandler difficulty_handler_;
  PeerHandler connect_peer_handler_;
  PeerHandler disconnect_peer_handler_;
  PeerCountPolicyHandler peer_count_policy_handler_;
};

}  // namespace bbp
