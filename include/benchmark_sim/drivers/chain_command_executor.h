#pragma once

#include <vector>

#include "benchmark_sim/drivers/chain_driver.h"
#include "benchmark_sim/simulation_command.h"

namespace bsim {

class ChainCommandExecutor {
 public:
  ChainCommandExecutor(const ChainDriver& driver,
                       std::vector<ChainNodeConfig> nodes);

  void Execute(const SimulationCommand& command) const;

 private:
  const ChainNodeConfig& FindNode(const std::string& node_id) const;

  const ChainDriver& driver_;
  std::vector<ChainNodeConfig> nodes_;
};

}  // namespace bsim
