#pragma once

#include <boost/json/object.hpp>
#include <boost/program_options/variables_map.hpp>
#include <cstdint>
#include <vector>

namespace bbp {

struct Options;

namespace simulator_app_internal {

struct ScenarioNodeRoles {
  bool configured = false;
  std::vector<uint32_t> wallet_nodes;
  std::vector<uint32_t> miner_nodes;
};

ScenarioNodeRoles ParseScenarioNodes(
    const boost::json::object& scenario,
    const boost::program_options::variables_map& vm, Options* options);

}  // namespace simulator_app_internal
}  // namespace bbp
