#pragma once

#include <boost/json/object.hpp>
#include <optional>
#include <string_view>

#include "bbp/scenario_node_config.h"
#include "bbp/simulation_registry.h"

namespace bbp::simulator_app_internal {

std::optional<ScenarioNodeWalletConfig> ParseScenarioNodeWalletConfig(
    const boost::json::object& node, std::string_view node_id, bool wallet_role,
    const WalletInitialization& global_initialization);
WalletInitialization ParseWalletInitializationObject(
    const boost::json::object& topology);

}  // namespace bbp::simulator_app_internal
