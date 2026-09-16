#pragma once

#include <boost/json/object.hpp>
#include <filesystem>
#include <string>

#include "bbp/run_process_state.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/node_runtime.h"

namespace bbp::simulator_app_internal {

// Caller holds node mutation admission and the process-state guard.
boost::json::object DeliverNodeSignal(
    NodeRuntime& node, const SimulationCommand& command,
    const RunProcessState::Guard& guard,
    boost::json::object wallet_selection = {});
void PublishNodeSignalObservation(NodeRuntime& node,
                                  const RunProcessState::Guard& guard,
                                  const std::filesystem::path& events_path,
                                  const std::string& run_id);

}  // namespace bbp::simulator_app_internal
