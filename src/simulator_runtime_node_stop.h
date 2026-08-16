#pragma once

#include <filesystem>
#include <stop_token>

namespace bbp {

class ChainDriver;
struct NodeRuntime;
struct Options;
struct SimulationCommandControl;

namespace simulator_app_internal {

void StopNodeProcess(const Options& options,
                     const std::filesystem::path& events_path,
                     const ChainDriver& driver, NodeRuntime& node,
                     std::stop_token stop_token,
                     bool allow_rpc_unavailable = false,
                     SimulationCommandControl* operation_control = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
