#pragma once

#include <filesystem>
#include <stop_token>

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
class SimulationRegistry;
struct Options;

namespace simulator_app_internal {

void InitializeWalletNodes(const Options& options,
                           const std::filesystem::path& events_path,
                           const ChainDriver& driver,
                           const RuntimeNodeSnapshot& nodes,
                           SimulationRegistry& registry,
                           std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
