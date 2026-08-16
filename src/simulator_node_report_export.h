#pragma once

#include <filesystem>

namespace bbp {

struct SimulationCommand;

namespace simulator_app_internal {

void ExportNodeReport(const std::filesystem::path& run_root,
                      const SimulationCommand& command);

}  // namespace simulator_app_internal
}  // namespace bbp
