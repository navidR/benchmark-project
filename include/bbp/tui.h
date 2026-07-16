#pragma once

#include <cstdint>
#include <filesystem>
#include <stop_token>

namespace bbp {

class SimulationCommandQueue;

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms,
                 SimulationCommandQueue* command_queue = nullptr,
                 std::stop_token stop_token = {});

}  // namespace bbp
