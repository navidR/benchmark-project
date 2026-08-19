#pragma once

#include <cstdint>
#include <filesystem>

namespace bbp::simulator_app_internal {

int RunRetainedTuiWithMcp(const std::filesystem::path& run_root,
                          const std::filesystem::path& state_directory,
                          bool once, std::uint32_t refresh_ms);

}  // namespace bbp::simulator_app_internal
