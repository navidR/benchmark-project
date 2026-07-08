#pragma once

#include <cstdint>
#include <filesystem>

namespace bsim {

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms);

}  // namespace bsim
