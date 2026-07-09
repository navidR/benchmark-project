#pragma once

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

#include "benchmark_sim/result.h"

namespace bsim {

std::filesystem::path RunLogPath(const std::filesystem::path& run_root);
Result<std::vector<std::string>> ReadRecentLogLines(
    const std::filesystem::path& log_path, std::size_t max_lines);

}  // namespace bsim
