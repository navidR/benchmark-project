#pragma once

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

namespace bsim {

std::filesystem::path RunLogPath(const std::filesystem::path& run_root);
std::vector<std::string> ReadRecentLogLines(
    const std::filesystem::path& log_path, std::size_t max_lines);

}  // namespace bsim
