#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

namespace bbp {

std::string BuildRunReportJson(const std::filesystem::path& run_root);
std::string BuildNodeReportJson(const std::filesystem::path& run_root,
                                std::string_view node_id,
                                std::uint64_t operator_command_sequence);

}  // namespace bbp
