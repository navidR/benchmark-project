#pragma once

#include <cstdint>
#include <filesystem>
#include <stop_token>
#include <string_view>
#include <vector>

#include "bbp/mcp_host_application.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

void WriteRetainedRunRegistrySummary(const Options& options,
                                     std::string_view state,
                                     std::uint32_t node_count);

std::vector<McpRetainedRunSnapshot> DiscoverRetainedRuns(
    const std::filesystem::path& benchmark_root, std::string_view active_run_id,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
