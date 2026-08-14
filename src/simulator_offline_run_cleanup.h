#pragma once

#include <chrono>
#include <optional>
#include <stop_token>

#include "bbp/mcp_host_application.h"
#include "bbp/runtime_node_resource_manifest.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

McpRunCleanupResult CleanupRun(
    Options options,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {}, bool remove_retained_artifacts = false,
    const RunOwnership* expected_ownership = nullptr,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    bool* external_cleanup_complete = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
