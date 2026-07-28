#pragma once

#ifdef BBP_ENABLE_TEST_HOOKS
#include <chrono>
#include <filesystem>
#include <functional>
#include <stop_token>
#include <string_view>

#include "bbp/mcp_host_application.h"
#endif

namespace bbp {

class SimulatorApp {
 public:
  int Run(int argc, char** argv);
};

#ifdef BBP_ENABLE_TEST_HOOKS
bool RuntimeNodeSupportDestructionAllowedForTest(
    bool daemon_absence_verified, bool exact_cgroup_acquired,
    bool exact_cgroup_empty, bool allow_partial_preparation = false);
void SetRunCleanupRootRemovedHookForTest(std::function<void()> hook);
McpRunCleanupResult CleanEditorRetainedRunForTest(
    const std::filesystem::path& benchmark_root, std::string_view run_id,
    std::chrono::seconds timeout, bool remove_retained_artifacts,
    std::stop_token stop_token = {});
#endif

}  // namespace bbp
