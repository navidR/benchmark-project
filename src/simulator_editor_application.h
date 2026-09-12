#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>

#include "simulator_stop_coordination.h"

namespace bbp {

struct McpRunCleanupResult;
struct Options;
class McpLiveApplication;
class RuntimeNodeInventory;
class SimulationCommandQueue;

namespace simulator_app_internal {

enum class BenchmarkTerminalOutcome {
  kFinished,
  kCancelled,
};

struct BenchmarkHeadlessResult {
  int result = 0;
  BenchmarkTerminalOutcome terminal_outcome =
      BenchmarkTerminalOutcome::kFinished;
};

struct EditorApplicationDependencies {
  BenchmarkHeadlessResult (*run_benchmark_headless)(
      Options options, SimulationCommandQueue& command_queue,
      McpLiveApplication& mcp_application, RuntimeNodeInventory& node_inventory,
      std::stop_source& simulation_stop_source,
      std::atomic<RunStopTick>& run_stop_tick,
      std::stop_token external_stop_token);
  std::string (*exception_message)(const std::exception_ptr& error);
  void (*require_safe_output_directory)(
      const std::filesystem::path& output_dir);
  std::shared_ptr<std::timed_mutex> (*runtime_publication_mutex)();
};

int RunEditorApplication(
    Options options, const std::filesystem::path& state_directory,
#ifdef BBP_ENABLE_TEST_HOOKS
    const std::function<void()>& run_cleanup_root_removed_test_hook,
#endif
    EditorApplicationDependencies dependencies);

#ifdef BBP_ENABLE_TEST_HOOKS
McpRunCleanupResult CleanEditorRetainedRunForTest(
    const std::filesystem::path& benchmark_root, std::string_view run_id,
    std::chrono::seconds timeout, bool remove_retained_artifacts,
    std::stop_token stop_token,
    const std::function<void()>& run_cleanup_root_removed_test_hook,
    EditorApplicationDependencies dependencies);
#endif

}  // namespace simulator_app_internal
}  // namespace bbp
