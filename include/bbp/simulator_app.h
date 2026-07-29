#pragma once

#ifdef BBP_ENABLE_TEST_HOOKS
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_host_application.h"
#include "bbp/mcp_live_application.h"
#include "bbp/perf_counter.h"
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

struct LiveInstrumentationNodeStateForTest {
  std::vector<PerfCounterKind> counters;
  PerfCounterTargetKind target_kind = PerfCounterTargetKind::kNode;
  std::string target_id;
  int target_pid = -1;
  int attached_pid = -1;
  std::uint64_t process_generation = 0U;
  std::string cgroup_path;
  std::vector<int> cpus;
  std::optional<PerfCounterErrorKind> error_kind;
  std::string error;

  friend bool operator==(const LiveInstrumentationNodeStateForTest&,
                         const LiveInstrumentationNodeStateForTest&) = default;
};

class LiveInstrumentationHarnessForTest {
 public:
  explicit LiveInstrumentationHarnessForTest(
      std::vector<std::string> node_ids,
      std::chrono::milliseconds default_sample_interval =
          std::chrono::hours(24));
  ~LiveInstrumentationHarnessForTest();

  LiveInstrumentationHarnessForTest(const LiveInstrumentationHarnessForTest&) =
      delete;
  LiveInstrumentationHarnessForTest& operator=(
      const LiveInstrumentationHarnessForTest&) = delete;

  std::shared_ptr<McpLiveInstrumentationService> service() const;
  LiveInstrumentationNodeStateForTest NodeState(std::string_view node_id) const;
  void SetNodeRunning(std::string_view node_id, bool running);
  void FailAttachmentOnAttempt(std::optional<std::size_t> attempt);
  void FailNextSample();
  std::uint64_t attachment_attempts() const;
  void ApplyPerfMutation(std::string_view node_id, PerfCounterKind counter);
  void SampleNow();
  void ExpireNow();
  void Shutdown(bool run_failed = false);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
#endif

}  // namespace bbp
