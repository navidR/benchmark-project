#include "simulator_node_lifecycle_event_details.h"

#include <sys/wait.h>

#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <optional>
#include <string>

#include "bbp/node_lifecycle_policy.h"
#include "bbp/process.h"
#include "bbp/simulation_time_scale.h"
#include "bbp/simulator/node_runtime.h"

namespace bbp::simulator_app_internal {

std::uint64_t ElapsedMilliseconds(
    std::chrono::steady_clock::time_point epoch,
    std::chrono::steady_clock::time_point timestamp) {
  if (timestamp <= epoch) {
    return 0U;
  }
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(timestamp - epoch)
          .count();
  return static_cast<std::uint64_t>(elapsed);
}

std::string ProcessExitDetail(const ChildProcess& process,
                              const RunProcessState::Guard&) {
  const bool running = process.running();
  boost::json::object detail;
  detail["running"] = running;
  detail["pid"] = process.pid();
  const std::optional<int> status = process.exit_status();
  if (!status) {
    return boost::json::serialize(detail);
  }
  detail["raw_status"] = *status;
  if (WIFEXITED(*status)) {
    detail["kind"] = "exit";
    detail["exit_code"] = WEXITSTATUS(*status);
  } else if (WIFSIGNALED(*status)) {
    detail["kind"] = "signal";
    detail["signal"] = WTERMSIG(*status);
  } else {
    detail["kind"] = "other";
  }
  return boost::json::serialize(detail);
}

std::string RestartDetail(pid_t pid, uint64_t restart_count,
                          std::string_view reason) {
  boost::json::object detail;
  detail["pid"] = pid;
  detail["restart_count"] = restart_count;
  detail["reason"] = reason;
  return boost::json::serialize(detail);
}

std::string RestartRequestedDetail(NodeRuntime& node, std::string_view reason) {
  boost::json::object detail;
  detail["restart_count"] = node.RestartCount() + 1U;
  detail["reason"] = reason;
  detail["restart_policy"] =
      std::string(NodeRestartPolicyName(node.lifecycle_policy.restart_policy));
  return boost::json::serialize(detail);
}

std::string RestartPolicyAppliedDetail(const NodeRuntime& node, int wait_status,
                                       bool restart,
                                       std::string_view suppression_reason) {
  boost::json::object detail;
  detail["restart_policy"] =
      std::string(NodeRestartPolicyName(node.lifecycle_policy.restart_policy));
  detail["restart"] = restart;
  if (suppression_reason.empty()) {
    detail["suppression_reason"] = nullptr;
  } else {
    detail["suppression_reason"] = suppression_reason;
  }
  detail["raw_status"] = wait_status;
  if (WIFEXITED(wait_status)) {
    detail["exit_kind"] = "exit";
    detail["exit_code"] = WEXITSTATUS(wait_status);
  } else if (WIFSIGNALED(wait_status)) {
    detail["exit_kind"] = "signal";
    detail["signal"] = WTERMSIG(wait_status);
  } else {
    detail["exit_kind"] = "other";
  }
  return boost::json::serialize(detail);
}

std::string NodeLifecycleDeadlineDetail(
    const NodeRuntime& node, const SimulationTimeScale& time_scale,
    std::chrono::steady_clock::time_point simulation_epoch,
    std::chrono::milliseconds simulation_offset, std::string_view reason) {
  const std::chrono::milliseconds wall_offset =
      time_scale.WallDuration(simulation_offset);
  boost::json::object detail;
  detail["reason"] = reason;
  detail["restart_policy"] =
      std::string(NodeRestartPolicyName(node.lifecycle_policy.restart_policy));
  detail["scheduled_simulation_ms"] = simulation_offset.count();
  detail["scheduled_wall_ms"] = wall_offset.count();
  const std::uint64_t elapsed =
      ElapsedMilliseconds(simulation_epoch, std::chrono::steady_clock::now());
  detail["elapsed_wall_ms"] = elapsed;
  detail["lateness_ms"] =
      elapsed > static_cast<std::uint64_t>(wall_offset.count())
          ? elapsed - static_cast<std::uint64_t>(wall_offset.count())
          : 0U;
  return boost::json::serialize(detail);
}

std::string ProcessStartedDetail(
    const NodeRuntime& node, std::string_view reason,
    std::chrono::steady_clock::time_point simulation_epoch,
    const SimulationTimeScale& time_scale, const RunProcessState::Guard&) {
  boost::json::object detail;
  detail["pid"] = node.process.pid();
  detail["reason"] = reason;
  detail["restart_policy"] =
      std::string(NodeRestartPolicyName(node.lifecycle_policy.restart_policy));
  if (node.lifecycle_policy.start_time) {
    detail["scheduled_simulation_ms"] =
        node.lifecycle_policy.start_time->count();
    detail["scheduled_wall_ms"] =
        time_scale.WallDuration(*node.lifecycle_policy.start_time).count();
  } else {
    detail["scheduled_simulation_ms"] = 0;
    detail["scheduled_wall_ms"] = 0;
  }
  detail["elapsed_wall_ms"] =
      ElapsedMilliseconds(simulation_epoch, std::chrono::steady_clock::now());
  return boost::json::serialize(detail);
}

}  // namespace bbp::simulator_app_internal
