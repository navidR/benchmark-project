#include "simulator_node_signal.h"

#include <sys/wait.h>

#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/node_lifecycle_policy.h"
#include "bbp/simulation_event_kind.h"
#include "simulator_event_writing.h"

namespace bbp::simulator_app_internal {
namespace {

boost::json::object Observe(NodeRuntime& node) {
  const std::string_view state = node.process.ObservedState();
  boost::json::object observation{
      {"state", state},
      {"restart_count", node.RestartCount()},
      {"restart_policy",
       NodeRestartPolicyName(node.lifecycle_policy.restart_policy)},
      {"restart_action", "none_observed"},
      {"cleanup_state", "not_requested_by_signal"}};
  const auto status = node.process.exit_status();
  if (status) {
    observation["raw_status"] = *status;
    if (WIFEXITED(*status)) observation["exit_code"] = WEXITSTATUS(*status);
    if (WIFSIGNALED(*status)) {
      observation["terminating_signal"] = WTERMSIG(*status);
      observation["core_dumped"] = WCOREDUMP(*status) != 0;
    }
    observation["restart_action"] = "awaiting_lifecycle_supervisor";
  }
  return observation;
}

}  // namespace

boost::json::object DeliverNodeSignal(NodeRuntime& node,
                                      const SimulationCommand& command,
                                      const RunProcessState::Guard&,
                                      boost::json::object wallet_selection) {
  if (!command.signal_request) {
    throw std::logic_error("process signal command requires a signal request");
  }
  const auto delivery = node.process.DeliverSignal(
      command.signal_request->signal, command.signal_request->scope);
  if (command.operation_control) command.operation_control->MarkCommitted();
  try {
    boost::json::object result{
        {"target", command.kind == SimulationCommandKind::kSignalMiner
                       ? "miner_node_daemon"
                   : wallet_selection.empty() ? "node_daemon"
                                              : "wallet_node_daemon"},
        {"target_pid", delivery.target_pid},
        {"process_group_id", delivery.process_group_id},
        {"signal", delivery.signal},
        {"signal_name", ProcessSignalName(delivery.signal)},
        {"scope", ProcessSignalScopeName(delivery.scope)},
        {"kernel_result", delivery.kernel_result},
        {"errno", delivery.error_number},
        {"accepted", delivery.kernel_result == 0},
        {"observation", Observe(node)},
        {"evidence_families",
         boost::json::array{"events", "lifecycle", "logs", "cleanup_state"}}};
    if (!wallet_selection.empty()) {
      result["wallet"] = std::move(wallet_selection);
    }
    if (delivery.kernel_result == 0) {
      node.signal_observation =
          NodeSignalObservation{.command_sequence = command.sequence,
                                .target_pid = delivery.target_pid,
                                .restart_count = node.RestartCount(),
                                .last_state = {}};
    }
    return result;
  } catch (const std::exception& error) {
    throw SimulationCommandOutcomeUnconfirmed(
        "signal attempted but observation failed: " +
        std::string(error.what()));
  }
}

void PublishNodeSignalObservation(NodeRuntime& node,
                                  const RunProcessState::Guard&,
                                  const std::filesystem::path& events_path,
                                  const std::string& run_id) {
  if (!node.signal_observation) return;
  auto& pending = *node.signal_observation;
  if (node.process.pid() != pending.target_pid ||
      node.RestartCount() != pending.restart_count) {
    node.signal_observation.reset();
    return;
  }
  boost::json::object observation = Observe(node);
  const std::string state(observation.at("state").as_string());
  if (pending.last_state == state) return;
  observation["command_id"] =
      "command-" + std::to_string(pending.command_sequence);
  observation["target_pid"] = pending.target_pid;
  if (state == "exited" && node.Lifecycle() == NodeRuntimeLifecycle::kRunning) {
    observation["restart_action"] =
        NodeRestartPolicyAllowsRestart(node.lifecycle_policy.restart_policy,
                                       *node.process.exit_status())
            ? "restart_requested"
            : "run_stop_requested";
  }
  WriteEvent(events_path, run_id, node.config.id,
             SimulationEventKind::kProcessSignalObserved,
             boost::json::serialize(observation));
  pending.last_state = state;
  if (state == "exited") node.signal_observation.reset();
}

}  // namespace bbp::simulator_app_internal
