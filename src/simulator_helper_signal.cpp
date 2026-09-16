#include "simulator_helper_signal.h"

#include <sys/wait.h>

#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <stdexcept>

#include "bbp/simulation_event_kind.h"
#include "simulator_event_writing.h"

namespace bbp::simulator_app_internal {
namespace {

boost::json::object Observe(NetworkNamespace& network_namespace) {
  const auto state = network_namespace.HelperObservedState();
  boost::json::object observation{
      {"state", state},
      {"restart_count", 0U},
      {"restart_policy", "never"},
      {"restart_action", state == "exited" ? "not_restarted" : "none_observed"},
      {"cleanup_state", "namespace_retained"}};
  if (const auto status = network_namespace.helper_exit_status()) {
    observation["raw_status"] = *status;
    if (WIFEXITED(*status)) observation["exit_code"] = WEXITSTATUS(*status);
    if (WIFSIGNALED(*status)) {
      observation["terminating_signal"] = WTERMSIG(*status);
      observation["core_dumped"] = WCOREDUMP(*status) != 0;
    }
  }
  return observation;
}

}  // namespace

boost::json::object DeliverHelperSignal(NodeRuntime& node,
                                        const SimulationCommand& command,
                                        const RunProcessState::Guard&) {
  if (!command.signal_request || !node.network_namespace) {
    throw std::logic_error(
        "helper signal requires a request and owned namespace");
  }
  auto& network_namespace = *node.network_namespace;
  const auto delivery = network_namespace.DeliverHelperSignal(
      command.signal_request->signal, command.signal_request->scope);
  if (command.operation_control) command.operation_control->MarkCommitted();
  try {
    boost::json::object result{
        {"target", "network_namespace_helper"},
        {"target_pid", delivery.target_pid},
        {"process_group_id", delivery.process_group_id},
        {"signal", delivery.signal},
        {"signal_name", ProcessSignalName(delivery.signal)},
        {"scope", ProcessSignalScopeName(delivery.scope)},
        {"kernel_result", delivery.kernel_result},
        {"errno", delivery.error_number},
        {"accepted", delivery.kernel_result == 0},
        {"observation", Observe(network_namespace)},
        {"evidence_families",
         boost::json::array{"events", "lifecycle", "logs", "cleanup_state"}}};
    if (delivery.kernel_result == 0) {
      node.helper_signal_observation =
          NodeSignalObservation{.command_sequence = command.sequence,
                                .target_pid = delivery.target_pid,
                                .restart_count = 0U,
                                .last_state = {}};
    }
    return result;
  } catch (const std::exception& error) {
    throw SimulationCommandOutcomeUnconfirmed(
        "helper signal attempted but observation failed: " +
        std::string(error.what()));
  }
}

void PublishHelperSignalObservation(NodeRuntime& node,
                                    const RunProcessState::Guard&,
                                    const std::filesystem::path& events_path,
                                    const std::string& run_id) {
  if (!node.helper_signal_observation) return;
  auto& pending = *node.helper_signal_observation;
  if (!node.network_namespace ||
      node.network_namespace->helper_pid() != pending.target_pid) {
    node.helper_signal_observation.reset();
    return;
  }
  auto observation = Observe(*node.network_namespace);
  const std::string state(observation.at("state").as_string());
  if (pending.last_state == state) return;
  observation["command_id"] =
      "command-" + std::to_string(pending.command_sequence);
  observation["target"] = "network_namespace_helper";
  observation["target_pid"] = pending.target_pid;
  WriteEvent(events_path, run_id, node.config.id,
             SimulationEventKind::kProcessSignalObserved,
             boost::json::serialize(observation));
  pending.last_state = state;
  if (state == "exited") node.helper_signal_observation.reset();
}

}  // namespace bbp::simulator_app_internal
