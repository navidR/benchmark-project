#include "simulator_resource_limit_orchestration.h"

#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/logging.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/resource_limit_patch.h"
#include "bbp/simulator/resource_limits.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_resource_event_details.h"
#include "simulator_resource_limit_application.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

void ApplyResourceLimitUpdate(const Options& options,
                              const std::filesystem::path& events_path,
                              NodeRuntime& node,
                              const ResourceLimitPatch& patch,
                              std::mutex& node_resource_state_mutex,
                              std::stop_token stop_token,
                              const std::function<void()>& authorize_mutation,
                              std::optional<uint32_t> workload_index,
                              std::optional<uint32_t> workload_count,
                              std::optional<uint32_t> workload_node,
                              std::optional<std::uint64_t> operator_sequence,
                              bool resolve_operator_io_limit) {
  std::lock_guard<std::mutex> lock(node_resource_state_mutex);
  if (!node.cgroup) {
    throw std::runtime_error("resource update requires a node cgroup");
  }
  const ResourceLimits previous = node.resources;
  const ResourceLimitPatch effective_patch =
      resolve_operator_io_limit
          ? ResolveOperatorResourceLimitPatch(previous, patch)
          : patch;
  const ResourceLimits next =
      ApplyResourceLimitPatch(previous, effective_patch, node.config.id);
  ThrowIfStopRequested(stop_token);
  const bool mutation_admitted = static_cast<bool>(authorize_mutation);
  if (authorize_mutation) {
    authorize_mutation();
  }
  try {
    WriteResourceLimits(*node.cgroup, previous, next);
    ThrowIfStopRequested(stop_token);
  } catch (...) {
    const std::exception_ptr apply_error = std::current_exception();
    std::vector<std::string> rollback_errors;
    try {
      WriteResourceLimits(*node.cgroup, next, previous);
    } catch (const std::exception& restore_error) {
      rollback_errors.push_back(restore_error.what());
      BBP_LOG(error) << "failed to restore partially applied resource update "
                        "for "
                     << node.config.id << ": " << restore_error.what();
    } catch (...) {
      rollback_errors.push_back("unknown exception");
      BBP_LOG(error) << "failed to restore partially applied resource update "
                        "for "
                     << node.config.id << ": unknown exception";
    }
    if (!rollback_errors.empty()) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "resource limit update outcome is unconfirmed", apply_error,
          rollback_errors);
    }
    if (mutation_admitted) {
      RethrowWorkloadMutationAfterVerifiedRollback(apply_error);
    }
    std::rethrow_exception(apply_error);
  }
  try {
    node.resources = next;
    node.resource_profile.clear();
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kResourceLimitsUpdated,
               ResourceLimitUpdateDetail(patch, previous, next, workload_index,
                                         workload_count, workload_node,
                                         operator_sequence));
  } catch (...) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "resource limit update completed without a publishable outcome",
        std::current_exception());
  }
}

void ApplyRuntimeResourceLimitUpdates(const Options& options,
                                      const std::filesystem::path& events_path,
                                      const RuntimeNodeSnapshot& nodes,
                                      std::mutex& node_resource_state_mutex,
                                      std::stop_token stop_token) {
  for (const auto& [node_index, patch] :
       options.runtime_node_resource_updates) {
    ThrowIfStopRequested(stop_token);
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime resource update node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    ApplyResourceLimitUpdate(options, events_path, node, patch,
                             node_resource_state_mutex, stop_token);
  }
}

}  // namespace bbp::simulator_app_internal
