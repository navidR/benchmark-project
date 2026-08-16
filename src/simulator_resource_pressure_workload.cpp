#include "simulator_resource_pressure_workload.h"

#include <chrono>
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
#include "bbp/simulator/resource_limits.h"
#include "bbp/simulator/resource_pressure_workload.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_metrics_sampling.h"
#include "simulator_resource_event_details.h"
#include "simulator_resource_limit_application.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {
namespace {

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

}  // namespace

void ApplyResourcePressureWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const std::filesystem::path& metrics_path, const ChainDriver& driver,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_network_state_mutex,
    std::mutex& node_resource_state_mutex, RunProcessState& run_process_state,
    const NodeRoleTopology& runtime_role_topology,
    const ResourcePressureWorkload& workload, uint32_t workload_index,
    uint32_t workload_count, std::stop_token stop_token) {
  NodeRuntime& node = nodes[workload.node - 1U];
  if (!node.cgroup) {
    throw std::runtime_error("resource pressure requires a node cgroup");
  }

  ResourceLimits previous_limits;
  ResourceLimits pressure_limits;
  std::string previous_profile;
  {
    std::lock_guard<std::mutex> lock(node_resource_state_mutex);
    previous_limits = node.resources;
    previous_profile = node.resource_profile;
    pressure_limits = ApplyResourceLimitPatch(previous_limits, workload.patch,
                                              node.config.id);
    try {
      WriteResourceLimits(*node.cgroup, previous_limits, pressure_limits);
    } catch (...) {
      const std::exception_ptr apply_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      try {
        WriteResourceLimits(*node.cgroup, pressure_limits, previous_limits);
      } catch (const std::exception& restore_error) {
        rollback_errors.push_back(restore_error.what());
        BBP_LOG(error) << "failed to restore partially applied resource "
                          "pressure limits for "
                       << node.config.id << ": " << restore_error.what();
      } catch (...) {
        rollback_errors.push_back("unknown exception");
        BBP_LOG(error) << "failed to restore partially applied resource "
                          "pressure limits for "
                       << node.config.id << ": unknown exception";
      }
      if (!rollback_errors.empty()) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "resource pressure admission outcome is unconfirmed", apply_error,
            rollback_errors);
      }
      std::rethrow_exception(apply_error);
    }
    try {
      node.resources = pressure_limits;
      node.resource_profile.clear();
    } catch (...) {
      const std::exception_ptr publication_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      try {
        WriteResourceLimits(*node.cgroup, pressure_limits, previous_limits);
      } catch (...) {
        rollback_errors.push_back("kernel limits: " +
                                  ExceptionMessage(std::current_exception()));
      }
      try {
        node.resources = previous_limits;
        node.resource_profile = previous_profile;
      } catch (...) {
        rollback_errors.push_back("runtime limits: " +
                                  ExceptionMessage(std::current_exception()));
      }
      if (!rollback_errors.empty()) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "resource pressure kernel state was applied without coherent "
            "runtime state",
            publication_error, rollback_errors);
      }
      std::rethrow_exception(publication_error);
    }
  }

  try {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kResourcePressureStarted,
               ResourcePressureDetail(workload, previous_limits,
                                      pressure_limits, pressure_limits,
                                      workload_index, workload_count));
    WaitForDuration(std::chrono::milliseconds(workload.duration_ms),
                    stop_token);
    WriteMetricsSnapshot(metrics_path, options, driver, nodes,
                         run_process_state,
                         {node_network_state_mutex, node_resource_state_mutex},
                         {}, {}, stop_token, &runtime_role_topology);
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::vector<std::string> rollback_errors;
    try {
      {
        std::lock_guard<std::mutex> lock(node_resource_state_mutex);
        WriteResourceLimits(*node.cgroup, node.resources, previous_limits);
        node.resources = previous_limits;
        node.resource_profile = previous_profile;
      }
    } catch (const std::exception& restore_error) {
      rollback_errors.push_back(restore_error.what());
      BBP_LOG(error) << "failed to restore resource pressure limits for "
                     << node.config.id << ": " << restore_error.what();
    } catch (...) {
      rollback_errors.push_back("unknown exception");
      BBP_LOG(error) << "failed to restore resource pressure limits for "
                     << node.config.id << ": unknown exception";
    }
    if (!rollback_errors.empty()) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "resource pressure outcome is unconfirmed", original_error,
          rollback_errors);
    }
    try {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kResourcePressureRestoredAfterError,
                 ResourcePressureDetail(workload, previous_limits,
                                        pressure_limits, previous_limits,
                                        workload_index, workload_count));
    } catch (const std::exception& event_error) {
      BBP_LOG(error) << "failed to record restored resource pressure for "
                     << node.config.id << ": " << event_error.what();
    } catch (...) {
      BBP_LOG(error) << "failed to record restored resource pressure for "
                     << node.config.id << ": unknown exception";
    }
    std::rethrow_exception(original_error);
  }

  try {
    {
      std::lock_guard<std::mutex> lock(node_resource_state_mutex);
      WriteResourceLimits(*node.cgroup, node.resources, previous_limits);
      node.resources = previous_limits;
      node.resource_profile = previous_profile;
    }
  } catch (...) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "resource pressure completion could not restore prior limits",
        std::current_exception());
  }
  try {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kResourcePressureFinished,
               ResourcePressureDetail(workload, previous_limits,
                                      pressure_limits, previous_limits,
                                      workload_index, workload_count));
  } catch (...) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "resource pressure restored prior limits without a publishable "
        "completion",
        std::current_exception());
  }
}

}  // namespace bbp::simulator_app_internal
