#include "simulator_profile_switching.h"

#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/profile_switch_workload.h"
#include "bbp/simulator/resource_limits.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_resource_event_details.h"
#include "simulator_resource_limit_application.h"
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

void WriteProfileRollbackFailureEventSafely(
    const Options& options, const std::filesystem::path& events_path,
    WorkloadKind kind, std::string_view profile,
    const std::exception_ptr& original_error,
    const std::vector<std::string>& rollback_errors) noexcept {
  if (rollback_errors.empty()) {
    return;
  }
  try {
    WriteEvent(
        events_path, options.run_id, "sim",
        SimulationEventKind::kProfileUpdateRollbackFailed,
        ProfileRollbackFailureDetail(
            kind, profile, ExceptionMessage(original_error), rollback_errors));
  } catch (const std::exception& event_error) {
    BBP_LOG(error) << "failed to record profile rollback failure: "
                   << event_error.what();
  } catch (...) {
    BBP_LOG(error) << "failed to record profile rollback failure: unknown "
                      "exception";
  }
}

}  // namespace

void ApplyResourceProfileSwitch(
    const Options& options, const std::filesystem::path& events_path,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_resource_state_mutex,
    const ProfileSwitchWorkload& workload, uint32_t workload_index,
    uint32_t workload_count, std::stop_token stop_token,
    const std::function<void()>& authorize_mutation) {
  const ResourceLimits& desired =
      options.resource_profiles.at(workload.profile);
  struct PreviousState {
    uint32_t node = 0U;
    ResourceLimits limits;
    std::string profile;
  };
  std::vector<PreviousState> previous_states;
  previous_states.reserve(workload.nodes.size());
  std::vector<std::size_t> attempted;

  {
    std::lock_guard<std::mutex> lock(node_resource_state_mutex);
    for (const uint32_t one_based_node : workload.nodes) {
      if (one_based_node == 0U || one_based_node > nodes.size()) {
        throw std::runtime_error(
            "resource profile target node is out of range");
      }
      const NodeRuntime& runtime = nodes[one_based_node - 1U];
      if (!runtime.cgroup) {
        throw std::runtime_error("resource profile update requires a cgroup");
      }
      previous_states.push_back(PreviousState{
          .node = one_based_node,
          .limits = runtime.resources,
          .profile = runtime.resource_profile,
      });
    }

    attempted.reserve(previous_states.size());
    bool mutation_admitted = false;
    try {
      for (std::size_t index = 0; index < previous_states.size(); ++index) {
        ThrowIfStopRequested(stop_token);
        NodeRuntime& runtime = nodes[previous_states[index].node - 1U];
        if (!mutation_admitted && authorize_mutation) {
          authorize_mutation();
          mutation_admitted = true;
        }
        attempted.push_back(index);
        WriteResourceLimits(*runtime.cgroup, previous_states[index].limits,
                            desired);
        ThrowIfStopRequested(stop_token);
      }
    } catch (...) {
      const std::exception_ptr original_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      for (auto iter = attempted.rbegin(); iter != attempted.rend(); ++iter) {
        const PreviousState& previous = previous_states[*iter];
        NodeRuntime& runtime = nodes[previous.node - 1U];
        try {
          WriteResourceLimits(*runtime.cgroup, desired, previous.limits);
        } catch (const std::exception& error) {
          rollback_errors.push_back(runtime.config.id + ": " + error.what());
        } catch (...) {
          rollback_errors.push_back(runtime.config.id +
                                    ": unknown rollback error");
        }
      }
      WriteProfileRollbackFailureEventSafely(
          options, events_path, WorkloadKind::kSetResourceProfile,
          workload.profile, original_error, rollback_errors);
      if (!rollback_errors.empty()) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "resource profile update outcome is unconfirmed", original_error,
            rollback_errors);
      }
      if (mutation_admitted) {
        RethrowWorkloadMutationAfterVerifiedRollback(original_error);
      }
      std::rethrow_exception(original_error);
    }

    try {
      for (const PreviousState& previous : previous_states) {
        NodeRuntime& runtime = nodes[previous.node - 1U];
        runtime.resources = desired;
        runtime.resource_profile = workload.profile;
      }
    } catch (...) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "resource profile kernel state was applied without a coherent "
          "runtime state",
          std::current_exception());
    }
  }

  try {
    for (const PreviousState& previous : previous_states) {
      const NodeRuntime& runtime = nodes[previous.node - 1U];
      WriteEvent(events_path, options.run_id, runtime.config.id,
                 SimulationEventKind::kResourceProfileUpdated,
                 ResourceProfileUpdateDetail(
                     workload, previous.node, previous.profile, previous.limits,
                     desired, workload_index, workload_count));
    }
  } catch (...) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "resource profile update completed without a publishable outcome",
        std::current_exception());
  }
}

void ApplyNetworkProfileSwitch(const Options& options,
                               const std::filesystem::path& events_path,
                               const RuntimeNodeSnapshot& nodes,
                               std::mutex& node_network_state_mutex,
                               const ProfileSwitchWorkload& workload,
                               uint32_t workload_index, uint32_t workload_count,
                               std::stop_token stop_token) {
  const NetworkCondition& desired =
      options.network_profiles.at(workload.profile);
  struct PreviousState {
    uint32_t node = 0U;
    NodeVethConfig network;
    std::string profile;
    NodeVethConfig current_network;
    QdiscInfo applied_qdisc{};
  };
  std::vector<PreviousState> previous_states;
  previous_states.reserve(workload.nodes.size());
  std::vector<std::size_t> attempted;

  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    for (const uint32_t one_based_node : workload.nodes) {
      if (one_based_node == 0U || one_based_node > nodes.size()) {
        throw std::runtime_error("network profile target node is out of range");
      }
      const NodeRuntime& runtime = nodes[one_based_node - 1U];
      if (!runtime.network) {
        throw std::runtime_error(
            "network profile update requires isolated networking");
      }
      previous_states.push_back(PreviousState{
          .node = one_based_node,
          .network = *runtime.network,
          .profile = runtime.network_profile,
          .current_network = {},
          .applied_qdisc = {},
      });
    }

    try {
      for (std::size_t index = 0; index < previous_states.size(); ++index) {
        ThrowIfStopRequested(stop_token);
        attempted.push_back(index);
        PreviousState& previous = previous_states[index];
        NodeVethConfig desired_network = previous.network;
        desired_network.apply_condition = true;
        desired_network.condition = desired;
        ReplaceNetworkConditionQdisc(desired_network.host_name, desired);
        ThrowIfStopRequested(stop_token);
        previous.applied_qdisc =
            VerifyNodeNetworkCondition(desired_network, stop_token);
        previous.current_network = std::move(desired_network);
      }
    } catch (...) {
      const std::exception_ptr original_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      for (auto iter = attempted.rbegin(); iter != attempted.rend(); ++iter) {
        const PreviousState& previous = previous_states[*iter];
        try {
          RestoreNodeNetworkCondition(previous.network);
        } catch (const std::exception& error) {
          rollback_errors.push_back(nodes[previous.node - 1U].config.id + ": " +
                                    error.what());
        } catch (...) {
          rollback_errors.push_back(nodes[previous.node - 1U].config.id +
                                    ": unknown rollback error");
        }
      }
      WriteProfileRollbackFailureEventSafely(
          options, events_path, WorkloadKind::kSetNetworkProfile,
          workload.profile, original_error, rollback_errors);
      if (!rollback_errors.empty()) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "network profile update outcome is unconfirmed", original_error,
            rollback_errors);
      }
      std::rethrow_exception(original_error);
    }

    try {
      for (PreviousState& previous : previous_states) {
        NodeRuntime& runtime = nodes[previous.node - 1U];
        runtime.network = previous.current_network;
        runtime.network_profile = workload.profile;
      }
    } catch (...) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "network profile kernel state was applied without a coherent "
          "runtime state",
          std::current_exception());
    }
  }

  try {
    for (const PreviousState& previous : previous_states) {
      const NodeRuntime& runtime = nodes[previous.node - 1U];
      WriteEvent(events_path, options.run_id, runtime.config.id,
                 SimulationEventKind::kNetworkProfileUpdated,
                 NetworkProfileUpdateDetail(
                     workload, previous.node, previous.profile,
                     previous.network, previous.current_network,
                     previous.applied_qdisc, workload_index, workload_count));
    }
  } catch (...) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "network profile update completed without a publishable outcome",
        std::current_exception());
  }
}

}  // namespace bbp::simulator_app_internal
