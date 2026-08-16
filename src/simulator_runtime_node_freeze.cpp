#include "simulator_runtime_node_freeze.h"

#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
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

std::string PersistentFreezeDetail(bool frozen) {
  boost::json::object detail;
  detail["frozen"] = frozen;
  detail["persistent"] = true;
  return boost::json::serialize(detail);
}

std::string FreezeDetail(std::uint32_t duration_ms, bool frozen) {
  boost::json::object detail;
  detail["duration_ms"] = duration_ms;
  detail["frozen"] = frozen;
  return boost::json::serialize(detail);
}

}  // namespace

bool WaitForNodeFrozenState(const Cgroup& cgroup, bool expected,
                            std::stop_token stop_token) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    ThrowIfStopRequested(stop_token);
    if (cgroup.Frozen() == expected) {
      return true;
    }
    WaitForDuration(std::chrono::milliseconds(20), stop_token);
  }
  return false;
}

void SetNodeFrozen(const Options& options,
                   const std::filesystem::path& events_path, NodeRuntime& node,
                   bool frozen, std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node freeze control requires a node cgroup");
  }
  if (frozen) {
    node.cgroup->Freeze();
  } else {
    node.cgroup->Thaw();
  }
  if (!WaitForNodeFrozenState(*node.cgroup, frozen, stop_token)) {
    throw std::runtime_error("node cgroup did not report " +
                             std::string(frozen ? "frozen: " : "thawed: ") +
                             node.config.id);
  }
  WriteEvent(events_path, options.run_id, node.config.id,
             frozen ? SimulationEventKind::kCgroupFrozen
                    : SimulationEventKind::kCgroupThawed,
             PersistentFreezeDetail(frozen));
}

void FreezeNodeForDuration(const Options& options,
                           const std::filesystem::path& events_path,
                           NodeRuntime& node, std::uint32_t duration_ms,
                           std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node freeze requires a node cgroup");
  }
  if (node.cgroup->Frozen()) {
    throw std::runtime_error(
        "timed node freeze requires an initially thawed cgroup");
  }

  bool freeze_attempted = false;
  bool thaw_publication_started = false;
  try {
    freeze_attempted = true;
    node.cgroup->Freeze();
    if (!WaitForNodeFrozenState(*node.cgroup, true, stop_token)) {
      throw std::runtime_error("node cgroup did not report frozen: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kCgroupFrozen,
               FreezeDetail(duration_ms, true));
    WaitForDuration(std::chrono::milliseconds(duration_ms), stop_token);
    node.cgroup->Thaw();
    if (!WaitForNodeFrozenState(*node.cgroup, false, stop_token)) {
      throw std::runtime_error("node cgroup did not report thawed: " +
                               node.config.id);
    }
    thaw_publication_started = true;
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kCgroupThawed,
               FreezeDetail(duration_ms, false));
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::vector<std::string> rollback_errors;
    try {
      node.cgroup->Thaw();
      if (!WaitForNodeFrozenState(*node.cgroup, false, {})) {
        throw std::runtime_error("node cgroup remained frozen after rollback");
      }
    } catch (const std::exception& rollback_error) {
      rollback_errors.push_back(rollback_error.what());
    } catch (...) {
      rollback_errors.push_back("unknown exception");
    }
    if (!rollback_errors.empty()) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "node freeze outcome is unconfirmed", original_error,
          rollback_errors);
    }
    if (thaw_publication_started) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "node thaw completion publication is unconfirmed", original_error);
    }
    if (freeze_attempted) {
      try {
        WriteEvent(events_path, options.run_id, node.config.id,
                   SimulationEventKind::kCgroupThawed,
                   FreezeDetail(duration_ms, false));
      } catch (...) {
        const std::string rollback_event_error =
            ExceptionMessage(std::current_exception());
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "node freeze rollback completed without publishable thaw "
            "evidence",
            original_error, {rollback_event_error});
      }
    }
    std::rethrow_exception(original_error);
  }
}

void ApplyRuntimeNodeFreezes(const Options& options,
                             const std::filesystem::path& events_path,
                             const RuntimeNodeSnapshot& nodes,
                             std::stop_token stop_token) {
  for (const FreezeRequest& freeze : options.runtime_node_freezes) {
    ThrowIfStopRequested(stop_token);
    if (freeze.node_index >= nodes.size()) {
      throw std::runtime_error("runtime freeze node is out of range");
    }
    FreezeNodeForDuration(options, events_path, nodes[freeze.node_index],
                          freeze.duration_ms, stop_token);
  }
}

}  // namespace bbp::simulator_app_internal
