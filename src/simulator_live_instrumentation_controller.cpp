#include "simulator_live_instrumentation_controller.h"

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "bbp/logging.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/mcp_registry.h"
#include "bbp/perf_counter.h"
#include "bbp/positive_duration.h"
#include "bbp/run_process_state.h"
#include "bbp/run_report.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_json_field_decoding.h"
#include "simulator_metrics_sampling.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

NodeRuntime& FindNodeRuntimeById(RuntimeNodeSnapshot& nodes,
                                 const std::string& node_id) {
  const auto node = std::find_if(nodes.begin(), nodes.end(),
                                 [&node_id](const NodeRuntime& candidate) {
                                   return candidate.config.id == node_id;
                                 });
  if (node == nodes.end()) {
    throw std::runtime_error("unknown block producer node: " + node_id);
  }
  return *node;
}

constexpr std::size_t kMaximumRetainedInstrumentationMeasurements = 1024U;
constexpr std::size_t kMaximumRetainedInstrumentationMeasurementBytes =
    kMcpMaximumRetainedResultBytes;
constexpr std::size_t kRetainedInstrumentationMeasurementJsonOverhead = 128U;

enum class LiveInstrumentationState {
  kRunning,
  kSucceeded,
  kCancelled,
  kFailed,
};

std::string_view LiveInstrumentationStateName(LiveInstrumentationState state) {
  switch (state) {
    case LiveInstrumentationState::kRunning:
      return "running";
    case LiveInstrumentationState::kSucceeded:
      return "succeeded";
    case LiveInstrumentationState::kCancelled:
      return "cancelled";
    case LiveInstrumentationState::kFailed:
      return "failed";
  }
  throw std::logic_error("unknown live instrumentation state");
}

bool IsTerminalLiveInstrumentationState(LiveInstrumentationState state) {
  return state != LiveInstrumentationState::kRunning;
}

struct LiveInstrumentationMeasurement {
  std::uint64_t sample = 0U;
  std::uint64_t configuration_revision = 0U;
  std::string record;
};

struct LiveInstrumentationConfiguration {
  std::uint64_t configuration_revision = 0U;
  std::vector<PerfCounterTarget> targets;
  std::vector<PerfCounterKind> counters;
  std::chrono::milliseconds sample_interval{1};
};

static_assert(
    std::is_nothrow_move_constructible_v<LiveInstrumentationConfiguration>);

struct LiveInstrumentationRecord {
  mutable std::mutex mutex;
  mutable std::mutex worker_lifecycle_mutex;
  std::condition_variable_any changed;
  std::timed_mutex sampling_boundary;
  std::string id;
  std::vector<PerfCounterTarget> targets;
  std::vector<PerfCounterKind> counters;
  std::map<std::string, NodePerfCounterConfiguration, std::less<>>
      baseline_configurations;
  std::chrono::milliseconds sample_interval{1};
  std::optional<std::chrono::milliseconds> window;
  std::chrono::steady_clock::time_point started_at =
      std::chrono::steady_clock::now();
  std::optional<std::chrono::steady_clock::time_point> deadline;
  std::uint64_t started_at_ms = 0U;
  std::optional<std::uint64_t> completed_at_ms;
  LiveInstrumentationState state = LiveInstrumentationState::kRunning;
  std::optional<std::string> failure;
  std::uint64_t configuration_revision = 1U;
  std::vector<LiveInstrumentationConfiguration> configuration_history;
  std::uint64_t sample_count = 0U;
  std::deque<LiveInstrumentationMeasurement> measurements;
  std::size_t measurement_storage_bytes = 0U;
  bool measurements_truncated = false;
  std::uint64_t dropped_measurement_count = 0U;
  std::jthread worker;
};

void RequestStopLiveInstrumentationWorker(
    const std::shared_ptr<LiveInstrumentationRecord>& record) {
  std::lock_guard<std::mutex> lock(record->worker_lifecycle_mutex);
  record->worker.request_stop();
}

void JoinLiveInstrumentationWorker(
    const std::shared_ptr<LiveInstrumentationRecord>& record) {
  std::lock_guard<std::mutex> lock(record->worker_lifecycle_mutex);
  if (record->worker.joinable()) {
    record->worker.join();
  }
}

struct LiveInstrumentationRegistry {
  mutable std::mutex mutex;
  std::map<std::string, std::shared_ptr<LiveInstrumentationRecord>, std::less<>>
      records;
  std::string active_id;
  std::uint64_t next_id = 1U;
  std::size_t retained_configuration_revisions = 0U;
  bool shutting_down = false;
};

boost::json::object LiveInstrumentationTargetJson(
    const PerfCounterTarget& target) {
  boost::json::array node_ids;
  node_ids.reserve(target.node_ids.size());
  for (const std::string& node_id : target.node_ids) {
    node_ids.emplace_back(node_id);
  }
  return boost::json::object{
      {"kind", PerfCounterTargetKindName(target.kind)},
      {"id", target.id},
      {"node_ids", std::move(node_ids)},
  };
}

boost::json::array LiveInstrumentationTargetsJson(
    const std::vector<PerfCounterTarget>& targets) {
  boost::json::array result;
  result.reserve(targets.size());
  for (const PerfCounterTarget& target : targets) {
    result.emplace_back(LiveInstrumentationTargetJson(target));
  }
  return result;
}

boost::json::object LiveInstrumentationConfigurationJson(
    const LiveInstrumentationConfiguration& configuration) {
  return boost::json::object{
      {"configuration_revision", configuration.configuration_revision},
      {"targets", LiveInstrumentationTargetsJson(configuration.targets)},
      {"counters", PerfCounterNamesJson(configuration.counters)},
      {"sample_interval_ms", configuration.sample_interval.count()},
  };
}

void RequireLiveInstrumentationConfigurationHistoryConsistent(
    const LiveInstrumentationRecord& record) {
  if (record.configuration_history.empty() ||
      record.configuration_history.size() >
          kMaximumRetainedInstrumentationConfigurationRevisions ||
      record.configuration_history.size() != record.configuration_revision) {
    throw std::logic_error(
        "instrumentation configuration history is inconsistent");
  }
  for (std::size_t index = 0U; index < record.configuration_history.size();
       ++index) {
    if (record.configuration_history[index].configuration_revision !=
        index + 1U) {
      throw std::logic_error(
          "instrumentation configuration history revision is inconsistent");
    }
  }
  const LiveInstrumentationConfiguration& current =
      record.configuration_history.back();
  if (current.configuration_revision != record.configuration_revision ||
      current.targets != record.targets ||
      current.counters != record.counters ||
      current.sample_interval != record.sample_interval) {
    throw std::logic_error(
        "instrumentation current configuration does not match its history");
  }
}

[[noreturn]] void ThrowLiveInstrumentationConfigurationHistoryCapacity() {
  throw McpOperationFailure(
      "instrumentation_configuration_history_capacity",
      "instrumentation configuration history reached its bounded run "
      "capacity",
      false);
}

boost::json::object LiveInstrumentationResultJson(
    const LiveInstrumentationRecord& record) {
  std::lock_guard<std::mutex> lock(record.mutex);
  return boost::json::object{
      {"instrumentation_id", record.id},
      {"state", LiveInstrumentationStateName(record.state)},
      {"sample_count", record.sample_count},
      {"targets", LiveInstrumentationTargetsJson(record.targets)},
  };
}

boost::json::object LiveInstrumentationResourceJson(
    const LiveInstrumentationRecord& record, bool include_measurements) {
  std::lock_guard<std::mutex> lock(record.mutex);
  RequireLiveInstrumentationConfigurationHistoryConsistent(record);
  boost::json::array configuration_history;
  configuration_history.reserve(record.configuration_history.size());
  for (const LiveInstrumentationConfiguration& configuration :
       record.configuration_history) {
    configuration_history.emplace_back(
        LiveInstrumentationConfigurationJson(configuration));
  }
  boost::json::object result{
      {"instrumentation_id", record.id},
      {"state", LiveInstrumentationStateName(record.state)},
      {"sample_count", record.sample_count},
      {"targets", LiveInstrumentationTargetsJson(record.targets)},
      {"counters", PerfCounterNamesJson(record.counters)},
      {"sample_interval_ms", record.sample_interval.count()},
      {"started_at_ms", record.started_at_ms},
      {"configuration_revision", record.configuration_revision},
      {"configuration_history", std::move(configuration_history)},
      {"retained_measurement_count", record.measurements.size()},
      {"measurements_truncated", record.measurements_truncated},
      {"dropped_measurement_count", record.dropped_measurement_count},
  };
  if (record.window) {
    result["window_ms"] = record.window->count();
  } else {
    result["window_ms"] = nullptr;
  }
  if (record.completed_at_ms) {
    result["completed_at_ms"] = *record.completed_at_ms;
  } else {
    result["completed_at_ms"] = nullptr;
  }
  if (record.failure) {
    result["failure"] = *record.failure;
  } else {
    result["failure"] = nullptr;
  }
  if (include_measurements) {
    boost::json::array measurements;
    measurements.reserve(record.measurements.size());
    for (const LiveInstrumentationMeasurement& measurement :
         record.measurements) {
      measurements.emplace_back(boost::json::object{
          {"sample", measurement.sample},
          {"configuration_revision", measurement.configuration_revision},
          {"record", boost::json::parse(measurement.record)},
      });
    }
    result["measurement_records"] = std::move(measurements);
  }
  return result;
}

std::vector<PerfCounterTarget> ParseLiveInstrumentationTargets(
    const boost::json::object& arguments) {
  const boost::json::value* value = arguments.if_contains("targets");
  if (value == nullptr || !value->is_array() || value->as_array().empty() ||
      value->as_array().size() > kMcpMaximumSelectionItems) {
    throw std::invalid_argument(
        "instrumentation targets must be a non-empty bounded array");
  }
  std::vector<PerfCounterTarget> targets;
  targets.reserve(value->as_array().size());
  std::set<std::string> target_ids;
  std::set<std::string> resolved_node_ids;
  constexpr std::array<std::string_view, 3U> kTargetFields = {"kind", "id",
                                                              "node_ids"};
  for (const boost::json::value& entry : value->as_array()) {
    if (!entry.is_object()) {
      throw std::invalid_argument(
          "instrumentation target entries must be objects");
    }
    const boost::json::object& object = entry.as_object();
    RejectUnsupportedFields(object, kTargetFields, "instrumentation target");
    const boost::json::value* kind_value = object.if_contains("kind");
    const boost::json::value* id_value = object.if_contains("id");
    const boost::json::value* node_ids_value = object.if_contains("node_ids");
    if (kind_value == nullptr || !kind_value->is_string() ||
        id_value == nullptr || !id_value->is_string() ||
        id_value->as_string().empty() || node_ids_value == nullptr ||
        !node_ids_value->is_array() || node_ids_value->as_array().empty() ||
        node_ids_value->as_array().size() > kMcpMaximumSelectionItems) {
      throw std::invalid_argument(
          "instrumentation target requires kind, id, and node_ids");
    }
    const std::optional<PerfCounterTargetKind> kind =
        PerfCounterTargetKindFromName(kind_value->as_string());
    if (!kind) {
      throw std::invalid_argument("instrumentation target kind is unsupported");
    }
    PerfCounterTarget target;
    target.kind = *kind;
    target.id = std::string(id_value->as_string());
    ValidateMcpIdentifier(target.id, "instrumentation target id");
    if (!target_ids.insert(target.id).second) {
      throw std::invalid_argument(
          "instrumentation targets contain a duplicate id");
    }
    std::set<std::string> target_nodes;
    target.node_ids.reserve(node_ids_value->as_array().size());
    for (const boost::json::value& node_id_value : node_ids_value->as_array()) {
      if (!node_id_value.is_string() || node_id_value.as_string().empty()) {
        throw std::invalid_argument(
            "instrumentation target node_ids must contain identifiers");
      }
      std::string node_id(node_id_value.as_string());
      ValidateMcpIdentifier(node_id, "instrumentation target node id");
      if (!target_nodes.insert(node_id).second) {
        throw std::invalid_argument(
            "instrumentation target contains a duplicate node id");
      }
      if (!resolved_node_ids.insert(node_id).second) {
        throw std::invalid_argument(
            "instrumentation targets resolve the same node more than once");
      }
      target.node_ids.push_back(std::move(node_id));
    }
    if (target.kind != PerfCounterTargetKind::kGroup &&
        target.node_ids.size() != 1U) {
      throw std::invalid_argument(
          "non-group instrumentation target must resolve one node");
    }
    if ((target.kind == PerfCounterTargetKind::kNode ||
         target.kind == PerfCounterTargetKind::kCgroup) &&
        target.id != target.node_ids.front()) {
      throw std::invalid_argument(
          "node and cgroup instrumentation target ids must equal their node");
    }
    if (target.kind == PerfCounterTargetKind::kWallet &&
        !IsCanonicalWalletPerfTargetId(target.id)) {
      throw std::invalid_argument(
          "wallet instrumentation target id must be "
          "wallet-<positive-index>");
    }
    targets.push_back(std::move(target));
  }
  return targets;
}

std::vector<PerfCounterKind> ParseLiveInstrumentationCounters(
    const boost::json::object& arguments) {
  const boost::json::value* value = arguments.if_contains("counters");
  if (value == nullptr || !value->is_array() || value->as_array().empty() ||
      value->as_array().size() > DefaultPerfCounterKinds().size()) {
    throw std::invalid_argument(
        "instrumentation counters must be a non-empty bounded array");
  }
  std::vector<PerfCounterKind> counters;
  counters.reserve(value->as_array().size());
  std::set<PerfCounterKind> unique;
  for (const boost::json::value& counter : value->as_array()) {
    if (!counter.is_string()) {
      throw std::invalid_argument(
          "instrumentation counters must contain strings");
    }
    const std::optional<PerfCounterKind> kind =
        PerfCounterKindFromName(counter.as_string());
    if (!kind) {
      throw std::invalid_argument("instrumentation counter is unsupported: " +
                                  std::string(counter.as_string()));
    }
    if (!unique.insert(*kind).second) {
      throw std::invalid_argument(
          "instrumentation counters contain a duplicate");
    }
    counters.push_back(*kind);
  }
  return counters;
}

std::optional<std::chrono::milliseconds> OptionalLiveInstrumentationDuration(
    const boost::json::object& arguments, std::string_view field) {
  const boost::json::value* value = arguments.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (!value->is_string()) {
    throw std::invalid_argument("instrumentation " + std::string(field) +
                                " must be a duration string");
  }
  try {
    return PositiveDuration::Parse(value->as_string()).value();
  } catch (const std::exception& error) {
    throw std::invalid_argument("instrumentation " + std::string(field) +
                                " is invalid: " + error.what());
  }
}

std::map<std::string, NodePerfCounterConfiguration, std::less<>>
LiveInstrumentationConfigurations(
    const std::vector<PerfCounterTarget>& targets,
    const std::vector<PerfCounterKind>& counters) {
  std::map<std::string, NodePerfCounterConfiguration, std::less<>>
      configurations;
  for (const PerfCounterTarget& target : targets) {
    for (const std::string& node_id : target.node_ids) {
      const bool inserted = configurations
                                .emplace(node_id,
                                         NodePerfCounterConfiguration{
                                             .node_id = node_id,
                                             .kinds = counters,
                                             .target_kind = target.kind,
                                             .target_id = target.id,
                                         })
                                .second;
      if (!inserted) {
        throw std::invalid_argument(
            "instrumentation targets resolve the same node more than once");
      }
    }
  }
  return configurations;
}

std::vector<NodePerfCounterAssignment> ResolvePerfCounterAssignments(
    const std::map<std::string, NodePerfCounterConfiguration, std::less<>>&
        configurations,
    RuntimeNodeSnapshot& nodes, bool require_running, bool require_attachment) {
  std::vector<NodePerfCounterAssignment> assignments;
  assignments.reserve(configurations.size());
  for (const auto& [node_id, configuration] : configurations) {
    assignments.push_back(NodePerfCounterAssignment{
        .node = &FindNodeRuntimeById(nodes, node_id),
        .configuration = configuration,
        .require_running = require_running,
        .require_attachment = require_attachment,
    });
  }
  return assignments;
}

void BoundLiveInstrumentationRecordMeasurements(
    LiveInstrumentationRecord& record, std::size_t maximum_bytes) {
  std::size_t retained = 0U;
  std::size_t retained_bytes = 0U;
  for (auto measurement = record.measurements.rbegin();
       measurement != record.measurements.rend() &&
       retained < kMaximumRetainedInstrumentationMeasurements;
       ++measurement) {
    const std::size_t remaining = maximum_bytes - retained_bytes;
    if (remaining < kRetainedInstrumentationMeasurementJsonOverhead ||
        measurement->record.size() >
            remaining - kRetainedInstrumentationMeasurementJsonOverhead) {
      break;
    }
    retained_bytes += measurement->record.size() +
                      kRetainedInstrumentationMeasurementJsonOverhead;
    ++retained;
  }
  const std::size_t dropped = record.measurements.size() - retained;
  for (std::size_t index = 0U; index < dropped; ++index) {
    record.measurements.pop_front();
  }
  record.measurement_storage_bytes = retained_bytes;
  if (dropped != 0U) {
    record.measurements_truncated = true;
    const std::uint64_t remaining = std::numeric_limits<std::uint64_t>::max() -
                                    record.dropped_measurement_count;
    record.dropped_measurement_count +=
        std::min<std::uint64_t>(remaining, dropped);
  }
}

void AppendLiveInstrumentationRound(
    LiveInstrumentationRecord& record,
    std::vector<std::string> measurement_records) {
  std::lock_guard<std::mutex> lock(record.mutex);
  if (record.state != LiveInstrumentationState::kRunning) {
    return;
  }
  if (record.sample_count == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("instrumentation sample count exceeds uint64");
  }
  RequireLiveInstrumentationConfigurationHistoryConsistent(record);
  const std::uint64_t sample = record.sample_count + 1U;
  const std::uint64_t configuration_revision = record.configuration_revision;
  std::size_t appended = 0U;
  try {
    for (std::string& measurement_record : measurement_records) {
      record.measurements.push_back(LiveInstrumentationMeasurement{
          .sample = sample,
          .configuration_revision = configuration_revision,
          .record = std::move(measurement_record),
      });
      ++appended;
    }
  } catch (...) {
    while (appended > 0U) {
      record.measurements.pop_back();
      --appended;
    }
    throw;
  }
  BoundLiveInstrumentationRecordMeasurements(
      record, kMaximumRetainedInstrumentationMeasurementBytes);
  record.sample_count = sample;
  record.changed.notify_all();
}

void BoundLiveInstrumentationPersistenceMeasurements(
    const std::vector<std::shared_ptr<LiveInstrumentationRecord>>& records) {
  struct Candidate {
    std::shared_ptr<LiveInstrumentationRecord> record;
    std::uint64_t started_at_ms;
    std::size_t order;
  };
  std::vector<Candidate> terminal;
  terminal.reserve(records.size());
  for (std::size_t index = 0U; index < records.size(); ++index) {
    std::lock_guard<std::mutex> lock(records[index]->mutex);
    if (IsTerminalLiveInstrumentationState(records[index]->state)) {
      terminal.push_back(Candidate{
          .record = records[index],
          .started_at_ms = records[index]->started_at_ms,
          .order = index,
      });
    }
  }
  std::sort(terminal.begin(), terminal.end(),
            [](const Candidate& left, const Candidate& right) {
              return left.started_at_ms > right.started_at_ms ||
                     (left.started_at_ms == right.started_at_ms &&
                      left.order > right.order);
            });

  std::size_t remaining = kMaximumRetainedInstrumentationMeasurementBytes;
  for (const Candidate& candidate : terminal) {
    std::lock_guard<std::mutex> lock(candidate.record->mutex);
    BoundLiveInstrumentationRecordMeasurements(*candidate.record, remaining);
    remaining -= candidate.record->measurement_storage_bytes;
  }
}

std::unique_lock<std::timed_mutex> AcquireInstrumentationTimedLock(
    std::timed_mutex& mutex,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token, std::string_view component) {
  std::unique_lock<std::timed_mutex> lock(mutex, std::defer_lock);
  while (true) {
    ThrowIfStopRequested(stop_token);
    const auto now = std::chrono::steady_clock::now();
    if (deadline && now >= *deadline) {
      throw McpOperationFailure(
          "instrumentation_operation_timeout",
          "instrumentation timed out waiting for " + std::string(component),
          true);
    }
    const auto attempt_deadline =
        deadline ? std::min(*deadline, now + std::chrono::milliseconds(20))
                 : now + std::chrono::milliseconds(20);
    if (lock.try_lock_until(attempt_deadline)) {
      ThrowIfStopRequested(stop_token);
      return lock;
    }
  }
}

bool LiveInstrumentationCommandConflicts(
    const LiveInstrumentationRegistry& registry,
    const SimulationCommand& command) {
  std::shared_ptr<LiveInstrumentationRecord> active;
  {
    std::lock_guard<std::mutex> lock(registry.mutex);
    if (registry.active_id.empty()) {
      return false;
    }
    const auto found = registry.records.find(registry.active_id);
    if (found == registry.records.end()) {
      throw std::logic_error("active instrumentation record is not retained");
    }
    active = found->second;
  }
  const std::vector<std::string>* affected_nodes = nullptr;
  if (command.kind == SimulationCommandKind::kSetPerfCounters) {
    if (!command.perf_counter_target) {
      return false;
    }
    affected_nodes = &command.perf_counter_target->node_ids;
  } else if (command.kind == SimulationCommandKind::kRemoveNodes &&
             command.node_remove) {
    affected_nodes = &command.node_remove->node_ids;
  } else {
    return false;
  }
  std::lock_guard<std::mutex> lock(active->mutex);
  return std::any_of(affected_nodes->begin(), affected_nodes->end(),
                     [&](const std::string& node_id) {
                       return active->baseline_configurations.contains(node_id);
                     });
}

void RequireNoLiveInstrumentationCommandConflict(
    const LiveInstrumentationRegistry& registry,
    const SimulationCommand& command) {
  if (LiveInstrumentationCommandConflicts(registry, command)) {
    throw std::runtime_error(
        "simulation command conflicts with the active instrumentation window");
  }
}

std::string RequireLiveInstrumentationId(const boost::json::object& arguments) {
  const boost::json::value* value = arguments.if_contains("instrumentation_id");
  if (value == nullptr || !value->is_string() || value->as_string().empty()) {
    throw std::invalid_argument(
        "instrumentation_id must be a non-empty identifier");
  }
  std::string id(value->as_string());
  ValidateMcpIdentifier(id, "instrumentation_id");
  return id;
}

void ValidateLiveInstrumentationMappings(
    const std::vector<PerfCounterTarget>& targets, RuntimeNodeSnapshot& nodes,
    const RuntimeWalletSnapshot& wallets, const Options& options) {
  std::map<std::string, std::set<std::string>, std::less<>>
      authoritative_groups;
  std::set<std::string> all_nodes;
  std::map<std::string, std::set<std::string>, std::less<>> role_nodes;
  std::vector<std::string> node_ids;
  node_ids.reserve(nodes.size());
  for (std::size_t index = 0U; index < nodes.size(); ++index) {
    const std::string& node_id = nodes[index].config.id;
    node_ids.push_back(node_id);
    all_nodes.insert(node_id);
    role_nodes["role-" + NodeRoleName(options,
                                      static_cast<std::uint32_t>(index),
                                      &wallets.registry().topology())]
        .insert(node_id);
  }
  if (!all_nodes.empty()) {
    authoritative_groups.emplace("all", std::move(all_nodes));
  }
  const auto append_index_groups =
      [&](const std::vector<std::vector<std::uint32_t>>& groups,
          std::string_view prefix) {
        for (std::size_t index = 0U; index < groups.size(); ++index) {
          std::set<std::string> members;
          for (const std::uint32_t node_index : groups[index]) {
            if (node_index >= node_ids.size()) {
              throw std::logic_error(
                  "active topology group references an unknown node");
            }
            members.insert(node_ids[node_index]);
          }
          if (!members.empty()) {
            authoritative_groups.emplace(
                std::string(prefix) + std::to_string(index + 1U),
                std::move(members));
          }
        }
      };
  const PeerTopologyConfig& topology =
      wallets.registry().topology().peer_topology;
  append_index_groups(topology.groups, "topology-");
  append_index_groups(topology.regions, "region-");
  authoritative_groups.insert(std::make_move_iterator(role_nodes.begin()),
                              std::make_move_iterator(role_nodes.end()));

  for (const PerfCounterTarget& target : targets) {
    for (const std::string& node_id : target.node_ids) {
      static_cast<void>(FindNodeRuntimeById(nodes, node_id));
    }
    if (target.kind == PerfCounterTargetKind::kGroup) {
      const auto group = authoritative_groups.find(target.id);
      const std::set<std::string> requested_members(target.node_ids.begin(),
                                                    target.node_ids.end());
      if (group == authoritative_groups.end() ||
          group->second != requested_members) {
        throw std::invalid_argument(
            "group instrumentation target does not match the active "
            "topology");
      }
      continue;
    }
    if (target.kind != PerfCounterTargetKind::kWallet) {
      continue;
    }
    constexpr std::string_view kWalletPrefix = "wallet-";
    std::string_view index_text(target.id);
    index_text.remove_prefix(kWalletPrefix.size());
    std::uint32_t wallet_index = 0U;
    const auto [end, error] = std::from_chars(
        index_text.data(), index_text.data() + index_text.size(), wallet_index);
    if (error != std::errc() || end != index_text.data() + index_text.size() ||
        wallet_index == 0U) {
      throw std::invalid_argument(
          "wallet instrumentation target id is invalid");
    }
    const auto wallet =
        std::find_if(wallets.wallets().begin(), wallets.wallets().end(),
                     [wallet_index](const WalletIdentity& candidate) {
                       return candidate.wallet_index == wallet_index;
                     });
    if (wallet == wallets.wallets().end() ||
        wallet->node_id != target.node_ids.front()) {
      throw std::invalid_argument(
          "wallet instrumentation target does not match the active wallet "
          "registry");
    }
  }
}

void ValidateLiveInstrumentationDuration(std::chrono::milliseconds duration,
                                         std::string_view field) {
  try {
    static_cast<void>(
        SteadyDeadline(std::chrono::steady_clock::now(), duration));
  } catch (const std::exception& error) {
    throw std::invalid_argument(
        "instrumentation " + std::string(field) +
        " exceeds the monotonic clock range: " + error.what());
  }
}

class LiveInstrumentationController {
 public:
  LiveInstrumentationController(
      const Options& options, std::filesystem::path metrics_path,
      std::filesystem::path events_path, ChainDriver& driver,
      RuntimeNodeInventory& node_inventory,
      RuntimeWalletRegistry& wallet_registry,
      RunProcessState& run_process_state, std::timed_mutex& node_mutation_mutex,
      MetricsSnapshotSynchronization metrics_synchronization,
      std::shared_ptr<LiveInstrumentationRegistry> registry,
      std::optional<NodePerfCounterTransactionBackend> transaction_backend =
          std::nullopt,
      LiveInstrumentationMeasurementCollector measurement_collector = {})
      : options_(options),
        metrics_path_(std::move(metrics_path)),
        events_path_(std::move(events_path)),
        driver_(driver),
        node_inventory_(node_inventory),
        wallet_registry_(wallet_registry),
        run_process_state_(run_process_state),
        node_mutation_mutex_(node_mutation_mutex),
        metrics_synchronization_(metrics_synchronization),
        registry_(std::move(registry)),
        transaction_backend_(std::move(transaction_backend)),
        measurement_collector_(std::move(measurement_collector)) {
    if (!registry_) {
      throw std::invalid_argument(
          "live instrumentation registry must not be null");
    }
    if (transaction_backend_ &&
        (!transaction_backend_->is_running || !transaction_backend_->attach ||
         !transaction_backend_->reset)) {
      throw std::invalid_argument(
          "live instrumentation transaction backend is incomplete");
    }
  }

  std::shared_ptr<McpLiveInstrumentationService> MakeService() {
    auto service = std::make_shared<McpLiveInstrumentationService>();
    service->operation = [this](McpOperationKind kind,
                                const boost::json::object& arguments,
                                std::stop_token stop_token) {
      return Operate(kind, arguments, stop_token);
    };
    service->read = [this](McpInformationFamily family,
                           std::stop_token stop_token) {
      return Read(family, stop_token);
    };
    return service;
  }

  void Shutdown(bool run_failed) {
    std::shared_ptr<LiveInstrumentationRecord> active;
    std::vector<std::shared_ptr<LiveInstrumentationRecord>> records;
    {
      std::lock_guard<std::mutex> lock(registry_->mutex);
      registry_->shutting_down = true;
      records.reserve(registry_->records.size());
      for (const auto& [id, record] : registry_->records) {
        static_cast<void>(id);
        records.push_back(record);
      }
      if (!registry_->active_id.empty()) {
        const auto found = registry_->records.find(registry_->active_id);
        if (found == registry_->records.end()) {
          throw std::logic_error(
              "active instrumentation record is not retained");
        }
        active = found->second;
      }
    }
    if (active) {
      RequestStopLiveInstrumentationWorker(active);
      active->changed.notify_all();
    }

    auto lifecycle_lock = AcquireInstrumentationTimedLock(
        lifecycle_mutex_, std::nullopt, {}, "instrumentation lifecycle");
    std::exception_ptr restoration_failure;
    if (active) {
      try {
        auto mutation_lock = AcquireInstrumentationTimedLock(
            node_mutation_mutex_, std::nullopt, {}, "node mutation boundary");
        auto sampling_lock = AcquireInstrumentationTimedLock(
            active->sampling_boundary, std::nullopt, {}, "sampling boundary");
        if (IsActiveRecord(active)) {
          const LiveInstrumentationState terminal_state =
              run_failed ? LiveInstrumentationState::kFailed
                         : LiveInstrumentationState::kCancelled;
          try {
            RestoreBaseline(*active, {});
            MarkRestored(active, terminal_state,
                         run_failed
                             ? std::optional<std::string>(
                                   "run failed while instrumentation was "
                                   "active")
                             : std::nullopt);
          } catch (const std::exception& error) {
            MarkRestorationFailed(
                active,
                "instrumentation shutdown could not restore the "
                "pre-window configuration: " +
                    std::string(error.what()));
            restoration_failure = std::current_exception();
          } catch (...) {
            MarkRestorationFailed(
                active,
                "instrumentation shutdown could not restore the "
                "pre-window configuration");
            restoration_failure = std::current_exception();
          }
        }
      } catch (const std::exception& error) {
        MarkRestorationFailed(
            active,
            "instrumentation shutdown could not enter its restoration "
            "boundary: " +
                std::string(error.what()));
        restoration_failure = std::current_exception();
      } catch (...) {
        MarkRestorationFailed(
            active,
            "instrumentation shutdown could not enter its restoration "
            "boundary");
        restoration_failure = std::current_exception();
      }
    }

    for (const std::shared_ptr<LiveInstrumentationRecord>& record : records) {
      RequestStopLiveInstrumentationWorker(record);
      record->changed.notify_all();
    }
    for (const std::shared_ptr<LiveInstrumentationRecord>& record : records) {
      JoinLiveInstrumentationWorker(record);
    }
    std::exception_ptr persistence_failure;
    if (!metrics_path_.empty()) {
      try {
        const std::optional<std::string> active_id =
            ActiveInstrumentationIdForPersistence();
        PersistRetainedStateLocked(
            active_id ? std::optional<std::string_view>(*active_id)
                      : std::nullopt);
      } catch (...) {
        persistence_failure = std::current_exception();
      }
    }
    if (restoration_failure) {
      std::rethrow_exception(restoration_failure);
    }
    if (persistence_failure) {
      std::rethrow_exception(persistence_failure);
    }
  }

#ifdef BBP_ENABLE_TEST_HOOKS
  void ApplyPerfMutationForTest(std::string_view node_id,
                                PerfCounterKind counter) {
    SimulationCommand command;
    command.kind = SimulationCommandKind::kSetPerfCounters;
    command.node_id = node_id;
    command.perf_counter_target = PerfCounterTarget{
        .kind = PerfCounterTargetKind::kNode,
        .id = std::string(node_id),
        .node_ids = {std::string(node_id)},
    };
    command.perf_counter_kinds = {counter};

    auto mutation_lock = AcquireInstrumentationTimedLock(
        node_mutation_mutex_, std::nullopt, {}, "node mutation boundary");
    RequireNoLiveInstrumentationCommandConflict(*registry_, command);
    RuntimeNodeSnapshot nodes = node_inventory_.Snapshot();
    auto process_guard = run_process_state_.Lock();
    ApplyPerfCounterCommand(
        command, nodes, process_guard,
        transaction_backend_ ? &*transaction_backend_ : nullptr);
  }

  void SampleNowForTest() {
    auto mutation_lock = AcquireInstrumentationTimedLock(
        node_mutation_mutex_, std::nullopt, {}, "node mutation boundary");
    const std::shared_ptr<LiveInstrumentationRecord> record =
        CurrentActiveRecordForTest();
    auto sampling_lock = AcquireInstrumentationTimedLock(
        record->sampling_boundary, std::nullopt, {}, "sampling boundary");
    RequireSameActiveRecord(record);
    CaptureRound(*record, {});
  }

  void ExpireNowForTest() {
    std::shared_ptr<LiveInstrumentationRecord> record;
    {
      auto mutation_lock = AcquireInstrumentationTimedLock(
          node_mutation_mutex_, std::nullopt, {}, "node mutation boundary");
      record = CurrentActiveRecordForTest();
      auto sampling_lock = AcquireInstrumentationTimedLock(
          record->sampling_boundary, std::nullopt, {}, "sampling boundary");
      RequireSameActiveRecord(record);
      std::lock_guard<std::mutex> record_lock(record->mutex);
      record->deadline = std::chrono::steady_clock::now();
      record->changed.notify_all();
    }
    {
      std::unique_lock<std::mutex> record_lock(record->mutex);
      if (!record->changed.wait_for(
              record_lock, std::chrono::seconds(5), [&record] {
                return IsTerminalLiveInstrumentationState(record->state);
              })) {
        throw std::runtime_error(
            "instrumentation worker did not process forced expiry");
      }
    }
    JoinLiveInstrumentationWorker(record);
  }

  void SetExpiredWithoutWorkerWakeForTest() {
    auto lifecycle_lock = AcquireInstrumentationTimedLock(
        lifecycle_mutex_, std::nullopt, {}, "instrumentation lifecycle");
    auto mutation_lock = AcquireInstrumentationTimedLock(
        node_mutation_mutex_, std::nullopt, {}, "node mutation boundary");
    const std::shared_ptr<LiveInstrumentationRecord> record =
        CurrentActiveRecordForTest();
    auto sampling_lock = AcquireInstrumentationTimedLock(
        record->sampling_boundary, std::nullopt, {}, "sampling boundary");
    RequireSameActiveRecord(record);
    std::lock_guard<std::mutex> record_lock(record->mutex);
    if (record->state != LiveInstrumentationState::kRunning) {
      throw std::logic_error(
          "cannot expire terminal instrumentation for a test");
    }
    record->deadline = std::chrono::steady_clock::now();
  }
#endif

 private:
  boost::json::object Operate(McpOperationKind kind,
                              const boost::json::object& arguments,
                              std::stop_token stop_token) {
    switch (kind) {
      case McpOperationKind::kStartInstrumentation:
        return Start(arguments, stop_token);
      case McpOperationKind::kReconfigureInstrumentation:
        return Reconfigure(arguments, stop_token);
      case McpOperationKind::kStopInstrumentation:
        return Stop(arguments, stop_token);
      default:
        throw std::logic_error("unknown live instrumentation operation");
    }
  }

  boost::json::object Start(const boost::json::object& arguments,
                            std::stop_token stop_token) {
    constexpr std::array<std::string_view, 6U> kAllowedFields = {
        "run_id",   "instrumentation_id", "targets",
        "counters", "sample_interval",    "window"};
    RejectUnsupportedFields(arguments, kAllowedFields, "instrumentation.start");
    std::vector<PerfCounterTarget> targets =
        ParseLiveInstrumentationTargets(arguments);
    std::vector<PerfCounterKind> counters =
        ParseLiveInstrumentationCounters(arguments);
    const std::chrono::milliseconds sample_interval =
        OptionalLiveInstrumentationDuration(arguments, "sample_interval")
            .value_or(options_.metrics_interval);
    const std::optional<std::chrono::milliseconds> window =
        OptionalLiveInstrumentationDuration(arguments, "window");
    ValidateLiveInstrumentationDuration(sample_interval, "sample_interval");
    if (window) {
      ValidateLiveInstrumentationDuration(*window, "window");
    }
    std::optional<std::string> requested_id;
    if (arguments.if_contains("instrumentation_id") != nullptr) {
      requested_id = RequireLiveInstrumentationId(arguments);
    }

    auto lifecycle_lock = AcquireInstrumentationTimedLock(
        lifecycle_mutex_, std::nullopt, stop_token,
        "instrumentation lifecycle");
    ThrowIfStopRequested(stop_token);

    auto record = std::make_shared<LiveInstrumentationRecord>();
    {
      std::lock_guard<std::mutex> lock(registry_->mutex);
      RequireOperationalRegistry();
      if (!registry_->active_id.empty()) {
        throw McpOperationFailure(
            "instrumentation_already_active",
            "only one instrumentation window may be active per run", true);
      }
      if (registry_->retained_configuration_revisions >=
          kMaximumRetainedInstrumentationConfigurationRevisions) {
        ThrowLiveInstrumentationConfigurationHistoryCapacity();
      }
      if (registry_->records.size() >= kMaximumScenarioActionCount) {
        throw McpOperationFailure(
            "instrumentation_history_capacity",
            "instrumentation history reached its bounded run capacity", false);
      }
      if (requested_id) {
        if (registry_->records.contains(*requested_id)) {
          throw McpOperationFailure(
              "instrumentation_id_conflict",
              "instrumentation_id is already retained: " + *requested_id,
              false);
        }
        record->id = *requested_id;
      } else {
        while (true) {
          if (registry_->next_id == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error(
                "instrumentation identity sequence exceeds uint64");
          }
          const std::string candidate =
              "instrumentation-" + std::to_string(registry_->next_id++);
          if (!registry_->records.contains(candidate)) {
            record->id = candidate;
            break;
          }
        }
      }
    }
    record->targets = std::move(targets);
    record->counters = std::move(counters);
    record->sample_interval = sample_interval;
    record->window = window;
    record->configuration_history.reserve(
        kMaximumRetainedInstrumentationConfigurationRevisions);
    record->configuration_history.push_back(LiveInstrumentationConfiguration{
        .configuration_revision = record->configuration_revision,
        .targets = record->targets,
        .counters = record->counters,
        .sample_interval = record->sample_interval,
    });
    RequireLiveInstrumentationConfigurationHistoryConsistent(*record);

    const bool persistence_may_be_active = !metrics_path_.empty();
    std::unique_lock<std::timed_mutex> mutation_lock;
    std::vector<NodePerfCounterSnapshot> snapshots;
    bool published = false;
    try {
      PersistRetainedStateLocked(record->id);
      mutation_lock =
          AcquireInstrumentationTimedLock(node_mutation_mutex_, std::nullopt,
                                          stop_token, "node mutation boundary");
      {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        RequireOperationalRegistry();
        if (!registry_->active_id.empty() ||
            registry_->records.contains(record->id)) {
          throw McpOperationFailure(
              "instrumentation_admission_conflict",
              "instrumentation admission changed before mutation", true);
        }
      }
      RuntimeNodeSnapshot nodes = node_inventory_.Snapshot();
      const RuntimeWalletSnapshot wallets = wallet_registry_.Snapshot();
      ValidateLiveInstrumentationMappings(record->targets, nodes, wallets,
                                          options_);
      const auto configurations =
          LiveInstrumentationConfigurations(record->targets, record->counters);
      std::vector<NodePerfCounterAssignment> assignments =
          ResolvePerfCounterAssignments(configurations, nodes, true, true);
      {
        auto process_guard = run_process_state_.Lock();
        snapshots = ReplaceNodePerfCountersTransactional(
            assignments, process_guard, stop_token,
            transaction_backend_ ? &*transaction_backend_ : nullptr);
      }
      for (const NodePerfCounterSnapshot& snapshot : snapshots) {
        record->baseline_configurations.emplace(
            snapshot.node->config.id,
            SnapshotPerfCounterConfiguration(snapshot));
      }
      CaptureRound(*record, stop_token);
      ThrowIfStopRequested(stop_token);

      const auto committed_at = std::chrono::steady_clock::now();
      record->started_at = committed_at;
      record->started_at_ms = NowUnixMillis();
      if (window) {
        record->deadline = SteadyDeadline(committed_at, *window);
      }
      boost::json::object result = LiveInstrumentationResultJson(*record);

      {
        std::unique_lock<std::mutex> worker_lock(
            record->worker_lifecycle_mutex);
        std::lock_guard<std::mutex> lock(registry_->mutex);
        RequireOperationalRegistry();
        if (!registry_->active_id.empty() ||
            registry_->records.contains(record->id)) {
          throw McpOperationFailure(
              "instrumentation_admission_conflict",
              "instrumentation admission changed before publication", true);
        }
        if (registry_->retained_configuration_revisions >=
            kMaximumRetainedInstrumentationConfigurationRevisions) {
          ThrowLiveInstrumentationConfigurationHistoryCapacity();
        }
        const auto [inserted_record, inserted] =
            registry_->records.emplace(record->id, record);
        if (!inserted) {
          throw std::logic_error(
              "instrumentation publication lost its identity reservation");
        }
        try {
          registry_->active_id = record->id;
          record->worker =
              std::jthread([this, record](std::stop_token worker_stop_token) {
                RunWorker(record, worker_stop_token);
              });
          ++registry_->retained_configuration_revisions;
          published = true;
        } catch (...) {
          registry_->active_id.clear();
          registry_->records.erase(inserted_record);
          throw;
        }
      }
      return result;
    } catch (...) {
      RequestStopLiveInstrumentationWorker(record);
      record->changed.notify_all();
      JoinLiveInstrumentationWorker(record);
      if (published) {
        std::lock_guard<std::mutex> lock(registry_->mutex);
        if (registry_->active_id == record->id) {
          registry_->active_id.clear();
        }
        registry_->records.erase(record->id);
      }
      RollBackNodePerfCounterSnapshots(snapshots, run_process_state_);
      if (mutation_lock.owns_lock()) {
        mutation_lock.unlock();
      }
      if (persistence_may_be_active) {
        try {
          PersistRetainedStateLocked(std::nullopt);
        } catch (...) {
        }
      }
      throw;
    }
  }

  boost::json::object Reconfigure(const boost::json::object& arguments,
                                  std::stop_token stop_token) {
    constexpr std::array<std::string_view, 5U> kAllowedFields = {
        "run_id", "instrumentation_id", "targets", "counters",
        "sample_interval"};
    RejectUnsupportedFields(arguments, kAllowedFields,
                            "instrumentation.reconfigure");
    const std::string instrumentation_id =
        RequireLiveInstrumentationId(arguments);
    std::vector<PerfCounterTarget> targets =
        ParseLiveInstrumentationTargets(arguments);
    std::vector<PerfCounterKind> counters =
        ParseLiveInstrumentationCounters(arguments);
    const std::optional<std::chrono::milliseconds> requested_interval =
        OptionalLiveInstrumentationDuration(arguments, "sample_interval");
    if (requested_interval) {
      ValidateLiveInstrumentationDuration(*requested_interval,
                                          "sample_interval");
    }

    auto lifecycle_lock = AcquireInstrumentationTimedLock(
        lifecycle_mutex_, std::nullopt, stop_token,
        "instrumentation lifecycle");
    auto mutation_lock =
        AcquireInstrumentationTimedLock(node_mutation_mutex_, std::nullopt,
                                        stop_token, "node mutation boundary");
    const std::shared_ptr<LiveInstrumentationRecord> record =
        RequireActiveRecord(instrumentation_id);
    auto sampling_lock =
        AcquireInstrumentationTimedLock(record->sampling_boundary, std::nullopt,
                                        stop_token, "sampling boundary");
    RequireSameActiveRecord(record);
    ThrowIfStopRequested(stop_token);
    const auto expire_at_boundary = [&] {
      RequestStopLiveInstrumentationWorker(record);
      record->changed.notify_all();
      CompleteAtSamplingBoundary(record, LiveInstrumentationState::kSucceeded,
                                 std::nullopt);
      std::optional<std::string> restoration_failure;
      {
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (record->state == LiveInstrumentationState::kFailed) {
          restoration_failure = record->failure.value_or(
              "instrumentation expiry could not restore the pre-window "
              "configuration");
        }
      }
      sampling_lock.unlock();
      mutation_lock.unlock();
      JoinLiveInstrumentationWorker(record);
      PersistTerminalStateLocked(record);
      if (restoration_failure) {
        throw McpOperationFailure("instrumentation_restoration_failed",
                                  *restoration_failure, true);
      }
      throw McpOperationFailure(
          "instrumentation_not_active",
          "instrumentation window expired before reconfiguration", false);
    };

    std::vector<PerfCounterTarget> prior_targets;
    std::vector<PerfCounterKind> prior_counters;
    std::map<std::string, NodePerfCounterConfiguration, std::less<>>
        prior_baseline;
    std::chrono::milliseconds sample_interval;
    std::uint64_t next_revision = 0U;
    std::uint64_t sample_count = 0U;
    bool deadline_reached = false;
    {
      std::lock_guard<std::mutex> registry_lock(registry_->mutex);
      RequireOperationalRegistry();
      const auto active = registry_->records.find(record->id);
      if (registry_->active_id != record->id ||
          active == registry_->records.end() || active->second != record) {
        throw McpOperationFailure(
            "instrumentation_admission_conflict",
            "instrumentation admission changed before reconfiguration", true);
      }
      std::lock_guard<std::mutex> record_lock(record->mutex);
      if (record->state != LiveInstrumentationState::kRunning) {
        throw McpOperationFailure(
            "instrumentation_not_active",
            "terminal instrumentation cannot be reconfigured: " +
                instrumentation_id,
            false);
      }
      deadline_reached = record->deadline &&
                         std::chrono::steady_clock::now() >= *record->deadline;
      if (deadline_reached) {
        record->changed.notify_all();
      } else {
        RequireLiveInstrumentationConfigurationHistoryConsistent(*record);
        if (registry_->retained_configuration_revisions >=
                kMaximumRetainedInstrumentationConfigurationRevisions ||
            record->configuration_history.size() >=
                kMaximumRetainedInstrumentationConfigurationRevisions) {
          ThrowLiveInstrumentationConfigurationHistoryCapacity();
        }
        if (record->configuration_revision ==
            std::numeric_limits<std::uint64_t>::max()) {
          throw std::overflow_error(
              "instrumentation configuration revision exceeds uint64");
        }
        prior_targets = record->targets;
        prior_counters = record->counters;
        prior_baseline = record->baseline_configurations;
        sample_interval = requested_interval.value_or(record->sample_interval);
        next_revision = record->configuration_revision + 1U;
        sample_count = record->sample_count;
        record->configuration_history.reserve(
            kMaximumRetainedInstrumentationConfigurationRevisions);
      }
    }
    if (deadline_reached) {
      expire_at_boundary();
    }
    boost::json::object result{
        {"instrumentation_id", record->id},
        {"state",
         LiveInstrumentationStateName(LiveInstrumentationState::kRunning)},
        {"sample_count", sample_count},
        {"targets", LiveInstrumentationTargetsJson(targets)},
    };
    LiveInstrumentationConfiguration next_configuration{
        .configuration_revision = next_revision,
        .targets = targets,
        .counters = counters,
        .sample_interval = sample_interval,
    };

    RuntimeNodeSnapshot nodes = node_inventory_.Snapshot();
    const RuntimeWalletSnapshot wallets = wallet_registry_.Snapshot();
    ValidateLiveInstrumentationMappings(targets, nodes, wallets, options_);
    const auto prior_configurations =
        LiveInstrumentationConfigurations(prior_targets, prior_counters);
    const auto next_configurations =
        LiveInstrumentationConfigurations(targets, counters);

    std::vector<NodePerfCounterAssignment> assignments;
    assignments.reserve(prior_configurations.size() +
                        next_configurations.size());
    for (const auto& [node_id, configuration] : next_configurations) {
      assignments.push_back(NodePerfCounterAssignment{
          .node = &FindNodeRuntimeById(nodes, node_id),
          .configuration = configuration,
          .require_running = true,
          .require_attachment = true,
      });
    }
    for (const auto& [node_id, configuration] : prior_configurations) {
      static_cast<void>(configuration);
      if (next_configurations.contains(node_id)) {
        continue;
      }
      const auto baseline = prior_baseline.find(node_id);
      if (baseline == prior_baseline.end()) {
        throw std::logic_error(
            "instrumentation baseline is missing an active node");
      }
      assignments.push_back(NodePerfCounterAssignment{
          .node = &FindNodeRuntimeById(nodes, node_id),
          .configuration = baseline->second,
          .require_running = false,
          .require_attachment = true,
      });
    }

    std::vector<NodePerfCounterSnapshot> snapshots;
    try {
      {
        auto process_guard = run_process_state_.Lock();
        snapshots = ReplaceNodePerfCountersTransactional(
            assignments, process_guard, stop_token,
            transaction_backend_ ? &*transaction_backend_ : nullptr);
      }
      std::map<std::string, NodePerfCounterConfiguration, std::less<>>
          next_baseline = prior_baseline;
      for (const auto& [node_id, configuration] : prior_configurations) {
        static_cast<void>(configuration);
        if (!next_configurations.contains(node_id)) {
          next_baseline.erase(node_id);
        }
      }
      for (const NodePerfCounterSnapshot& snapshot : snapshots) {
        const std::string& node_id = snapshot.node->config.id;
        if (!prior_configurations.contains(node_id)) {
          next_baseline.emplace(node_id,
                                SnapshotPerfCounterConfiguration(snapshot));
        }
      }
      ThrowIfStopRequested(stop_token);
      {
        std::lock_guard<std::mutex> registry_lock(registry_->mutex);
        RequireOperationalRegistry();
        const auto active = registry_->records.find(record->id);
        if (registry_->active_id != record->id ||
            active == registry_->records.end() || active->second != record) {
          throw McpOperationFailure(
              "instrumentation_admission_conflict",
              "instrumentation admission changed during reconfiguration", true);
        }
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (record->state != LiveInstrumentationState::kRunning) {
          throw McpOperationFailure(
              "instrumentation_not_active",
              "instrumentation became terminal during reconfiguration", true);
        }
        deadline_reached =
            record->deadline &&
            std::chrono::steady_clock::now() >= *record->deadline;
        if (!deadline_reached) {
          RequireLiveInstrumentationConfigurationHistoryConsistent(*record);
          if (registry_->retained_configuration_revisions >=
                  kMaximumRetainedInstrumentationConfigurationRevisions ||
              record->configuration_history.size() >=
                  kMaximumRetainedInstrumentationConfigurationRevisions) {
            ThrowLiveInstrumentationConfigurationHistoryCapacity();
          }
          if (record->configuration_history.capacity() <
              kMaximumRetainedInstrumentationConfigurationRevisions) {
            throw std::logic_error(
                "instrumentation configuration history lost its reserved "
                "capacity");
          }
          record->configuration_history.push_back(
              std::move(next_configuration));
          record->targets.swap(targets);
          record->counters.swap(counters);
          record->baseline_configurations.swap(next_baseline);
          record->sample_interval = sample_interval;
          record->configuration_revision = next_revision;
          ++registry_->retained_configuration_revisions;
          record->changed.notify_all();
        }
      }
      if (deadline_reached) {
        RollBackNodePerfCounterSnapshots(snapshots, run_process_state_);
        snapshots.clear();
        expire_at_boundary();
      }
    } catch (...) {
      RollBackNodePerfCounterSnapshots(snapshots, run_process_state_);
      throw;
    }
    return result;
  }

  boost::json::object Stop(const boost::json::object& arguments,
                           std::stop_token stop_token) {
    constexpr std::array<std::string_view, 3U> kAllowedFields = {
        "run_id", "instrumentation_id", "timeout_sec"};
    RejectUnsupportedFields(arguments, kAllowedFields, "instrumentation.stop");
    const std::string instrumentation_id =
        RequireLiveInstrumentationId(arguments);
    const std::uint32_t timeout_sec =
        JsonOptionalUint32Field(arguments, "timeout_sec", 60U);
    if (timeout_sec == 0U || timeout_sec > 3600U) {
      throw std::invalid_argument(
          "instrumentation.stop timeout_sec must be in 1..3600");
    }
    const auto deadline =
        SteadyDeadline(std::chrono::steady_clock::now(),
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::seconds(timeout_sec)));

    auto lifecycle_lock = AcquireInstrumentationTimedLock(
        lifecycle_mutex_, deadline, stop_token, "instrumentation lifecycle");
    std::shared_ptr<LiveInstrumentationRecord> record;
    {
      auto mutation_lock = AcquireInstrumentationTimedLock(
          node_mutation_mutex_, deadline, stop_token, "node mutation boundary");
      record = RequireActiveRecord(instrumentation_id);
      auto sampling_lock = AcquireInstrumentationTimedLock(
          record->sampling_boundary, deadline, stop_token, "sampling boundary");
      RequireSameActiveRecord(record);
      ThrowIfStopRequested(stop_token);

      LiveInstrumentationState terminal_state;
      {
        std::lock_guard<std::mutex> record_lock(record->mutex);
        terminal_state = record->state == LiveInstrumentationState::kFailed
                             ? LiveInstrumentationState::kFailed
                             : LiveInstrumentationState::kSucceeded;
      }
      RequestStopLiveInstrumentationWorker(record);
      record->changed.notify_all();
      try {
        RestoreBaseline(*record, {});
        MarkRestored(record, terminal_state, std::nullopt);
      } catch (const std::exception& error) {
        MarkRestorationFailed(
            record,
            "instrumentation.stop could not restore the pre-window "
            "configuration: " +
                std::string(error.what()));
      } catch (...) {
        MarkRestorationFailed(
            record,
            "instrumentation.stop could not restore the pre-window "
            "configuration");
      }
    }
    JoinLiveInstrumentationWorker(record);
    PersistTerminalStateLocked(record);
    return LiveInstrumentationResultJson(*record);
  }

  boost::json::value Read(McpInformationFamily family,
                          std::stop_token stop_token) const {
    ThrowIfStopRequested(stop_token);
    std::string active_id;
    std::vector<std::shared_ptr<LiveInstrumentationRecord>> records;
    {
      std::lock_guard<std::mutex> lock(registry_->mutex);
      active_id = registry_->active_id;
      records.reserve(registry_->records.size());
      for (const auto& [id, record] : registry_->records) {
        static_cast<void>(id);
        records.push_back(record);
      }
    }
    if (family == McpInformationFamily::kInstrumentation) {
      boost::json::array result;
      if (!active_id.empty()) {
        const auto active = std::find_if(records.begin(), records.end(),
                                         [&active_id](const auto& record) {
                                           return record->id == active_id;
                                         });
        if (active == records.end()) {
          throw std::logic_error(
              "active instrumentation record is not retained");
        }
        result.emplace_back(LiveInstrumentationResourceJson(**active, false));
      }
      return result;
    }
    if (family == McpInformationFamily::kMeasurements) {
      if (active_id.empty()) {
        return boost::json::object{
            {"instrumentation_id", nullptr},
            {"sample_count", 0U},
            {"retained_measurement_count", 0U},
            {"measurements_truncated", false},
            {"dropped_measurement_count", 0U},
            {"measurement_records", boost::json::array{}},
        };
      }
      const auto active = std::find_if(
          records.begin(), records.end(),
          [&active_id](const auto& record) { return record->id == active_id; });
      if (active == records.end()) {
        throw std::logic_error("active instrumentation record is not retained");
      }
      return LiveInstrumentationResourceJson(**active, true);
    }
    if (family == McpInformationFamily::kMeasurementHistory) {
      boost::json::array result;
      for (const std::shared_ptr<LiveInstrumentationRecord>& record : records) {
        ThrowIfStopRequested(stop_token);
        if (record->id == active_id) {
          continue;
        }
        {
          std::lock_guard<std::mutex> lock(record->mutex);
          if (!IsTerminalLiveInstrumentationState(record->state)) {
            continue;
          }
        }
        result.emplace_back(LiveInstrumentationResourceJson(*record, true));
      }
      return result;
    }
    throw std::logic_error("unknown live instrumentation resource");
  }

  void RequireOperationalRegistry() const {
    if (registry_->shutting_down) {
      throw McpOperationFailure(
          "instrumentation_service_stopping",
          "instrumentation service is stopping with its run", true);
    }
  }

  std::shared_ptr<LiveInstrumentationRecord> RequireActiveRecord(
      std::string_view instrumentation_id) const {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    RequireOperationalRegistry();
    if (registry_->active_id != instrumentation_id) {
      throw McpOperationFailure("instrumentation_not_active",
                                "instrumentation is not the active window: " +
                                    std::string(instrumentation_id),
                                false);
    }
    const auto found = registry_->records.find(registry_->active_id);
    if (found == registry_->records.end()) {
      throw std::logic_error("active instrumentation record is not retained");
    }
    return found->second;
  }

  void RequireSameActiveRecord(
      const std::shared_ptr<LiveInstrumentationRecord>& record) const {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    RequireOperationalRegistry();
    const auto found = registry_->records.find(record->id);
    if (registry_->active_id != record->id ||
        found == registry_->records.end() || found->second != record) {
      throw McpOperationFailure(
          "instrumentation_admission_conflict",
          "instrumentation admission changed at the sampling boundary", true);
    }
  }

  bool IsActiveRecord(
      const std::shared_ptr<LiveInstrumentationRecord>& record) const {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    const auto found = registry_->records.find(record->id);
    return registry_->active_id == record->id &&
           found != registry_->records.end() && found->second == record;
  }

#ifdef BBP_ENABLE_TEST_HOOKS
  std::shared_ptr<LiveInstrumentationRecord> CurrentActiveRecordForTest()
      const {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    if (registry_->active_id.empty()) {
      throw std::logic_error("no active instrumentation record");
    }
    const auto found = registry_->records.find(registry_->active_id);
    if (found == registry_->records.end()) {
      throw std::logic_error("active instrumentation record is not retained");
    }
    return found->second;
  }
#endif

  std::set<std::string> SelectedNodeIds(
      const LiveInstrumentationRecord& record) const {
    std::lock_guard<std::mutex> lock(record.mutex);
    std::set<std::string> selected;
    for (const PerfCounterTarget& target : record.targets) {
      selected.insert(target.node_ids.begin(), target.node_ids.end());
    }
    return selected;
  }

  void CaptureRound(LiveInstrumentationRecord& record,
                    std::stop_token stop_token) {
    const std::set<std::string> selected = SelectedNodeIds(record);
    if (measurement_collector_) {
      std::vector<std::string> measurements =
          measurement_collector_(selected, stop_token);
      ThrowIfStopRequested(stop_token);
      AppendLiveInstrumentationRound(record, std::move(measurements));
      return;
    }
    RuntimeNodeSnapshot nodes = node_inventory_.Snapshot();
    const RuntimeWalletSnapshot wallets = wallet_registry_.Snapshot();
    std::vector<std::string> measurements;
    WriteMetricsSnapshot(
        metrics_path_, options_, driver_, nodes, run_process_state_,
        metrics_synchronization_,
        [&](const NodeRuntime& node, std::string_view error) {
          boost::json::object detail;
          {
            std::lock_guard<std::mutex> lock(record.mutex);
            detail["instrumentation_id"] = record.id;
            detail["sample"] = record.sample_count + 1U;
          }
          detail["error"] = error;
          WriteEvent(events_path_, options_.run_id, node.config.id,
                     SimulationEventKind::kMetricsNodeUnavailable,
                     boost::json::serialize(detail));
          BBP_LOG(warning) << "instrumentation metrics skipped "
                           << node.config.id << ": " << error;
        },
        {}, stop_token, &wallets.registry().topology(), &selected,
        [&](const NodeRuntime&, std::string_view measurement) {
          measurements.emplace_back(measurement);
        });
    ThrowIfStopRequested(stop_token);
    AppendLiveInstrumentationRound(record, std::move(measurements));
  }

  void RestoreBaseline(LiveInstrumentationRecord& record,
                       std::stop_token stop_token) {
    std::map<std::string, NodePerfCounterConfiguration, std::less<>> baseline;
    {
      std::lock_guard<std::mutex> lock(record.mutex);
      baseline = record.baseline_configurations;
    }
    RuntimeNodeSnapshot nodes = node_inventory_.Snapshot();
    std::vector<NodePerfCounterAssignment> assignments =
        ResolvePerfCounterAssignments(baseline, nodes, false, true);
    auto process_guard = run_process_state_.Lock();
    static_cast<void>(ReplaceNodePerfCountersTransactional(
        assignments, process_guard, stop_token,
        transaction_backend_ ? &*transaction_backend_ : nullptr));
  }

  void PersistRetainedStateLocked(
      std::optional<std::string_view> active_instrumentation_id) {
    if (metrics_path_.empty()) {
      return;
    }
    std::vector<std::shared_ptr<LiveInstrumentationRecord>> records;
    {
      std::lock_guard<std::mutex> registry_lock(registry_->mutex);
      records.reserve(registry_->records.size());
      for (const auto& [id, record] : registry_->records) {
        static_cast<void>(id);
        records.push_back(record);
      }
    }
    BoundLiveInstrumentationPersistenceMeasurements(records);
    boost::json::array history;
    history.reserve(records.size());
    for (const std::shared_ptr<LiveInstrumentationRecord>& record : records) {
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (!IsTerminalLiveInstrumentationState(record->state)) {
          continue;
        }
      }
      history.emplace_back(LiveInstrumentationResourceJson(*record, true));
    }
    WriteRetainedInstrumentationHistory(metrics_path_.parent_path(),
                                        options_.run_id, history,
                                        active_instrumentation_id);
  }

  void MarkRestored(const std::shared_ptr<LiveInstrumentationRecord>& record,
                    LiveInstrumentationState state,
                    std::optional<std::string> failure) {
    std::lock_guard<std::mutex> registry_lock(registry_->mutex);
    const auto found = registry_->records.find(record->id);
    if (registry_->active_id != record->id ||
        found == registry_->records.end() || found->second != record) {
      throw std::logic_error(
          "restored instrumentation record is no longer active");
    }
    std::lock_guard<std::mutex> record_lock(record->mutex);
    if (record->state == LiveInstrumentationState::kFailed) {
      state = LiveInstrumentationState::kFailed;
    }
    record->state = state;
    if (failure) {
      record->failure = std::move(failure);
    }
    record->completed_at_ms = NowUnixMillis();
    registry_->active_id.clear();
    record->changed.notify_all();
  }

  void MarkRestorationFailed(
      const std::shared_ptr<LiveInstrumentationRecord>& record,
      std::string failure) noexcept {
    try {
      std::lock_guard<std::mutex> lock(record->mutex);
      record->state = LiveInstrumentationState::kFailed;
      record->completed_at_ms.reset();
      if (record->failure) {
        *record->failure += "; " + failure;
      } else {
        record->failure = std::move(failure);
      }
      record->changed.notify_all();
    } catch (...) {
    }
  }

  void MarkPersistenceFailed(
      const std::shared_ptr<LiveInstrumentationRecord>& record,
      std::string failure) noexcept {
    try {
      std::lock_guard<std::mutex> lock(record->mutex);
      record->state = LiveInstrumentationState::kFailed;
      if (record->failure) {
        *record->failure += "; retained history persistence failed: " + failure;
      } else {
        record->failure =
            "retained history persistence failed: " + std::move(failure);
      }
      record->changed.notify_all();
    } catch (...) {
    }
  }

  std::optional<std::string> ActiveInstrumentationIdForPersistence() const {
    std::lock_guard<std::mutex> lock(registry_->mutex);
    if (registry_->active_id.empty()) {
      return std::nullopt;
    }
    return registry_->active_id;
  }

  void PersistTerminalStateLocked(
      const std::shared_ptr<LiveInstrumentationRecord>& record) noexcept {
    const std::optional<std::string> active_id =
        ActiveInstrumentationIdForPersistence();
    const auto persist = [&] {
      PersistRetainedStateLocked(
          active_id ? std::optional<std::string_view>(*active_id)
                    : std::nullopt);
    };
    try {
      persist();
      return;
    } catch (const std::exception& error) {
      MarkPersistenceFailed(record, error.what());
    } catch (...) {
      MarkPersistenceFailed(record, "unknown persistence failure");
    }
    try {
      persist();
    } catch (...) {
    }
  }

  void CheckpointFailedTerminalState(
      const std::shared_ptr<LiveInstrumentationRecord>& record,
      std::stop_token stop_token) noexcept {
    try {
      auto lifecycle_lock = AcquireInstrumentationTimedLock(
          lifecycle_mutex_, std::nullopt, stop_token,
          "instrumentation lifecycle");
      PersistTerminalStateLocked(record);
    } catch (...) {
    }
  }

  void CompleteAtSamplingBoundary(
      const std::shared_ptr<LiveInstrumentationRecord>& record,
      LiveInstrumentationState state,
      std::optional<std::string> failure) noexcept {
    try {
      RestoreBaseline(*record, {});
      MarkRestored(record, state, failure);
    } catch (const std::exception& error) {
      std::string detail =
          failure.value_or("instrumentation completion failed");
      detail += "; pre-window configuration restoration failed: " +
                std::string(error.what());
      MarkRestorationFailed(record, std::move(detail));
    } catch (...) {
      std::string detail =
          failure.value_or("instrumentation completion failed");
      detail += "; pre-window configuration restoration failed";
      MarkRestorationFailed(record, std::move(detail));
    }
  }

  void CompleteFromWorker(
      const std::shared_ptr<LiveInstrumentationRecord>& record,
      LiveInstrumentationState state, std::optional<std::string> failure,
      std::stop_token worker_stop_token) noexcept {
    try {
      auto lifecycle_lock = AcquireInstrumentationTimedLock(
          lifecycle_mutex_, std::nullopt, worker_stop_token,
          "instrumentation lifecycle");
      auto mutation_lock = AcquireInstrumentationTimedLock(
          node_mutation_mutex_, std::nullopt, worker_stop_token,
          "node mutation boundary");
      auto sampling_lock = AcquireInstrumentationTimedLock(
          record->sampling_boundary, std::nullopt, worker_stop_token,
          "sampling boundary");
      if (!IsActiveRecord(record)) {
        return;
      }
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (record->state != LiveInstrumentationState::kRunning) {
          return;
        }
      }
      CompleteAtSamplingBoundary(record, state, std::move(failure));
      sampling_lock.unlock();
      mutation_lock.unlock();
      PersistTerminalStateLocked(record);
    } catch (const SimulationCancelled&) {
    } catch (const std::exception& error) {
      if (!worker_stop_token.stop_requested()) {
        MarkRestorationFailed(
            record,
            "instrumentation worker could not enter its restoration "
            "boundary: " +
                std::string(error.what()));
        CheckpointFailedTerminalState(record, worker_stop_token);
      }
    } catch (...) {
      if (!worker_stop_token.stop_requested()) {
        MarkRestorationFailed(
            record,
            "instrumentation worker could not enter its restoration boundary");
        CheckpointFailedTerminalState(record, worker_stop_token);
      }
    }
  }

  void RunWorker(const std::shared_ptr<LiveInstrumentationRecord>& record,
                 std::stop_token worker_stop_token) noexcept {
    try {
      std::uint64_t revision;
      std::chrono::steady_clock::time_point next_sample;
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        revision = record->configuration_revision;
        next_sample =
            SteadyDeadline(record->started_at, record->sample_interval);
      }
      while (!worker_stop_token.stop_requested()) {
        std::optional<std::chrono::steady_clock::time_point> deadline;
        {
          std::unique_lock<std::mutex> lock(record->mutex);
          if (record->state != LiveInstrumentationState::kRunning) {
            return;
          }
          deadline = record->deadline;
          const auto wake_at =
              deadline ? std::min(next_sample, *deadline) : next_sample;
          static_cast<void>(
              record->changed.wait_until(lock, worker_stop_token, wake_at, [&] {
                return record->state != LiveInstrumentationState::kRunning ||
                       record->configuration_revision != revision ||
                       record->deadline != deadline;
              }));
          if (worker_stop_token.stop_requested() ||
              record->state != LiveInstrumentationState::kRunning) {
            return;
          }
          if (record->configuration_revision != revision) {
            revision = record->configuration_revision;
            next_sample = SteadyDeadline(std::chrono::steady_clock::now(),
                                         record->sample_interval);
            continue;
          }
        }

        auto lifecycle_lock = AcquireInstrumentationTimedLock(
            lifecycle_mutex_, std::nullopt, worker_stop_token,
            "instrumentation lifecycle");
        auto mutation_lock = AcquireInstrumentationTimedLock(
            node_mutation_mutex_, std::nullopt, worker_stop_token,
            "node mutation boundary");
        auto sampling_lock = AcquireInstrumentationTimedLock(
            record->sampling_boundary, std::nullopt, worker_stop_token,
            "sampling boundary");
        bool expired = false;
        {
          std::lock_guard<std::mutex> lock(record->mutex);
          if (record->state != LiveInstrumentationState::kRunning) {
            return;
          }
          if (record->configuration_revision != revision) {
            revision = record->configuration_revision;
            next_sample = SteadyDeadline(std::chrono::steady_clock::now(),
                                         record->sample_interval);
            continue;
          }
          expired = record->deadline &&
                    std::chrono::steady_clock::now() >= *record->deadline;
        }
        if (expired) {
          CompleteAtSamplingBoundary(
              record, LiveInstrumentationState::kSucceeded, std::nullopt);
          sampling_lock.unlock();
          mutation_lock.unlock();
          PersistTerminalStateLocked(record);
          return;
        }
        try {
          CaptureRound(*record, worker_stop_token);
        } catch (const SimulationCancelled&) {
          if (worker_stop_token.stop_requested()) {
            return;
          }
          throw;
        } catch (const std::exception& error) {
          CompleteAtSamplingBoundary(
              record, LiveInstrumentationState::kFailed,
              "instrumentation sampling failed: " + std::string(error.what()));
          sampling_lock.unlock();
          mutation_lock.unlock();
          PersistTerminalStateLocked(record);
          return;
        } catch (...) {
          CompleteAtSamplingBoundary(record, LiveInstrumentationState::kFailed,
                                     "instrumentation sampling failed");
          sampling_lock.unlock();
          mutation_lock.unlock();
          PersistTerminalStateLocked(record);
          return;
        }
        {
          std::lock_guard<std::mutex> lock(record->mutex);
          next_sample = SteadyDeadline(std::chrono::steady_clock::now(),
                                       record->sample_interval);
        }
      }
    } catch (const SimulationCancelled&) {
    } catch (const std::exception& error) {
      if (!worker_stop_token.stop_requested()) {
        CompleteFromWorker(
            record, LiveInstrumentationState::kFailed,
            "instrumentation worker failed: " + std::string(error.what()),
            worker_stop_token);
      }
    } catch (...) {
      if (!worker_stop_token.stop_requested()) {
        CompleteFromWorker(record, LiveInstrumentationState::kFailed,
                           "instrumentation worker failed", worker_stop_token);
      }
    }
  }

  const Options& options_;
  std::filesystem::path metrics_path_;
  std::filesystem::path events_path_;
  ChainDriver& driver_;
  RuntimeNodeInventory& node_inventory_;
  RuntimeWalletRegistry& wallet_registry_;
  RunProcessState& run_process_state_;
  std::timed_mutex& node_mutation_mutex_;
  MetricsSnapshotSynchronization metrics_synchronization_;
  std::shared_ptr<LiveInstrumentationRegistry> registry_;
  std::timed_mutex lifecycle_mutex_;
  std::optional<NodePerfCounterTransactionBackend> transaction_backend_;
  LiveInstrumentationMeasurementCollector measurement_collector_;
};

std::shared_ptr<LiveInstrumentationRegistry> MakeLiveInstrumentationRegistry() {
  return std::make_shared<LiveInstrumentationRegistry>();
}

void LiveInstrumentationControllerDeleter::operator()(
    LiveInstrumentationController* controller) const noexcept {
  delete controller;
}

LiveInstrumentationControllerPtr MakeLiveInstrumentationController(
    const Options& options, std::filesystem::path metrics_path,
    std::filesystem::path events_path, ChainDriver& driver,
    RuntimeNodeInventory& node_inventory,
    RuntimeWalletRegistry& wallet_registry, RunProcessState& run_process_state,
    std::timed_mutex& node_mutation_mutex,
    MetricsSnapshotSynchronization metrics_synchronization,
    std::shared_ptr<LiveInstrumentationRegistry> registry,
    std::optional<NodePerfCounterTransactionBackend> transaction_backend,
    LiveInstrumentationMeasurementCollector measurement_collector) {
  return LiveInstrumentationControllerPtr(new LiveInstrumentationController(
      options, std::move(metrics_path), std::move(events_path), driver,
      node_inventory, wallet_registry, run_process_state, node_mutation_mutex,
      metrics_synchronization, std::move(registry),
      std::move(transaction_backend), std::move(measurement_collector)));
}

std::shared_ptr<McpLiveInstrumentationService> MakeLiveInstrumentationService(
    LiveInstrumentationController& controller) {
  return controller.MakeService();
}

void ShutdownLiveInstrumentation(LiveInstrumentationController& controller,
                                 bool run_failed) {
  controller.Shutdown(run_failed);
}

#ifdef BBP_ENABLE_TEST_HOOKS
void ApplyLiveInstrumentationPerfMutationForTest(
    LiveInstrumentationController& controller, std::string_view node_id,
    PerfCounterKind counter) {
  controller.ApplyPerfMutationForTest(node_id, counter);
}

void SampleLiveInstrumentationNowForTest(
    LiveInstrumentationController& controller) {
  controller.SampleNowForTest();
}

void ExpireLiveInstrumentationNowForTest(
    LiveInstrumentationController& controller) {
  controller.ExpireNowForTest();
}

void SetLiveInstrumentationExpiredWithoutWorkerWakeForTest(
    LiveInstrumentationController& controller) {
  controller.SetExpiredWithoutWorkerWakeForTest();
}
#endif

}  // namespace bbp::simulator_app_internal
