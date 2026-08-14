#include "simulator_scheduled_command_event_details.h"

#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <utility>

#include "bbp/chain_kind.h"
#include "bbp/perf_counter.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_partition.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/scheduled_scenario_event.h"
#include "bbp/simulator/workload_kind.h"
#include "bbp/util.h"
#include "simulator_node_lifecycle_event_details.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

boost::json::object ScheduledEventLifecycleDetail(
    const ScheduledScenarioEvent& event,
    std::chrono::milliseconds scheduled_wall_delay,
    std::chrono::steady_clock::time_point epoch,
    std::chrono::steady_clock::time_point started,
    std::optional<std::chrono::steady_clock::time_point> finished,
    std::optional<std::string_view> error) {
  const std::uint64_t scheduled_at_ms =
      static_cast<std::uint64_t>(event.at.count());
  const std::uint64_t scheduled_wall_at_ms =
      static_cast<std::uint64_t>(scheduled_wall_delay.count());
  const std::uint64_t started_at_ms = ElapsedMilliseconds(epoch, started);
  boost::json::object detail;
  detail["sequence"] = event.sequence;
  if (const auto* workload = std::get_if<ScenarioWorkload>(&event.action)) {
    detail["action"] = std::string(WorkloadKindName(workload->kind));
  } else {
    detail["action"] = std::string(SimulationCommandKindName(
        std::get<SimulationCommand>(event.action).kind));
  }
  detail["scheduled_at_ms"] = scheduled_at_ms;
  detail["scheduled_wall_at_ms"] = scheduled_wall_at_ms;
  detail["started_at_ms"] = started_at_ms;
  detail["lateness_ms"] = started_at_ms > scheduled_wall_at_ms
                              ? started_at_ms - scheduled_wall_at_ms
                              : 0U;
  if (finished) {
    const std::uint64_t finished_at_ms = ElapsedMilliseconds(epoch, *finished);
    detail["finished_at_ms"] = finished_at_ms;
    detail["duration_ms"] =
        finished_at_ms > started_at_ms ? finished_at_ms - started_at_ms : 0U;
  }
  if (error) {
    detail["error"] = *error;
  }
  return detail;
}

std::string RestartNodeWorkloadDetail(uint32_t workload_index,
                                      uint32_t workload_count, uint32_t node,
                                      uint64_t restart_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

std::string FreezeNodeWorkloadDetail(uint32_t workload_index,
                                     uint32_t workload_count, uint32_t node,
                                     uint32_t duration_ms) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["duration_ms"] = duration_ms;
  return boost::json::serialize(detail);
}

std::string CheckpointWorkloadDetail(std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     std::string_view name,
                                     std::uint32_t node_metric_samples,
                                     std::uint32_t wallet_metric_samples) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["name"] = name;
  detail["node_metric_samples"] = node_metric_samples;
  detail["wallet_metric_samples"] = wallet_metric_samples;
  detail["total_metric_samples"] =
      static_cast<std::uint64_t>(node_metric_samples) +
      static_cast<std::uint64_t>(wallet_metric_samples);
  return boost::json::serialize(detail);
}

std::string ScheduledBlockDetail(const std::vector<std::string>& hashes) {
  boost::json::object detail;
  boost::json::array block_hashes;
  block_hashes.reserve(hashes.size());
  for (const std::string& hash : hashes) {
    block_hashes.emplace_back(hash);
  }
  detail["hashes"] = std::move(block_hashes);
  return boost::json::serialize(detail);
}

std::filesystem::path NodeReportRelativePath(const SimulationCommand& command) {
  return std::filesystem::path("node-reports") /
         (command.node_id + "-" + std::to_string(command.sequence) + ".json");
}

std::string SimulationCommandDetail(const SimulationCommand& command,
                                    std::string_view error,
                                    const SimulationCommandOutcome* outcome) {
  boost::json::object detail;
  detail["sequence"] = command.sequence;
  detail["kind"] = SimulationCommandKindName(command.kind);
  if (command.block_production_policy) {
    detail["period_ms"] = command.block_production_policy->period().count();
    detail["probability"] = command.block_production_policy->probability();
    detail["seed"] = command.block_production_policy->seed();
  }
  if (command.mining_difficulty) {
    detail["difficulty"] = command.mining_difficulty->value();
  }
  if (command.peer_node_id) {
    detail["peer_node_id"] = *command.peer_node_id;
  }
  if (command.peer_count_policy) {
    detail["minimum_peer_count"] = command.peer_count_policy->minimum();
    detail["maximum_peer_count"] = command.peer_count_policy->maximum();
  }
  if (command.block_count) {
    detail["block_count"] = *command.block_count;
  }
  if (command.profile) {
    detail["profile"] = *command.profile;
  }
  if (command.resource_limit_patch) {
    detail["resource_limits"] =
        ResourceLimitPatchJson(*command.resource_limit_patch);
  }
  if (command.network_condition) {
    detail["network_condition"] =
        NetworkConditionJson(*command.network_condition);
  }
  if (command.network_flow) {
    boost::json::object flow;
    if (!command.network_flow->src_address.empty()) {
      flow["src_address"] = command.network_flow->src_address;
    }
    if (command.network_flow->src_port != 0U) {
      flow["src_port"] = command.network_flow->src_port;
    }
    if (!command.network_flow->dst_address.empty()) {
      flow["dst_address"] = command.network_flow->dst_address;
    }
    if (command.network_flow->dst_port != 0U) {
      flow["dst_port"] = command.network_flow->dst_port;
    }
    if (command.network_flow->handle != 0U) {
      flow["handle"] = command.network_flow->handle;
    }
    detail["network_flow"] = std::move(flow);
  }
  if (command.partition) {
    const auto group_json = [](const SimulationPartitionGroup& group) {
      boost::json::object object;
      boost::json::array group_ids;
      group_ids.reserve(group.group_ids.size());
      for (const std::string& group_id : group.group_ids) {
        group_ids.emplace_back(group_id);
      }
      boost::json::array node_ids;
      node_ids.reserve(group.node_ids.size());
      for (const std::string& node_id : group.node_ids) {
        node_ids.emplace_back(node_id);
      }
      object["group_ids"] = std::move(group_ids);
      object["node_ids"] = std::move(node_ids);
      return object;
    };
    boost::json::object partition;
    partition["scope"] =
        std::string(SimulationPartitionScopeName(command.partition->scope));
    partition["group_a"] = group_json(command.partition->group_a);
    partition["group_b"] = group_json(command.partition->group_b);
    detail["partition"] = std::move(partition);
  }
  if (command.perf_counter_target) {
    boost::json::object target;
    target["kind"] =
        PerfCounterTargetKindName(command.perf_counter_target->kind);
    target["id"] = command.perf_counter_target->id;
    boost::json::array node_ids;
    node_ids.reserve(command.perf_counter_target->node_ids.size());
    for (const std::string& node_id : command.perf_counter_target->node_ids) {
      node_ids.emplace_back(node_id);
    }
    target["node_ids"] = std::move(node_ids);
    detail["perf_target"] = std::move(target);
  }
  if (!command.perf_counter_kinds.empty()) {
    detail["perf_counters"] = PerfCounterNamesJson(command.perf_counter_kinds);
  }
  if (command.wallet_send) {
    boost::json::object send;
    send["sender_wallet_index"] = command.wallet_send->sender_wallet_index;
    send["receiver_wallet_index"] = command.wallet_send->receiver_wallet_index;
    send["amount"] = FormatFixed8Amount(command.wallet_send->amount_satoshis);
    send["amount_satoshis"] = command.wallet_send->amount_satoshis;
    send["fee"] = FormatFixed8Amount(command.wallet_send->fee_satoshis);
    send["fee_satoshis"] = command.wallet_send->fee_satoshis;
    send["timeout_sec"] = command.wallet_send->timeout_sec;
    detail["wallet_send"] = std::move(send);
  }
  if (command.node_add) {
    boost::json::object add;
    add["chain"] = std::string(ChainKindName(command.node_add->chain));
    add["count"] = command.node_add->count;
    boost::json::array node_ids;
    for (const std::string& node_id : command.node_add->node_ids) {
      node_ids.emplace_back(node_id);
    }
    add["node_ids"] = std::move(node_ids);
    add["ready_timeout_sec"] = command.node_add->ready_timeout_sec;
    add["sync_timeout_sec"] = command.node_add->sync_timeout_sec;
    detail["node_add"] = std::move(add);
  }
  if (command.node_replace) {
    boost::json::object replacement;
    replacement["chain"] =
        std::string(ChainKindName(command.node_replace->chain));
    replacement["count"] = command.node_replace->count;
    boost::json::array node_ids;
    for (const std::string& node_id : command.node_replace->node_ids) {
      node_ids.emplace_back(node_id);
    }
    replacement["node_ids"] = std::move(node_ids);
    if (command.node_replace->binary) {
      replacement["binary"] = *command.node_replace->binary;
    }
    if (command.node_replace->resources) {
      replacement["resources"] =
          ResourceLimitPatchJson(*command.node_replace->resources);
    }
    if (command.node_replace->network) {
      replacement["network"] =
          NetworkConditionJson(*command.node_replace->network);
    }
    replacement["ready_timeout_sec"] = command.node_replace->ready_timeout_sec;
    replacement["sync_timeout_sec"] = command.node_replace->sync_timeout_sec;
    detail["node_replace"] = std::move(replacement);
  }
  if (command.node_remove) {
    boost::json::object remove;
    boost::json::array node_ids;
    for (const std::string& node_id : command.node_remove->node_ids) {
      node_ids.emplace_back(node_id);
    }
    remove["node_ids"] = std::move(node_ids);
    remove["timeout_sec"] = command.node_remove->timeout_sec;
    detail["node_remove"] = std::move(remove);
  }
  if (command.role_mutation) {
    boost::json::object mutation;
    boost::json::array node_ids;
    node_ids.reserve(command.role_mutation->node_ids.size());
    for (const std::string& node_id : command.role_mutation->node_ids) {
      node_ids.emplace_back(node_id);
    }
    mutation["node_ids"] = std::move(node_ids);
    mutation["roles"] = boost::json::array{
        std::string(SimulationRoleKindName(command.role_mutation->role))};
    if (command.role_mutation->mode) {
      mutation["mode"] =
          std::string(WalletPrivacyModeName(*command.role_mutation->mode));
    }
    if (command.role_mutation->funding_wallet_id) {
      mutation["funding_wallet_id"] = *command.role_mutation->funding_wallet_id;
    }
    if (command.role_mutation->timeout_sec) {
      mutation["timeout_sec"] = *command.role_mutation->timeout_sec;
    }
    detail["role_mutation_request"] = std::move(mutation);
  }
  if (outcome != nullptr &&
      (command.kind == SimulationCommandKind::kAddNodes ||
       command.kind == SimulationCommandKind::kReplaceNode ||
       command.kind == SimulationCommandKind::kRemoveNodes)) {
    boost::json::array added_node_ids;
    added_node_ids.reserve(outcome->added_node_ids.size());
    for (const std::string& node_id : outcome->added_node_ids) {
      added_node_ids.emplace_back(node_id);
    }
    detail["added_node_ids"] = std::move(added_node_ids);
    boost::json::array removed_node_ids;
    removed_node_ids.reserve(outcome->removed_node_ids.size());
    for (const std::string& node_id : outcome->removed_node_ids) {
      removed_node_ids.emplace_back(node_id);
    }
    detail["removed_node_ids"] = std::move(removed_node_ids);
    if (outcome->inventory_generation) {
      detail["inventory_generation"] = *outcome->inventory_generation;
    }
    if (outcome->final_node_count) {
      detail["final_node_count"] = *outcome->final_node_count;
    }
  }
  if (outcome != nullptr && outcome->role_mutation) {
    detail["role_mutation"] = *outcome->role_mutation;
  }
  if (command.kind == SimulationCommandKind::kExportNodeReport) {
    detail["output_path"] = NodeReportRelativePath(command).generic_string();
  }
  detail["confirmed"] = command.confirmed;
  if (command.scheduled_event_sequence) {
    detail["scheduled_event_sequence"] = *command.scheduled_event_sequence;
  }
  if (!error.empty()) {
    detail["error"] = error;
  }
  return boost::json::serialize(detail);
}

}  // namespace bbp::simulator_app_internal
