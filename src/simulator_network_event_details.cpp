#include "simulator_network_event_details.h"

#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <utility>

#include "bbp/network.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/profile_switch_workload.h"
#include "bbp/simulator/workload_kind.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

boost::json::object QdiscJson(const QdiscInfo& qdisc) {
  boost::json::object qdisc_json;
  qdisc_json["if_index"] = qdisc.if_index;
  qdisc_json["if_name"] = qdisc.if_name;
  qdisc_json["kind"] = qdisc.kernel_kind;
  qdisc_json["handle"] = qdisc.handle;
  qdisc_json["parent"] = qdisc.parent;
  qdisc_json["info"] = qdisc.info;
  qdisc_json["has_stats"] = qdisc.has_stats;
  qdisc_json["bytes"] = qdisc.bytes;
  qdisc_json["packets"] = qdisc.packets;
  qdisc_json["drops"] = qdisc.drops;
  qdisc_json["overlimits"] = qdisc.overlimits;
  qdisc_json["qlen"] = qdisc.qlen;
  qdisc_json["backlog"] = qdisc.backlog;
  qdisc_json["requeues"] = qdisc.requeues;
  qdisc_json["has_netem_options"] = qdisc.has_netem_options;
  qdisc_json["netem_latency_us"] = qdisc.netem_latency_us;
  qdisc_json["netem_jitter_us"] = qdisc.netem_jitter_us;
  qdisc_json["netem_loss"] = qdisc.netem_loss;
  qdisc_json["netem_duplicate"] = qdisc.netem_duplicate;
  qdisc_json["netem_corrupt"] = qdisc.netem_corrupt;
  qdisc_json["netem_reorder"] = qdisc.netem_reorder;
  qdisc_json["netem_limit_packets"] = qdisc.netem_limit_packets;
  qdisc_json["has_tbf_options"] = qdisc.has_tbf_options;
  qdisc_json["tbf_rate_bytes_per_sec"] = qdisc.tbf_rate_bytes_per_sec;
  qdisc_json["tbf_limit_bytes"] = qdisc.tbf_limit_bytes;
  qdisc_json["tbf_buffer_ticks"] = qdisc.tbf_buffer_ticks;
  qdisc_json["tbf_mtu_ticks"] = qdisc.tbf_mtu_ticks;
  qdisc_json["has_prio_options"] = qdisc.has_prio_options;
  qdisc_json["prio_bands"] = qdisc.prio_bands;
  return qdisc_json;
}

std::string NetworkConditionVerificationDetail(
    const NodeVethConfig& config, const QdiscInfo& qdisc,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::optional<std::uint64_t> operator_sequence) {
  boost::json::object detail;
  detail["host_if"] = config.host_name;
  detail["condition"] = NetworkConditionJson(config.condition);
  detail["qdisc_kind"] = qdisc.kernel_kind;
  detail["qdisc_handle"] = qdisc.handle;
  detail["qdisc_parent"] = qdisc.parent;
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  return boost::json::serialize(detail);
}

std::string TopologyEdgeUpdateDetail(WorkloadKind action,
                                     std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     const RuntimePeerTopologyEdge& previous,
                                     const RuntimePeerTopologyEdge& current,
                                     bool kernel_verified, bool peer_verified,
                                     std::uint32_t timeout_sec) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["action"] = WorkloadKindName(action);
  detail["from"] = current.from + 1U;
  detail["to"] = current.to + 1U;
  detail["previous"] = RuntimePeerTopologyEdgeJson(previous);
  detail["current"] = RuntimePeerTopologyEdgeJson(current);
  detail["kernel_verified"] = kernel_verified;
  detail["peer_verified"] = peer_verified;
  if (action != WorkloadKind::kSetEdgeCondition) {
    detail["timeout_sec"] = timeout_sec;
  }
  return boost::json::serialize(detail);
}

std::string TopologyEdgeRollbackFailureDetail(
    WorkloadKind action, const RuntimePeerTopologyEdge& previous,
    const RuntimePeerTopologyEdge& attempted, std::string_view original_error,
    const std::vector<std::string>& rollback_errors) {
  boost::json::object detail;
  detail["action"] = WorkloadKindName(action);
  detail["from"] = previous.from + 1U;
  detail["to"] = previous.to + 1U;
  detail["previous"] = RuntimePeerTopologyEdgeJson(previous);
  detail["attempted"] = RuntimePeerTopologyEdgeJson(attempted);
  detail["original_error"] = original_error;
  boost::json::array errors;
  for (const std::string& error : rollback_errors) {
    errors.emplace_back(error);
  }
  detail["rollback_errors"] = std::move(errors);
  return boost::json::serialize(detail);
}

std::string NetworkProfileUpdateDetail(
    const ProfileSwitchWorkload& workload, uint32_t node,
    std::string_view previous_profile, const NodeVethConfig& previous,
    const NodeVethConfig& current, const QdiscInfo& qdisc,
    uint32_t workload_index, uint32_t workload_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["profile"] = workload.profile;
  if (previous_profile.empty()) {
    detail["previous_profile"] = nullptr;
  } else {
    detail["previous_profile"] = previous_profile;
  }
  detail["previous"] =
      previous.apply_condition
          ? boost::json::value(NetworkConditionJson(previous.condition))
          : boost::json::value(nullptr);
  detail["current"] = NetworkConditionJson(current.condition);
  detail["qdisc"] = QdiscJson(qdisc);
  detail["kernel_verified"] = true;
  return boost::json::serialize(detail);
}

std::string NetworkBlockRuleDetail(
    const NodeRuntime& node, const NetworkBlockRule& rule, bool existed_before,
    bool present_after, std::uint32_t workload_index,
    std::uint32_t workload_count,
    std::optional<std::uint64_t> operator_sequence) {
  boost::json::object detail = NetworkBlockRuleJson(rule);
  if (node.network) {
    detail["host_if"] = node.network->host_name;
  } else {
    detail["host_if"] = nullptr;
  }
  detail["existed_before"] = existed_before;
  detail["present_after"] = present_after;
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  return boost::json::serialize(detail);
}

boost::json::object PartitionRuleResultJson(const NodeRuntime& node,
                                            const NetworkBlockRule& rule,
                                            bool existed_before,
                                            bool present_after) {
  boost::json::object object = NetworkBlockRuleJson(rule);
  object["node_id"] = node.config.id;
  if (node.network) {
    object["host_if"] = node.network->host_name;
  } else {
    object["host_if"] = nullptr;
  }
  object["existed_before"] = existed_before;
  object["present_after"] = present_after;
  return object;
}

std::string NetworkPartitionDetail(
    const NetworkPartitionRule& partition,
    const boost::json::array& rule_results, uint32_t workload_index,
    uint32_t workload_count, std::optional<std::uint64_t> operator_sequence) {
  boost::json::object detail = NetworkPartitionRuleJson(partition);
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  detail["rules"] = rule_results;
  detail["scope"] = "source_aware_group";
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  return boost::json::serialize(detail);
}

}  // namespace bbp::simulator_app_internal
