#include "simulator_scenario_serialization.h"

#include <yaml.h>

#include <boost/json/serialize.hpp>
#include <chrono>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/default_peer_topology.h"
#include "bbp/json_secret_redaction.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/wallet_funding_strategy.h"
#include "bbp/simulator/wallet_transactions_workload.h"
#include "bbp/simulator/wallet_transfer_strategy.h"
#include "bbp/simulator/yaml_helpers.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {
namespace {

void AddNetworkConditionJsonFields(const NetworkCondition& condition,
                                   boost::json::object* object) {
  (*object)["bandwidth_kbps"] = condition.bandwidth_kbps;
  (*object)["delay_ms"] = condition.delay_ms;
  (*object)["jitter_ms"] = condition.jitter_ms;
  (*object)["loss_basis_points"] = condition.loss_basis_points;
  (*object)["duplicate_basis_points"] = condition.duplicate_basis_points;
  (*object)["corrupt_basis_points"] = condition.corrupt_basis_points;
  (*object)["reorder_basis_points"] = condition.reorder_basis_points;
  (*object)["limit_packets"] = condition.limit_packets;
}

boost::json::array NodeGroupJson(const std::vector<uint32_t>& nodes) {
  boost::json::array array;
  for (uint32_t node_index : nodes) {
    array.push_back(node_index + 1U);
  }
  return array;
}

boost::json::object WalletInitializationJson(
    const WalletInitialization& initialization) {
  boost::json::object object;
  object["strategy"] =
      std::string(WalletInitializationStrategyName(initialization.strategy));
  object["mode"] = std::string(WalletPrivacyModeName(initialization.mode));
  return object;
}

std::string_view PeerConnectivityModeName(PeerConnectivityMode mode) {
  switch (mode) {
    case PeerConnectivityMode::kFixedCount:
      return "fixed_count";
    case PeerConnectivityMode::kAllPeers:
      return "all_peers";
  }
  throw std::runtime_error("unknown peer connectivity mode");
}

boost::json::object PeerConnectivityPolicyJson(
    const PeerConnectivityPolicy& policy) {
  boost::json::object object;
  object["node"] = policy.node + 1U;
  object["mode"] = std::string(PeerConnectivityModeName(policy.mode));
  if (policy.mode == PeerConnectivityMode::kAllPeers) {
    object["all_peers"] = true;
  } else {
    object["min_peer_count"] = policy.peer_count.minimum();
    object["max_peer_count"] = policy.peer_count.maximum();
  }
  return object;
}

boost::json::array PeerConnectivityPoliciesJson(
    const std::vector<PeerConnectivityPolicy>& policies) {
  boost::json::array array;
  for (const PeerConnectivityPolicy& policy : policies) {
    array.push_back(PeerConnectivityPolicyJson(policy));
  }
  return array;
}

boost::json::array TopologyGroupsJson(
    const std::vector<std::vector<uint32_t>>& groups) {
  boost::json::array array;
  for (const std::vector<uint32_t>& group : groups) {
    array.push_back(NodeGroupJson(group));
  }
  return array;
}

boost::json::array PeerTopologyEdgesJson(
    const std::vector<PeerTopologyEdge>& edges) {
  boost::json::array array;
  for (const PeerTopologyEdge& edge : edges) {
    boost::json::object object;
    object["from"] = edge.from + 1U;
    object["to"] = edge.to + 1U;
    object["bidirectional"] = edge.bidirectional;
    object["active"] = edge.active;
    if (edge.latency_ms) {
      object["latency_ms"] = *edge.latency_ms;
    }
    if (edge.condition) {
      AddNetworkConditionJsonFields(*edge.condition, &object);
    }
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::array PeerTopologyRegionEdgesJson(
    const std::vector<PeerTopologyRegionEdge>& edges) {
  boost::json::array array;
  for (const PeerTopologyRegionEdge& edge : edges) {
    boost::json::object object;
    object["from_region"] = edge.from_region + 1U;
    object["to_region"] = edge.to_region + 1U;
    object["bidirectional"] = edge.bidirectional;
    object["active"] = edge.active;
    if (edge.latency_ms) {
      object["latency_ms"] = *edge.latency_ms;
    }
    if (edge.condition) {
      AddNetworkConditionJsonFields(*edge.condition, &object);
    }
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::array LatencyMatrixJson(
    const std::vector<std::vector<std::optional<uint32_t>>>& matrix) {
  boost::json::array result;
  for (const auto& input_row : matrix) {
    boost::json::array row;
    for (const std::optional<uint32_t>& latency_ms : input_row) {
      if (latency_ms) {
        row.push_back(*latency_ms);
      } else {
        row.push_back(nullptr);
      }
    }
    result.push_back(std::move(row));
  }
  return result;
}

boost::json::array ResolvedPeerTopologyEdgesJson(
    const PeerTopologyConfig& topology, uint32_t node_count) {
  boost::json::array array;
  for (const ResolvedPeerTopologyEdge& edge :
       ResolvePeerTopologyEdges(topology, node_count)) {
    boost::json::object object;
    object["from"] = edge.from + 1U;
    object["to"] = edge.to + 1U;
    if (edge.latency_ms) {
      object["latency_ms"] = *edge.latency_ms;
    }
    if (edge.condition) {
      AddNetworkConditionJsonFields(*edge.condition, &object);
    }
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::object RuntimePeerTopologyEdgeJsonImpl(
    const RuntimePeerTopologyEdge& edge) {
  boost::json::object object;
  object["from"] = edge.from + 1U;
  object["to"] = edge.to + 1U;
  object["band"] = edge.band;
  object["active"] = edge.active;
  if (edge.condition) {
    object["condition"] = NetworkConditionJson(*edge.condition);
  } else {
    object["condition"] = nullptr;
  }
  return object;
}

void AddPeerTopologyJsonImpl(const PeerTopologyConfig& topology,
                             uint32_t node_count, boost::json::object* object) {
  (*object)["type"] = std::string(PeerTopologyKindName(topology.kind));
  switch (topology.kind) {
    case PeerTopologyKind::kFullMesh:
    case PeerTopologyKind::kRing:
      break;
    case PeerTopologyKind::kStar:
      (*object)["center_node"] = topology.star_center + 1U;
      break;
    case PeerTopologyKind::kRandomGraph:
      (*object)["seed"] = topology.seed;
      (*object)["average_degree"] = topology.average_degree;
      break;
    case PeerTopologyKind::kScaleFreeGraph:
      (*object)["seed"] = topology.seed;
      (*object)["average_degree"] = topology.average_degree;
      (*object)["attachment_count"] = topology.attachment_count;
      break;
    case PeerTopologyKind::kLatencyMatrix:
      (*object)["latency_matrix_ms"] =
          LatencyMatrixJson(topology.latency_matrix_ms);
      break;
    case PeerTopologyKind::kCustomEdgeList:
      (*object)["edges"] = PeerTopologyEdgesJson(topology.edges);
      break;
    case PeerTopologyKind::kPartitionedGroups:
      (*object)["groups"] = TopologyGroupsJson(topology.groups);
      break;
    case PeerTopologyKind::kInternetLikeRegionGraph:
      (*object)["regions"] = TopologyGroupsJson(topology.regions);
      if (!topology.region_edges.empty()) {
        (*object)["region_edges"] =
            PeerTopologyRegionEdgesJson(topology.region_edges);
      }
      break;
    case PeerTopologyKind::kCount:
      throw std::logic_error("unknown peer topology kind");
  }
  (*object)["resolved_edges"] =
      ResolvedPeerTopologyEdgesJson(topology, node_count);
}

void EmitYamlScalar(YamlEmitter* emitter, std::string_view value,
                    yaml_scalar_style_t style) {
  if (value.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    throw std::runtime_error("YAML scalar is too large");
  }
  yaml_event_t event;
  yaml_scalar_event_initialize(
      &event, nullptr, nullptr,
      const_cast<yaml_char_t*>(
          reinterpret_cast<const yaml_char_t*>(value.data())),
      static_cast<int>(value.size()), 1, 1, style);
  emitter->Emit(&event);
}

void EmitYamlJsonValue(YamlEmitter* emitter, const boost::json::value& value);

void EmitYamlJsonObject(YamlEmitter* emitter,
                        const boost::json::object& object) {
  yaml_event_t event;
  yaml_mapping_start_event_initialize(&event, nullptr, nullptr, 1,
                                      YAML_BLOCK_MAPPING_STYLE);
  emitter->Emit(&event);
  for (const auto& item : object) {
    EmitYamlScalar(emitter, item.key(), YAML_PLAIN_SCALAR_STYLE);
    EmitYamlJsonValue(emitter, item.value());
  }
  yaml_mapping_end_event_initialize(&event);
  emitter->Emit(&event);
}

void EmitYamlJsonArray(YamlEmitter* emitter, const boost::json::array& array) {
  yaml_event_t event;
  yaml_sequence_start_event_initialize(&event, nullptr, nullptr, 1,
                                       YAML_BLOCK_SEQUENCE_STYLE);
  emitter->Emit(&event);
  for (const boost::json::value& value : array) {
    EmitYamlJsonValue(emitter, value);
  }
  yaml_sequence_end_event_initialize(&event);
  emitter->Emit(&event);
}

void EmitYamlJsonValue(YamlEmitter* emitter, const boost::json::value& value) {
  if (value.is_object()) {
    EmitYamlJsonObject(emitter, value.as_object());
  } else if (value.is_array()) {
    EmitYamlJsonArray(emitter, value.as_array());
  } else if (value.is_string()) {
    EmitYamlScalar(emitter, std::string_view(value.as_string()),
                   YAML_DOUBLE_QUOTED_SCALAR_STYLE);
  } else if (value.is_bool()) {
    EmitYamlScalar(emitter, value.as_bool() ? "true" : "false",
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_int64()) {
    EmitYamlScalar(emitter, std::to_string(value.as_int64()),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_uint64()) {
    EmitYamlScalar(emitter, std::to_string(value.as_uint64()),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_double()) {
    EmitYamlScalar(emitter, boost::json::serialize(value),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_null()) {
    EmitYamlScalar(emitter, "null", YAML_PLAIN_SCALAR_STYLE);
  }
}

boost::json::array WalletIndexesJson(
    const std::vector<std::uint32_t>& wallets) {
  boost::json::array array;
  array.reserve(wallets.size());
  for (const std::uint32_t wallet : wallets) {
    array.emplace_back(wallet);
  }
  return array;
}

boost::json::value AmountDistributionConfigurationJson(
    const AmountDistribution& distribution) {
  if (distribution.kind == ValueDistributionKind::kFixed) {
    return boost::json::value(
        FormatFixed8Amount(distribution.minimum_satoshis));
  }
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min"] = FormatFixed8Amount(distribution.minimum_satoshis);
  object["max"] = FormatFixed8Amount(distribution.maximum_satoshis);
  return object;
}

boost::json::value IntervalDistributionConfigurationJson(
    const IntervalDistribution& distribution) {
  const auto duration_text = [](std::chrono::milliseconds duration) {
    return std::to_string(duration.count()) + "ms";
  };
  if (distribution.kind == ValueDistributionKind::kFixed) {
    return boost::json::value(duration_text(distribution.minimum));
  }
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min"] = duration_text(distribution.minimum);
  object["max"] = duration_text(distribution.maximum);
  return object;
}

bool IsTopologyEdgeAction(WorkloadKind kind) {
  return kind == WorkloadKind::kSetEdgeCondition ||
         kind == WorkloadKind::kActivateEdge ||
         kind == WorkloadKind::kDeactivateEdge ||
         kind == WorkloadKind::kRestoreEdge;
}

boost::json::object BlockGenerationWorkloadJsonImpl(
    const BlockGenerationWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kBlockGeneration));
  object["node"] = workload.node;
  object["count"] = workload.count;
  object["sync_timeout_sec"] = workload.sync_timeout_sec;
  return object;
}

boost::json::object WaitUntilHeightWorkloadJsonImpl(
    const WaitUntilHeightWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kWaitUntilHeight));
  object["node"] = workload.node;
  object["height"] = workload.height;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WaitForPeersWorkloadJsonImpl(
    const WaitForPeersWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kWaitForPeers));
  object["node"] = workload.node;
  object["peer_count"] = workload.peer_count;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object ConnectPeerWorkloadJson(
    const ConnectPeerWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kConnectPeer));
  object["node"] = workload.node;
  object["peer"] = workload.peer;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object DisconnectPeerWorkloadJson(
    const DisconnectPeerWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kDisconnectPeer));
  object["node"] = workload.node;
  object["peer"] = workload.peer;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object RestartNodeWorkloadJson(
    const RestartNodeWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kRestartNode));
  object["node"] = workload.node;
  return object;
}

boost::json::object FreezeNodeWorkloadJson(const FreezeNodeWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kFreezeNode));
  object["node"] = workload.node;
  object["duration_ms"] = workload.duration_ms;
  return object;
}

boost::json::object ResourceLimitUpdateWorkloadJson(
    const ResourceLimitUpdateWorkload& workload) {
  boost::json::object object = ResourceLimitPatchJson(workload.patch);
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kUpdateResourceLimits));
  object["node"] = workload.node;
  return object;
}

boost::json::object ProfileSwitchWorkloadJson(
    const ProfileSwitchWorkload& workload, WorkloadKind kind) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(kind));
  boost::json::array nodes;
  for (const std::string& node_id : workload.node_ids) {
    nodes.emplace_back(node_id);
  }
  object["nodes"] = std::move(nodes);
  object["profile"] = workload.profile;
  return object;
}

boost::json::object ResourcePressureWorkloadJson(
    const ResourcePressureWorkload& workload) {
  boost::json::object object = ResourceLimitPatchJson(workload.patch);
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kResourcePressure));
  object["node"] = workload.node;
  object["duration_ms"] = workload.duration_ms;
  return object;
}

boost::json::object NetworkPartitionWorkloadJson(
    const NetworkPartitionWorkload& workload, WorkloadKind kind) {
  boost::json::object object = NetworkPartitionRuleJson(workload.partition);
  object["type"] = std::string(WorkloadKindName(kind));
  return object;
}

boost::json::object NetworkConditionWorkloadJson(
    const NetworkConditionWorkload& workload) {
  boost::json::object object = NetworkConditionJson(workload.condition);
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kSetNetworkCondition));
  object["node"] = workload.node;
  return object;
}

boost::json::object NetworkBlockWorkloadJson(
    const NetworkBlockWorkload& workload, WorkloadKind kind) {
  boost::json::object object = NetworkBlockRuleJson(workload.rule);
  object["type"] = std::string(WorkloadKindName(kind));
  return object;
}

boost::json::object TopologyEdgeWorkloadJson(
    const TopologyEdgeWorkload& workload, WorkloadKind kind) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(kind));
  object["from"] = workload.from;
  object["to"] = workload.to;
  if (kind == WorkloadKind::kSetEdgeCondition) {
    if (!workload.condition) {
      throw std::runtime_error(
          "set_edge_condition workload is missing its condition");
    }
    AddNetworkConditionJsonFields(*workload.condition, &object);
  } else {
    object["timeout_sec"] = workload.timeout_sec;
  }
  return object;
}

boost::json::object SendRawTransactionWorkloadJson(
    const SendRawTransactionWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kSendRawTransaction));
  object["funding_node"] = workload.funding_node;
  object["submit_node"] = workload.submit_node;
  object["source_address"] = workload.source_address;
  object["source_private_key"] = std::string(kPrivateSigningMaterialRedaction);
  object["destination_address"] = workload.destination_address;
  object["funding_blocks"] = workload.funding_blocks;
  object["amount"] = FormatFixed8Amount(workload.amount_satoshis);
  object["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WalletTransactionsWorkloadJsonImpl(
    const WalletTransactionsWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kWalletTransactions));
  object["funding_strategy"] =
      std::string(WalletFundingStrategyName(workload.funding_strategy));
  object["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  object["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  object["readiness_confirmations"] = workload.readiness_confirmations;
  object["funding_threshold"] =
      FormatFixed8Amount(workload.funding_threshold_satoshis);
  if (workload.transaction_count != 0U) {
    object["transaction_count"] = workload.transaction_count;
  } else {
    object["transaction_count"] = nullptr;
  }
  if (workload.transaction_rate) {
    object["transaction_rate"] = workload.transaction_rate->value();
    object["transaction_rate_millionths"] =
        workload.transaction_rate->millionths();
  } else {
    object["transaction_rate"] = nullptr;
    object["transaction_rate_millionths"] = nullptr;
  }
  if (IsTransactionLoadStrategy(workload.strategy)) {
    if (workload.duration) {
      object["duration"] = std::to_string(workload.duration->count()) + "ms";
    } else {
      object["duration"] = nullptr;
    }
    object["concurrency"] = workload.concurrency;
    object["queue_capacity"] = workload.queue_capacity;
    object["mode"] = std::string(WalletPrivacyModeName(workload.mode));
    object["fee_policy"] =
        std::string(WalletTransactionFeePolicyName(workload.fee_policy));
    object["fee_reserve"] = FormatFixed8Amount(
        EffectiveWalletTransactionFeeReserveSatoshis(workload));
    object["fee_reserve_satoshis"] =
        EffectiveWalletTransactionFeeReserveSatoshis(workload);
    if (workload.retained_balance_basis_points) {
      object["retained_balance_percentage"] =
          static_cast<double>(*workload.retained_balance_basis_points) / 100.0;
      object["retained_balance_basis_points"] =
          *workload.retained_balance_basis_points;
    }
  }
  object["amount"] = AmountDistributionConfigurationJson(workload.amount);
  if (workload.interval.kind != ValueDistributionKind::kFixed ||
      workload.interval.minimum != std::chrono::milliseconds(0)) {
    object["interval"] =
        IntervalDistributionConfigurationJson(workload.interval);
  }
  object["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  object["seed"] = workload.random_seed;
  if (!workload.sender_wallets.empty()) {
    object["sender_wallets"] = WalletIndexesJson(workload.sender_wallets);
  }
  if (!workload.receiver_wallets.empty()) {
    object["receiver_wallets"] = WalletIndexesJson(workload.receiver_wallets);
  }
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object CheckpointWorkloadJson(const CheckpointWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kCheckpoint));
  if (!workload.name.empty()) {
    object["name"] = workload.name;
  }
  return object;
}

boost::json::object SimulationCommandScenarioJson(
    const SimulationCommand& command) {
  boost::json::object object;
  object["action"] = std::string(SimulationCommandKindName(command.kind));
  if (command.kind != SimulationCommandKind::kSetBlockProductionPolicy &&
      command.kind != SimulationCommandKind::kPartitionNodes &&
      command.kind != SimulationCommandKind::kHealPartition &&
      command.kind != SimulationCommandKind::kSetPerfCounters &&
      command.kind != SimulationCommandKind::kAssignRole &&
      command.kind != SimulationCommandKind::kRemoveRole) {
    object["node"] = command.node_id;
  }
  if (command.block_production_policy) {
    object["period_ms"] = command.block_production_policy->period().count();
    object["probability"] = command.block_production_policy->probability();
    object["seed"] = command.block_production_policy->seed();
  }
  if (command.mining_difficulty) {
    object["difficulty"] = command.mining_difficulty->value();
  }
  if (command.peer_node_id) {
    object["peer_node_id"] = *command.peer_node_id;
  }
  if (command.peer_count_policy) {
    object["minimum_peer_count"] = command.peer_count_policy->minimum();
    object["maximum_peer_count"] = command.peer_count_policy->maximum();
  }
  if (command.block_count) {
    object["block_count"] = *command.block_count;
  }
  if (command.profile) {
    object["profile"] = *command.profile;
  }
  if (command.resource_limit_patch) {
    object["resource_limits"] =
        ResourceLimitPatchJson(*command.resource_limit_patch);
  }
  if (command.network_condition) {
    object["network_condition"] =
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
    object["network_flow"] = std::move(flow);
  }
  if (command.partition) {
    const auto group_json = [](const SimulationPartitionGroup& group) {
      boost::json::object group_object;
      boost::json::array group_ids;
      for (const std::string& group_id : group.group_ids) {
        group_ids.emplace_back(group_id);
      }
      boost::json::array node_ids;
      for (const std::string& node_id : group.node_ids) {
        node_ids.emplace_back(node_id);
      }
      group_object["group_ids"] = std::move(group_ids);
      group_object["node_ids"] = std::move(node_ids);
      return group_object;
    };
    boost::json::object partition;
    partition["scope"] =
        std::string(SimulationPartitionScopeName(command.partition->scope));
    partition["group_a"] = group_json(command.partition->group_a);
    partition["group_b"] = group_json(command.partition->group_b);
    object["partition"] = std::move(partition);
  }
  if (command.perf_counter_target) {
    boost::json::object target;
    target["kind"] =
        PerfCounterTargetKindName(command.perf_counter_target->kind);
    target["id"] = command.perf_counter_target->id;
    boost::json::array node_ids;
    for (const std::string& node_id : command.perf_counter_target->node_ids) {
      node_ids.emplace_back(node_id);
    }
    target["node_ids"] = std::move(node_ids);
    object["perf_target"] = std::move(target);
  }
  if (!command.perf_counter_kinds.empty()) {
    object["perf_counters"] = PerfCounterNamesJson(command.perf_counter_kinds);
  }
  if (command.wallet_send) {
    boost::json::object send;
    send["sender_wallet_index"] = command.wallet_send->sender_wallet_index;
    send["receiver_wallet_index"] = command.wallet_send->receiver_wallet_index;
    send["amount"] = FormatFixed8Amount(command.wallet_send->amount_satoshis);
    send["fee"] = FormatFixed8Amount(command.wallet_send->fee_satoshis);
    send["timeout_sec"] = command.wallet_send->timeout_sec;
    object["wallet_send"] = std::move(send);
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
    object["role_mutation"] = std::move(mutation);
  }
  return object;
}

}  // namespace

boost::json::object RuntimePeerTopologyEdgeJson(
    const RuntimePeerTopologyEdge& edge) {
  return RuntimePeerTopologyEdgeJsonImpl(edge);
}

void AddPeerTopologyJson(const PeerTopologyConfig& topology,
                         uint32_t node_count, boost::json::object* object) {
  AddPeerTopologyJsonImpl(topology, node_count, object);
}

boost::json::object BlockGenerationWorkloadJson(
    const BlockGenerationWorkload& workload) {
  return BlockGenerationWorkloadJsonImpl(workload);
}

boost::json::object WaitUntilHeightWorkloadJson(
    const WaitUntilHeightWorkload& workload) {
  return WaitUntilHeightWorkloadJsonImpl(workload);
}

boost::json::object WaitForPeersWorkloadJson(
    const WaitForPeersWorkload& workload) {
  return WaitForPeersWorkloadJsonImpl(workload);
}

boost::json::object WalletTransactionsWorkloadJson(
    const WalletTransactionsWorkload& workload) {
  return WalletTransactionsWorkloadJsonImpl(workload);
}

boost::json::object NetworkConditionJson(const NetworkCondition& condition) {
  boost::json::object object;
  AddNetworkConditionJsonFields(condition, &object);
  return object;
}

boost::json::array DirectionalNetworkPoliciesJson(
    const std::vector<DirectionalNetworkPolicy>& policies) {
  boost::json::array array;
  for (const DirectionalNetworkPolicy& policy : policies) {
    boost::json::object object;
    object["band"] = policy.band;
    object["destination_address"] = policy.destination_address;
    object["condition"] = NetworkConditionJson(policy.condition);
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::object NetworkBlockRuleJson(const NetworkBlockRule& rule) {
  boost::json::object object;
  object["node"] = rule.node_index + 1U;
  if (!rule.src_address.empty()) {
    object["src_address"] = rule.src_address;
  }
  if (rule.src_port != 0U) {
    object["src_port"] = rule.src_port;
  }
  object["dst_address"] = rule.dst_address;
  object["dst_port"] = rule.dst_port;
  object["handle"] = rule.handle;
  return object;
}

boost::json::object NetworkPartitionRuleJson(const NetworkPartitionRule& rule) {
  boost::json::object object;
  object["group_a"] = NodeGroupJson(rule.group_a);
  object["group_b"] = NodeGroupJson(rule.group_b);
  return object;
}

boost::json::object ScenarioNodeWalletConfigJson(
    const ScenarioNodeWalletConfig& config) {
  boost::json::object object;
  object["enabled"] = config.enabled;
  if (config.enabled) {
    boost::json::object initialization;
    initialization["strategy"] =
        std::string(WalletInitializationStrategyName(config.strategy));
    initialization["mode"] = std::string(WalletPrivacyModeName(config.mode));
    object["initialization"] = std::move(initialization);
  }
  return object;
}

boost::json::array RuntimePeerTopologyEdgesJson(
    const RuntimePeerTopology& topology) {
  boost::json::array array;
  for (const RuntimePeerTopologyEdge& edge : topology.edges()) {
    array.push_back(RuntimePeerTopologyEdgeJson(edge));
  }
  return array;
}

boost::json::object NodeRoleTopologyJson(
    const NodeRoleTopology& topology,
    const WalletInitialization& wallet_initialization) {
  boost::json::object object;
  object["node_count"] = topology.node_count;
  object["wallet_node_count"] = topology.wallet_node_count;
  object["miner_node_count"] = topology.miner_node_count;
  object["allow_miner_wallet_overlap"] = topology.allow_miner_wallet_overlap;
  object["wallet_nodes"] = NodeGroupJson(topology.wallet_nodes);
  object["miner_nodes"] = NodeGroupJson(topology.miner_nodes);
  object["wallet_initialization"] =
      WalletInitializationJson(wallet_initialization);
  AddPeerTopologyJson(topology.peer_topology, topology.node_count, &object);
  if (!topology.peer_connectivity.empty()) {
    object["peer_connectivity"] =
        PeerConnectivityPoliciesJson(topology.peer_connectivity);
  }
  return object;
}

boost::json::array IoLimitsJson(const std::vector<IoLimit>& io_limits) {
  boost::json::array array;
  for (const IoLimit& limit : io_limits) {
    boost::json::object item;
    item["device"] = BlockDeviceIdText(limit.device);
    const auto add_limit = [&](const char* field,
                               const std::optional<uint64_t>& value) {
      if (value) {
        item[field] = *value;
      } else {
        item[field] = nullptr;
      }
    };
    add_limit("read_bytes_per_sec", limit.read_bytes_per_sec);
    add_limit("write_bytes_per_sec", limit.write_bytes_per_sec);
    add_limit("read_operations_per_sec", limit.read_operations_per_sec);
    add_limit("write_operations_per_sec", limit.write_operations_per_sec);
    array.push_back(std::move(item));
  }
  return array;
}

boost::json::object ResourceLimitsJson(const ResourceLimits& limits) {
  boost::json::object object;
  object["memory_high_bytes"] = limits.memory_high_bytes;
  object["memory_max_bytes"] = limits.memory_max_bytes;
  if (limits.cpu_quota_us) {
    object["cpu_quota_us"] = *limits.cpu_quota_us;
  } else {
    object["cpu_quota_us"] = nullptr;
  }
  object["cpu_period_us"] = limits.cpu_period_us;
  object["cpu_weight"] = limits.cpu_weight;
  object["io_weight"] = limits.io_weight;
  object["io_max"] = IoLimitsJson(limits.io_limits);
  object["pids_max"] = limits.pids_max;
  return object;
}

boost::json::object ResourceLimitPatchJson(const ResourceLimitPatch& patch) {
  boost::json::object object;
  if (patch.memory_high_bytes) {
    object["memory_high_bytes"] = *patch.memory_high_bytes;
  }
  if (patch.memory_max_bytes) {
    object["memory_max_bytes"] = *patch.memory_max_bytes;
  }
  if (patch.cpu_quota_present) {
    if (patch.cpu_quota_us) {
      object["cpu_quota_us"] = *patch.cpu_quota_us;
    } else {
      object["cpu_quota_us"] = nullptr;
    }
  }
  if (patch.cpu_period_us) {
    object["cpu_period_us"] = *patch.cpu_period_us;
  }
  if (patch.cpu_weight) {
    object["cpu_weight"] = *patch.cpu_weight;
  }
  if (patch.io_weight) {
    object["io_weight"] = *patch.io_weight;
  }
  if (patch.io_limits_present) {
    object["io_max"] = IoLimitsJson(patch.io_limits);
  }
  if (patch.pids_max) {
    object["pids_max"] = *patch.pids_max;
  }
  return object;
}

boost::json::array PerfCounterNamesJson(
    const std::vector<PerfCounterKind>& kinds) {
  boost::json::array names;
  names.reserve(kinds.size());
  for (const PerfCounterKind kind : kinds) {
    names.emplace_back(PerfCounterKindName(kind));
  }
  return names;
}

std::string YamlFromJson(const boost::json::value& value) {
  YamlEmitter emitter;
  yaml_event_t event;
  yaml_stream_start_event_initialize(&event, YAML_UTF8_ENCODING);
  emitter.Emit(&event);
  yaml_document_start_event_initialize(&event, nullptr, nullptr, nullptr, 0);
  emitter.Emit(&event);
  EmitYamlJsonValue(&emitter, value);
  yaml_document_end_event_initialize(&event, 0);
  emitter.Emit(&event);
  yaml_stream_end_event_initialize(&event);
  emitter.Emit(&event);
  return emitter.Output();
}

boost::json::object WorkloadJson(const ScenarioWorkload& workload) {
  if (workload.kind == WorkloadKind::kBlockGeneration) {
    return BlockGenerationWorkloadJson(workload.block_generation);
  }
  if (workload.kind == WorkloadKind::kWaitUntilHeight) {
    return WaitUntilHeightWorkloadJson(workload.wait_until_height);
  }
  if (workload.kind == WorkloadKind::kWaitForPeers) {
    return WaitForPeersWorkloadJson(workload.wait_for_peers);
  }
  if (workload.kind == WorkloadKind::kConnectPeer) {
    return ConnectPeerWorkloadJson(workload.connect_peer);
  }
  if (workload.kind == WorkloadKind::kDisconnectPeer) {
    return DisconnectPeerWorkloadJson(workload.disconnect_peer);
  }
  if (workload.kind == WorkloadKind::kRestartNode) {
    return RestartNodeWorkloadJson(workload.restart_node);
  }
  if (workload.kind == WorkloadKind::kFreezeNode) {
    return FreezeNodeWorkloadJson(workload.freeze_node);
  }
  if (workload.kind == WorkloadKind::kUpdateResourceLimits) {
    return ResourceLimitUpdateWorkloadJson(workload.update_resource_limits);
  }
  if (workload.kind == WorkloadKind::kSetResourceProfile ||
      workload.kind == WorkloadKind::kSetNetworkProfile) {
    return ProfileSwitchWorkloadJson(workload.profile_switch, workload.kind);
  }
  if (workload.kind == WorkloadKind::kResourcePressure) {
    return ResourcePressureWorkloadJson(workload.resource_pressure);
  }
  if (workload.kind == WorkloadKind::kSetNetworkCondition) {
    return NetworkConditionWorkloadJson(workload.network_condition);
  }
  if (workload.kind == WorkloadKind::kBlockNetworkFlow ||
      workload.kind == WorkloadKind::kUnblockNetworkFlow) {
    return NetworkBlockWorkloadJson(workload.network_block, workload.kind);
  }
  if (workload.kind == WorkloadKind::kPartitionNodes) {
    return NetworkPartitionWorkloadJson(workload.network_partition,
                                        WorkloadKind::kPartitionNodes);
  }
  if (workload.kind == WorkloadKind::kHealPartition) {
    return NetworkPartitionWorkloadJson(workload.network_partition,
                                        WorkloadKind::kHealPartition);
  }
  if (IsTopologyEdgeAction(workload.kind)) {
    return TopologyEdgeWorkloadJson(workload.topology_edge, workload.kind);
  }
  if (workload.kind == WorkloadKind::kSendRawTransaction) {
    return SendRawTransactionWorkloadJson(workload.send_raw_transaction);
  }
  if (workload.kind == WorkloadKind::kWalletTransactions) {
    return WalletTransactionsWorkloadJson(workload.wallet_transactions);
  }
  if (workload.kind == WorkloadKind::kCheckpoint) {
    return CheckpointWorkloadJson(workload.checkpoint);
  }
  throw std::runtime_error("unknown scenario workload kind");
}

boost::json::object ScheduledScenarioEventJson(
    const ScheduledScenarioEvent& event,
    const SimulationTimeScale& time_scale) {
  boost::json::object object;
  if (const auto* workload = std::get_if<ScenarioWorkload>(&event.action)) {
    object = WorkloadJson(*workload);
    object.erase("type");
    object["action"] = std::string(WorkloadKindName(workload->kind));
  } else {
    object = SimulationCommandScenarioJson(
        std::get<SimulationCommand>(event.action));
  }
  object["sequence"] = event.sequence;
  object["at"] = std::to_string(event.at.count()) + "ms";
  object["at_ms"] = event.at.count();
  object["wall_at_ms"] = time_scale.WallDuration(event.at).count();
  return object;
}

}  // namespace bbp::simulator_app_internal
