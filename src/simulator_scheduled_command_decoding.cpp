#include "simulator_scheduled_command_decoding.h"

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/network.h"
#include "bbp/scenario_fields.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulator/options.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_resource_limit_decoding.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_wallet_transaction_distribution_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

void RejectUnsupportedScenarioCommandFields(const boost::json::object& object,
                                            SimulationCommandKind kind) {
  for (const auto& member : object) {
    if (member.key() != "at" && member.key() != "action" &&
        !ScenarioCommandFieldAllowed(kind, member.key())) {
      throw std::runtime_error(
          "scenario scheduled command " +
          std::string(SimulationCommandKindName(kind)) +
          " has unsupported field: " + std::string(member.key()));
    }
  }
}

std::string ScenarioCommandNodeId(const boost::json::object& object,
                                  const char* field, const Options& options) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("scenario scheduled command requires " +
                             std::string(field));
  }
  if (value->is_string()) {
    const std::string node_id(value->as_string());
    for (std::uint32_t node_index = 0U; node_index < options.nodes;
         ++node_index) {
      if (ScenarioNodeId(options, node_index) == node_id) {
        return node_id;
      }
    }
    throw std::runtime_error(
        "scenario scheduled command references unknown node ID: " + node_id);
  }
  const std::uint32_t node = JsonUint32Value(*value, field);
  if (node == 0U || node > options.nodes) {
    throw std::runtime_error("scenario scheduled command " +
                             std::string(field) + " must be in 1.." +
                             std::to_string(options.nodes));
  }
  return ScenarioNodeId(options, node - 1U);
}

std::vector<std::string> ScenarioCommandNodeIds(
    const boost::json::object& object, const char* field,
    const Options& options) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_array() || value->as_array().empty()) {
    throw std::runtime_error("scenario scheduled command " +
                             std::string(field) + " must be a non-empty array");
  }
  std::vector<std::string> node_ids;
  std::set<std::string> unique;
  node_ids.reserve(value->as_array().size());
  for (const boost::json::value& node : value->as_array()) {
    boost::json::object wrapper;
    wrapper["node"] = node;
    std::string node_id = ScenarioCommandNodeId(wrapper, "node", options);
    if (!unique.insert(node_id).second) {
      throw std::runtime_error("scenario scheduled command " +
                               std::string(field) +
                               " contains duplicate node ID: " + node_id);
    }
    node_ids.push_back(std::move(node_id));
  }
  return node_ids;
}

SimulationPartitionScope ParseSimulationPartitionScope(std::string_view value) {
  if (value ==
      SimulationPartitionScopeName(SimulationPartitionScope::kNodePair)) {
    return SimulationPartitionScope::kNodePair;
  }
  if (value ==
      SimulationPartitionScopeName(SimulationPartitionScope::kPartitionGroup)) {
    return SimulationPartitionScope::kPartitionGroup;
  }
  if (value ==
      SimulationPartitionScopeName(SimulationPartitionScope::kRegion)) {
    return SimulationPartitionScope::kRegion;
  }
  if (value == SimulationPartitionScopeName(SimulationPartitionScope::kRole)) {
    return SimulationPartitionScope::kRole;
  }
  throw std::runtime_error("unsupported simulation partition scope: " +
                           std::string(value));
}

SimulationPartitionGroup ParseSimulationPartitionGroup(
    const boost::json::object& object, const Options& options,
    std::string_view context) {
  RejectUnsupportedFields(
      object,
      ScenarioObjectFields(ScenarioObjectKind::kSimulationPartitionGroup),
      context);
  const boost::json::value* groups = object.if_contains("group_ids");
  if (groups == nullptr || !groups->is_array() || groups->as_array().empty()) {
    throw std::runtime_error(std::string(context) +
                             " group_ids must be a non-empty array");
  }
  SimulationPartitionGroup group;
  std::set<std::string> unique_groups;
  for (const boost::json::value& value : groups->as_array()) {
    if (!value.is_string() || value.as_string().empty()) {
      throw std::runtime_error(std::string(context) +
                               " group_ids entries must be non-empty strings");
    }
    std::string group_id(value.as_string());
    if (!unique_groups.insert(group_id).second) {
      throw std::runtime_error(std::string(context) +
                               " contains duplicate group ID: " + group_id);
    }
    group.group_ids.push_back(std::move(group_id));
  }
  group.node_ids = ScenarioCommandNodeIds(object, "node_ids", options);
  return group;
}

SimulationPartition ParseSimulationPartition(const boost::json::object& object,
                                             const Options& options) {
  RejectUnsupportedFields(
      object, ScenarioObjectFields(ScenarioObjectKind::kSimulationPartition),
      "scenario scheduled command partition");
  SimulationPartition partition;
  partition.scope =
      ParseSimulationPartitionScope(JsonStringField(object, "scope"));
  const boost::json::value* group_a = object.if_contains("group_a");
  const boost::json::value* group_b = object.if_contains("group_b");
  if (group_a == nullptr || !group_a->is_object() || group_b == nullptr ||
      !group_b->is_object()) {
    throw std::runtime_error(
        "scenario scheduled command partition groups must be objects");
  }
  partition.group_a = ParseSimulationPartitionGroup(
      group_a->as_object(), options, "scenario scheduled command group_a");
  partition.group_b = ParseSimulationPartitionGroup(
      group_b->as_object(), options, "scenario scheduled command group_b");
  std::set<std::string> group_a_nodes(partition.group_a.node_ids.begin(),
                                      partition.group_a.node_ids.end());
  for (const std::string& node_id : partition.group_b.node_ids) {
    if (group_a_nodes.contains(node_id)) {
      throw std::runtime_error(
          "scenario scheduled command partition groups overlap at node " +
          node_id);
    }
  }
  if (partition.scope == SimulationPartitionScope::kNodePair &&
      (partition.group_a.group_ids.size() != 1U ||
       partition.group_a.node_ids.size() != 1U ||
       partition.group_b.group_ids.size() != 1U ||
       partition.group_b.node_ids.size() != 1U)) {
    throw std::runtime_error(
        "scenario scheduled node-pair partition requires one node per group");
  }
  return partition;
}

PerfCounterTarget ParseScenarioPerfTarget(const boost::json::object& object,
                                          const Options& options) {
  RejectUnsupportedFields(object,
                          ScenarioObjectFields(ScenarioObjectKind::kPerfTarget),
                          "scenario scheduled command perf_target");
  const std::string kind_name = JsonStringField(object, "kind");
  const std::optional<PerfCounterTargetKind> kind =
      PerfCounterTargetKindFromName(kind_name);
  if (!kind) {
    throw std::runtime_error("unsupported perf counter target kind: " +
                             kind_name);
  }
  PerfCounterTarget target;
  target.kind = *kind;
  target.id = JsonStringField(object, "id");
  if (target.id.empty()) {
    throw std::runtime_error(
        "scenario scheduled command perf target id must not be empty");
  }
  target.node_ids = ScenarioCommandNodeIds(object, "node_ids", options);
  return target;
}

std::vector<PerfCounterKind> ParseScenarioPerfCounters(
    const boost::json::object& object) {
  const boost::json::value* value = object.if_contains("perf_counters");
  if (value == nullptr || !value->is_array() || value->as_array().empty()) {
    throw std::runtime_error(
        "scenario scheduled command perf_counters must be a non-empty array");
  }
  std::vector<PerfCounterKind> counters;
  std::set<PerfCounterKind> unique;
  for (const boost::json::value& counter : value->as_array()) {
    if (!counter.is_string()) {
      throw std::runtime_error(
          "scenario scheduled command perf_counters entries must be strings");
    }
    const std::string name(counter.as_string());
    const std::optional<PerfCounterKind> kind = PerfCounterKindFromName(name);
    if (!kind) {
      throw std::runtime_error("unsupported perf counter: " + name);
    }
    if (!unique.insert(*kind).second) {
      throw std::runtime_error(
          "scenario scheduled command perf_counters contains duplicate: " +
          name);
    }
    counters.push_back(*kind);
  }
  return counters;
}

SimulationRoleMutationRequest ParseScenarioRoleMutation(
    const boost::json::object& object, SimulationCommandKind kind,
    const Options& options) {
  const boost::json::value* mutation = object.if_contains("role_mutation");
  if (mutation == nullptr || !mutation->is_object()) {
    throw std::runtime_error(
        "scenario scheduled command role_mutation must be an object");
  }
  const boost::json::object& mutation_object = mutation->as_object();
  constexpr std::array<std::string_view, 5U> kFields = {
      "node_ids", "roles", "mode", "funding_wallet_id", "timeout_sec"};
  RejectUnsupportedFields(mutation_object, kFields,
                          "scenario scheduled command role_mutation");

  SimulationRoleMutationRequest request;
  const boost::json::value* node_ids = mutation_object.if_contains("node_ids");
  if (node_ids == nullptr || !node_ids->is_array() ||
      node_ids->as_array().empty()) {
    throw std::runtime_error(
        "scenario scheduled role_mutation node_ids must be a non-empty "
        "array");
  }
  if (std::any_of(node_ids->as_array().begin(), node_ids->as_array().end(),
                  [](const boost::json::value& node_id) {
                    return !node_id.is_string();
                  })) {
    throw std::runtime_error(
        "scenario scheduled role_mutation node_ids must contain strings");
  }
  request.node_ids =
      ScenarioCommandNodeIds(mutation_object, "node_ids", options);
  const boost::json::value* roles = mutation_object.if_contains("roles");
  if (roles == nullptr || !roles->is_array() ||
      roles->as_array().size() != 1U ||
      !roles->as_array().front().is_string()) {
    throw std::runtime_error(
        "scenario scheduled role_mutation roles must contain exactly one "
        "string");
  }
  const std::string role_name(roles->as_array().front().as_string());
  const std::optional<SimulationRoleKind> role =
      SimulationRoleKindFromName(role_name);
  if (!role) {
    throw std::runtime_error(
        "scenario scheduled role_mutation role must be wallet, miner, or "
        "masternode");
  }
  request.role = *role;

  if (mutation_object.if_contains("mode") != nullptr) {
    const std::string mode_name = JsonStringField(mutation_object, "mode");
    request.mode = WalletPrivacyModeFromName(mode_name);
    if (!request.mode) {
      throw std::runtime_error(
          "scenario scheduled role_mutation mode must be public or private");
    }
  }
  if (mutation_object.if_contains("funding_wallet_id") != nullptr) {
    request.funding_wallet_id =
        JsonStringField(mutation_object, "funding_wallet_id");
  }
  if (mutation_object.if_contains("timeout_sec") != nullptr) {
    request.timeout_sec = JsonUint32Field(mutation_object, "timeout_sec");
  }
  ValidateSimulationRoleMutationRequest(kind, request);
  return request;
}

}  // namespace

SimulationCommand ParseScheduledSimulationCommand(
    const boost::json::object& object, SimulationCommandKind kind,
    const Options& options) {
  RejectUnsupportedScenarioCommandFields(object, kind);
  SimulationCommand command;
  command.kind = kind;
  command.confirmed = true;
  if (kind == SimulationCommandKind::kSetBlockProductionPolicy ||
      kind == SimulationCommandKind::kPartitionNodes ||
      kind == SimulationCommandKind::kHealPartition ||
      kind == SimulationCommandKind::kSetPerfCounters ||
      kind == SimulationCommandKind::kAddNodes ||
      kind == SimulationCommandKind::kRemoveNodes ||
      kind == SimulationCommandKind::kAssignRole ||
      kind == SimulationCommandKind::kRemoveRole) {
    command.node_id = "sim";
  } else {
    command.node_id = ScenarioCommandNodeId(object, "node", options);
  }

  if (kind == SimulationCommandKind::kSetBlockProductionPolicy) {
    const std::uint64_t period_ms = JsonUint64Field(object, "period_ms");
    if (period_ms == 0U ||
        period_ms > static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max())) {
      throw std::runtime_error(
          "scenario scheduled command period_ms is out of range");
    }
    if (object.if_contains("probability") == nullptr) {
      throw std::runtime_error(
          "scenario scheduled command requires probability");
    }
    command.block_production_policy = BlockProductionPolicy(
        std::chrono::milliseconds(static_cast<std::int64_t>(period_ms)),
        JsonOptionalDoubleField(object, "probability", 0.0),
        JsonUint64Field(object, "seed"));
  } else if (kind == SimulationCommandKind::kSetMiningDifficulty) {
    if (object.if_contains("difficulty") == nullptr) {
      throw std::runtime_error(
          "scenario scheduled command requires difficulty");
    }
    command.mining_difficulty =
        MiningDifficulty(JsonOptionalDoubleField(object, "difficulty", 0.0));
  } else if (kind == SimulationCommandKind::kConnectPeer ||
             kind == SimulationCommandKind::kDisconnectPeer) {
    command.peer_node_id =
        ScenarioCommandNodeId(object, "peer_node_id", options);
    if (command.node_id == command.peer_node_id) {
      throw std::runtime_error(
          "scenario scheduled peer command source and target must differ");
    }
  } else if (kind == SimulationCommandKind::kSetPeerCountPolicy) {
    command.peer_count_policy =
        PeerCountPolicy(JsonUint32Field(object, "minimum_peer_count"),
                        JsonUint32Field(object, "maximum_peer_count"));
  } else if (kind == SimulationCommandKind::kGenerateBlocks) {
    const std::uint32_t block_count = JsonUint32Field(object, "block_count");
    if (block_count == 0U) {
      throw std::runtime_error(
          "scenario scheduled generate_blocks count must be positive");
    }
    command.block_count = block_count;
  } else if (kind == SimulationCommandKind::kSetResourceProfile ||
             kind == SimulationCommandKind::kSetNetworkProfile) {
    command.profile = JsonStringField(object, "profile");
    RequireSafeScenarioIdentifier(*command.profile,
                                  "scenario scheduled profile name");
  } else if (kind == SimulationCommandKind::kSetResourceLimits) {
    const boost::json::value* limits = object.if_contains("resource_limits");
    if (limits == nullptr || !limits->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command resource_limits must be an object");
    }
    RejectUnsupportedFields(
        limits->as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kResourceLimits),
        "scenario scheduled command resource_limits");
    command.resource_limit_patch =
        ParseResourceLimitPatchObject(limits->as_object());
  } else if (kind == SimulationCommandKind::kSetNetworkCondition) {
    const boost::json::value* condition =
        object.if_contains("network_condition");
    if (condition == nullptr || !condition->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command network_condition must be an object");
    }
    RejectUnsupportedFields(
        condition->as_object(),
        ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition),
        "scenario scheduled command network_condition");
    command.network_condition =
        ParseNetworkConditionObject(condition->as_object());
  } else if (kind == SimulationCommandKind::kBlockNetworkFlow ||
             kind == SimulationCommandKind::kUnblockNetworkFlow) {
    const boost::json::value* flow = object.if_contains("network_flow");
    if (flow == nullptr || !flow->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command network_flow must be an object");
    }
    const boost::json::object& flow_object = flow->as_object();
    RejectUnsupportedFields(
        flow_object, ScenarioObjectFields(ScenarioObjectKind::kNetworkFlow),
        "scenario scheduled command network_flow");
    SimulationNetworkFlow parsed;
    parsed.src_address =
        JsonOptionalStringField(flow_object, "src_address", "");
    parsed.dst_address =
        JsonOptionalStringField(flow_object, "dst_address", "");
    const std::uint32_t src_port =
        JsonOptionalUint32Field(flow_object, "src_port", 0U);
    const std::uint32_t dst_port =
        JsonOptionalUint32Field(flow_object, "dst_port", 0U);
    if (src_port > 65535U || dst_port > 65535U) {
      throw std::runtime_error(
          "scenario scheduled command network flow port exceeds 65535");
    }
    parsed.src_port = static_cast<std::uint16_t>(src_port);
    parsed.dst_port = static_cast<std::uint16_t>(dst_port);
    parsed.handle = JsonOptionalUint32Field(flow_object, "handle", 0U);
    if (!parsed.src_address.empty()) {
      ValidateIpv4Address(parsed.src_address,
                          "scenario scheduled network flow source");
    }
    if (!parsed.dst_address.empty()) {
      ValidateIpv4Address(parsed.dst_address,
                          "scenario scheduled network flow destination");
    }
    if (parsed.dst_address.empty()) {
      if (kind != SimulationCommandKind::kUnblockNetworkFlow ||
          parsed.handle == 0U || parsed.dst_port != 0U) {
        throw std::runtime_error(
            "scenario scheduled handle-only network flow must be unblock");
      }
    } else if (parsed.dst_port == 0U) {
      throw std::runtime_error(
          "scenario scheduled network flow destination port must be positive");
    }
    command.network_flow = std::move(parsed);
  } else if (kind == SimulationCommandKind::kPartitionNodes ||
             kind == SimulationCommandKind::kHealPartition) {
    const boost::json::value* partition = object.if_contains("partition");
    if (partition == nullptr || !partition->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command partition must be an object");
    }
    command.partition =
        ParseSimulationPartition(partition->as_object(), options);
    if (command.partition->scope == SimulationPartitionScope::kNodePair) {
      command.node_id = command.partition->group_a.node_ids.front();
    }
  } else if (kind == SimulationCommandKind::kSetPerfCounters) {
    const boost::json::value* target = object.if_contains("perf_target");
    if (target == nullptr || !target->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command perf_target must be an object");
    }
    command.perf_counter_target =
        ParseScenarioPerfTarget(target->as_object(), options);
    command.perf_counter_kinds = ParseScenarioPerfCounters(object);
    if (command.perf_counter_target->kind != PerfCounterTargetKind::kGroup &&
        command.perf_counter_target->node_ids.size() != 1U) {
      throw std::runtime_error(
          "scenario scheduled non-group perf target must contain one node");
    }
    if (command.perf_counter_target->kind != PerfCounterTargetKind::kGroup) {
      command.node_id = command.perf_counter_target->node_ids.front();
    }
  } else if (kind == SimulationCommandKind::kSendWalletTransaction) {
    const boost::json::value* send = object.if_contains("wallet_send");
    if (send == nullptr || !send->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command wallet_send must be an object");
    }
    const boost::json::object& send_object = send->as_object();
    RejectUnsupportedFields(
        send_object, ScenarioObjectFields(ScenarioObjectKind::kWalletSend),
        "scenario scheduled command wallet_send");
    SimulationWalletSend parsed;
    parsed.sender_wallet_index =
        JsonUint32Field(send_object, "sender_wallet_index");
    parsed.receiver_wallet_index =
        JsonUint32Field(send_object, "receiver_wallet_index");
    parsed.amount_satoshis = JsonAmountField(send_object, "amount");
    parsed.fee_satoshis = JsonAmountField(send_object, "fee");
    parsed.timeout_sec = JsonUint32Field(send_object, "timeout_sec");
    const std::size_t wallet_count = options.topology.wallet_nodes.size();
    if (parsed.sender_wallet_index == 0U ||
        parsed.receiver_wallet_index == 0U ||
        parsed.sender_wallet_index > wallet_count ||
        parsed.receiver_wallet_index > wallet_count ||
        parsed.sender_wallet_index == parsed.receiver_wallet_index ||
        parsed.amount_satoshis == 0U || parsed.timeout_sec == 0U ||
        parsed.amount_satoshis >
            std::numeric_limits<std::uint64_t>::max() - parsed.fee_satoshis) {
      throw std::runtime_error(
          "scenario scheduled command wallet_send payload is invalid");
    }
    const std::uint32_t sender_node =
        options.topology.wallet_nodes[parsed.sender_wallet_index - 1U];
    const std::string sender_node_id = ScenarioNodeId(options, sender_node);
    if (command.node_id != sender_node_id) {
      throw std::runtime_error(
          "scenario scheduled command wallet sender does not match node");
    }
    command.wallet_send = parsed;
  } else if (kind == SimulationCommandKind::kAddNodes) {
    const boost::json::value* node_add = object.if_contains("node_add");
    if (node_add == nullptr || !node_add->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command node_add must be an object");
    }
    command.node_add = ParseAndValidateSimulationNodeAddRequest(
        node_add->as_object(), options);
  } else if (kind == SimulationCommandKind::kReplaceNode) {
    const boost::json::value* node_replace = object.if_contains("node_replace");
    if (node_replace == nullptr || !node_replace->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command node_replace must be an object");
    }
    command.node_replace = ParseAndValidateSimulationNodeReplaceRequest(
        node_replace->as_object(), command.node_id, options);
  } else if (kind == SimulationCommandKind::kRemoveNodes) {
    const boost::json::value* node_remove = object.if_contains("node_remove");
    if (node_remove == nullptr || !node_remove->is_object()) {
      throw std::runtime_error(
          "scenario scheduled command node_remove must be an object");
    }
    command.node_remove = ParseAndValidateSimulationNodeRemoveRequest(
        node_remove->as_object(), options);
  } else if (kind == SimulationCommandKind::kAssignRole ||
             kind == SimulationCommandKind::kRemoveRole) {
    command.role_mutation = ParseScenarioRoleMutation(object, kind, options);
  }
  command.scheduled_event_sequence = 1U;
  SimulationCommandQueue validation_queue;
  validation_queue.PushScenarioCommand(command);
  command.scheduled_event_sequence.reset();
  return command;
}

}  // namespace bbp::simulator_app_internal
