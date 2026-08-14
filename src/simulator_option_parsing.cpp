#include "simulator_option_parsing.h"

#include <algorithm>
#include <boost/json/parse.hpp>
#include <boost/json/value.hpp>
#include <boost/program_options.hpp>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/positive_duration.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/legacy_cli_inputs.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_peer_topology_decoding.h"
#include "simulator_resource_limit_decoding.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_scenario_option_application.h"
#include "simulator_wallet_transaction_validation.h"
#include "simulator_yaml_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

std::uint32_t ParseCliUint32Text(std::string_view text,
                                 std::string_view option) {
  if (!text.empty() && text.front() == '+') {
    text.remove_prefix(1U);
  }
  std::uint32_t value = 0U;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value, 10);
  if (text.empty() || error != std::errc{} ||
      end != text.data() + text.size()) {
    throw std::runtime_error(
        std::string(option) + " must be an unsigned decimal integer in 0.." +
        std::to_string(std::numeric_limits<std::uint32_t>::max()));
  }
  return value;
}

const PeerConnectivityPolicy* FindPeerConnectivityPolicy(
    const NodeRoleTopology& topology, uint32_t node_index) {
  for (const PeerConnectivityPolicy& policy : topology.peer_connectivity) {
    if (policy.node == node_index) {
      return &policy;
    }
  }
  return nullptr;
}

bool WorkloadsRequireIsolatedNetwork(const Options& options) {
  for (const ScenarioWorkload& workload : options.workloads) {
    if (workload.kind == WorkloadKind::kPartitionNodes ||
        workload.kind == WorkloadKind::kHealPartition ||
        workload.kind == WorkloadKind::kSetNetworkCondition ||
        workload.kind == WorkloadKind::kBlockNetworkFlow ||
        workload.kind == WorkloadKind::kUnblockNetworkFlow ||
        workload.kind == WorkloadKind::kSetNetworkProfile) {
      return true;
    }
  }
  for (const ScheduledScenarioEvent& event : options.scheduled_events) {
    if (const auto* workload = std::get_if<ScenarioWorkload>(&event.action)) {
      if (workload->kind == WorkloadKind::kPartitionNodes ||
          workload->kind == WorkloadKind::kHealPartition ||
          workload->kind == WorkloadKind::kSetNetworkCondition ||
          workload->kind == WorkloadKind::kBlockNetworkFlow ||
          workload->kind == WorkloadKind::kUnblockNetworkFlow ||
          workload->kind == WorkloadKind::kSetNetworkProfile) {
        return true;
      }
      continue;
    }
    const SimulationCommandKind command_kind =
        std::get<SimulationCommand>(event.action).kind;
    if (command_kind == SimulationCommandKind::kPartitionNodes ||
        command_kind == SimulationCommandKind::kHealPartition ||
        command_kind == SimulationCommandKind::kSetNetworkCondition ||
        command_kind == SimulationCommandKind::kBlockNetworkFlow ||
        command_kind == SimulationCommandKind::kUnblockNetworkFlow ||
        command_kind == SimulationCommandKind::kSetNetworkProfile) {
      return true;
    }
  }
  return false;
}

struct ConfiguredScenarioAction {
  ScenarioWorkload workload;
  std::uint32_t available_node_count;
};

std::vector<ConfiguredScenarioAction> ConfiguredScenarioActions(
    const Options& options) {
  std::vector<ConfiguredScenarioAction> actions;
  actions.reserve(options.workloads.size() + options.scheduled_events.size());
  for (const ScenarioWorkload& workload : options.workloads) {
    actions.push_back(ConfiguredScenarioAction{
        .workload = workload, .available_node_count = options.nodes});
  }
  std::uint32_t planned_node_count = options.nodes;
  for (const ScheduledScenarioEvent& event :
       OrderScheduledScenarioEvents(options.scheduled_events)) {
    if (const auto* workload = std::get_if<ScenarioWorkload>(&event.action)) {
      actions.push_back(ConfiguredScenarioAction{
          .workload = *workload, .available_node_count = planned_node_count});
      continue;
    }
    const SimulationCommand& command =
        std::get<SimulationCommand>(event.action);
    if (command.kind == SimulationCommandKind::kAddNodes) {
      planned_node_count += command.node_add->count;
    } else if (command.kind == SimulationCommandKind::kRemoveNodes) {
      const std::uint32_t removed_count =
          static_cast<std::uint32_t>(command.node_remove->node_ids.size());
      if (removed_count > planned_node_count) {
        throw std::logic_error("configured node.remove count would underflow");
      }
      planned_node_count -= removed_count;
    }
  }
  return actions;
}

bool IsTopologyEdgeAction(WorkloadKind kind) {
  return kind == WorkloadKind::kSetEdgeCondition ||
         kind == WorkloadKind::kActivateEdge ||
         kind == WorkloadKind::kDeactivateEdge ||
         kind == WorkloadKind::kRestoreEdge;
}

std::vector<ScenarioWorkload> OrderedConfiguredScenarioActions(
    const Options& options) {
  std::vector<ScenarioWorkload> actions = options.workloads;
  for (const ScheduledScenarioEvent& event :
       OrderScheduledScenarioEvents(options.scheduled_events)) {
    if (const auto* workload = std::get_if<ScenarioWorkload>(&event.action)) {
      actions.push_back(*workload);
    }
  }
  return actions;
}

void ValidateRuntimeTopologyPeerPolicy(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    std::uint32_t node_index) {
  const PeerConnectivityPolicy* policy =
      FindPeerConnectivityPolicy(options.topology, node_index);
  if (policy != nullptr &&
      policy->peer_count.minimum() >
          runtime_topology.ActivePeerIndexes(node_index).size()) {
    throw std::runtime_error(
        "peer policy minimum exceeds allowed logical peers after topology "
        "edge action");
  }
}

bool ValidateRuntimeTopologyActionSequence(
    const Options& options, RuntimePeerTopology* runtime_topology) {
  bool requires_isolated_network = false;
  for (const ScenarioWorkload& action :
       OrderedConfiguredScenarioActions(options)) {
    if (IsTopologyEdgeAction(action.kind)) {
      const TopologyEdgeWorkload& edge = action.topology_edge;
      const std::uint32_t from = edge.from - 1U;
      const std::uint32_t to = edge.to - 1U;
      if (action.kind == WorkloadKind::kSetEdgeCondition) {
        if (!edge.condition) {
          throw std::runtime_error(
              "set_edge_condition action is missing its typed condition");
        }
        static_cast<void>(
            runtime_topology->SetCondition(from, to, *edge.condition));
      } else if (action.kind == WorkloadKind::kActivateEdge) {
        static_cast<void>(runtime_topology->SetActive(from, to, true));
      } else if (action.kind == WorkloadKind::kDeactivateEdge) {
        static_cast<void>(runtime_topology->SetActive(from, to, false));
      } else {
        static_cast<void>(runtime_topology->RestoreBaseline(from, to));
      }
      ValidateRuntimeTopologyPeerPolicy(options, *runtime_topology, from);
      const RuntimePeerTopologyEdge& current = runtime_topology->Edge(from, to);
      requires_isolated_network =
          requires_isolated_network || (current.active && current.condition);
    } else if (action.kind == WorkloadKind::kConnectPeer) {
      const std::uint32_t from = action.connect_peer.node - 1U;
      const std::uint32_t to = action.connect_peer.peer - 1U;
      if (!runtime_topology->Edge(from, to).active) {
        throw std::runtime_error(
            "scenario connect_peer target is not active at this point in "
            "the topology action sequence");
      }
    }
  }
  return requires_isolated_network;
}

void ParseNodeNetworkConditionTexts(
    const std::vector<std::string>& texts, uint32_t nodes,
    std::string_view option_name,
    std::map<uint32_t, NetworkCondition>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kNodeNetworkCondition),
        option_name);
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(option_name) +
                               " node must be in 1..--nodes");
    }
    output[node - 1U] = ParseNetworkConditionObject(object);
  }
}

void ParseRuntimeNodeResourceTexts(
    const std::vector<std::string>& texts, uint32_t nodes,
    std::map<uint32_t, ResourceLimitPatch>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-resource-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object,
        ScenarioObjectFields(ScenarioObjectKind::kRuntimeResourceLimits),
        "--runtime-node-resource-json");
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-resource-json node must be in 1..--nodes");
    }
    output[node - 1U] = ParseResourceLimitPatchObject(object);
  }
}

void ParseRuntimeNodeBlockTexts(const std::vector<std::string>& texts,
                                uint32_t nodes, std::string_view option_name,
                                std::vector<NetworkBlockRule>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kNetworkBlockRule),
        option_name);
    NetworkBlockRule rule = ParseNetworkBlockRuleObject(object);
    if (rule.node_index >= nodes) {
      throw std::runtime_error(std::string(option_name) +
                               " node must be in 1..--nodes");
    }
    output.push_back(std::move(rule));
  }
}

void ParseRuntimePartitionTexts(const std::vector<std::string>& texts,
                                uint32_t nodes, std::string_view option_name,
                                std::vector<NetworkPartitionRule>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kNetworkPartition),
        option_name);
    NetworkPartitionRule rule = ParseNetworkPartitionRuleObject(object);
    for (uint32_t node_index : rule.group_a) {
      if (node_index >= nodes) {
        throw std::runtime_error(std::string(option_name) +
                                 " group_a node must be in 1..--nodes");
      }
    }
    for (uint32_t node_index : rule.group_b) {
      if (node_index >= nodes) {
        throw std::runtime_error(std::string(option_name) +
                                 " group_b node must be in 1..--nodes");
      }
    }
    output.push_back(std::move(rule));
  }
}

void ParseRuntimeNodeRestartTexts(const std::vector<std::string>& texts,
                                  uint32_t nodes,
                                  std::vector<uint32_t>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-restart-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kProcessRestart),
        "--runtime-node-restart-json");
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-restart-json node must be in 1..--nodes");
    }
    output.push_back(node - 1U);
  }
}

void ParseRuntimeNodeFreezeTexts(const std::vector<std::string>& texts,
                                 uint32_t nodes,
                                 std::vector<FreezeRequest>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-freeze-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kProcessFreeze),
        "--runtime-node-freeze-json");
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-freeze-json node must be in 1..--nodes");
    }
    const uint32_t duration_ms = JsonUint32Field(object, "duration_ms");
    if (duration_ms == 0U) {
      throw std::runtime_error(
          "--runtime-node-freeze-json duration_ms must be greater than zero");
    }
    output.push_back(
        FreezeRequest{.node_index = node - 1U, .duration_ms = duration_ms});
  }
}

void ParseLegacyCliInputs(const LegacyCliInputs& inputs, Options& options) {
  ParseNodeNetworkConditionTexts(inputs.node_network_conditions, options.nodes,
                                 "--node-network-condition-json",
                                 options.node_network_conditions);
  ParseNodeNetworkConditionTexts(inputs.runtime_node_network_conditions,
                                 options.nodes,
                                 "--runtime-node-network-condition-json",
                                 options.runtime_node_network_conditions);
  ParseRuntimeNodeBlockTexts(inputs.runtime_node_blocks, options.nodes,
                             "--runtime-node-block-json",
                             options.runtime_node_blocks);
  ParseRuntimeNodeBlockTexts(inputs.runtime_node_unblocks, options.nodes,
                             "--runtime-node-unblock-json",
                             options.runtime_node_unblocks);
  ParseRuntimePartitionTexts(inputs.runtime_partitions, options.nodes,
                             "--runtime-partition-json",
                             options.runtime_partitions);
  ParseRuntimePartitionTexts(inputs.runtime_partition_heals, options.nodes,
                             "--runtime-heal-partition-json",
                             options.runtime_partition_heals);
  ParseRuntimeNodeResourceTexts(inputs.runtime_node_resources, options.nodes,
                                options.runtime_node_resource_updates);
  ParseRuntimeNodeRestartTexts(inputs.runtime_node_restarts, options.nodes,
                               options.runtime_node_restarts);
  ParseRuntimeNodeFreezeTexts(inputs.runtime_node_freezes, options.nodes,
                              options.runtime_node_freezes);
}

bool TopologyHasDirectionalNetworkConditions(const Options& options);

void ApplyDirectTransactionLoadOptions(
    const boost::program_options::variables_map& vm,
    std::string_view strategy_name, std::uint32_t wallet_node_count,
    Options* options) {
  const bool strategy_provided =
      OptionProvided(vm, "transaction-load-strategy");
  const bool wallet_count_provided = OptionProvided(vm, "wallet-node-count");
  if (!strategy_provided && !wallet_count_provided) {
    return;
  }
  if (!strategy_provided) {
    throw std::runtime_error(
        "--wallet-node-count requires --transaction-load-strategy");
  }
  if (!wallet_count_provided) {
    throw std::runtime_error(
        "--transaction-load-strategy requires --wallet-node-count");
  }
  if (!OptionProvided(vm, "nodes")) {
    throw std::runtime_error(
        "--transaction-load-strategy requires an explicit --nodes value");
  }
  if (OptionProvided(vm, "scenario") || OptionProvided(vm, "scenario-json") ||
      OptionProvided(vm, "scenario-yaml")) {
    throw std::runtime_error(
        "direct transaction load options must not be combined with a "
        "scenario; use the scenario workload for advanced overrides");
  }
  if (OptionProvided(vm, "run") || OptionProvided(vm, "report-run") ||
      OptionProvided(vm, "cleanup-run")) {
    throw std::runtime_error(
        "direct transaction load options require a new simulation run");
  }
  if (OptionProvided(vm, "generate-node")) {
    throw std::runtime_error(
        "direct transaction load selects the first non-wallet node as its "
        "miner; use a scenario for a custom miner");
  }
  const std::uint32_t maximum_nodes =
      ChainDriverSpecFor(options->chain).max_nodes;
  if (options->nodes < 1U || options->nodes > maximum_nodes) {
    throw std::runtime_error("--nodes currently supports 1.." +
                             std::to_string(maximum_nodes) +
                             " for chain smoke runs");
  }
  if (wallet_node_count < 2U) {
    throw std::runtime_error(
        "--wallet-node-count must be at least 2 for transaction load");
  }
  if (wallet_node_count > options->nodes) {
    throw std::runtime_error("--wallet-node-count must not exceed --nodes");
  }

  const WalletTransferStrategy strategy =
      ParseWalletTransferStrategy(strategy_name);
  if (!IsTransactionLoadStrategy(strategy)) {
    throw std::runtime_error(
        "--transaction-load-strategy must be random_bruteforce or "
        "equal_fanout");
  }

  options->topology.configured = true;
  options->topology.node_count = options->nodes;
  options->topology.wallet_node_count = wallet_node_count;
  options->topology.miner_node_count = 1U;
  options->topology.allow_miner_wallet_overlap =
      wallet_node_count == options->nodes;
  options->topology.wallet_nodes =
      ConsecutiveNodeIndexes(0U, wallet_node_count);
  const std::uint32_t miner_node = options->topology.allow_miner_wallet_overlap
                                       ? wallet_node_count - 1U
                                       : wallet_node_count;
  options->topology.miner_nodes = {miner_node};
  options->topology.peer_topology.kind = PeerTopologyKind::kFullMesh;
  options->wallet_initialization =
      WalletInitialization{.strategy = WalletInitializationStrategy::kDriverRpc,
                           .mode = WalletPrivacyMode::kPublic};
  options->generate_node = miner_node + 1U;

  WalletTransactionsWorkload load;
  load.funding_strategy = WalletFundingStrategy::kRoundRobin;
  load.strategy = strategy;
  const std::uint32_t coinbase_confirmations =
      ChainDriverSpecFor(options->chain).coinbase_spendable_confirmations;
  load.funding_blocks_per_wallet = coinbase_confirmations;
  load.readiness_confirmations = coinbase_confirmations;
  load.transaction_rate = WalletTransactionRate::FromDouble(2.0);
  load.concurrency = 2U;
  load.queue_capacity = strategy == WalletTransferStrategy::kRandomBruteforce
                            ? std::max<std::uint32_t>(8U, wallet_node_count)
                            : 8U;
  load.mode = WalletPrivacyMode::kPublic;
  load.amount = AmountDistribution{
      .kind = ValueDistributionKind::kUniform,
      .minimum_satoshis = 1'000'000U,
      .maximum_satoshis = 10'000'000U,
  };
  load.fee_policy = WalletTransactionFeePolicy::kFixed;
  load.fee_satoshis = 1'000U;
  load.fee_reserve_satoshis =
      CreateChainDriver(options->chain)
          ->WalletTransactionFeeReserveSatoshis(ToChainWalletMode(load.mode),
                                                load.fee_satoshis);
  load.funding_threshold_satoshis =
      load.amount.maximum_satoshis + load.fee_reserve_satoshis;
  if (strategy == WalletTransferStrategy::kRandomBruteforce) {
    load.retained_balance_basis_points = 8'000U;
  }
  load.random_seed = options->simulation_seed;
  load.timeout_sec = options->sync_timeout_sec;

  if (strategy == WalletTransferStrategy::kEqualFanout) {
    const std::uint32_t sender_count = wallet_node_count / 2U;
    for (std::uint32_t wallet = 1U; wallet <= sender_count; ++wallet) {
      load.sender_wallets.push_back(wallet);
    }
    for (std::uint32_t wallet = sender_count + 1U; wallet <= wallet_node_count;
         ++wallet) {
      load.receiver_wallets.push_back(wallet);
    }
  }
  ScenarioWorkload workload;
  workload.kind = WorkloadKind::kWalletTransactions;
  workload.wallet_transactions = std::move(load);
  options->workloads.push_back(std::move(workload));
  options->workloads_configured = true;
  options->wallet_backed_workload_requested = true;
}

bool TopologyHasDirectionalNetworkConditions(const Options& options) {
  if (options.empty_control_plane) {
    return false;
  }
  const std::vector<ResolvedPeerTopologyEdge> edges =
      ResolvePeerTopologyEdges(options.topology.peer_topology, options.nodes);
  return std::any_of(edges.begin(), edges.end(), [](const auto& edge) {
    return edge.condition.has_value();
  });
}

}  // namespace

void ValidateScenarioWorkload(const ScenarioWorkload& workload,
                              std::uint32_t available_node_count,
                              const Options& options) {
  if (workload.kind == WorkloadKind::kBlockGeneration) {
    if (workload.block_generation.node == 0U ||
        workload.block_generation.node > available_node_count) {
      throw std::runtime_error(
          "scenario block_generation workload node must be in 1..--nodes");
    }
  } else if (workload.kind == WorkloadKind::kWaitUntilHeight) {
    if (workload.wait_until_height.node == 0U ||
        workload.wait_until_height.node > available_node_count) {
      throw std::runtime_error(
          "scenario wait_until_height workload node must be in 1..--nodes");
    }
    if (workload.wait_until_height.timeout_sec == 0U) {
      throw std::runtime_error(
          "scenario wait_until_height timeout_sec must be greater than zero");
    }
  } else if (workload.kind == WorkloadKind::kWaitForPeers) {
    if (workload.wait_for_peers.node == 0U ||
        workload.wait_for_peers.node > available_node_count) {
      throw std::runtime_error(
          "scenario wait_for_peers workload node must be in 1..--nodes");
    }
    if (workload.wait_for_peers.peer_count == 0U) {
      throw std::runtime_error(
          "scenario wait_for_peers peer_count must be greater than zero");
    }
    if (workload.wait_for_peers.timeout_sec == 0U) {
      throw std::runtime_error(
          "scenario wait_for_peers timeout_sec must be greater than zero");
    }
  } else if (workload.kind == WorkloadKind::kConnectPeer) {
    if (workload.connect_peer.node == 0U ||
        workload.connect_peer.node > available_node_count) {
      throw std::runtime_error(
          "scenario connect_peer workload node must be in 1..--nodes");
    }
    if (workload.connect_peer.peer == 0U ||
        workload.connect_peer.peer > available_node_count) {
      throw std::runtime_error(
          "scenario connect_peer workload peer must be in 1..--nodes");
    }
    if (workload.connect_peer.node == workload.connect_peer.peer) {
      throw std::runtime_error(
          "scenario connect_peer workload node and peer must differ");
    }
    if (workload.connect_peer.timeout_sec == 0U) {
      throw std::runtime_error(
          "scenario connect_peer timeout_sec must be greater than zero");
    }
  } else if (workload.kind == WorkloadKind::kDisconnectPeer) {
    if (workload.disconnect_peer.node == 0U ||
        workload.disconnect_peer.node > available_node_count) {
      throw std::runtime_error(
          "scenario disconnect_peer workload node must be in 1..--nodes");
    }
    if (workload.disconnect_peer.peer == 0U ||
        workload.disconnect_peer.peer > available_node_count) {
      throw std::runtime_error(
          "scenario disconnect_peer workload peer must be in 1..--nodes");
    }
    if (workload.disconnect_peer.node == workload.disconnect_peer.peer) {
      throw std::runtime_error(
          "scenario disconnect_peer workload node and peer must differ");
    }
    if (workload.disconnect_peer.timeout_sec == 0U) {
      throw std::runtime_error(
          "scenario disconnect_peer timeout_sec must be greater than zero");
    }
  } else if (workload.kind == WorkloadKind::kRestartNode) {
    if (workload.restart_node.node == 0U ||
        workload.restart_node.node > available_node_count) {
      throw std::runtime_error(
          "scenario restart_node workload node must be in 1..--nodes");
    }
  } else if (workload.kind == WorkloadKind::kFreezeNode) {
    if (workload.freeze_node.node == 0U ||
        workload.freeze_node.node > available_node_count) {
      throw std::runtime_error(
          "scenario freeze_node workload node must be in 1..--nodes");
    }
    if (workload.freeze_node.duration_ms == 0U) {
      throw std::runtime_error(
          "scenario freeze_node duration_ms must be greater than zero");
    }
  } else if (workload.kind == WorkloadKind::kUpdateResourceLimits) {
    if (workload.update_resource_limits.node == 0U ||
        workload.update_resource_limits.node > available_node_count) {
      throw std::runtime_error(
          "scenario update_resource_limits workload node must be in "
          "1..--nodes");
    }
  } else if (workload.kind == WorkloadKind::kSetResourceProfile ||
             workload.kind == WorkloadKind::kSetNetworkProfile) {
    if (workload.profile_switch.nodes.empty()) {
      throw std::runtime_error(
          "scenario profile switch workload requires target nodes");
    }
    for (const uint32_t node : workload.profile_switch.nodes) {
      if (node == 0U || node > available_node_count) {
        throw std::runtime_error(
            "scenario profile switch workload node must be in "
            "1..--nodes");
      }
    }
  } else if (workload.kind == WorkloadKind::kResourcePressure) {
    if (workload.resource_pressure.node == 0U ||
        workload.resource_pressure.node > available_node_count) {
      throw std::runtime_error(
          "scenario resource_pressure workload node must be in "
          "1..--nodes");
    }
    if (workload.resource_pressure.duration_ms == 0U) {
      throw std::runtime_error(
          "scenario resource_pressure duration_ms must be greater than "
          "zero");
    }
  } else if (workload.kind == WorkloadKind::kSetNetworkCondition) {
    if (workload.network_condition.node == 0U ||
        workload.network_condition.node > available_node_count) {
      throw std::runtime_error(
          "scenario set_network_condition workload node must be in "
          "1..--nodes");
    }
    ValidateNetworkCondition(workload.network_condition.condition);
  } else if (workload.kind == WorkloadKind::kBlockNetworkFlow ||
             workload.kind == WorkloadKind::kUnblockNetworkFlow) {
    if (workload.network_block.rule.node_index >= available_node_count) {
      throw std::runtime_error(
          "scenario network flow workload node must be in 1..--nodes");
    }
  } else if (workload.kind == WorkloadKind::kPartitionNodes) {
    ValidateNetworkPartitionRule(workload.network_partition.partition,
                                 available_node_count,
                                 "scenario partition_nodes workload");
  } else if (workload.kind == WorkloadKind::kHealPartition) {
    ValidateNetworkPartitionRule(workload.network_partition.partition,
                                 available_node_count,
                                 "scenario heal_partition workload");
  } else if (IsTopologyEdgeAction(workload.kind)) {
    const TopologyEdgeWorkload& edge = workload.topology_edge;
    if (edge.from == 0U || edge.from > available_node_count) {
      throw std::runtime_error(
          "scenario topology edge action from must be in 1..--nodes");
    }
    if (edge.to == 0U || edge.to > available_node_count) {
      throw std::runtime_error(
          "scenario topology edge action to must be in 1..--nodes");
    }
    if (edge.from == edge.to) {
      throw std::runtime_error(
          "scenario topology edge action from and to must differ");
    }
    if (workload.kind == WorkloadKind::kSetEdgeCondition) {
      if (!edge.condition) {
        throw std::runtime_error(
            "scenario set_edge_condition requires condition fields");
      }
      ValidateNetworkCondition(*edge.condition);
    } else if (edge.timeout_sec == 0U) {
      throw std::runtime_error(
          "scenario topology edge action timeout_sec must be greater than "
          "zero");
    }
  } else if (workload.kind == WorkloadKind::kSendRawTransaction) {
    const SendRawTransactionWorkload& transaction =
        workload.send_raw_transaction;
    if (transaction.funding_node == 0U ||
        transaction.funding_node > available_node_count) {
      throw std::runtime_error(
          "scenario send_raw_transaction funding_node must be in "
          "1..--nodes");
    }
    if (transaction.submit_node == 0U ||
        transaction.submit_node > available_node_count) {
      throw std::runtime_error(
          "scenario send_raw_transaction submit_node must be in 1..--nodes");
    }
    if (transaction.source_address == transaction.destination_address) {
      throw std::runtime_error(
          "scenario send_raw_transaction source_address and "
          "destination_address must differ");
    }
    if (transaction.funding_blocks < kDefaultCoinbaseSpendableConfirmations) {
      throw std::runtime_error(
          "scenario send_raw_transaction funding_blocks must be at least " +
          std::to_string(kDefaultCoinbaseSpendableConfirmations));
    }
    if (transaction.amount_satoshis == 0U) {
      throw std::runtime_error(
          "scenario send_raw_transaction amount must be greater than zero");
    }
    if (transaction.fee_satoshis == 0U) {
      throw std::runtime_error(
          "scenario send_raw_transaction fee must be greater than zero");
    }
    if (transaction.amount_satoshis >
        std::numeric_limits<uint64_t>::max() - transaction.fee_satoshis) {
      throw std::runtime_error(
          "scenario send_raw_transaction amount plus fee overflows uint64");
    }
    if (transaction.timeout_sec == 0U) {
      throw std::runtime_error(
          "scenario send_raw_transaction timeout_sec must be greater than "
          "zero");
    }
  } else if (workload.kind == WorkloadKind::kWalletTransactions) {
    ValidateWalletTransactionsWorkload(workload.wallet_transactions, options);
  }
}

Options ParseOptions(int argc, char** argv,
                     const boost::json::object* in_memory_scenario = nullptr) {
  namespace po = boost::program_options;
  if (in_memory_scenario != nullptr && (argc != 0 || argv != nullptr)) {
    throw std::logic_error(
        "in-memory scenario parsing must not receive command-line arguments");
  }
  Options options;
  LegacyCliInputs legacy_inputs;
  const ChainDriverSpec& default_chain_spec = DefaultChainDriverSpec();
  std::string chain_name = std::string(ChainKindName(options.chain));
  std::string log_level_name = std::string(LogLevelName(options.log_level));
  std::string metrics_interval_text = "1s";
  std::uint32_t legacy_metrics_interval_ms = 1000U;
  std::uint32_t block_production_period_ms = 1000U;
  double block_production_probability = 0.5;
  std::uint64_t block_production_seed = 0U;
  double mining_difficulty = 1.0;
  bool no_mining = false;
  bool native_mining = false;
  bool keep_cgroups = false;
  bool isolate_network = false;
  bool no_isolate_network = false;
  std::string cleanup_run_id;
  std::uint32_t wallet_node_count = 0U;
  std::string transaction_load_strategy;
  std::string ready_timeout_text;
  std::string sync_timeout_text;

  const std::string nodes_help =
      "chain regtest nodes, 1.." + std::to_string(default_chain_spec.max_nodes);
  po::options_description canonical_options(
      "Blockchain Benchmark Project options");
  canonical_options.add_options()("help", "show this help")(
      "scenario", po::value<std::filesystem::path>(),
      "JSON or YAML scenario file")("chain", po::value<std::string>(),
                                    "firo, bitcoin, or monero")(
      "node-binary", po::value<std::filesystem::path>(),
      "daemon binary for the selected chain")(
      "benchmark-root", po::value<std::filesystem::path>(),
      "root directory for all benchmark artifacts")(
      "run-id", po::value<std::string>(), "optional stable run identifier")(
      "replace-run", "replace a validated simulator-owned run directory")(
      "cleanup-run", po::value<std::string>()->implicit_value(""),
      "clean a previous run, optionally by run id")(
      "report-run", po::value<std::filesystem::path>(),
      "report a previous run id or directory")(
      "run", po::value<std::filesystem::path>(),
      "view a previous run id or directory in the TUI")(
      "no-tui", "run a new simulation headlessly")(
      "log-level", po::value<std::string>(),
      "trace, debug, info, warning, error, or fatal")(
      "metrics-interval", po::value<std::string>(),
      "metrics interval with an explicit unit, such as 250ms or 1s")(
      "nodes", po::value<std::uint32_t>(),
      "total nodes for a direct transaction-load run")(
      "node-capacity", po::value<std::uint32_t>(),
      "maximum live nodes for this run; defaults to the chain limit")(
      "wallet-node-count", po::value<std::uint32_t>(),
      "wallet nodes for a direct transaction-load run (minimum 2)")(
      "transaction-load-strategy", po::value<std::string>(),
      "direct random_bruteforce or equal_fanout load; defaults: full mesh, "
      "isolated namespaces, one miner (the final wallet when all nodes are "
      "wallets), public driver wallets, all-wallet random redistribution "
      "retaining 80 percent or one complete equal fan-out at 2 tx/s, "
      "concurrency 2, queue 8, uniform 0.01..0.10 coin random amounts, "
      "fixed 0.00001000 coin fee, simulation seed, and continuous metrics "
      "until explicit stop")(
      "isolate-network",
      "explicitly select the default per-node isolated networking mode")(
      "no-isolate-network",
      "explicitly opt out of per-node isolation and use loopback networking")(
      "metrics-sample-count", po::value<std::uint32_t>(),
      "periodic metric sample limit; zero keeps the run active until explicit "
      "stop, while a positive count makes the run finite")(
      "keep-artifacts", "preserve run artifacts")(
      "once", "render one frame when viewing a previous run");
  po::options_description desc("Allowed options");
  desc.add_options()("help", "show this help")(
      "scenario", po::value<std::filesystem::path>(&options.scenario),
      "JSON or YAML scenario file selected by extension")(
      "scenario-json", po::value<std::filesystem::path>(&options.scenario_json),
      "legacy Boost.JSON scenario file option")(
      "scenario-yaml", po::value<std::filesystem::path>(&options.scenario_yaml),
      "legacy libyaml scenario file option")(
      "chain", po::value<std::string>(&chain_name),
      "chain driver: firo, bitcoin, or monero")(
      "log-level", po::value<std::string>(&log_level_name),
      "minimum Boost.Log severity: trace, debug, info, warning, error, or "
      "fatal")("node-binary",
               po::value<std::filesystem::path>(&options.chain_daemon),
               "daemon binary for the selected chain")(
      "chain-daemon", po::value<std::filesystem::path>(&options.chain_daemon),
      "legacy alias for --node-binary")(
      default_chain_spec.daemon_option_name.c_str(),
      po::value<std::filesystem::path>(&options.chain_daemon),
      "legacy Firo daemon binary alias")(
      "benchmark-root", po::value<std::filesystem::path>(&options.output_dir),
      "root directory for run data, node directories, metrics, events, and "
      "logs")("output-dir",
              po::value<std::filesystem::path>(&options.output_dir),
              "legacy alias for --benchmark-root")(
      "run-id", po::value<std::string>(&options.run_id), "safe run id")(
      "report-run", po::value<std::filesystem::path>(&options.report_run),
      "summarize an existing run directory as JSON and exit")(
      "run", po::value<std::filesystem::path>(&options.tui_run),
      "view an existing run directory in the integrated ncurses TUI")(
      "no-tui", po::bool_switch(&options.no_tui),
      "run a new benchmark headlessly with Boost.Log output instead of the "
      "integrated ncurses TUI")(
      "once", po::bool_switch(&options.tui_once),
      "render one integrated TUI frame and exit; requires --run")(
      "refresh-ms", po::value<std::uint32_t>(&options.tui_refresh_ms),
      "milliseconds between integrated TUI report refreshes")(
      "nodes", po::value<uint32_t>(&options.nodes), nodes_help.c_str())(
      "node-capacity", po::value<std::uint32_t>(&options.node_capacity),
      "maximum live nodes for this run; defaults to the selected chain limit")(
      "wallet-node-count", po::value<std::uint32_t>(&wallet_node_count),
      "wallet nodes for direct transaction load; assigns wallets first and "
      "one miner next, or overlaps the final wallet when all nodes are "
      "wallets")(
      "transaction-load-strategy",
      po::value<std::string>(&transaction_load_strategy),
      "direct random_bruteforce or equal_fanout transaction load using "
      "bounded isolated defaults")(
      "generate-node", po::value<uint32_t>(&options.generate_node),
      "default 1-based miner node when no topology is configured")(
      "no-mining", po::bool_switch(&no_mining),
      "disable scheduled block production")(
      "native-mining", po::bool_switch(&native_mining),
      "use the chain's native continuous miner instead of scheduled block "
      "production")(
      "block-production-period-ms",
      po::value<std::uint32_t>(&block_production_period_ms),
      "milliseconds between global Bernoulli block-production draws")(
      "block-production-probability",
      po::value<double>(&block_production_probability),
      "probability in [0,1] of producing one block at each draw")(
      "block-production-seed", po::value<std::uint64_t>(&block_production_seed),
      "reproducible global Bernoulli scheduler seed")(
      "mining-difficulty", po::value<double>(&mining_difficulty),
      "chain-specific mining difficulty requested for configured miners")(
      "ready-timeout-sec", po::value<std::string>(&ready_timeout_text),
      "RPC startup timeout")("sync-timeout-sec",
                             po::value<std::string>(&sync_timeout_text),
                             "block propagation timeout")(
      "metrics-sample-count",
      po::value<uint32_t>(&options.metrics_sample_count),
      "periodic metric sample limit; zero keeps the run active until explicit "
      "stop, while a positive count makes the run finite")(
      "metrics-interval", po::value<std::string>(&metrics_interval_text),
      "typed interval between metric and node-log samples, such as 250ms or "
      "1s")("metrics-interval-ms",
            po::value<uint32_t>(&legacy_metrics_interval_ms),
            "legacy millisecond alias for --metrics-interval")(
      "memory-high-bytes", po::value<uint64_t>(&options.memory_high_bytes),
      "cgroup memory.high soft pressure threshold in bytes")(
      "memory-max-bytes", po::value<uint64_t>(&options.memory_max_bytes),
      "cgroup memory.max hard limit in bytes")(
      "cpu-quota-us", po::value<uint64_t>(&options.cpu_quota_us),
      "optional cgroup cpu.max quota in microseconds per period")(
      "cpu-period-us", po::value<uint64_t>(&options.cpu_period_us),
      "cgroup cpu.max period in microseconds")(
      "cpu-weight", po::value<uint64_t>(&options.cpu_weight),
      "cgroup cpu.weight proportional share in 1..10000")(
      "io-weight", po::value<uint64_t>(&options.io_weight),
      "cgroup default io.weight proportional share in 1..10000")(
      "pids-max", po::value<uint64_t>(&options.pids_max),
      "cgroup pids.max process limit")(
      "keep-artifacts",
      po::value<bool>(&options.keep_artifacts)
          ->zero_tokens()
          ->implicit_value(true)
          ->default_value(true),
      "preserve run data, logs, metrics, events, and resolved scenarios")(
      "keep-cgroups", po::bool_switch(&keep_cgroups),
      "leave cgroups after exit for inspection")(
      "cleanup-run",
      po::value<std::string>(&cleanup_run_id)->implicit_value(""),
      "remove stale simulator-owned objects for the optional run id and exit")(
      "isolate-network", po::bool_switch(&isolate_network),
      "explicitly run each chain node in its own network namespace and veth "
      "link")("no-isolate-network", po::bool_switch(&no_isolate_network),
              "explicitly use loopback-only node networking")(
      "network-bandwidth-kbps",
      po::value<uint32_t>(&options.network_condition.bandwidth_kbps),
      "TBF bandwidth limit in decimal kilobytes per second for each isolated "
      "node host-side veth; 0 means unlimited")(
      "network-delay-ms",
      po::value<uint32_t>(&options.network_condition.delay_ms),
      "netem delay applied to each isolated node host-side veth")(
      "network-jitter-ms",
      po::value<uint32_t>(&options.network_condition.jitter_ms),
      "netem jitter applied to each isolated node host-side veth")(
      "network-loss-bps",
      po::value<uint32_t>(&options.network_condition.loss_basis_points),
      "netem packet loss in basis points, 10000 = 100%")(
      "network-duplicate-bps",
      po::value<uint32_t>(&options.network_condition.duplicate_basis_points),
      "netem packet duplication in basis points, 10000 = 100%")(
      "network-corrupt-bps",
      po::value<uint32_t>(&options.network_condition.corrupt_basis_points),
      "netem packet corruption in basis points, 10000 = 100%")(
      "network-reorder-bps",
      po::value<uint32_t>(&options.network_condition.reorder_basis_points),
      "netem packet reordering in basis points, 10000 = 100%")(
      "network-limit-packets",
      po::value<uint32_t>(&options.network_condition.limit_packets),
      "netem queue limit applied to each isolated node host-side veth")(
      "node-network-condition-json",
      po::value<std::vector<std::string>>(
          &legacy_inputs.node_network_conditions)
          ->composing(),
      "repeatable JSON object with node plus network condition fields for one "
      "isolated "
      "node")(
      "runtime-node-network-condition-json",
      po::value<std::vector<std::string>>(
          &legacy_inputs.runtime_node_network_conditions)
          ->composing(),
      "repeatable JSON object with node plus live network condition fields to "
      "apply after isolated nodes are running")(
      "runtime-node-block-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_blocks)
          ->composing(),
      "repeatable JSON object with node, optional src_address and src_port, "
      "dst_address, dst_port, and optional handle for one live host-side TCP "
      "drop filter")(
      "runtime-node-unblock-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_unblocks)
          ->composing(),
      "repeatable JSON object with node, optional src_address and src_port, "
      "dst_address, dst_port, and optional handle for one live host-side TCP "
      "drop filter removal")(
      "runtime-partition-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_partitions)
          ->composing(),
      "repeatable JSON object with group_a and group_b arrays for one live "
      "source-aware group partition")(
      "runtime-heal-partition-json",
      po::value<std::vector<std::string>>(
          &legacy_inputs.runtime_partition_heals)
          ->composing(),
      "repeatable JSON object with group_a and group_b arrays for one live "
      "source-aware group partition heal")(
      "runtime-node-resource-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_resources)
          ->composing(),
      "repeatable JSON object with node plus live cgroup limit fields to apply "
      "after nodes are running")(
      "runtime-node-restart-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_restarts)
          ->composing(),
      "repeatable JSON object with node field for one live node restart after "
      "nodes are running")(
      "runtime-node-freeze-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_freezes)
          ->composing(),
      "repeatable JSON object with node and duration_ms for one live cgroup "
      "freeze/thaw after nodes are running")(
      "replace-run", po::bool_switch(&options.replace_run),
      "remove an existing run directory first")(
      "probe-address", po::bool_switch(&options.probe_address),
      "assign and inspect an IPv4 address inside a temporary netns through "
      "libmnl")(
      "probe-bandwidth-limit", po::bool_switch(&options.probe_bandwidth_limit),
      "apply and remove a TBF bandwidth limit on a temporary veth peer through "
      "libmnl")(
      "probe-capabilities", po::bool_switch(&options.probe_capabilities),
      "report effective Linux capabilities needed by privileged simulator "
      "paths")(
      "probe-cgroup-freeze", po::bool_switch(&options.probe_cgroup_freeze),
      "attach a child process to a cgroup and verify cgroup.freeze/thaw "
      "paths")(
      "probe-drop-filter", po::bool_switch(&options.probe_drop_filter),
      "apply and remove a flower/gact TCP drop filter on a temporary veth "
      "through libmnl")(
      "probe-directional-network-condition",
      po::bool_switch(&options.probe_directional_network_condition),
      "apply and remove exact-destination prio/flower/TBF/netem policies "
      "inside a temporary network namespace through libmnl")(
      "probe-netns", po::bool_switch(&options.probe_netns),
      "create a temporary network namespace and inspect it "
      "through setns/libmnl")(
      "probe-network-condition",
      po::bool_switch(&options.probe_network_condition),
      "apply and remove a netem network condition on a temporary veth peer "
      "through libmnl")(
      "probe-combined-network-condition",
      po::bool_switch(&options.probe_combined_network_condition),
      "apply and remove a combined TBF/netem condition on a temporary veth "
      "peer through libmnl")(
      "probe-network-condition-update",
      po::bool_switch(&options.probe_network_condition_update),
      "replace a live host-side netem network condition on a temporary veth "
      "through libmnl")(
      "probe-qdisc", po::bool_switch(&options.probe_qdisc),
      "dump qdisc state for a temporary veth peer through libmnl")(
      "probe-qdisc-mutation", po::bool_switch(&options.probe_qdisc_mutation),
      "replace and delete a root pfifo qdisc on a temporary veth peer through "
      "libmnl")("probe-route", po::bool_switch(&options.probe_route),
                "assign and inspect an IPv4 route inside a temporary netns "
                "through libmnl")(
      "probe-veth", po::bool_switch(&options.probe_veth),
      "create, move, inspect, and delete a temporary veth pair through libmnl")(
      "probe-network", po::bool_switch(&options.probe_network),
      "list links through rtnetlink/libmnl and exit");

  po::variables_map vm;
  if (in_memory_scenario == nullptr) {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  }
  if (OptionProvided(vm, "ready-timeout-sec")) {
    options.ready_timeout_sec =
        ParseCliUint32Text(ready_timeout_text, "--ready-timeout-sec");
  }
  if (OptionProvided(vm, "sync-timeout-sec")) {
    options.sync_timeout_sec =
        ParseCliUint32Text(sync_timeout_text, "--sync-timeout-sec");
  }

  options.chain = ParseChainKind(chain_name);
  options.log_level = ParseLogLevel(log_level_name);
  if (OptionProvided(vm, "isolate-network") &&
      OptionProvided(vm, "no-isolate-network")) {
    throw std::runtime_error(
        "--isolate-network and --no-isolate-network are mutually exclusive");
  }
  if (isolate_network) {
    options.isolate_network = true;
  } else if (no_isolate_network) {
    options.isolate_network = false;
  }
  if (keep_cgroups) {
    options.cleanup_policy = CleanupPolicy::kRetainCgroups;
  }
  if (OptionProvided(vm, "metrics-interval") &&
      OptionProvided(vm, "metrics-interval-ms")) {
    throw std::runtime_error(
        "--metrics-interval and --metrics-interval-ms must not be combined");
  }
  if (OptionProvided(vm, "metrics-interval")) {
    options.metrics_interval =
        PositiveDuration::Parse(metrics_interval_text).value();
  } else if (OptionProvided(vm, "metrics-interval-ms")) {
    options.metrics_interval =
        PositiveDuration::FromMilliseconds(legacy_metrics_interval_ms).value();
  }
  if (OptionProvided(vm, "cleanup-run")) {
    options.cleanup_run = true;
    if (!cleanup_run_id.empty()) {
      if (OptionProvided(vm, "run-id") && options.run_id != cleanup_run_id) {
        throw std::runtime_error(
            "--cleanup-run id and --run-id must match when both are provided");
      }
      options.run_id = cleanup_run_id;
    }
  }

  if (OptionProvided(vm, "scenario")) {
    if (OptionProvided(vm, "scenario-json") ||
        OptionProvided(vm, "scenario-yaml")) {
      throw std::runtime_error(
          "--scenario cannot be combined with its legacy format-specific "
          "aliases");
    }
    const std::string extension = options.scenario.extension().string();
    if (extension == ".json") {
      options.scenario_json = options.scenario;
    } else if (extension == ".yaml" || extension == ".yml") {
      options.scenario_yaml = options.scenario;
    } else {
      throw std::runtime_error(
          "--scenario must use a .json, .yaml, or .yml extension");
    }
  }

  options.block_production.enabled = !no_mining;
  options.block_production.mode = native_mining
                                      ? MiningMode::kNativeMining
                                      : MiningMode::kScheduledBlockProduction;
  options.block_production.policy = BlockProductionPolicy(
      std::chrono::milliseconds(block_production_period_ms),
      block_production_probability, block_production_seed);
  if (OptionProvided(vm, "mining-difficulty")) {
    options.block_production.difficulty = MiningDifficulty(mining_difficulty);
  }

  if (vm.count("help") != 0U) {
    BBP_LOG(info) << "Usage: " << argv[0] << " [options]\n"
                  << canonical_options;
    std::exit(0);
  }
  if (!options.scenario_json.empty() && !options.scenario_yaml.empty()) {
    throw std::runtime_error(
        "--scenario-json and --scenario-yaml are mutually exclusive");
  }
  if (OptionProvided(vm, "benchmark-root") &&
      OptionProvided(vm, "output-dir")) {
    throw std::runtime_error(
        "--benchmark-root and --output-dir are aliases and must not both be "
        "provided");
  }
  const std::uint32_t node_binary_option_count =
      static_cast<std::uint32_t>(OptionProvided(vm, "node-binary")) +
      static_cast<std::uint32_t>(OptionProvided(vm, "chain-daemon")) +
      static_cast<std::uint32_t>(
          OptionProvided(vm, default_chain_spec.daemon_option_name.c_str()));
  if (node_binary_option_count > 1U) {
    throw std::runtime_error(
        "--node-binary and its legacy aliases must not be combined");
  }
  options.chain_daemon_cli_override = node_binary_option_count == 1U;
  const std::uint32_t stored_run_operation_count =
      static_cast<std::uint32_t>(OptionProvided(vm, "run")) +
      static_cast<std::uint32_t>(OptionProvided(vm, "report-run")) +
      static_cast<std::uint32_t>(options.cleanup_run);
  if (stored_run_operation_count > 1U) {
    throw std::runtime_error(
        "--run, --report-run, and --cleanup-run are mutually exclusive");
  }
  if (vm.count("run") == 0U && OptionProvided(vm, "once")) {
    throw std::runtime_error("--once requires --run");
  }
  if (no_mining && native_mining) {
    throw std::runtime_error(
        "--no-mining and --native-mining are mutually exclusive");
  }
  if (options.tui_refresh_ms == 0U) {
    throw std::runtime_error("--refresh-ms must be greater than zero");
  }
  ApplyDirectTransactionLoadOptions(vm, transaction_load_strategy,
                                    wallet_node_count, &options);
  if (in_memory_scenario != nullptr) {
    ApplyScenarioJson(*in_memory_scenario, vm, options);
  }
  if (!options.scenario_json.empty()) {
    const boost::json::value scenario =
        boost::json::parse(ReadText(options.scenario_json));
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-json root must be a JSON object");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
  }
  if (!options.scenario_yaml.empty()) {
    const boost::json::value scenario =
        simulator_app_internal::ParseYamlDocument(
            ReadText(options.scenario_yaml), options.scenario_yaml);
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-yaml root must be a YAML mapping");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
  }
  const bool initial_inventory_requested =
      in_memory_scenario != nullptr || !options.scenario_json.empty() ||
      !options.scenario_yaml.empty() || OptionProvided(vm, "nodes") ||
      OptionProvided(vm, "generate-node") || node_binary_option_count != 0U;
  const bool explicitly_empty_inventory = options.nodes == 0U &&
                                          options.node_ids.empty() &&
                                          options.scenario_node_configs.empty();
  options.initial_run_requested =
      stored_run_operation_count == 0U && initial_inventory_requested;
  if (stored_run_operation_count == 0U &&
      (!initial_inventory_requested || explicitly_empty_inventory)) {
    options.empty_control_plane = true;
    options.nodes = 0U;
    options.generate_node = 0U;
    options.block_production.enabled = false;
  }
  if (options.simulation_name.empty()) {
    options.simulation_name = options.run_id;
  }
  const std::string active_chain_name(ChainKindName(options.chain));
  auto [active_chain, inserted] = options.chains.try_emplace(
      active_chain_name, ScenarioChain{.driver = options.chain,
                                       .default_binary = options.chain_daemon});
  static_cast<void>(inserted);
  active_chain->second.driver = options.chain;
  active_chain->second.default_binary = options.chain_daemon;
  if (options.simulation_duration) {
    if (options.metrics_sample_count != 0U) {
      throw std::runtime_error(
          "simulation duration and metrics_sample_count must not be "
          "combined");
    }
    const std::chrono::milliseconds wall_duration =
        options.time_scale.WallDuration(*options.simulation_duration);
    const auto maximum_monotonic_delay =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::duration::max());
    if (wall_duration > maximum_monotonic_delay) {
      throw std::runtime_error(
          "scaled simulation duration exceeds monotonic clock range");
    }
  }
  if (options.metrics_sample_count != 0U && HasTimedNodeLifecycle(options)) {
    throw std::runtime_error(
        "timed node lifecycle and metrics_sample_count must not be combined");
  }
  options.network_condition_requested =
      options.network_condition_requested ||
      vm.count("network-bandwidth-kbps") != 0U ||
      vm.count("network-delay-ms") != 0U ||
      vm.count("network-jitter-ms") != 0U ||
      vm.count("network-loss-bps") != 0U ||
      vm.count("network-duplicate-bps") != 0U ||
      vm.count("network-corrupt-bps") != 0U ||
      vm.count("network-reorder-bps") != 0U ||
      vm.count("network-limit-packets") != 0U;
  options.cpu_quota_requested =
      options.cpu_quota_requested || vm.count("cpu-quota-us") != 0U;
  ParseLegacyCliInputs(legacy_inputs, options);
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  if (OptionProvided(vm, "node-capacity") && options.node_capacity == 0U) {
    throw std::runtime_error("--node-capacity must be greater than zero");
  }
  if (options.node_capacity == 0U) {
    options.node_capacity = chain_spec.max_nodes;
  }
  if (options.memory_high_bytes > options.memory_max_bytes) {
    throw std::runtime_error(
        "--memory-high-bytes must be less than or equal to --memory-max-bytes");
  }
  if (options.cpu_period_us == 0U) {
    throw std::runtime_error("--cpu-period-us must be greater than zero");
  }
  if (options.cpu_quota_requested && options.cpu_quota_us == 0U) {
    throw std::runtime_error("--cpu-quota-us must be greater than zero");
  }
  RequireCgroupWeight(options.cpu_weight, "--cpu-weight");
  RequireCgroupWeight(options.io_weight, "--io-weight");
  if (options.pids_max == 0U) {
    throw std::runtime_error("--pids-max must be greater than zero");
  }
  if ((options.network_condition_requested ||
       !options.node_network_conditions.empty() ||
       !options.runtime_node_network_conditions.empty() ||
       !options.runtime_node_blocks.empty() ||
       !options.runtime_node_unblocks.empty() ||
       !options.runtime_partitions.empty() ||
       !options.runtime_partition_heals.empty() ||
       TopologyHasDirectionalNetworkConditions(options) ||
       WorkloadsRequireIsolatedNetwork(options)) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network runtime options require --isolate-network");
  }
  if (options.node_capacity > chain_spec.max_nodes) {
    throw std::runtime_error(
        "--node-capacity must not exceed the selected chain limit " +
        std::to_string(chain_spec.max_nodes));
  }
  if (options.nodes > options.node_capacity) {
    throw std::runtime_error("--nodes must not exceed --node-capacity");
  }
  if (options.empty_control_plane) {
    if (options.nodes != 0U || options.generate_node != 0U ||
        options.topology.configured || !options.node_ids.empty() ||
        !options.node_roles.empty() || !options.scenario_node_configs.empty()) {
      throw std::runtime_error(
          "empty control-plane runs must not define an initial node inventory");
    }
  } else if (options.nodes < 1 || options.nodes > chain_spec.max_nodes) {
    throw std::runtime_error("--nodes currently supports 1.." +
                             std::to_string(chain_spec.max_nodes) +
                             " for chain smoke runs");
  }
  RuntimePeerTopology validated_runtime_topology(options.topology.peer_topology,
                                                 options.nodes,
                                                 options.empty_control_plane);
  if (!options.empty_control_plane &&
      (options.generate_node == 0U || options.generate_node > options.nodes)) {
    throw std::runtime_error("--generate-node must be in 1..--nodes");
  }
  if (options.workloads.size() > kMaximumScenarioActionCount ||
      options.scheduled_events.size() >
          kMaximumScenarioActionCount - options.workloads.size()) {
    throw std::runtime_error("scenario action count exceeds retained limit " +
                             std::to_string(kMaximumScenarioActionCount));
  }
  for (const ConfiguredScenarioAction& configured_action :
       ConfiguredScenarioActions(options)) {
    ValidateScenarioWorkload(configured_action.workload,
                             configured_action.available_node_count, options);
  }
  if (ValidateRuntimeTopologyActionSequence(options,
                                            &validated_runtime_topology) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network runtime options require --isolate-network");
  }
  if (options.topology.configured) {
    SimulationRegistry::FromTopology(options.topology,
                                     options.wallet_initialization);
    if (options.block_production.enabled &&
        options.topology.miner_nodes.empty()) {
      throw std::runtime_error(
          "enabled block production requires at least one configured miner");
    }
  }
  RequireSafeRunId(options.run_id);
  const bool needs_chain_daemon =
      !options.empty_control_plane && !options.probe_network &&
      options.report_run.empty() && options.tui_run.empty() &&
      !options.probe_bandwidth_limit && !options.probe_capabilities &&
      !options.probe_cgroup_freeze && !options.probe_drop_filter &&
      !options.probe_directional_network_condition && !options.probe_netns &&
      !options.probe_veth && !options.probe_address && !options.probe_route &&
      !options.probe_qdisc && !options.probe_qdisc_mutation &&
      !options.probe_network_condition &&
      !options.probe_combined_network_condition &&
      !options.probe_network_condition_update && !options.cleanup_run;
  if (needs_chain_daemon && options.chain_daemon.empty()) {
    throw std::runtime_error("chain runs require --chain-daemon or --" +
                             chain_spec.daemon_option_name);
  }
  if (needs_chain_daemon) {
    std::set<std::filesystem::path> checked_binaries;
    for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
      const std::filesystem::path binary =
          EffectiveNodeBinary(options, node_index);
      if (binary.empty()) {
        throw std::runtime_error("scenario node " +
                                 ScenarioNodeId(options, node_index) +
                                 " requires an explicit binary");
      }
      if (checked_binaries.insert(binary).second) {
        RequireExecutable(binary);
      }
    }
  }
  return options;
}

}  // namespace bbp::simulator_app_internal
