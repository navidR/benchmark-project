#include "simulator_scenario_workload_decoding.h"

#include <algorithm>
#include <boost/json/value.hpp>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/positive_duration.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/options.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_peer_topology_decoding.h"
#include "simulator_profile_assignment.h"
#include "simulator_resource_limit_decoding.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_wallet_transaction_distribution_decoding.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {
namespace {

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

bool IsRejectedWalletMaterialField(std::string_view field) {
  return field == "wallets" || field == "private_key" ||
         field == "source_private_key" || field == "address" ||
         field == "source_address" || field == "destination_address";
}

}  // namespace

void RejectUnsupportedScenarioActionFields(const boost::json::object& object,
                                           WorkloadKind kind, bool scheduled) {
  const bool state_edge_action = kind == WorkloadKind::kActivateEdge ||
                                 kind == WorkloadKind::kDeactivateEdge ||
                                 kind == WorkloadKind::kRestoreEdge;
  for (const auto& member : object) {
    const bool structural =
        scheduled ? member.key() == "at" || member.key() == "action"
                  : member.key() == "type";
    const bool dedicated_rejection =
        (kind == WorkloadKind::kSetEdgeCondition &&
         member.key() == "timeout_sec") ||
        (state_edge_action && IsTopologyEdgeConditionField(member.key())) ||
        (kind == WorkloadKind::kWalletTransactions &&
         IsRejectedWalletMaterialField(member.key()));
    if (!structural && !dedicated_rejection &&
        !ScenarioWorkloadFieldAllowed(kind, member.key())) {
      throw std::runtime_error(
          std::string(scheduled ? "scenario scheduled action "
                                : "scenario workload ") +
          std::string(WorkloadKindName(kind)) +
          " has unsupported field: " + std::string(member.key()));
    }
  }
}

void ApplyScenarioWorkloads(const boost::json::array& workloads,
                            const boost::program_options::variables_map& vm,
                            Options& options) {
  if (options.workloads.size() > kMaximumScenarioActionCount ||
      options.scheduled_events.size() >
          kMaximumScenarioActionCount - options.workloads.size() ||
      workloads.size() > kMaximumScenarioActionCount -
                             options.workloads.size() -
                             options.scheduled_events.size()) {
    throw std::runtime_error("scenario action count exceeds retained limit " +
                             std::to_string(kMaximumScenarioActionCount));
  }
  for (const boost::json::value& value : workloads) {
    if (!value.is_object()) {
      throw std::runtime_error(
          "scenario workloads entries must be JSON objects");
    }
    const boost::json::object& workload = value.as_object();
    const std::string type_name = JsonStringField(workload, "type");
    const std::optional<WorkloadKind> kind = ParseWorkloadKind(type_name);
    if (!kind) {
      throw std::runtime_error("unsupported scenario workload type: " +
                               type_name);
    }
    RejectUnsupportedScenarioActionFields(workload, *kind, false);
    if (*kind == WorkloadKind::kBlockGeneration) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP block_generation workload uses node, not nodes");
      }
      BlockGenerationWorkload block_generation;
      block_generation.count = JsonUint32Field(workload, "count");
      block_generation.node =
          OptionProvided(vm, "generate-node")
              ? options.generate_node
              : JsonOptionalUint32Field(workload, "node",
                                        options.generate_node);
      block_generation.sync_timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "sync_timeout_sec",
                                        options.sync_timeout_sec);
      if (block_generation.sync_timeout_sec <
              kBlockGenerationMinimumSyncTimeoutSeconds ||
          block_generation.sync_timeout_sec >
              kBlockGenerationMaximumSyncTimeoutSeconds) {
        throw std::runtime_error(
            "block_generation sync_timeout_sec must be in " +
            std::to_string(kBlockGenerationMinimumSyncTimeoutSeconds) + ".." +
            std::to_string(kBlockGenerationMaximumSyncTimeoutSeconds));
      }
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kBlockGeneration;
      scenario_workload.block_generation = block_generation;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kWaitUntilHeight) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP wait_until_height workload uses node, not nodes");
      }
      WaitUntilHeightWorkload wait;
      wait.node = JsonOptionalUint32Field(workload, "node", wait.node);
      if (wait.node != 0U && wait.node <= options.nodes) {
        wait.node_id = ScenarioNodeId(options, wait.node - 1U);
      }
      wait.height = JsonUint64Field(workload, "height");
      wait.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWaitUntilHeight;
      scenario_workload.wait_until_height = wait;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kWaitForPeers) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP wait_for_peers workload uses node, not nodes");
      }
      WaitForPeersWorkload wait;
      wait.node = JsonOptionalUint32Field(workload, "node", wait.node);
      wait.peer_count = JsonUint64Field(workload, "peer_count");
      wait.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWaitForPeers;
      scenario_workload.wait_for_peers = wait;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kConnectPeer) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP connect_peer workload uses node, not nodes");
      }
      ConnectPeerWorkload connect;
      connect.node = JsonOptionalUint32Field(workload, "node", connect.node);
      connect.peer = JsonUint32Field(workload, "peer");
      connect.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kConnectPeer;
      scenario_workload.connect_peer = connect;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kDisconnectPeer) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP disconnect_peer workload uses node, not nodes");
      }
      DisconnectPeerWorkload disconnect;
      disconnect.node =
          JsonOptionalUint32Field(workload, "node", disconnect.node);
      disconnect.peer = JsonUint32Field(workload, "peer");
      disconnect.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kDisconnectPeer;
      scenario_workload.disconnect_peer = disconnect;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kRestartNode) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP restart_node workload uses node, not nodes");
      }
      RestartNodeWorkload restart;
      restart.node = JsonOptionalUint32Field(workload, "node", restart.node);
      if (restart.node != 0U && restart.node <= options.nodes) {
        restart.node_id = ScenarioNodeId(options, restart.node - 1U);
      }
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kRestartNode;
      scenario_workload.restart_node = restart;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kFreezeNode) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP freeze_node workload uses node, not nodes");
      }
      FreezeNodeWorkload freeze;
      freeze.node = JsonOptionalUint32Field(workload, "node", freeze.node);
      freeze.duration_ms = JsonUint32Field(workload, "duration_ms");
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kFreezeNode;
      scenario_workload.freeze_node = freeze;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kUpdateResourceLimits) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP update_resource_limits workload uses node, not "
            "nodes");
      }
      ResourceLimitUpdateWorkload update;
      update.node = JsonOptionalUint32Field(workload, "node", update.node);
      update.patch = ParseResourceLimitPatchObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kUpdateResourceLimits;
      scenario_workload.update_resource_limits = update;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kSetResourceProfile ||
               *kind == WorkloadKind::kSetNetworkProfile) {
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = *kind;
      scenario_workload.profile_switch =
          ParseProfileSwitchWorkload(workload, options, *kind);
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kResourcePressure) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP resource_pressure workload uses node, not nodes");
      }
      ResourcePressureWorkload pressure;
      pressure.node = JsonOptionalUint32Field(workload, "node", pressure.node);
      pressure.patch = ParseResourceLimitPatchObject(workload);
      pressure.duration_ms = JsonUint32Field(workload, "duration_ms");
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kResourcePressure;
      scenario_workload.resource_pressure = pressure;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kSetNetworkCondition) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP set_network_condition workload uses node, not "
            "nodes");
      }
      NetworkConditionWorkload update;
      update.node = JsonOptionalUint32Field(workload, "node", update.node);
      update.condition = ParseNetworkConditionObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kSetNetworkCondition;
      scenario_workload.network_condition = update;
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kBlockNetworkFlow ||
               *kind == WorkloadKind::kUnblockNetworkFlow) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP network flow workload uses node, not nodes");
      }
      NetworkBlockWorkload flow;
      flow.rule = ParseNetworkBlockRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = *kind;
      scenario_workload.network_block = std::move(flow);
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kPartitionNodes) {
      NetworkPartitionWorkload partition;
      partition.partition = ParseNetworkPartitionRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kPartitionNodes;
      scenario_workload.network_partition = partition;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kHealPartition) {
      NetworkPartitionWorkload partition;
      partition.partition = ParseNetworkPartitionRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kHealPartition;
      scenario_workload.network_partition = partition;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kSetEdgeCondition ||
               *kind == WorkloadKind::kActivateEdge ||
               *kind == WorkloadKind::kDeactivateEdge ||
               *kind == WorkloadKind::kRestoreEdge) {
      TopologyEdgeWorkload edge;
      edge.from = JsonUint32Field(workload, "from");
      edge.to = JsonUint32Field(workload, "to");
      if (*kind == WorkloadKind::kSetEdgeCondition) {
        if (workload.if_contains("timeout_sec") != nullptr) {
          throw std::runtime_error(
              "scenario set_edge_condition does not accept timeout_sec");
        }
        edge.condition = ParseTopologyEdgeWorkloadCondition(workload);
      } else {
        RejectTopologyEdgeConditionFields(workload, WorkloadKindName(*kind));
        edge.timeout_sec =
            OptionProvided(vm, "sync-timeout-sec")
                ? options.sync_timeout_sec
                : JsonOptionalUint32Field(workload, "timeout_sec",
                                          options.sync_timeout_sec);
      }
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = *kind;
      scenario_workload.topology_edge = edge;
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kSendRawTransaction) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP send_raw_transaction workload uses "
            "funding_node and submit_node, not nodes");
      }
      SendRawTransactionWorkload transaction;
      transaction.funding_node = JsonOptionalUint32Field(
          workload, "funding_node", transaction.funding_node);
      transaction.submit_node = JsonOptionalUint32Field(
          workload, "submit_node", transaction.submit_node);
      transaction.source_address = JsonStringField(workload, "source_address");
      transaction.source_private_key =
          JsonStringField(workload, "source_private_key");
      transaction.destination_address =
          JsonStringField(workload, "destination_address");
      transaction.funding_blocks = JsonOptionalUint32Field(
          workload, "funding_blocks", transaction.funding_blocks);
      transaction.amount_satoshis = JsonAmountField(workload, "amount");
      transaction.fee_satoshis = JsonAmountField(workload, "fee");
      transaction.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kSendRawTransaction;
      scenario_workload.send_raw_transaction = transaction;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kWalletTransactions) {
      const bool wallet_material_present =
          std::any_of(workload.begin(), workload.end(), [](const auto& member) {
            return IsRejectedWalletMaterialField(member.key());
          });
      if (wallet_material_present) {
        throw std::runtime_error(
            "scenario wallet_transactions workload must use "
            "topology.wallet_initialization instead of wallet keys or "
            "addresses");
      }
      WalletTransactionsWorkload transactions;
      const std::uint32_t coinbase_confirmations =
          ChainDriverSpecFor(options.chain).coinbase_spendable_confirmations;
      transactions.funding_blocks_per_wallet = coinbase_confirmations;
      transactions.readiness_confirmations = coinbase_confirmations;
      transactions.funding_strategy =
          ParseWalletFundingStrategy(JsonOptionalStringField(
              workload, "funding_strategy",
              WalletFundingStrategyName(transactions.funding_strategy)));
      transactions.strategy =
          ParseWalletTransferStrategy(JsonOptionalStringField(
              workload, "strategy",
              WalletTransferStrategyName(transactions.strategy)));
      const bool load_strategy =
          IsTransactionLoadStrategy(transactions.strategy);
      transactions.funding_blocks_per_wallet =
          JsonOptionalUint32Field(workload, "funding_blocks_per_wallet",
                                  transactions.funding_blocks_per_wallet);
      transactions.readiness_confirmations =
          JsonOptionalUint64Field(workload, "readiness_confirmations",
                                  transactions.readiness_confirmations);
      const bool transaction_count_present =
          workload.if_contains("transaction_count") != nullptr;
      const bool transaction_rate_present =
          workload.if_contains("transaction_rate") != nullptr;
      const bool duration_present = workload.if_contains("duration") != nullptr;
      const bool load_control_present =
          duration_present || workload.if_contains("concurrency") != nullptr ||
          workload.if_contains("queue_capacity") != nullptr ||
          workload.if_contains("mode") != nullptr ||
          workload.if_contains("fee_policy") != nullptr;
      if (!load_strategy && load_control_present) {
        throw std::runtime_error(
            "scenario wallet_transactions load controls require "
            "random_bruteforce or equal_fanout strategy");
      }
      if (load_strategy) {
        if (transaction_count_present && duration_present) {
          throw std::runtime_error(
              "scenario transaction load transaction_count and duration are "
              "mutually exclusive");
        }
        if (transaction_count_present) {
          transactions.transaction_count =
              JsonOptionalUint32Field(workload, "transaction_count", 0U);
        }
        if (duration_present) {
          const boost::json::value& duration = workload.at("duration");
          if (!duration.is_string()) {
            throw std::runtime_error(
                "scenario transaction load duration must be a duration "
                "string");
          }
          transactions.duration =
              PositiveDuration::Parse(std::string_view(duration.as_string()))
                  .value();
        }
        transactions.concurrency = JsonOptionalUint32Field(
            workload, "concurrency", transactions.concurrency);
        transactions.queue_capacity = JsonOptionalUint32Field(
            workload, "queue_capacity", transactions.queue_capacity);
        transactions.mode = ParseWalletTransactionMode(JsonOptionalStringField(
            workload, "mode",
            WalletPrivacyModeName(options.wallet_initialization.mode)));
        transactions.fee_policy =
            ParseWalletTransactionFeePolicy(JsonOptionalStringField(
                workload, "fee_policy",
                WalletTransactionFeePolicyName(transactions.fee_policy)));
        if (workload.if_contains("interval") != nullptr) {
          throw std::runtime_error(
              "scenario transaction load does not accept interval");
        }
      } else if (transaction_count_present && transaction_rate_present) {
        throw std::runtime_error(
            "scenario wallet_transactions transaction_count and "
            "transaction_rate are mutually exclusive");
      }
      if (transaction_rate_present) {
        transactions.transaction_rate = WalletTransactionRate::FromDouble(
            JsonOptionalDoubleField(workload, "transaction_rate", 0.0));
        if (!load_strategy && workload.if_contains("interval") != nullptr) {
          throw std::runtime_error(
              "scenario wallet_transactions transaction_rate does not accept "
              "interval");
        }
      }
      transactions.amount = ParseAmountDistribution(workload, "amount");
      if (!load_strategy) {
        transactions.interval = ParseIntervalDistribution(workload, "interval");
      }
      transactions.fee_satoshis = JsonAmountField(workload, "fee");
      if (load_strategy) {
        const std::unique_ptr<ChainDriver> load_driver =
            CreateChainDriver(options.chain);
        transactions.fee_reserve_satoshis =
            load_driver->WalletTransactionFeeReserveSatoshis(
                ToChainWalletMode(transactions.mode),
                transactions.fee_satoshis);
      }
      const std::uint64_t fee_reserve_satoshis =
          EffectiveWalletTransactionFeeReserveSatoshis(transactions);
      const std::uint64_t default_funding_threshold =
          transactions.amount.maximum_satoshis <=
                  std::numeric_limits<std::uint64_t>::max() -
                      fee_reserve_satoshis
              ? transactions.amount.maximum_satoshis + fee_reserve_satoshis
              : 0U;
      transactions.funding_threshold_satoshis = JsonOptionalAmountField(
          workload, "funding_threshold", default_funding_threshold);
      if (workload.if_contains("retained_balance_percentage") != nullptr) {
        transactions.retained_balance_basis_points =
            JsonPercentBasisPoints(workload, "retained_balance_percentage");
      }
      transactions.random_seed =
          JsonOptionalUint64Field(workload, "seed", options.simulation_seed);
      const std::size_t wallet_count = options.topology.wallet_nodes.size();
      transactions.sender_wallets =
          ParseWalletIndexList(workload, "sender_wallets", wallet_count);
      transactions.receiver_wallets =
          ParseWalletIndexList(workload, "receiver_wallets", wallet_count);
      const bool sender_wallets_present =
          workload.if_contains("sender_wallets") != nullptr;
      const bool receiver_wallets_present =
          workload.if_contains("receiver_wallets") != nullptr;
      if ((transactions.strategy == WalletTransferStrategy::kRoundRobin ||
           transactions.strategy == WalletTransferStrategy::kRandom) &&
          (sender_wallets_present || receiver_wallets_present)) {
        throw std::runtime_error(
            "scenario wallet_transactions wallet selectors require fanout "
            "or hotspot strategy");
      }
      if (transactions.strategy == WalletTransferStrategy::kFanout &&
          receiver_wallets_present) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout does not accept "
            "receiver_wallets");
      }
      if (transactions.strategy == WalletTransferStrategy::kHotspot &&
          sender_wallets_present) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot does not accept "
            "sender_wallets");
      }
      if (transactions.strategy == WalletTransferStrategy::kRandomBruteforce &&
          (sender_wallets_present || receiver_wallets_present)) {
        throw std::runtime_error(
            "scenario random_bruteforce uses every wallet and does not "
            "accept sender_wallets or receiver_wallets");
      }
      transactions.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWalletTransactions;
      scenario_workload.wallet_transactions = std::move(transactions);
      options.workloads.push_back(std::move(scenario_workload));
      options.wallet_backed_workload_requested = true;
    } else if (*kind == WorkloadKind::kCheckpoint) {
      CheckpointWorkload checkpoint;
      if (workload.if_contains("name") != nullptr) {
        checkpoint.name = JsonStringField(workload, "name");
        RequireSafeScenarioIdentifier(checkpoint.name, "checkpoint name");
      }
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kCheckpoint;
      scenario_workload.checkpoint = std::move(checkpoint);
      options.workloads.push_back(std::move(scenario_workload));
    }
  }
}

}  // namespace bbp::simulator_app_internal
