#include "simulator_runtime_workload_validation.h"

#include <boost/json/array.hpp>
#include <boost/program_options/variables_map.hpp>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/mcp_operation_service.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulator/options.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_option_parsing.h"
#include "simulator_scenario_workload_decoding.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {

BlockGenerationWorkload ParseAndValidateLiveBlockGenerationWorkload(
    const boost::json::object& workload, const Options& options,
    const RuntimeNodeSnapshot& nodes) {
  if (nodes.empty()) {
    throw McpOperationFailure(
        "workload_node_unavailable",
        "block generation requires at least one active node", true);
  }
  boost::json::array workloads;
  workloads.emplace_back(workload);
  Options validation_options = options;
  validation_options.workloads.clear();
  validation_options.scheduled_events.clear();
  if (validation_options.generate_node == 0U) {
    validation_options.generate_node = 1U;
  }
  boost::program_options::variables_map variables;
  ApplyScenarioWorkloads(workloads, variables, validation_options);
  if (validation_options.workloads.size() != 1U ||
      validation_options.workloads.front().kind !=
          WorkloadKind::kBlockGeneration) {
    throw std::runtime_error(
        "workload operation requires a block_generation workload");
  }
  const BlockGenerationWorkload parsed =
      validation_options.workloads.front().block_generation;
  static_cast<void>(RequireRuntimeNodeNumber(nodes, parsed.node,
                                             "block generation workload"));
  return parsed;
}

WaitUntilHeightWorkload ParseAndValidateLiveWaitUntilHeightWorkload(
    const boost::json::object& workload, const Options& options,
    const RuntimeNodeSnapshot& nodes) {
  if (nodes.empty()) {
    throw McpOperationFailure(
        "workload_node_unavailable",
        "wait-until-height requires at least one active node", true);
  }
  boost::json::array workloads;
  workloads.emplace_back(workload);
  Options validation_options = options;
  validation_options.workloads.clear();
  validation_options.scheduled_events.clear();
  boost::program_options::variables_map variables;
  ApplyScenarioWorkloads(workloads, variables, validation_options);
  if (validation_options.workloads.size() != 1U ||
      validation_options.workloads.front().kind !=
          WorkloadKind::kWaitUntilHeight) {
    throw std::runtime_error(
        "workload operation requires a wait_until_height workload");
  }
  WaitUntilHeightWorkload parsed =
      validation_options.workloads.front().wait_until_height;
  parsed.node_id =
      RequireRuntimeNodeNumber(nodes, parsed.node, "wait_until_height workload")
          .config.id;
  if (parsed.timeout_sec == 0U) {
    throw std::runtime_error(
        "wait_until_height timeout_sec must be greater than zero");
  }
  return parsed;
}

WaitForPeersWorkload ParseAndValidateLiveWaitForPeersWorkload(
    const boost::json::object& workload, const Options& options,
    const RuntimeNodeSnapshot& nodes) {
  if (nodes.empty()) {
    throw McpOperationFailure(
        "workload_node_unavailable",
        "wait-for-peers requires at least one active node", true);
  }
  boost::json::array workloads;
  workloads.emplace_back(workload);
  Options validation_options = options;
  validation_options.workloads.clear();
  validation_options.scheduled_events.clear();
  boost::program_options::variables_map variables;
  ApplyScenarioWorkloads(workloads, variables, validation_options);
  if (validation_options.workloads.size() != 1U ||
      validation_options.workloads.front().kind !=
          WorkloadKind::kWaitForPeers) {
    throw std::runtime_error(
        "workload operation requires a wait_for_peers workload");
  }
  const WaitForPeersWorkload parsed =
      validation_options.workloads.front().wait_for_peers;
  static_cast<void>(
      RequireRuntimeNodeNumber(nodes, parsed.node, "wait_for_peers workload"));
  if (parsed.peer_count == 0U) {
    throw std::runtime_error(
        "wait_for_peers peer_count must be greater than zero");
  }
  if (parsed.timeout_sec == 0U) {
    throw std::runtime_error(
        "wait_for_peers timeout_sec must be greater than zero");
  }
  return parsed;
}

Options RuntimeOneShotWorkloadValidationOptions(
    const Options& options, const RuntimeNodeSnapshot& nodes,
    const RuntimeWalletSnapshot& roles,
    const PeerTopologyConfig& live_topology_config) {
  if (nodes.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::overflow_error(
        "runtime node inventory exceeds one-shot workload uint32 limit");
  }
  Options validation = options;
  validation.nodes = static_cast<std::uint32_t>(nodes.size());
  validation.node_ids.clear();
  validation.node_ids.reserve(nodes.size());
  for (const NodeRuntime& node : nodes) {
    validation.node_ids.push_back(node.config.id);
  }
  validation.topology = roles.registry().topology();
  validation.topology.node_count = validation.nodes;
  validation.topology.peer_topology = live_topology_config;
  validation.empty_control_plane = nodes.empty();
  validation.generate_node = nodes.empty() ? 0U : 1U;
  validation.workloads.clear();
  validation.scheduled_events.clear();
  validation.workloads_configured = false;
  validation.wallet_backed_workload_requested = false;
  return validation;
}

ScenarioWorkload ParseAndValidateOneShotWorkload(
    const boost::json::object& workload, Options validation_options) {
  boost::json::array workloads;
  workloads.emplace_back(workload);
  boost::program_options::variables_map variables;
  ApplyScenarioWorkloads(workloads, variables, validation_options);
  if (validation_options.workloads.size() != 1U) {
    throw std::logic_error(
        "one-shot workload parser did not produce exactly one workload");
  }
  ScenarioWorkload parsed = std::move(validation_options.workloads.front());
  if (!IsOneShotWorkloadKind(parsed.kind)) {
    throw McpOperationFailure(
        "workload_not_one_shot",
        "workload.invoke requires a finite one-shot workload; use "
        "workload.start for " +
            std::string(WorkloadKindName(parsed.kind)),
        false);
  }
  ValidateScenarioWorkload(parsed, validation_options.nodes,
                           validation_options);
  return parsed;
}

}  // namespace bbp::simulator_app_internal

namespace bbp {

using simulator_app_internal::ApplyScenarioWorkloads;
using simulator_app_internal::ValidateWalletTransactionsWorkload;

WalletTransactionsWorkload ParseAndValidateWalletTransactionsWorkload(
    const boost::json::object& workload, const Options& options) {
  boost::json::array workloads;
  workloads.emplace_back(workload);
  Options validation_options = options;
  validation_options.workloads.clear();
  validation_options.scheduled_events.clear();
  validation_options.wallet_backed_workload_requested = false;
  boost::program_options::variables_map variables;
  ApplyScenarioWorkloads(workloads, variables, validation_options);
  if (validation_options.workloads.size() != 1U ||
      validation_options.workloads.front().kind !=
          WorkloadKind::kWalletTransactions) {
    throw std::runtime_error(
        "workload operation requires a wallet_transactions workload");
  }
  const WalletTransactionsWorkload& parsed =
      validation_options.workloads.front().wallet_transactions;
  ValidateWalletTransactionsWorkload(parsed, validation_options);
  return parsed;
}

}  // namespace bbp
