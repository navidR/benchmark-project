#include "simulator_network_block_application.h"

#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/simulator/node_runtime.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

bool NetworkBlockRulePresent(const NodeRuntime& node,
                             const NetworkBlockRule& rule) {
  if (!node.network) {
    return false;
  }
  const std::vector<TcFilterInfo> filters =
      ListTcFiltersForInterface(node.network->host_name);
  for (const TcFilterInfo& filter : filters) {
    if (TcFilterMatchesEgressIpv4TcpDrop(
            filter, node.network->host_name, rule.src_address, rule.src_port,
            rule.dst_address, rule.dst_port, rule.handle)) {
      return true;
    }
  }
  return false;
}

std::optional<NetworkBlockRule> NetworkBlockRuleForHandle(
    const NodeRuntime& node, std::uint32_t handle) {
  if (!node.network) {
    return std::nullopt;
  }
  const std::vector<TcFilterInfo> filters =
      ListTcFiltersForInterface(node.network->host_name);
  for (const TcFilterInfo& filter : filters) {
    if (filter.handle != handle ||
        !TcFilterIsEgressIpv4TcpDropPolicy(filter, node.network->host_name)) {
      continue;
    }
    NetworkBlockRule rule;
    rule.src_address = filter.has_ipv4_src ? filter.ipv4_src : "";
    rule.src_port = filter.has_tcp_src ? filter.tcp_src : 0U;
    rule.dst_address = filter.ipv4_dst;
    rule.dst_port = filter.tcp_dst;
    rule.handle = filter.handle;
    return rule;
  }
  return std::nullopt;
}

void RequireNetworkBlockHandleAvailable(const NodeRuntime& node,
                                        const NetworkBlockRule& rule) {
  if (!node.network) {
    return;
  }
  const std::vector<TcFilterInfo> filters =
      ListTcFiltersForInterface(node.network->host_name);
  for (const TcFilterInfo& filter : filters) {
    if (filter.handle != rule.handle) {
      continue;
    }
    if (!TcFilterMatchesEgressIpv4TcpDrop(
            filter, node.network->host_name, rule.src_address, rule.src_port,
            rule.dst_address, rule.dst_port, rule.handle)) {
      throw std::runtime_error(
          "network block rule handle is already used by different filter: " +
          std::to_string(rule.handle));
    }
  }
}

void RequireNetworkBlockNode(const NodeRuntime& node) {
  if (!node.network) {
    throw std::runtime_error(
        "runtime network block rule requires isolated networking");
  }
}

void RestoreNetworkBlockRule(const NodeRuntime& node,
                             const NetworkBlockRule& rule,
                             bool should_be_present) {
  RequireNetworkBlockHandleAvailable(node, rule);
  const bool present = NetworkBlockRulePresent(node, rule);
  if (should_be_present && !present) {
    ReplaceEgressIpv4TcpDropFilter(node.network->host_name, rule.src_address,
                                   rule.src_port, rule.dst_address,
                                   rule.dst_port, rule.handle);
  } else if (!should_be_present && present) {
    DeleteEgressIpv4TcpDropFilter(node.network->host_name, rule.handle);
  }
  if (NetworkBlockRulePresent(node, rule) != should_be_present) {
    throw std::runtime_error(
        "network block rule rollback did not restore prior state");
  }
}

NetworkBlockMutationResult MutateNetworkBlockRuleTransactional(
    const NodeRuntime& node, const NetworkBlockRule& rule, bool remove,
    std::stop_token stop_token) {
  RequireNetworkBlockNode(node);
  ThrowIfStopRequested(stop_token);
  RequireNetworkBlockHandleAvailable(node, rule);
  const bool existed_before = NetworkBlockRulePresent(node, rule);
  try {
    if (remove) {
      if (existed_before) {
        DeleteEgressIpv4TcpDropFilter(node.network->host_name, rule.handle);
      }
    } else {
      ReplaceEgressIpv4TcpDropFilter(node.network->host_name, rule.src_address,
                                     rule.src_port, rule.dst_address,
                                     rule.dst_port, rule.handle);
    }
    ThrowIfStopRequested(stop_token);
    const bool present_after = NetworkBlockRulePresent(node, rule);
    if (!remove && !present_after) {
      throw std::runtime_error(
          "runtime network block rule was not visible after apply");
    }
    if (remove && present_after) {
      throw std::runtime_error(
          "runtime network block rule remained after unblock");
    }
    return NetworkBlockMutationResult{
        .existed_before = existed_before,
        .present_after = present_after,
    };
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::string rollback_error;
    try {
      RestoreNetworkBlockRule(node, rule, existed_before);
    } catch (const std::exception& error) {
      rollback_error = error.what();
      BBP_LOG(error) << "failed to restore network block rule " << rule.handle
                     << " on " << node.config.id << ": " << error.what();
    } catch (...) {
      rollback_error = "unknown exception";
      BBP_LOG(error) << "failed to restore network block rule " << rule.handle
                     << " on " << node.config.id << ": unknown exception";
    }
    if (!rollback_error.empty()) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "network block mutation outcome is unconfirmed", original_error,
          {rollback_error});
    }
    std::rethrow_exception(original_error);
  }
}

}  // namespace bbp::simulator_app_internal
