#pragma once

#include <cstdint>
#include <optional>
#include <stop_token>

#include "bbp/simulator/network_block_rule.h"

namespace bbp {

struct NodeRuntime;

namespace simulator_app_internal {

struct NetworkBlockMutationResult {
  bool existed_before = false;
  bool present_after = false;
};

bool NetworkBlockRulePresent(const NodeRuntime& node,
                             const NetworkBlockRule& rule);

std::optional<NetworkBlockRule> NetworkBlockRuleForHandle(
    const NodeRuntime& node, std::uint32_t handle);

void RequireNetworkBlockHandleAvailable(const NodeRuntime& node,
                                        const NetworkBlockRule& rule);

void RequireNetworkBlockNode(const NodeRuntime& node);

void RestoreNetworkBlockRule(const NodeRuntime& node,
                             const NetworkBlockRule& rule,
                             bool should_be_present);

NetworkBlockMutationResult MutateNetworkBlockRuleTransactional(
    const NodeRuntime& node, const NetworkBlockRule& rule, bool remove,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
