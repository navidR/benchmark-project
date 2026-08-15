#pragma once

#include <stop_token>

namespace bbp {

struct NetworkCondition;
struct NodeRuntime;
struct NodeVethConfig;
struct QdiscInfo;

namespace simulator_app_internal {

QdiscInfo VerifyNodeNetworkCondition(const NodeVethConfig& config,
                                     std::stop_token stop_token = {});

void RestoreNodeNetworkCondition(const NodeVethConfig& previous);

QdiscInfo ReplaceNodeNetworkConditionTransactional(
    NodeRuntime* node, const NetworkCondition& condition,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
