#include "simulator_network_condition_application.h"

#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/simulator/node_runtime.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {
namespace {

const QdiscInfo* FindQdiscByInterfaceName(const std::vector<QdiscInfo>& qdiscs,
                                          std::string_view name) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == name) {
      return &qdisc;
    }
  }
  return nullptr;
}

}  // namespace

QdiscInfo VerifyNodeNetworkCondition(const NodeVethConfig& config,
                                     std::stop_token stop_token) {
  const std::vector<QdiscInfo> qdiscs = ListQdiscs(stop_token);
  QdiscInfo summary;
  if (!QdiscsMatchNetworkCondition(qdiscs, config.host_name, config.condition,
                                   &summary)) {
    throw std::runtime_error(
        "host-side qdisc does not match requested network condition: " +
        config.host_name);
  }
  return summary;
}

void RestoreNodeNetworkCondition(const NodeVethConfig& previous) {
  if (previous.apply_condition) {
    ReplaceNetworkConditionQdisc(previous.host_name, previous.condition);
    static_cast<void>(VerifyNodeNetworkCondition(previous));
    return;
  }
  std::exception_ptr delete_error;
  try {
    DeleteRootQdisc(previous.host_name);
  } catch (...) {
    delete_error = std::current_exception();
  }
  const QdiscInfo* qdisc =
      FindQdiscByInterfaceName(ListQdiscs(), previous.host_name);
  if (qdisc != nullptr &&
      (qdisc->kind == QdiscKind::kNetem || qdisc->kind == QdiscKind::kTbf ||
       qdisc->kind == QdiscKind::kTbfNetem)) {
    if (delete_error) {
      std::rethrow_exception(delete_error);
    }
    throw std::runtime_error(
        "network condition qdisc remained after rollback removal");
  }
}

QdiscInfo ReplaceNodeNetworkConditionTransactional(
    NodeRuntime* node, const NetworkCondition& condition,
    std::stop_token stop_token) {
  if (!node->network) {
    throw std::runtime_error(
        "runtime network condition requires isolated networking");
  }
  const NodeVethConfig previous = *node->network;
  NodeVethConfig updated = previous;
  updated.apply_condition = true;
  updated.condition = condition;
  try {
    ThrowIfStopRequested(stop_token);
    ReplaceNetworkConditionQdisc(updated.host_name, updated.condition);
    ThrowIfStopRequested(stop_token);
    const QdiscInfo qdisc = VerifyNodeNetworkCondition(updated, stop_token);
    node->network = updated;
    node->network_profile.clear();
    return qdisc;
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::string rollback_error;
    try {
      RestoreNodeNetworkCondition(previous);
    } catch (const std::exception& restore_error) {
      rollback_error = restore_error.what();
      BBP_LOG(error) << "failed to restore network condition for "
                     << node->config.id << ": " << restore_error.what();
    } catch (...) {
      rollback_error = "unknown exception";
      BBP_LOG(error) << "failed to restore network condition for "
                     << node->config.id << ": unknown exception";
    }
    if (!rollback_error.empty()) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "network condition update outcome is unconfirmed", original_error,
          {rollback_error});
    }
    std::rethrow_exception(original_error);
  }
}

}  // namespace bbp::simulator_app_internal
