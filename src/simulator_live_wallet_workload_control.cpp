#include "simulator_live_wallet_workload_control.h"

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/mcp_operation_service.h"
#include "bbp/mcp_registry.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulator/wallet_transactions_workload.h"
#include "simulator_live_workload_state.h"

namespace bbp::simulator_app_internal {

LiveWalletWorkloadOperation MakeLiveWalletWorkloadOperation(
    std::shared_ptr<LiveWalletWorkloadRegistry> wallet_workloads,
    LiveWalletWorkloadStarter start_workload,
    LiveWalletWorkloadReconfigurationParser parse_reconfiguration,
    LiveWalletWorkloadSnapshotValidator snapshot_validator) {
  return [wallet_workloads = std::move(wallet_workloads),
          start_workload = std::move(start_workload),
          parse_reconfiguration = std::move(parse_reconfiguration),
          snapshot_validator = std::move(snapshot_validator)](
             McpOperationKind kind, const boost::json::object& arguments,
             std::stop_token operation_stop_token) {
    const auto require_argument_string = [&](std::string_view field) {
      const boost::json::value* value = arguments.if_contains(field);
      if (value == nullptr || !value->is_string() ||
          value->as_string().empty()) {
        throw std::invalid_argument("workload operation requires string " +
                                    std::string(field));
      }
      return std::string(value->as_string());
    };
    const auto operation_timeout = [&] {
      std::uint64_t seconds = 30U;
      if (const boost::json::value* value =
              arguments.if_contains("timeout_sec")) {
        if (value->is_uint64()) {
          seconds = value->as_uint64();
        } else if (value->is_int64() && value->as_int64() >= 0) {
          seconds = static_cast<std::uint64_t>(value->as_int64());
        } else {
          throw std::invalid_argument(
              "workload timeout_sec must be an unsigned integer");
        }
      }
      if (seconds == 0U || seconds > 3600U) {
        throw std::invalid_argument("workload timeout_sec must be in 1..3600");
      }
      return std::chrono::seconds(seconds);
    };
    if (kind == McpOperationKind::kStartWorkload) {
      const boost::json::value* workload_value =
          arguments.if_contains("workload");
      if (workload_value == nullptr || !workload_value->is_object()) {
        throw std::invalid_argument(
            "workload.start requires a workload object");
      }
      std::optional<std::string> requested_id;
      if (arguments.if_contains("workload_id") != nullptr) {
        requested_id = require_argument_string("workload_id");
      }
      const std::shared_ptr<LiveWalletWorkloadRecord> record =
          start_workload(workload_value->as_object(), std::move(requested_id),
                         operation_stop_token);
      std::unique_lock<std::mutex> lock(record->mutex);
      if (!record->changed.wait(lock, operation_stop_token, [&] {
            return record->state != LiveWalletWorkloadState::kStarting;
          })) {
        record->request = LiveWalletWorkloadRequest::kStopCancel;
        record->epoch_stop_source.request_stop();
        record->changed.notify_all();
        throw McpOperationCancelled();
      }
      lock.unlock();
      return LiveWalletWorkloadJson(*record);
    }

    const std::string workload_id = require_argument_string("workload_id");
    std::shared_ptr<LiveWalletWorkloadRecord> record;
    {
      std::lock_guard<std::mutex> lock(wallet_workloads->mutex);
      const auto found = wallet_workloads->records.find(workload_id);
      if (found == wallet_workloads->records.end()) {
        throw McpOperationFailure(
            "workload_not_found",
            "workload instance is not registered: " + workload_id, false);
      }
      record = found->second;
    }
    if (kind == McpOperationKind::kInspectWorkload) {
      return LiveWalletWorkloadJson(*record);
    }
    if (kind == McpOperationKind::kReconfigureWorkload) {
      const boost::json::value* workload_value =
          arguments.if_contains("workload");
      if (workload_value == nullptr || !workload_value->is_object()) {
        throw std::invalid_argument(
            "workload.reconfigure requires a workload object");
      }
      WalletTransactionsWorkload updated =
          parse_reconfiguration(workload_value->as_object());
      RuntimeWalletSnapshot updated_wallet_snapshot;
      std::vector<std::uint32_t> updated_claims;
      {
        std::lock_guard<std::mutex> registry_lock(wallet_workloads->mutex);
        updated_wallet_snapshot = snapshot_validator(updated);
        updated_claims = ClaimedWalletsForWorkload(
            updated, updated_wallet_snapshot.wallets().size());
        for (const auto& [other_id, other] : wallet_workloads->records) {
          if (other_id == workload_id) {
            continue;
          }
          std::lock_guard<std::mutex> other_lock(other->mutex);
          if (!IsTerminalLiveWalletWorkloadState(other->state) &&
              WalletClaimsOverlap(updated_claims, other->claimed_wallets)) {
            throw McpOperationFailure(
                "wallet_admission_conflict",
                "reconfigured sender admission overlaps active workload " +
                    other_id,
                true);
          }
        }
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (IsTerminalLiveWalletWorkloadState(record->state)) {
          throw McpOperationFailure(
              "workload_not_active",
              "terminal wallet workload cannot be reconfigured", false);
        }
        if (record->state == LiveWalletWorkloadState::kStopping) {
          throw McpOperationFailure(
              "workload_transition_in_progress",
              "wallet workload already has a lifecycle transition in "
              "progress",
              true);
        }
        if (updated.transaction_count != 0U &&
            record->next_transaction_index > updated.transaction_count) {
          throw McpOperationFailure(
              "workload_limit_conflict",
              "reconfigured maximum transaction count is below lifetime "
              "progress",
              false);
        }
        if (updated.strategy == WalletTransferStrategy::kEqualFanout &&
            updated.transaction_count != 0U &&
            (static_cast<std::uint64_t>(updated.transaction_count) -
             record->next_transaction_index) %
                    static_cast<std::uint64_t>(
                        updated.receiver_wallets.size()) !=
                0U) {
          throw McpOperationFailure(
              "workload_limit_conflict",
              "reconfigured equal_fanout maximum does not leave a complete "
              "receiver batch",
              false);
        }
        record->claimed_wallets = updated_claims;
        record->wallet_snapshot = updated_wallet_snapshot;
        if (record->state == LiveWalletWorkloadState::kPaused) {
          record->workload = std::move(updated);
          if (record->configuration_revision ==
              std::numeric_limits<std::uint64_t>::max()) {
            throw std::runtime_error(
                "wallet workload configuration revision exceeds uint64");
          }
          ++record->configuration_revision;
          record->changed.notify_all();
        } else {
          const bool execution_started =
              record->state != LiveWalletWorkloadState::kStarting;
          record->pending_workload = std::move(updated);
          record->request = LiveWalletWorkloadRequest::kReconfigure;
          record->state = LiveWalletWorkloadState::kStopping;
          if (execution_started) {
            record->epoch_stop_source.request_stop();
          }
          record->changed.notify_all();
        }
      }
      std::unique_lock<std::mutex> lock(record->mutex);
      const std::uint64_t expected_revision = record->configuration_revision;
      if (record->pending_workload &&
          !record->changed.wait(lock, operation_stop_token, [&] {
            return (record->configuration_revision > expected_revision &&
                    record->state == LiveWalletWorkloadState::kRunning) ||
                   IsTerminalLiveWalletWorkloadState(record->state);
          })) {
        throw McpOperationCancelled();
      }
      lock.unlock();
      return LiveWalletWorkloadJson(*record);
    }
    if (kind == McpOperationKind::kResumeWorkload) {
      const auto deadline =
          std::chrono::steady_clock::now() + operation_timeout();
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (record->state != LiveWalletWorkloadState::kPaused) {
          throw McpOperationFailure(
              "workload_not_paused",
              "wallet workload is not paused: " + workload_id, false);
        }
        record->request = LiveWalletWorkloadRequest::kNone;
        record->state = LiveWalletWorkloadState::kStarting;
        record->changed.notify_all();
      }
      std::unique_lock<std::mutex> lock(record->mutex);
      if (!record->changed.wait_until(
              lock, operation_stop_token, deadline, [&] {
                return record->state == LiveWalletWorkloadState::kRunning ||
                       IsTerminalLiveWalletWorkloadState(record->state);
              })) {
        if (operation_stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        throw McpOperationFailure(
            "workload_operation_timeout",
            "wallet workload did not resume before timeout", true);
      }
      lock.unlock();
      return LiveWalletWorkloadJson(*record);
    }
    const auto deadline =
        std::chrono::steady_clock::now() + operation_timeout();
    {
      std::lock_guard<std::mutex> lock(record->mutex);
      if (IsTerminalLiveWalletWorkloadState(record->state)) {
        throw McpOperationFailure(
            "workload_not_active",
            "wallet workload is already terminal: " + workload_id, false);
      }
      if (record->state == LiveWalletWorkloadState::kStopping) {
        throw McpOperationFailure(
            "workload_transition_in_progress",
            "wallet workload already has a lifecycle transition in progress",
            true);
      }
      if (kind == McpOperationKind::kPauseWorkload) {
        const bool execution_started =
            record->state != LiveWalletWorkloadState::kStarting;
        record->request = LiveWalletWorkloadRequest::kPause;
        record->state = LiveWalletWorkloadState::kStopping;
        if (execution_started) {
          record->epoch_stop_source.request_stop();
        }
      } else if (kind == McpOperationKind::kStopWorkload) {
        const bool execution_started =
            record->state != LiveWalletWorkloadState::kStarting;
        std::string policy = "cancel";
        if (const boost::json::value* value = arguments.if_contains("policy")) {
          if (!value->is_string()) {
            throw std::invalid_argument(
                "workload.stop policy must be a string");
          }
          policy = std::string(value->as_string());
        }
        if (policy != "cancel" && policy != "settle") {
          throw std::invalid_argument(
              "workload.stop policy must be cancel or settle");
        }
        record->request = policy == "settle"
                              ? LiveWalletWorkloadRequest::kStopSettle
                              : LiveWalletWorkloadRequest::kStopCancel;
        record->state = LiveWalletWorkloadState::kStopping;
        if (policy == "cancel" || !execution_started) {
          record->epoch_stop_source.request_stop();
        }
      } else {
        throw std::logic_error("unknown wallet workload operation");
      }
      record->changed.notify_all();
    }
    std::unique_lock<std::mutex> lock(record->mutex);
    const auto reached_target = [&] {
      return kind == McpOperationKind::kPauseWorkload
                 ? record->state == LiveWalletWorkloadState::kPaused ||
                       IsTerminalLiveWalletWorkloadState(record->state)
                 : IsTerminalLiveWalletWorkloadState(record->state);
    };
    if (!record->changed.wait_until(lock, operation_stop_token, deadline,
                                    reached_target)) {
      if (operation_stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      throw McpOperationFailure(
          "workload_operation_timeout",
          "wallet workload did not reach the requested state before timeout",
          true);
    }
    lock.unlock();
    return LiveWalletWorkloadJson(*record);
  };
}

}  // namespace bbp::simulator_app_internal
