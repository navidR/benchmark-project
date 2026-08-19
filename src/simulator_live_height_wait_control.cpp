#include "simulator_live_height_wait_control.h"

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/mcp_operation_service.h"
#include "bbp/mcp_registry.h"
#include "bbp/simulator/wait_until_height_workload.h"
#include "simulator_live_workload_state.h"

namespace bbp::simulator_app_internal {

LiveWaitUntilHeightOperation MakeLiveWaitUntilHeightOperation(
    LiveWaitUntilHeightRecordFinder find_record,
    LiveWaitUntilHeightStarter start_workload,
    LiveWaitUntilHeightReconfigurationParser parse_reconfiguration,
    LiveWaitUntilHeightStatePublisher publish_state) {
  return [find_record = std::move(find_record),
          start_workload = std::move(start_workload),
          parse_reconfiguration = std::move(parse_reconfiguration),
          publish_state = std::move(publish_state)](
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
    const auto expire_epoch_if_due =
        [&](LiveWaitUntilHeightWorkloadRecord& record) {
          if (record.completion_pending ||
              IsTerminalLiveWorkloadState(record.state) ||
              (record.state != LiveWorkloadState::kRunning &&
               !(record.state == LiveWorkloadState::kStopping &&
                 record.request == LiveWorkloadRequest::kStopSettle)) ||
              record.epoch_deadline ==
                  std::chrono::steady_clock::time_point{} ||
              (record.epoch_run_stop_requested_at &&
               *record.epoch_run_stop_requested_at < record.epoch_deadline) ||
              std::chrono::steady_clock::now() < record.epoch_deadline ||
              (record.request != LiveWorkloadRequest::kNone &&
               record.request != LiveWorkloadRequest::kStopSettle)) {
            return record.epoch_timed_out;
          }
          record.epoch_timed_out = true;
          record.epoch_stop_source.request_stop();
          record.changed.notify_all();
          return true;
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
      const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord> record =
          start_workload(workload_value->as_object(), std::move(requested_id),
                         operation_stop_token);
      std::unique_lock<std::mutex> lock(record->mutex);
      if (!record->changed.wait(lock, operation_stop_token, [&] {
            return record->state != LiveWorkloadState::kStarting;
          })) {
        record->request = LiveWorkloadRequest::kStopCancel;
        record->epoch_stop_source.request_stop();
        record->changed.notify_all();
        throw McpOperationCancelled();
      }
      lock.unlock();
      return LiveWaitUntilHeightWorkloadJson(*record);
    }

    const std::string workload_id = require_argument_string("workload_id");
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord> record =
        find_record(workload_id);
    if (!record) {
      throw McpOperationFailure(
          "workload_not_found",
          "wait-until-height workload instance is not registered: " +
              workload_id,
          false);
    }
    if (kind == McpOperationKind::kInspectWorkload) {
      return LiveWaitUntilHeightWorkloadJson(*record);
    }
    if (kind == McpOperationKind::kReconfigureWorkload) {
      const boost::json::value* workload_value =
          arguments.if_contains("workload");
      if (workload_value == nullptr || !workload_value->is_object()) {
        throw std::invalid_argument(
            "workload.reconfigure requires a workload object");
      }
      WaitUntilHeightWorkload updated = parse_reconfiguration(
          workload_value->as_object(), operation_stop_token);
      bool publish_paused_revision = false;
      std::uint64_t expected_revision = 0U;
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (IsTerminalLiveWorkloadState(record->state)) {
          throw McpOperationFailure(
              "workload_not_active",
              "terminal wait-until-height workload cannot be reconfigured",
              false);
        }
        if (expire_epoch_if_due(*record)) {
          throw McpOperationFailure(
              "workload_transition_in_progress",
              "wait-until-height workload epoch deadline has elapsed", true);
        }
        if (record->state == LiveWorkloadState::kStarting ||
            record->state == LiveWorkloadState::kStopping) {
          throw McpOperationFailure(
              "workload_transition_in_progress",
              "wait-until-height workload already has a lifecycle transition "
              "in progress",
              true);
        }
        if (record->configuration_revision ==
            std::numeric_limits<std::uint64_t>::max()) {
          throw std::runtime_error(
              "wait-until-height workload configuration revision exceeds "
              "uint64");
        }
        expected_revision = record->configuration_revision;
        if (record->state == LiveWorkloadState::kPaused) {
          record->workload = updated;
          ++record->configuration_revision;
          publish_paused_revision = true;
        } else {
          record->pending_workload = updated;
          record->request = LiveWorkloadRequest::kReconfigure;
          record->state = LiveWorkloadState::kStopping;
          record->epoch_stop_source.request_stop();
        }
        record->changed.notify_all();
      }
      if (publish_paused_revision) {
        publish_state(*record);
      } else {
        std::unique_lock<std::mutex> lock(record->mutex);
        if (!record->changed.wait(lock, operation_stop_token, [&] {
              return (record->configuration_revision > expected_revision &&
                      record->state == LiveWorkloadState::kRunning) ||
                     IsTerminalLiveWorkloadState(record->state);
            })) {
          throw McpOperationCancelled();
        }
      }
      return LiveWaitUntilHeightWorkloadJson(*record);
    }
    if (kind == McpOperationKind::kResumeWorkload) {
      const auto deadline =
          std::chrono::steady_clock::now() + operation_timeout();
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (record->state != LiveWorkloadState::kPaused) {
          throw McpOperationFailure(
              "workload_not_paused",
              "wait-until-height workload is not paused: " + workload_id,
              false);
        }
        record->request = LiveWorkloadRequest::kNone;
        record->state = LiveWorkloadState::kStarting;
        record->changed.notify_all();
      }
      std::unique_lock<std::mutex> lock(record->mutex);
      if (!record->changed.wait_until(
              lock, operation_stop_token, deadline, [&] {
                return record->state == LiveWorkloadState::kRunning ||
                       IsTerminalLiveWorkloadState(record->state);
              })) {
        if (operation_stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        throw McpOperationFailure(
            "workload_operation_timeout",
            "wait-until-height workload did not resume before timeout", true);
      }
      lock.unlock();
      return LiveWaitUntilHeightWorkloadJson(*record);
    }
    const auto deadline =
        std::chrono::steady_clock::now() + operation_timeout();
    std::optional<std::string> stop_policy;
    if (kind == McpOperationKind::kStopWorkload) {
      stop_policy = "cancel";
      if (const boost::json::value* value = arguments.if_contains("policy")) {
        if (!value->is_string()) {
          throw std::invalid_argument("workload.stop policy must be a string");
        }
        stop_policy = std::string(value->as_string());
      }
      if (*stop_policy != "cancel" && *stop_policy != "settle") {
        throw std::invalid_argument(
            "workload.stop policy must be cancel or settle");
      }
    }
    bool already_paused = false;
    {
      std::lock_guard<std::mutex> lock(record->mutex);
      if (IsTerminalLiveWorkloadState(record->state)) {
        throw McpOperationFailure(
            "workload_not_active",
            "wait-until-height workload is already terminal: " + workload_id,
            false);
      }
      if (expire_epoch_if_due(*record)) {
        throw McpOperationFailure(
            "workload_transition_in_progress",
            "wait-until-height workload epoch deadline has elapsed", true);
      }
      if (record->state == LiveWorkloadState::kStarting) {
        throw McpOperationFailure(
            "workload_transition_in_progress",
            "wait-until-height workload already has a lifecycle transition in "
            "progress",
            true);
      }
      if (kind == McpOperationKind::kPauseWorkload &&
          record->state == LiveWorkloadState::kPaused) {
        already_paused = true;
      } else if (record->state == LiveWorkloadState::kStopping) {
        if (kind == McpOperationKind::kStopWorkload &&
            stop_policy == "cancel" &&
            record->request == LiveWorkloadRequest::kStopSettle &&
            !record->completion_pending) {
          record->request = LiveWorkloadRequest::kStopCancel;
          record->epoch_stop_source.request_stop();
        } else {
          throw McpOperationFailure(
              "workload_transition_in_progress",
              "wait-until-height workload already has a lifecycle transition "
              "in progress",
              true);
        }
      } else if (kind == McpOperationKind::kPauseWorkload) {
        record->request = LiveWorkloadRequest::kPause;
        record->state = LiveWorkloadState::kStopping;
        record->epoch_stop_source.request_stop();
      } else if (kind == McpOperationKind::kStopWorkload) {
        record->request = stop_policy == "settle"
                              ? LiveWorkloadRequest::kStopSettle
                              : LiveWorkloadRequest::kStopCancel;
        record->state = LiveWorkloadState::kStopping;
        if (stop_policy == "cancel") {
          record->epoch_stop_source.request_stop();
        }
      } else {
        throw std::logic_error("unknown wait-until-height workload operation");
      }
      record->changed.notify_all();
    }
    if (already_paused) {
      return LiveWaitUntilHeightWorkloadJson(*record);
    }
    std::unique_lock<std::mutex> lock(record->mutex);
    const auto reached_target = [&] {
      return kind == McpOperationKind::kPauseWorkload
                 ? record->state == LiveWorkloadState::kPaused ||
                       IsTerminalLiveWorkloadState(record->state)
                 : IsTerminalLiveWorkloadState(record->state);
    };
    if (!record->changed.wait_until(lock, operation_stop_token, deadline,
                                    reached_target)) {
      if (operation_stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      throw McpOperationFailure(
          "workload_operation_timeout",
          "wait-until-height workload did not reach the requested state before "
          "timeout",
          true);
    }
    lock.unlock();
    return LiveWaitUntilHeightWorkloadJson(*record);
  };
}

}  // namespace bbp::simulator_app_internal
