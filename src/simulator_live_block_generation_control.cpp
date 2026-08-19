#include "simulator_live_block_generation_control.h"

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
#include "bbp/simulator/block_generation_workload.h"
#include "simulator_live_workload_state.h"

namespace bbp::simulator_app_internal {

LiveBlockGenerationOperation MakeLiveBlockGenerationOperation(
    LiveBlockGenerationRecordFinder find_record,
    LiveBlockGenerationStarter start_workload,
    LiveBlockGenerationReconfigurationParser parse_reconfiguration,
    LiveBlockGenerationStatePublisher publish_state) {
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
      const std::shared_ptr<LiveBlockGenerationWorkloadRecord> record =
          start_workload(workload_value->as_object(), std::move(requested_id),
                         operation_stop_token);
      std::unique_lock<std::mutex> lock(record->mutex);
      if (!record->changed.wait(lock, operation_stop_token, [&] {
            return record->state != LiveWorkloadState::kStarting;
          })) {
        record->request = LiveWorkloadRequest::kStopCancel;
        record->boundary_stop_source.request_stop();
        record->changed.notify_all();
        throw McpOperationCancelled();
      }
      lock.unlock();
      return LiveBlockGenerationWorkloadJson(*record);
    }

    const std::string workload_id = require_argument_string("workload_id");
    const std::shared_ptr<LiveBlockGenerationWorkloadRecord> record =
        find_record(workload_id);
    if (!record) {
      throw McpOperationFailure(
          "workload_not_found",
          "block generation workload instance is not registered: " +
              workload_id,
          false);
    }
    if (kind == McpOperationKind::kInspectWorkload) {
      return LiveBlockGenerationWorkloadJson(*record);
    }
    if (kind == McpOperationKind::kReconfigureWorkload) {
      const boost::json::value* workload_value =
          arguments.if_contains("workload");
      if (workload_value == nullptr || !workload_value->is_object()) {
        throw std::invalid_argument(
            "workload.reconfigure requires a workload object");
      }
      const BlockGenerationWorkload updated =
          parse_reconfiguration(workload_value->as_object());
      bool publish_paused_revision = false;
      std::uint64_t expected_revision = 0U;
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (IsTerminalLiveWorkloadState(record->state)) {
          throw McpOperationFailure(
              "workload_not_active",
              "terminal block generation workload cannot be reconfigured",
              false);
        }
        if (record->state == LiveWorkloadState::kStopping) {
          throw McpOperationFailure(
              "workload_transition_in_progress",
              "block generation workload already has a lifecycle transition "
              "in progress",
              true);
        }
        if (updated.count < record->attempted) {
          throw McpOperationFailure(
              "workload_limit_conflict",
              "reconfigured block count is below lifetime progress", false);
        }
        if (record->configuration_revision ==
            std::numeric_limits<std::uint64_t>::max()) {
          throw std::runtime_error(
              "block generation workload configuration revision exceeds "
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
      return LiveBlockGenerationWorkloadJson(*record);
    }
    if (kind == McpOperationKind::kResumeWorkload) {
      const auto deadline =
          std::chrono::steady_clock::now() + operation_timeout();
      {
        std::lock_guard<std::mutex> lock(record->mutex);
        if (record->state != LiveWorkloadState::kPaused) {
          throw McpOperationFailure(
              "workload_not_paused",
              "block generation workload is not paused: " + workload_id, false);
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
            "block generation workload did not resume before timeout", true);
      }
      lock.unlock();
      return LiveBlockGenerationWorkloadJson(*record);
    }
    const auto deadline =
        std::chrono::steady_clock::now() + operation_timeout();
    bool already_paused = false;
    {
      std::lock_guard<std::mutex> lock(record->mutex);
      if (IsTerminalLiveWorkloadState(record->state)) {
        throw McpOperationFailure(
            "workload_not_active",
            "block generation workload is already terminal: " + workload_id,
            false);
      }
      if (kind == McpOperationKind::kPauseWorkload &&
          record->state == LiveWorkloadState::kPaused) {
        already_paused = true;
      } else if (record->state == LiveWorkloadState::kStopping) {
        throw McpOperationFailure(
            "workload_transition_in_progress",
            "block generation workload already has a lifecycle transition in "
            "progress",
            true);
      } else if (kind == McpOperationKind::kPauseWorkload) {
        record->request = LiveWorkloadRequest::kPause;
        record->state = LiveWorkloadState::kStopping;
      } else if (kind == McpOperationKind::kStopWorkload) {
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
        record->request = policy == "settle" ? LiveWorkloadRequest::kStopSettle
                                             : LiveWorkloadRequest::kStopCancel;
        record->state = LiveWorkloadState::kStopping;
        if (policy == "cancel") {
          record->boundary_stop_source.request_stop();
        }
      } else {
        throw std::logic_error("unknown block generation workload operation");
      }
      record->changed.notify_all();
    }
    if (already_paused) {
      return LiveBlockGenerationWorkloadJson(*record);
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
          "block generation workload did not reach the requested state before "
          "timeout",
          true);
    }
    lock.unlock();
    return LiveBlockGenerationWorkloadJson(*record);
  };
}

}  // namespace bbp::simulator_app_internal
