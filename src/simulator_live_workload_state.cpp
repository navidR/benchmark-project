#include "simulator_live_workload_state.h"

#include <algorithm>
#include <boost/json/serialize.hpp>
#include <limits>
#include <stdexcept>
#include <utility>

#include "bbp/simulation_cancelled.h"
#include "simulator_event_writing.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

std::string_view LiveWorkloadStateName(LiveWorkloadState state) {
  switch (state) {
    case LiveWorkloadState::kStarting:
      return "starting";
    case LiveWorkloadState::kRunning:
      return "running";
    case LiveWorkloadState::kPaused:
      return "paused";
    case LiveWorkloadState::kStopping:
      return "stopping";
    case LiveWorkloadState::kStopped:
      return "stopped";
    case LiveWorkloadState::kCompleted:
      return "completed";
    case LiveWorkloadState::kCancelled:
      return "cancelled";
    case LiveWorkloadState::kFailed:
      return "failed";
  }
  throw std::logic_error("unknown live workload state");
}

std::string_view LiveWalletWorkloadStateName(LiveWalletWorkloadState state) {
  return LiveWorkloadStateName(state);
}

bool IsTerminalLiveWorkloadState(LiveWorkloadState state) {
  return state == LiveWorkloadState::kStopped ||
         state == LiveWorkloadState::kCompleted ||
         state == LiveWorkloadState::kCancelled ||
         state == LiveWorkloadState::kFailed;
}

bool IsTerminalLiveWalletWorkloadState(LiveWalletWorkloadState state) {
  return IsTerminalLiveWorkloadState(state);
}

ScenarioHeightWaitAdmissionLease AcquireScenarioHeightWaitAdmission(
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>& registry,
    const std::string& node_id) {
  if (!registry) {
    throw std::logic_error("scenario height-wait workload service is missing");
  }
  std::string admitted_node_id = node_id;
  std::lock_guard<std::mutex> lock(registry->mutex);
  if (registry->shutting_down) {
    throw SimulationCancelled();
  }
  ++registry->scenario_admissions[admitted_node_id];
  return ScenarioHeightWaitAdmissionLease(registry,
                                          std::move(admitted_node_id));
}

boost::json::object LiveBlockGenerationWorkloadJson(
    const LiveBlockGenerationWorkloadRecord& record) {
  std::lock_guard<std::mutex> lock(record.mutex);
  if (record.completed_boundaries >
          std::numeric_limits<std::uint64_t>::max() - record.failed ||
      record.completed_boundaries + record.failed >
          std::numeric_limits<std::uint64_t>::max() - record.cancelled) {
    throw std::runtime_error(
        "block generation workload final accounting exceeds uint64");
  }
  const std::uint64_t finalized =
      record.completed_boundaries + record.failed + record.cancelled;
  if (finalized > record.attempted || record.generated > record.attempted) {
    throw std::runtime_error(
        "block generation workload accounting is inconsistent");
  }
  const std::uint64_t target = record.workload.count;
  const std::uint64_t remaining = target > record.completed_boundaries
                                      ? target - record.completed_boundaries
                                      : 0U;
  const std::uint64_t outstanding = record.attempted - finalized;
  boost::json::object accounting{
      {"target", target},
      {"attempted", record.attempted},
      {"generated", record.generated},
      {"completed_boundaries", record.completed_boundaries},
      {"failed", record.failed},
      {"cancelled", record.cancelled},
      {"remaining", remaining},
      {"outstanding", outstanding},
      {"in_flight", outstanding},
      {"generation_outcome_unconfirmed", record.generation_outcome_unconfirmed},
  };
  boost::json::object result{
      {"workload_id", record.id},
      {"state", LiveWorkloadStateName(record.state)},
      {"terminal_outcome", record.terminal_outcome},
      {"configuration_revision", record.configuration_revision},
      {"configuration", BlockGenerationWorkloadJson(record.workload)},
      {"accounting", std::move(accounting)},
  };
  if (record.last_result) {
    result["last_result"] = boost::json::object{
        {"generator_node", record.last_result->generator_node},
        {"generator_node_id", record.last_result->generator_node_id},
        {"start_height", record.last_result->start_height},
        {"target_height", record.last_result->target_height},
        {"block_hash", record.last_result->block_hash},
        {"reward_address", record.last_result->reward_address},
        {"synchronized", record.last_result->synchronized},
    };
  } else {
    result["last_result"] = nullptr;
  }
  result["failure"] = record.failure ? boost::json::value(*record.failure)
                                     : boost::json::value(nullptr);
  return result;
}

boost::json::object LiveWaitUntilHeightWorkloadJson(
    const LiveWaitUntilHeightWorkloadRecord& record) {
  std::lock_guard<std::mutex> lock(record.mutex);
  boost::json::object result{
      {"workload_id", record.id},
      {"state", LiveWorkloadStateName(record.state)},
      {"terminal_outcome", record.terminal_outcome},
      {"configuration_revision", record.configuration_revision},
      {"configuration", WaitUntilHeightWorkloadJson(record.workload)},
  };
  if (record.result) {
    result["result"] = boost::json::object{
        {"node", record.result->node},
        {"node_id", record.result->node_id},
        {"target_height", record.result->target_height},
        {"observed_height", record.result->observed_height},
    };
  } else {
    result["result"] = nullptr;
  }
  result["failure"] = record.failure ? boost::json::value(*record.failure)
                                     : boost::json::value(nullptr);
  return result;
}

boost::json::object LiveWaitForPeersWorkloadJson(
    const LiveWaitForPeersWorkloadRecord& record) {
  std::lock_guard<std::mutex> lock(record.mutex);
  boost::json::object result{
      {"workload_id", record.id},
      {"state", LiveWorkloadStateName(record.state)},
      {"terminal_outcome", record.terminal_outcome},
      {"configuration_revision", record.configuration_revision},
      {"configuration", WaitForPeersWorkloadJson(record.workload)},
  };
  if (record.result) {
    result["result"] = boost::json::object{
        {"node", record.result->node},
        {"node_id", record.result->node_id},
        {"target_peer_count", record.result->target_peer_count},
        {"observed_peer_count", record.result->observed_peer_count},
    };
  } else {
    result["result"] = nullptr;
  }
  result["failure"] = record.failure ? boost::json::value(*record.failure)
                                     : boost::json::value(nullptr);
  return result;
}

void WriteLiveBlockGenerationWorkloadState(
    const std::filesystem::path& events_path, const Options& options,
    const LiveBlockGenerationWorkloadRecord& record) {
  WriteEvent(events_path, options.run_id, record.id,
             SimulationEventKind::kWorkloadState,
             boost::json::serialize(LiveBlockGenerationWorkloadJson(record)));
}

void WriteLiveWaitUntilHeightWorkloadState(
    const std::filesystem::path& events_path, const Options& options,
    const LiveWaitUntilHeightWorkloadRecord& record) {
  WriteEvent(events_path, options.run_id, record.id,
             SimulationEventKind::kWorkloadState,
             boost::json::serialize(LiveWaitUntilHeightWorkloadJson(record)));
}

void WriteLiveWaitForPeersWorkloadState(
    const std::filesystem::path& events_path, const Options& options,
    const LiveWaitForPeersWorkloadRecord& record) {
  WriteEvent(events_path, options.run_id, record.id,
             SimulationEventKind::kWorkloadState,
             boost::json::serialize(LiveWaitForPeersWorkloadJson(record)));
}

void RequireNoActiveBlockGenerationWorkloads(
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>& registry,
    std::string_view operation) {
  if (!registry) {
    throw std::logic_error(std::string(operation) +
                           " block workload service is missing");
  }
  std::vector<std::shared_ptr<LiveBlockGenerationWorkloadRecord>> records;
  {
    std::lock_guard<std::mutex> lock(registry->mutex);
    records.reserve(registry->records.size());
    for (const auto& [id, record] : registry->records) {
      static_cast<void>(id);
      records.push_back(record);
    }
  }
  for (const std::shared_ptr<LiveBlockGenerationWorkloadRecord>& record :
       records) {
    std::lock_guard<std::mutex> lock(record->mutex);
    if (!IsTerminalLiveWorkloadState(record->state)) {
      throw std::runtime_error(
          std::string(operation) +
          " is unavailable while block generation workload " + record->id +
          " is " + std::string(LiveWorkloadStateName(record->state)));
    }
  }
}

void RequireNoActiveWaitUntilHeightWorkloads(
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>& registry,
    std::string_view operation, const std::set<std::string>& mutated_node_ids) {
  if (!registry) {
    throw std::logic_error(std::string(operation) +
                           " height-wait workload service is missing");
  }
  std::vector<std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>> records;
  {
    std::lock_guard<std::mutex> lock(registry->mutex);
    for (const std::string& node_id : mutated_node_ids) {
      if (registry->scenario_admissions.contains(node_id)) {
        throw std::runtime_error(
            std::string(operation) +
            " is unavailable while a scenario wait_until_height is active "
            "for node " +
            node_id);
      }
    }
    records.reserve(registry->records.size());
    for (const auto& [id, record] : registry->records) {
      static_cast<void>(id);
      records.push_back(record);
    }
  }
  for (const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>& record :
       records) {
    std::lock_guard<std::mutex> lock(record->mutex);
    if (!IsTerminalLiveWorkloadState(record->state)) {
      throw std::runtime_error(
          std::string(operation) +
          " is unavailable while wait-until-height workload " + record->id +
          " is " + std::string(LiveWorkloadStateName(record->state)));
    }
  }
}

void RequireNoActiveWaitForPeersWorkloads(
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>& registry,
    std::string_view operation) {
  if (!registry) {
    throw std::logic_error(std::string(operation) +
                           " peer-wait workload service is missing");
  }
  std::vector<std::shared_ptr<LiveWaitForPeersWorkloadRecord>> records;
  {
    std::lock_guard<std::mutex> lock(registry->mutex);
    records.reserve(registry->records.size());
    for (const auto& [id, record] : registry->records) {
      static_cast<void>(id);
      records.push_back(record);
    }
  }
  for (const std::shared_ptr<LiveWaitForPeersWorkloadRecord>& record :
       records) {
    std::lock_guard<std::mutex> lock(record->mutex);
    if (!IsTerminalLiveWorkloadState(record->state)) {
      throw std::runtime_error(
          std::string(operation) + " is unavailable while wait-for-peers " +
          "workload " + record->id + " is " +
          std::string(LiveWorkloadStateName(record->state)));
    }
  }
}

std::vector<std::uint32_t> ClaimedWalletsForWorkload(
    const WalletTransactionsWorkload& workload, std::size_t wallet_count) {
  std::vector<std::uint32_t> claimed;
  switch (workload.strategy) {
    case WalletTransferStrategy::kRoundRobin:
    case WalletTransferStrategy::kRandom:
    case WalletTransferStrategy::kRandomBruteforce:
      claimed.reserve(wallet_count);
      for (std::size_t index = 0U; index < wallet_count; ++index) {
        claimed.push_back(static_cast<std::uint32_t>(index + 1U));
      }
      break;
    case WalletTransferStrategy::kFanout:
    case WalletTransferStrategy::kEqualFanout:
      claimed = workload.sender_wallets;
      break;
    case WalletTransferStrategy::kHotspot:
      claimed.reserve(wallet_count);
      for (std::size_t index = 0U; index < wallet_count; ++index) {
        const std::uint32_t wallet = static_cast<std::uint32_t>(index + 1U);
        if (std::find(workload.receiver_wallets.begin(),
                      workload.receiver_wallets.end(),
                      wallet) == workload.receiver_wallets.end()) {
          claimed.push_back(wallet);
        }
      }
      break;
  }
  std::sort(claimed.begin(), claimed.end());
  return claimed;
}

bool WalletClaimsOverlap(std::span<const std::uint32_t> lhs,
                         std::span<const std::uint32_t> rhs) {
  return std::any_of(lhs.begin(), lhs.end(), [&](std::uint32_t wallet) {
    return std::binary_search(rhs.begin(), rhs.end(), wallet);
  });
}

void CheckedWalletWorkloadAdd(std::uint64_t amount, std::uint64_t* target,
                              std::string_view field) {
  if (*target > std::numeric_limits<std::uint64_t>::max() - amount) {
    throw std::runtime_error("wallet workload " + std::string(field) +
                             " accounting exceeds uint64");
  }
  *target += amount;
}

boost::json::object LiveWalletWorkloadJson(
    const LiveWalletWorkloadRecord& record) {
  std::lock_guard<std::mutex> lock(record.mutex);
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - record.started_at);
  const TransactionLoadSnapshot accounting =
      record.accounting->Snapshot(elapsed);
  const std::uint64_t in_flight = record.accepted >= accounting.attempted
                                      ? record.accepted - accounting.attempted
                                      : 0U;
  const std::uint64_t awaiting_confirmation =
      accounting.submitted >= accounting.confirmed
          ? accounting.submitted - accounting.confirmed
          : 0U;
  const std::uint64_t tracked_awaiting_confirmation =
      awaiting_confirmation >= record.cancelled_tracking
          ? awaiting_confirmation - record.cancelled_tracking
          : 0U;
  if (in_flight > std::numeric_limits<std::uint64_t>::max() -
                      tracked_awaiting_confirmation) {
    throw std::runtime_error(
        "wallet workload outstanding accounting exceeds uint64");
  }
  if (accounting.cancelled >
      std::numeric_limits<std::uint64_t>::max() - record.cancelled_tracking) {
    throw std::runtime_error(
        "wallet workload cancellation accounting exceeds uint64");
  }
  boost::json::object exact{
      {"planned", record.planned},
      {"accepted", record.accepted},
      {"attempted", accounting.attempted},
      {"submitted", accounting.submitted},
      {"propagated", accounting.propagated},
      {"confirmed", accounting.confirmed},
      {"rejected", accounting.rejected},
      {"timed_out", accounting.timed_out},
      {"backpressured", accounting.backpressured},
      {"dropped", accounting.dropped},
      {"failed", accounting.failed},
      {"retried", 0U},
      {"cancelled", accounting.cancelled + record.cancelled_tracking},
      {"cancelled_tracking", record.cancelled_tracking},
      {"outstanding", in_flight + tracked_awaiting_confirmation},
      {"in_flight", in_flight},
      {"reserved_atomic_units", record.reserved_atomic_units},
      {"released_atomic_units", record.released_atomic_units},
  };
  boost::json::object result{
      {"workload_id", record.id},
      {"state", LiveWalletWorkloadStateName(record.state)},
      {"terminal_outcome", record.terminal_outcome},
      {"configuration_revision", record.configuration_revision},
      {"configuration", WalletTransactionsWorkloadJson(record.workload)},
      {"accounting", std::move(exact)},
      {"queue_maximum_depth", record.queue_maximum_size},
  };
  if (record.failure) {
    result["failure"] = *record.failure;
  } else {
    result["failure"] = nullptr;
  }
  return result;
}

void WriteLiveWalletWorkloadState(const std::filesystem::path& events_path,
                                  const Options& options,
                                  const LiveWalletWorkloadRecord& record) {
  WriteEvent(events_path, options.run_id, record.id,
             SimulationEventKind::kWalletWorkloadState,
             boost::json::serialize(LiveWalletWorkloadJson(record)));
}

}  // namespace bbp::simulator_app_internal
