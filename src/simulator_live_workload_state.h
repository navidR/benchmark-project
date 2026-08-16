#pragma once

#include <boost/json/object.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/transaction_load.h"
#include "simulator_wallet_transaction_workload_execution.h"

namespace bbp::simulator_app_internal {

enum class LiveWorkloadState {
  kStarting,
  kRunning,
  kPaused,
  kStopping,
  kStopped,
  kCompleted,
  kCancelled,
  kFailed,
};

enum class LiveWorkloadRequest {
  kNone,
  kPause,
  kReconfigure,
  kStopSettle,
  kStopCancel,
  kShutdown,
  kRunFailure,
};

using LiveWalletWorkloadState = LiveWorkloadState;
using LiveWalletWorkloadRequest = LiveWorkloadRequest;

std::string_view LiveWorkloadStateName(LiveWorkloadState state);
std::string_view LiveWalletWorkloadStateName(LiveWalletWorkloadState state);
bool IsTerminalLiveWorkloadState(LiveWorkloadState state);
bool IsTerminalLiveWalletWorkloadState(LiveWalletWorkloadState state);

struct LiveWalletWorkloadRecord {
  mutable std::mutex mutex;
  std::condition_variable_any changed;
  std::string id;
  std::uint32_t ordinal = 0U;
  WalletTransactionsWorkload workload;
  RuntimeWalletSnapshot wallet_snapshot;
  std::optional<WalletTransactionsWorkload> pending_workload;
  std::vector<std::uint32_t> claimed_wallets;
  LiveWalletWorkloadState state = LiveWalletWorkloadState::kStarting;
  LiveWalletWorkloadRequest request = LiveWalletWorkloadRequest::kNone;
  std::string terminal_outcome = "none";
  std::optional<std::string> failure;
  std::uint64_t configuration_revision = 1U;
  std::uint64_t next_transaction_index = 0U;
  std::uint64_t planned = 0U;
  std::uint64_t accepted = 0U;
  std::uint64_t reserved_atomic_units = 0U;
  std::uint64_t released_atomic_units = 0U;
  std::uint64_t cancelled_tracking = 0U;
  std::size_t queue_maximum_size = 0U;
  std::optional<std::vector<WalletWorkloadFundingState>> prepared_funding;
  std::chrono::steady_clock::time_point started_at =
      std::chrono::steady_clock::now();
  std::stop_source epoch_stop_source;
  std::shared_ptr<TransactionLoadAccounting> accounting =
      std::make_shared<TransactionLoadAccounting>();
  std::thread worker;
};

struct LiveWalletWorkloadRegistry {
  mutable std::mutex mutex;
  std::map<std::string, std::shared_ptr<LiveWalletWorkloadRecord>> records;
  std::uint64_t next_id = 1U;
  bool shutting_down = false;
};

struct LiveBlockGenerationBoundaryResult {
  std::uint32_t generator_node = 0U;
  std::string generator_node_id;
  std::uint64_t start_height = 0U;
  std::uint64_t target_height = 0U;
  std::string block_hash;
  std::string reward_address;
  bool synchronized = false;
};

struct LiveBlockGenerationWorkloadRecord {
  mutable std::mutex mutex;
  std::condition_variable_any changed;
  std::string id;
  std::uint32_t ordinal = 0U;
  BlockGenerationWorkload workload;
  std::optional<BlockGenerationWorkload> pending_workload;
  LiveWorkloadState state = LiveWorkloadState::kStarting;
  LiveWorkloadRequest request = LiveWorkloadRequest::kNone;
  std::string terminal_outcome = "none";
  std::optional<std::string> failure;
  std::uint64_t configuration_revision = 1U;
  std::uint64_t attempted = 0U;
  std::uint64_t generated = 0U;
  std::uint64_t completed_boundaries = 0U;
  std::uint64_t failed = 0U;
  std::uint64_t cancelled = 0U;
  bool generation_outcome_unconfirmed = false;
  bool boundary_mutation_admitted = false;
  std::optional<LiveBlockGenerationBoundaryResult> last_result;
  std::chrono::steady_clock::time_point started_at =
      std::chrono::steady_clock::now();
  std::stop_source boundary_stop_source;
  std::thread worker;
};

struct LiveBlockGenerationWorkloadRegistry {
  mutable std::mutex mutex;
  std::map<std::string, std::shared_ptr<LiveBlockGenerationWorkloadRecord>>
      records;
  std::uint64_t next_id = 1U;
  bool shutting_down = false;
};

struct LiveWaitUntilHeightResult {
  std::uint32_t node = 0U;
  std::string node_id;
  std::uint64_t target_height = 0U;
  std::uint64_t observed_height = 0U;
};

struct LiveWaitUntilHeightWorkloadRecord {
  mutable std::mutex mutex;
  std::condition_variable_any changed;
  std::string id;
  std::uint32_t ordinal = 0U;
  WaitUntilHeightWorkload workload;
  std::optional<WaitUntilHeightWorkload> pending_workload;
  LiveWorkloadState state = LiveWorkloadState::kStarting;
  LiveWorkloadRequest request = LiveWorkloadRequest::kNone;
  std::string terminal_outcome = "none";
  std::optional<std::string> failure;
  std::uint64_t configuration_revision = 1U;
  std::optional<LiveWaitUntilHeightResult> result;
  bool completion_pending = false;
  std::chrono::steady_clock::time_point epoch_deadline{};
  bool epoch_timed_out = false;
  std::optional<std::chrono::steady_clock::time_point>
      epoch_run_stop_requested_at;
  std::stop_source epoch_stop_source;
  std::thread worker;
};

struct LiveWaitUntilHeightWorkloadRegistry {
  mutable std::mutex mutex;
  std::map<std::string, std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>>
      records;
  std::map<std::string, std::size_t> scenario_admissions;
  std::uint64_t next_id = 1U;
  bool shutting_down = false;
};

class ScenarioHeightWaitAdmissionLease {
 public:
  ScenarioHeightWaitAdmissionLease(
      std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry> registry,
      std::string node_id)
      : registry_(std::move(registry)), node_id_(std::move(node_id)) {}

  ScenarioHeightWaitAdmissionLease(const ScenarioHeightWaitAdmissionLease&) =
      delete;
  ScenarioHeightWaitAdmissionLease& operator=(
      const ScenarioHeightWaitAdmissionLease&) = delete;
  ScenarioHeightWaitAdmissionLease(
      ScenarioHeightWaitAdmissionLease&&) noexcept = default;
  ScenarioHeightWaitAdmissionLease& operator=(
      ScenarioHeightWaitAdmissionLease&&) = delete;

  ~ScenarioHeightWaitAdmissionLease() {
    if (!registry_) {
      return;
    }
    std::lock_guard<std::mutex> lock(registry_->mutex);
    const auto found = registry_->scenario_admissions.find(node_id_);
    if (found == registry_->scenario_admissions.end()) {
      return;
    }
    if (found->second > 1U) {
      --found->second;
    } else {
      registry_->scenario_admissions.erase(found);
    }
  }

 private:
  std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry> registry_;
  std::string node_id_;
};

ScenarioHeightWaitAdmissionLease AcquireScenarioHeightWaitAdmission(
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>& registry,
    const std::string& node_id);

struct LiveWaitForPeersResult {
  std::uint32_t node = 0U;
  std::string node_id;
  std::uint64_t target_peer_count = 0U;
  std::uint64_t observed_peer_count = 0U;
};

struct LiveWaitForPeersWorkloadRecord {
  mutable std::mutex mutex;
  std::condition_variable_any changed;
  std::string id;
  std::uint32_t ordinal = 0U;
  WaitForPeersWorkload workload;
  std::optional<WaitForPeersWorkload> pending_workload;
  LiveWorkloadState state = LiveWorkloadState::kStarting;
  LiveWorkloadRequest request = LiveWorkloadRequest::kNone;
  std::string terminal_outcome = "none";
  std::optional<std::string> failure;
  std::uint64_t configuration_revision = 1U;
  std::optional<LiveWaitForPeersResult> result;
  bool completion_pending = false;
  std::chrono::steady_clock::time_point epoch_deadline{};
  bool epoch_timed_out = false;
  std::optional<std::chrono::steady_clock::time_point>
      epoch_run_stop_requested_at;
  std::stop_source epoch_stop_source;
  std::thread worker;
};

struct LiveWaitForPeersWorkloadRegistry {
  mutable std::mutex mutex;
  std::map<std::string, std::shared_ptr<LiveWaitForPeersWorkloadRecord>>
      records;
  std::uint64_t next_id = 1U;
  bool shutting_down = false;
};

boost::json::object LiveBlockGenerationWorkloadJson(
    const LiveBlockGenerationWorkloadRecord& record);
boost::json::object LiveWaitUntilHeightWorkloadJson(
    const LiveWaitUntilHeightWorkloadRecord& record);
boost::json::object LiveWaitForPeersWorkloadJson(
    const LiveWaitForPeersWorkloadRecord& record);
boost::json::object LiveWalletWorkloadJson(
    const LiveWalletWorkloadRecord& record);

void WriteLiveBlockGenerationWorkloadState(
    const std::filesystem::path& events_path, const Options& options,
    const LiveBlockGenerationWorkloadRecord& record);
void WriteLiveWaitUntilHeightWorkloadState(
    const std::filesystem::path& events_path, const Options& options,
    const LiveWaitUntilHeightWorkloadRecord& record);
void WriteLiveWaitForPeersWorkloadState(
    const std::filesystem::path& events_path, const Options& options,
    const LiveWaitForPeersWorkloadRecord& record);
void WriteLiveWalletWorkloadState(const std::filesystem::path& events_path,
                                  const Options& options,
                                  const LiveWalletWorkloadRecord& record);

void RequireNoActiveBlockGenerationWorkloads(
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>& registry,
    std::string_view operation);
void RequireNoActiveWaitUntilHeightWorkloads(
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>& registry,
    std::string_view operation, const std::set<std::string>& mutated_node_ids);
void RequireNoActiveWaitForPeersWorkloads(
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>& registry,
    std::string_view operation);

std::vector<std::uint32_t> ClaimedWalletsForWorkload(
    const WalletTransactionsWorkload& workload, std::size_t wallet_count);
bool WalletClaimsOverlap(std::span<const std::uint32_t> lhs,
                         std::span<const std::uint32_t> rhs);
void CheckedWalletWorkloadAdd(std::uint64_t amount, std::uint64_t* target,
                              std::string_view field);

}  // namespace bbp::simulator_app_internal
