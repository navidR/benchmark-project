#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <memory>

#include "bbp/simulator/options.h"
#include "simulator_live_workload_state.h"

namespace bbp::simulator_app_internal {

struct WorkloadShutdownRecords {
  std::array<std::shared_ptr<LiveWalletWorkloadRecord>,
             kMaximumScenarioActionCount>
      wallets;
  std::array<std::shared_ptr<LiveBlockGenerationWorkloadRecord>,
             kMaximumScenarioActionCount>
      block_generators;
  std::array<std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>,
             kMaximumScenarioActionCount>
      height_waits;
  std::array<std::shared_ptr<LiveWaitForPeersWorkloadRecord>,
             kMaximumScenarioActionCount>
      peer_waits;
  std::size_t wallet_count = 0U;
  std::size_t block_generator_count = 0U;
  std::size_t height_wait_count = 0U;
  std::size_t peer_wait_count = 0U;
};

WorkloadShutdownRecords RequestLiveWorkloadShutdown(
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    bool run_failed,
    std::chrono::steady_clock::time_point shutdown_requested_at);

}  // namespace bbp::simulator_app_internal
