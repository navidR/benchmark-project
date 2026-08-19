#pragma once

#include <boost/json/value.hpp>
#include <memory>
#include <stop_token>

namespace bbp::simulator_app_internal {

struct LiveWalletWorkloadRegistry;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;

boost::json::value ReadLiveWorkloads(
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    bool history, std::stop_token read_stop_token);

}  // namespace bbp::simulator_app_internal
