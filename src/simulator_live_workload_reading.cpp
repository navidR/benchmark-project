#include "simulator_live_workload_reading.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/mcp_operation_service.h"
#include "simulator_live_workload_state.h"

namespace bbp::simulator_app_internal {

boost::json::value ReadLiveWorkloads(
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    bool history, std::stop_token read_stop_token) {
  boost::json::array result;
  std::vector<std::shared_ptr<LiveWalletWorkloadRecord>> records;
  std::vector<std::shared_ptr<LiveBlockGenerationWorkloadRecord>> block_records;
  std::vector<std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>>
      height_wait_records;
  std::vector<std::shared_ptr<LiveWaitForPeersWorkloadRecord>>
      peer_wait_records;
  {
    std::scoped_lock lock(
        wallet_workloads->mutex, block_generation_workloads->mutex,
        wait_until_height_workloads->mutex, wait_for_peers_workloads->mutex);
    records.reserve(wallet_workloads->records.size());
    for (const auto& [id, record] : wallet_workloads->records) {
      static_cast<void>(id);
      records.push_back(record);
    }
    block_records.reserve(block_generation_workloads->records.size());
    for (const auto& [id, record] : block_generation_workloads->records) {
      static_cast<void>(id);
      block_records.push_back(record);
    }
    height_wait_records.reserve(wait_until_height_workloads->records.size());
    for (const auto& [id, record] : wait_until_height_workloads->records) {
      static_cast<void>(id);
      height_wait_records.push_back(record);
    }
    peer_wait_records.reserve(wait_for_peers_workloads->records.size());
    for (const auto& [id, record] : wait_for_peers_workloads->records) {
      static_cast<void>(id);
      peer_wait_records.push_back(record);
    }
  }
  const auto append_if_selected = [&](boost::json::object snapshot) {
    const std::string_view state = snapshot.at("state").as_string();
    const bool terminal = state == "stopped" || state == "completed" ||
                          state == "cancelled" || state == "failed";
    if (history == terminal) {
      result.emplace_back(std::move(snapshot));
    }
  };
  for (const std::shared_ptr<LiveWalletWorkloadRecord>& record : records) {
    if (read_stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    append_if_selected(LiveWalletWorkloadJson(*record));
  }
  for (const std::shared_ptr<LiveBlockGenerationWorkloadRecord>& record :
       block_records) {
    if (read_stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    append_if_selected(LiveBlockGenerationWorkloadJson(*record));
  }
  for (const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>& record :
       height_wait_records) {
    if (read_stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    append_if_selected(LiveWaitUntilHeightWorkloadJson(*record));
  }
  for (const std::shared_ptr<LiveWaitForPeersWorkloadRecord>& record :
       peer_wait_records) {
    if (read_stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    append_if_selected(LiveWaitForPeersWorkloadJson(*record));
  }
  return boost::json::value(std::move(result));
}

}  // namespace bbp::simulator_app_internal
