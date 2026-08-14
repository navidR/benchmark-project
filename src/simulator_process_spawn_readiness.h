#pragma once

#include <chrono>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

class ChainDriver;
struct ChainNodeConfig;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

class NodeExitedBeforeRpcReady final : public std::runtime_error {
 public:
  NodeExitedBeforeRpcReady(std::string_view node_id, int wait_status)
      : std::runtime_error("node process exited before RPC readiness: " +
                           std::string(node_id)),
        wait_status_(wait_status) {}

  int wait_status() const { return wait_status_; }

 private:
  int wait_status_;
};

void StartNodeProcessAttempt(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node,
    std::mutex& node_network_state_mutex, std::string_view reason,
    std::chrono::steady_clock::time_point lifecycle_epoch, bool restart_attempt,
    bool transition_to_running, std::stop_token stop_token,
    const ChainNodeConfig* process_config_override = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
