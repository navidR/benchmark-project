#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/peer_count_policy.h"

namespace bbp {

enum class PeerConnectivityAction {
  kConnected,
  kDisconnected,
};

class PeerConnectivityController {
 public:
  using NodeAvailableHandler = std::function<bool(std::string_view node_id)>;
  using ActionHandler = std::function<void(
      std::string_view node_id, std::string_view peer_node_id,
      PeerConnectivityAction action, const PeerCountPolicy& policy)>;
  using FailureHandler =
      std::function<void(std::string_view node_id, std::string_view error)>;

  PeerConnectivityController(const ChainDriver& driver,
                             std::vector<ChainNodeConfig> nodes,
                             std::map<std::string, PeerCountPolicy> policies,
                             std::chrono::milliseconds interval,
                             NodeAvailableHandler node_available_handler,
                             ActionHandler action_handler,
                             FailureHandler failure_handler);
  PeerConnectivityController(const PeerConnectivityController&) = delete;
  PeerConnectivityController& operator=(const PeerConnectivityController&) =
      delete;
  ~PeerConnectivityController();

  void Start();
  void Stop();
  void SetPolicy(std::string_view node_id, PeerCountPolicy policy);
  void ConnectPeer(std::string_view node_id, std::string_view peer_node_id,
                   std::chrono::seconds timeout,
                   std::stop_token stop_token = {});
  void DisconnectPeer(std::string_view node_id, std::string_view peer_node_id,
                      std::chrono::seconds timeout,
                      std::stop_token stop_token = {});

 private:
  const ChainNodeConfig& FindNode(std::string_view node_id) const;
  void RequireUnambiguousPeerIdentity(const ChainNodeConfig& node,
                                      std::stop_token stop_token) const;
  void ValidatePolicy(std::string_view node_id,
                      const PeerCountPolicy& policy) const;
  void Run(std::stop_token stop_token);
  void EnforcePolicies(std::stop_token stop_token);
  void EnforcePolicy(const ChainNodeConfig& node, const PeerCountPolicy& policy,
                     std::stop_token stop_token);
  void ReportFailure(std::string_view node_id, std::string_view error);

  const ChainDriver& driver_;
  std::vector<ChainNodeConfig> nodes_;
  std::map<std::string, PeerCountPolicy> policies_;
  std::chrono::milliseconds interval_;
  NodeAvailableHandler node_available_handler_;
  ActionHandler action_handler_;
  FailureHandler failure_handler_;
  std::map<std::string, std::string> last_failures_;
  std::mutex operation_mutex_;
  std::jthread thread_;
  bool started_ = false;
};

}  // namespace bbp
