#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/peer_count_policy.h"

namespace bbp {

enum class PeerConnectivityAction {
  kConnected,
  kDisconnected,
  kTopologyRestored,
};

class PeerMutationOutcomeUnconfirmed : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class PeerConnectivityController {
 private:
  struct PreparedFinalState;

 public:
  using AllowedPeerMap = std::map<std::string, std::vector<std::string>>;
  using OptionalPolicyMap =
      std::map<std::string, std::optional<PeerCountPolicy>>;
  using NodeAvailableHandler = std::function<bool(std::string_view node_id)>;
  using ActionHandler = std::function<void(
      std::string_view node_id, std::string_view peer_node_id,
      PeerConnectivityAction action, const PeerCountPolicy& policy)>;
  using FailureHandler =
      std::function<void(std::string_view node_id, std::string_view error)>;

  class RpcMutationLease {
   public:
    RpcMutationLease(RpcMutationLease&&) noexcept = default;
    RpcMutationLease& operator=(RpcMutationLease&&) noexcept = default;
    RpcMutationLease(const RpcMutationLease&) = delete;
    RpcMutationLease& operator=(const RpcMutationLease&) = delete;

   private:
    friend class PeerConnectivityController;
    explicit RpcMutationLease(std::unique_lock<std::mutex> lock)
        : lock_(std::move(lock)) {}

    std::unique_lock<std::mutex> lock_;
  };

  class PreparedFinalRegistration {
   public:
    PreparedFinalRegistration(PreparedFinalRegistration&&) noexcept;
    PreparedFinalRegistration& operator=(PreparedFinalRegistration&&) noexcept;
    PreparedFinalRegistration(const PreparedFinalRegistration&) = delete;
    PreparedFinalRegistration& operator=(const PreparedFinalRegistration&) =
        delete;
    ~PreparedFinalRegistration();

    void Commit() noexcept;

   private:
    friend class PeerConnectivityController;
    PreparedFinalRegistration(PeerConnectivityController* owner,
                              std::unique_lock<std::mutex> operation_lock,
                              std::unique_lock<std::mutex> restoration_lock,
                              std::unique_ptr<PreparedFinalState> state);

    PeerConnectivityController* owner_ = nullptr;
    std::unique_lock<std::mutex> operation_lock_;
    std::unique_lock<std::mutex> restoration_lock_;
    std::unique_ptr<PreparedFinalState> state_;
    bool committed_ = false;
  };

  PeerConnectivityController(
      const ChainDriver& driver, std::vector<ChainNodeConfig> nodes,
      std::map<std::string, PeerCountPolicy> policies,
      AllowedPeerMap allowed_peers, std::chrono::milliseconds interval,
      NodeAvailableHandler node_available_handler, ActionHandler action_handler,
      FailureHandler failure_handler,
      std::set<std::string> all_peer_policy_node_ids = {});
  PeerConnectivityController(const PeerConnectivityController&) = delete;
  PeerConnectivityController& operator=(const PeerConnectivityController&) =
      delete;
  ~PeerConnectivityController();

  void Start();
  void Stop();
  void RegisterNode(ChainNodeConfig node, std::optional<PeerCountPolicy> policy,
                    std::vector<std::string> allowed_peer_node_ids);
  void RegisterNodes(const std::vector<ChainNodeConfig>& nodes,
                     const OptionalPolicyMap& policies,
                     const AllowedPeerMap& allowed_peers);
  RpcMutationLease AcquireRpcMutationLease(std::stop_token stop_token = {});
  PreparedFinalRegistration PrepareFinalRegistration(
      const std::vector<ChainNodeConfig>& final_nodes,
      const OptionalPolicyMap& new_policies,
      const AllowedPeerMap& final_allowed_peers,
      const std::set<std::string>& new_all_peer_policy_node_ids,
      const RpcMutationLease& lease);
  void UnregisterNode(std::string_view node_id,
                      std::stop_token stop_token = {});
  void SetPolicy(std::string_view node_id, PeerCountPolicy policy);
  void RequestTopologyRestore(std::string_view changed_node_id);
  void SetAllowedPeers(std::string_view node_id,
                       std::vector<std::string> peer_node_ids);
  void ValidateAllowedPeerUpdate(std::string_view node_id,
                                 const std::vector<std::string>& peer_node_ids);
  std::vector<std::string> AllowedPeersFor(std::string_view node_id);
  void ConnectPeer(std::string_view node_id, std::string_view peer_node_id,
                   std::chrono::seconds timeout,
                   std::stop_token stop_token = {});
  void DisconnectPeer(std::string_view node_id, std::string_view peer_node_id,
                      std::chrono::seconds timeout,
                      std::stop_token stop_token = {});

 private:
  const ChainNodeConfig& FindNodeUnlocked(std::string_view node_id) const;
  const std::vector<std::string>& AllowedPeersUnlocked(
      std::string_view node_id) const;
  void ValidateAllowedPeersUnlocked(
      std::string_view node_id,
      const std::vector<std::string>& peer_node_ids) const;
  void ValidateAllowedPeerUpdateUnlocked(
      std::string_view node_id,
      const std::vector<std::string>& peer_node_ids) const;
  void RequireUnambiguousPeerIdentity(const std::vector<ChainNodeConfig>& nodes,
                                      const ChainNodeConfig& node,
                                      std::stop_token stop_token) const;
  void SetPeerConnectionState(const ChainNodeConfig& node,
                              const std::string& endpoint, bool connected,
                              std::chrono::seconds timeout,
                              std::stop_token stop_token) const;
  std::unique_lock<std::mutex> LockRpc(std::stop_token stop_token);
  std::uint64_t NextTopologyRestoreSequence();
  void Run(std::stop_token stop_token);
  void EnforcePolicies(std::stop_token stop_token);
  bool EnforcePolicy(const ChainNodeConfig& node,
                     const std::vector<ChainNodeConfig>& nodes,
                     const std::vector<std::string>& allowed_peer_ids,
                     const AllowedPeerMap& all_allowed_peers,
                     const PeerCountPolicy* policy,
                     std::optional<std::uint64_t> restore_request_sequence,
                     std::string_view changed_node_id,
                     std::uint64_t expected_configuration_sequence,
                     std::stop_token stop_token);
  void ReportFailure(
      std::string_view node_id, std::string_view error,
      std::optional<std::uint64_t> expected_configuration_sequence);
  void ReportRestorationFailure(
      std::string_view node_id, std::string_view error,
      std::optional<std::uint64_t> expected_configuration_sequence);

  const ChainDriver& driver_;
  std::vector<ChainNodeConfig> nodes_;
  std::map<std::string, PeerCountPolicy> policies_;
  std::set<std::string> all_peer_policy_node_ids_;
  AllowedPeerMap allowed_peers_;
  std::chrono::milliseconds interval_;
  NodeAvailableHandler node_available_handler_;
  ActionHandler action_handler_;
  FailureHandler failure_handler_;
  std::map<std::string, std::string> last_failures_;
  std::map<std::string, std::string> last_restoration_failures_;
  mutable std::mutex operation_mutex_;
  std::mutex rpc_mutex_;
  std::mutex restoration_mutex_;
  std::atomic<std::uint64_t> configuration_sequence_ = 0U;
  std::atomic<std::uint64_t> topology_restore_sequence_ = 0U;
  std::map<std::pair<std::string, std::string>, std::uint64_t>
      topology_restore_suppressions_;
  std::map<std::string, std::uint64_t> topology_restore_requests_;
  std::map<std::string, std::uint64_t> topology_restore_completions_;
  std::jthread thread_;
  bool started_ = false;
};

}  // namespace bbp
