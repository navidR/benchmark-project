#include "bbp/peer_connectivity_controller.h"

#include <algorithm>
#include <condition_variable>
#include <set>
#include <stdexcept>
#include <utility>

#include "bbp/simulation_cancelled.h"

namespace bbp {
namespace {

std::string PeerEndpoint(const ChainNodeConfig& config) {
  return config.p2p_host + ":" + std::to_string(config.p2p_port);
}

}  // namespace

PeerConnectivityController::PeerConnectivityController(
    const ChainDriver& driver, std::vector<ChainNodeConfig> nodes,
    std::map<std::string, PeerCountPolicy> policies,
    AllowedPeerMap allowed_peers, std::chrono::milliseconds interval,
    NodeAvailableHandler node_available_handler, ActionHandler action_handler,
    FailureHandler failure_handler)
    : driver_(driver),
      nodes_(std::move(nodes)),
      policies_(std::move(policies)),
      allowed_peers_(std::move(allowed_peers)),
      interval_(interval),
      node_available_handler_(std::move(node_available_handler)),
      action_handler_(std::move(action_handler)),
      failure_handler_(std::move(failure_handler)) {
  if (nodes_.empty()) {
    throw std::runtime_error(
        "peer connectivity controller requires at least one node");
  }
  if (interval_.count() <= 0) {
    throw std::runtime_error(
        "peer connectivity controller interval must be positive");
  }
  if (!node_available_handler_ || !action_handler_ || !failure_handler_) {
    throw std::runtime_error(
        "peer connectivity controller requires all handlers");
  }
  if (allowed_peers_.size() != nodes_.size()) {
    throw std::runtime_error(
        "peer connectivity controller requires allowed peers for every node");
  }
  for (const ChainNodeConfig& node : nodes_) {
    const auto allowed = allowed_peers_.find(node.id);
    if (allowed == allowed_peers_.end()) {
      throw std::runtime_error(
          "peer connectivity controller is missing allowed peers for " +
          node.id);
    }
    ValidateAllowedPeers(node.id, allowed->second);
  }
  for (const auto& [node_id, policy] : policies_) {
    ValidatePolicy(node_id, policy);
  }
}

PeerConnectivityController::~PeerConnectivityController() { Stop(); }

void PeerConnectivityController::Start() {
  if (started_) {
    throw std::runtime_error("peer connectivity controller is already started");
  }
  started_ = true;
  thread_ =
      std::jthread([this](std::stop_token stop_token) { Run(stop_token); });
}

void PeerConnectivityController::Stop() {
  if (!started_) {
    return;
  }
  thread_.request_stop();
  if (thread_.joinable()) {
    thread_.join();
  }
  started_ = false;
}

void PeerConnectivityController::SetPolicy(std::string_view node_id,
                                           PeerCountPolicy policy) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  ValidatePolicy(node_id, policy);
  policies_.insert_or_assign(std::string(node_id), policy);
  last_failures_.erase(std::string(node_id));
}

void PeerConnectivityController::SetAllowedPeers(
    std::string_view node_id, std::vector<std::string> peer_node_ids) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  ValidateAllowedPeers(node_id, peer_node_ids);
  const auto policy = policies_.find(std::string(node_id));
  if (policy != policies_.end() &&
      policy->second.minimum() > peer_node_ids.size()) {
    throw std::runtime_error(
        "peer policy minimum exceeds allowed logical peers for " +
        std::string(node_id));
  }
  allowed_peers_.insert_or_assign(std::string(node_id),
                                  std::move(peer_node_ids));
  last_failures_.erase(std::string(node_id));
}

void PeerConnectivityController::ConnectPeer(std::string_view node_id,
                                             std::string_view peer_node_id,
                                             std::chrono::seconds timeout,
                                             std::stop_token stop_token) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  const ChainNodeConfig& node = FindNode(node_id);
  const ChainNodeConfig& peer = FindNode(peer_node_id);
  if (node.id == peer.id) {
    throw std::runtime_error("peer source and target must differ");
  }
  const std::vector<std::string>& allowed = AllowedPeers(node.id);
  if (std::find(allowed.begin(), allowed.end(), peer.id) == allowed.end()) {
    throw std::runtime_error("peer target is not an active logical edge from " +
                             node.id + ": " + peer.id);
  }
  if (!node_available_handler_(node.id) || !node_available_handler_(peer.id)) {
    throw std::runtime_error("peer source and target must both be running");
  }
  RequireUnambiguousPeerIdentity(node, stop_token);
  const std::string endpoint = PeerEndpoint(peer);
  driver_.ConnectPeer(node, endpoint, stop_token);
  driver_.WaitForPeerAddress(node, endpoint, timeout, stop_token);
}

void PeerConnectivityController::DisconnectPeer(std::string_view node_id,
                                                std::string_view peer_node_id,
                                                std::chrono::seconds timeout,
                                                std::stop_token stop_token) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  const ChainNodeConfig& node = FindNode(node_id);
  const ChainNodeConfig& peer = FindNode(peer_node_id);
  if (node.id == peer.id) {
    throw std::runtime_error("peer source and target must differ");
  }
  if (!node_available_handler_(node.id)) {
    throw std::runtime_error("peer source node is not running");
  }
  RequireUnambiguousPeerIdentity(node, stop_token);
  const std::string endpoint = PeerEndpoint(peer);
  driver_.DisconnectPeer(node, endpoint, stop_token);
  driver_.WaitForPeerAddressAbsent(node, endpoint, timeout, stop_token);
}

const ChainNodeConfig& PeerConnectivityController::FindNode(
    std::string_view node_id) const {
  const auto node =
      std::find_if(nodes_.begin(), nodes_.end(),
                   [node_id](const auto& item) { return item.id == node_id; });
  if (node == nodes_.end()) {
    throw std::runtime_error("unknown simulation node: " +
                             std::string(node_id));
  }
  return *node;
}

const std::vector<std::string>& PeerConnectivityController::AllowedPeers(
    std::string_view node_id) const {
  const auto allowed = allowed_peers_.find(std::string(node_id));
  if (allowed == allowed_peers_.end()) {
    throw std::runtime_error("missing allowed peer set for simulation node: " +
                             std::string(node_id));
  }
  return allowed->second;
}

void PeerConnectivityController::ValidateAllowedPeers(
    std::string_view node_id,
    const std::vector<std::string>& peer_node_ids) const {
  const ChainNodeConfig& node = FindNode(node_id);
  std::set<std::string> unique;
  for (const std::string& peer_node_id : peer_node_ids) {
    const ChainNodeConfig& peer = FindNode(peer_node_id);
    if (peer.id == node.id) {
      throw std::runtime_error("allowed peer set must not contain its node");
    }
    if (!unique.insert(peer.id).second) {
      throw std::runtime_error("allowed peer set contains a duplicate peer: " +
                               peer.id);
    }
  }
}

void PeerConnectivityController::RequireUnambiguousPeerIdentity(
    const ChainNodeConfig& node, std::stop_token stop_token) const {
  std::vector<std::string> candidate_endpoints;
  candidate_endpoints.reserve(nodes_.size() - 1U);
  for (const ChainNodeConfig& candidate : nodes_) {
    if (candidate.id != node.id) {
      candidate_endpoints.push_back(PeerEndpoint(candidate));
    }
  }
  static_cast<void>(
      driver_.ConnectedPeerAddresses(node, candidate_endpoints, stop_token));
}

void PeerConnectivityController::ValidatePolicy(
    std::string_view node_id, const PeerCountPolicy& policy) const {
  static_cast<void>(FindNode(node_id));
  const std::size_t maximum_possible = nodes_.size() - 1U;
  if (policy.maximum() > maximum_possible) {
    throw std::runtime_error("maximum peer count for " + std::string(node_id) +
                             " exceeds available simulation peers");
  }
  if (policy.minimum() > AllowedPeers(node_id).size()) {
    throw std::runtime_error("minimum peer count for " + std::string(node_id) +
                             " exceeds allowed logical peers");
  }
}

void PeerConnectivityController::Run(std::stop_token stop_token) {
  std::condition_variable_any wakeup;
  std::mutex wakeup_mutex;
  while (!stop_token.stop_requested()) {
    EnforcePolicies(stop_token);
    std::unique_lock<std::mutex> lock(wakeup_mutex);
    wakeup.wait_for(lock, stop_token, interval_, [] { return false; });
  }
}

void PeerConnectivityController::EnforcePolicies(std::stop_token stop_token) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  for (const auto& [node_id, policy] : policies_) {
    if (stop_token.stop_requested()) {
      return;
    }
    if (!node_available_handler_(node_id)) {
      continue;
    }
    try {
      EnforcePolicy(FindNode(node_id), policy, stop_token);
      last_failures_.erase(node_id);
    } catch (const SimulationCancelled&) {
      if (stop_token.stop_requested()) {
        return;
      }
      throw;
    } catch (const std::exception& error) {
      ReportFailure(node_id, error.what());
    }
  }
}

void PeerConnectivityController::EnforcePolicy(const ChainNodeConfig& node,
                                               const PeerCountPolicy& policy,
                                               std::stop_token stop_token) {
  std::vector<const ChainNodeConfig*> candidates;
  std::vector<std::string> candidate_endpoints;
  for (const std::string& peer_node_id : AllowedPeers(node.id)) {
    const ChainNodeConfig& candidate = FindNode(peer_node_id);
    if (!node_available_handler_(candidate.id)) {
      continue;
    }
    candidates.push_back(&candidate);
    candidate_endpoints.push_back(PeerEndpoint(candidate));
  }
  if (candidates.size() < policy.minimum()) {
    throw std::runtime_error(
        "peer policy minimum cannot be met with running "
        "simulation peers");
  }

  const std::vector<std::string> connected_addresses =
      driver_.ConnectedPeerAddresses(node, candidate_endpoints, stop_token);
  const std::set<std::string> connected(connected_addresses.begin(),
                                        connected_addresses.end());
  std::vector<std::size_t> connected_indexes;
  std::vector<std::size_t> disconnected_indexes;
  for (std::size_t index = 0; index < candidate_endpoints.size(); ++index) {
    if (connected.contains(candidate_endpoints[index])) {
      connected_indexes.push_back(index);
    } else {
      disconnected_indexes.push_back(index);
    }
  }

  while (connected_indexes.size() < policy.minimum() &&
         !disconnected_indexes.empty()) {
    const std::size_t index = disconnected_indexes.front();
    disconnected_indexes.erase(disconnected_indexes.begin());
    driver_.ConnectPeer(node, candidate_endpoints[index], stop_token);
    driver_.WaitForPeerAddress(node, candidate_endpoints[index],
                               std::chrono::seconds(10), stop_token);
    connected_indexes.push_back(index);
    action_handler_(node.id, candidates[index]->id,
                    PeerConnectivityAction::kConnected, policy);
  }

  while (connected_indexes.size() > policy.maximum()) {
    const std::size_t index = connected_indexes.back();
    connected_indexes.pop_back();
    driver_.DisconnectPeer(node, candidate_endpoints[index], stop_token);
    driver_.WaitForPeerAddressAbsent(node, candidate_endpoints[index],
                                     std::chrono::seconds(10), stop_token);
    action_handler_(node.id, candidates[index]->id,
                    PeerConnectivityAction::kDisconnected, policy);
  }
}

void PeerConnectivityController::ReportFailure(std::string_view node_id,
                                               std::string_view error) {
  const std::string key(node_id);
  const std::string detail(error);
  const auto previous = last_failures_.find(key);
  if (previous != last_failures_.end() && previous->second == detail) {
    return;
  }
  last_failures_.insert_or_assign(key, detail);
  failure_handler_(node_id, error);
}

}  // namespace bbp
