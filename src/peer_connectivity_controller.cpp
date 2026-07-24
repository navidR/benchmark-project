#include "bbp/peer_connectivity_controller.h"

#include <algorithm>
#include <condition_variable>
#include <exception>
#include <iterator>
#include <limits>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

#include "bbp/simulation_cancelled.h"

namespace bbp {
namespace {

constexpr auto kCancelledMutationRollbackBound = std::chrono::milliseconds(200);

std::string PeerEndpoint(const ChainNodeConfig& config) {
  return config.p2p_host + ":" + std::to_string(config.p2p_port);
}

const ChainNodeConfig& FindNodeConfig(const std::vector<ChainNodeConfig>& nodes,
                                      std::string_view node_id) {
  const auto node = std::find_if(nodes.begin(), nodes.end(),
                                 [node_id](const ChainNodeConfig& candidate) {
                                   return candidate.id == node_id;
                                 });
  if (node == nodes.end()) {
    throw std::runtime_error("unknown simulation node: " +
                             std::string(node_id));
  }
  return *node;
}

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

[[noreturn]] void RethrowPeerMutationFailure(
    const std::exception_ptr& original_error, std::string_view operation,
    const std::exception_ptr& rollback_error) {
  const std::string message =
      ExceptionMessage(original_error) + "; peer " + std::string(operation) +
      " rollback failed: " + ExceptionMessage(rollback_error);
  throw PeerMutationOutcomeUnconfirmed(message);
}

}  // namespace

struct PeerConnectivityController::PreparedFinalState {
  std::vector<ChainNodeConfig> nodes;
  std::map<std::string, PeerCountPolicy> policies;
  std::set<std::string> all_peer_policy_node_ids;
  AllowedPeerMap allowed_peers;
  std::map<std::string, std::string> last_failures;
  std::map<std::string, std::string> last_restoration_failures;
  std::map<std::pair<std::string, std::string>, std::uint64_t>
      topology_restore_suppressions;
  std::map<std::string, std::uint64_t> topology_restore_requests;
  std::map<std::string, std::uint64_t> topology_restore_completions;
};

PeerConnectivityController::PreparedFinalRegistration::
    PreparedFinalRegistration(PeerConnectivityController* owner,
                              std::unique_lock<std::mutex> operation_lock,
                              std::unique_lock<std::mutex> restoration_lock,
                              std::unique_ptr<PreparedFinalState> state)
    : owner_(owner),
      operation_lock_(std::move(operation_lock)),
      restoration_lock_(std::move(restoration_lock)),
      state_(std::move(state)) {}

PeerConnectivityController::PreparedFinalRegistration::
    PreparedFinalRegistration(PreparedFinalRegistration&&) noexcept = default;

PeerConnectivityController::PreparedFinalRegistration&
PeerConnectivityController::PreparedFinalRegistration::operator=(
    PreparedFinalRegistration&&) noexcept = default;

PeerConnectivityController::PreparedFinalRegistration::
    ~PreparedFinalRegistration() = default;

PeerConnectivityController::PeerConnectivityController(
    const ChainDriver& driver, std::vector<ChainNodeConfig> nodes,
    std::map<std::string, PeerCountPolicy> policies,
    AllowedPeerMap allowed_peers, std::chrono::milliseconds interval,
    NodeAvailableHandler node_available_handler, ActionHandler action_handler,
    FailureHandler failure_handler,
    std::set<std::string> all_peer_policy_node_ids)
    : driver_(driver),
      nodes_(std::move(nodes)),
      policies_(std::move(policies)),
      all_peer_policy_node_ids_(std::move(all_peer_policy_node_ids)),
      allowed_peers_(std::move(allowed_peers)),
      interval_(interval),
      node_available_handler_(std::move(node_available_handler)),
      action_handler_(std::move(action_handler)),
      failure_handler_(std::move(failure_handler)) {
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
    ValidateAllowedPeersUnlocked(node.id, allowed->second);
    topology_restore_requests_.emplace(node.id, 0U);
    topology_restore_completions_.emplace(node.id, 0U);
  }
  for (const std::string& node_id : all_peer_policy_node_ids_) {
    const auto policy = policies_.find(node_id);
    if (policy == policies_.end()) {
      throw std::runtime_error("all-peers mode requires a peer policy for " +
                               node_id);
    }
    const std::size_t count = AllowedPeersUnlocked(node_id).size();
    if (count > std::numeric_limits<std::uint32_t>::max()) {
      throw std::overflow_error("all-peers policy count exceeds uint32");
    }
    policy->second = PeerCountPolicy(static_cast<std::uint32_t>(count),
                                     static_cast<std::uint32_t>(count));
  }
  for (const auto& [node_id, policy] : policies_) {
    const std::size_t maximum_possible = nodes_.size() - 1U;
    if (policy.maximum() > maximum_possible) {
      throw std::runtime_error("maximum peer count for " + node_id +
                               " exceeds available simulation peers");
    }
    if (policy.minimum() > AllowedPeersUnlocked(node_id).size()) {
      throw std::runtime_error("minimum peer count for " + node_id +
                               " exceeds allowed logical peers");
    }
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

void PeerConnectivityController::RegisterNode(
    ChainNodeConfig node, std::optional<PeerCountPolicy> policy,
    std::vector<std::string> allowed_peer_node_ids) {
  const std::string node_id = node.id;
  std::vector<ChainNodeConfig> nodes;
  nodes.push_back(std::move(node));
  OptionalPolicyMap policies;
  policies.emplace(node_id, std::move(policy));
  AllowedPeerMap allowed_peers;
  allowed_peers.emplace(node_id, std::move(allowed_peer_node_ids));
  RegisterNodes(nodes, policies, allowed_peers);
}

void PeerConnectivityController::RegisterNodes(
    const std::vector<ChainNodeConfig>& nodes,
    const OptionalPolicyMap& policies, const AllowedPeerMap& allowed_peers) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  if (nodes.empty()) {
    throw std::runtime_error(
        "peer connectivity controller registration batch must not be empty");
  }

  std::set<std::string> batch_ids;
  for (const ChainNodeConfig& node : nodes) {
    if (node.id.empty()) {
      throw std::runtime_error(
          "peer connectivity controller node id must not be empty");
    }
    if (!batch_ids.insert(node.id).second) {
      throw std::runtime_error(
          "peer connectivity controller registration batch contains a "
          "duplicate node id: " +
          node.id);
    }
    if (std::any_of(nodes_.begin(), nodes_.end(),
                    [&](const ChainNodeConfig& candidate) {
                      return candidate.id == node.id;
                    })) {
      throw std::runtime_error("simulation node is already registered: " +
                               node.id);
    }
  }
  if (policies.size() != nodes.size()) {
    throw std::runtime_error(
        "peer connectivity controller registration batch requires one "
        "optional policy entry for every node");
  }
  if (allowed_peers.size() < nodes.size()) {
    throw std::runtime_error(
        "peer connectivity controller registration batch requires allowed "
        "peers for every new node");
  }
  for (const std::string& node_id : batch_ids) {
    if (!policies.contains(node_id)) {
      throw std::runtime_error(
          "peer connectivity controller registration batch is missing a "
          "policy entry for " +
          node_id);
    }
    if (!allowed_peers.contains(node_id)) {
      throw std::runtime_error(
          "peer connectivity controller registration batch is missing "
          "allowed peers for " +
          node_id);
    }
  }
  for (const auto& [node_id, policy] : policies) {
    static_cast<void>(policy);
    if (!batch_ids.contains(node_id)) {
      throw std::runtime_error(
          "peer connectivity controller registration batch contains an "
          "extra policy entry for " +
          node_id);
    }
  }
  std::vector<ChainNodeConfig> next_nodes = nodes_;
  next_nodes.reserve(next_nodes.size() + nodes.size());
  next_nodes.insert(next_nodes.end(), nodes.begin(), nodes.end());
  for (const auto& [node_id, peers] : allowed_peers) {
    static_cast<void>(peers);
    if (std::none_of(
            next_nodes.begin(), next_nodes.end(),
            [&](const ChainNodeConfig& node) { return node.id == node_id; })) {
      throw std::runtime_error(
          "peer connectivity controller registration batch contains extra "
          "allowed peers for " +
          node_id);
    }
  }

  const std::size_t maximum_possible = next_nodes.size() - 1U;
  for (const auto& [node_id, peers] : allowed_peers) {
    std::set<std::string> unique_peers;
    for (const std::string& peer_node_id : peers) {
      const ChainNodeConfig& peer = FindNodeConfig(next_nodes, peer_node_id);
      if (peer.id == node_id) {
        throw std::runtime_error("allowed peer set must not contain its node");
      }
      if (!unique_peers.insert(peer.id).second) {
        throw std::runtime_error(
            "allowed peer set contains a duplicate peer: " + peer.id);
      }
    }
    const auto new_policy = policies.find(node_id);
    const auto existing_policy = policies_.find(node_id);
    const std::optional<PeerCountPolicy> policy =
        new_policy != policies.end()
            ? new_policy->second
            : (existing_policy != policies_.end()
                   ? std::optional<PeerCountPolicy>(existing_policy->second)
                   : std::nullopt);
    if (policy && (policy->maximum() > maximum_possible ||
                   policy->minimum() > peers.size())) {
      throw std::runtime_error(
          "peer policy is incompatible with the registered node set");
    }
  }

  std::map<std::string, PeerCountPolicy> next_policies = policies_;
  AllowedPeerMap next_allowed_peers = allowed_peers_;
  for (const auto& [node_id, peers] : allowed_peers) {
    next_allowed_peers.insert_or_assign(node_id, peers);
  }
  for (const ChainNodeConfig& node : nodes) {
    const std::optional<PeerCountPolicy>& policy = policies.at(node.id);
    if (policy && !next_policies.emplace(node.id, *policy).second) {
      throw std::logic_error(
          "peer connectivity policy registry is internally inconsistent");
    }
  }

  std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
  auto next_topology_restore_requests = topology_restore_requests_;
  auto next_topology_restore_completions = topology_restore_completions_;
  for (const ChainNodeConfig& node : nodes) {
    if (!next_topology_restore_requests.emplace(node.id, 0U).second ||
        !next_topology_restore_completions.emplace(node.id, 0U).second) {
      throw std::logic_error(
          "peer connectivity restoration registry is internally "
          "inconsistent");
    }
  }

  nodes_.swap(next_nodes);
  policies_.swap(next_policies);
  allowed_peers_.swap(next_allowed_peers);
  topology_restore_requests_.swap(next_topology_restore_requests);
  topology_restore_completions_.swap(next_topology_restore_completions);
  configuration_sequence_.fetch_add(1U, std::memory_order_release);
}

PeerConnectivityController::RpcMutationLease
PeerConnectivityController::AcquireRpcMutationLease(
    std::stop_token stop_token) {
  return RpcMutationLease(LockRpc(stop_token));
}

PeerConnectivityController::PreparedFinalRegistration
PeerConnectivityController::PrepareFinalRegistration(
    const std::vector<ChainNodeConfig>& final_nodes,
    const OptionalPolicyMap& new_policies,
    const AllowedPeerMap& final_allowed_peers,
    const std::set<std::string>& new_all_peer_policy_node_ids,
    const RpcMutationLease& lease) {
  if (!lease.lock_.owns_lock() || lease.lock_.mutex() != &rpc_mutex_) {
    throw std::logic_error(
        "peer final registration requires its controller RPC lease");
  }
  std::unique_lock<std::mutex> operation_lock(operation_mutex_);
  std::unique_lock<std::mutex> restoration_lock(restoration_mutex_);
  if (final_nodes.size() < nodes_.size()) {
    throw std::runtime_error(
        "peer final registration must retain every existing node");
  }

  std::set<std::string> final_ids;
  for (const ChainNodeConfig& node : final_nodes) {
    if (node.id.empty() || !final_ids.insert(node.id).second) {
      throw std::runtime_error(
          "peer final registration has an empty or duplicate node id");
    }
  }
  std::set<std::string> existing_ids;
  for (const ChainNodeConfig& node : nodes_) {
    existing_ids.insert(node.id);
    if (!final_ids.contains(node.id)) {
      throw std::runtime_error(
          "peer final registration omitted existing node: " + node.id);
    }
  }
  std::set<std::string> new_ids;
  std::set_difference(final_ids.begin(), final_ids.end(), existing_ids.begin(),
                      existing_ids.end(),
                      std::inserter(new_ids, new_ids.end()));
  if (new_policies.size() != new_ids.size()) {
    throw std::runtime_error(
        "peer final registration requires one optional policy for every "
        "new node");
  }
  for (const auto& [node_id, policy] : new_policies) {
    static_cast<void>(policy);
    if (!new_ids.contains(node_id)) {
      throw std::runtime_error(
          "peer final registration contains a policy for a non-new node: " +
          node_id);
    }
  }
  for (const std::string& node_id : new_all_peer_policy_node_ids) {
    if (!new_ids.contains(node_id)) {
      throw std::runtime_error(
          "peer final registration all-peers mode is limited to new nodes");
    }
    const auto policy = new_policies.find(node_id);
    if (policy == new_policies.end() || !policy->second) {
      throw std::runtime_error(
          "peer final registration all-peers node requires a policy");
    }
  }
  if (final_allowed_peers.size() != final_nodes.size()) {
    throw std::runtime_error(
        "peer final registration requires an exact allowed-peer map");
  }
  for (const ChainNodeConfig& node : final_nodes) {
    const auto allowed = final_allowed_peers.find(node.id);
    if (allowed == final_allowed_peers.end()) {
      throw std::runtime_error(
          "peer final registration is missing allowed peers for " + node.id);
    }
    std::set<std::string> unique;
    for (const std::string& peer_id : allowed->second) {
      if (!final_ids.contains(peer_id)) {
        throw std::runtime_error(
            "peer final registration references unknown peer: " + peer_id);
      }
      if (peer_id == node.id || !unique.insert(peer_id).second) {
        throw std::runtime_error(
            "peer final registration contains a self or duplicate peer");
      }
    }
  }

  auto state = std::make_unique<PreparedFinalState>();
  state->nodes = final_nodes;
  state->policies = policies_;
  state->all_peer_policy_node_ids = all_peer_policy_node_ids_;
  state->allowed_peers = final_allowed_peers;
  state->last_failures = last_failures_;
  state->last_restoration_failures = last_restoration_failures_;
  state->topology_restore_suppressions = topology_restore_suppressions_;
  state->topology_restore_requests = topology_restore_requests_;
  state->topology_restore_completions = topology_restore_completions_;
  for (const std::string& node_id : new_ids) {
    const std::optional<PeerCountPolicy>& policy = new_policies.at(node_id);
    if (policy) {
      state->policies.emplace(node_id, *policy);
    }
    state->topology_restore_requests.emplace(node_id, 0U);
    state->topology_restore_completions.emplace(node_id, 0U);
  }
  state->all_peer_policy_node_ids.insert(new_all_peer_policy_node_ids.begin(),
                                         new_all_peer_policy_node_ids.end());
  for (const std::string& node_id : state->all_peer_policy_node_ids) {
    const auto policy = state->policies.find(node_id);
    if (policy == state->policies.end()) {
      throw std::logic_error("peer all-peers registry lost its typed policy");
    }
    const std::size_t count = state->allowed_peers.at(node_id).size();
    if (count > std::numeric_limits<std::uint32_t>::max()) {
      throw std::overflow_error("all-peers policy count exceeds uint32");
    }
    policy->second = PeerCountPolicy(static_cast<std::uint32_t>(count),
                                     static_cast<std::uint32_t>(count));
  }
  const std::size_t maximum_possible =
      final_nodes.empty() ? 0U : final_nodes.size() - 1U;
  for (const auto& [node_id, policy] : state->policies) {
    if (!final_ids.contains(node_id) || policy.maximum() > maximum_possible ||
        policy.minimum() > state->allowed_peers.at(node_id).size()) {
      throw std::runtime_error(
          "peer policy is incompatible with the final node set: " + node_id);
    }
  }
  std::erase_if(state->topology_restore_suppressions, [&](const auto& item) {
    const auto allowed = state->allowed_peers.find(item.first.first);
    return allowed == state->allowed_peers.end() ||
           std::find(allowed->second.begin(), allowed->second.end(),
                     item.first.second) == allowed->second.end();
  });
  for (const ChainNodeConfig& node : final_nodes) {
    state->last_failures.erase(node.id);
    state->last_restoration_failures.erase(node.id);
  }
  return PreparedFinalRegistration(this, std::move(operation_lock),
                                   std::move(restoration_lock),
                                   std::move(state));
}

void PeerConnectivityController::PreparedFinalRegistration::Commit() noexcept {
  if (owner_ == nullptr || !state_ || !operation_lock_.owns_lock() ||
      operation_lock_.mutex() != &owner_->operation_mutex_ ||
      !restoration_lock_.owns_lock() ||
      restoration_lock_.mutex() != &owner_->restoration_mutex_ || committed_) {
    std::terminate();
  }
  owner_->nodes_.swap(state_->nodes);
  owner_->policies_.swap(state_->policies);
  owner_->all_peer_policy_node_ids_.swap(state_->all_peer_policy_node_ids);
  owner_->allowed_peers_.swap(state_->allowed_peers);
  owner_->last_failures_.swap(state_->last_failures);
  owner_->last_restoration_failures_.swap(state_->last_restoration_failures);
  owner_->topology_restore_suppressions_.swap(
      state_->topology_restore_suppressions);
  owner_->topology_restore_requests_.swap(state_->topology_restore_requests);
  owner_->topology_restore_completions_.swap(
      state_->topology_restore_completions);
  owner_->configuration_sequence_.fetch_add(1U, std::memory_order_release);
  committed_ = true;
}

void PeerConnectivityController::UnregisterNode(std::string_view node_id,
                                                std::stop_token stop_token) {
  auto rpc_lock = LockRpc(stop_token);
  std::lock_guard<std::mutex> lock(operation_mutex_);
  const std::string key(node_id);
  const auto node = std::find_if(nodes_.begin(), nodes_.end(),
                                 [&](const ChainNodeConfig& candidate) {
                                   return candidate.id == node_id;
                                 });
  if (node == nodes_.end()) {
    throw std::runtime_error("unknown simulation node: " + key);
  }
  for (const auto& [source_id, allowed] : allowed_peers_) {
    if (source_id != key &&
        std::find(allowed.begin(), allowed.end(), key) != allowed.end()) {
      throw std::runtime_error(
          "cannot unregister a node retained by an allowed peer set: " + key);
    }
  }
  const std::size_t remaining_peer_capacity =
      nodes_.size() > 1U ? nodes_.size() - 2U : 0U;
  for (const auto& [source_id, policy] : policies_) {
    if (source_id != key && policy.maximum() > remaining_peer_capacity) {
      throw std::runtime_error(
          "cannot unregister a node while a peer policy exceeds the "
          "remaining node set: " +
          source_id);
    }
  }
  nodes_.erase(node);
  policies_.erase(key);
  all_peer_policy_node_ids_.erase(key);
  allowed_peers_.erase(key);
  last_failures_.erase(key);
  last_restoration_failures_.erase(key);
  {
    std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
    topology_restore_requests_.erase(key);
    topology_restore_completions_.erase(key);
    std::erase_if(topology_restore_suppressions_, [&](const auto& item) {
      return item.first.first == key || item.first.second == key;
    });
  }
  configuration_sequence_.fetch_add(1U, std::memory_order_release);
}

void PeerConnectivityController::SetPolicy(std::string_view node_id,
                                           PeerCountPolicy policy) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  const std::size_t maximum_possible = nodes_.size() - 1U;
  if (policy.maximum() > maximum_possible) {
    throw std::runtime_error("maximum peer count for " + std::string(node_id) +
                             " exceeds available simulation peers");
  }
  if (policy.minimum() > AllowedPeersUnlocked(node_id).size()) {
    throw std::runtime_error("minimum peer count for " + std::string(node_id) +
                             " exceeds allowed logical peers");
  }
  policies_.insert_or_assign(std::string(node_id), policy);
  all_peer_policy_node_ids_.erase(std::string(node_id));
  last_failures_.erase(std::string(node_id));
  last_restoration_failures_.erase(std::string(node_id));
  configuration_sequence_.fetch_add(1U, std::memory_order_release);
}

void PeerConnectivityController::RequestTopologyRestore(
    std::string_view changed_node_id) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  const std::string node_id = FindNodeUnlocked(changed_node_id).id;
  const std::uint64_t requested = NextTopologyRestoreSequence();
  std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
  topology_restore_requests_.at(node_id) = requested;
}

void PeerConnectivityController::SetAllowedPeers(
    std::string_view node_id, std::vector<std::string> peer_node_ids) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  ValidateAllowedPeerUpdateUnlocked(node_id, peer_node_ids);
  allowed_peers_.insert_or_assign(std::string(node_id),
                                  std::move(peer_node_ids));
  last_failures_.erase(std::string(node_id));
  last_restoration_failures_.erase(std::string(node_id));
  configuration_sequence_.fetch_add(1U, std::memory_order_release);
}

void PeerConnectivityController::ValidateAllowedPeerUpdate(
    std::string_view node_id, const std::vector<std::string>& peer_node_ids) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  ValidateAllowedPeerUpdateUnlocked(node_id, peer_node_ids);
}

std::vector<std::string> PeerConnectivityController::AllowedPeersFor(
    std::string_view node_id) {
  std::lock_guard<std::mutex> lock(operation_mutex_);
  return AllowedPeersUnlocked(node_id);
}

void PeerConnectivityController::ConnectPeer(std::string_view node_id,
                                             std::string_view peer_node_id,
                                             std::chrono::seconds timeout,
                                             std::stop_token stop_token) {
  ChainNodeConfig node;
  ChainNodeConfig peer;
  std::vector<ChainNodeConfig> nodes;
  std::uint64_t expected_configuration_sequence = 0U;
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    node = FindNodeUnlocked(node_id);
    peer = FindNodeUnlocked(peer_node_id);
    nodes = nodes_;
    expected_configuration_sequence =
        configuration_sequence_.load(std::memory_order_acquire);
  }
  if (node.id == peer.id) {
    throw std::runtime_error("peer source and target must differ");
  }
  if (!node_available_handler_(node.id) || !node_available_handler_(peer.id)) {
    throw std::runtime_error("peer source and target must both be running");
  }
  auto rpc_lock = LockRpc(stop_token);
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    if (configuration_sequence_.load(std::memory_order_acquire) !=
        expected_configuration_sequence) {
      throw std::runtime_error(
          "peer connection node inventory changed before admission");
    }
    node = FindNodeUnlocked(node_id);
    peer = FindNodeUnlocked(peer_node_id);
    nodes = nodes_;
    const std::vector<std::string>& allowed = AllowedPeersUnlocked(node.id);
    if (std::find(allowed.begin(), allowed.end(), peer.id) == allowed.end()) {
      throw std::runtime_error(
          "peer target is not an active logical edge from " + node.id + ": " +
          peer.id);
    }
  }
  RequireUnambiguousPeerIdentity(nodes, node, stop_token);
  const std::string endpoint = PeerEndpoint(peer);
  SetPeerConnectionState(node, endpoint, true, timeout, stop_token);
  bool still_allowed = false;
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    const std::vector<std::string>& allowed = AllowedPeersUnlocked(node.id);
    still_allowed =
        std::find(allowed.begin(), allowed.end(), peer.id) != allowed.end();
  }
  if (!still_allowed) {
    try {
      SetPeerConnectionState(node, endpoint, false, timeout, stop_token);
    } catch (...) {
      throw PeerMutationOutcomeUnconfirmed(
          "peer target was removed from the active logical edge during "
          "connection and compensation failed: " +
          ExceptionMessage(std::current_exception()));
    }
    throw std::runtime_error(
        "peer target was removed from the active logical edge during "
        "connection: " +
        node.id + ": " + peer.id);
  }
  std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
  topology_restore_suppressions_.erase({node.id, peer.id});
}

void PeerConnectivityController::DisconnectPeer(std::string_view node_id,
                                                std::string_view peer_node_id,
                                                std::chrono::seconds timeout,
                                                std::stop_token stop_token) {
  ChainNodeConfig node;
  ChainNodeConfig peer;
  std::vector<ChainNodeConfig> nodes;
  std::uint64_t expected_configuration_sequence = 0U;
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    node = FindNodeUnlocked(node_id);
    peer = FindNodeUnlocked(peer_node_id);
    nodes = nodes_;
    expected_configuration_sequence =
        configuration_sequence_.load(std::memory_order_acquire);
  }
  if (node.id == peer.id) {
    throw std::runtime_error("peer source and target must differ");
  }
  if (!node_available_handler_(node.id)) {
    throw std::runtime_error("peer source node is not running");
  }
  auto rpc_lock = LockRpc(stop_token);
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    if (configuration_sequence_.load(std::memory_order_acquire) !=
        expected_configuration_sequence) {
      throw std::runtime_error(
          "peer disconnection node inventory changed before admission");
    }
    node = FindNodeUnlocked(node_id);
    peer = FindNodeUnlocked(peer_node_id);
    nodes = nodes_;
  }
  RequireUnambiguousPeerIdentity(nodes, node, stop_token);
  const std::string endpoint = PeerEndpoint(peer);
  SetPeerConnectionState(node, endpoint, false, timeout, stop_token);
  const std::uint64_t sequence = NextTopologyRestoreSequence();
  std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
  topology_restore_suppressions_.insert_or_assign({node.id, peer.id}, sequence);
}

void PeerConnectivityController::SetPeerConnectionState(
    const ChainNodeConfig& node, const std::string& endpoint, bool connected,
    std::chrono::seconds timeout, std::stop_token stop_token) const {
  const bool connected_before =
      !driver_.ConnectedPeerAddresses(node, {endpoint}, stop_token).empty();
  try {
    if (connected) {
      driver_.ConnectPeer(node, endpoint, stop_token);
      driver_.WaitForPeerAddress(node, endpoint, timeout, stop_token);
    } else {
      driver_.DisconnectPeer(node, endpoint, stop_token);
      driver_.WaitForPeerAddressAbsent(node, endpoint, timeout, stop_token);
    }
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    const bool cancelled = stop_token.stop_requested();
    std::stop_source rollback_stop_source;
    std::optional<std::jthread> rollback_timer;
    if (cancelled) {
      rollback_timer.emplace(
          [&rollback_stop_source](std::stop_token timer_stop_token) {
            std::condition_variable_any condition;
            std::mutex mutex;
            std::unique_lock<std::mutex> lock(mutex);
            const bool stopped = condition.wait_for(
                lock, timer_stop_token, kCancelledMutationRollbackBound,
                [] { return false; });
            if (!stopped) {
              rollback_stop_source.request_stop();
            }
          });
    }
    const std::stop_token rollback_stop_token =
        cancelled ? rollback_stop_source.get_token() : std::stop_token{};
    try {
      if (connected_before) {
        driver_.ConnectPeer(node, endpoint, rollback_stop_token);
        driver_.WaitForPeerAddress(node, endpoint, timeout,
                                   rollback_stop_token);
      } else {
        driver_.DisconnectPeer(node, endpoint, rollback_stop_token);
        driver_.WaitForPeerAddressAbsent(node, endpoint, timeout,
                                         rollback_stop_token);
      }
    } catch (...) {
      if (rollback_timer) {
        rollback_timer->request_stop();
        rollback_timer->join();
      }
      RethrowPeerMutationFailure(original_error,
                                 connected ? "connection" : "disconnection",
                                 std::current_exception());
    }
    if (rollback_timer) {
      rollback_timer->request_stop();
      rollback_timer->join();
    }
    std::rethrow_exception(original_error);
  }
}

const ChainNodeConfig& PeerConnectivityController::FindNodeUnlocked(
    std::string_view node_id) const {
  return FindNodeConfig(nodes_, node_id);
}

const std::vector<std::string>&
PeerConnectivityController::AllowedPeersUnlocked(
    std::string_view node_id) const {
  const auto allowed = allowed_peers_.find(std::string(node_id));
  if (allowed == allowed_peers_.end()) {
    throw std::runtime_error("missing allowed peer set for simulation node: " +
                             std::string(node_id));
  }
  return allowed->second;
}

void PeerConnectivityController::ValidateAllowedPeersUnlocked(
    std::string_view node_id,
    const std::vector<std::string>& peer_node_ids) const {
  const ChainNodeConfig& node = FindNodeUnlocked(node_id);
  std::set<std::string> unique;
  for (const std::string& peer_node_id : peer_node_ids) {
    const ChainNodeConfig& peer = FindNodeUnlocked(peer_node_id);
    if (peer.id == node.id) {
      throw std::runtime_error("allowed peer set must not contain its node");
    }
    if (!unique.insert(peer.id).second) {
      throw std::runtime_error("allowed peer set contains a duplicate peer: " +
                               peer.id);
    }
  }
}

void PeerConnectivityController::ValidateAllowedPeerUpdateUnlocked(
    std::string_view node_id,
    const std::vector<std::string>& peer_node_ids) const {
  ValidateAllowedPeersUnlocked(node_id, peer_node_ids);
  const auto policy = policies_.find(std::string(node_id));
  if (policy != policies_.end() &&
      policy->second.minimum() > peer_node_ids.size()) {
    throw std::runtime_error(
        "peer policy minimum exceeds allowed logical peers for " +
        std::string(node_id));
  }
}

void PeerConnectivityController::RequireUnambiguousPeerIdentity(
    const std::vector<ChainNodeConfig>& nodes, const ChainNodeConfig& node,
    std::stop_token stop_token) const {
  std::vector<std::string> candidate_endpoints;
  candidate_endpoints.reserve(nodes.size() - 1U);
  for (const ChainNodeConfig& candidate : nodes) {
    if (candidate.id != node.id) {
      candidate_endpoints.push_back(PeerEndpoint(candidate));
    }
  }
  static_cast<void>(
      driver_.ConnectedPeerAddresses(node, candidate_endpoints, stop_token));
}

std::unique_lock<std::mutex> PeerConnectivityController::LockRpc(
    std::stop_token stop_token) {
  std::unique_lock<std::mutex> lock(rpc_mutex_, std::defer_lock);
  while (true) {
    if (stop_token.stop_requested()) {
      throw SimulationCancelled();
    }
    if (lock.try_lock()) {
      return lock;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

std::uint64_t PeerConnectivityController::NextTopologyRestoreSequence() {
  std::uint64_t current =
      topology_restore_sequence_.load(std::memory_order_relaxed);
  while (true) {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      throw std::overflow_error("peer topology restore sequence overflow");
    }
    if (topology_restore_sequence_.compare_exchange_weak(
            current, current + 1U, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return current + 1U;
    }
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
  std::vector<ChainNodeConfig> nodes;
  std::map<std::string, PeerCountPolicy> policies;
  AllowedPeerMap allowed_peers;
  std::map<std::string, std::uint64_t> topology_restore_requests;
  std::map<std::string, std::uint64_t> topology_restore_completions;
  std::uint64_t configuration_sequence = 0U;
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    nodes = nodes_;
    policies = policies_;
    allowed_peers = allowed_peers_;
    configuration_sequence =
        configuration_sequence_.load(std::memory_order_acquire);
    std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
    topology_restore_requests = topology_restore_requests_;
    topology_restore_completions = topology_restore_completions_;
  }
  for (const auto& [node_id, policy] : policies) {
    if (stop_token.stop_requested()) {
      return;
    }
    if (!node_available_handler_(node_id)) {
      continue;
    }
    try {
      if (EnforcePolicy(FindNodeConfig(nodes, node_id), nodes,
                        allowed_peers.at(node_id), allowed_peers, &policy,
                        std::nullopt, {}, configuration_sequence, stop_token)) {
        std::lock_guard<std::mutex> lock(operation_mutex_);
        last_failures_.erase(node_id);
      }
    } catch (const SimulationCancelled&) {
      if (stop_token.stop_requested()) {
        return;
      }
      throw;
    } catch (const PeerMutationOutcomeUnconfirmed& error) {
      if (stop_token.stop_requested()) {
        return;
      }
      ReportFailure(node_id, error.what(), std::nullopt);
    } catch (const std::exception& error) {
      if (stop_token.stop_requested()) {
        return;
      }
      ReportFailure(node_id, error.what(), configuration_sequence);
    }
  }

  for (const ChainNodeConfig& changed_node : nodes) {
    const std::uint64_t requested =
        topology_restore_requests.at(changed_node.id);
    if (requested == topology_restore_completions.at(changed_node.id)) {
      continue;
    }
    const std::string& changed_node_id = changed_node.id;
    bool restored = true;
    for (const ChainNodeConfig& source : nodes) {
      const std::vector<std::string>& allowed = allowed_peers.at(source.id);
      const bool affected = source.id == changed_node_id ||
                            std::find(allowed.begin(), allowed.end(),
                                      changed_node_id) != allowed.end();
      if (!affected) {
        continue;
      }
      if (!node_available_handler_(source.id)) {
        restored = false;
        continue;
      }
      try {
        const auto policy = policies.find(source.id);
        if (policy != policies.end()) {
          continue;
        }
        if (!EnforcePolicy(source, nodes, allowed, allowed_peers, nullptr,
                           requested, changed_node_id, configuration_sequence,
                           stop_token)) {
          restored = false;
        } else {
          std::lock_guard<std::mutex> lock(operation_mutex_);
          last_restoration_failures_.erase(source.id);
        }
      } catch (const SimulationCancelled&) {
        if (stop_token.stop_requested()) {
          return;
        }
        throw;
      } catch (const PeerMutationOutcomeUnconfirmed& error) {
        if (stop_token.stop_requested()) {
          return;
        }
        restored = false;
        ReportRestorationFailure(source.id, error.what(), std::nullopt);
      } catch (const std::exception& error) {
        if (stop_token.stop_requested()) {
          return;
        }
        restored = false;
        ReportRestorationFailure(source.id, error.what(),
                                 configuration_sequence);
      }
    }
    if (restored && configuration_sequence_.load(std::memory_order_acquire) ==
                        configuration_sequence) {
      std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
      const auto current = topology_restore_requests_.find(changed_node_id);
      if (current != topology_restore_requests_.end() &&
          current->second == requested) {
        topology_restore_completions_[changed_node_id] = requested;
      }
    }
  }
}

bool PeerConnectivityController::EnforcePolicy(
    const ChainNodeConfig& node, const std::vector<ChainNodeConfig>& nodes,
    const std::vector<std::string>& allowed_peer_ids,
    const AllowedPeerMap& all_allowed_peers,
    const PeerCountPolicy* policy,
    std::optional<std::uint64_t> restore_request_sequence,
    std::string_view changed_node_id,
    std::uint64_t expected_configuration_sequence, std::stop_token stop_token) {
  auto rpc_lock = LockRpc(stop_token);
  if (configuration_sequence_.load(std::memory_order_acquire) !=
      expected_configuration_sequence) {
    return false;
  }
  std::set<std::string> excluded_peer_ids;
  if (restore_request_sequence) {
    if (node.id != changed_node_id) {
      for (const std::string& peer_node_id : allowed_peer_ids) {
        if (peer_node_id != changed_node_id) {
          excluded_peer_ids.insert(peer_node_id);
        }
      }
    }
    std::lock_guard<std::mutex> restoration_lock(restoration_mutex_);
    for (const auto& [edge, suppression_sequence] :
         topology_restore_suppressions_) {
      if (edge.first == node.id &&
          suppression_sequence > *restore_request_sequence) {
        excluded_peer_ids.insert(edge.second);
      }
    }
  }
  std::vector<const ChainNodeConfig*> candidates;
  std::vector<std::string> candidate_endpoints;
  std::vector<const ChainNodeConfig*> all_candidates;
  std::vector<std::string> all_candidate_endpoints;
  std::set<std::string> allowed_peer_id_set(allowed_peer_ids.begin(),
                                            allowed_peer_ids.end());
  for (const ChainNodeConfig& candidate : nodes) {
    if (candidate.id == node.id) {
      continue;
    }
    all_candidates.push_back(&candidate);
    all_candidate_endpoints.push_back(PeerEndpoint(candidate));
  }
  std::size_t eligible_peer_count = 0U;
  for (const std::string& peer_node_id : allowed_peer_ids) {
    if (excluded_peer_ids.contains(peer_node_id)) {
      continue;
    }
    ++eligible_peer_count;
    const ChainNodeConfig& candidate = FindNodeConfig(nodes, peer_node_id);
    if (!node_available_handler_(candidate.id)) {
      continue;
    }
    candidates.push_back(&candidate);
    candidate_endpoints.push_back(PeerEndpoint(candidate));
  }
  if (policy != nullptr && candidates.size() < policy->minimum()) {
    throw std::runtime_error(
        "peer policy minimum cannot be met with running "
        "simulation peers");
  }
  const auto candidate_count = static_cast<std::uint32_t>(candidates.size());
  const PeerCountPolicy effective_policy =
      policy == nullptr ? PeerCountPolicy(candidate_count, candidate_count)
                        : *policy;

  const std::vector<std::string> connected_addresses =
      driver_.ConnectedPeerAddresses(node, all_candidate_endpoints, stop_token);
  if (configuration_sequence_.load(std::memory_order_acquire) !=
      expected_configuration_sequence) {
    return false;
  }
  const std::set<std::string> connected(connected_addresses.begin(),
                                        connected_addresses.end());
  for (std::size_t index = 0; index < all_candidates.size(); ++index) {
    const auto reverse_allowed = all_allowed_peers.find(
        all_candidates[index]->id);
    const bool reverse_requires_physical_session =
        reverse_allowed != all_allowed_peers.end() &&
        std::find(reverse_allowed->second.begin(), reverse_allowed->second.end(),
                  node.id) != reverse_allowed->second.end();
    if (allowed_peer_id_set.contains(all_candidates[index]->id) ||
        reverse_requires_physical_session ||
        !connected.contains(all_candidate_endpoints[index])) {
      continue;
    }
    SetPeerConnectionState(node, all_candidate_endpoints[index], false,
                           std::chrono::seconds(10), stop_token);
    if (configuration_sequence_.load(std::memory_order_acquire) !=
        expected_configuration_sequence) {
      SetPeerConnectionState(node, all_candidate_endpoints[index], true,
                             std::chrono::seconds(10), stop_token);
      return false;
    }
    action_handler_(node.id, all_candidates[index]->id,
                    PeerConnectivityAction::kDisconnected, effective_policy);
  }
  std::vector<std::size_t> connected_indexes;
  std::vector<std::size_t> disconnected_indexes;
  for (std::size_t index = 0; index < candidate_endpoints.size(); ++index) {
    if (connected.contains(candidate_endpoints[index])) {
      connected_indexes.push_back(index);
    } else {
      disconnected_indexes.push_back(index);
    }
  }

  while (connected_indexes.size() < effective_policy.minimum() &&
         !disconnected_indexes.empty()) {
    const std::size_t index = disconnected_indexes.front();
    disconnected_indexes.erase(disconnected_indexes.begin());
    SetPeerConnectionState(node, candidate_endpoints[index], true,
                           std::chrono::seconds(10), stop_token);
    if (configuration_sequence_.load(std::memory_order_acquire) !=
        expected_configuration_sequence) {
      SetPeerConnectionState(node, candidate_endpoints[index], false,
                             std::chrono::seconds(10), stop_token);
      return false;
    }
    connected_indexes.push_back(index);
    action_handler_(node.id, candidates[index]->id,
                    policy == nullptr
                        ? PeerConnectivityAction::kTopologyRestored
                        : PeerConnectivityAction::kConnected,
                    effective_policy);
  }

  while (connected_indexes.size() > effective_policy.maximum()) {
    const std::size_t index = connected_indexes.back();
    connected_indexes.pop_back();
    SetPeerConnectionState(node, candidate_endpoints[index], false,
                           std::chrono::seconds(10), stop_token);
    if (configuration_sequence_.load(std::memory_order_acquire) !=
        expected_configuration_sequence) {
      SetPeerConnectionState(node, candidate_endpoints[index], true,
                             std::chrono::seconds(10), stop_token);
      return false;
    }
    action_handler_(node.id, candidates[index]->id,
                    PeerConnectivityAction::kDisconnected, effective_policy);
  }
  return policy != nullptr || candidates.size() == eligible_peer_count;
}

void PeerConnectivityController::ReportFailure(
    std::string_view node_id, std::string_view error,
    std::optional<std::uint64_t> expected_configuration_sequence) {
  const std::string key(node_id);
  const std::string detail(error);
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    if (expected_configuration_sequence &&
        configuration_sequence_.load(std::memory_order_acquire) !=
            *expected_configuration_sequence) {
      return;
    }
    const auto previous = last_failures_.find(key);
    if (previous != last_failures_.end() && previous->second == detail) {
      return;
    }
    last_failures_.insert_or_assign(key, detail);
  }
  failure_handler_(node_id, error);
}

void PeerConnectivityController::ReportRestorationFailure(
    std::string_view node_id, std::string_view error,
    std::optional<std::uint64_t> expected_configuration_sequence) {
  const std::string key(node_id);
  const std::string detail(error);
  {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    if (expected_configuration_sequence &&
        configuration_sequence_.load(std::memory_order_acquire) !=
            *expected_configuration_sequence) {
      return;
    }
    const auto previous = last_restoration_failures_.find(key);
    if (previous != last_restoration_failures_.end() &&
        previous->second == detail) {
      return;
    }
    last_restoration_failures_.insert_or_assign(key, detail);
  }
  failure_handler_(node_id, "restart topology restoration failed: " + detail);
}

}  // namespace bbp
