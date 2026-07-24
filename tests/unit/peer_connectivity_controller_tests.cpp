#include <atomic>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bbp/drivers/chain_command_executor.h"
#include "bbp/node_log_collector.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/simulation_cancelled.h"

namespace {

class TestChainDriver final : public bbp::ChainDriver {
 public:
  bbp::ProcessSpec RenderProcess(const bbp::ChainNodeConfig&) const override {
    return {};
  }
  std::optional<bbp::LogTailChunk> ReadLogTail(const bbp::ChainNodeConfig& node,
                                               bbp::ChainLogSource,
                                               const bbp::LogTailCursor&,
                                               std::uint64_t) const override {
    if (block_log_read_.load(std::memory_order_acquire) &&
        node.id == blocked_log_node_id_) {
      log_read_blocked_.store(true, std::memory_order_release);
      while (!release_log_read_.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    ++log_read_counts_[node.id];
    return std::nullopt;
  }
  bbp::RpcEndpoint Endpoint(const bbp::ChainNodeConfig&) const override {
    return {};
  }
  void WaitReady(const bbp::ChainNodeConfig&, std::chrono::seconds,
                 std::stop_token) const override {}
  void WaitForHeight(const bbp::ChainNodeConfig&, std::uint64_t,
                     std::chrono::seconds, std::stop_token) const override {}
  void WaitForPeerCount(const bbp::ChainNodeConfig&, std::uint64_t,
                        std::chrono::seconds, std::stop_token) const override {}
  void WaitForPeerAddress(const bbp::ChainNodeConfig& config,
                          const std::string& address, std::chrono::seconds,
                          std::stop_token stop_token) const override {
    if (block_connected_wait_.load(std::memory_order_acquire) &&
        config.id == blocked_connected_wait_node_id_) {
      connected_wait_blocked_.store(true, std::memory_order_release);
      while (!release_connected_wait_.load(std::memory_order_acquire)) {
        if (stop_token.stop_requested()) {
          throw bbp::SimulationCancelled();
        }
        std::this_thread::yield();
      }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (connected_wait_failure_) {
      const std::string message = std::move(*connected_wait_failure_);
      connected_wait_failure_.reset();
      throw std::runtime_error(message);
    }
    const auto found = connections_.find(config.id);
    if (found == connections_.end() || !found->second.contains(address)) {
      throw std::runtime_error("peer did not reach connected state");
    }
  }
  void WaitForPeerAddressAbsent(const bbp::ChainNodeConfig& config,
                                const std::string& address,
                                std::chrono::seconds,
                                std::stop_token stop_token) const override {
    if (block_disconnected_wait_.load(std::memory_order_acquire) &&
        config.id == blocked_disconnected_wait_node_id_) {
      disconnected_wait_blocked_.store(true, std::memory_order_release);
      while (!release_disconnected_wait_.load(std::memory_order_acquire)) {
        if (stop_token.stop_requested()) {
          throw bbp::SimulationCancelled();
        }
        std::this_thread::yield();
      }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (disconnected_wait_failure_) {
      const std::string message = std::move(*disconnected_wait_failure_);
      disconnected_wait_failure_.reset();
      throw std::runtime_error(message);
    }
    const auto found = connections_.find(config.id);
    if (found != connections_.end() && found->second.contains(address)) {
      throw std::runtime_error("peer did not reach disconnected state");
    }
  }
  bbp::ChainMetrics ReadMetrics(const bbp::ChainNodeConfig&,
                                std::stop_token) const override {
    return {};
  }
  std::vector<std::string> PeerAddresses(const bbp::ChainNodeConfig& config,
                                         std::stop_token) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = connections_.find(config.id);
    return found == connections_.end()
               ? std::vector<std::string>{}
               : std::vector<std::string>(found->second.begin(),
                                          found->second.end());
  }
  std::vector<std::string> ConnectedPeerAddresses(
      const bbp::ChainNodeConfig& config,
      const std::vector<std::string>& candidate_addresses,
      std::stop_token) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> connected;
    const auto found = connections_.find(config.id);
    if (found == connections_.end()) {
      return connected;
    }
    for (const std::string& candidate : candidate_addresses) {
      if (found->second.contains(candidate)) {
        connected.push_back(candidate);
      }
    }
    return connected;
  }
  std::vector<std::string> GenerateBlocks(const bbp::ChainNodeConfig&,
                                          std::uint32_t, const std::string&,
                                          std::stop_token) const override {
    return {};
  }
  std::uint64_t ReadBlockNonRewardTransactionCount(
      const bbp::ChainNodeConfig&, const std::string&,
      std::stop_token) const override {
    return 0U;
  }
  std::string CreateWalletAddress(const bbp::ChainNodeConfig&,
                                  bbp::ChainWalletMode,
                                  std::stop_token) const override {
    return {};
  }
  std::uint64_t WaitForWalletBalance(const bbp::ChainNodeConfig&,
                                     bbp::ChainWalletMode, std::uint64_t,
                                     std::uint64_t, std::chrono::seconds,
                                     std::stop_token) const override {
    return 0U;
  }
  bbp::ChainWalletSnapshot ReadWalletSnapshot(const bbp::ChainNodeConfig&,
                                              bbp::ChainWalletMode,
                                              std::uint32_t,
                                              std::stop_token) const override {
    return {};
  }
  bbp::ChainUtxo FindSpendableOutput(const bbp::ChainNodeConfig&,
                                     const std::vector<std::string>&,
                                     const std::string&, std::uint64_t,
                                     std::uint64_t,
                                     std::stop_token) const override {
    return {};
  }
  bbp::ChainRawTransactionResult SendRawTransaction(
      const bbp::ChainNodeConfig&, const bbp::ChainUtxo&, const std::string&,
      const std::string&, const std::string&, std::uint64_t, std::uint64_t,
      std::chrono::seconds, std::stop_token) const override {
    return {};
  }
  bbp::ChainWalletTransactionResult SendWalletTransaction(
      const bbp::ChainNodeConfig&, bbp::ChainWalletMode, const std::string&,
      std::uint64_t, std::uint64_t, std::chrono::seconds,
      std::stop_token) const override {
    return {};
  }
  bbp::ChainTransactionObservation ObserveTransaction(
      const bbp::ChainNodeConfig&, const std::string&,
      std::stop_token) const override {
    return {};
  }
  bbp::ChainTransactionObservation WaitForTransaction(
      const bbp::ChainNodeConfig&, const std::string&, std::chrono::seconds,
      std::stop_token) const override {
    return {};
  }
  std::uint64_t WaitForMempoolTransaction(const bbp::ChainNodeConfig&,
                                          const std::string&,
                                          std::chrono::seconds,
                                          std::stop_token) const override {
    return 0U;
  }
  void ConnectPeer(const bbp::ChainNodeConfig& config,
                   const std::string& address, std::stop_token) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ignore_next_connect_) {
      ignore_next_connect_ = false;
      return;
    }
    connections_[config.id].insert(address);
  }
  void DisconnectPeer(const bbp::ChainNodeConfig& config,
                      const std::string& address,
                      std::stop_token) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++disconnect_call_count_;
    if (disconnect_failure_) {
      const std::string message = std::move(*disconnect_failure_);
      disconnect_failure_.reset();
      throw std::runtime_error(message);
    }
    connections_[config.id].erase(address);
  }
  void ChangeLogVerbosity(const bbp::ChainNodeConfig& config,
                          bbp::ChainLogVerbosityChange,
                          std::stop_token stop_token) const override {
    if (block_verbosity_.load(std::memory_order_acquire) &&
        config.id == blocked_verbosity_node_id_) {
      verbosity_blocked_.store(true, std::memory_order_release);
      while (!release_verbosity_.load(std::memory_order_acquire)) {
        if (stop_token.stop_requested()) {
          throw bbp::SimulationCancelled();
        }
        std::this_thread::yield();
      }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    last_verbosity_node_ = config.id;
  }
  void SetMiningDifficulty(const bbp::ChainNodeConfig&, bbp::MiningDifficulty,
                           std::stop_token) const override {}
  void StartMining(const bbp::ChainNodeConfig&, const std::string&,
                   std::stop_token) const override {}
  void StopMining(const bbp::ChainNodeConfig&, std::stop_token) const override {
  }
  void SetNetworkActive(const bbp::ChainNodeConfig&, bool,
                        std::stop_token) const override {}
  void Stop(const bbp::ChainNodeConfig&, std::stop_token) const override {}

  std::size_t ConnectionCount(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = connections_.find(node_id);
    return found == connections_.end() ? 0U : found->second.size();
  }

  std::uint64_t LogReadCount(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = log_read_counts_.find(node_id);
    return found == log_read_counts_.end() ? 0U : found->second;
  }

  std::string LastVerbosityNode() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_verbosity_node_;
  }

  std::uint64_t DisconnectCallCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return disconnect_call_count_;
  }

  void BlockLogReadFor(std::string node_id) {
    blocked_log_node_id_ = std::move(node_id);
    release_log_read_.store(false, std::memory_order_release);
    log_read_blocked_.store(false, std::memory_order_release);
    block_log_read_.store(true, std::memory_order_release);
  }

  bool LogReadBlocked() const {
    return log_read_blocked_.load(std::memory_order_acquire);
  }

  void ReleaseLogRead() {
    release_log_read_.store(true, std::memory_order_release);
    block_log_read_.store(false, std::memory_order_release);
  }

  void BlockVerbosityFor(std::string node_id) {
    blocked_verbosity_node_id_ = std::move(node_id);
    release_verbosity_.store(false, std::memory_order_release);
    verbosity_blocked_.store(false, std::memory_order_release);
    block_verbosity_.store(true, std::memory_order_release);
  }

  bool VerbosityBlocked() const {
    return verbosity_blocked_.load(std::memory_order_acquire);
  }

  void ReleaseVerbosity() {
    release_verbosity_.store(true, std::memory_order_release);
    block_verbosity_.store(false, std::memory_order_release);
  }

  bool IsConnected(const std::string& node_id,
                   const std::string& address) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = connections_.find(node_id);
    return found != connections_.end() && found->second.contains(address);
  }

  void IgnoreNextConnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    ignore_next_connect_ = true;
  }

  void FailNextConnectedWait(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    connected_wait_failure_ = std::move(message);
  }

  void FailNextDisconnectedWait(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    disconnected_wait_failure_ = std::move(message);
  }

  void FailNextDisconnect(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    disconnect_failure_ = std::move(message);
  }

  void BlockConnectedWaitFor(std::string node_id) {
    blocked_connected_wait_node_id_ = std::move(node_id);
    release_connected_wait_.store(false, std::memory_order_release);
    connected_wait_blocked_.store(false, std::memory_order_release);
    block_connected_wait_.store(true, std::memory_order_release);
  }

  bool ConnectedWaitBlocked() const {
    return connected_wait_blocked_.load(std::memory_order_acquire);
  }

  void ReleaseConnectedWait() {
    release_connected_wait_.store(true, std::memory_order_release);
    block_connected_wait_.store(false, std::memory_order_release);
  }

  void BlockDisconnectedWaitFor(std::string node_id) {
    blocked_disconnected_wait_node_id_ = std::move(node_id);
    release_disconnected_wait_.store(false, std::memory_order_release);
    disconnected_wait_blocked_.store(false, std::memory_order_release);
    block_disconnected_wait_.store(true, std::memory_order_release);
  }

  bool DisconnectedWaitBlocked() const {
    return disconnected_wait_blocked_.load(std::memory_order_acquire);
  }

  void ReleaseDisconnectedWait() {
    release_disconnected_wait_.store(true, std::memory_order_release);
    block_disconnected_wait_.store(false, std::memory_order_release);
  }

 private:
  mutable std::mutex mutex_;
  mutable std::map<std::string, std::set<std::string>> connections_;
  mutable std::map<std::string, std::uint64_t> log_read_counts_;
  mutable std::string last_verbosity_node_;
  mutable std::uint64_t disconnect_call_count_ = 0U;
  std::string blocked_log_node_id_;
  std::string blocked_verbosity_node_id_;
  mutable std::atomic<bool> block_log_read_ = false;
  mutable std::atomic<bool> log_read_blocked_ = false;
  mutable std::atomic<bool> release_log_read_ = false;
  mutable std::atomic<bool> block_verbosity_ = false;
  mutable std::atomic<bool> verbosity_blocked_ = false;
  mutable std::atomic<bool> release_verbosity_ = false;
  mutable bool ignore_next_connect_ = false;
  mutable std::optional<std::string> disconnect_failure_;
  mutable std::optional<std::string> connected_wait_failure_;
  mutable std::optional<std::string> disconnected_wait_failure_;
  std::string blocked_connected_wait_node_id_;
  std::string blocked_disconnected_wait_node_id_;
  mutable std::atomic<bool> block_connected_wait_ = false;
  mutable std::atomic<bool> connected_wait_blocked_ = false;
  mutable std::atomic<bool> release_connected_wait_ = false;
  mutable std::atomic<bool> block_disconnected_wait_ = false;
  mutable std::atomic<bool> disconnected_wait_blocked_ = false;
  mutable std::atomic<bool> release_disconnected_wait_ = false;
};

std::vector<bbp::ChainNodeConfig> TestNodes() {
  std::vector<bbp::ChainNodeConfig> nodes(3);
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    nodes[index].id = "node-" + std::to_string(index + 1U);
    nodes[index].p2p_host = "10.0.0." + std::to_string(index + 1U);
    nodes[index].p2p_port = static_cast<std::uint16_t>(18000U + index);
  }
  return nodes;
}

bbp::PeerConnectivityController::AllowedPeerMap FullAllowedPeers() {
  return {
      {"node-1", {"node-2", "node-3"}},
      {"node-2", {"node-1", "node-3"}},
      {"node-3", {"node-1", "node-2"}},
  };
}

bool WaitFor(const std::function<bool()>& predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return predicate();
}

}  // namespace

BOOST_AUTO_TEST_CASE(chain_command_executor_resolves_live_node_inventory) {
  TestChainDriver driver;
  std::mutex nodes_mutex;
  std::vector<bbp::ChainNodeConfig> nodes = TestNodes();
  std::shared_ptr<int> generation;
  bbp::ChainCommandExecutor executor(
      driver,
      [&] {
        std::lock_guard<std::mutex> lock(nodes_mutex);
        return bbp::NodeConfigSnapshot(nodes, generation);
      },
      [](const bbp::ChainNodeConfig&, std::stop_token) {},
      [](bbp::BlockProductionPolicy) {},
      [](const bbp::ChainNodeConfig&, bbp::MiningDifficulty, std::stop_token) {
      },
      [](const bbp::ChainNodeConfig&, const bbp::ChainNodeConfig&,
         std::stop_token) {},
      [](const bbp::ChainNodeConfig&, const bbp::ChainNodeConfig&,
         std::stop_token) {},
      [](const bbp::ChainNodeConfig&, bbp::PeerCountPolicy) {});

  bbp::SimulationCommand command;
  command.kind = bbp::SimulationCommandKind::kIncreaseLogVerbosity;
  command.node_id = "node-4";
  BOOST_CHECK_THROW(executor.Execute(command), std::runtime_error);

  bbp::ChainNodeConfig added;
  added.id = "node-4";
  std::weak_ptr<int> generation_lease;
  {
    std::lock_guard<std::mutex> lock(nodes_mutex);
    nodes.push_back(added);
    generation = std::make_shared<int>(4);
    generation_lease = generation;
  }
  driver.BlockVerbosityFor("node-4");
  std::jthread execution([&] { executor.Execute(command); });
  BOOST_REQUIRE(WaitFor([&] { return driver.VerbosityBlocked(); }));
  {
    std::lock_guard<std::mutex> lock(nodes_mutex);
    nodes.clear();
    generation.reset();
  }
  BOOST_TEST(!generation_lease.expired());
  driver.ReleaseVerbosity();
  execution.join();
  BOOST_TEST(driver.LastVerbosityNode() == "node-4");
  BOOST_TEST(generation_lease.expired());
}

BOOST_AUTO_TEST_CASE(node_log_collector_tracks_live_node_inventory) {
  TestChainDriver driver;
  std::mutex nodes_mutex;
  std::vector<bbp::ChainNodeConfig> nodes = {TestNodes().front()};
  std::shared_ptr<int> generation = std::make_shared<int>(1);
  std::weak_ptr<int> generation_lease = generation;
  bbp::NodeLogCollector collector(
      driver,
      [&] {
        std::lock_guard<std::mutex> lock(nodes_mutex);
        return bbp::NodeConfigSnapshot(nodes, generation);
      },
      std::chrono::milliseconds(5), 1024U,
      [](const bbp::ChainNodeConfig&, bbp::ChainLogSource,
         const bbp::LogTailChunk&) {});

  driver.BlockLogReadFor("node-1");
  collector.Start();
  BOOST_REQUIRE(WaitFor([&] { return driver.LogReadBlocked(); }));
  {
    std::lock_guard<std::mutex> lock(nodes_mutex);
    nodes.clear();
    generation.reset();
  }
  BOOST_TEST(!generation_lease.expired());
  driver.ReleaseLogRead();
  BOOST_REQUIRE(WaitFor([&] { return generation_lease.expired(); }));
  const std::uint64_t removed_read_count = driver.LogReadCount("node-1");
  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  BOOST_TEST(driver.LogReadCount("node-1") == removed_read_count);
  collector.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_registers_live_node_transactionally) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  controller.Start();

  bbp::ChainNodeConfig node;
  node.id = "node-4";
  node.p2p_host = "10.0.0.4";
  node.p2p_port = 18003U;
  controller.RegisterNode(node, std::nullopt, {"node-1", "node-2", "node-3"});
  controller.SetAllowedPeers("node-1", {"node-2", "node-3", "node-4"});
  controller.SetAllowedPeers("node-2", {"node-1", "node-3", "node-4"});
  controller.SetAllowedPeers("node-3", {"node-1", "node-2", "node-4"});
  controller.RequestTopologyRestore("node-4");
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-4") == 3U; }));

  bbp::ChainNodeConfig invalid = node;
  invalid.id = "node-5";
  BOOST_CHECK_THROW(controller.RegisterNode(invalid, std::nullopt, {"missing"}),
                    std::runtime_error);
  BOOST_CHECK_THROW(controller.RequestTopologyRestore("node-5"),
                    std::runtime_error);

  controller.SetAllowedPeers("node-1", {"node-2", "node-3"});
  controller.SetAllowedPeers("node-2", {"node-1", "node-3"});
  controller.SetAllowedPeers("node-3", {"node-1", "node-2"});
  controller.UnregisterNode("node-4");
  BOOST_CHECK_THROW(controller.AllowedPeersFor("node-4"), std::runtime_error);
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_registers_mutually_referencing_batch) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, {}, {}, {}, std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  std::vector<bbp::ChainNodeConfig> nodes(2);
  nodes[0].id = "node-1";
  nodes[1].id = "node-2";

  controller.RegisterNodes(
      nodes,
      {{"node-1", bbp::PeerCountPolicy(1U, 1U)}, {"node-2", std::nullopt}},
      {{"node-1", {"node-2"}}, {"node-2", {"node-1"}}});

  BOOST_TEST(controller.AllowedPeersFor("node-1") ==
                 std::vector<std::string>({"node-2"}),
             boost::test_tools::per_element());
  BOOST_TEST(controller.AllowedPeersFor("node-2") ==
                 std::vector<std::string>({"node-1"}),
             boost::test_tools::per_element());
  controller.SetPolicy("node-2", bbp::PeerCountPolicy(1U, 1U));
  controller.RequestTopologyRestore("node-1");
  controller.RequestTopologyRestore("node-2");
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_prepares_atomic_final_map_and_resizes_only_all_peers) {
  const auto final_nodes = [] {
    std::vector<bbp::ChainNodeConfig> nodes = TestNodes();
    bbp::ChainNodeConfig added;
    added.id = "node-4";
    added.p2p_host = "10.0.0.4";
    added.p2p_port = 18003U;
    nodes.push_back(std::move(added));
    return nodes;
  }();
  const bbp::PeerConnectivityController::AllowedPeerMap final_allowed{
      {"node-1", {"node-2", "node-3", "node-4"}},
      {"node-2", {"node-1", "node-3", "node-4"}},
      {"node-3", {"node-1", "node-2", "node-4"}},
      {"node-4", {"node-1", "node-2", "node-3"}},
  };

  TestChainDriver all_peer_driver;
  bbp::PeerConnectivityController all_peer_controller(
      all_peer_driver, TestNodes(), {{"node-1", bbp::PeerCountPolicy(2U, 2U)}},
      FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {}, {"node-1"});
  {
    auto lease = all_peer_controller.AcquireRpcMutationLease();
    auto prepared = all_peer_controller.PrepareFinalRegistration(
        final_nodes, {{"node-4", std::nullopt}}, final_allowed, {}, lease);
    BOOST_TEST(all_peer_driver.ConnectionCount("node-1") == 0U);
    prepared.Commit();
  }
  BOOST_TEST(all_peer_controller.AllowedPeersFor("node-1") ==
                 std::vector<std::string>({"node-2", "node-3", "node-4"}),
             boost::test_tools::per_element());
  all_peer_controller.RequestTopologyRestore("node-1");
  all_peer_controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return all_peer_driver.ConnectionCount("node-1") == 3U; }));
  all_peer_controller.Stop();

  TestChainDriver fixed_driver;
  bbp::PeerConnectivityController fixed_controller(
      fixed_driver, TestNodes(), {{"node-1", bbp::PeerCountPolicy(2U, 2U)}},
      FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  {
    auto lease = fixed_controller.AcquireRpcMutationLease();
    auto prepared = fixed_controller.PrepareFinalRegistration(
        final_nodes, {{"node-4", std::nullopt}}, final_allowed, {}, lease);
    prepared.Commit();
  }
  fixed_controller.RequestTopologyRestore("node-1");
  fixed_controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return fixed_driver.ConnectionCount("node-1") == 2U; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  BOOST_TEST(fixed_driver.ConnectionCount("node-1") == 2U);
  fixed_controller.Stop();
}

BOOST_AUTO_TEST_CASE(peer_connectivity_controller_rejects_batch_atomically) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  std::vector<bbp::ChainNodeConfig> nodes(2);
  nodes[0].id = "node-4";
  nodes[1].id = "node-5";

  BOOST_CHECK_THROW(
      controller.RegisterNodes(
          nodes, {{"node-4", std::nullopt}, {"node-5", std::nullopt}},
          {{"node-4", {"node-5"}}, {"node-5", {"missing"}}}),
      std::runtime_error);
  BOOST_CHECK_THROW(controller.AllowedPeersFor("node-4"), std::runtime_error);
  BOOST_CHECK_THROW(controller.AllowedPeersFor("node-5"), std::runtime_error);
  BOOST_TEST(controller.AllowedPeersFor("node-1") ==
                 std::vector<std::string>({"node-2", "node-3"}),
             boost::test_tools::per_element());

  controller.RegisterNodes(
      nodes, {{"node-4", std::nullopt}, {"node-5", std::nullopt}},
      {{"node-1", {"node-2", "node-3", "node-4", "node-5"}},
       {"node-4", {"node-1", "node-5"}},
       {"node-5", {"node-1", "node-4"}}});
  BOOST_TEST(
      controller.AllowedPeersFor("node-1") ==
          std::vector<std::string>({"node-2", "node-3", "node-4", "node-5"}),
      boost::test_tools::per_element());
  BOOST_TEST(controller.AllowedPeersFor("node-4") ==
                 std::vector<std::string>({"node-1", "node-5"}),
             boost::test_tools::per_element());
  BOOST_TEST(controller.AllowedPeersFor("node-5") ==
                 std::vector<std::string>({"node-1", "node-4"}),
             boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_bounds_cancelled_unregistration) {
  TestChainDriver driver;
  driver.BlockConnectedWaitFor("node-1");
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  bbp::ChainNodeConfig node;
  node.id = "node-4";
  node.p2p_host = "10.0.0.4";
  node.p2p_port = 18003U;
  controller.RegisterNode(node, std::nullopt, {});
  controller.RequestTopologyRestore("node-1");
  controller.Start();
  BOOST_REQUIRE(WaitFor([&] { return driver.ConnectedWaitBlocked(); }));

  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::jthread unregister([&] {
    try {
      controller.UnregisterNode("node-4", stop_source.get_token());
    } catch (const bbp::SimulationCancelled&) {
      cancelled.store(true, std::memory_order_release);
    }
  });
  stop_source.request_stop();
  unregister.join();
  BOOST_TEST(cancelled.load(std::memory_order_acquire));
  BOOST_TEST(controller.AllowedPeersFor("node-4").empty());

  driver.ReleaseConnectedWait();
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(peer_connectivity_controller_supports_empty_inventory) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, {}, {}, {}, std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  controller.Start();

  bbp::ChainNodeConfig node;
  node.id = "node-1";
  node.p2p_host = "10.0.0.1";
  node.p2p_port = 18000U;
  controller.RegisterNode(node, std::nullopt, {});
  BOOST_TEST(controller.AllowedPeersFor("node-1").empty());
  controller.UnregisterNode("node-1");
  BOOST_CHECK_THROW(controller.AllowedPeersFor("node-1"), std::runtime_error);
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_rejects_removed_disconnect_generation) {
  TestChainDriver driver;
  std::atomic<bool> availability_check_blocked = false;
  std::atomic<bool> release_availability_check = false;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [&](std::string_view node_id) {
        if (node_id == "node-1") {
          availability_check_blocked.store(true, std::memory_order_release);
          while (!release_availability_check.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
        }
        return true;
      },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  std::atomic<bool> rejected = false;
  std::jthread disconnect([&] {
    try {
      controller.DisconnectPeer("node-1", "node-2", std::chrono::seconds(1));
    } catch (const std::runtime_error&) {
      rejected.store(true, std::memory_order_release);
    }
  });
  BOOST_REQUIRE(WaitFor([&] {
    return availability_check_blocked.load(std::memory_order_acquire);
  }));
  controller.SetAllowedPeers("node-1", {"node-3"});
  controller.SetAllowedPeers("node-3", {"node-1"});
  controller.UnregisterNode("node-2");
  release_availability_check.store(true, std::memory_order_release);
  disconnect.join();

  BOOST_TEST(rejected.load(std::memory_order_acquire));
  BOOST_TEST(driver.DisconnectCallCount() == 0U);
}

BOOST_AUTO_TEST_CASE(peer_connectivity_controller_enforces_typed_range) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {{"node-1", bbp::PeerCountPolicy(1U, 2U)}},
      FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-1") == 1U; }));
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.2:18001"));

  controller.SetPolicy("node-1", bbp::PeerCountPolicy(2U, 2U));
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-1") == 2U; }));

  controller.SetPolicy("node-1", bbp::PeerCountPolicy(0U, 1U));
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-1") == 1U; }));
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.2:18001"));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.3:18002"));
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_preserves_reverse_directed_session) {
  TestChainDriver driver;
  const std::vector<bbp::ChainNodeConfig> nodes = TestNodes();
  driver.ConnectPeer(nodes[1], "10.0.0.1:18000", {});
  bbp::PeerConnectivityController controller(
      driver, nodes, {{"node-2", bbp::PeerCountPolicy(0U, 0U)}},
      {{"node-1", {"node-2"}}, {"node-2", {}}, {"node-3", {}}},
      std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  BOOST_TEST(driver.IsConnected("node-2", "10.0.0.1:18000"));
  BOOST_TEST(driver.DisconnectCallCount() == 0U);
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_restores_default_allowed_peers) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  BOOST_TEST(driver.ConnectionCount("node-1") == 0U);

  controller.RequestTopologyRestore("node-1");
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-1") == 2U; }));
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.2:18001"));
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.3:18002"));
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_retries_default_peer_when_available) {
  TestChainDriver driver;
  std::atomic<bool> node_two_available = false;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [&](std::string_view node_id) {
        return node_id != "node-2" ||
               node_two_available.load(std::memory_order_acquire);
      },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.RequestTopologyRestore("node-1");
  controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return driver.IsConnected("node-1", "10.0.0.3:18002"); }));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));

  controller.DisconnectPeer("node-1", "node-3", std::chrono::seconds(1));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.3:18002"));

  node_two_available.store(true, std::memory_order_release);
  BOOST_REQUIRE(
      WaitFor([&] { return driver.IsConnected("node-1", "10.0.0.2:18001"); }));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.3:18002"));

  controller.RequestTopologyRestore("node-1");
  BOOST_REQUIRE(
      WaitFor([&] { return driver.IsConnected("node-1", "10.0.0.3:18002"); }));
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_restore_honors_explicit_maximum) {
  TestChainDriver driver;
  std::atomic<bool> maximum_exceeded = false;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {{"node-1", bbp::PeerCountPolicy(1U, 1U)}},
      FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [&](std::string_view node_id, std::string_view,
          bbp::PeerConnectivityAction, const bbp::PeerCountPolicy&) {
        if (node_id == "node-1" && driver.ConnectionCount("node-1") > 1U) {
          maximum_exceeded.store(true, std::memory_order_release);
        }
      },
      [](std::string_view, std::string_view) {});

  controller.RequestTopologyRestore("node-1");
  controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-1") == 1U; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  BOOST_TEST(!maximum_exceeded.load(std::memory_order_acquire));
  BOOST_TEST(driver.ConnectionCount("node-1") == 1U);
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_restart_preserves_unrelated_disconnect) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1));
  controller.DisconnectPeer("node-1", "node-2", std::chrono::seconds(1));
  controller.RequestTopologyRestore("node-3");
  controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return driver.IsConnected("node-1", "10.0.0.3:18002"); }));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_older_restore_preserves_later_disconnect) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1));
  controller.RequestTopologyRestore("node-1");
  driver.BlockDisconnectedWaitFor("node-1");
  std::jthread disconnect([&] {
    controller.DisconnectPeer("node-1", "node-2", std::chrono::seconds(1));
  });
  BOOST_REQUIRE(WaitFor([&] { return driver.DisconnectedWaitBlocked(); }));

  controller.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  driver.ReleaseDisconnectedWait();
  disconnect.join();
  BOOST_REQUIRE(
      WaitFor([&] { return driver.IsConnected("node-1", "10.0.0.3:18002"); }));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_rejects_stale_policy_snapshot) {
  TestChainDriver driver;
  const std::vector<bbp::ChainNodeConfig> nodes = TestNodes();
  driver.ConnectPeer(nodes[0], "10.0.0.2:18001", {});
  std::atomic<bool> policy_blocked = false;
  std::atomic<bool> release_policy = false;
  std::atomic<bool> first_node_one_check = true;
  std::atomic<std::uint32_t> stale_actions = 0U;
  bbp::PeerConnectivityController controller(
      driver, nodes, {{"node-1", bbp::PeerCountPolicy(1U, 1U)}},
      FullAllowedPeers(), std::chrono::milliseconds(5),
      [&](std::string_view node_id) {
        if (node_id == "node-1" &&
            first_node_one_check.exchange(false, std::memory_order_acq_rel)) {
          policy_blocked.store(true, std::memory_order_release);
          while (!release_policy.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
        }
        return true;
      },
      [&](std::string_view node_id, std::string_view peer_node_id,
          bbp::PeerConnectivityAction, const bbp::PeerCountPolicy&) {
        if (node_id == "node-1" && peer_node_id == "node-2") {
          stale_actions.fetch_add(1U, std::memory_order_relaxed);
        }
      },
      [](std::string_view, std::string_view) {});

  controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return policy_blocked.load(std::memory_order_acquire); }));
  controller.SetAllowedPeers("node-1", {"node-3"});
  controller.DisconnectPeer("node-1", "node-2", std::chrono::seconds(1));
  release_policy.store(true, std::memory_order_release);

  BOOST_REQUIRE(
      WaitFor([&] { return driver.IsConnected("node-1", "10.0.0.3:18002"); }));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
  BOOST_TEST(stale_actions.load(std::memory_order_acquire) == 0U);
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_compensates_removed_queued_connection) {
  TestChainDriver driver;
  driver.BlockConnectedWaitFor("node-1");
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  std::atomic<bool> rejected = false;
  std::jthread mutation([&] {
    try {
      controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1));
    } catch (const std::runtime_error&) {
      rejected.store(true, std::memory_order_release);
    }
  });
  BOOST_REQUIRE(WaitFor([&] { return driver.ConnectedWaitBlocked(); }));
  controller.SetAllowedPeers("node-1", {"node-3"});
  driver.ReleaseConnectedWait();
  mutation.join();

  BOOST_TEST(rejected.load(std::memory_order_acquire));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_restoration_does_not_trap_cancelled_mutation) {
  TestChainDriver driver;
  driver.BlockConnectedWaitFor("node-1");
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.RequestTopologyRestore("node-1");
  controller.Start();
  BOOST_REQUIRE(WaitFor([&] { return driver.ConnectedWaitBlocked(); }));

  std::stop_source stop_source;
  std::atomic<bool> completed = false;
  std::atomic<bool> cancelled = false;
  const auto started_at = std::chrono::steady_clock::now();
  std::jthread mutation([&] {
    try {
      controller.DisconnectPeer("node-3", "node-2", std::chrono::seconds(1),
                                stop_source.get_token());
    } catch (const bbp::SimulationCancelled&) {
      cancelled.store(true, std::memory_order_release);
    }
    completed.store(true, std::memory_order_release);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  stop_source.request_stop();
  BOOST_REQUIRE(
      WaitFor([&] { return completed.load(std::memory_order_acquire); }));
  BOOST_TEST(cancelled.load(std::memory_order_acquire));
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  BOOST_TEST(elapsed_ms < 250);
  BOOST_TEST(!driver.IsConnected("node-3", "10.0.0.2:18001"));

  driver.ReleaseConnectedWait();
  mutation.join();
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_bounds_cancelled_mutation_rollback) {
  TestChainDriver driver;
  driver.BlockConnectedWaitFor("node-1");
  driver.BlockDisconnectedWaitFor("node-1");
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  std::stop_source stop_source;
  std::atomic<bool> outcome_unconfirmed = false;
  const auto started_at = std::chrono::steady_clock::now();
  std::jthread mutation([&] {
    try {
      controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(10),
                             stop_source.get_token());
    } catch (const bbp::PeerMutationOutcomeUnconfirmed&) {
      outcome_unconfirmed.store(true, std::memory_order_release);
    }
  });
  BOOST_REQUIRE(WaitFor([&] { return driver.ConnectedWaitBlocked(); }));
  stop_source.request_stop();
  BOOST_REQUIRE(WaitFor([&] { return driver.DisconnectedWaitBlocked(); }));
  mutation.join();
  const auto elapsed = std::chrono::steady_clock::now() - started_at;
  BOOST_TEST(outcome_unconfirmed.load(std::memory_order_acquire));
  BOOST_TEST(elapsed < std::chrono::milliseconds(500));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
  driver.ReleaseConnectedWait();
  driver.ReleaseDisconnectedWait();
}

BOOST_AUTO_TEST_CASE(peer_connectivity_controller_rejects_impossible_policy) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  BOOST_CHECK_THROW(
      controller.SetPolicy("node-1", bbp::PeerCountPolicy(0U, 3U)),
      std::runtime_error);
  BOOST_CHECK_THROW(
      controller.SetPolicy("missing", bbp::PeerCountPolicy(0U, 1U)),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_enforces_logical_peer_eligibility) {
  TestChainDriver driver;
  bbp::PeerConnectivityController::AllowedPeerMap allowed = {
      {"node-1", {"node-3"}},
      {"node-2", {"node-1"}},
      {"node-3", {"node-1"}},
  };
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {{"node-1", bbp::PeerCountPolicy(1U, 1U)}}, allowed,
      std::chrono::milliseconds(5), [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  controller.Start();
  BOOST_REQUIRE(
      WaitFor([&] { return driver.ConnectionCount("node-1") == 1U; }));
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.3:18002"));
  BOOST_CHECK_THROW(
      controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1)),
      std::runtime_error);

  controller.SetAllowedPeers("node-1", {"node-2"});
  controller.DisconnectPeer("node-1", "node-3", std::chrono::seconds(1));
  controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1));
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.2:18001"));
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.3:18002"));
  controller.Stop();
}

BOOST_AUTO_TEST_CASE(peer_connectivity_controller_validates_allowed_peer_sets) {
  TestChainDriver driver;
  BOOST_CHECK_THROW(
      bbp::PeerConnectivityController(
          driver, TestNodes(), {}, {{"node-1", {}}},
          std::chrono::milliseconds(5), [](std::string_view) { return true; },
          [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
             const bbp::PeerCountPolicy&) {},
          [](std::string_view, std::string_view) {}),
      std::runtime_error);

  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});
  BOOST_CHECK_THROW(controller.SetAllowedPeers("node-1", {"node-1"}),
                    std::runtime_error);
  BOOST_CHECK_THROW(controller.SetAllowedPeers("node-1", {"node-2", "node-2"}),
                    std::runtime_error);
  BOOST_CHECK_THROW(controller.SetAllowedPeers("node-1", {"missing"}),
                    std::runtime_error);
  BOOST_TEST(controller.AllowedPeersFor("node-1") ==
                 std::vector<std::string>({"node-2", "node-3"}),
             boost::test_tools::per_element());
  controller.SetPolicy("node-1", bbp::PeerCountPolicy(2U, 2U));
  BOOST_CHECK_THROW(controller.ValidateAllowedPeerUpdate("node-1", {"node-2"}),
                    std::runtime_error);
  BOOST_CHECK_THROW(controller.SetAllowedPeers("node-1", {"node-2"}),
                    std::runtime_error);
  BOOST_TEST(controller.AllowedPeersFor("node-1") ==
                 std::vector<std::string>({"node-2", "node-3"}),
             boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_rejects_contradictory_peer_state) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  driver.IgnoreNextConnect();
  BOOST_CHECK_EXCEPTION(
      controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1)),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what()).find("did not reach connected") !=
               std::string::npos;
      });
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_restores_disconnected_state_after_failure) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  driver.FailNextConnectedWait("original connection postcondition failure");
  BOOST_CHECK_EXCEPTION(
      controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1)),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "original connection postcondition failure";
      });
  BOOST_TEST(!driver.IsConnected("node-1", "10.0.0.2:18001"));
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_restores_connected_state_after_failure) {
  TestChainDriver driver;
  const std::vector<bbp::ChainNodeConfig> nodes = TestNodes();
  driver.ConnectPeer(nodes[0], "10.0.0.2:18001", {});
  bbp::PeerConnectivityController controller(
      driver, nodes, {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  driver.FailNextDisconnectedWait(
      "original disconnection postcondition failure");
  BOOST_CHECK_EXCEPTION(
      controller.DisconnectPeer("node-1", "node-2", std::chrono::seconds(1)),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "original disconnection postcondition failure";
      });
  BOOST_TEST(driver.IsConnected("node-1", "10.0.0.2:18001"));
}

BOOST_AUTO_TEST_CASE(
    peer_connectivity_controller_reports_original_and_rollback_failures) {
  TestChainDriver driver;
  bbp::PeerConnectivityController controller(
      driver, TestNodes(), {}, FullAllowedPeers(), std::chrono::milliseconds(5),
      [](std::string_view) { return true; },
      [](std::string_view, std::string_view, bbp::PeerConnectivityAction,
         const bbp::PeerCountPolicy&) {},
      [](std::string_view, std::string_view) {});

  driver.FailNextConnectedWait("original connection failure");
  driver.FailNextDisconnect("rollback disconnect failure");
  BOOST_CHECK_EXCEPTION(
      controller.ConnectPeer("node-1", "node-2", std::chrono::seconds(1)),
      bbp::PeerMutationOutcomeUnconfirmed,
      [](const bbp::PeerMutationOutcomeUnconfirmed& error) {
        const std::string message = error.what();
        return message.find("original connection failure") !=
                   std::string::npos &&
               message.find("peer connection rollback failed") !=
                   std::string::npos &&
               message.find("rollback disconnect failure") != std::string::npos;
      });
}
