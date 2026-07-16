#include <boost/test/unit_test.hpp>
#include <chrono>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "bbp/peer_connectivity_controller.h"

namespace {

class TestChainDriver final : public bbp::ChainDriver {
 public:
  bbp::ProcessSpec RenderProcess(const bbp::ChainNodeConfig&) const override {
    return {};
  }
  std::optional<bbp::LogTailChunk> ReadLogTail(const bbp::ChainNodeConfig&,
                                               bbp::ChainLogSource,
                                               const bbp::LogTailCursor&,
                                               std::uint64_t) const override {
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
  void WaitForPeerAddress(const bbp::ChainNodeConfig&, const std::string&,
                          std::chrono::seconds,
                          std::stop_token) const override {}
  void WaitForPeerAddressAbsent(const bbp::ChainNodeConfig&, const std::string&,
                                std::chrono::seconds,
                                std::stop_token) const override {}
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
    connections_[config.id].insert(address);
  }
  void DisconnectPeer(const bbp::ChainNodeConfig& config,
                      const std::string& address,
                      std::stop_token) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    connections_[config.id].erase(address);
  }
  void ChangeLogVerbosity(const bbp::ChainNodeConfig&,
                          bbp::ChainLogVerbosityChange,
                          std::stop_token) const override {}
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

  bool IsConnected(const std::string& node_id,
                   const std::string& address) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = connections_.find(node_id);
    return found != connections_.end() && found->second.contains(address);
  }

 private:
  mutable std::mutex mutex_;
  mutable std::map<std::string, std::set<std::string>> connections_;
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
