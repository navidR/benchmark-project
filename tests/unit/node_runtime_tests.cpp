#include <boost/test/unit_test.hpp>
#include <optional>

#include "bbp/simulator/node_runtime.h"

BOOST_AUTO_TEST_CASE(node_runtime_only_collects_chain_metrics_while_running) {
  bbp::NodeRuntime node;
  BOOST_CHECK(node.Lifecycle() == bbp::NodeRuntimeLifecycle::kStarting);
  BOOST_TEST(!node.AllowsChainMetrics());

  node.SetLifecycle(bbp::NodeRuntimeLifecycle::kRunning);
  BOOST_TEST(node.AllowsChainMetrics());

  node.SetLifecycle(bbp::NodeRuntimeLifecycle::kKilling);
  BOOST_TEST(!node.AllowsChainMetrics());

  node.SetLifecycle(bbp::NodeRuntimeLifecycle::kKilled);
  BOOST_TEST(!node.AllowsChainMetrics());
}

BOOST_AUTO_TEST_CASE(node_runtime_lifecycle_names_match_event_states) {
  constexpr bbp::NodeRuntimeLifecycle kStates[] = {
      bbp::NodeRuntimeLifecycle::kPreparing,
      bbp::NodeRuntimeLifecycle::kStarting,
      bbp::NodeRuntimeLifecycle::kNetworkNamespaceReady,
      bbp::NodeRuntimeLifecycle::kCgroupReady,
      bbp::NodeRuntimeLifecycle::kRunning,
      bbp::NodeRuntimeLifecycle::kRestarting,
      bbp::NodeRuntimeLifecycle::kStopping,
      bbp::NodeRuntimeLifecycle::kStopped,
      bbp::NodeRuntimeLifecycle::kCleaning,
      bbp::NodeRuntimeLifecycle::kCleaned,
      bbp::NodeRuntimeLifecycle::kFailed,
      bbp::NodeRuntimeLifecycle::kKilling,
      bbp::NodeRuntimeLifecycle::kKilled,
  };

  for (const bbp::NodeRuntimeLifecycle state : kStates) {
    const std::optional<bbp::NodeRuntimeLifecycle> parsed =
        bbp::ParseNodeRuntimeLifecycleName(
            bbp::NodeRuntimeLifecycleName(state));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == state);
  }
  BOOST_TEST(!bbp::ParseNodeRuntimeLifecycleName("UnknownState"));
}

BOOST_AUTO_TEST_CASE(node_runtime_counts_generated_blocks_and_transactions) {
  bbp::NodeRuntime node;
  node.AddGeneratedBlocks(2U);
  node.AddGeneratedBlocks(3U);
  node.AddMinedTransactions(4U);
  node.AddMinedTransactions(5U);

  BOOST_TEST(node.GeneratedBlockCount() == 5U);
  BOOST_TEST(node.MinedTransactionCount() == 9U);
  BOOST_TEST(node.MinedTransactionCountComplete());
  node.MarkMinedTransactionCountIncomplete();
  BOOST_TEST(!node.MinedTransactionCountComplete());
}
