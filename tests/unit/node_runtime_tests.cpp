#include <boost/test/unit_test.hpp>

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
