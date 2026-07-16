#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <limits>

#include "bbp/network_rule_pane.h"

namespace {

boost::json::object MakeReport(std::uint32_t rule_count) {
  boost::json::array rules;
  for (std::uint32_t index = 0; index < rule_count; ++index) {
    boost::json::object rule;
    rule["handle"] = index + 1U;
    if (index == 0U) {
      rule["src_address"] = nullptr;
    } else {
      rule["src_address"] = "10.0.0.1";
    }
    rule["dst_address"] = "10.0.0.2";
    rule["dst_port"] = 18168U;
    rule["match_packets"] = index + 2U;
    rule["drop_packets"] = index + 3U;
    rules.push_back(std::move(rule));
  }
  boost::json::object metrics;
  metrics["network_active_block_rules"] = std::move(rules);
  boost::json::object node;
  node["node_id"] = "firo-1";
  node["last_metrics"] = std::move(metrics);
  boost::json::array nodes;
  nodes.push_back(std::move(node));
  boost::json::object report;
  report["nodes_summary"] = std::move(nodes);
  return report;
}

}  // namespace

BOOST_AUTO_TEST_CASE(network_rule_pane_loads_typed_active_rules) {
  bbp::NetworkRulePane pane;
  pane.Toggle(MakeReport(2U), 0U);

  BOOST_TEST(pane.IsOpen());
  BOOST_TEST(pane.NodeId() == "firo-1");
  BOOST_REQUIRE_EQUAL(pane.Rules().size(), 2U);
  BOOST_TEST(pane.Rules()[0].handle == 1U);
  BOOST_TEST(pane.Rules()[0].source_address.empty());
  BOOST_TEST(pane.Rules()[1].source_address == "10.0.0.1");
  BOOST_TEST(pane.Rules()[1].destination_address == "10.0.0.2");
  BOOST_TEST(pane.Rules()[1].destination_port == 18168U);
  BOOST_TEST(pane.Rules()[1].drop_packets == 4U);
}

BOOST_AUTO_TEST_CASE(network_rule_pane_scrolls_and_refreshes) {
  bbp::NetworkRulePane pane;
  pane.Toggle(MakeReport(4U), 0U);
  pane.ScrollDown(2U, 1U);
  BOOST_TEST(pane.FirstVisibleRule(2U) == 1U);
  pane.ScrollDown(2U, std::numeric_limits<std::size_t>::max());
  BOOST_TEST(pane.FirstVisibleRule(2U) == 2U);
  pane.ScrollEnd(2U);
  BOOST_TEST(pane.FirstVisibleRule(2U) == 2U);
  pane.Refresh(MakeReport(1U), 0U);
  BOOST_TEST(pane.FirstVisibleRule(2U) == 0U);
  BOOST_REQUIRE_EQUAL(pane.Rules().size(), 1U);
}
