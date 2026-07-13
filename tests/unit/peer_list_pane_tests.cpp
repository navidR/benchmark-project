#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <string_view>
#include <utility>

#include "bbp/peer_list_pane.h"

namespace {

boost::json::object MakeReport(
    std::initializer_list<std::string_view> peer_addresses) {
  boost::json::array addresses;
  for (std::string_view address : peer_addresses) {
    addresses.emplace_back(address);
  }
  boost::json::object metrics;
  metrics["peer_count"] = peer_addresses.size();
  metrics["peer_addresses"] = std::move(addresses);
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

BOOST_AUTO_TEST_CASE(peer_list_pane_loads_selected_node_peers) {
  bbp::PeerListPane pane;
  pane.Toggle(MakeReport({"10.0.0.2:18168", "10.0.0.3:18168"}), 0U);

  BOOST_TEST(pane.IsOpen());
  BOOST_TEST(pane.NodeId() == "firo-1");
  BOOST_TEST(pane.PeerCount() == 2U);
  BOOST_REQUIRE_EQUAL(pane.Peers().size(), 2U);
  BOOST_TEST(pane.Peers()[1] == "10.0.0.3:18168");
}

BOOST_AUTO_TEST_CASE(peer_list_pane_scrolls_peer_addresses) {
  bbp::PeerListPane pane;
  pane.Toggle(MakeReport({"peer-1", "peer-2", "peer-3", "peer-4"}), 0U);

  BOOST_TEST(pane.FirstVisiblePeer(2U) == 0U);
  BOOST_TEST(pane.LastVisiblePeer(2U) == 2U);
  pane.ScrollDown(2U, 1U);
  BOOST_TEST(pane.FirstVisiblePeer(2U) == 1U);
  pane.ScrollEnd(2U);
  BOOST_TEST(pane.FirstVisiblePeer(2U) == 2U);
  pane.ScrollHome();
  BOOST_TEST(pane.FirstVisiblePeer(2U) == 0U);
}

BOOST_AUTO_TEST_CASE(peer_list_pane_refreshes_live_peer_set) {
  bbp::PeerListPane pane;
  pane.Toggle(MakeReport({"peer-1"}), 0U);
  pane.Refresh(MakeReport({"peer-1", "peer-2"}), 0U);

  BOOST_TEST(pane.PeerCount() == 2U);
  BOOST_REQUIRE_EQUAL(pane.Peers().size(), 2U);
}
