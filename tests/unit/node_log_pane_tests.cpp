#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <string_view>
#include <utility>

#include "benchmark_sim/node_log_pane.h"

namespace {

boost::json::object MakeReport(std::string_view source, std::string_view text) {
  boost::json::object detail;
  detail["text"] = text;
  boost::json::object tails;
  tails[source] = std::move(detail);
  boost::json::object node;
  node["node_id"] = "firo-1";
  node["log_tails"] = std::move(tails);
  boost::json::array nodes;
  nodes.push_back(std::move(node));
  boost::json::object report;
  report["nodes_summary"] = std::move(nodes);
  return report;
}

}  // namespace

BOOST_AUTO_TEST_CASE(node_log_pane_is_closed_by_default_and_toggles) {
  const boost::json::object report =
      MakeReport("daemon_log", "first\nsecond\nthird\n");
  bsim::NodeLogPane pane;

  BOOST_TEST(!pane.IsOpen());
  pane.Toggle(report, 0U);
  BOOST_TEST(pane.IsOpen());
  BOOST_TEST(pane.NodeId() == "firo-1");
  BOOST_TEST(pane.SourceName() == "daemon_log");
  BOOST_REQUIRE_EQUAL(pane.Lines().size(), 3U);
  BOOST_TEST(pane.Lines()[1] == "second");

  pane.Toggle(report, 0U);
  BOOST_TEST(!pane.IsOpen());
}

BOOST_AUTO_TEST_CASE(node_log_pane_scrolls_and_returns_to_live_tail) {
  const boost::json::object report =
      MakeReport("daemon_log", "one\ntwo\nthree\nfour\n");
  bsim::NodeLogPane pane;
  pane.Toggle(report, 0U);

  BOOST_TEST(pane.FirstVisibleLine(2U) == 2U);
  BOOST_TEST(pane.LastVisibleLine(2U) == 4U);
  pane.ScrollUp(2U, 1U);
  BOOST_TEST(pane.FirstVisibleLine(2U) == 1U);
  BOOST_TEST(pane.LastVisibleLine(2U) == 3U);
  pane.ScrollHome(2U);
  BOOST_TEST(pane.FirstVisibleLine(2U) == 0U);
  pane.ScrollEnd();
  BOOST_TEST(pane.FirstVisibleLine(2U) == 2U);
}

BOOST_AUTO_TEST_CASE(node_log_pane_falls_back_to_stderr) {
  const boost::json::object report = MakeReport("stderr", "fatal error\n");
  bsim::NodeLogPane pane;
  pane.Toggle(report, 0U);

  BOOST_TEST(pane.SourceName() == "stderr");
  BOOST_REQUIRE_EQUAL(pane.Lines().size(), 1U);
  BOOST_TEST(pane.Lines().front() == "fatal error");
}

BOOST_AUTO_TEST_CASE(node_log_pane_does_not_follow_when_scrolled_up) {
  bsim::NodeLogPane pane;
  pane.Toggle(MakeReport("daemon_log", "one\ntwo\nthree\nfour\n"), 0U);
  pane.ScrollUp(2U, 1U);
  BOOST_TEST(pane.FirstVisibleLine(2U) == 1U);

  pane.Refresh(MakeReport("daemon_log", "one\ntwo\nthree\nfour\nfive\n"), 0U);
  BOOST_TEST(pane.FirstVisibleLine(2U) == 1U);
  BOOST_TEST(pane.LastVisibleLine(2U) == 3U);

  pane.ScrollEnd();
  pane.Refresh(MakeReport("daemon_log", "one\ntwo\nthree\nfour\nfive\nsix\n"),
               0U);
  BOOST_TEST(pane.FirstVisibleLine(2U) == 4U);
  BOOST_TEST(pane.LastVisibleLine(2U) == 6U);
}
