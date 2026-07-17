#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>

#include "bbp/perf_counter_target_resolver.h"

namespace {

boost::json::object Report() {
  return boost::json::
      parse(R"({"nodes_summary":[{"node_id":"firo-1","node_index":1},{"node_id":"firo-2","node_index":2}],"wallets_summary":[{"wallet_index":1,"node":2}],"topology_groups_summary":[{"group":"all","node_ids":["firo-1","firo-2"]},{"group":"topology-1","node_ids":["firo-2"]}]})")
          .as_object();
}

}  // namespace

BOOST_AUTO_TEST_CASE(perf_counter_target_resolver_infers_selected_targets) {
  const boost::json::object report = Report();
  bbp::PerfCounterTarget target = bbp::ResolvePerfCounterTarget(
      report, {.view = bbp::TuiView::kNodes, .selected_node = 1U}, std::nullopt,
      std::nullopt);
  BOOST_CHECK(target.kind == bbp::PerfCounterTargetKind::kNode);
  BOOST_TEST(target.id == "firo-2");
  BOOST_REQUIRE_EQUAL(target.node_ids.size(), 1U);

  target = bbp::ResolvePerfCounterTarget(
      report, {.view = bbp::TuiView::kWallets, .selected_wallet = 0U},
      std::nullopt, std::nullopt);
  BOOST_CHECK(target.kind == bbp::PerfCounterTargetKind::kWallet);
  BOOST_TEST(target.id == "wallet-1");
  BOOST_TEST(target.node_ids.front() == "firo-2");

  target = bbp::ResolvePerfCounterTarget(
      report, {.view = bbp::TuiView::kTopology, .selected_topology_group = 1U},
      std::nullopt, std::nullopt);
  BOOST_CHECK(target.kind == bbp::PerfCounterTargetKind::kGroup);
  BOOST_TEST(target.id == "topology-1");
  BOOST_TEST(target.node_ids.front() == "firo-2");
}

BOOST_AUTO_TEST_CASE(perf_counter_target_resolver_resolves_typed_targets) {
  const boost::json::object report = Report();
  const bbp::PerfCounterSelectionContext selection;
  bbp::PerfCounterTarget target = bbp::ResolvePerfCounterTarget(
      report, selection, bbp::PerfCounterTargetKind::kCgroup,
      std::string("firo-2"));
  BOOST_CHECK(target.kind == bbp::PerfCounterTargetKind::kCgroup);
  BOOST_TEST(target.id == "firo-2");

  target = bbp::ResolvePerfCounterTarget(report, selection,
                                         bbp::PerfCounterTargetKind::kWallet,
                                         std::string("#1"));
  BOOST_TEST(target.id == "wallet-1");
  BOOST_TEST(target.node_ids.front() == "firo-2");

  target = bbp::ResolvePerfCounterTarget(report, selection,
                                         bbp::PerfCounterTargetKind::kGroup,
                                         std::string("all"));
  BOOST_REQUIRE_EQUAL(target.node_ids.size(), 2U);
  BOOST_TEST(target.node_ids[0] == "firo-1");
  BOOST_TEST(target.node_ids[1] == "firo-2");
}

BOOST_AUTO_TEST_CASE(perf_counter_target_resolver_rejects_invalid_targets) {
  const boost::json::object report = Report();
  BOOST_CHECK_THROW(bbp::ResolvePerfCounterTarget(
                        report, {.view = bbp::TuiView::kNodes},
                        bbp::PerfCounterTargetKind::kWallet, std::nullopt),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ResolvePerfCounterTarget(
                        report, {.view = bbp::TuiView::kTopology},
                        bbp::PerfCounterTargetKind::kNode, std::nullopt),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ResolvePerfCounterTarget(
                        report, {}, bbp::PerfCounterTargetKind::kNode,
                        std::string("missing")),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ResolvePerfCounterTarget(
          report, {}, bbp::PerfCounterTargetKind::kWallet, std::string("01")),
      std::runtime_error);

  boost::json::object malformed = Report();
  malformed["topology_groups_summary"] =
      boost::json::parse(R"([{"group":"bad","node_ids":["firo-1","firo-1"]}])");
  BOOST_CHECK_THROW(bbp::ResolvePerfCounterTarget(
                        malformed, {}, bbp::PerfCounterTargetKind::kGroup,
                        std::string("bad")),
                    std::runtime_error);

  malformed = Report();
  malformed["topology_groups_summary"] =
      boost::json::parse(R"([{"group":"bad","node_ids":["missing"]}])");
  BOOST_CHECK_THROW(bbp::ResolvePerfCounterTarget(
                        malformed, {}, bbp::PerfCounterTargetKind::kGroup,
                        std::string("bad")),
                    std::runtime_error);

  malformed = Report();
  malformed["nodes_summary"] = boost::json::parse(
      R"([{"node_id":"firo-1","node_index":1},{"node_id":"firo-1","node_index":2}])");
  BOOST_CHECK_THROW(bbp::ResolvePerfCounterTarget(
                        malformed, {}, bbp::PerfCounterTargetKind::kNode,
                        std::string("firo-1")),
                    std::runtime_error);

  malformed = Report();
  malformed["nodes_summary"] = boost::json::parse(
      R"([{"node_id":"firo-1","node_index":2},{"node_id":"firo-2","node_index":2}])");
  BOOST_CHECK_THROW(
      bbp::ResolvePerfCounterTarget(
          malformed, {}, bbp::PerfCounterTargetKind::kWallet, std::string("1")),
      std::runtime_error);

  malformed = Report();
  malformed["nodes_summary"] = boost::json::parse(
      R"([{"node_id":"firo-1","node_index":1},{"node_id":"firo-2","node_index":3}])");
  BOOST_CHECK_THROW(
      bbp::ResolvePerfCounterTarget(
          malformed, {}, bbp::PerfCounterTargetKind::kWallet, std::string("1")),
      std::runtime_error);

  malformed = Report();
  malformed["wallets_summary"] = boost::json::parse(
      R"([{"wallet_index":1,"node":1},{"wallet_index":1,"node":2}])");
  BOOST_CHECK_THROW(
      bbp::ResolvePerfCounterTarget(
          malformed, {}, bbp::PerfCounterTargetKind::kWallet, std::string("1")),
      std::runtime_error);
}
