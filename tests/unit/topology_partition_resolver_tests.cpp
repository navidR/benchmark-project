#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>

#include "bbp/topology_partition_resolver.h"

namespace {

boost::json::object ParseObject(std::string_view text) {
  return boost::json::parse(text).as_object();
}

}  // namespace

BOOST_AUTO_TEST_CASE(topology_partition_resolver_builds_typed_node_pair) {
  const bbp::SimulationPartition partition =
      bbp::MakeNodePairPartition("firo-1", "firo-2");
  BOOST_CHECK(partition.scope == bbp::SimulationPartitionScope::kNodePair);
  BOOST_REQUIRE_EQUAL(partition.group_a.group_ids.size(), 1U);
  BOOST_REQUIRE_EQUAL(partition.group_b.node_ids.size(), 1U);
  BOOST_TEST(partition.group_a.group_ids.front() == "firo-1");
  BOOST_TEST(partition.group_a.node_ids.front() == "firo-1");
  BOOST_TEST(partition.group_b.group_ids.front() == "firo-2");
  BOOST_TEST(partition.group_b.node_ids.front() == "firo-2");
  BOOST_TEST(bbp::SimulationPartitionTargetText(partition) ==
             "firo-1 vs firo-2");

  BOOST_CHECK_THROW(bbp::MakeNodePairPartition("firo-1", "firo-1"),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::MakeNodePairPartition("", "firo-1"),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    topology_partition_resolver_selects_group_against_same_kind_counterparts) {
  const boost::json::object report = ParseObject(
      R"({"topology_groups_summary":[)"
      R"({"group":"all","kind":"all","node_ids":["firo-1","firo-2","firo-3","firo-4"]},)"
      R"({"group":"topology-1","kind":"partition_group","node_ids":["firo-1","firo-2"]},)"
      R"({"group":"topology-2","kind":"partition_group","node_ids":["firo-3"]},)"
      R"({"group":"topology-3","kind":"partition_group","node_ids":["firo-4"]},)"
      R"({"group":"role-wallet","kind":"role","node_ids":["firo-1","firo-3"]})"
      R"(]})");

  const bbp::SimulationPartition partition =
      bbp::ResolveSelectedTopologyPartition(report, 1U);
  BOOST_CHECK(partition.scope ==
              bbp::SimulationPartitionScope::kPartitionGroup);
  BOOST_REQUIRE_EQUAL(partition.group_a.group_ids.size(), 1U);
  BOOST_TEST(partition.group_a.group_ids.front() == "topology-1");
  BOOST_REQUIRE_EQUAL(partition.group_a.node_ids.size(), 2U);
  BOOST_REQUIRE_EQUAL(partition.group_b.group_ids.size(), 2U);
  BOOST_TEST(partition.group_b.group_ids[0] == "topology-2");
  BOOST_TEST(partition.group_b.group_ids[1] == "topology-3");
  BOOST_REQUIRE_EQUAL(partition.group_b.node_ids.size(), 2U);
  BOOST_TEST(partition.group_b.node_ids[0] == "firo-3");
  BOOST_TEST(partition.group_b.node_ids[1] == "firo-4");
}

BOOST_AUTO_TEST_CASE(topology_partition_resolver_selects_role_counterpart) {
  const boost::json::object report = ParseObject(
      R"({"topology_groups_summary":[)"
      R"({"group":"all","kind":"all","node_ids":["firo-1","firo-2","firo-3"]},)"
      R"({"group":"role-wallet","kind":"role","node_ids":["firo-1","firo-2"]},)"
      R"({"group":"role-miner","kind":"role","node_ids":["firo-3"]})"
      R"(]})");
  const bbp::SimulationPartition partition =
      bbp::ResolveSelectedTopologyPartition(report, 1U);
  BOOST_CHECK(partition.scope == bbp::SimulationPartitionScope::kRole);
  BOOST_TEST(partition.group_b.group_ids.front() == "role-miner");
  BOOST_TEST(partition.group_b.node_ids.front() == "firo-3");
}

BOOST_AUTO_TEST_CASE(topology_partition_resolver_rejects_ambiguous_groups) {
  const boost::json::object all_only = ParseObject(
      R"({"topology_groups_summary":[{"group":"all","kind":"all","node_ids":["firo-1"]}]})");
  BOOST_CHECK_THROW(bbp::ResolveSelectedTopologyPartition(all_only, 0U),
                    std::runtime_error);

  const boost::json::object no_counterpart = ParseObject(
      R"({"topology_groups_summary":[{"group":"role-wallet","kind":"role","node_ids":["firo-1"]}]})");
  BOOST_CHECK_THROW(bbp::ResolveSelectedTopologyPartition(no_counterpart, 0U),
                    std::runtime_error);

  const boost::json::object overlap = ParseObject(
      R"({"topology_groups_summary":[)"
      R"({"group":"region-1","kind":"region","node_ids":["firo-1","firo-2"]},)"
      R"({"group":"region-2","kind":"region","node_ids":["firo-2","firo-3"]})"
      R"(]})");
  BOOST_CHECK_THROW(bbp::ResolveSelectedTopologyPartition(overlap, 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ResolveSelectedTopologyPartition(overlap, 1U),
                    std::runtime_error);
}
