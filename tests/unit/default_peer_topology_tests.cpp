#include <boost/test/unit_test.hpp>

#include "bbp/default_peer_topology.h"

BOOST_AUTO_TEST_CASE(default_peer_topology_connects_every_node_pair) {
  const std::vector<std::uint32_t> first =
      bbp::DefaultStartupPeerIndexes(4U, 0U);
  const std::vector<std::uint32_t> middle =
      bbp::DefaultStartupPeerIndexes(4U, 2U);

  BOOST_TEST(first == std::vector<std::uint32_t>({1U, 2U, 3U}),
             boost::test_tools::per_element());
  BOOST_TEST(middle == std::vector<std::uint32_t>({0U, 1U, 3U}),
             boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(default_peer_topology_handles_single_node) {
  BOOST_TEST(bbp::DefaultStartupPeerIndexes(1U, 0U).empty());
}

BOOST_AUTO_TEST_CASE(default_peer_topology_rejects_invalid_node) {
  BOOST_CHECK_THROW(bbp::DefaultStartupPeerIndexes(0U, 0U), std::runtime_error);
  BOOST_CHECK_THROW(bbp::DefaultStartupPeerIndexes(3U, 3U), std::runtime_error);
}
