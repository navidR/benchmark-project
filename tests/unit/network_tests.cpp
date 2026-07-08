#include "benchmark_sim/network.h"

#include <algorithm>
#include <string>
#include <vector>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(rtnetlink_lists_loopback_with_libmnl) {
  const std::vector<bsim::LinkInfo> links = bsim::ListNetworkLinks();

  BOOST_TEST(!links.empty());
  const auto loopback =
      std::find_if(links.begin(), links.end(),
                   [](const bsim::LinkInfo& link) { return link.name == "lo"; });
  BOOST_REQUIRE(loopback != links.end());
  BOOST_TEST(loopback->index > 0);
}
