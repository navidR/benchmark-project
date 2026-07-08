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

BOOST_AUTO_TEST_CASE(rtnetlink_lists_ipv4_loopback_with_libmnl) {
  const std::vector<bsim::AddressInfo> addresses = bsim::ListIpv4Addresses();

  BOOST_TEST(!addresses.empty());
  const auto loopback = std::find_if(
      addresses.begin(), addresses.end(), [](const bsim::AddressInfo& address) {
        return address.if_name == "lo" && address.address == "127.0.0.1";
      });
  BOOST_REQUIRE(loopback != addresses.end());
  BOOST_TEST(loopback->if_index > 0);
  BOOST_TEST(loopback->prefix_len == 8U);
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_ipv4_routes_with_libmnl) {
  const std::vector<bsim::RouteInfo> routes = bsim::ListIpv4Routes();

  BOOST_TEST(!routes.empty());
  const auto valid_route =
      std::find_if(routes.begin(), routes.end(),
                   [](const bsim::RouteInfo& route) {
                     return !route.destination.empty() &&
                            route.prefix_len <= 32U;
                   });
  BOOST_REQUIRE(valid_route != routes.end());
}
