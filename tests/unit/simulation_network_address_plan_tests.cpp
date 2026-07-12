#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "bbp/simulation_network_address_plan.h"

BOOST_AUTO_TEST_CASE(simulation_network_plan_assigns_distinct_node_subnets) {
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.4.0/26", 16U);

  BOOST_TEST(plan.Cidr() == "10.210.4.0/26");
  BOOST_TEST(plan.HostAddress(0U) == "10.210.4.1");
  BOOST_TEST(plan.NodeAddress(0U) == "10.210.4.2");
  BOOST_TEST(plan.HostAddress(15U) == "10.210.4.61");
  BOOST_TEST(plan.NodeAddress(15U) == "10.210.4.62");
  BOOST_TEST(plan.NodePrefixLength() == 30U);
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_skips_overlapping_kernel_route) {
  const bbp::SimulationNetworkAddressPlan first =
      bbp::SimulationNetworkAddressPlan::Allocate("same-run", 8U, {});
  bbp::RouteInfo occupied;
  occupied.destination = first.NodeAddress(0U);
  occupied.prefix_len = 32U;

  const bbp::SimulationNetworkAddressPlan second =
      bbp::SimulationNetworkAddressPlan::Allocate("same-run", 8U, {occupied});

  BOOST_TEST(second.Cidr() != first.Cidr());
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_ignores_default_route) {
  bbp::RouteInfo default_route;
  default_route.destination = "0.0.0.0";
  default_route.prefix_len = 0U;

  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::Allocate("default-route", 1U,
                                                  {default_route});

  BOOST_TEST(!plan.Cidr().empty());
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_rejects_invalid_persisted_range) {
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("10.211.0.0/26", 1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.0.0/24", 1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.0.1/26", 1U),
      std::runtime_error);
}
