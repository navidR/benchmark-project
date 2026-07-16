#include <linux/if_ether.h>
#include <linux/pkt_sched.h>
#include <netinet/in.h>

#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <limits>
#include <string>
#include <vector>

#include "bbp/network.h"

BOOST_AUTO_TEST_CASE(rtnetlink_lists_loopback_with_libmnl) {
  const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();

  BOOST_TEST(!links.empty());
  const auto loopback =
      std::find_if(links.begin(), links.end(),
                   [](const bbp::LinkInfo& link) { return link.name == "lo"; });
  BOOST_REQUIRE(loopback != links.end());
  BOOST_TEST(loopback->index > 0);
  BOOST_TEST(loopback->has_stats);
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_ipv4_loopback_with_libmnl) {
  const std::vector<bbp::AddressInfo> addresses = bbp::ListIpv4Addresses();

  BOOST_TEST(!addresses.empty());
  const auto loopback = std::find_if(
      addresses.begin(), addresses.end(), [](const bbp::AddressInfo& address) {
        return address.if_name == "lo" && address.address == "127.0.0.1";
      });
  BOOST_REQUIRE(loopback != addresses.end());
  BOOST_TEST(loopback->if_index > 0);
  BOOST_TEST(loopback->prefix_len == 8U);
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_ipv4_routes_with_libmnl) {
  const std::vector<bbp::RouteInfo> routes = bbp::ListIpv4Routes();

  BOOST_TEST(!routes.empty());
  const auto valid_route = std::find_if(
      routes.begin(), routes.end(), [](const bbp::RouteInfo& route) {
        return !route.destination.empty() && route.prefix_len <= 32U;
      });
  BOOST_REQUIRE(valid_route != routes.end());
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_qdiscs_with_libmnl) {
  const std::vector<bbp::QdiscInfo> qdiscs = bbp::ListQdiscs();

  BOOST_TEST(!qdiscs.empty());
  const auto parsed_qdisc = std::find_if(
      qdiscs.begin(), qdiscs.end(), [](const bbp::QdiscInfo& qdisc) {
        return qdisc.if_index > 0 && !qdisc.kernel_kind.empty();
      });
  BOOST_REQUIRE(parsed_qdisc != qdiscs.end());
}

BOOST_AUTO_TEST_CASE(qdisc_summary_matches_combined_tbf_netem_condition) {
  bbp::NetworkCondition condition;
  condition.bandwidth_mbps = 20;
  condition.delay_ms = 40;
  condition.jitter_ms = 5;
  condition.loss_basis_points = 10;
  condition.duplicate_basis_points = 5;
  condition.corrupt_basis_points = 5;
  condition.reorder_basis_points = 10;
  condition.limit_packets = 1000;

  bbp::QdiscInfo tbf;
  tbf.if_name = "veth0";
  tbf.kind = bbp::QdiscKind::kTbf;
  tbf.kernel_kind = "tbf";
  tbf.handle = TC_H_MAKE(1U << 16, 0U);
  tbf.parent = TC_H_ROOT;
  tbf.has_tbf_options = true;
  tbf.tbf_rate_bytes_per_sec = 2500000;
  tbf.tbf_limit_bytes = 250000;

  bbp::QdiscInfo netem;
  netem.if_name = "veth0";
  netem.kind = bbp::QdiscKind::kNetem;
  netem.kernel_kind = "netem";
  netem.handle = TC_H_MAKE(2U << 16, 0U);
  netem.parent = TC_H_MAKE(1U << 16, 1U);
  netem.has_netem_options = true;
  netem.netem_latency_us = 40000;
  netem.netem_jitter_us = 5000;
  netem.netem_loss = 4294967;
  netem.netem_duplicate = 2147483;
  netem.netem_corrupt = 2147483;
  netem.netem_reorder = 4294967;
  netem.netem_limit_packets = 1000;

  const std::vector<bbp::QdiscInfo> qdiscs = {tbf, netem};
  bbp::QdiscInfo summary;

  BOOST_TEST(
      bbp::QdiscsMatchNetworkCondition(qdiscs, "veth0", condition, &summary));
  BOOST_CHECK(summary.kind == bbp::QdiscKind::kTbfNetem);
  BOOST_TEST(summary.has_tbf_options);
  BOOST_TEST(summary.has_netem_options);
  BOOST_TEST(summary.tbf_rate_bytes_per_sec == 2500000U);
  BOOST_TEST(summary.netem_latency_us == 40000U);
  BOOST_TEST(bbp::QdiscMatchesNetworkCondition(summary, condition));
}

BOOST_AUTO_TEST_CASE(qdisc_summary_rejects_missing_child_netem) {
  bbp::NetworkCondition condition;
  condition.bandwidth_mbps = 20;
  condition.delay_ms = 40;

  bbp::QdiscInfo tbf;
  tbf.if_name = "veth0";
  tbf.kind = bbp::QdiscKind::kTbf;
  tbf.kernel_kind = "tbf";
  tbf.handle = TC_H_MAKE(1U << 16, 0U);
  tbf.parent = TC_H_ROOT;
  tbf.has_tbf_options = true;
  tbf.tbf_rate_bytes_per_sec = 2500000;
  tbf.tbf_limit_bytes = 250000;

  bbp::QdiscInfo summary;
  BOOST_TEST(
      !bbp::QdiscsMatchNetworkCondition({tbf}, "veth0", condition, &summary));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_matches_egress_ipv4_tcp_drop) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18168, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_matches_source_aware_tcp_drop) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_src = true;
  filter.ipv4_src = "198.51.100.11";
  filter.has_ipv4_src_mask = true;
  filter.ipv4_src_mask = "255.255.255.255";
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.11", "198.51.100.7", 18168, 1001));
  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.12", "198.51.100.7", 18168, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_rejects_source_filter_for_dst_only) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_src = true;
  filter.ipv4_src = "198.51.100.11";
  filter.has_ipv4_src_mask = true;
  filter.ipv4_src_mask = "255.255.255.255";
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18168, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_rejects_wrong_port) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18169, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_policy_summary_aggregates_owned_drop_counters) {
  bbp::TcFilterInfo first;
  first.if_name = "veth0";
  first.kind = bbp::TcFilterKind::kFlower;
  first.handle = 1001;
  first.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  first.protocol = ETH_P_IP;
  first.egress = true;
  first.has_eth_type = true;
  first.eth_type = ETH_P_IP;
  first.has_ip_proto = true;
  first.ip_proto = IPPROTO_TCP;
  first.has_ipv4_dst = true;
  first.ipv4_dst = "198.51.100.7";
  first.has_ipv4_dst_mask = true;
  first.ipv4_dst_mask = "255.255.255.255";
  first.has_tcp_dst = true;
  first.tcp_dst = 18168;
  first.has_tcp_dst_mask = true;
  first.tcp_dst_mask = 0xFFFFU;
  first.has_drop_action = true;
  first.has_stats = true;
  first.match_bytes = 100;
  first.match_packets = 2;
  first.drop_packets = 2;

  bbp::TcFilterInfo second = first;
  second.handle = 1002;
  second.match_bytes = 250;
  second.match_packets = 3;
  second.drop_packets = 3;
  bbp::TcFilterInfo unrelated = first;
  unrelated.if_name = "veth1";

  const bbp::TcFilterStatsSummary summary =
      bbp::SummarizeEgressIpv4TcpDropPolicies({first, second, unrelated},
                                              "veth0");
  BOOST_TEST(summary.policy_count == 2U);
  BOOST_TEST(summary.policies_with_stats == 2U);
  BOOST_TEST(summary.match_bytes == 350U);
  BOOST_TEST(summary.match_packets == 5U);
  BOOST_TEST(summary.drop_packets == 5U);
}

BOOST_AUTO_TEST_CASE(tc_filter_policy_summary_rejects_counter_overflow) {
  bbp::TcFilterInfo first;
  first.if_name = "veth0";
  first.kind = bbp::TcFilterKind::kFlower;
  first.handle = 1001;
  first.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  first.protocol = ETH_P_IP;
  first.egress = true;
  first.has_eth_type = true;
  first.eth_type = ETH_P_IP;
  first.has_ip_proto = true;
  first.ip_proto = IPPROTO_TCP;
  first.has_ipv4_dst = true;
  first.has_ipv4_dst_mask = true;
  first.ipv4_dst_mask = "255.255.255.255";
  first.has_tcp_dst = true;
  first.has_tcp_dst_mask = true;
  first.tcp_dst_mask = 0xFFFFU;
  first.has_drop_action = true;
  first.has_stats = true;
  first.match_bytes = std::numeric_limits<std::uint64_t>::max();
  bbp::TcFilterInfo second = first;
  second.handle = 1002;
  second.match_bytes = 1;

  BOOST_CHECK_THROW(
      bbp::SummarizeEgressIpv4TcpDropPolicies({first, second}, "veth0"),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(tc_filter_policy_requires_exact_masks) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_dst = true;
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.0";
  filter.has_tcp_dst = true;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(!bbp::TcFilterIsEgressIpv4TcpDropPolicy(filter, "veth0"));
}
