#include <linux/capability.h>

#include <boost/test/unit_test.hpp>

#include "benchmark_sim/capability.h"

BOOST_AUTO_TEST_CASE(capability_parser_reads_effective_mask) {
  const bsim::Result<uint64_t> mask_result = bsim::ParseEffectiveCapabilities(
      "Name:\tbenchmark-sim\n"
      "State:\tR (running)\n"
      "Uid:\t0\t0\t0\t0\n"
      "CapEff:\t0000000000201000\n");
  BOOST_REQUIRE(mask_result.has_value());
  const uint64_t mask = mask_result.unsafe_value();

  BOOST_TEST(bsim::HasCapability(mask, CAP_NET_ADMIN));
  BOOST_TEST(bsim::HasCapability(mask, CAP_SYS_ADMIN));
  BOOST_TEST(!bsim::HasCapability(mask, CAP_SYS_RESOURCE));
}

BOOST_AUTO_TEST_CASE(capability_parser_rejects_missing_effective_mask) {
  const bsim::Result<uint64_t> mask_result =
      bsim::ParseEffectiveCapabilities("Name:\ttest\n");

  BOOST_REQUIRE(!mask_result.has_value());
  BOOST_TEST(mask_result.error() == "missing CapEff in /proc/self/status");
}
