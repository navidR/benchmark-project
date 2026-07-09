#include "benchmark_sim/capability.h"

#include <linux/capability.h>

#include <stdexcept>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(capability_parser_reads_effective_mask) {
  const uint64_t mask = bsim::ParseEffectiveCapabilities(
      "Name:\tbenchmark-sim\n"
      "State:\tR (running)\n"
      "Uid:\t0\t0\t0\t0\n"
      "CapEff:\t0000000000201000\n");

  BOOST_TEST(bsim::HasCapability(mask, CAP_NET_ADMIN));
  BOOST_TEST(bsim::HasCapability(mask, CAP_SYS_ADMIN));
  BOOST_TEST(!bsim::HasCapability(mask, CAP_SYS_RESOURCE));
}

BOOST_AUTO_TEST_CASE(capability_parser_rejects_missing_effective_mask) {
  BOOST_CHECK_THROW(bsim::ParseEffectiveCapabilities("Name:\ttest\n"),
                    std::runtime_error);
}
