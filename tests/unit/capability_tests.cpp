#include <sys/capability.h>

#include <boost/test/unit_test.hpp>
#include <stdexcept>

#include "benchmark_sim/capability.h"

BOOST_AUTO_TEST_CASE(capability_check_rejects_invalid_capability_numbers) {
  BOOST_TEST(!bsim::HasEffectiveCapability(-1));
  BOOST_TEST(!bsim::HasEffectiveCapability(cap_max_bits()));
}

BOOST_AUTO_TEST_CASE(capability_check_reads_current_process_capabilities) {
  BOOST_CHECK_NO_THROW(
      static_cast<void>(bsim::HasEffectiveCapability(CAP_NET_ADMIN)));
}

BOOST_AUTO_TEST_CASE(capability_requirement_rejects_invalid_capability) {
  BOOST_CHECK_THROW(
      bsim::RequireEffectiveCapability(cap_max_bits(), "invalid capability"),
      std::runtime_error);
}
