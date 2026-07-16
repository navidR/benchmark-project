#include <linux/capability.h>

#include <boost/test/unit_test.hpp>
#include <stdexcept>

#include "bbp/capability.h"

BOOST_AUTO_TEST_CASE(capability_check_rejects_invalid_capability_numbers) {
  BOOST_TEST(!bbp::HasEffectiveCapability(-1));
  BOOST_TEST(!bbp::HasEffectiveCapability(CAP_LAST_CAP + 1));
}

BOOST_AUTO_TEST_CASE(capability_check_reads_current_process_capabilities) {
  BOOST_CHECK_NO_THROW(
      static_cast<void>(bbp::HasEffectiveCapability(CAP_NET_ADMIN)));
}

BOOST_AUTO_TEST_CASE(capability_requirement_rejects_invalid_capability) {
  BOOST_CHECK_THROW(
      bbp::RequireEffectiveCapability(CAP_LAST_CAP + 1, "invalid capability"),
      std::runtime_error);
}
