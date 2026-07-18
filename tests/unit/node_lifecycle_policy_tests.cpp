#include <sys/wait.h>

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <optional>

#include "bbp/node_lifecycle_policy.h"

namespace {

int ExitedStatus(int exit_code) { return exit_code << 8; }

}  // namespace

BOOST_AUTO_TEST_CASE(node_restart_policy_names_round_trip) {
  constexpr bbp::NodeRestartPolicy kPolicies[] = {
      bbp::NodeRestartPolicy::kNever,
      bbp::NodeRestartPolicy::kOnFailure,
      bbp::NodeRestartPolicy::kAlways,
  };
  for (const bbp::NodeRestartPolicy policy : kPolicies) {
    const std::optional<bbp::NodeRestartPolicy> parsed =
        bbp::NodeRestartPolicyFromName(bbp::NodeRestartPolicyName(policy));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == policy);
  }
  BOOST_TEST(!bbp::NodeRestartPolicyFromName("sometimes"));
}

BOOST_AUTO_TEST_CASE(node_lifecycle_policy_validates_order_and_duration) {
  bbp::ValidateNodeLifecyclePolicy(
      {.start_time = std::chrono::milliseconds(100),
       .stop_time = std::chrono::milliseconds(200),
       .restart_policy = bbp::NodeRestartPolicy::kOnFailure},
      std::chrono::milliseconds(300));
  bbp::ValidateNodeLifecyclePolicy(
      {.start_time = std::nullopt,
       .stop_time = std::chrono::milliseconds(200),
       .restart_policy = bbp::NodeRestartPolicy::kNever},
      std::nullopt);

  BOOST_CHECK_THROW(bbp::ValidateNodeLifecyclePolicy(
                        {.start_time = std::chrono::milliseconds(200),
                         .stop_time = std::chrono::milliseconds(200),
                         .restart_policy = bbp::NodeRestartPolicy::kNever},
                        std::nullopt),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ValidateNodeLifecyclePolicy(
                        {.start_time = std::chrono::milliseconds(300),
                         .stop_time = std::nullopt,
                         .restart_policy = bbp::NodeRestartPolicy::kNever},
                        std::chrono::milliseconds(300)),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ValidateNodeLifecyclePolicy(
                        {.start_time = std::nullopt,
                         .stop_time = std::chrono::milliseconds(300),
                         .restart_policy = bbp::NodeRestartPolicy::kNever},
                        std::chrono::milliseconds(300)),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(node_restart_policy_distinguishes_clean_and_failed_exit) {
  const int clean = ExitedStatus(0);
  const int failed = ExitedStatus(3);
  const int signaled = SIGKILL;

  BOOST_TEST(bbp::ProcessExitSucceeded(clean));
  BOOST_TEST(!bbp::ProcessExitSucceeded(failed));
  BOOST_TEST(!bbp::ProcessExitSucceeded(signaled));

  BOOST_TEST(!bbp::NodeRestartPolicyAllowsRestart(
      bbp::NodeRestartPolicy::kNever, failed));
  BOOST_TEST(!bbp::NodeRestartPolicyAllowsRestart(
      bbp::NodeRestartPolicy::kOnFailure, clean));
  BOOST_TEST(bbp::NodeRestartPolicyAllowsRestart(
      bbp::NodeRestartPolicy::kOnFailure, failed));
  BOOST_TEST(bbp::NodeRestartPolicyAllowsRestart(
      bbp::NodeRestartPolicy::kOnFailure, signaled));
  BOOST_TEST(bbp::NodeRestartPolicyAllowsRestart(
      bbp::NodeRestartPolicy::kAlways, clean));
}
