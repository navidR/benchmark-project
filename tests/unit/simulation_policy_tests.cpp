#include <boost/test/unit_test.hpp>

#include "bbp/simulation_policy.h"

BOOST_AUTO_TEST_CASE(simulation_policy_names_round_trip) {
  constexpr bbp::CleanupPolicy kCleanupPolicies[] = {
      bbp::CleanupPolicy::kAutomatic,
      bbp::CleanupPolicy::kRetainCgroups,
  };
  for (const bbp::CleanupPolicy policy : kCleanupPolicies) {
    const std::optional<bbp::CleanupPolicy> parsed =
        bbp::CleanupPolicyFromName(bbp::CleanupPolicyName(policy));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == policy);
  }

  const std::optional<bbp::PrivilegeMode> privilege =
      bbp::PrivilegeModeFromName(
          bbp::PrivilegeModeName(bbp::PrivilegeMode::kDirect));
  BOOST_REQUIRE(privilege);
  BOOST_CHECK(*privilege == bbp::PrivilegeMode::kDirect);

  const std::optional<bbp::LogRetentionPolicy> log_retention =
      bbp::LogRetentionPolicyFromName(
          bbp::LogRetentionPolicyName(bbp::LogRetentionPolicy::kPreserve));
  BOOST_REQUIRE(log_retention);
  BOOST_CHECK(*log_retention == bbp::LogRetentionPolicy::kPreserve);

  BOOST_TEST(!bbp::CleanupPolicyFromName("unknown"));
  BOOST_TEST(!bbp::PrivilegeModeFromName("helper"));
  BOOST_TEST(!bbp::LogRetentionPolicyFromName("delete"));
}
