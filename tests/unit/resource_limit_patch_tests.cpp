#include <boost/test/unit_test.hpp>
#include <utility>

#include "bbp/simulator/resource_limit_patch.h"

namespace {

bbp::ResourceLimits CurrentLimits() {
  return bbp::ResourceLimits{
      .memory_high_bytes = 1024U,
      .memory_max_bytes = 2048U,
      .cpu_quota_us = 50000U,
      .cpu_period_us = 100000U,
      .cpu_weight = 100U,
      .io_weight = 100U,
      .io_limits =
          {
              bbp::IoLimit{
                  .device = {.major = 8U, .minor = 0U},
                  .read_bytes_per_sec = 1000U,
                  .write_bytes_per_sec = 2000U,
                  .read_operations_per_sec = 30U,
                  .write_operations_per_sec = 40U,
              },
              bbp::IoLimit{
                  .device = {.major = 8U, .minor = 1U},
                  .read_bytes_per_sec = 5000U,
                  .write_bytes_per_sec = 6000U,
                  .read_operations_per_sec = 70U,
                  .write_operations_per_sec = 80U,
              },
          },
      .pids_max = 128U,
  };
}

bbp::IoLimit EmptyIoLimit(std::uint32_t major, std::uint32_t minor) {
  bbp::IoLimit limit;
  limit.device = {.major = major, .minor = minor};
  return limit;
}

bbp::ResourceLimitPatch IoPatch(std::vector<bbp::IoLimit> limits) {
  bbp::ResourceLimitPatch patch;
  patch.io_limits_present = true;
  patch.io_limits = std::move(limits);
  return patch;
}

}  // namespace

BOOST_AUTO_TEST_CASE(resource_limit_patch_validates_live_fields) {
  bbp::ResourceLimitPatch patch;
  patch.memory_high_bytes = 0U;
  bbp::ValidateResourceLimitPatch(patch);
  patch = {};
  patch.cpu_weight = 1U;
  bbp::ValidateResourceLimitPatch(patch);
  patch.cpu_weight = 10000U;
  bbp::ValidateResourceLimitPatch(patch);
  patch = {};
  patch.io_weight = 1U;
  bbp::ValidateResourceLimitPatch(patch);
  patch.io_weight = 10000U;
  bbp::ValidateResourceLimitPatch(patch);
  patch = {};
  patch.cpu_quota_present = true;
  patch.cpu_period_us = 100000U;
  bbp::ValidateResourceLimitPatch(patch);

  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(bbp::ResourceLimitPatch{}),
                    std::runtime_error);
  patch = {};
  patch.memory_max_bytes = 0U;
  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(patch), std::runtime_error);
  patch = {};
  patch.cpu_weight = 0U;
  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(patch), std::runtime_error);
  patch.cpu_weight = 10001U;
  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(patch), std::runtime_error);
  patch = {};
  patch.io_weight = 0U;
  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(patch), std::runtime_error);
  patch.io_weight = 10001U;
  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(patch), std::runtime_error);
  bbp::IoLimit zero_io = EmptyIoLimit(8U, 0U);
  zero_io.read_bytes_per_sec = 0U;
  BOOST_CHECK_THROW(bbp::ValidateResourceLimitPatch(IoPatch({zero_io})),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(resource_limit_patch_merges_one_operator_io_device) {
  const bbp::ResourceLimits current = CurrentLimits();
  const bbp::IoLimit replacement{
      .device = {.major = 8U, .minor = 0U},
      .read_bytes_per_sec = 9000U,
      .write_bytes_per_sec = std::nullopt,
      .read_operations_per_sec = 90U,
      .write_operations_per_sec = std::nullopt,
  };
  const bbp::ResourceLimitPatch resolved =
      bbp::ResolveOperatorResourceLimitPatch(current, IoPatch({replacement}));

  BOOST_REQUIRE_EQUAL(resolved.io_limits.size(), 2U);
  BOOST_CHECK(resolved.io_limits[0] == replacement);
  BOOST_CHECK(resolved.io_limits[1] == current.io_limits[1]);
}

BOOST_AUTO_TEST_CASE(resource_limit_patch_adds_one_operator_io_device) {
  const bbp::ResourceLimits current = CurrentLimits();
  const bbp::IoLimit addition{
      .device = {.major = 259U, .minor = 7U},
      .read_bytes_per_sec = 9000U,
      .write_bytes_per_sec = 8000U,
      .read_operations_per_sec = 90U,
      .write_operations_per_sec = 80U,
  };
  const bbp::ResourceLimitPatch resolved =
      bbp::ResolveOperatorResourceLimitPatch(current, IoPatch({addition}));

  BOOST_REQUIRE_EQUAL(resolved.io_limits.size(), 3U);
  BOOST_CHECK(resolved.io_limits[0] == current.io_limits[0]);
  BOOST_CHECK(resolved.io_limits[1] == current.io_limits[1]);
  BOOST_CHECK(resolved.io_limits[2] == addition);
}

BOOST_AUTO_TEST_CASE(resource_limit_patch_clears_only_requested_io_device) {
  const bbp::ResourceLimits current = CurrentLimits();
  const bbp::ResourceLimitPatch resolved =
      bbp::ResolveOperatorResourceLimitPatch(current,
                                             IoPatch({EmptyIoLimit(8U, 0U)}));

  BOOST_REQUIRE_EQUAL(resolved.io_limits.size(), 1U);
  BOOST_CHECK(resolved.io_limits.front() == current.io_limits[1]);
}

BOOST_AUTO_TEST_CASE(resource_limit_patch_rejects_ambiguous_operator_io_set) {
  const bbp::ResourceLimits current = CurrentLimits();
  BOOST_CHECK_THROW(
      bbp::ResolveOperatorResourceLimitPatch(current, IoPatch({})),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ResolveOperatorResourceLimitPatch(
          current, IoPatch({EmptyIoLimit(8U, 0U), EmptyIoLimit(8U, 1U)})),
      std::runtime_error);
}
