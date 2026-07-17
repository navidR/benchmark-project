#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "bbp/perf_counter.h"

BOOST_AUTO_TEST_CASE(perf_counter_names_round_trip) {
  const std::vector<bbp::PerfCounterKind>& kinds =
      bbp::DefaultPerfCounterKinds();
  BOOST_TEST(kinds.size() == 9U);
  std::set<std::string> names;
  for (const bbp::PerfCounterKind kind : kinds) {
    const std::string name(bbp::PerfCounterKindName(kind));
    names.insert(name);
    const std::optional<bbp::PerfCounterKind> parsed =
        bbp::PerfCounterKindFromName(name);
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == kind);
  }
  BOOST_TEST(names.size() == kinds.size());
  BOOST_TEST(!bbp::PerfCounterKindFromName("cpu-cycles"));
  BOOST_TEST(!bbp::PerfCounterKindFromName(""));
}

BOOST_AUTO_TEST_CASE(perf_counter_scaling_handles_multiplexing_and_overflow) {
  bbp::ScaledPerfCounterValue value =
      bbp::ScalePerfCounterValue(50U, 100U, 25U);
  BOOST_REQUIRE(value.value);
  BOOST_TEST(*value.value == 200U);
  BOOST_TEST(value.scaled);
  BOOST_TEST(!value.overflow);

  value = bbp::ScalePerfCounterValue(50U, 25U, 25U);
  BOOST_REQUIRE(value.value);
  BOOST_TEST(*value.value == 50U);
  BOOST_TEST(!value.scaled);
  BOOST_TEST(!value.overflow);

  value = bbp::ScalePerfCounterValue(50U, 25U, 0U);
  BOOST_TEST(!value.value);
  BOOST_TEST(!value.scaled);
  BOOST_TEST(!value.overflow);

  value =
      bbp::ScalePerfCounterValue(std::numeric_limits<std::uint64_t>::max(),
                                 std::numeric_limits<std::uint64_t>::max(), 1U);
  BOOST_REQUIRE(value.value);
  BOOST_TEST(*value.value == std::numeric_limits<std::uint64_t>::max());
  BOOST_TEST(value.scaled);
  BOOST_TEST(value.overflow);
}

BOOST_AUTO_TEST_CASE(perf_counter_open_validates_pid_and_selection) {
  try {
    (void)bbp::ProcessPerfCounters::Open(0, {bbp::PerfCounterKind::kTaskClock});
    BOOST_FAIL("zero PID was accepted");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_CHECK(error.kind() == bbp::PerfCounterErrorKind::kInvalidArgument);
  }

  try {
    (void)bbp::ProcessPerfCounters::Open(getpid(), {});
    BOOST_FAIL("empty counter selection was accepted");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_CHECK(error.kind() == bbp::PerfCounterErrorKind::kInvalidArgument);
  }

  try {
    (void)bbp::ProcessPerfCounters::Open(
        getpid(),
        {bbp::PerfCounterKind::kCycles, bbp::PerfCounterKind::kCycles});
    BOOST_FAIL("duplicate counter selection was accepted");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_CHECK(error.kind() == bbp::PerfCounterErrorKind::kInvalidArgument);
  }
}

BOOST_AUTO_TEST_CASE(perf_counter_collects_task_clock_for_owned_process) {
  try {
    bbp::ProcessPerfCounters counters = bbp::ProcessPerfCounters::Open(
        getpid(), {bbp::PerfCounterKind::kTaskClock});
    volatile std::uint64_t accumulator = 0;
    for (std::uint64_t index = 0; index < 1'000'000U; ++index) {
      accumulator = accumulator + index;
    }
    (void)accumulator;

    const std::vector<bbp::PerfCounterValue> values = counters.Read();
    BOOST_REQUIRE(values.size() == 1U);
    BOOST_CHECK(values[0].kind == bbp::PerfCounterKind::kTaskClock);
    BOOST_TEST(values[0].time_enabled_ns > 0U);
    BOOST_TEST(values[0].time_running_ns > 0U);
    BOOST_REQUIRE(values[0].scaled_value);
    BOOST_TEST(*values[0].scaled_value > 0U);
  } catch (const bbp::PerfCounterError& error) {
    if (error.kind() == bbp::PerfCounterErrorKind::kPermissionDenied ||
        error.kind() == bbp::PerfCounterErrorKind::kUnsupported) {
      BOOST_TEST_MESSAGE("kernel perf access unavailable: " << error.what());
      return;
    }
    throw;
  }
}
