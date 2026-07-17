#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "bbp/perf_counter.h"
#include "bbp/util.h"

namespace {

std::filesystem::path CurrentCgroupV2Path() {
  std::string membership = bbp::ReadText("/proc/self/cgroup");
  while (!membership.empty() &&
         (membership.back() == '\n' || membership.back() == '\r')) {
    membership.pop_back();
  }
  constexpr std::string_view kUnifiedPrefix = "0::/";
  if (!membership.starts_with(kUnifiedPrefix)) {
    return {};
  }
  return std::filesystem::path("/sys/fs/cgroup") /
         std::filesystem::path(membership.substr(kUnifiedPrefix.size()));
}

}  // namespace

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

  constexpr bbp::PerfCounterTargetKind kTargets[] = {
      bbp::PerfCounterTargetKind::kNode,
      bbp::PerfCounterTargetKind::kWallet,
      bbp::PerfCounterTargetKind::kGroup,
      bbp::PerfCounterTargetKind::kCgroup,
  };
  for (const bbp::PerfCounterTargetKind target : kTargets) {
    const std::optional<bbp::PerfCounterTargetKind> parsed =
        bbp::PerfCounterTargetKindFromName(
            bbp::PerfCounterTargetKindName(target));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == target);
  }
  BOOST_TEST(!bbp::PerfCounterTargetKindFromName("process"));
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

  try {
    (void)bbp::CgroupPerfCounters::Open({}, {bbp::PerfCounterKind::kTaskClock});
    BOOST_FAIL("empty cgroup path was accepted");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_CHECK(error.kind() == bbp::PerfCounterErrorKind::kInvalidArgument);
  }

  try {
    (void)bbp::CgroupPerfCounters::Open("/sys/fs/cgroup", {});
    BOOST_FAIL("empty cgroup counter selection was accepted");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_CHECK(error.kind() == bbp::PerfCounterErrorKind::kInvalidArgument);
  }

  try {
    (void)bbp::CgroupPerfCounters::Open(
        "/sys/fs/cgroup",
        {bbp::PerfCounterKind::kPageFaults, bbp::PerfCounterKind::kPageFaults});
    BOOST_FAIL("duplicate cgroup counter selection was accepted");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_CHECK(error.kind() == bbp::PerfCounterErrorKind::kInvalidArgument);
  }
}

BOOST_AUTO_TEST_CASE(perf_counter_open_handles_unavailable_process) {
  try {
    (void)bbp::ProcessPerfCounters::Open(std::numeric_limits<pid_t>::max(),
                                         {bbp::PerfCounterKind::kTaskClock});
    BOOST_FAIL("unavailable PID unexpectedly accepted perf counters");
  } catch (const bbp::PerfCounterError& error) {
    BOOST_TEST(error.error_number() != 0);
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

BOOST_AUTO_TEST_CASE(perf_counter_collects_task_clock_for_cgroup) {
  const std::filesystem::path cgroup_path = CurrentCgroupV2Path();
  if (cgroup_path.empty() ||
      !std::filesystem::exists(cgroup_path / "cgroup.procs")) {
    BOOST_TEST_MESSAGE("cgroup v2 is unavailable");
    return;
  }
  try {
    bbp::CgroupPerfCounters counters = bbp::CgroupPerfCounters::Open(
        cgroup_path, {bbp::PerfCounterKind::kTaskClock});
    BOOST_TEST(!counters.cpus().empty());
    const int initial_cpu = sched_getcpu();
    BOOST_REQUIRE(initial_cpu >= 0);
    const bool current_cpu_is_profiled =
        std::find(counters.cpus().begin(), counters.cpus().end(),
                  initial_cpu) != counters.cpus().end();
    BOOST_TEST(current_cpu_is_profiled);
    volatile std::uint64_t accumulator = 0;
    for (std::uint64_t index = 0; index < 10'000'000U; ++index) {
      accumulator = accumulator + index;
    }
    (void)accumulator;

    const std::vector<bbp::PerfCounterValue> values = counters.Read();
    BOOST_REQUIRE(values.size() == 1U);
    BOOST_CHECK(values[0].kind == bbp::PerfCounterKind::kTaskClock);
    BOOST_TEST(values[0].raw_value > 0U);
    BOOST_TEST((values[0].time_enabled_ns == 0U) ==
               (values[0].time_running_ns == 0U));
    BOOST_REQUIRE(values[0].scaled_value);
    BOOST_TEST(*values[0].scaled_value > 0U);
  } catch (const bbp::PerfCounterError& error) {
    if (error.kind() == bbp::PerfCounterErrorKind::kPermissionDenied ||
        error.kind() == bbp::PerfCounterErrorKind::kUnsupported) {
      BOOST_TEST_MESSAGE(
          "kernel cgroup perf access unavailable: " << error.what());
      return;
    }
    throw;
  }
}
