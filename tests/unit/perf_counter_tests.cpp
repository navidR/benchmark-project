#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "bbp/perf_counter.h"
#include "bbp/util.h"

namespace {

class ChildGuard {
 public:
  explicit ChildGuard(pid_t pid) : pid_(pid) {}
  ChildGuard(const ChildGuard&) = delete;
  ChildGuard& operator=(const ChildGuard&) = delete;

  ~ChildGuard() {
    if (pid_ <= 0) {
      return;
    }
    (void)kill(pid_, SIGKILL);
    int status = 0;
    while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
    }
  }

  int Wait() {
    int status = 0;
    pid_t waited = -1;
    do {
      waited = waitpid(pid_, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited == pid_) {
      pid_ = -1;
    }
    return status;
  }

 private:
  pid_t pid_;
};

bool WriteByte(int fd) {
  const char value = 'x';
  ssize_t written = -1;
  do {
    written = write(fd, &value, 1U);
  } while (written < 0 && errno == EINTR);
  return written == 1;
}

bool ReadByte(int fd) {
  char value = 0;
  ssize_t read_count = -1;
  do {
    read_count = read(fd, &value, 1U);
  } while (read_count < 0 && errno == EINTR);
  return read_count == 1;
}

void ClosePipe(int pipe_fds[2]) {
  (void)close(pipe_fds[0]);
  (void)close(pipe_fds[1]);
}

bool BurnThreadCpu(std::int64_t target_ns) {
  timespec cpu_started{};
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_started) != 0) {
    return false;
  }
  volatile std::uint64_t accumulator = 0U;
  while (true) {
    for (std::uint64_t index = 0U; index < 100'000U; ++index) {
      accumulator = accumulator + index;
    }
    timespec cpu_now{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_now) != 0) {
      return false;
    }
    const std::int64_t elapsed_ns =
        static_cast<std::int64_t>(cpu_now.tv_sec - cpu_started.tv_sec) *
            1'000'000'000LL +
        static_cast<std::int64_t>(cpu_now.tv_nsec - cpu_started.tv_nsec);
    if (elapsed_ns >= target_ns) {
      break;
    }
  }
  (void)accumulator;
  return true;
}

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

BOOST_AUTO_TEST_CASE(
    item11_prefixed_process_perf_counts_preexisting_worker_thread) {
  constexpr std::size_t kWorkerCount = 2U;
  int ready[2] = {-1, -1};
  int start[2] = {-1, -1};
  int done[2] = {-1, -1};
  int release[2] = {-1, -1};
  BOOST_REQUIRE(pipe2(ready, O_CLOEXEC) == 0);
  BOOST_REQUIRE(pipe2(start, O_CLOEXEC) == 0);
  BOOST_REQUIRE(pipe2(done, O_CLOEXEC) == 0);
  BOOST_REQUIRE(pipe2(release, O_CLOEXEC) == 0);

  const pid_t child_pid = fork();
  BOOST_REQUIRE(child_pid >= 0);
  if (child_pid == 0) {
    (void)close(ready[0]);
    (void)close(start[1]);
    (void)close(done[0]);
    (void)close(release[1]);
    std::vector<std::thread> workers;
    for (std::size_t worker_index = 0U; worker_index < kWorkerCount;
         ++worker_index) {
      workers.emplace_back([&] {
        if (!WriteByte(ready[1]) || !ReadByte(start[0])) {
          _exit(120);
        }
        if (!BurnThreadCpu(200'000'000LL)) {
          _exit(121);
        }
        if (!WriteByte(done[1])) {
          _exit(122);
        }
      });
    }
    for (std::thread& worker : workers) {
      worker.join();
    }
    if (!ReadByte(release[0])) {
      _exit(123);
    }
    _exit(0);
  }

  ChildGuard child(child_pid);
  (void)close(ready[1]);
  (void)close(start[0]);
  (void)close(done[1]);
  (void)close(release[0]);
  for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
    BOOST_REQUIRE(ReadByte(ready[0]));
  }
  bbp::ProcessPerfCounters counters = bbp::ProcessPerfCounters::Open(
      child_pid, {bbp::PerfCounterKind::kTaskClock});
  for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
    BOOST_REQUIRE(WriteByte(start[1]));
  }
  for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
    BOOST_REQUIRE(ReadByte(done[0]));
  }

  const std::vector<bbp::PerfCounterValue> values = counters.Read();
  BOOST_REQUIRE_EQUAL(values.size(), 1U);
  BOOST_TEST_MESSAGE(
      "two pre-existing workers task-clock ns: " << values.front().raw_value);
  BOOST_TEST(values.front().raw_value >= 300'000'000U);
  BOOST_TEST(values.front().raw_value <= 650'000'000U);

  BOOST_REQUIRE(WriteByte(release[1]));
  const int status = child.Wait();
  BOOST_REQUIRE(WIFEXITED(status));
  BOOST_TEST(WEXITSTATUS(status) == 0);
  ClosePipe(ready);
  ClosePipe(start);
  ClosePipe(done);
  ClosePipe(release);
}

BOOST_AUTO_TEST_CASE(process_perf_counts_future_worker_thread_once) {
  int ready[2] = {-1, -1};
  int start[2] = {-1, -1};
  int done[2] = {-1, -1};
  int release[2] = {-1, -1};
  BOOST_REQUIRE(pipe2(ready, O_CLOEXEC) == 0);
  BOOST_REQUIRE(pipe2(start, O_CLOEXEC) == 0);
  BOOST_REQUIRE(pipe2(done, O_CLOEXEC) == 0);
  BOOST_REQUIRE(pipe2(release, O_CLOEXEC) == 0);

  const pid_t child_pid = fork();
  BOOST_REQUIRE(child_pid >= 0);
  if (child_pid == 0) {
    (void)close(ready[0]);
    (void)close(start[1]);
    (void)close(done[0]);
    (void)close(release[1]);
    if (!WriteByte(ready[1]) || !ReadByte(start[0])) {
      _exit(130);
    }
    std::thread worker([] {
      if (!BurnThreadCpu(250'000'000LL)) {
        _exit(131);
      }
    });
    worker.join();
    if (!WriteByte(done[1]) || !ReadByte(release[0])) {
      _exit(132);
    }
    _exit(0);
  }

  ChildGuard child(child_pid);
  (void)close(ready[1]);
  (void)close(start[0]);
  (void)close(done[1]);
  (void)close(release[0]);
  BOOST_REQUIRE(ReadByte(ready[0]));
  bbp::ProcessPerfCounters counters = bbp::ProcessPerfCounters::Open(
      child_pid, {bbp::PerfCounterKind::kTaskClock});
  BOOST_REQUIRE(WriteByte(start[1]));
  BOOST_REQUIRE(ReadByte(done[0]));

  const std::vector<bbp::PerfCounterValue> values = counters.Read();
  BOOST_REQUIRE_EQUAL(values.size(), 1U);
  BOOST_TEST_MESSAGE(
      "future worker task-clock ns: " << values.front().raw_value);
  BOOST_TEST(values.front().raw_value >= 150'000'000U);
  BOOST_TEST(values.front().raw_value <= 400'000'000U);

  BOOST_REQUIRE(WriteByte(release[1]));
  const int status = child.Wait();
  BOOST_REQUIRE(WIFEXITED(status));
  BOOST_TEST(WEXITSTATUS(status) == 0);
  ClosePipe(ready);
  ClosePipe(start);
  ClosePipe(done);
  ClosePipe(release);
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
