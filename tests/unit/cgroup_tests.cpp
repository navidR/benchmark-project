#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/network.h"
#include "bbp/run_ownership.h"
#include "bbp/simulator_app.h"
#include "bbp/util.h"

namespace {

std::filesystem::path TestDirectory(std::string_view suffix) {
  return std::filesystem::temp_directory_path() /
         ("bbp-cgroup-test-" + std::to_string(getpid()) + "-" +
          std::string(suffix));
}

std::string UniqueRunId(std::string_view suffix) {
  static std::atomic<std::uint64_t> sequence{0U};
  return "cgt-" + std::to_string(getpid()) + "-" +
         std::to_string(sequence.fetch_add(1U)) + "-" + std::string(suffix);
}

std::filesystem::path RunCgroupPath(std::string_view run_id) {
  return std::filesystem::path("/sys/fs/cgroup/bbp") / run_id;
}

class PreparedRunGuard {
 public:
  explicit PreparedRunGuard(std::string run_id) : run_id_(std::move(run_id)) {
    bbp::Cgroup::PrepareRun(run_id_);
  }

  PreparedRunGuard(const PreparedRunGuard&) = delete;
  PreparedRunGuard& operator=(const PreparedRunGuard&) = delete;

  ~PreparedRunGuard() {
    if (!active_) {
      return;
    }
    try {
      bbp::Cgroup::RemoveRun(run_id_);
    } catch (const std::exception&) {
    }
  }

  const std::string& run_id() const { return run_id_; }
  void Release() { active_ = false; }

 private:
  std::string run_id_;
  bool active_ = true;
};

class ChildGuard {
 public:
  explicit ChildGuard(pid_t pid) : pid_(pid) {}
  ChildGuard(const ChildGuard&) = delete;
  ChildGuard& operator=(const ChildGuard&) = delete;

  ~ChildGuard() {
    if (pid_ <= 0) {
      return;
    }
    kill(pid_, SIGKILL);
    int ignored = 0;
    while (waitpid(pid_, &ignored, 0) < 0 && errno == EINTR) {
    }
  }

  int Wait() {
    int status = 0;
    pid_t waited = -1;
    do {
      waited = waitpid(pid_, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited != pid_) {
      throw std::runtime_error("waitpid failed for cgroup test child");
    }
    pid_ = -1;
    return status;
  }

 private:
  pid_t pid_ = -1;
};

std::unique_ptr<PreparedRunGuard> PreparePrivilegedTestRun(std::string run_id) {
  try {
    return std::make_unique<PreparedRunGuard>(std::move(run_id));
  } catch (const std::exception& error) {
    BOOST_TEST_MESSAGE("skipping privileged cgroup test: " << error.what());
    return nullptr;
  }
}

bool TryAttachPrivilegedTestProcess(const std::filesystem::path& cgroup,
                                    pid_t pid, std::string_view test_name) {
  try {
    bbp::WriteText(cgroup / "cgroup.procs", std::to_string(pid));
    return true;
  } catch (const std::exception& error) {
    BOOST_TEST_MESSAGE("skipping " << test_name << ": " << error.what());
    return false;
  }
}

void WriteMetricFixture(const std::filesystem::path& dir,
                        std::string_view io_stat,
                        std::string_view memory_stat =
                            "anon 123\nfile 456\npgfault 7\npgmajfault 2\n") {
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  bbp::WriteText(dir / "cpu.stat",
                 "usage_usec 100\n"
                 "throttled_usec 7\n");
  bbp::WriteText(dir / "cpu.pressure",
                 "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\n"
                 "full avg10=0.00 avg60=0.00 avg300=0.00 total=56\n");
  bbp::WriteText(dir / "memory.current", "2048\n");
  bbp::WriteText(dir / "memory.peak", "4096\n");
  bbp::WriteText(dir / "memory.high", "8192\n");
  bbp::WriteText(dir / "memory.max", "max\n");
  bbp::WriteText(dir / "memory.stat", memory_stat);
  bbp::WriteText(dir / "cpu.max", "50000 100000\n");
  bbp::WriteText(dir / "cpu.weight", "250\n");
  bbp::WriteText(dir / "memory.events",
                 "low 4\n"
                 "high 5\n"
                 "max 6\n"
                 "oom 2\n"
                 "oom_kill 1\n"
                 "oom_group_kill 3\n");
  bbp::WriteText(dir / "cgroup.events", "populated 1\nfrozen 0\n");
  bbp::WriteText(dir / "io.max", "8:0 rbps=1000 wbps=max riops=30 wiops=max\n");
  bbp::WriteText(dir / "io.weight", "default 300\n");
  bbp::WriteText(dir / "io.stat", io_stat);
  bbp::WriteText(dir / "io.pressure",
                 "some avg10=0.00 avg60=0.00 avg300=0.00 total=77\n"
                 "full avg10=0.00 avg60=0.00 avg300=0.00 total=8\n");
  bbp::WriteText(dir / "pids.current", "3\n");
  bbp::WriteText(dir / "pids.max", "max\n");
  bbp::WriteText(dir / "pids.events", "max 9\n");
}

}  // namespace

BOOST_AUTO_TEST_CASE(cgroup_metrics_read_io_memory_and_pressure_totals) {
  const std::filesystem::path dir = TestDirectory("metrics");
  WriteMetricFixture(
      dir,
      "8:0 rbytes=10 wbytes=20 rios=1 wios=2 dbytes=5 dios=6\n"
      "8:16 rbytes=30 wbytes=40 rios=3 wios=4 dbytes=7 dios=8\n");

  const bbp::CgroupMetrics metrics = bbp::Cgroup(dir).ReadMetrics();

  BOOST_TEST(metrics.cpu_usage_usec == 100U);
  BOOST_TEST(metrics.cpu_throttled_usec == 7U);
  BOOST_TEST(metrics.cpu_pressure_some_total_usec == 1234U);
  BOOST_TEST(metrics.cpu_pressure_full_total_usec == 56U);
  BOOST_TEST(metrics.memory_current == 2048U);
  BOOST_TEST(metrics.memory_peak == 4096U);
  BOOST_TEST(metrics.memory_high_limit_bytes.has_value());
  BOOST_TEST(*metrics.memory_high_limit_bytes == 8192U);
  BOOST_TEST(!metrics.memory_max_limit_bytes.has_value());
  BOOST_TEST(metrics.cpu_quota_us.has_value());
  BOOST_TEST(*metrics.cpu_quota_us == 50000U);
  BOOST_TEST(metrics.cpu_period_us == 100000U);
  BOOST_TEST(metrics.cpu_weight == 250U);
  BOOST_TEST(metrics.io_weight == 300U);
  BOOST_REQUIRE_EQUAL(metrics.io_limits.size(), 1U);
  BOOST_TEST(metrics.io_limits[0].device.major == 8U);
  BOOST_TEST(metrics.io_limits[0].device.minor == 0U);
  BOOST_TEST(*metrics.io_limits[0].read_bytes_per_sec == 1000U);
  BOOST_TEST(!metrics.io_limits[0].write_bytes_per_sec.has_value());
  BOOST_TEST(*metrics.io_limits[0].read_operations_per_sec == 30U);
  BOOST_TEST(!metrics.io_limits[0].write_operations_per_sec.has_value());
  BOOST_TEST(metrics.io_read_bytes == 40U);
  BOOST_TEST(metrics.io_write_bytes == 60U);
  BOOST_TEST(metrics.io_read_operations == 4U);
  BOOST_TEST(metrics.io_write_operations == 6U);
  BOOST_TEST(metrics.io_discard_bytes == 12U);
  BOOST_TEST(metrics.io_discard_operations == 14U);
  BOOST_TEST(metrics.io_pressure_some_total_usec == 77U);
  BOOST_TEST(metrics.io_pressure_full_total_usec == 8U);
  BOOST_TEST(metrics.pids_current == 3U);
  BOOST_TEST(!metrics.pids_max_limit.has_value());
  BOOST_TEST(metrics.pids_max_events == 9U);
  BOOST_TEST(metrics.cgroup_populated == 1U);
  BOOST_TEST(metrics.cgroup_frozen == 0U);
  BOOST_TEST(metrics.memory_low == 4U);
  BOOST_TEST(metrics.memory_high == 5U);
  BOOST_TEST(metrics.memory_max == 6U);
  BOOST_TEST(metrics.oom == 2U);
  BOOST_TEST(metrics.oom_kill == 1U);
  BOOST_TEST(metrics.oom_group_kill == 3U);
  BOOST_TEST(metrics.memory_stat.at("anon") == 123U);
  BOOST_TEST(metrics.memory_stat.at("file") == 456U);
  BOOST_TEST(metrics.memory_stat.at("pgfault") == 7U);
  BOOST_TEST(metrics.memory_stat.at("pgmajfault") == 2U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(cgroup_weights_and_io_max_use_exact_kernel_formats) {
  const std::filesystem::path dir = TestDirectory("limits");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  bbp::WriteText(dir / "cpu.weight", "100\n");
  bbp::WriteText(dir / "io.weight", "default 100\n");
  bbp::WriteText(dir / "io.max", "");
  const bbp::Cgroup cgroup(dir);

  cgroup.SetCpuWeight(567);
  cgroup.SetIoWeight(789);
  cgroup.SetIoMax(bbp::IoLimit{.device = {.major = 252, .minor = 1},
                               .read_bytes_per_sec = 1048576,
                               .write_bytes_per_sec = std::nullopt,
                               .read_operations_per_sec = 300,
                               .write_operations_per_sec = std::nullopt});

  BOOST_TEST(bbp::ReadText(dir / "cpu.weight") == "567");
  BOOST_TEST(bbp::ReadText(dir / "io.weight") == "default 789");
  BOOST_TEST(bbp::ReadText(dir / "io.max") ==
             "252:1 rbps=1048576 wbps=max riops=300 wiops=max");
  BOOST_CHECK_THROW(cgroup.SetCpuWeight(0), std::runtime_error);
  BOOST_CHECK_THROW(cgroup.SetCpuWeight(10001), std::runtime_error);
  BOOST_CHECK_THROW(cgroup.SetIoWeight(0), std::runtime_error);
  BOOST_CHECK_THROW(cgroup.SetIoWeight(10001), std::runtime_error);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(block_device_ids_are_strict_and_bounded) {
  const bbp::BlockDeviceId device = bbp::ParseBlockDeviceId("252:1");
  BOOST_TEST(device.major == 252U);
  BOOST_TEST(device.minor == 1U);
  BOOST_TEST(bbp::BlockDeviceIdText(device) == "252:1");
  BOOST_CHECK_THROW(bbp::ParseBlockDeviceId("252"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseBlockDeviceId(":1"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseBlockDeviceId("252:"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseBlockDeviceId("+252:1"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseBlockDeviceId("252:1x"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseBlockDeviceId("4294967296:1"),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(cgroup_metrics_reject_io_total_overflow) {
  const std::filesystem::path dir = TestDirectory("overflow");
  WriteMetricFixture(dir,
                     "8:0 rbytes=18446744073709551615 wbytes=0 rios=0 wios=0 "
                     "dbytes=0 dios=0\n"
                     "8:16 rbytes=1 wbytes=0 rios=0 wios=0 dbytes=0 dios=0\n");
  BOOST_CHECK_THROW(bbp::Cgroup(dir).ReadMetrics(), std::runtime_error);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(cgroup_metrics_reject_duplicate_memory_stat_keys) {
  const std::filesystem::path dir = TestDirectory("memory-duplicate");
  WriteMetricFixture(dir, "", "anon 1\nanon 2\n");
  BOOST_CHECK_THROW(bbp::Cgroup(dir).ReadMetrics(), std::runtime_error);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    cgroup_operations_refuse_a_replacement_at_the_acquired_path) {
  const std::filesystem::path root = TestDirectory("bound-replacement");
  const std::filesystem::path acquired_path = root / "node";
  const std::filesystem::path displaced_path = root / "displaced";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(acquired_path);

  {
    const bbp::Cgroup cgroup(acquired_path);
    std::filesystem::rename(acquired_path, displaced_path);
    std::filesystem::create_directory(acquired_path);
    bbp::WriteText(acquired_path / "sentinel", "foreign\n");

    const auto rejects_replacement = [](const std::runtime_error& error) {
      return std::string(error.what()).find("replaced cgroup") !=
             std::string::npos;
    };
    BOOST_CHECK_EXCEPTION(cgroup.AttachPid(getpid()), std::runtime_error,
                          rejects_replacement);
    BOOST_CHECK_EXCEPTION(cgroup.SetMemoryMax(4096U), std::runtime_error,
                          rejects_replacement);
    BOOST_CHECK_EXCEPTION(cgroup.ReadMetrics(), std::runtime_error,
                          rejects_replacement);
    BOOST_CHECK_EXCEPTION(
        cgroup.KillAll(std::chrono::steady_clock::now() +
                       std::chrono::seconds(1)),
        std::runtime_error, rejects_replacement);
    BOOST_CHECK_EXCEPTION(
        cgroup.Remove(std::chrono::steady_clock::now() +
                      std::chrono::seconds(1)),
        std::runtime_error, rejects_replacement);

    BOOST_TEST(bbp::ReadText(acquired_path / "sentinel") == "foreign\n");
    BOOST_TEST(!std::filesystem::exists(acquired_path / "cgroup.procs"));
    BOOST_TEST(!std::filesystem::exists(acquired_path / "memory.max"));
    BOOST_TEST(std::filesystem::is_directory(acquired_path));
  }
  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_support_destruction_requires_both_positive_verifications) {
  BOOST_TEST(!bbp::RuntimeNodeSupportDestructionAllowedForTest(false, true,
                                                               true));
  BOOST_TEST(!bbp::RuntimeNodeSupportDestructionAllowedForTest(true, false,
                                                               true));
  BOOST_TEST(!bbp::RuntimeNodeSupportDestructionAllowedForTest(true, true,
                                                               false));
  BOOST_TEST(
      bbp::RuntimeNodeSupportDestructionAllowedForTest(true, true, true));
}

BOOST_AUTO_TEST_CASE(cgroup_refuses_unprepared_and_preexisting_run_ownership) {
  const std::string run_id = UniqueRunId("foreign");
  const std::filesystem::path run_path = RunCgroupPath(run_id);
  std::error_code ec;
  const bool created = std::filesystem::create_directory(run_path, ec);
  if (ec) {
    BOOST_TEST_MESSAGE(
        "skipping privileged cgroup ownership test: " << ec.message());
    return;
  }
  BOOST_REQUIRE(created);

  try {
    BOOST_CHECK_EXCEPTION(
        bbp::Cgroup::PrepareRun(run_id), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what())
                     .find("refusing to adopt pre-existing run cgroup") !=
                 std::string::npos;
        });
    BOOST_CHECK_EXCEPTION(
        bbp::Cgroup::Create(run_id, "node-1"), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what())
                     .find("refusing to adopt an unprepared run cgroup") !=
                 std::string::npos;
        });
    BOOST_CHECK_EXCEPTION(
        bbp::Cgroup::RemoveRun(run_id), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what())
                     .find("refusing to remove an unowned run cgroup") !=
                 std::string::npos;
        });
    BOOST_TEST(std::filesystem::is_directory(run_path));
    BOOST_TEST(!std::filesystem::exists(run_path / "node-1"));
  } catch (...) {
    std::filesystem::remove(run_path, ec);
    throw;
  }
  ec.clear();
  BOOST_REQUIRE(std::filesystem::remove(run_path, ec));
  BOOST_REQUIRE(!ec);
}

BOOST_AUTO_TEST_CASE(stale_cgroup_cleanup_requires_exact_run_marker) {
  const std::string run_id = UniqueRunId("stale-owner");
  const std::filesystem::path parent = TestDirectory("stale-owner");
  const std::filesystem::path run_directory = parent / run_id;
  std::filesystem::remove_all(parent);
  std::filesystem::create_directories(run_directory);
  const bbp::RunOwnership ownership =
      bbp::CreateRunOwnership(run_id, run_directory);
  bbp::WriteText(run_directory / ".bbp-run", "not a simulator marker\n");

  BOOST_CHECK_EXCEPTION(
      bbp::Cgroup::RemoveStaleRun(ownership), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find("invalid run ownership marker") !=
               std::string::npos;
      });

  std::filesystem::remove_all(parent);
}

BOOST_AUTO_TEST_CASE(stale_cleanup_is_scoped_to_one_same_id_run_instance) {
  const std::string run_id = UniqueRunId("same-id");
  const std::filesystem::path parent = TestDirectory("same-id-roots");
  const std::filesystem::path first_root = parent / "first" / run_id;
  const std::filesystem::path second_root = parent / "second" / run_id;
  std::filesystem::remove_all(parent);
  std::filesystem::create_directories(first_root);
  std::filesystem::create_directories(second_root);
  const bbp::RunOwnership first = bbp::CreateRunOwnership(run_id, first_root);
  const bbp::RunOwnership second = bbp::CreateRunOwnership(run_id, second_root);
  bbp::WriteRunOwnershipMarker(first);
  bbp::WriteRunOwnershipMarker(second);

  try {
    bbp::Cgroup::PrepareRun(first);
    bbp::Cgroup::PrepareRun(second);
  } catch (const std::exception& error) {
    BOOST_TEST_MESSAGE(
        "skipping privileged same-id ownership test: " << error.what());
    try {
      bbp::Cgroup::RemoveRun(second.cgroup_name);
    } catch (const std::exception&) {
    }
    try {
      bbp::Cgroup::RemoveRun(first.cgroup_name);
    } catch (const std::exception&) {
    }
    std::filesystem::remove_all(parent);
    return;
  }

  try {
    const bbp::Cgroup first_node =
        bbp::Cgroup::Create(first.cgroup_name, "node-1");
    const pid_t pid = fork();
    BOOST_REQUIRE(pid >= 0);
    if (pid == 0) {
      for (;;) {
        pause();
      }
    }
    ChildGuard child(pid);
    first_node.AttachPid(pid);

    bbp::Cgroup::RemoveStaleRun(second);
    BOOST_TEST(std::filesystem::is_directory(RunCgroupPath(first.cgroup_name)));
    BOOST_TEST(!std::filesystem::exists(RunCgroupPath(second.cgroup_name)));
    BOOST_TEST(kill(pid, 0) == 0);

    bbp::Cgroup::RemoveRun(first.cgroup_name);
    const int status = child.Wait();
    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_TEST(WTERMSIG(status) == SIGKILL);
  } catch (...) {
    try {
      bbp::Cgroup::RemoveRun(second.cgroup_name);
    } catch (const std::exception&) {
    }
    try {
      bbp::Cgroup::RemoveRun(first.cgroup_name);
    } catch (const std::exception&) {
    }
    std::filesystem::remove_all(parent);
    throw;
  }
  std::filesystem::remove_all(parent);
}

BOOST_AUTO_TEST_CASE(
    stale_cgroup_cleanup_refuses_an_exact_resource_id_collision) {
  const std::string first_run_id = UniqueRunId("collision-owner");
  const std::string second_run_id = UniqueRunId("collision-request");
  const std::filesystem::path parent = TestDirectory("resource-collision");
  const std::filesystem::path first_root = parent / first_run_id;
  const std::filesystem::path second_root = parent / second_run_id;
  std::filesystem::remove_all(parent);
  std::filesystem::create_directories(first_root);
  std::filesystem::create_directories(second_root);
  const bbp::RunOwnership first =
      bbp::CreateRunOwnership(first_run_id, first_root);
  bbp::WriteRunOwnershipMarker(first);
  bbp::RunOwnership collision = first;
  collision.run_id = second_run_id;
  collision.run_root = std::filesystem::canonical(second_root);
  bbp::WriteRunOwnershipMarker(collision);

  try {
    bbp::Cgroup::PrepareRun(first);
  } catch (const std::exception& error) {
    BOOST_TEST_MESSAGE(
        "skipping privileged resource-collision test: " << error.what());
    std::filesystem::remove_all(parent);
    return;
  }

  try {
    const bbp::Cgroup node = bbp::Cgroup::Create(first.cgroup_name, "node-1");
    const pid_t pid = fork();
    BOOST_REQUIRE(pid >= 0);
    if (pid == 0) {
      for (;;) {
        pause();
      }
    }
    ChildGuard child(pid);
    node.AttachPid(pid);

    BOOST_CHECK_EXCEPTION(
        bbp::Cgroup::RemoveStaleRun(collision), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what())
                     .find("binding does not match exact run ownership") !=
                 std::string::npos;
        });
    BOOST_TEST(std::filesystem::is_directory(RunCgroupPath(first.cgroup_name)));
    BOOST_TEST(kill(pid, 0) == 0);

    bbp::Cgroup::RemoveRun(first.cgroup_name);
    const int status = child.Wait();
    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_TEST(WTERMSIG(status) == SIGKILL);
  } catch (...) {
    try {
      bbp::Cgroup::RemoveRun(first.cgroup_name);
    } catch (const std::exception&) {
    }
    std::filesystem::remove_all(parent);
    throw;
  }
  std::filesystem::remove_all(parent);
}

BOOST_AUTO_TEST_CASE(
    stale_cgroup_cleanup_refuses_replaced_run_root_and_cgroup_inodes) {
  const std::string run_id = UniqueRunId("inode-owner");
  const std::filesystem::path parent = TestDirectory("inode-owner");
  const std::filesystem::path run_root = parent / run_id;
  const std::filesystem::path displaced_root = parent / "displaced";
  std::filesystem::remove_all(parent);
  std::filesystem::create_directories(run_root);
  const bbp::RunOwnership ownership = bbp::CreateRunOwnership(run_id, run_root);
  bbp::WriteRunOwnershipMarker(ownership);

  try {
    bbp::Cgroup::PrepareRun(ownership);
  } catch (const std::exception& error) {
    BOOST_TEST_MESSAGE(
        "skipping privileged inode-ownership test: " << error.what());
    std::filesystem::remove_all(parent);
    return;
  }

  try {
    std::filesystem::rename(run_root, displaced_root);
    std::filesystem::create_directory(run_root);
    bbp::WriteRunOwnershipMarker(ownership);
    BOOST_CHECK_EXCEPTION(
        bbp::Cgroup::RemoveStaleRun(ownership), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what())
                     .find("binding does not match exact run ownership") !=
                 std::string::npos;
        });
    BOOST_TEST(
        std::filesystem::is_directory(RunCgroupPath(ownership.cgroup_name)));
    std::filesystem::remove_all(run_root);
    std::filesystem::rename(displaced_root, run_root);

    const std::filesystem::path cgroup_path =
        RunCgroupPath(ownership.cgroup_name);
    BOOST_REQUIRE(std::filesystem::remove(cgroup_path));
    BOOST_REQUIRE(std::filesystem::create_directory(cgroup_path));
    BOOST_CHECK_EXCEPTION(
        bbp::Cgroup::RemoveStaleRun(ownership), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what())
                     .find("replaced run cgroup identity") != std::string::npos;
        });
    BOOST_TEST(std::filesystem::is_directory(cgroup_path));
    BOOST_REQUIRE(std::filesystem::remove(cgroup_path));
    bbp::Cgroup::RemoveRun(ownership.cgroup_name);
  } catch (...) {
    std::error_code ignored;
    std::filesystem::remove(RunCgroupPath(ownership.cgroup_name), ignored);
    try {
      bbp::Cgroup::RemoveRun(ownership.cgroup_name);
    } catch (const std::exception&) {
    }
    if (std::filesystem::exists(displaced_root) &&
        !std::filesystem::exists(run_root)) {
      std::filesystem::rename(displaced_root, run_root, ignored);
    }
    std::filesystem::remove_all(parent);
    throw;
  }
  std::filesystem::remove_all(parent);
}

BOOST_AUTO_TEST_CASE(cgroup_recursively_kills_and_removes_nested_descendants) {
  std::unique_ptr<PreparedRunGuard> run =
      PreparePrivilegedTestRun(UniqueRunId("nested"));
  if (!run) {
    return;
  }
  const bbp::Cgroup node = bbp::Cgroup::Create(run->run_id(), "node-1");
  BOOST_CHECK_THROW(bbp::Cgroup::Create(run->run_id(), "node-1"),
                    std::runtime_error);
  const std::filesystem::path child = node.path() / "worker";
  const std::filesystem::path leaf = child / "leaf";
  std::filesystem::create_directory(child);
  std::filesystem::create_directory(leaf);

  const pid_t pid = fork();
  BOOST_REQUIRE(pid >= 0);
  if (pid == 0) {
    for (;;) {
      pause();
    }
  }
  ChildGuard process(pid);
  bbp::WriteText(leaf / "cgroup.procs", std::to_string(pid));
  BOOST_TEST(bbp::ReadText(leaf / "cgroup.procs").find(std::to_string(pid)) !=
             std::string::npos);

  bbp::Cgroup::RemoveRun(run->run_id());
  run->Release();
  const int status = process.Wait();
  BOOST_REQUIRE(WIFSIGNALED(status));
  BOOST_TEST(WTERMSIG(status) == SIGKILL);
  BOOST_TEST(!std::filesystem::exists(RunCgroupPath(run->run_id())));

  // A completed removal remains harmless when a caller retries it.
  bbp::Cgroup::RemoveRun(run->run_id());
}

BOOST_AUTO_TEST_CASE(cgroup_pidfd_fallback_kills_a_helper_and_its_descendant) {
  std::unique_ptr<PreparedRunGuard> run =
      PreparePrivilegedTestRun(UniqueRunId("pidfd-fallback"));
  if (!run) {
    return;
  }
  const bbp::Cgroup node = bbp::Cgroup::Create(run->run_id(), "node-1");
  int release_fork[2] = {-1, -1};
  int descendant_pipe[2] = {-1, -1};
  BOOST_REQUIRE(pipe(release_fork) == 0);
  BOOST_REQUIRE(pipe(descendant_pipe) == 0);
  BOOST_REQUIRE(prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0);
  const pid_t helper = fork();
  BOOST_REQUIRE(helper >= 0);
  if (helper == 0) {
    close(release_fork[1]);
    close(descendant_pipe[0]);
    char release = 0;
    ssize_t received = -1;
    do {
      received = read(release_fork[0], &release, 1U);
    } while (received < 0 && errno == EINTR);
    if (received != 1) {
      _exit(20);
    }
    const pid_t descendant = fork();
    if (descendant < 0) {
      _exit(21);
    }
    if (descendant == 0) {
      for (;;) {
        pause();
      }
    }
    ssize_t written = -1;
    do {
      written = write(descendant_pipe[1], &descendant, sizeof(descendant));
    } while (written < 0 && errno == EINTR);
    if (written != static_cast<ssize_t>(sizeof(descendant))) {
      _exit(22);
    }
    for (;;) {
      pause();
    }
  }
  close(release_fork[0]);
  close(descendant_pipe[1]);
  ChildGuard helper_guard(helper);
  try {
    node.AttachPid(helper);
    BOOST_REQUIRE(write(release_fork[1], "x", 1U) == 1);
    close(release_fork[1]);
    release_fork[1] = -1;
    pid_t descendant = -1;
    ssize_t received = -1;
    do {
      received = read(descendant_pipe[0], &descendant, sizeof(descendant));
    } while (received < 0 && errno == EINTR);
    BOOST_REQUIRE(received == static_cast<ssize_t>(sizeof(descendant)));
    close(descendant_pipe[0]);
    descendant_pipe[0] = -1;

    bbp::KillCgroupProcessesWithPidfdFallbackForTest(
        node.path(),
        std::chrono::steady_clock::now() + std::chrono::seconds(2));
    const int helper_status = helper_guard.Wait();
    int descendant_status = 0;
    pid_t waited = -1;
    do {
      waited = waitpid(descendant, &descendant_status, 0);
    } while (waited < 0 && errno == EINTR);
    BOOST_REQUIRE(waited == descendant);
    BOOST_REQUIRE(WIFSIGNALED(helper_status));
    BOOST_REQUIRE(WIFSIGNALED(descendant_status));
    BOOST_TEST(WTERMSIG(helper_status) == SIGKILL);
    BOOST_TEST(WTERMSIG(descendant_status) == SIGKILL);
    BOOST_TEST(bbp::SplitWhitespace(bbp::ReadText(node.path() / "cgroup.procs"))
                   .empty());
    node.Remove(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    bbp::Cgroup::RemoveRun(run->run_id());
    run->Release();
    BOOST_REQUIRE(prctl(PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0) == 0);
  } catch (...) {
    if (release_fork[1] >= 0) {
      close(release_fork[1]);
    }
    if (descendant_pipe[0] >= 0) {
      close(descendant_pipe[0]);
    }
    static_cast<void>(prctl(PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0));
    throw;
  }
}

BOOST_AUTO_TEST_CASE(network_namespace_helper_is_owned_by_node_cgroup) {
  std::unique_ptr<PreparedRunGuard> run =
      PreparePrivilegedTestRun(UniqueRunId("netns"));
  if (!run) {
    return;
  }
  const bbp::Cgroup node = bbp::Cgroup::Create(run->run_id(), "node-1");
  bbp::NetworkNamespace network_namespace;
  try {
    network_namespace = bbp::NetworkNamespace::Create(node.access_path());
  } catch (const std::exception& error) {
    BOOST_TEST_MESSAGE(
        "skipping privileged network namespace test: " << error.what());
    return;
  }
  const std::string helper_pid = std::to_string(network_namespace.helper_pid());
  const std::vector<std::string> node_pids =
      bbp::SplitWhitespace(bbp::ReadText(node.path() / "cgroup.procs"));
  BOOST_CHECK(std::find(node_pids.begin(), node_pids.end(), helper_pid) !=
              node_pids.end());

  bbp::Cgroup::RemoveRun(run->run_id());
  run->Release();
  network_namespace.Stop();
  BOOST_TEST(!std::filesystem::exists(RunCgroupPath(run->run_id())));
}

BOOST_AUTO_TEST_CASE(
    cgroup_scope_restores_processes_and_controller_state_once) {
  const std::string suffix = UniqueRunId("scope");
  const std::filesystem::path scope_root =
      std::filesystem::path("/sys/fs/cgroup/bbp") / suffix;
  const std::filesystem::path state_file = TestDirectory("scope-state");
  std::error_code error;
  std::filesystem::remove(state_file, error);
  error.clear();
  if (!std::filesystem::create_directory(scope_root, error)) {
    BOOST_TEST_MESSAGE("skipping real cgroup scope test: " << error.message());
    return;
  }

  const pid_t pid = fork();
  BOOST_REQUIRE(pid >= 0);
  if (pid == 0) {
    for (;;) {
      pause();
    }
  }
  ChildGuard child(pid);
  if (!TryAttachPrivilegedTestProcess(scope_root, pid,
                                      "real cgroup scope test")) {
    std::filesystem::remove(scope_root, error);
    return;
  }
  try {
    const std::string process_cgroup_before =
        bbp::ReadText("/proc/" + std::to_string(pid) + "/cgroup");
    const std::string root_controllers_before =
        bbp::ReadText(scope_root / "cgroup.subtree_control");
    const bbp::CgroupScopeTestConfig config{
        .root = scope_root,
        .simulator_name = "bbp",
        .state_file = state_file,
    };
    const std::string first = "scope-first";
    const std::string second = "scope-second";

    int gate[2] = {-1, -1};
    BOOST_REQUIRE(pipe(gate) == 0);
    const auto fork_preparer = [&](const std::string& run_id) {
      const pid_t preparer = fork();
      if (preparer == 0) {
        close(gate[1]);
        char signal = 0;
        ssize_t received = -1;
        do {
          received = read(gate[0], &signal, 1U);
        } while (received < 0 && errno == EINTR);
        if (received != 1) {
          _exit(20);
        }
        try {
          bbp::PrepareCgroupRunInTestScope(config, run_id);
          _exit(0);
        } catch (const std::exception&) {
          _exit(21);
        }
      }
      return preparer;
    };
    const pid_t first_preparer = fork_preparer(first);
    BOOST_REQUIRE(first_preparer > 0);
    const pid_t second_preparer = fork_preparer(second);
    BOOST_REQUIRE(second_preparer > 0);
    close(gate[0]);
    BOOST_REQUIRE(write(gate[1], "xx", 2U) == 2);
    close(gate[1]);
    int first_status = 0;
    int second_status = 0;
    BOOST_REQUIRE(waitpid(first_preparer, &first_status, 0) == first_preparer);
    BOOST_REQUIRE(waitpid(second_preparer, &second_status, 0) ==
                  second_preparer);
    BOOST_REQUIRE(WIFEXITED(first_status));
    BOOST_REQUIRE(WIFEXITED(second_status));
    BOOST_TEST(WEXITSTATUS(first_status) == 0);
    BOOST_TEST(WEXITSTATUS(second_status) == 0);

    BOOST_TEST(std::filesystem::exists(state_file));
    BOOST_TEST(bbp::SplitWhitespace(bbp::ReadText(scope_root / "cgroup.procs"))
                   .empty());
    BOOST_TEST(bbp::ReadText("/proc/" + std::to_string(pid) + "/cgroup")
                   .find(".bbp-controller-v1-") != std::string::npos);
    bbp::RemoveCgroupRunInTestScope(config, first);
    BOOST_TEST(std::filesystem::exists(state_file));
    BOOST_TEST(std::filesystem::exists(scope_root / "bbp" / second));
    BOOST_TEST(bbp::ReadText("/proc/" + std::to_string(pid) + "/cgroup")
                   .find(".bbp-controller-v1-") != std::string::npos);

    bbp::RemoveCgroupRunInTestScope(config, second);
    BOOST_TEST(!std::filesystem::exists(state_file));
    BOOST_TEST(!std::filesystem::exists(scope_root / "bbp"));
    BOOST_TEST(bbp::ReadText(scope_root / "cgroup.subtree_control") ==
               root_controllers_before);
    BOOST_TEST(bbp::ReadText("/proc/" + std::to_string(pid) + "/cgroup") ==
               process_cgroup_before);
    const std::vector<std::string> restored =
        bbp::SplitWhitespace(bbp::ReadText(scope_root / "cgroup.procs"));
    BOOST_CHECK(std::find(restored.begin(), restored.end(),
                          std::to_string(pid)) != restored.end());

    kill(pid, SIGKILL);
    const int status = child.Wait();
    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_TEST(WTERMSIG(status) == SIGKILL);
    BOOST_REQUIRE(std::filesystem::remove(scope_root, error));
    BOOST_REQUIRE(!error);
  } catch (...) {
    try {
      bbp::RemoveCgroupRunInTestScope(
          bbp::CgroupScopeTestConfig{
              .root = scope_root,
              .simulator_name = "bbp",
              .state_file = state_file,
          },
          "scope-first");
    } catch (const std::exception&) {
    }
    try {
      bbp::RemoveCgroupRunInTestScope(
          bbp::CgroupScopeTestConfig{
              .root = scope_root,
              .simulator_name = "bbp",
              .state_file = state_file,
          },
          "scope-second");
    } catch (const std::exception&) {
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(
    cgroup_scope_recovers_an_exact_bound_pending_created_run) {
  const std::string suffix = UniqueRunId("scope-pending");
  const std::filesystem::path scope_root =
      std::filesystem::path("/sys/fs/cgroup/bbp") / suffix;
  const std::filesystem::path state_file =
      TestDirectory("scope-pending-state");
  const std::filesystem::path ownership_roots =
      TestDirectory("scope-pending-roots");
  std::error_code error;
  std::filesystem::remove(state_file, error);
  std::filesystem::remove_all(ownership_roots, error);
  error.clear();
  if (!std::filesystem::create_directory(scope_root, error)) {
    BOOST_TEST_MESSAGE(
        "skipping real pending-recovery test: " << error.message());
    return;
  }
  std::filesystem::create_directories(ownership_roots / "first");
  std::filesystem::create_directories(ownership_roots / "second");
  const bbp::RunOwnership first = bbp::CreateRunOwnership(
      UniqueRunId("pending-first"), ownership_roots / "first");
  const bbp::RunOwnership second = bbp::CreateRunOwnership(
      UniqueRunId("pending-second"), ownership_roots / "second");
  bbp::WriteRunOwnershipMarker(first);
  bbp::WriteRunOwnershipMarker(second);
  const bbp::CgroupScopeTestConfig config{
      .root = scope_root,
      .simulator_name = "bbp",
      .state_file = state_file,
  };

  try {
    bbp::PrepareCgroupRunInTestScope(config, first);
  } catch (const std::exception& failure) {
    BOOST_TEST_MESSAGE(
        "skipping real pending-recovery test: " << failure.what());
    std::filesystem::remove(scope_root, error);
    std::filesystem::remove_all(ownership_roots);
    return;
  }

  try {
    boost::json::value state = boost::json::parse(bbp::ReadText(state_file));
    BOOST_REQUIRE(state.is_object());
    state.as_object()["active_runs"] = boost::json::array{};
    state.as_object()["pending_run"] = first.cgroup_name;
    state.as_object()["pending_run_created"] = true;
    bbp::WriteText(state_file, boost::json::serialize(state) + "\n");

    bbp::PrepareCgroupRunInTestScope(config, second);
    BOOST_TEST(!std::filesystem::exists(scope_root / "bbp" /
                                        first.cgroup_name));
    BOOST_TEST(std::filesystem::is_directory(scope_root / "bbp" /
                                             second.cgroup_name));
    bbp::RemoveCgroupRunInTestScope(config, second.cgroup_name);
    BOOST_TEST(!std::filesystem::exists(state_file));
    BOOST_TEST(!std::filesystem::exists(scope_root / "bbp"));
    BOOST_REQUIRE(std::filesystem::remove(scope_root, error));
    BOOST_REQUIRE(!error);
  } catch (...) {
    try {
      bbp::RemoveCgroupRunInTestScope(config, first.cgroup_name);
    } catch (const std::exception&) {
    }
    try {
      bbp::RemoveCgroupRunInTestScope(config, second.cgroup_name);
    } catch (const std::exception&) {
    }
    std::filesystem::remove(scope_root, error);
    std::filesystem::remove_all(ownership_roots);
    throw;
  }
  std::filesystem::remove_all(ownership_roots);
}

BOOST_AUTO_TEST_CASE(cgroup_scope_prepare_failure_restores_partial_mutations) {
  const std::string suffix = UniqueRunId("scope-rollback");
  const std::filesystem::path scope_root =
      std::filesystem::path("/sys/fs/cgroup/bbp") / suffix;
  const std::filesystem::path simulator = scope_root / "bbp";
  const std::string run_id = "preexisting-run";
  const std::filesystem::path state_file = TestDirectory("rollback-state");
  std::error_code error;
  std::filesystem::remove(state_file, error);
  error.clear();
  if (!std::filesystem::create_directory(scope_root, error)) {
    BOOST_TEST_MESSAGE(
        "skipping real cgroup rollback test: " << error.message());
    return;
  }
  std::filesystem::create_directory(simulator);
  std::filesystem::create_directory(simulator / run_id);

  const pid_t pid = fork();
  BOOST_REQUIRE(pid >= 0);
  if (pid == 0) {
    for (;;) {
      pause();
    }
  }
  ChildGuard child(pid);
  if (!TryAttachPrivilegedTestProcess(scope_root, pid,
                                      "real cgroup rollback test")) {
    std::filesystem::remove(simulator / run_id, error);
    error.clear();
    std::filesystem::remove(simulator, error);
    error.clear();
    std::filesystem::remove(scope_root, error);
    return;
  }
  try {
    const std::string process_cgroup_before =
        bbp::ReadText("/proc/" + std::to_string(pid) + "/cgroup");
    const std::string root_controllers_before =
        bbp::ReadText(scope_root / "cgroup.subtree_control");
    const std::string simulator_controllers_before =
        bbp::ReadText(simulator / "cgroup.subtree_control");
    const bbp::CgroupScopeTestConfig config{
        .root = scope_root,
        .simulator_name = "bbp",
        .state_file = state_file,
    };

    BOOST_CHECK_EXCEPTION(
        bbp::PrepareCgroupRunInTestScope(config, run_id), std::runtime_error,
        [](const std::runtime_error& failure) {
          return std::string(failure.what())
                     .find("refusing to adopt pre-existing run cgroup") !=
                 std::string::npos;
        });
    BOOST_TEST(!std::filesystem::exists(state_file));
    BOOST_TEST(std::filesystem::exists(simulator / run_id));
    BOOST_TEST(bbp::ReadText(scope_root / "cgroup.subtree_control") ==
               root_controllers_before);
    BOOST_TEST(bbp::ReadText(simulator / "cgroup.subtree_control") ==
               simulator_controllers_before);
    BOOST_TEST(bbp::ReadText("/proc/" + std::to_string(pid) + "/cgroup") ==
               process_cgroup_before);

    kill(pid, SIGKILL);
    const int status = child.Wait();
    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_TEST(WTERMSIG(status) == SIGKILL);
    BOOST_REQUIRE(std::filesystem::remove(simulator / run_id, error));
    BOOST_REQUIRE(!error);
    error.clear();
    BOOST_REQUIRE(std::filesystem::remove(simulator, error));
    BOOST_REQUIRE(!error);
    error.clear();
    BOOST_REQUIRE(std::filesystem::remove(scope_root, error));
    BOOST_REQUIRE(!error);
  } catch (...) {
    throw;
  }
}
