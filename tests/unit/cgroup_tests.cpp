#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <limits>
#include <string>

#include "bbp/cgroup.h"
#include "bbp/util.h"

namespace {

std::filesystem::path TestDirectory(std::string_view suffix) {
  return std::filesystem::temp_directory_path() /
         ("bbp-cgroup-test-" + std::to_string(getpid()) + "-" +
          std::string(suffix));
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
