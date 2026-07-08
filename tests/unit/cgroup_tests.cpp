#include "benchmark_sim/cgroup.h"
#include "benchmark_sim/util.h"

#include <unistd.h>

#include <filesystem>
#include <string>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(cgroup_metrics_read_io_and_pressure_totals) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("benchmark-sim-cgroup-test-" + std::to_string(getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  bsim::WriteText(dir / "cpu.stat",
                  "usage_usec 100\n"
                  "throttled_usec 7\n");
  bsim::WriteText(dir / "cpu.pressure",
                  "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\n"
                  "full avg10=0.00 avg60=0.00 avg300=0.00 total=56\n");
  bsim::WriteText(dir / "memory.current", "2048\n");
  bsim::WriteText(dir / "memory.peak", "4096\n");
  bsim::WriteText(dir / "memory.events",
                  "low 0\n"
                  "high 0\n"
                  "max 0\n"
                  "oom 2\n"
                  "oom_kill 1\n");
  bsim::WriteText(dir / "io.stat",
                  "8:0 rbytes=10 wbytes=20 rios=1 wios=2 dbytes=0 dios=0\n"
                  "8:16 rbytes=30 wbytes=40 rios=3 wios=4 dbytes=0 dios=0\n");
  bsim::WriteText(dir / "io.pressure",
                  "some avg10=0.00 avg60=0.00 avg300=0.00 total=77\n"
                  "full avg10=0.00 avg60=0.00 avg300=0.00 total=8\n");
  bsim::WriteText(dir / "pids.current", "3\n");

  const bsim::CgroupMetrics metrics = bsim::Cgroup(dir).ReadMetrics();

  BOOST_TEST(metrics.cpu_usage_usec == 100U);
  BOOST_TEST(metrics.cpu_throttled_usec == 7U);
  BOOST_TEST(metrics.cpu_pressure_some_total_usec == 1234U);
  BOOST_TEST(metrics.cpu_pressure_full_total_usec == 56U);
  BOOST_TEST(metrics.memory_current == 2048U);
  BOOST_TEST(metrics.memory_peak == 4096U);
  BOOST_TEST(metrics.io_read_bytes == 40U);
  BOOST_TEST(metrics.io_write_bytes == 60U);
  BOOST_TEST(metrics.io_pressure_some_total_usec == 77U);
  BOOST_TEST(metrics.io_pressure_full_total_usec == 8U);
  BOOST_TEST(metrics.pids_current == 3U);
  BOOST_TEST(metrics.oom == 2U);
  BOOST_TEST(metrics.oom_kill == 1U);

  std::filesystem::remove_all(dir);
}
