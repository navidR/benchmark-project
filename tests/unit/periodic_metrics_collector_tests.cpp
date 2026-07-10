#include <atomic>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <stdexcept>
#include <vector>

#include "benchmark_sim/periodic_metrics_collector.h"

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_collects_requested_samples) {
  std::vector<std::uint32_t> samples;
  bsim::PeriodicMetricsCollector collector(
      3U, std::chrono::milliseconds(1),
      [&samples](std::uint32_t sample) { samples.push_back(sample); });

  collector.Start();
  collector.Wait();

  BOOST_REQUIRE_EQUAL(samples.size(), 3U);
  BOOST_TEST(samples[0] == 1U);
  BOOST_TEST(samples[1] == 2U);
  BOOST_TEST(samples[2] == 3U);
}

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_stops_before_next_sample) {
  std::atomic<std::uint32_t> sample_count = 0U;
  bsim::PeriodicMetricsCollector collector(
      1U, std::chrono::seconds(10),
      [&sample_count](std::uint32_t) { ++sample_count; });

  collector.Start();
  collector.Stop();

  BOOST_TEST(sample_count.load() == 0U);
}

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_propagates_sample_failure) {
  bsim::PeriodicMetricsCollector collector(
      1U, std::chrono::milliseconds(1),
      [](std::uint32_t) { throw std::runtime_error("sample failed"); });

  collector.Start();
  BOOST_CHECK_EXCEPTION(collector.Wait(), std::runtime_error,
                        [](const std::runtime_error& error) {
                          return std::string(error.what()) == "sample failed";
                        });
}
