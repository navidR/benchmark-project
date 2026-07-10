#include <atomic>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <vector>

#include "bbp/periodic_metrics_collector.h"

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_collects_requested_samples) {
  std::vector<std::uint32_t> samples;
  bbp::PeriodicMetricsCollector collector(
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
  bbp::PeriodicMetricsCollector collector(
      1U, std::chrono::seconds(10),
      [&sample_count](std::uint32_t) { ++sample_count; });

  collector.Start();
  collector.Stop();

  BOOST_TEST(sample_count.load() == 0U);
}

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_propagates_sample_failure) {
  bbp::PeriodicMetricsCollector collector(
      1U, std::chrono::milliseconds(1),
      [](std::uint32_t) { throw std::runtime_error("sample failed"); });

  collector.Start();
  BOOST_CHECK_EXCEPTION(collector.Wait(), std::runtime_error,
                        [](const std::runtime_error& error) {
                          return std::string(error.what()) == "sample failed";
                        });
}

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_runs_until_external_stop) {
  std::atomic<std::uint32_t> sample_count = 0U;
  std::atomic<bool> stop_requested = false;
  bbp::PeriodicMetricsCollector collector(
      0U, std::chrono::milliseconds(1),
      [&sample_count, &stop_requested](std::uint32_t) {
        if (++sample_count == 3U) {
          stop_requested = true;
        }
      },
      [&stop_requested] { return stop_requested.load(); });

  collector.Start();
  collector.Wait();

  BOOST_TEST(sample_count.load() == 3U);
}

BOOST_AUTO_TEST_CASE(periodic_metrics_collector_external_stop_is_prompt) {
  std::atomic<std::uint32_t> sample_count = 0U;
  std::atomic<bool> stop_requested = false;
  bbp::PeriodicMetricsCollector collector(
      0U, std::chrono::hours(24),
      [&sample_count](std::uint32_t) { ++sample_count; },
      [&stop_requested] { return stop_requested.load(); });

  collector.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  const auto stop_started = std::chrono::steady_clock::now();
  stop_requested = true;
  collector.Wait();
  const auto stop_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - stop_started);

  BOOST_TEST(sample_count.load() == 0U);
  BOOST_TEST(stop_duration.count() < 500);
}
