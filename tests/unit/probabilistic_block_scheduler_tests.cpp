#include <atomic>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "benchmark_sim/block_production_policy.h"
#include "benchmark_sim/probabilistic_block_scheduler.h"

namespace {

using namespace std::chrono_literals;

}  // namespace

BOOST_AUTO_TEST_CASE(probabilistic_block_scheduler_produces_and_stops) {
  std::mutex mutex;
  std::condition_variable produced;
  std::vector<std::string> miners;
  bsim::ProbabilisticBlockScheduler scheduler(
      {"node-1", "node-2"}, bsim::BlockProductionPolicy(2ms, 1.0, 9U),
      [&](const std::string& node_id) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          miners.push_back(node_id);
        }
        produced.notify_all();
      },
      [](const std::string&, std::string_view) {
        BOOST_FAIL("block production should not fail");
      });

  scheduler.Start();
  {
    std::unique_lock<std::mutex> lock(mutex);
    BOOST_REQUIRE(
        produced.wait_for(lock, 1s, [&miners] { return miners.size() >= 3U; }));
  }
  scheduler.Stop();

  std::lock_guard<std::mutex> lock(mutex);
  BOOST_TEST(miners.size() >= 3U);
  for (const std::string& miner : miners) {
    BOOST_TEST((miner == "node-1" || miner == "node-2"));
  }
}

BOOST_AUTO_TEST_CASE(probabilistic_block_scheduler_updates_policy) {
  std::mutex mutex;
  std::condition_variable produced;
  std::size_t count = 0;
  bsim::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bsim::BlockProductionPolicy(2ms, 0.0, 1U),
      [&](const std::string&) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          ++count;
        }
        produced.notify_all();
      },
      [](const std::string&, std::string_view) {
        BOOST_FAIL("block production should not fail");
      });

  scheduler.Start();
  scheduler.UpdatePolicy(bsim::BlockProductionPolicy(2ms, 1.0, 2U));
  {
    std::unique_lock<std::mutex> lock(mutex);
    BOOST_REQUIRE(
        produced.wait_for(lock, 1s, [&count] { return count >= 1U; }));
  }
  scheduler.StopMiner("node-1");
  scheduler.Stop();

  BOOST_CHECK_THROW(scheduler.StopMiner("unknown"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    probabilistic_block_scheduler_propagates_failure_handler_errors) {
  std::mutex mutex;
  std::condition_variable failure_reported;
  bool failed = false;
  bsim::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bsim::BlockProductionPolicy(1ms, 1.0, 1U),
      [](const std::string&) { throw std::runtime_error("production failed"); },
      [&](const std::string&, std::string_view) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          failed = true;
        }
        failure_reported.notify_all();
        throw std::runtime_error("failure reporting failed");
      });

  scheduler.Start();
  {
    std::unique_lock<std::mutex> lock(mutex);
    BOOST_REQUIRE(
        failure_reported.wait_for(lock, 1s, [&failed] { return failed; }));
  }
  BOOST_CHECK_EXCEPTION(scheduler.Stop(), std::runtime_error,
                        [](const std::runtime_error& error) {
                          return std::string(error.what()) ==
                                 "failure reporting failed";
                        });
}

BOOST_AUTO_TEST_CASE(
    probabilistic_block_scheduler_stop_miner_waits_for_in_flight_block) {
  std::mutex mutex;
  std::condition_variable state_changed;
  bool handler_started = false;
  bool release_handler = false;
  std::atomic<std::uint32_t> production_count = 0U;
  std::atomic<bool> stop_returned = false;
  bsim::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bsim::BlockProductionPolicy(1ms, 1.0, 1U),
      [&](const std::string&) {
        std::unique_lock<std::mutex> lock(mutex);
        ++production_count;
        handler_started = true;
        state_changed.notify_all();
        state_changed.wait(lock,
                           [&release_handler] { return release_handler; });
      },
      [](const std::string&, std::string_view) {
        BOOST_FAIL("block production should not fail");
      });

  scheduler.Start();
  bool observed_handler = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    observed_handler = state_changed.wait_for(
        lock, 1s, [&handler_started] { return handler_started; });
  }
  if (!observed_handler) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      release_handler = true;
    }
    state_changed.notify_all();
    scheduler.Stop();
    BOOST_FAIL("block production handler did not start");
    return;
  }

  std::thread stop_thread([&] {
    scheduler.StopMiner("node-1");
    stop_returned = true;
  });
  std::this_thread::sleep_for(20ms);
  BOOST_TEST(!stop_returned.load());
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_handler = true;
  }
  state_changed.notify_all();
  stop_thread.join();
  std::this_thread::sleep_for(10ms);
  scheduler.Stop();

  BOOST_TEST(stop_returned.load());
  BOOST_TEST(production_count.load() == 1U);
}
