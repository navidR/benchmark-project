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

#include "bbp/block_production_policy.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/simulation_cancelled.h"

namespace {

using namespace std::chrono_literals;

}  // namespace

BOOST_AUTO_TEST_CASE(probabilistic_block_scheduler_produces_and_stops) {
  std::mutex mutex;
  std::condition_variable produced;
  std::vector<std::string> miners;
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1", "node-2"}, bbp::BlockProductionPolicy(2ms, 1.0, 9U),
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
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bbp::BlockProductionPolicy(2ms, 0.0, 1U),
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
  scheduler.UpdatePolicy(bbp::BlockProductionPolicy(2ms, 1.0, 2U));
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
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bbp::BlockProductionPolicy(1ms, 1.0, 1U),
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
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bbp::BlockProductionPolicy(1ms, 1.0, 1U),
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

BOOST_AUTO_TEST_CASE(probabilistic_block_scheduler_resumes_stopped_miner) {
  std::mutex mutex;
  std::condition_variable produced;
  std::size_t production_count = 0U;
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bbp::BlockProductionPolicy(2ms, 1.0, 1U),
      [&](const std::string&) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          ++production_count;
        }
        produced.notify_all();
      },
      [](const std::string&, std::string_view) {
        BOOST_FAIL("block production should not fail");
      });

  scheduler.StopMiner("node-1");
  scheduler.Start();
  std::this_thread::sleep_for(15ms);
  {
    std::lock_guard<std::mutex> lock(mutex);
    BOOST_TEST(production_count == 0U);
  }
  scheduler.StartMiner("node-1");
  {
    std::unique_lock<std::mutex> lock(mutex);
    BOOST_REQUIRE(produced.wait_for(
        lock, 1s, [&production_count] { return production_count > 0U; }));
  }
  scheduler.Stop();

  BOOST_CHECK_THROW(scheduler.StartMiner("unknown"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    probabilistic_block_scheduler_prepares_and_commits_miner_addition) {
  std::mutex mutex;
  std::condition_variable produced;
  std::vector<std::string> miners;
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bbp::BlockProductionPolicy(1ms, 1.0, 7U),
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

  {
    auto abandoned = scheduler.PrepareAddMiners({"node-2"});
    static_cast<void>(abandoned);
  }
  BOOST_CHECK_THROW(scheduler.StopMiner("node-2"), std::runtime_error);

  scheduler.StopMiner("node-1");
  auto prepared = scheduler.PrepareAddMiners({"node-2"});
  prepared.Commit();
  scheduler.Start();
  {
    std::unique_lock<std::mutex> lock(mutex);
    BOOST_REQUIRE(
        produced.wait_for(lock, 1s, [&miners] { return !miners.empty(); }));
  }
  scheduler.Stop();
  BOOST_TEST(miners.front() == "node-2");
  BOOST_CHECK_THROW(scheduler.PrepareAddMiners({"node-2"}),
                    std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(
    probabilistic_block_scheduler_prepares_removes_and_readds_miners) {
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1", "node-2"}, bbp::BlockProductionPolicy(1ms, 0.0, 7U),
      [](const std::string&) {},
      [](const std::string&, std::string_view) {
        BOOST_FAIL("block production should not fail");
      });

  {
    auto abandoned = scheduler.PrepareRemoveMiners({"node-2"});
    static_cast<void>(abandoned);
  }
  BOOST_TEST(scheduler.StopMiner("node-2"));
  scheduler.StartMiner("node-2");

  auto prepared = scheduler.PrepareRemoveMiners({"node-2"});
  prepared.Commit();
  BOOST_CHECK_THROW(scheduler.StopMiner("node-2"), std::runtime_error);

  auto readded = scheduler.PrepareAddMiners({"node-2"});
  readded.Commit();
  BOOST_TEST(scheduler.StopMiner("node-2"));
  BOOST_CHECK_THROW(scheduler.PrepareRemoveMiners({"node-1", "node-1"}),
                    std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(
    probabilistic_block_scheduler_cancels_in_flight_miner_removal) {
  std::mutex mutex;
  std::condition_variable state_changed;
  bool handler_started = false;
  bool release_handler = false;
  bbp::ProbabilisticBlockScheduler scheduler(
      {"node-1"}, bbp::BlockProductionPolicy(1ms, 1.0, 1U),
      [&](const std::string&) {
        std::unique_lock<std::mutex> lock(mutex);
        handler_started = true;
        state_changed.notify_all();
        state_changed.wait(lock,
                           [&release_handler] { return release_handler; });
      },
      [](const std::string&, std::string_view) {
        BOOST_FAIL("block production should not fail");
      });

  scheduler.Start();
  {
    std::unique_lock<std::mutex> lock(mutex);
    BOOST_REQUIRE(state_changed.wait_for(
        lock, 1s, [&handler_started] { return handler_started; }));
  }

  std::stop_source cancellation;
  std::exception_ptr removal_failure;
  std::thread remove_thread([&] {
    try {
      static_cast<void>(
          scheduler.PrepareRemoveMiners({"node-1"}, cancellation.get_token()));
    } catch (...) {
      removal_failure = std::current_exception();
    }
  });
  std::this_thread::sleep_for(20ms);
  cancellation.request_stop();
  remove_thread.join();
  BOOST_REQUIRE(removal_failure);
  BOOST_CHECK_EXCEPTION(std::rethrow_exception(removal_failure),
                        bbp::SimulationCancelled,
                        [](const bbp::SimulationCancelled&) { return true; });

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_handler = true;
  }
  state_changed.notify_all();
  scheduler.Stop();
  BOOST_TEST(scheduler.StopMiner("node-1"));
}
