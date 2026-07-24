#include <unistd.h>

#include <atomic>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bbp/run_process_state.h"
#include "bbp/simulator/node_runtime.h"

namespace {

bbp::ChildProcess SpawnStateTestChild(const std::filesystem::path& run_dir,
                                      std::uint64_t sequence,
                                      std::string duration) {
  bbp::ProcessSpec spec;
  spec.binary = "/bin/sleep";
  spec.argv = {std::move(duration)};
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / (std::to_string(sequence) + ".out");
  spec.stderr_path = run_dir / (std::to_string(sequence) + ".err");
  return bbp::ChildProcess::Spawn(spec, std::nullopt);
}

}  // namespace

BOOST_AUTO_TEST_CASE(run_process_state_serializes_miner_transition_stress) {
  bbp::RunProcessState state;
  constexpr std::uint32_t kIterations = 2000U;
  std::atomic<bool> start = false;
  std::vector<std::jthread> workers;
  workers.reserve(6U);

  for (std::uint32_t worker = 0; worker < 6U; ++worker) {
    workers.emplace_back([&, worker] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      const std::string node_id = "node-" + std::to_string(worker % 3U);
      for (std::uint32_t iteration = 0; iteration < kIterations; ++iteration) {
        auto guard = state.Lock();
        if ((iteration + worker) % 2U == 0U) {
          state.AddActiveNativeMiner(guard, node_id);
          state.PauseScheduledMiner(guard, node_id);
        } else {
          state.RemoveActiveNativeMiner(guard, node_id);
          static_cast<void>(state.ResumeScheduledMiner(guard, node_id));
        }
        static_cast<void>(state.IsActiveNativeMiner(guard, node_id));
        static_cast<void>(state.IsPausedScheduledMiner(guard, node_id));
      }
    });
  }

  start.store(true, std::memory_order_release);
  workers.clear();
  auto guard = state.Lock();
  const std::vector<std::string> active = state.ActiveNativeMiners(guard);
  BOOST_TEST(active.size() <= 3U);
}

BOOST_AUTO_TEST_CASE(
    run_process_state_reconciles_native_miner_after_restart_failure) {
  bbp::RunProcessState state;
  {
    auto mining_guard = state.LockNativeMiningRpc();
    auto guard = state.Lock();
    state.AddActiveNativeMiner(guard, "node-1");
    state.ReconcileActiveNativeMinerAfterRestartFailure(mining_guard, guard,
                                                        "node-1", false, true);
    BOOST_TEST(!state.IsActiveNativeMiner(guard, "node-1"));

    state.AddActiveNativeMiner(guard, "node-1");
    state.ReconcileActiveNativeMinerAfterRestartFailure(mining_guard, guard,
                                                        "node-1", false, false);
    BOOST_TEST(state.IsActiveNativeMiner(guard, "node-1"));

    state.ReconcileActiveNativeMinerAfterRestartFailure(mining_guard, guard,
                                                        "node-1", true, true);
    BOOST_TEST(state.IsActiveNativeMiner(guard, "node-1"));
  }
}

BOOST_AUTO_TEST_CASE(
    run_process_state_native_mining_reconciliation_lock_is_bounded) {
  bbp::RunProcessState state;
  std::atomic<bool> locked = false;
  std::atomic<bool> release = false;
  std::jthread holder([&] {
    auto mining_guard = state.LockNativeMiningRpc();
    locked.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  while (!locked.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  const auto started_at = std::chrono::steady_clock::now();
  const auto deadline = started_at + std::chrono::milliseconds(20);
  std::optional<bbp::RunProcessState::NativeMiningRpcGuard> timed_guard =
      state.TryLockNativeMiningRpcUntil(deadline);
  const auto elapsed = std::chrono::steady_clock::now() - started_at;
  BOOST_TEST(!timed_guard);
  BOOST_TEST(elapsed < std::chrono::milliseconds(250));

  std::stop_source stop_source;
  std::jthread canceler([&](std::stop_token) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    stop_source.request_stop();
  });
  const auto cancellation_started_at = std::chrono::steady_clock::now();
  timed_guard = state.TryLockNativeMiningRpcUntil(
      cancellation_started_at + std::chrono::milliseconds(250),
      stop_source.get_token());
  const auto cancellation_elapsed =
      std::chrono::steady_clock::now() - cancellation_started_at;
  BOOST_TEST(!timed_guard);
  BOOST_TEST(cancellation_elapsed < std::chrono::milliseconds(200));
  canceler.join();

  release.store(true, std::memory_order_release);
  holder.join();
  timed_guard = state.TryLockNativeMiningRpcUntil(
      std::chrono::steady_clock::now() + std::chrono::milliseconds(50));
  BOOST_REQUIRE(timed_guard);
  timed_guard.reset();
  timed_guard = state.TryLockNativeMiningRpcUntil(
      std::chrono::steady_clock::now() - std::chrono::milliseconds(1));
  BOOST_TEST(!timed_guard);
}

BOOST_AUTO_TEST_CASE(run_process_state_cleanup_lock_is_bounded) {
  bbp::RunProcessState state;
  std::atomic<bool> locked = false;
  std::atomic<bool> release = false;
  std::jthread holder([&] {
    auto guard = state.Lock();
    locked.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  while (!locked.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  const auto started_at = std::chrono::steady_clock::now();
  std::optional<bbp::RunProcessState::Guard> timed_guard =
      state.TryLockUntil(started_at + std::chrono::milliseconds(20));
  BOOST_TEST(!timed_guard);
  const auto lock_elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now() - started_at)
                                   .count();
  BOOST_TEST(lock_elapsed_ns <
             std::chrono::duration_cast<std::chrono::nanoseconds>(
                 std::chrono::milliseconds(250))
                 .count());

  std::stop_source stop_source;
  std::jthread canceler([&](std::stop_token) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    stop_source.request_stop();
  });
  const auto cancellation_started_at = std::chrono::steady_clock::now();
  timed_guard = state.TryLockUntil(
      cancellation_started_at + std::chrono::milliseconds(250),
      stop_source.get_token());
  BOOST_TEST(!timed_guard);
  const auto cancellation_elapsed_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - cancellation_started_at)
          .count();
  BOOST_TEST(cancellation_elapsed_ns <
             std::chrono::duration_cast<std::chrono::nanoseconds>(
                 std::chrono::milliseconds(200))
                 .count());

  release.store(true, std::memory_order_release);
  holder.join();
  canceler.join();
  timed_guard = state.TryLockUntil(std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(50));
  BOOST_REQUIRE(timed_guard);
}

BOOST_AUTO_TEST_CASE(
    run_process_state_resume_snapshot_waits_for_native_mining_publication) {
  bbp::RunProcessState state;
  std::atomic<bool> rpc_started = false;
  std::atomic<bool> publish_miner = false;
  std::jthread start_rpc([&] {
    auto mining_guard = state.LockNativeMiningRpc();
    rpc_started.store(true, std::memory_order_release);
    while (!publish_miner.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    auto guard = state.Lock();
    state.AddActiveNativeMiner(guard, "node-1");
  });
  while (!rpc_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::jthread publisher([&](std::stop_token) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    publish_miner.store(true, std::memory_order_release);
  });

  std::optional<bbp::RunProcessState::NativeMiningRpcGuard> mining_guard =
      state.TryLockNativeMiningRpcUntil(std::chrono::steady_clock::now() +
                                        std::chrono::milliseconds(250));
  BOOST_REQUIRE(mining_guard);
  auto guard = state.Lock();
  BOOST_TEST(state.IsActiveNativeMiner(guard, "node-1"));
  publisher.join();
  start_rpc.join();
}

BOOST_AUTO_TEST_CASE(
    run_process_state_restart_intent_holds_late_mining_publication) {
  bbp::RunProcessState state;
  std::optional<bbp::RunProcessState::NativeMiningRestartIntent>
      restart_intent = state.TryBeginNativeMiningRestart(
          "node-1",
          std::chrono::steady_clock::now() + std::chrono::milliseconds(250));
  BOOST_REQUIRE(restart_intent);
  BOOST_TEST(!restart_intent->native_miner_active);

  std::atomic<bool> publisher_waiting = false;
  std::atomic<bool> restart_completed = false;
  std::atomic<bool> publication_preceded_restart = false;
  std::jthread publisher([&] {
    publisher_waiting.store(true, std::memory_order_release);
    auto mining_guard = state.LockNativeMiningRpc();
    publication_preceded_restart.store(
        !restart_completed.load(std::memory_order_acquire),
        std::memory_order_release);
    auto guard = state.Lock();
    state.AddActiveNativeMiner(guard, "node-1");
  });
  while (!publisher_waiting.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  {
    auto guard = state.Lock();
    BOOST_TEST(!state.IsActiveNativeMiner(guard, "node-1"));
  }

  restart_completed.store(true, std::memory_order_release);
  restart_intent.reset();
  publisher.join();
  BOOST_TEST(!publication_preceded_restart.load(std::memory_order_acquire));
  auto guard = state.Lock();
  BOOST_TEST(state.IsActiveNativeMiner(guard, "node-1"));
}

BOOST_AUTO_TEST_CASE(
    run_process_state_serializes_native_mining_generation_reconciliation) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-native-mining-generation-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);

  bbp::RunProcessState state;
  bbp::NodeRuntime node;
  node.config.id = "node-1";
  node.run_process_state = &state;
  {
    bbp::ChildProcess first = SpawnStateTestChild(run_dir, 1U, "10");
    auto guard = state.Lock();
    node.process = std::move(first);
    node.process_started_at = std::chrono::steady_clock::now();
    node.SetLifecycle(bbp::NodeRuntimeLifecycle::kRunning);
  }

  std::atomic<bool> old_rpc_started = false;
  std::atomic<bool> finish_old_rpc = false;
  std::atomic<bool> replacement_rpc_entered = false;
  std::atomic<bool> old_rpc_observed_replacement = false;
  std::atomic<bool> replacement_rpc_observed_current_process = false;
  std::atomic<bool> replacement_rpc_followed_reconciliation = false;
  std::atomic<std::uint32_t> reconciliation_order = 0U;
  pid_t original_pid = -1;
  std::jthread old_generation_rpc([&] {
    auto mining_guard = state.LockNativeMiningRpc();
    {
      auto guard = state.Lock();
      original_pid = node.process.pid();
    }
    old_rpc_started.store(true, std::memory_order_release);
    while (!finish_old_rpc.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    {
      auto guard = state.Lock();
      old_rpc_observed_replacement.store(node.process.pid() != original_pid,
                                         std::memory_order_release);
      state.RemoveActiveNativeMiner(guard, node.config.id);
    }
    reconciliation_order.store(1U, std::memory_order_release);
  });

  while (!old_rpc_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  {
    auto guard = state.Lock();
    static_cast<void>(node.process.RequestKill());
    BOOST_REQUIRE(node.process.WaitForExit(std::chrono::seconds(2)));
  }
  {
    bbp::ChildProcess replacement = SpawnStateTestChild(run_dir, 2U, "10");
    auto guard = state.Lock();
    node.process = std::move(replacement);
    node.process_started_at = std::chrono::steady_clock::now();
    node.IncrementRestartCount();
    node.SetLifecycle(bbp::NodeRuntimeLifecycle::kRunning);
  }

  std::jthread replacement_generation_rpc([&] {
    auto mining_guard = state.LockNativeMiningRpc();
    replacement_rpc_entered.store(true, std::memory_order_release);
    replacement_rpc_followed_reconciliation.store(
        reconciliation_order.load(std::memory_order_acquire) == 1U,
        std::memory_order_release);
    auto guard = state.Lock();
    replacement_rpc_observed_current_process.store(
        node.process.pid() != original_pid && node.process.running(),
        std::memory_order_release);
    state.AddActiveNativeMiner(guard, node.config.id);
    reconciliation_order.store(2U, std::memory_order_release);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  BOOST_TEST(!replacement_rpc_entered.load(std::memory_order_acquire));
  finish_old_rpc.store(true, std::memory_order_release);
  old_generation_rpc.join();
  replacement_generation_rpc.join();
  BOOST_TEST(old_rpc_observed_replacement.load(std::memory_order_acquire));
  BOOST_TEST(
      replacement_rpc_observed_current_process.load(std::memory_order_acquire));
  BOOST_TEST(
      replacement_rpc_followed_reconciliation.load(std::memory_order_acquire));
  {
    auto guard = state.Lock();
    BOOST_TEST(state.IsActiveNativeMiner(guard, node.config.id));
    static_cast<void>(node.process.RequestKill());
    BOOST_TEST(node.process.WaitForExit(std::chrono::seconds(2)));
  }
  BOOST_TEST(reconciliation_order.load(std::memory_order_acquire) == 2U);
  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(
    run_process_state_serializes_real_process_lifecycle_and_miner_stress) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-run-process-state-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);

  bbp::RunProcessState state;
  bbp::NodeRuntime node;
  node.config.id = "node-1";
  node.run_process_state = &state;
  std::atomic<std::uint64_t> child_sequence = 1U;
  {
    bbp::ChildProcess first =
        SpawnStateTestChild(run_dir, child_sequence.fetch_add(1U), "0.03");
    auto guard = state.Lock();
    node.process = std::move(first);
    node.process_started_at = std::chrono::steady_clock::now();
    node.perf_counter_target_pid = node.process.pid();
    node.perf_counter_attached_pid = node.process.pid();
    node.perf_counter_process_generation = 1U;
    node.perf_counter_cgroup_path = "/bbp/state-test";
    node.perf_counter_cpus = {0, 1};
    node.SetLifecycle(bbp::NodeRuntimeLifecycle::kRunning);
    state.AddActiveNativeMiner(guard, node.config.id);
  }

  std::atomic<bool> abort = false;
  std::atomic<bool> finish = false;
  std::atomic<std::uint32_t> natural_restarts = 0U;
  std::atomic<std::uint64_t> metric_snapshots = 0U;
  std::mutex failure_mutex;
  std::exception_ptr failure;
  const auto record_failure = [&](std::exception_ptr error) {
    std::lock_guard<std::mutex> lock(failure_mutex);
    if (!failure) {
      failure = std::move(error);
    }
    abort.store(true, std::memory_order_release);
  };
  const auto install_replacement = [&](bbp::ChildProcess replacement,
                                       bbp::NodeRuntimeLifecycle expected) {
    auto guard = state.Lock();
    if (node.Lifecycle() != expected || node.process.running()) {
      throw std::runtime_error("process replacement lost lifecycle ownership");
    }
    node.process = std::move(replacement);
    node.process_started_at = std::chrono::steady_clock::now();
    node.perf_counter_target_pid = node.process.pid();
    node.perf_counter_attached_pid = node.process.pid();
    ++node.perf_counter_process_generation;
    node.perf_counter_cgroup_path = "/bbp/state-test";
    node.perf_counter_cpus = {0, 1};
    node.perf_counter_error_kind.reset();
    node.perf_counter_error.clear();
    node.SetLifecycle(bbp::NodeRuntimeLifecycle::kRunning);
  };
  const auto wait_for_exit = [&](std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      {
        auto guard = state.Lock();
        if (!node.process.running()) {
          return true;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto guard = state.Lock();
    return !node.process.running();
  };

  std::jthread natural_exit_monitor([&] {
    try {
      while (!finish.load(std::memory_order_acquire) &&
             !abort.load(std::memory_order_acquire)) {
        bool owns_restart = false;
        {
          auto guard = state.Lock();
          if (node.Lifecycle() == bbp::NodeRuntimeLifecycle::kRunning &&
              !node.process.running()) {
            node.process_perf_counters.reset();
            node.cgroup_perf_counters.reset();
            node.perf_counter_target_pid = -1;
            node.perf_counter_attached_pid = -1;
            node.perf_counter_cgroup_path.clear();
            node.perf_counter_cpus.clear();
            node.SetLifecycle(bbp::NodeRuntimeLifecycle::kFailed);
            owns_restart = true;
          }
        }
        if (owns_restart) {
          bbp::ChildProcess replacement =
              SpawnStateTestChild(run_dir, child_sequence.fetch_add(1U), "1");
          install_replacement(std::move(replacement),
                              bbp::NodeRuntimeLifecycle::kFailed);
          natural_restarts.fetch_add(1U, std::memory_order_release);
        }
        std::this_thread::yield();
      }
    } catch (...) {
      record_failure(std::current_exception());
    }
  });

  std::jthread metrics_reader([&] {
    try {
      while (!finish.load(std::memory_order_acquire) &&
             !abort.load(std::memory_order_acquire)) {
        auto guard = state.Lock();
        const pid_t pid = node.process.pid();
        const bool running = node.process.running();
        const std::optional<int> status = node.process.exit_status();
        const auto started_at = node.process_started_at;
        const pid_t target_pid = node.perf_counter_target_pid;
        const pid_t attached_pid = node.perf_counter_attached_pid;
        const std::uint64_t generation = node.perf_counter_process_generation;
        const std::filesystem::path cgroup_path = node.perf_counter_cgroup_path;
        const std::vector<int> cpus = node.perf_counter_cpus;
        const bool active = state.IsActiveNativeMiner(guard, node.config.id);
        const bool paused = state.IsPausedScheduledMiner(guard, node.config.id);
        static_cast<void>(pid);
        static_cast<void>(running);
        static_cast<void>(status);
        static_cast<void>(started_at);
        static_cast<void>(target_pid);
        static_cast<void>(attached_pid);
        static_cast<void>(generation);
        static_cast<void>(cgroup_path);
        static_cast<void>(cpus);
        static_cast<void>(active);
        static_cast<void>(paused);
        metric_snapshots.fetch_add(1U, std::memory_order_relaxed);
      }
    } catch (...) {
      record_failure(std::current_exception());
    }
  });

  std::jthread miner_transitions([&] {
    try {
      std::uint64_t iteration = 0U;
      while (!finish.load(std::memory_order_acquire) &&
             !abort.load(std::memory_order_acquire)) {
        auto guard = state.Lock();
        if ((iteration++ & 1U) == 0U) {
          state.AddActiveNativeMiner(guard, node.config.id);
          state.PauseScheduledMiner(guard, node.config.id);
        } else {
          state.RemoveActiveNativeMiner(guard, node.config.id);
          static_cast<void>(state.ResumeScheduledMiner(guard, node.config.id));
        }
      }
    } catch (...) {
      record_failure(std::current_exception());
    }
  });

  const auto natural_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (natural_restarts.load(std::memory_order_acquire) == 0U &&
         !abort.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < natural_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (natural_restarts.load(std::memory_order_acquire) == 0U) {
    record_failure(std::make_exception_ptr(
        std::runtime_error("natural process exit was not restarted")));
  }

  constexpr std::uint32_t kOperationCount = 12U;
  const auto run_operations = [&](bbp::NodeRuntimeLifecycle operation,
                                  bool force_kill) {
    try {
      for (std::uint32_t iteration = 0U; iteration < kOperationCount &&
                                         !abort.load(std::memory_order_acquire);
           ++iteration) {
        bool owns_operation = false;
        while (!owns_operation && !abort.load(std::memory_order_acquire)) {
          {
            auto guard = state.Lock();
            if (node.Lifecycle() == bbp::NodeRuntimeLifecycle::kRunning &&
                node.process.running()) {
              node.SetLifecycle(operation);
              node.process_perf_counters.reset();
              node.cgroup_perf_counters.reset();
              node.perf_counter_target_pid = -1;
              node.perf_counter_attached_pid = -1;
              node.perf_counter_cgroup_path.clear();
              node.perf_counter_cpus.clear();
              if (force_kill) {
                static_cast<void>(node.process.RequestKill());
              } else {
                static_cast<void>(node.process.RequestTerminate());
              }
              owns_operation = true;
            }
          }
          if (!owns_operation) {
            std::this_thread::yield();
          }
        }
        if (!owns_operation) {
          return;
        }
        if (!wait_for_exit(std::chrono::seconds(2))) {
          {
            auto guard = state.Lock();
            static_cast<void>(node.process.RequestKill());
          }
          if (!wait_for_exit(std::chrono::seconds(2))) {
            throw std::runtime_error("state-test child survived SIGKILL");
          }
        }
        bbp::ChildProcess replacement =
            SpawnStateTestChild(run_dir, child_sequence.fetch_add(1U), "1");
        {
          auto guard = state.Lock();
          node.SetLifecycle(
              operation == bbp::NodeRuntimeLifecycle::kStopping
                  ? bbp::NodeRuntimeLifecycle::kStopped
                  : (operation == bbp::NodeRuntimeLifecycle::kKilling
                         ? bbp::NodeRuntimeLifecycle::kKilled
                         : bbp::NodeRuntimeLifecycle::kFailed));
        }
        const bbp::NodeRuntimeLifecycle expected = node.Lifecycle();
        install_replacement(std::move(replacement), expected);
      }
    } catch (...) {
      record_failure(std::current_exception());
    }
  };

  std::jthread restart_worker(
      [&] { run_operations(bbp::NodeRuntimeLifecycle::kRestarting, false); });
  std::jthread stop_worker(
      [&] { run_operations(bbp::NodeRuntimeLifecycle::kStopping, false); });
  std::jthread kill_worker(
      [&] { run_operations(bbp::NodeRuntimeLifecycle::kKilling, true); });
  restart_worker.join();
  stop_worker.join();
  kill_worker.join();
  finish.store(true, std::memory_order_release);
  natural_exit_monitor.join();
  metrics_reader.join();
  miner_transitions.join();

  {
    auto guard = state.Lock();
    if (node.process.running()) {
      static_cast<void>(node.process.RequestKill());
    }
  }
  static_cast<void>(wait_for_exit(std::chrono::seconds(2)));
  {
    auto guard = state.Lock();
    BOOST_TEST(!node.process.running());
  }
  std::filesystem::remove_all(run_dir);

  if (failure) {
    std::rethrow_exception(failure);
  }
  BOOST_TEST(natural_restarts.load(std::memory_order_acquire) >= 1U);
  BOOST_TEST(metric_snapshots.load(std::memory_order_relaxed) > 0U);
}
