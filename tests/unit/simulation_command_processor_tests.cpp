#include <atomic>
#include <boost/test/unit_test.hpp>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/run_process_state.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_processor.h"

BOOST_AUTO_TEST_CASE(simulation_command_processor_consumes_queued_commands) {
  bbp::SimulationCommandQueue queue;
  std::vector<bbp::SimulationCommand> handled;
  std::vector<std::string> failures;
  std::promise<void> handled_all;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&handled, &handled_all](const bbp::SimulationCommand& command) {
        handled.push_back(command);
        if (handled.size() == 2U) {
          handled_all.set_value();
        }
      },
      [&failures](const bbp::SimulationCommand&, std::string_view detail) {
        failures.emplace_back(detail);
      });

  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-1", true);
  queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-2", true);
  processor.Start();
  handled_all.get_future().wait();
  processor.Stop();

  BOOST_TEST(handled.size() == 2U);
  BOOST_TEST(handled[0].node_id == "firo-1");
  BOOST_TEST(handled[1].node_id == "firo-2");
  BOOST_TEST(failures.empty());
}

BOOST_AUTO_TEST_CASE(simulation_command_processor_reports_and_continues) {
  bbp::SimulationCommandQueue queue;
  std::vector<std::uint64_t> handled;
  std::vector<std::pair<std::uint64_t, std::string>> failures;
  std::promise<void> handled_all;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&handled, &handled_all](const bbp::SimulationCommand& command) {
        if (command.sequence == 1U) {
          throw std::runtime_error("expected failure");
        }
        handled.push_back(command.sequence);
        handled_all.set_value();
      },
      [&failures](const bbp::SimulationCommand& command,
                  std::string_view detail) {
        failures.emplace_back(command.sequence, detail);
      });

  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-1", true);
  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-2", true);
  processor.Start();
  handled_all.get_future().wait();
  processor.Stop();

  BOOST_TEST(handled.size() == 1U);
  BOOST_TEST(handled[0] == 2U);
  BOOST_TEST(failures.size() == 1U);
  BOOST_TEST(failures[0].first == 1U);
  BOOST_TEST(failures[0].second == "expected failure");
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_rejects_cancelled_operation_before_handler) {
  bbp::SimulationCommandQueue queue;
  std::atomic_bool handler_called = false;
  std::atomic_bool failure_reported = false;
  std::promise<bbp::SimulationCommandOutcome> outcome;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&handler_called](const bbp::SimulationCommand&) {
        handler_called = true;
      },
      [&failure_reported](const bbp::SimulationCommand&, std::string_view) {
        failure_reported = true;
      },
      [&outcome](const bbp::SimulationCommand&,
                 const bbp::SimulationCommandOutcome& result) {
        outcome.set_value(result);
      });
  bbp::SimulationCommand command;
  command.kind = bbp::SimulationCommandKind::kKillNode;
  command.node_id = "firo-1";
  command.confirmed = true;
  command.operation_control = std::make_shared<bbp::SimulationCommandControl>();
  BOOST_REQUIRE(command.operation_control->RequestCancellation(
      bbp::SimulationCommandCancellationCause::kClientCancel));
  queue.PushRuntimeCommand(std::move(command));

  processor.Start();
  const bbp::SimulationCommandOutcome result = outcome.get_future().get();
  processor.Stop();

  BOOST_TEST(!handler_called);
  BOOST_TEST(!failure_reported);
  BOOST_CHECK(result.state == bbp::SimulationCommandOutcomeState::kCancelled);
  BOOST_CHECK(result.cancellation_cause ==
              bbp::SimulationCommandCancellationCause::kClientCancel);
  BOOST_REQUIRE(result.error);
  BOOST_TEST(*result.error ==
             "simulation command operation cancelled before execution");
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_preserves_outcome_unconfirmed_failure) {
  bbp::SimulationCommandQueue queue;
  std::atomic_bool failure_reported = false;
  std::promise<bbp::SimulationCommandOutcome> outcome;
  bbp::SimulationCommandProcessor processor(
      queue,
      [](const bbp::SimulationCommand& command) {
        command.operation_control->outcome_unconfirmed.store(
            true, std::memory_order_release);
        throw std::runtime_error("physical state read-back timed out");
      },
      [&failure_reported](const bbp::SimulationCommand&, std::string_view) {
        failure_reported = true;
      },
      [&outcome](const bbp::SimulationCommand&,
                 const bbp::SimulationCommandOutcome& result) {
        outcome.set_value(result);
      });
  bbp::SimulationCommand command;
  command.kind = bbp::SimulationCommandKind::kRestartNode;
  command.node_id = "firo-1";
  command.confirmed = true;
  command.operation_control = std::make_shared<bbp::SimulationCommandControl>();
  queue.PushRuntimeCommand(std::move(command));

  processor.Start();
  const bbp::SimulationCommandOutcome result = outcome.get_future().get();
  processor.Stop();

  BOOST_TEST(!failure_reported);
  BOOST_CHECK(result.state ==
              bbp::SimulationCommandOutcomeState::kOutcomeUnconfirmed);
  BOOST_REQUIRE(result.error);
  BOOST_TEST(*result.error == "physical state read-back timed out");
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_preserves_local_outcome_unconfirmed_failure) {
  bbp::SimulationCommandQueue queue;
  std::atomic_bool failure_reported = false;
  std::promise<bbp::SimulationCommandOutcome> outcome;
  bbp::SimulationCommandProcessor processor(
      queue,
      [](const bbp::SimulationCommand&) {
        throw bbp::SimulationCommandOutcomeUnconfirmed(
            "physical state read-back timed out");
      },
      [&failure_reported](const bbp::SimulationCommand&, std::string_view) {
        failure_reported = true;
      },
      [&outcome](const bbp::SimulationCommand&,
                 const bbp::SimulationCommandOutcome& result) {
        outcome.set_value(result);
      });
  queue.Push(bbp::SimulationCommandKind::kRestartNode, "firo-1");

  processor.Start();
  const bbp::SimulationCommandOutcome result = outcome.get_future().get();
  processor.Stop();

  BOOST_TEST(!failure_reported);
  BOOST_CHECK(result.state ==
              bbp::SimulationCommandOutcomeState::kOutcomeUnconfirmed);
  BOOST_REQUIRE(result.error);
  BOOST_TEST(*result.error == "physical state read-back timed out");
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_local_restart_lock_wait_is_cancellable) {
  bbp::SimulationCommandQueue queue;
  bbp::RunProcessState run_process_state;
  std::optional<bbp::RunProcessState::NativeMiningRpcGuard> held_guard =
      run_process_state.LockNativeMiningRpc();
  std::stop_source command_rpc_stop_source;
  std::promise<void> handler_started;
  std::atomic_bool restart_admitted = false;
  std::atomic_bool processor_stopped = false;
  std::promise<bbp::SimulationCommandOutcome> outcome;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&](const bbp::SimulationCommand&) {
        handler_started.set_value();
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        auto restart_intent = run_process_state.TryBeginNativeMiningRestart(
            "firo-1", deadline, command_rpc_stop_source.get_token());
        if (!restart_intent) {
          if (command_rpc_stop_source.stop_requested()) {
            throw bbp::SimulationCancelled();
          }
          throw std::runtime_error(
              "native mining RPC lock deadline expired before node restart");
        }
        restart_admitted = true;
      },
      [](const bbp::SimulationCommand&, std::string_view) {},
      [&outcome](const bbp::SimulationCommand&,
                 const bbp::SimulationCommandOutcome& result) {
        outcome.set_value(result);
      });
  queue.Push(bbp::SimulationCommandKind::kRestartNode, "firo-1");
  processor.Start();
  handler_started.get_future().wait();

  const auto cancellation_started_at = std::chrono::steady_clock::now();
  command_rpc_stop_source.request_stop();
  std::jthread stopper([&] {
    processor.Stop();
    processor_stopped.store(true, std::memory_order_release);
  });
  const auto completion_deadline =
      cancellation_started_at + std::chrono::milliseconds(500);
  while (!processor_stopped.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < completion_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool stopped_before_release =
      processor_stopped.load(std::memory_order_acquire);
  const auto cancellation_elapsed =
      std::chrono::steady_clock::now() - cancellation_started_at;
  held_guard.reset();
  stopper.join();
  const bbp::SimulationCommandOutcome result = outcome.get_future().get();

  BOOST_TEST(stopped_before_release);
  BOOST_TEST(cancellation_elapsed < std::chrono::seconds(1));
  BOOST_TEST(!restart_admitted);
  BOOST_CHECK(result.state == bbp::SimulationCommandOutcomeState::kCancelled);
}

BOOST_AUTO_TEST_CASE(simulation_command_processor_requires_handlers) {
  bbp::SimulationCommandQueue queue;
  BOOST_CHECK_THROW(
      bbp::SimulationCommandProcessor(
          queue, bbp::SimulationCommandProcessor::CommandHandler{},
          [](const bbp::SimulationCommand&, std::string_view) {}),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::SimulationCommandProcessor(
                        queue, [](const bbp::SimulationCommand&) {},
                        bbp::SimulationCommandProcessor::FailureHandler{}),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_preserves_resource_command_payload) {
  bbp::SimulationCommandQueue queue;
  std::promise<bbp::SimulationCommand> handled;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&handled](const bbp::SimulationCommand& command) {
        handled.set_value(command);
      },
      [](const bbp::SimulationCommand&, std::string_view) {});
  bbp::ResourceLimitPatch patch;
  patch.memory_max_bytes = 4096U;

  queue.PushResourceLimits("firo-2", patch, true);
  processor.Start();
  const bbp::SimulationCommand command = handled.get_future().get();
  processor.Stop();

  BOOST_CHECK(command.kind == bbp::SimulationCommandKind::kSetResourceLimits);
  BOOST_REQUIRE(command.resource_limit_patch);
  BOOST_CHECK(*command.resource_limit_patch == patch);
  BOOST_TEST(command.confirmed);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_reports_ordered_command_outcomes) {
  bbp::SimulationCommandQueue queue;
  std::vector<std::uint64_t> handled;
  std::vector<std::pair<std::uint64_t, std::optional<std::string>>> outcomes;
  std::promise<void> all_outcomes;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&handled](const bbp::SimulationCommand& command) {
        handled.push_back(command.sequence);
        if (command.sequence == 2U) {
          throw std::runtime_error("scheduled failure");
        }
      },
      [](const bbp::SimulationCommand&, std::string_view) {},
      [&outcomes, &all_outcomes](const bbp::SimulationCommand& command,
                                 const bbp::SimulationCommandOutcome& outcome) {
        outcomes.emplace_back(command.sequence, outcome.error);
        if (outcomes.size() == 3U) {
          all_outcomes.set_value();
        }
      });

  queue.Push(bbp::SimulationCommandKind::kIncreaseLogVerbosity, "firo-1");
  bbp::SimulationCommand failing;
  failing.kind = bbp::SimulationCommandKind::kRestartNode;
  failing.node_id = "firo-1";
  failing.confirmed = true;
  failing.scheduled_event_sequence = 1U;
  queue.PushScenarioCommand(std::move(failing));
  bbp::SimulationCommand succeeding;
  succeeding.kind = bbp::SimulationCommandKind::kExportNodeReport;
  succeeding.node_id = "firo-1";
  succeeding.confirmed = true;
  succeeding.scheduled_event_sequence = 2U;
  queue.PushScenarioCommand(std::move(succeeding));

  processor.Start();
  all_outcomes.get_future().wait();
  processor.Stop();

  BOOST_REQUIRE_EQUAL(handled.size(), 3U);
  BOOST_TEST(handled[0] == 1U);
  BOOST_TEST(handled[1] == 2U);
  BOOST_TEST(handled[2] == 3U);
  BOOST_REQUIRE_EQUAL(outcomes.size(), 3U);
  BOOST_TEST(outcomes[0].first == 1U);
  BOOST_TEST(!outcomes[0].second);
  BOOST_TEST(outcomes[1].first == 2U);
  BOOST_REQUIRE(outcomes[1].second);
  BOOST_TEST(*outcomes[1].second == "scheduled failure");
  BOOST_TEST(outcomes[2].first == 3U);
  BOOST_TEST(!outcomes[2].second);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_processor_cancels_pending_commands_in_fifo_order) {
  bbp::SimulationCommandQueue queue;
  std::promise<void> first_started;
  std::promise<void> release_first;
  const std::shared_future<void> release = release_first.get_future().share();
  std::vector<std::uint64_t> handled;
  std::vector<std::pair<std::uint64_t, std::string>> failures;
  std::vector<std::pair<std::uint64_t, std::optional<std::string>>> outcomes;
  bbp::SimulationCommandProcessor processor(
      queue,
      [&](const bbp::SimulationCommand& command) {
        handled.push_back(command.sequence);
        first_started.set_value();
        release.wait();
      },
      [&](const bbp::SimulationCommand& command, std::string_view error) {
        failures.emplace_back(command.sequence, error);
      },
      [&](const bbp::SimulationCommand& command,
          const bbp::SimulationCommandOutcome& outcome) {
        outcomes.emplace_back(command.sequence, outcome.error);
      });

  queue.Push(bbp::SimulationCommandKind::kIncreaseLogVerbosity, "firo-1");
  queue.Push(bbp::SimulationCommandKind::kExportNodeReport, "firo-2");
  queue.Push(bbp::SimulationCommandKind::kThawNode, "firo-3");
  processor.Start();
  first_started.get_future().wait();
  std::future<void> stopped =
      std::async(std::launch::async, [&] { processor.Stop(); });
  while (!queue.IsClosed()) {
    std::this_thread::yield();
  }
  release_first.set_value();
  stopped.get();

  BOOST_REQUIRE_EQUAL(handled.size(), 1U);
  BOOST_TEST(handled[0] == 1U);
  BOOST_TEST(failures.empty());
  BOOST_REQUIRE_EQUAL(outcomes.size(), 3U);
  BOOST_TEST(outcomes[0].first == 1U);
  BOOST_TEST(!outcomes[0].second);
  BOOST_TEST(outcomes[1].first == 2U);
  BOOST_REQUIRE(outcomes[1].second);
  BOOST_TEST(*outcomes[1].second ==
             "simulation command processor stopped before execution");
  BOOST_TEST(outcomes[2].first == 3U);
  BOOST_REQUIRE(outcomes[2].second);
  BOOST_TEST(*outcomes[2].second ==
             "simulation command processor stopped before execution");
}
