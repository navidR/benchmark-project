#include <boost/test/unit_test.hpp>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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
                                 std::optional<std::string_view> error) {
        outcomes.emplace_back(
            command.sequence,
            error ? std::optional<std::string>(*error) : std::nullopt);
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
    simulation_command_processor_fails_pending_commands_in_fifo_order) {
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
          std::optional<std::string_view> error) {
        outcomes.emplace_back(
            command.sequence,
            error ? std::optional<std::string>(*error) : std::nullopt);
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
  BOOST_REQUIRE_EQUAL(failures.size(), 2U);
  BOOST_TEST(failures[0].first == 2U);
  BOOST_TEST(failures[1].first == 3U);
  BOOST_TEST(failures[0].second ==
             "simulation command processor stopped before execution");
  BOOST_TEST(failures[1].second == failures[0].second);
  BOOST_REQUIRE_EQUAL(outcomes.size(), 3U);
  BOOST_TEST(outcomes[0].first == 1U);
  BOOST_TEST(!outcomes[0].second);
  BOOST_TEST(outcomes[1].first == 2U);
  BOOST_REQUIRE(outcomes[1].second);
  BOOST_TEST(*outcomes[1].second == failures[0].second);
  BOOST_TEST(outcomes[2].first == 3U);
  BOOST_REQUIRE(outcomes[2].second);
  BOOST_TEST(*outcomes[2].second == failures[1].second);
}
