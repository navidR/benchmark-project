#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "benchmark_sim/simulation_command_processor.h"

BOOST_AUTO_TEST_CASE(simulation_command_processor_consumes_queued_commands) {
  bsim::SimulationCommandQueue queue;
  std::vector<bsim::SimulationCommand> handled;
  std::vector<std::string> failures;
  bsim::SimulationCommandProcessor processor(
      queue,
      [&handled](const bsim::SimulationCommand& command) {
        handled.push_back(command);
      },
      [&failures](const bsim::SimulationCommand&, std::string_view detail) {
        failures.emplace_back(detail);
      });

  queue.Push(bsim::SimulationCommandKind::kDisconnectNode, "firo-1");
  queue.Push(bsim::SimulationCommandKind::kKillNode, "firo-2");
  processor.Start();
  processor.Stop();

  BOOST_TEST(handled.size() == 2U);
  BOOST_TEST(handled[0].node_id == "firo-1");
  BOOST_TEST(handled[1].node_id == "firo-2");
  BOOST_TEST(failures.empty());
}

BOOST_AUTO_TEST_CASE(simulation_command_processor_reports_and_continues) {
  bsim::SimulationCommandQueue queue;
  std::vector<std::uint64_t> handled;
  std::vector<std::pair<std::uint64_t, std::string>> failures;
  bsim::SimulationCommandProcessor processor(
      queue,
      [&handled](const bsim::SimulationCommand& command) {
        if (command.sequence == 1U) {
          throw std::runtime_error("expected failure");
        }
        handled.push_back(command.sequence);
      },
      [&failures](const bsim::SimulationCommand& command,
                  std::string_view detail) {
        failures.emplace_back(command.sequence, detail);
      });

  queue.Push(bsim::SimulationCommandKind::kDisconnectNode, "firo-1");
  queue.Push(bsim::SimulationCommandKind::kDisconnectNode, "firo-2");
  processor.Start();
  processor.Stop();

  BOOST_TEST(handled.size() == 1U);
  BOOST_TEST(handled[0] == 2U);
  BOOST_TEST(failures.size() == 1U);
  BOOST_TEST(failures[0].first == 1U);
  BOOST_TEST(failures[0].second == "expected failure");
}

BOOST_AUTO_TEST_CASE(simulation_command_processor_requires_handlers) {
  bsim::SimulationCommandQueue queue;
  BOOST_CHECK_THROW(
      bsim::SimulationCommandProcessor(
          queue, bsim::SimulationCommandProcessor::CommandHandler{},
          [](const bsim::SimulationCommand&, std::string_view) {}),
      std::runtime_error);
  BOOST_CHECK_THROW(bsim::SimulationCommandProcessor(
                        queue, [](const bsim::SimulationCommand&) {},
                        bsim::SimulationCommandProcessor::FailureHandler{}),
                    std::runtime_error);
}
