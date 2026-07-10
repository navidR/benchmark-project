#include <boost/test/unit_test.hpp>
#include <future>
#include <stdexcept>
#include <string>
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

  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-1");
  queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-2");
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

  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-1");
  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-2");
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
