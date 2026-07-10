#include <boost/test/unit_test.hpp>
#include <chrono>
#include <future>
#include <optional>
#include <string>

#include "benchmark_sim/simulation_command_queue.h"

namespace {

using namespace std::chrono_literals;

}  // namespace

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_fifo_order) {
  bsim::SimulationCommandQueue queue;
  const std::uint64_t first_sequence =
      queue.Push(bsim::SimulationCommandKind::kDisconnectNode, "firo-1");
  const std::uint64_t second_sequence =
      queue.Push(bsim::SimulationCommandKind::kKillNode, "firo-2");

  BOOST_TEST(first_sequence == 1U);
  BOOST_TEST(second_sequence == 2U);
  const std::optional<bsim::SimulationCommand> first = queue.TryPop();
  const std::optional<bsim::SimulationCommand> second = queue.TryPop();
  BOOST_REQUIRE(first);
  BOOST_REQUIRE(second);
  BOOST_TEST(first->sequence == first_sequence);
  BOOST_CHECK(first->kind == bsim::SimulationCommandKind::kDisconnectNode);
  BOOST_TEST(first->node_id == "firo-1");
  BOOST_TEST(second->sequence == second_sequence);
  BOOST_CHECK(second->kind == bsim::SimulationCommandKind::kKillNode);
  BOOST_TEST(second->node_id == "firo-2");
  BOOST_TEST(!queue.TryPop());
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_wakes_waiting_consumer) {
  bsim::SimulationCommandQueue queue;
  std::future<std::optional<bsim::SimulationCommand>> pending =
      std::async(std::launch::async, [&queue] { return queue.WaitPop(); });

  BOOST_CHECK(pending.wait_for(20ms) == std::future_status::timeout);
  queue.Push(bsim::SimulationCommandKind::kStopMining, "firo-1");
  BOOST_CHECK(pending.wait_for(1s) == std::future_status::ready);
  const std::optional<bsim::SimulationCommand> command = pending.get();
  BOOST_REQUIRE(command);
  BOOST_CHECK(command->kind == bsim::SimulationCommandKind::kStopMining);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_closes_waiting_consumer) {
  bsim::SimulationCommandQueue queue;
  std::future<std::optional<bsim::SimulationCommand>> pending =
      std::async(std::launch::async, [&queue] { return queue.WaitPop(); });

  queue.Close();
  BOOST_CHECK(pending.wait_for(1s) == std::future_status::ready);
  BOOST_TEST(!pending.get());
  BOOST_TEST(queue.IsClosed());
  BOOST_CHECK_THROW(
      queue.Push(bsim::SimulationCommandKind::kKillNode, "firo-1"),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_queue_cancel_discards_pending_commands) {
  bsim::SimulationCommandQueue queue;
  queue.Push(bsim::SimulationCommandKind::kDisconnectNode, "firo-1");
  queue.Push(bsim::SimulationCommandKind::kKillNode, "firo-2");

  queue.Cancel();

  BOOST_TEST(queue.IsClosed());
  BOOST_TEST(!queue.TryPop());
  BOOST_TEST(!queue.WaitPop());
  BOOST_CHECK_THROW(
      queue.Push(bsim::SimulationCommandKind::kKillNode, "firo-3"),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_rejects_empty_node_id) {
  bsim::SimulationCommandQueue queue;
  BOOST_CHECK_THROW(
      queue.Push(bsim::SimulationCommandKind::kKillNode, std::string{}),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_typed_mining_payloads) {
  bsim::SimulationCommandQueue queue;
  queue.PushBlockProductionPolicy(
      bsim::BlockProductionPolicy(std::chrono::seconds(2), 0.25, 8U));
  queue.PushMiningDifficulty("firo-2", bsim::MiningDifficulty(3.0));

  const std::optional<bsim::SimulationCommand> policy = queue.TryPop();
  const std::optional<bsim::SimulationCommand> difficulty = queue.TryPop();
  BOOST_REQUIRE(policy);
  BOOST_TEST(policy->node_id == "sim");
  BOOST_REQUIRE(policy->block_production_policy);
  BOOST_TEST(policy->block_production_policy->period().count() == 2000);
  BOOST_TEST(policy->block_production_policy->probability() == 0.25);
  BOOST_REQUIRE(difficulty);
  BOOST_REQUIRE(difficulty->mining_difficulty);
  BOOST_TEST(difficulty->mining_difficulty->value() == 3.0);
}
