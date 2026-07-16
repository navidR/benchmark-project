#include <boost/test/unit_test.hpp>
#include <chrono>
#include <future>
#include <optional>
#include <string>

#include "bbp/simulation_command_queue.h"

namespace {

using namespace std::chrono_literals;

}  // namespace

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_fifo_order) {
  bbp::SimulationCommandQueue queue;
  const std::uint64_t first_sequence =
      queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-1", true);
  const std::uint64_t second_sequence =
      queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-2", true);

  BOOST_TEST(first_sequence == 1U);
  BOOST_TEST(second_sequence == 2U);
  const std::optional<bbp::SimulationCommand> first = queue.TryPop();
  const std::optional<bbp::SimulationCommand> second = queue.TryPop();
  BOOST_REQUIRE(first);
  BOOST_REQUIRE(second);
  BOOST_TEST(first->sequence == first_sequence);
  BOOST_CHECK(first->kind == bbp::SimulationCommandKind::kDisconnectNode);
  BOOST_TEST(first->node_id == "firo-1");
  BOOST_TEST(second->sequence == second_sequence);
  BOOST_CHECK(second->kind == bbp::SimulationCommandKind::kKillNode);
  BOOST_TEST(second->node_id == "firo-2");
  BOOST_TEST(!queue.TryPop());
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_wakes_waiting_consumer) {
  bbp::SimulationCommandQueue queue;
  std::future<std::optional<bbp::SimulationCommand>> pending =
      std::async(std::launch::async, [&queue] { return queue.WaitPop(); });

  BOOST_CHECK(pending.wait_for(20ms) == std::future_status::timeout);
  queue.Push(bbp::SimulationCommandKind::kStopMining, "firo-1", true);
  BOOST_CHECK(pending.wait_for(1s) == std::future_status::ready);
  const std::optional<bbp::SimulationCommand> command = pending.get();
  BOOST_REQUIRE(command);
  BOOST_CHECK(command->kind == bbp::SimulationCommandKind::kStopMining);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_closes_waiting_consumer) {
  bbp::SimulationCommandQueue queue;
  std::future<std::optional<bbp::SimulationCommand>> pending =
      std::async(std::launch::async, [&queue] { return queue.WaitPop(); });

  queue.Close();
  BOOST_CHECK(pending.wait_for(1s) == std::future_status::ready);
  BOOST_TEST(!pending.get());
  BOOST_TEST(queue.IsClosed());
  BOOST_CHECK_THROW(
      queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-1", true),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_queue_cancel_discards_pending_commands) {
  bbp::SimulationCommandQueue queue;
  queue.Push(bbp::SimulationCommandKind::kDisconnectNode, "firo-1", true);
  queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-2", true);

  queue.Cancel();

  BOOST_TEST(queue.IsClosed());
  BOOST_TEST(!queue.TryPop());
  BOOST_TEST(!queue.WaitPop());
  BOOST_CHECK_THROW(
      queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-3", true),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_rejects_empty_node_id) {
  bbp::SimulationCommandQueue queue;
  BOOST_CHECK_THROW(
      queue.Push(bbp::SimulationCommandKind::kKillNode, std::string{}, true),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_typed_mining_payloads) {
  bbp::SimulationCommandQueue queue;
  queue.PushBlockProductionPolicy(
      bbp::BlockProductionPolicy(std::chrono::seconds(2), 0.25, 8U));
  queue.PushMiningDifficulty("firo-2", bbp::MiningDifficulty(3.0));

  const std::optional<bbp::SimulationCommand> policy = queue.TryPop();
  const std::optional<bbp::SimulationCommand> difficulty = queue.TryPop();
  BOOST_REQUIRE(policy);
  BOOST_TEST(policy->node_id == "sim");
  BOOST_REQUIRE(policy->block_production_policy);
  BOOST_TEST(policy->block_production_policy->period().count() == 2000);
  BOOST_TEST(policy->block_production_policy->probability() == 0.25);
  BOOST_REQUIRE(difficulty);
  BOOST_REQUIRE(difficulty->mining_difficulty);
  BOOST_TEST(difficulty->mining_difficulty->value() == 3.0);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_peer_target) {
  bbp::SimulationCommandQueue queue;
  queue.PushPeerCommand(bbp::SimulationCommandKind::kDisconnectPeer, "firo-1",
                        "firo-2", true);

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_CHECK(command->kind == bbp::SimulationCommandKind::kDisconnectPeer);
  BOOST_TEST(command->node_id == "firo-1");
  BOOST_REQUIRE(command->peer_node_id);
  BOOST_TEST(*command->peer_node_id == "firo-2");

  BOOST_CHECK_THROW(
      queue.PushPeerCommand(bbp::SimulationCommandKind::kConnectPeer, "firo-1",
                            "firo-1"),
      std::runtime_error);
  BOOST_CHECK_THROW(queue.PushPeerCommand(bbp::SimulationCommandKind::kKillNode,
                                          "firo-1", "firo-2"),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_peer_count_policy) {
  bbp::SimulationCommandQueue queue;
  queue.PushPeerCountPolicy("firo-2", bbp::PeerCountPolicy(1U, 3U), true);

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_CHECK(command->kind == bbp::SimulationCommandKind::kSetPeerCountPolicy);
  BOOST_TEST(command->node_id == "firo-2");
  BOOST_REQUIRE(command->peer_count_policy);
  BOOST_TEST(command->peer_count_policy->minimum() == 1U);
  BOOST_TEST(command->peer_count_policy->maximum() == 3U);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_queue_requires_destructive_confirmation) {
  bbp::SimulationCommandQueue queue;
  BOOST_CHECK_THROW(queue.Push(bbp::SimulationCommandKind::kKillNode, "firo-1"),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushPeerCommand(bbp::SimulationCommandKind::kDisconnectPeer,
                            "firo-1", "firo-2"),
      std::runtime_error);
  BOOST_TEST(!queue.TryPop());

  queue.Push(bbp::SimulationCommandKind::kRestartNode, "firo-1", true);
  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_TEST(command->confirmed);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_operator_payloads) {
  bbp::SimulationCommandQueue queue;
  queue.PushGenerateBlocks("firo-1", 7U);
  queue.PushProfileCommand(bbp::SimulationCommandKind::kSetResourceProfile,
                           "firo-2", "constrained", true);

  const std::optional<bbp::SimulationCommand> generate = queue.TryPop();
  const std::optional<bbp::SimulationCommand> profile = queue.TryPop();
  BOOST_REQUIRE(generate);
  BOOST_REQUIRE(generate->block_count);
  BOOST_TEST(*generate->block_count == 7U);
  BOOST_REQUIRE(profile);
  BOOST_REQUIRE(profile->profile);
  BOOST_TEST(*profile->profile == "constrained");
  BOOST_TEST(profile->confirmed);

  BOOST_CHECK_THROW(queue.PushGenerateBlocks("firo-1", 0U), std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushProfileCommand(bbp::SimulationCommandKind::kKillNode, "firo-1",
                               "x", true),
      std::runtime_error);
}
