#include <boost/test/unit_test.hpp>
#include <chrono>
#include <future>
#include <limits>
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

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_node_report_exports) {
  bbp::SimulationCommandQueue queue;
  const std::uint64_t sequence =
      queue.Push(bbp::SimulationCommandKind::kExportNodeReport, "firo-2");

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_TEST(command->sequence == sequence);
  BOOST_CHECK(command->kind == bbp::SimulationCommandKind::kExportNodeReport);
  BOOST_TEST(command->node_id == "firo-2");
  BOOST_TEST(!command->confirmed);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_perf_counter_target) {
  bbp::SimulationCommandQueue queue;
  const bbp::PerfCounterTarget target{
      .kind = bbp::PerfCounterTargetKind::kGroup,
      .id = "topology-1",
      .node_ids = {"firo-1", "firo-2"},
  };
  const std::uint64_t sequence = queue.PushPerfCounters(
      target,
      {bbp::PerfCounterKind::kCycles, bbp::PerfCounterKind::kInstructions});

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_TEST(command->sequence == sequence);
  BOOST_CHECK(command->kind == bbp::SimulationCommandKind::kSetPerfCounters);
  BOOST_TEST(command->node_id == "sim");
  BOOST_REQUIRE(command->perf_counter_target);
  BOOST_CHECK(*command->perf_counter_target == target);
  BOOST_REQUIRE_EQUAL(command->perf_counter_kinds.size(), 2U);
  BOOST_TEST(!command->confirmed);

  BOOST_CHECK_THROW(
      queue.PushPerfCounters(
          bbp::PerfCounterTarget{.kind = bbp::PerfCounterTargetKind::kNode,
                                 .id = "firo-1",
                                 .node_ids = {"firo-1", "firo-2"}},
          {bbp::PerfCounterKind::kCycles}),
      std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushPerfCounters(
          bbp::PerfCounterTarget{.kind = bbp::PerfCounterTargetKind::kGroup,
                                 .id = "all",
                                 .node_ids = {"firo-1", "firo-1"}},
          {bbp::PerfCounterKind::kCycles}),
      std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushPerfCounters(
          bbp::PerfCounterTarget{.kind = bbp::PerfCounterTargetKind::kNode,
                                 .id = "firo-1",
                                 .node_ids = {"firo-1"}},
          {bbp::PerfCounterKind::kCycles, bbp::PerfCounterKind::kCycles}),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_wallet_send_identity) {
  bbp::SimulationCommandQueue queue;
  const bbp::SimulationWalletSend send{
      .sender_wallet_index = 1U,
      .receiver_wallet_index = 2U,
      .amount_satoshis = 10000000U,
      .fee_satoshis = 1000U,
      .timeout_sec = 45U,
  };
  const std::uint64_t sequence =
      queue.PushWalletSend("firo-wallet-a", send, true);

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_TEST(command->sequence == sequence);
  BOOST_CHECK(command->kind ==
              bbp::SimulationCommandKind::kSendWalletTransaction);
  BOOST_TEST(command->node_id == "firo-wallet-a");
  BOOST_REQUIRE(command->wallet_send);
  BOOST_CHECK(*command->wallet_send == send);
  BOOST_TEST(command->confirmed);

  BOOST_CHECK_THROW(queue.PushWalletSend("firo-wallet-a", send),
                    std::runtime_error);
  bbp::SimulationWalletSend invalid = send;
  invalid.sender_wallet_index = 0U;
  BOOST_CHECK_THROW(queue.PushWalletSend("firo-wallet-a", invalid, true),
                    std::runtime_error);
  invalid = send;
  invalid.receiver_wallet_index = invalid.sender_wallet_index;
  BOOST_CHECK_THROW(queue.PushWalletSend("firo-wallet-a", invalid, true),
                    std::runtime_error);
  invalid = send;
  invalid.amount_satoshis = 0U;
  BOOST_CHECK_THROW(queue.PushWalletSend("firo-wallet-a", invalid, true),
                    std::runtime_error);
  invalid = send;
  invalid.amount_satoshis = std::numeric_limits<std::uint64_t>::max();
  invalid.fee_satoshis = 1U;
  BOOST_CHECK_THROW(queue.PushWalletSend("firo-wallet-a", invalid, true),
                    std::runtime_error);
  invalid = send;
  invalid.timeout_sec = 0U;
  BOOST_CHECK_THROW(queue.PushWalletSend("firo-wallet-a", invalid, true),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      queue.Push(bbp::SimulationCommandKind::kSendWalletTransaction,
                 "firo-wallet-a", true),
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

BOOST_AUTO_TEST_CASE(
    simulation_command_queue_preserves_typed_network_condition) {
  bbp::SimulationCommandQueue queue;
  const bbp::NetworkCondition condition{
      .bandwidth_mbps = 20U,
      .delay_ms = 80U,
      .jitter_ms = 10U,
      .loss_basis_points = 11U,
      .duplicate_basis_points = 12U,
      .corrupt_basis_points = 13U,
      .reorder_basis_points = 14U,
      .limit_packets = 900U,
  };
  queue.PushNetworkCondition("firo-1", condition, true);

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_CHECK(command->kind ==
              bbp::SimulationCommandKind::kSetNetworkCondition);
  BOOST_REQUIRE(command->network_condition);
  BOOST_CHECK(*command->network_condition == condition);
  BOOST_TEST(command->confirmed);

  bbp::NetworkCondition invalid = condition;
  invalid.limit_packets = 0U;
  BOOST_CHECK_THROW(queue.PushNetworkCondition("firo-1", invalid, true),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_resource_limit_patch) {
  bbp::SimulationCommandQueue queue;
  bbp::ResourceLimitPatch patch;
  patch.cpu_quota_present = true;
  patch.cpu_period_us = 100000U;
  const std::uint64_t sequence =
      queue.PushResourceLimits("firo-1", patch, true);

  const std::optional<bbp::SimulationCommand> command = queue.TryPop();
  BOOST_REQUIRE(command);
  BOOST_TEST(command->sequence == sequence);
  BOOST_CHECK(command->kind == bbp::SimulationCommandKind::kSetResourceLimits);
  BOOST_TEST(command->node_id == "firo-1");
  BOOST_REQUIRE(command->resource_limit_patch);
  BOOST_CHECK(*command->resource_limit_patch == patch);
  BOOST_TEST(command->confirmed);

  BOOST_CHECK_THROW(queue.PushResourceLimits("firo-1", patch),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushResourceLimits("firo-1", bbp::ResourceLimitPatch{}, true),
      std::runtime_error);
  BOOST_CHECK_THROW(queue.Push(bbp::SimulationCommandKind::kSetResourceLimits,
                               "firo-1", true),
                    std::runtime_error);
  bbp::ResourceLimitPatch io_patch;
  io_patch.io_limits_present = true;
  bbp::IoLimit first;
  first.device = {.major = 8U, .minor = 0U};
  bbp::IoLimit second;
  second.device = {.major = 8U, .minor = 1U};
  io_patch.io_limits = {first, second};
  BOOST_CHECK_THROW(
      queue.PushResourceLimits("firo-1", std::move(io_patch), true),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_typed_network_flows) {
  bbp::SimulationCommandQueue queue;
  const bbp::SimulationNetworkFlow flow{
      .src_address = "10.210.1.2",
      .src_port = 43120U,
      .dst_address = "10.210.1.6",
      .dst_port = 18168U,
      .handle = 77U,
  };
  queue.PushNetworkFlowCommand(bbp::SimulationCommandKind::kBlockNetworkFlow,
                               "firo-1", flow, true);
  queue.PushNetworkFlowCommand(bbp::SimulationCommandKind::kUnblockNetworkFlow,
                               "firo-1", flow);
  queue.PushNetworkFlowCommand(bbp::SimulationCommandKind::kUnblockNetworkFlow,
                               "firo-1",
                               bbp::SimulationNetworkFlow{
                                   .src_address = {},
                                   .src_port = 0U,
                                   .dst_address = {},
                                   .dst_port = 0U,
                                   .handle = 77U,
                               });

  const std::optional<bbp::SimulationCommand> block = queue.TryPop();
  const std::optional<bbp::SimulationCommand> unblock = queue.TryPop();
  const std::optional<bbp::SimulationCommand> clear = queue.TryPop();
  BOOST_REQUIRE(block);
  BOOST_REQUIRE(block->network_flow);
  BOOST_TEST(block->network_flow->src_address == "10.210.1.2");
  BOOST_TEST(block->network_flow->src_port == 43120U);
  BOOST_TEST(block->network_flow->dst_address == "10.210.1.6");
  BOOST_TEST(block->network_flow->dst_port == 18168U);
  BOOST_TEST(block->network_flow->handle == 77U);
  BOOST_REQUIRE(unblock);
  BOOST_CHECK(unblock->kind == bbp::SimulationCommandKind::kUnblockNetworkFlow);
  BOOST_REQUIRE(clear);
  BOOST_REQUIRE(clear->network_flow);
  BOOST_TEST(clear->network_flow->src_port == 0U);
  BOOST_TEST(clear->network_flow->dst_address.empty());
  BOOST_TEST(clear->network_flow->handle == 77U);

  BOOST_CHECK_THROW(queue.PushNetworkFlowCommand(
                        bbp::SimulationCommandKind::kBlockNetworkFlow, "firo-1",
                        bbp::SimulationNetworkFlow{}, true),
                    std::runtime_error);
  BOOST_CHECK_THROW(queue.PushNetworkFlowCommand(
                        bbp::SimulationCommandKind::kUnblockNetworkFlow,
                        "firo-1", bbp::SimulationNetworkFlow{}),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushNetworkFlowCommand(bbp::SimulationCommandKind::kKillNode,
                                   "firo-1", flow, true),
      std::runtime_error);
  bbp::SimulationNetworkFlow invalid_address = flow;
  invalid_address.dst_address = "invalid";
  BOOST_CHECK_THROW(queue.PushNetworkFlowCommand(
                        bbp::SimulationCommandKind::kBlockNetworkFlow, "firo-1",
                        invalid_address, true),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_command_queue_preserves_typed_partitions) {
  bbp::SimulationCommandQueue queue;
  const bbp::SimulationPartition node_pair{
      .scope = bbp::SimulationPartitionScope::kNodePair,
      .group_a = {.group_ids = {"firo-1"}, .node_ids = {"firo-1"}},
      .group_b = {.group_ids = {"firo-2"}, .node_ids = {"firo-2"}},
  };
  const bbp::SimulationPartition groups{
      .scope = bbp::SimulationPartitionScope::kRole,
      .group_a = {.group_ids = {"role-wallet"},
                  .node_ids = {"firo-1", "firo-2"}},
      .group_b = {.group_ids = {"role-miner"}, .node_ids = {"firo-3"}},
  };
  queue.PushPartitionCommand(bbp::SimulationCommandKind::kPartitionNodes,
                             node_pair, true);
  queue.PushPartitionCommand(bbp::SimulationCommandKind::kHealPartition,
                             groups);

  const std::optional<bbp::SimulationCommand> partition = queue.TryPop();
  const std::optional<bbp::SimulationCommand> heal = queue.TryPop();
  BOOST_REQUIRE(partition);
  BOOST_TEST(partition->node_id == "firo-1");
  BOOST_REQUIRE(partition->partition);
  BOOST_CHECK(*partition->partition == node_pair);
  BOOST_TEST(partition->confirmed);
  BOOST_REQUIRE(heal);
  BOOST_CHECK(heal->kind == bbp::SimulationCommandKind::kHealPartition);
  BOOST_TEST(heal->node_id == "sim");
  BOOST_REQUIRE(heal->partition);
  BOOST_CHECK(*heal->partition == groups);

  bbp::SimulationPartition empty = node_pair;
  empty.group_b.node_ids.clear();
  BOOST_CHECK_THROW(
      queue.PushPartitionCommand(bbp::SimulationCommandKind::kPartitionNodes,
                                 empty, true),
      std::runtime_error);
  bbp::SimulationPartition overlap = groups;
  overlap.group_b.node_ids = {"firo-2"};
  BOOST_CHECK_THROW(
      queue.PushPartitionCommand(bbp::SimulationCommandKind::kPartitionNodes,
                                 overlap, true),
      std::runtime_error);
  BOOST_CHECK_THROW(queue.PushPartitionCommand(
                        bbp::SimulationCommandKind::kKillNode, node_pair, true),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    simulation_command_queue_requires_network_mutation_confirmation) {
  bbp::SimulationCommandQueue queue;
  const bbp::NetworkCondition condition{};
  const bbp::SimulationNetworkFlow flow{
      .src_address = {},
      .dst_address = "10.210.1.6",
      .dst_port = 18168U,
      .handle = 0U,
  };
  BOOST_CHECK_THROW(queue.PushNetworkCondition("firo-1", condition),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushNetworkFlowCommand(
          bbp::SimulationCommandKind::kBlockNetworkFlow, "firo-1", flow),
      std::runtime_error);
  BOOST_CHECK_THROW(
      queue.PushPartitionCommand(
          bbp::SimulationCommandKind::kPartitionNodes,
          bbp::SimulationPartition{
              .scope = bbp::SimulationPartitionScope::kNodePair,
              .group_a = {.group_ids = {"firo-1"}, .node_ids = {"firo-1"}},
              .group_b = {.group_ids = {"firo-2"}, .node_ids = {"firo-2"}},
          }),
      std::runtime_error);
  BOOST_TEST(!queue.TryPop());
}
