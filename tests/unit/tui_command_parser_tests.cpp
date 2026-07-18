#include <boost/test/unit_test.hpp>

#include "bbp/tui_command_parser.h"

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_typed_block_policy) {
  const bbp::ParsedTuiCommand command =
      bbp::TuiCommandParser::Parse("block-production 0.25 2000", 71U);
  BOOST_CHECK(command.kind ==
              bbp::SimulationCommandKind::kSetBlockProductionPolicy);
  BOOST_REQUIRE(command.block_production_policy);
  BOOST_TEST(command.block_production_policy->probability() == 0.25);
  BOOST_TEST(command.block_production_policy->period().count() == 2000);
  BOOST_TEST(command.block_production_policy->seed() == 71U);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_driver_commands) {
  BOOST_CHECK(bbp::TuiCommandParser::Parse("reconnect", 0U).kind ==
              bbp::SimulationCommandKind::kReconnectNode);
  const bbp::ParsedTuiCommand difficulty =
      bbp::TuiCommandParser::Parse("mining-difficulty 3.5", 0U);
  BOOST_REQUIRE(difficulty.mining_difficulty);
  BOOST_TEST(difficulty.mining_difficulty->value() == 3.5);

  const bbp::ParsedTuiCommand connect =
      bbp::TuiCommandParser::Parse("connect-peer firo-2", 0U);
  BOOST_CHECK(connect.kind == bbp::SimulationCommandKind::kConnectPeer);
  BOOST_REQUIRE(connect.peer_node_id);
  BOOST_TEST(*connect.peer_node_id == "firo-2");

  const bbp::ParsedTuiCommand disconnect =
      bbp::TuiCommandParser::Parse("disconnect-peer firo-3", 0U);
  BOOST_CHECK(disconnect.kind == bbp::SimulationCommandKind::kDisconnectPeer);
  BOOST_REQUIRE(disconnect.peer_node_id);
  BOOST_TEST(*disconnect.peer_node_id == "firo-3");

  const bbp::ParsedTuiCommand policy =
      bbp::TuiCommandParser::Parse("peer-policy 1 3", 0U);
  BOOST_CHECK(policy.kind == bbp::SimulationCommandKind::kSetPeerCountPolicy);
  BOOST_REQUIRE(policy.peer_count_policy);
  BOOST_TEST(policy.peer_count_policy->minimum() == 1U);
  BOOST_TEST(policy.peer_count_policy->maximum() == 3U);

  const bbp::ParsedTuiCommand generate =
      bbp::TuiCommandParser::Parse("generate-blocks 9", 0U);
  BOOST_CHECK(generate.kind == bbp::SimulationCommandKind::kGenerateBlocks);
  BOOST_REQUIRE(generate.block_count);
  BOOST_TEST(*generate.block_count == 9U);

  const bbp::ParsedTuiCommand resource =
      bbp::TuiCommandParser::Parse("resource-profile constrained", 0U);
  BOOST_CHECK(resource.kind == bbp::SimulationCommandKind::kSetResourceProfile);
  BOOST_REQUIRE(resource.profile);
  BOOST_TEST(*resource.profile == "constrained");

  BOOST_CHECK(bbp::TuiCommandParser::Parse("freeze", 0U).kind ==
              bbp::SimulationCommandKind::kFreezeNode);
  BOOST_CHECK(bbp::TuiCommandParser::Parse("thaw", 0U).kind ==
              bbp::SimulationCommandKind::kThawNode);
  BOOST_CHECK(bbp::TuiCommandParser::Parse("stop-node", 0U).kind ==
              bbp::SimulationCommandKind::kStopNode);
  BOOST_CHECK(bbp::TuiCommandParser::Parse("restart", 0U).kind ==
              bbp::SimulationCommandKind::kRestartNode);
  BOOST_CHECK(bbp::TuiCommandParser::Parse("kill", 0U).kind ==
              bbp::SimulationCommandKind::kKillNode);
  BOOST_CHECK(bbp::TuiCommandParser::Parse("export-node-report", 0U).kind ==
              bbp::SimulationCommandKind::kExportNodeReport);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_perf_counter_commands) {
  const bbp::ParsedTuiCommand selected = bbp::TuiCommandParser::Parse(
      "perf-counters cycles,instructions,task-clock", 0U);
  BOOST_CHECK(selected.kind == bbp::SimulationCommandKind::kSetPerfCounters);
  BOOST_TEST(!selected.perf_counter_target_kind);
  BOOST_TEST(!selected.perf_counter_target_id);
  BOOST_REQUIRE_EQUAL(selected.perf_counter_kinds.size(), 3U);
  BOOST_CHECK(selected.perf_counter_kinds[0] == bbp::PerfCounterKind::kCycles);
  BOOST_CHECK(selected.perf_counter_kinds[2] ==
              bbp::PerfCounterKind::kTaskClock);

  const bbp::ParsedTuiCommand selected_cgroup =
      bbp::TuiCommandParser::Parse("perf-counters cgroup page-faults", 0U);
  BOOST_REQUIRE(selected_cgroup.perf_counter_target_kind);
  BOOST_CHECK(*selected_cgroup.perf_counter_target_kind ==
              bbp::PerfCounterTargetKind::kCgroup);
  BOOST_TEST(!selected_cgroup.perf_counter_target_id);

  const bbp::ParsedTuiCommand group = bbp::TuiCommandParser::Parse(
      "perf-counters group topology-1 cache-misses,branch-misses", 0U);
  BOOST_REQUIRE(group.perf_counter_target_kind);
  BOOST_CHECK(*group.perf_counter_target_kind ==
              bbp::PerfCounterTargetKind::kGroup);
  BOOST_REQUIRE(group.perf_counter_target_id);
  BOOST_TEST(*group.perf_counter_target_id == "topology-1");
  BOOST_REQUIRE_EQUAL(group.perf_counter_kinds.size(), 2U);

  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("perf-counters", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("perf-counters unknown cycles", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("perf-counters cycles,cycles", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("perf-counters cycles,", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("perf-counters cpu-cycles", 0U),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_resource_limit_commands) {
  const bbp::ParsedTuiCommand memory_high =
      bbp::TuiCommandParser::Parse("resource-limit memory-high 0", 0U);
  BOOST_CHECK(memory_high.kind ==
              bbp::SimulationCommandKind::kSetResourceLimits);
  BOOST_REQUIRE(memory_high.resource_limit_patch);
  BOOST_REQUIRE(memory_high.resource_limit_patch->memory_high_bytes);
  BOOST_TEST(*memory_high.resource_limit_patch->memory_high_bytes == 0U);

  const bbp::ParsedTuiCommand memory_max = bbp::TuiCommandParser::Parse(
      "resource-limit memory-max 18446744073709551615", 0U);
  BOOST_REQUIRE(memory_max.resource_limit_patch);
  BOOST_REQUIRE(memory_max.resource_limit_patch->memory_max_bytes);
  BOOST_TEST(*memory_max.resource_limit_patch->memory_max_bytes ==
             18446744073709551615ULL);

  const bbp::ParsedTuiCommand cpu =
      bbp::TuiCommandParser::Parse("resource-limit cpu-max 50000 100000", 0U);
  BOOST_REQUIRE(cpu.resource_limit_patch);
  BOOST_TEST(cpu.resource_limit_patch->cpu_quota_present);
  BOOST_REQUIRE(cpu.resource_limit_patch->cpu_quota_us);
  BOOST_TEST(*cpu.resource_limit_patch->cpu_quota_us == 50000U);
  BOOST_REQUIRE(cpu.resource_limit_patch->cpu_period_us);
  BOOST_TEST(*cpu.resource_limit_patch->cpu_period_us == 100000U);

  const bbp::ParsedTuiCommand unlimited =
      bbp::TuiCommandParser::Parse("resource-limit cpu-max max 250000", 0U);
  BOOST_REQUIRE(unlimited.resource_limit_patch);
  BOOST_TEST(unlimited.resource_limit_patch->cpu_quota_present);
  BOOST_TEST(!unlimited.resource_limit_patch->cpu_quota_us);
  BOOST_REQUIRE(unlimited.resource_limit_patch->cpu_period_us);
  BOOST_TEST(*unlimited.resource_limit_patch->cpu_period_us == 250000U);

  const bbp::ParsedTuiCommand cpu_weight =
      bbp::TuiCommandParser::Parse("resource-limit cpu-weight 10000", 0U);
  BOOST_REQUIRE(cpu_weight.resource_limit_patch);
  BOOST_REQUIRE(cpu_weight.resource_limit_patch->cpu_weight);
  BOOST_TEST(*cpu_weight.resource_limit_patch->cpu_weight == 10000U);

  const bbp::ParsedTuiCommand io = bbp::TuiCommandParser::Parse(
      "resource-limit io-max 259:7 1000 max 30 max", 0U);
  BOOST_REQUIRE(io.resource_limit_patch);
  BOOST_TEST(io.resource_limit_patch->io_limits_present);
  BOOST_REQUIRE_EQUAL(io.resource_limit_patch->io_limits.size(), 1U);
  const bbp::IoLimit& io_limit = io.resource_limit_patch->io_limits.front();
  BOOST_TEST(io_limit.device.major == 259U);
  BOOST_TEST(io_limit.device.minor == 7U);
  BOOST_REQUIRE(io_limit.read_bytes_per_sec);
  BOOST_TEST(*io_limit.read_bytes_per_sec == 1000U);
  BOOST_TEST(!io_limit.write_bytes_per_sec);
  BOOST_REQUIRE(io_limit.read_operations_per_sec);
  BOOST_TEST(*io_limit.read_operations_per_sec == 30U);
  BOOST_TEST(!io_limit.write_operations_per_sec);

  const bbp::ParsedTuiCommand clear_io = bbp::TuiCommandParser::Parse(
      "resource-limit io-max 259:7 max max max max", 0U);
  BOOST_REQUIRE(clear_io.resource_limit_patch);
  BOOST_REQUIRE_EQUAL(clear_io.resource_limit_patch->io_limits.size(), 1U);
  const bbp::IoLimit& clear = clear_io.resource_limit_patch->io_limits.front();
  BOOST_TEST(!clear.read_bytes_per_sec);
  BOOST_TEST(!clear.write_bytes_per_sec);
  BOOST_TEST(!clear.read_operations_per_sec);
  BOOST_TEST(!clear.write_operations_per_sec);

  const bbp::ParsedTuiCommand io_weight =
      bbp::TuiCommandParser::Parse("resource-limit io-weight 1", 0U);
  BOOST_REQUIRE(io_weight.resource_limit_patch);
  BOOST_REQUIRE(io_weight.resource_limit_patch->io_weight);
  BOOST_TEST(*io_weight.resource_limit_patch->io_weight == 1U);

  const bbp::ParsedTuiCommand pids =
      bbp::TuiCommandParser::Parse("resource-limit pids-max 128", 0U);
  BOOST_REQUIRE(pids.resource_limit_patch);
  BOOST_REQUIRE(pids.resource_limit_patch->pids_max);
  BOOST_TEST(*pids.resource_limit_patch->pids_max == 128U);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_rejects_invalid_resource_limits) {
  constexpr std::string_view kInvalidCommands[] = {
      "resource-limit",
      "resource-limit unknown 1",
      "resource-limit memory-high",
      "resource-limit memory-high -1",
      "resource-limit memory-high +1",
      "resource-limit memory-high 18446744073709551616",
      "resource-limit memory-max 0",
      "resource-limit cpu-max 0 100000",
      "resource-limit cpu-max max 0",
      "resource-limit cpu-max MAX 100000",
      "resource-limit cpu-weight 0",
      "resource-limit cpu-weight 10001",
      "resource-limit io-weight 0",
      "resource-limit io-weight 10001",
      "resource-limit pids-max 0",
      "resource-limit io-max 259 1 2 3 4",
      "resource-limit io-max 4294967296:0 1 2 3 4",
      "resource-limit io-max 259:0 0 2 3 4",
      "resource-limit io-max 259:0 1 2 3",
      "resource-limit io-max 259:0 1 2 3 4 extra",
  };
  for (const std::string_view command : kInvalidCommands) {
    BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse(command, 0U),
                      std::runtime_error);
  }
}

BOOST_AUTO_TEST_CASE(tui_command_parser_completes_unique_command_prefix) {
  BOOST_TEST(bbp::TuiCommandParser::Complete("reco") == "reconnect ");
  BOOST_TEST(bbp::TuiCommandParser::Complete("log-") == "log-");
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("unknown", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("block-production nope 1000", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("connect-peer", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("peer-policy 2 1", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("generate-blocks 0", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("resource-profile", 0U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_network_commands) {
  const bbp::ParsedTuiCommand condition = bbp::TuiCommandParser::Parse(
      "network-condition 20 80 10 11 12 13 14 900", 0U);
  BOOST_CHECK(condition.kind ==
              bbp::SimulationCommandKind::kSetNetworkCondition);
  BOOST_REQUIRE(condition.network_condition);
  BOOST_TEST(condition.network_condition->bandwidth_mbps == 20U);
  BOOST_TEST(condition.network_condition->delay_ms == 80U);
  BOOST_TEST(condition.network_condition->jitter_ms == 10U);
  BOOST_TEST(condition.network_condition->loss_basis_points == 11U);
  BOOST_TEST(condition.network_condition->duplicate_basis_points == 12U);
  BOOST_TEST(condition.network_condition->corrupt_basis_points == 13U);
  BOOST_TEST(condition.network_condition->reorder_basis_points == 14U);
  BOOST_TEST(condition.network_condition->limit_packets == 900U);

  const bbp::ParsedTuiCommand default_limit =
      bbp::TuiCommandParser::Parse("network-condition 0 0 0 0 0 0 0", 0U);
  BOOST_REQUIRE(default_limit.network_condition);
  BOOST_TEST(default_limit.network_condition->limit_packets == 1000U);

  const bbp::ParsedTuiCommand block = bbp::TuiCommandParser::Parse(
      "block 10.210.1.6 18168 10.210.1.2 43120", 0U);
  BOOST_CHECK(block.kind == bbp::SimulationCommandKind::kBlockNetworkFlow);
  BOOST_REQUIRE(block.network_flow);
  BOOST_TEST(block.network_flow->src_address == "10.210.1.2");
  BOOST_TEST(block.network_flow->src_port == 43120U);
  BOOST_TEST(block.network_flow->dst_address == "10.210.1.6");
  BOOST_TEST(block.network_flow->dst_port == 18168U);

  const bbp::ParsedTuiCommand unblock =
      bbp::TuiCommandParser::Parse("unblock 10.210.1.6 18168", 0U);
  BOOST_CHECK(unblock.kind == bbp::SimulationCommandKind::kUnblockNetworkFlow);
  BOOST_REQUIRE(unblock.network_flow);
  BOOST_TEST(unblock.network_flow->src_address.empty());
  BOOST_TEST(unblock.network_flow->src_port == 0U);

  const bbp::ParsedTuiCommand wildcard_source =
      bbp::TuiCommandParser::Parse("unblock 10.210.1.6 18168 * 43120", 0U);
  BOOST_REQUIRE(wildcard_source.network_flow);
  BOOST_TEST(wildcard_source.network_flow->src_address.empty());
  BOOST_TEST(wildcard_source.network_flow->src_port == 43120U);

  const bbp::ParsedTuiCommand clear =
      bbp::TuiCommandParser::Parse("clear-rule 77", 0U);
  BOOST_REQUIRE(clear.network_flow);
  BOOST_TEST(clear.network_flow->src_port == 0U);
  BOOST_TEST(clear.network_flow->dst_address.empty());
  BOOST_TEST(clear.network_flow->handle == 77U);

  const bbp::ParsedTuiCommand partition =
      bbp::TuiCommandParser::Parse("partition firo-2", 0U);
  BOOST_CHECK(partition.kind == bbp::SimulationCommandKind::kPartitionNodes);
  BOOST_REQUIRE(partition.partition_target_kind);
  BOOST_CHECK(*partition.partition_target_kind ==
              bbp::TuiPartitionTargetKind::kNodePair);
  BOOST_REQUIRE(partition.peer_node_id);
  BOOST_TEST(*partition.peer_node_id == "firo-2");
  const bbp::ParsedTuiCommand selected =
      bbp::TuiCommandParser::Parse("heal", 0U);
  BOOST_CHECK(selected.kind == bbp::SimulationCommandKind::kHealPartition);
  BOOST_REQUIRE(selected.partition_target_kind);
  BOOST_CHECK(*selected.partition_target_kind ==
              bbp::TuiPartitionTargetKind::kSelectedTopologyGroup);
  BOOST_TEST(!selected.peer_node_id);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("partition firo-2 extra", 0U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_rejects_invalid_network_commands) {
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 4294968 0 0 0 0 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 0 4294968 0 0 0 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 0 0 10001 0 0 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 0 0 0 10001 0 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 0 0 0 0 10001 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 0 0 0 0 0 10001", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("network-condition 1 0 0 0 0 0 0 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("block 10.0.0.1 0", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("unblock 10.0.0.1 65536", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("block invalid 18168", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("block 10.0.0.1 18168 invalid", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("block 10.0.0.1 18168 * 0", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("block 10.0.0.1 18168 * 65536", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse(
                        "block 10.0.0.1 18168 10.0.0.2 43120 extra", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("clear-rule 0", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("heal firo-2 extra", 0U),
                    std::runtime_error);
}
