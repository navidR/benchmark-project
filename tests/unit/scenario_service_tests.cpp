#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulator/block_generation_workload.h"
#include "bbp/simulator/wallet_transaction_plan.h"

namespace bbp {
namespace {

boost::json::object MinimalScenario() {
  return boost::json::object{
      {"chain", "firo"},
      {"chain_daemon", "/bin/true"},
      {"run_id", "scenario-service-test"},
      {"nodes", 1U},
      {"block_production", boost::json::object{{"enabled", false}}}};
}

}  // namespace

BOOST_AUTO_TEST_CASE(scenario_service_uses_production_nested_field_validation) {
  boost::json::object scenario = MinimalScenario();
  scenario["simulation"] = boost::json::object{{"unexpected", true}};

  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(scenario), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario simulation has unsupported field: unexpected";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_uses_descriptor_validation_for_nested_contexts) {
  boost::json::object topology = MinimalScenario();
  topology["topology"] = boost::json::object{
      {"node_count", 1U}, {"type", "ring"}, {"center_node", 1U}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(topology), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario topology ring has unsupported field: center_node";
      });

  boost::json::object node = MinimalScenario();
  node["nodes"] = boost::json::array{boost::json::object{
      {"id", "firo-1"},
      {"chain", "firo"},
      {"role", "base"},
      {"wallet",
       boost::json::object{{"enabled", false}, {"unexpected", true}}}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(node), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario node firo-1 has unsupported wallet field: "
               "unexpected";
      });

  boost::json::object process = MinimalScenario();
  process["process"] = boost::json::object{
      {"runtime_node_restarts", boost::json::array{boost::json::object{
                                    {"node", 1U}, {"unexpected", true}}}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(process), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario process.runtime_node_restarts entry has unsupported "
               "field: unexpected";
      });

  boost::json::object workload = MinimalScenario();
  workload["workloads"] = boost::json::array{boost::json::object{
      {"type", "checkpoint"}, {"name", "before"}, {"unexpected", true}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(workload), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario workload checkpoint has unsupported field: "
               "unexpected";
      });
}

BOOST_AUTO_TEST_CASE(scenario_service_returns_production_resolved_document) {
  const boost::json::object scenario = MinimalScenario();
  const Options options = ParseAndValidateScenario(scenario);
  BOOST_CHECK(options.chain == ChainKind::kFiro);
  BOOST_TEST(options.nodes == 1U);
  BOOST_TEST(options.isolate_network);
  BOOST_TEST(options.chain_daemon == std::filesystem::path("/bin/true"));

  const boost::json::object resolved = ResolveScenario(scenario);
  BOOST_TEST(resolved.at("run_id").as_string() == "scenario-service-test");
  BOOST_TEST(resolved.at("chain").as_string() == "firo");
  BOOST_TEST(resolved.at("nodes").as_uint64() == 1U);
  BOOST_TEST(resolved.at("isolated_network").as_bool());
  BOOST_TEST(resolved.at("chain_daemon").as_string() == "/bin/true");
  BOOST_TEST(resolved.at("ready_timeout_sec").as_uint64() ==
             options.ready_timeout_sec);
  BOOST_TEST(resolved.at("sync_timeout_sec").as_uint64() ==
             options.sync_timeout_sec);
  BOOST_TEST(resolved.at("node_configs")
                 .as_array()
                 .front()
                 .as_object()
                 .at("id")
                 .as_string() == "firo-1");
}

BOOST_AUTO_TEST_CASE(scenario_service_preserves_explicit_network_opt_out) {
  boost::json::object scenario = MinimalScenario();
  scenario["isolated_network"] = false;
  const Options options = ParseAndValidateScenario(scenario);
  BOOST_TEST(!options.isolate_network);
  BOOST_TEST(!ResolveScenario(scenario).at("isolated_network").as_bool());
}

BOOST_AUTO_TEST_CASE(
    scenario_service_enforces_block_generation_sync_timeout_contract) {
  boost::json::object direct = MinimalScenario();
  direct["workloads"] = boost::json::array{boost::json::object{
      {"type", "block_generation"},
      {"node", 1U},
      {"count", 1U},
      {"sync_timeout_sec", kBlockGenerationMinimumSyncTimeoutSeconds}}};
  const Options direct_options = ParseAndValidateScenario(direct);
  BOOST_REQUIRE_EQUAL(direct_options.workloads.size(), 1U);
  BOOST_TEST(
      direct_options.workloads.front().block_generation.sync_timeout_sec ==
      kBlockGenerationMinimumSyncTimeoutSeconds);

  boost::json::object scheduled = MinimalScenario();
  scheduled["events"] = boost::json::array{boost::json::object{
      {"at", "1s"},
      {"action", "block_generation"},
      {"node", 1U},
      {"count", 1U},
      {"sync_timeout_sec", kBlockGenerationMaximumSyncTimeoutSeconds}}};
  const Options scheduled_options = ParseAndValidateScenario(scheduled);
  BOOST_REQUIRE_EQUAL(scheduled_options.scheduled_events.size(), 1U);
  const ScenarioWorkload& scheduled_workload = std::get<ScenarioWorkload>(
      scheduled_options.scheduled_events.front().action);
  BOOST_TEST(scheduled_workload.block_generation.sync_timeout_sec ==
             kBlockGenerationMaximumSyncTimeoutSeconds);

  const auto rejects_timeout = [](bool use_scheduled_event,
                                  std::uint32_t timeout) {
    boost::json::object scenario = MinimalScenario();
    boost::json::object action{
        {"node", 1U}, {"count", 1U}, {"sync_timeout_sec", timeout}};
    if (use_scheduled_event) {
      action["at"] = "1s";
      action["action"] = "block_generation";
      scenario["events"] = boost::json::array{std::move(action)};
    } else {
      action["type"] = "block_generation";
      scenario["workloads"] = boost::json::array{std::move(action)};
    }
    BOOST_CHECK_EXCEPTION(
        ParseAndValidateScenario(scenario), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what()) ==
                 "block_generation sync_timeout_sec must be in 1..3600";
        });
  };
  rejects_timeout(false, 0U);
  rejects_timeout(true, kBlockGenerationMaximumSyncTimeoutSeconds + 1U);

  boost::json::object wider_global = MinimalScenario();
  wider_global["sync_timeout_sec"] =
      kBlockGenerationMaximumSyncTimeoutSeconds + 1U;
  BOOST_TEST(ParseAndValidateScenario(wider_global).sync_timeout_sec ==
             kBlockGenerationMaximumSyncTimeoutSeconds + 1U);

  boost::json::object zero_global = MinimalScenario();
  zero_global["sync_timeout_sec"] = 0U;
  BOOST_TEST(ParseAndValidateScenario(zero_global).sync_timeout_sec == 0U);

  boost::json::object nullable_global = MinimalScenario();
  nullable_global["sync_timeout_sec"] = nullptr;
  BOOST_TEST(ParseAndValidateScenario(nullable_global).sync_timeout_sec == 30U);

  boost::json::object distinct = MinimalScenario();
  distinct["ready_timeout_sec"] = 41U;
  distinct["sync_timeout_sec"] = kBlockGenerationMaximumSyncTimeoutSeconds + 1U;
  distinct["workloads"] =
      boost::json::array{boost::json::object{{"type", "block_generation"},
                                             {"node", 1U},
                                             {"count", 1U},
                                             {"sync_timeout_sec", 43U}}};
  const Options distinct_options = ParseAndValidateScenario(distinct);
  const boost::json::object resolved = ResolveScenario(distinct);
  BOOST_TEST(resolved.at("ready_timeout_sec").as_uint64() ==
             distinct_options.ready_timeout_sec);
  BOOST_TEST(resolved.at("sync_timeout_sec").as_uint64() ==
             distinct_options.sync_timeout_sec);
  BOOST_TEST(
      resolved.at("workloads")
          .as_array()
          .front()
          .as_object()
          .at("sync_timeout_sec")
          .as_uint64() ==
      distinct_options.workloads.front().block_generation.sync_timeout_sec);

  wider_global["workloads"] = boost::json::array{boost::json::object{
      {"type", "block_generation"}, {"node", 1U}, {"count", 1U}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(wider_global), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "block_generation sync_timeout_sec must be in 1..3600";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_uses_decimal_kilobytes_per_second_for_bandwidth) {
  for (const std::uint32_t bandwidth_kbps : {0U, 1U, 1000U}) {
    boost::json::object scenario = MinimalScenario();
    scenario["network"] = boost::json::object{
        {"default_condition",
         boost::json::object{{"bandwidth_kbps", bandwidth_kbps}}}};
    const Options options = ParseAndValidateScenario(scenario);
    BOOST_TEST(options.network_condition_requested);
    BOOST_TEST(options.network_condition.bandwidth_kbps == bandwidth_kbps);
    const boost::json::object resolved = ResolveScenario(scenario);
    BOOST_TEST(resolved.at("default_network_condition")
                   .as_object()
                   .at("bandwidth_kbps")
                   .as_uint64() == bandwidth_kbps);
  }

  const auto rejects_bandwidth = [](boost::json::value value,
                                    std::string_view field) {
    boost::json::object scenario = MinimalScenario();
    scenario["network"] = boost::json::object{
        {"default_condition",
         boost::json::object{{std::string(field), std::move(value)}}}};
    BOOST_CHECK_THROW(ParseAndValidateScenario(scenario), std::runtime_error);
  };
  rejects_bandwidth(1U, "bandwidth_mbps");
  rejects_bandwidth(1.5, "bandwidth_kbps");
  rejects_bandwidth("1kB/s", "bandwidth_kbps");
}

BOOST_AUTO_TEST_CASE(scenario_service_allows_explicit_empty_active_run) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 0U;
  const Options options = ParseAndValidateScenario(scenario);
  BOOST_TEST(options.empty_control_plane);
  BOOST_TEST(options.initial_run_requested);
  BOOST_TEST(options.nodes == 0U);
  BOOST_TEST(options.generate_node == 0U);
  BOOST_TEST(!options.block_production.enabled);
  BOOST_TEST(options.node_capacity ==
             ChainDriverSpecFor(ChainKind::kFiro).max_nodes);

  const boost::json::object resolved = ResolveScenario(scenario);
  BOOST_TEST(resolved.at("nodes").as_uint64() == 0U);
  BOOST_TEST(resolved.at("node_capacity").as_uint64() ==
             ChainDriverSpecFor(ChainKind::kFiro).max_nodes);
}

BOOST_AUTO_TEST_CASE(scenario_service_preserves_absent_wallet_lifetime_limit) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 3U;
  scenario["topology"] =
      boost::json::object{{"node_count", 3U},
                          {"wallet_node_count", 2U},
                          {"miner_node_count", 1U},
                          {"wallet_nodes", boost::json::array{1U, 2U}},
                          {"miner_nodes", boost::json::array{3U}}};
  const Options options = ParseAndValidateScenario(scenario);
  const WalletTransactionsWorkload continuous =
      ParseAndValidateWalletTransactionsWorkload(
          boost::json::object{{"type", "wallet_transactions"},
                              {"strategy", "random_bruteforce"},
                              {"retained_balance_percentage", 80.0},
                              {"transaction_rate", 2.0},
                              {"amount", "1.00000000"},
                              {"fee", "0.00001000"}},
          options);
  BOOST_TEST(continuous.transaction_count == 0U);
  BOOST_TEST(!continuous.duration.has_value());
  BOOST_TEST(!ExplicitWalletTransactionAttemptLimit(continuous).has_value());

  const WalletTransactionsWorkload bounded =
      ParseAndValidateWalletTransactionsWorkload(
          boost::json::object{{"type", "wallet_transactions"},
                              {"strategy", "random_bruteforce"},
                              {"retained_balance_percentage", 80.0},
                              {"transaction_count", 8U},
                              {"amount", "1.00000000"},
                              {"fee", "0.00001000"}},
          options);
  BOOST_REQUIRE(ExplicitWalletTransactionAttemptLimit(bounded));
  BOOST_TEST(*ExplicitWalletTransactionAttemptLimit(bounded) == 8U);
}

BOOST_AUTO_TEST_CASE(
    scenario_service_initializes_explicit_wallet_role_without_workload) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] =
      boost::json::array{boost::json::object{{"id", "firo-wallet"},
                                             {"chain", "firo"},
                                             {"role", "wallet"},
                                             {"binary", "/bin/true"}}};

  const Options options = ParseAndValidateScenario(scenario);
  BOOST_TEST(options.wallet_backed_workload_requested);
  BOOST_REQUIRE_EQUAL(options.topology.wallet_nodes.size(), 1U);
  BOOST_TEST(options.topology.wallet_nodes.front() == 0U);
}

BOOST_AUTO_TEST_CASE(
    scenario_service_parses_live_commands_with_scheduled_rules) {
  const Options options = ParseAndValidateScenario(MinimalScenario());
  const boost::json::object input{
      {"kind", "set_resource_limits"},
      {"node", "firo-1"},
      {"resource_limits", boost::json::object{{"cpu_weight", 200U}}}};

  const SimulationCommand command =
      ParseAndValidateSimulationCommand(input, options);
  BOOST_CHECK(command.kind == SimulationCommandKind::kSetResourceLimits);
  BOOST_TEST(command.node_id == "firo-1");
  BOOST_REQUIRE(command.resource_limit_patch.has_value());
  BOOST_REQUIRE(command.resource_limit_patch->cpu_weight.has_value());
  BOOST_TEST(*command.resource_limit_patch->cpu_weight == 200U);

  boost::json::object invalid = input;
  invalid["unexpected"] = true;
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateSimulationCommand(invalid, options), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario scheduled command set_resource_limits has "
               "unsupported field: unexpected";
      });

  boost::json::object invalid_nested = input;
  invalid_nested["resource_limits"] =
      boost::json::object{{"cpu_weight", 200U}, {"unexpected", true}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateSimulationCommand(invalid_nested, options),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario scheduled command resource_limits has unsupported "
               "field: unexpected";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_preserves_scheduled_and_live_role_mutations) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 3U;
  scenario["events"] = boost::json::array{
      boost::json::object{
          {"at", "1s"},
          {"action", "assign_role"},
          {"role_mutation",
           boost::json::object{
               {"node_ids", boost::json::array{"firo-2", "firo-1"}},
               {"roles", boost::json::array{"wallet"}},
               {"mode", "public"},
               {"timeout_sec", 30U}}}},
      boost::json::object{
          {"at", "2s"},
          {"action", "remove_role"},
          {"role_mutation",
           boost::json::object{
               {"node_ids", boost::json::array{"firo-2", "firo-1"}},
               {"roles", boost::json::array{"wallet"}},
               {"timeout_sec", 20U}}}},
      boost::json::object{
          {"at", "3s"},
          {"action", "remove_nodes"},
          {"node_remove",
           boost::json::object{
               {"node_ids", boost::json::array{"firo-2", "firo-1"}}}}}};

  const Options options = ParseAndValidateScenario(scenario);
  BOOST_REQUIRE_EQUAL(options.scheduled_events.size(), 3U);
  const SimulationCommand& assign =
      std::get<SimulationCommand>(options.scheduled_events[0U].action);
  BOOST_TEST(options.scheduled_events[0U].sequence == 1U);
  BOOST_CHECK(assign.kind == SimulationCommandKind::kAssignRole);
  BOOST_TEST(assign.node_id == "sim");
  BOOST_REQUIRE(assign.role_mutation);
  BOOST_TEST(assign.role_mutation->node_ids ==
                 std::vector<std::string>({"firo-2", "firo-1"}),
             boost::test_tools::per_element());
  BOOST_CHECK(assign.role_mutation->role == SimulationRoleKind::kWallet);
  BOOST_REQUIRE(assign.role_mutation->mode);
  BOOST_CHECK(*assign.role_mutation->mode == WalletPrivacyMode::kPublic);
  BOOST_REQUIRE(assign.role_mutation->timeout_sec);
  BOOST_TEST(*assign.role_mutation->timeout_sec == 30U);

  const SimulationCommand& remove =
      std::get<SimulationCommand>(options.scheduled_events[1U].action);
  BOOST_TEST(options.scheduled_events[1U].sequence == 2U);
  BOOST_CHECK(remove.kind == SimulationCommandKind::kRemoveRole);
  BOOST_REQUIRE(remove.role_mutation);
  BOOST_TEST(remove.role_mutation->node_ids ==
                 std::vector<std::string>({"firo-2", "firo-1"}),
             boost::test_tools::per_element());
  BOOST_CHECK(remove.role_mutation->role == SimulationRoleKind::kWallet);
  BOOST_TEST(!remove.role_mutation->mode);
  BOOST_TEST(!remove.role_mutation->funding_wallet_id);
  BOOST_REQUIRE(remove.role_mutation->timeout_sec);
  BOOST_TEST(*remove.role_mutation->timeout_sec == 20U);

  const SimulationCommand& node_remove =
      std::get<SimulationCommand>(options.scheduled_events[2U].action);
  BOOST_CHECK(node_remove.kind == SimulationCommandKind::kRemoveNodes);
  BOOST_REQUIRE(node_remove.node_remove);
  BOOST_TEST(node_remove.node_remove->node_ids ==
                 std::vector<std::string>({"firo-2", "firo-1"}),
             boost::test_tools::per_element());

  const boost::json::object live_request{
      {"kind", "assign_role"},
      {"role_mutation",
       boost::json::object{{"node_ids", boost::json::array{"firo-3"}},
                           {"roles", boost::json::array{"masternode"}},
                           {"funding_wallet_id", "firo-1"},
                           {"timeout_sec", 45U}}}};
  const SimulationCommand live =
      ParseAndValidateSimulationCommand(live_request, options);
  BOOST_CHECK(live.kind == SimulationCommandKind::kAssignRole);
  BOOST_REQUIRE(live.role_mutation);
  BOOST_TEST(
      live.role_mutation->node_ids == std::vector<std::string>({"firo-3"}),
      boost::test_tools::per_element());
  BOOST_CHECK(live.role_mutation->role == SimulationRoleKind::kMasternode);
  BOOST_REQUIRE(live.role_mutation->funding_wallet_id);
  BOOST_TEST(*live.role_mutation->funding_wallet_id == "firo-1");
  BOOST_REQUIRE(live.role_mutation->timeout_sec);
  BOOST_TEST(*live.role_mutation->timeout_sec == 45U);

  const boost::json::object resolved = ResolveScenario(scenario);
  const boost::json::array& resolved_events = resolved.at("events").as_array();
  BOOST_REQUIRE_EQUAL(resolved_events.size(), 3U);
  const boost::json::object& resolved_mutation =
      resolved_events.front().as_object().at("role_mutation").as_object();
  BOOST_TEST(resolved_mutation.at("node_ids").as_array() ==
             boost::json::array({"firo-2", "firo-1"}));
  BOOST_TEST(resolved_mutation.at("roles").as_array() ==
             boost::json::array({"wallet"}));
  BOOST_TEST(resolved_mutation.at("mode").as_string() == "public");

  boost::json::object numeric_node_id = MinimalScenario();
  numeric_node_id["events"] = boost::json::array{boost::json::object{
      {"at", "1s"},
      {"action", "assign_role"},
      {"role_mutation",
       boost::json::object{{"node_ids", boost::json::array{1U}},
                           {"roles", boost::json::array{"wallet"}},
                           {"mode", "public"}}}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(numeric_node_id), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scenario scheduled role_mutation node_ids must contain "
               "strings";
      });

  boost::json::object mismatched_mode = MinimalScenario();
  mismatched_mode["events"] = boost::json::array{boost::json::object{
      {"at", "1s"},
      {"action", "assign_role"},
      {"role_mutation",
       boost::json::object{{"node_ids", boost::json::array{"firo-1"}},
                           {"roles", boost::json::array{"wallet"}},
                           {"mode", "private"}}}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(mismatched_mode), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scheduled wallet assignment mode must match the active run "
               "wallet mode";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_node_add_parser_enforces_shared_public_bounds) {
  boost::json::object empty_scenario = MinimalScenario();
  empty_scenario["nodes"] = 0U;
  const Options empty_options = ParseAndValidateScenario(empty_scenario);
  const SimulationNodeAddRequest maximum =
      ParseAndValidateSimulationNodeAddRequest(
          boost::json::object{{"chain", "firo"}, {"count", 16U}},
          empty_options);
  BOOST_TEST(maximum.count == 16U);
  BOOST_CHECK_THROW(ParseAndValidateSimulationNodeAddRequest(
                        boost::json::object{{"chain", "firo"}, {"count", 17U}},
                        empty_options),
                    std::runtime_error);

  const Options options = ParseAndValidateScenario(MinimalScenario());
  const std::string maximum_id(32U, 'a');
  const SimulationNodeAddRequest named =
      ParseAndValidateSimulationNodeAddRequest(
          boost::json::object{{"chain", "firo"},
                              {"count", 1U},
                              {"node_ids", boost::json::array{maximum_id}}},
          options);
  BOOST_TEST(named.node_ids.front() == maximum_id);
  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeAddRequest(
          boost::json::object{
              {"chain", "firo"},
              {"count", 1U},
              {"node_ids", boost::json::array{std::string(33U, 'a')}}},
          options),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeAddRequest(
          boost::json::object{
              {"chain", "firo"},
              {"count", 1U},
              {"topology", boost::json::object{{"type", "full_mesh"},
                                               {"wallet_node_count", 1U}}}},
          options),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeAddRequest(
          boost::json::object{{"chain", "bitcoin"}, {"count", 1U}}, options),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    scenario_service_node_replace_preserves_target_and_ignores_capacity) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 2U;
  scenario["node_capacity"] = 2U;
  const Options options = ParseAndValidateScenario(scenario);
  const boost::json::object replacement{
      {"chain", "firo"},
      {"count", 1U},
      {"node_ids", boost::json::array{"firo-2"}},
      {"binary", "/opt/firod"},
      {"resources",
       boost::json::object{{"memory_high_bytes", 1U * 1024U * 1024U},
                           {"memory_max_bytes", 2U * 1024U * 1024U}}},
      {"network", boost::json::object{{"delay_ms", 5U}}},
      {"ready_timeout_sec", 41U},
      {"sync_timeout_sec", 43U}};

  const SimulationNodeReplaceRequest parsed =
      ParseAndValidateSimulationNodeReplaceRequest(replacement, "firo-2",
                                                   options);
  BOOST_CHECK(parsed.chain == ChainKind::kFiro);
  BOOST_TEST(parsed.count == 1U);
  BOOST_TEST(parsed.node_ids == std::vector<std::string>({"firo-2"}),
             boost::test_tools::per_element());
  BOOST_REQUIRE(parsed.binary);
  BOOST_TEST(*parsed.binary == "/opt/firod");
  BOOST_REQUIRE(parsed.resources);
  BOOST_REQUIRE(parsed.resources->memory_max_bytes);
  BOOST_TEST(*parsed.resources->memory_max_bytes == 2U * 1024U * 1024U);
  BOOST_REQUIRE(parsed.network);
  BOOST_TEST(parsed.network->delay_ms == 5U);
  BOOST_TEST(parsed.ready_timeout_sec == 41U);
  BOOST_TEST(parsed.sync_timeout_sec == 43U);

  const SimulationCommand command = ParseAndValidateSimulationCommand(
      boost::json::object{{"kind", "replace_node"},
                          {"node", "firo-2"},
                          {"node_replace", replacement}},
      options);
  BOOST_CHECK(command.kind == SimulationCommandKind::kReplaceNode);
  BOOST_TEST(command.node_id == "firo-2");
  BOOST_REQUIRE(command.node_replace);

  const auto rejects = [&](boost::json::object invalid,
                           std::string_view target = "firo-2") {
    BOOST_CHECK_THROW(
        ParseAndValidateSimulationNodeReplaceRequest(invalid, target, options),
        std::runtime_error);
  };
  boost::json::object invalid = replacement;
  invalid["chain"] = "bitcoin";
  rejects(invalid);
  invalid = replacement;
  invalid["count"] = 2U;
  rejects(invalid);
  invalid = replacement;
  invalid["node_ids"] = boost::json::array{"firo-1"};
  rejects(invalid);
  invalid = replacement;
  invalid["topology"] = boost::json::object{{"type", "ring"}};
  rejects(invalid);
  rejects(replacement, "firo-3");
}

BOOST_AUTO_TEST_CASE(
    scenario_service_node_remove_parser_enforces_active_batch_bounds) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 3U;
  const Options options = ParseAndValidateScenario(scenario);

  const SimulationNodeRemoveRequest request =
      ParseAndValidateSimulationNodeRemoveRequest(
          boost::json::object{
              {"node_ids", boost::json::array{"firo-2", "firo-3"}},
              {"timeout_sec", 41U}},
          options);
  BOOST_TEST(request.node_ids == std::vector<std::string>({"firo-2", "firo-3"}),
             boost::test_tools::per_element());
  BOOST_TEST(request.timeout_sec == 41U);

  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeRemoveRequest(
          boost::json::object{
              {"node_ids", boost::json::array{"firo-2", "firo-2"}}},
          options),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeRemoveRequest(
          boost::json::object{{"node_ids", boost::json::array{"firo-missing"}}},
          options),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeRemoveRequest(
          boost::json::object{{"node_ids", boost::json::array{}}}, options),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ParseAndValidateSimulationNodeRemoveRequest(
          boost::json::object{{"node_ids", boost::json::array{"firo-2"}},
                              {"timeout_sec", 0U}},
          options),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    scenario_service_preflights_scheduled_node_adds_in_execution_order) {
  boost::json::object scenario = MinimalScenario();
  scenario["node_capacity"] = 4U;
  scenario["resource_profiles"] = boost::json::object{
      {"large", boost::json::object{{"memory_max", "2GiB"}}}};
  scenario["events"] = boost::json::array{
      boost::json::object{
          {"at", "2s"},
          {"action", "add_nodes"},
          {"node_add",
           boost::json::object{
               {"chain", "firo"},
               {"count", 2U},
               {"topology",
                boost::json::object{{"type", "star"}, {"center_node", 4U}}}}}},
      boost::json::object{
          {"at", "1s"},
          {"action", "add_nodes"},
          {"node_add",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"new-a"}}}}},
      boost::json::object{{"at", "3s"},
                          {"action", "set_resource_profile"},
                          {"nodes", boost::json::array{"new-a"}},
                          {"profile", "large"}},
      boost::json::object{
          {"at", "4s"}, {"action", "restart_node"}, {"node", "new-a"}},
      boost::json::object{
          {"at", "5s"}, {"action", "restart_node"}, {"node", 2U}}};

  const Options options = ParseAndValidateScenario(scenario);
  BOOST_REQUIRE_EQUAL(options.scheduled_events.size(), 5U);
  BOOST_TEST(options.nodes == 1U);
  BOOST_TEST(options.scheduled_events[0].sequence == 2U);
  BOOST_TEST(options.scheduled_events[1].sequence == 1U);
  const SimulationCommand& later =
      std::get<SimulationCommand>(options.scheduled_events[1].action);
  BOOST_REQUIRE(later.node_add);
  BOOST_REQUIRE(later.node_add->topology);
  BOOST_TEST(later.node_add->topology->star_center == 3U);
  BOOST_REQUIRE_EQUAL(later.node_add->node_ids.size(), 2U);
  BOOST_TEST(later.node_add->node_ids[0] == "firo-2");
  BOOST_TEST(later.node_add->node_ids[1] == "firo-3");
  const ScenarioWorkload& id_vector_target =
      std::get<ScenarioWorkload>(options.scheduled_events[2].action);
  BOOST_CHECK(id_vector_target.kind == WorkloadKind::kSetResourceProfile);
  BOOST_TEST(id_vector_target.profile_switch.node_ids ==
                 std::vector<std::string>{"new-a"},
             boost::test_tools::per_element());
  BOOST_TEST(
      id_vector_target.profile_switch.nodes == std::vector<std::uint32_t>{2U},
      boost::test_tools::per_element());
  const SimulationCommand& explicit_target =
      std::get<SimulationCommand>(options.scheduled_events[3].action);
  BOOST_TEST(explicit_target.node_id == "new-a");
  const ScenarioWorkload& numeric_target =
      std::get<ScenarioWorkload>(options.scheduled_events[4].action);
  BOOST_CHECK(numeric_target.kind == WorkloadKind::kRestartNode);
  BOOST_TEST(numeric_target.restart_node.node == 2U);
}

BOOST_AUTO_TEST_CASE(
    scenario_service_applies_default_capacity_before_scheduled_node_adds) {
  boost::json::object scenario = MinimalScenario();
  scenario["events"] = boost::json::array{boost::json::object{
      {"at", "1s"},
      {"action", "add_nodes"},
      {"node_add", boost::json::object{{"chain", "firo"}, {"count", 1U}}}}};

  const Options options = ParseAndValidateScenario(scenario);
  BOOST_TEST(options.node_capacity ==
             ChainDriverSpecFor(ChainKind::kFiro).max_nodes);
  BOOST_REQUIRE_EQUAL(options.scheduled_events.size(), 1U);
  const SimulationCommand& add =
      std::get<SimulationCommand>(options.scheduled_events.front().action);
  BOOST_REQUIRE(add.node_add);
  BOOST_REQUIRE_EQUAL(add.node_add->node_ids.size(), 1U);
  BOOST_TEST(add.node_add->node_ids.front() == "firo-2");
}

BOOST_AUTO_TEST_CASE(
    scenario_service_rejects_cumulative_scheduled_node_add_over_capacity) {
  boost::json::object scenario = MinimalScenario();
  scenario["node_capacity"] = 4U;
  scenario["events"] = boost::json::array{
      boost::json::object{
          {"at", "2s"},
          {"action", "add_nodes"},
          {"node_add", boost::json::object{{"chain", "firo"}, {"count", 2U}}}},
      boost::json::object{
          {"at", "1s"},
          {"action", "add_nodes"},
          {"node_add", boost::json::object{{"chain", "firo"}, {"count", 2U}}}}};

  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(scenario), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "node.add request exceeds the configured node capacity";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_rejects_duplicate_scheduled_node_add_ids_before_run) {
  boost::json::object scenario = MinimalScenario();
  scenario["node_capacity"] = 4U;
  scenario["events"] = boost::json::array{
      boost::json::object{
          {"at", "1s"},
          {"action", "add_nodes"},
          {"node_add",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"reserved"}}}}},
      boost::json::object{
          {"at", "2s"},
          {"action", "add_nodes"},
          {"node_add",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"reserved"}}}}}};

  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(scenario), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scheduled node.add node id is already reserved: reserved";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_plans_selected_remove_then_add_without_id_reuse) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 3U;
  scenario["node_capacity"] = 3U;
  scenario["events"] = boost::json::array{
      boost::json::object{
          {"at", "1s"},
          {"action", "remove_nodes"},
          {"node_remove",
           boost::json::object{{"node_ids", boost::json::array{"firo-2"}},
                               {"timeout_sec", 20U}}}},
      boost::json::object{
          {"at", "2s"},
          {"action", "add_nodes"},
          {"node_add", boost::json::object{{"chain", "firo"}, {"count", 1U}}}}};

  const Options options = ParseAndValidateScenario(scenario);
  BOOST_REQUIRE_EQUAL(options.scheduled_events.size(), 2U);
  const SimulationCommand& remove =
      std::get<SimulationCommand>(options.scheduled_events[0U].action);
  BOOST_CHECK(remove.kind == SimulationCommandKind::kRemoveNodes);
  BOOST_TEST(remove.node_id == "sim");
  BOOST_REQUIRE(remove.node_remove);
  BOOST_TEST(
      remove.node_remove->node_ids == std::vector<std::string>({"firo-2"}),
      boost::test_tools::per_element());
  BOOST_TEST(remove.node_remove->timeout_sec == 20U);
  const SimulationCommand& add =
      std::get<SimulationCommand>(options.scheduled_events[1U].action);
  BOOST_REQUIRE(add.node_add);
  BOOST_TEST(add.node_add->node_ids == std::vector<std::string>({"firo-4"}),
             boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(
    scenario_service_rejects_reusing_or_removing_inactive_scheduled_id) {
  boost::json::object reused = MinimalScenario();
  reused["nodes"] = 2U;
  reused["node_capacity"] = 2U;
  reused["events"] = boost::json::array{
      boost::json::object{
          {"at", "1s"},
          {"action", "remove_nodes"},
          {"node_remove",
           boost::json::object{{"node_ids", boost::json::array{"firo-2"}}}}},
      boost::json::object{
          {"at", "2s"},
          {"action", "add_nodes"},
          {"node_add",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"firo-2"}}}}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(reused), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scheduled node.add node id is already reserved: firo-2";
      });

  boost::json::object removed_twice = MinimalScenario();
  removed_twice["nodes"] = 2U;
  removed_twice["events"] = boost::json::array{
      boost::json::object{
          {"at", "1s"},
          {"action", "remove_nodes"},
          {"node_remove",
           boost::json::object{{"node_ids", boost::json::array{"firo-2"}}}}},
      boost::json::object{
          {"at", "2s"},
          {"action", "remove_nodes"},
          {"node_remove",
           boost::json::object{{"node_ids", boost::json::array{"firo-2"}}}}}};
  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(removed_twice), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "node.remove references an inactive node id: firo-2";
      });
}

BOOST_AUTO_TEST_CASE(
    scenario_service_freezes_height_wait_target_after_scheduled_compaction) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 2U;
  scenario["node_capacity"] = 2U;
  scenario["topology"] =
      boost::json::object{{"node_count", 2U},
                          {"wallet_node_count", 0U},
                          {"miner_node_count", 0U},
                          {"wallet_nodes", boost::json::array{}},
                          {"miner_nodes", boost::json::array{}}};
  scenario["events"] = boost::json::array{
      boost::json::object{
          {"at", "1s"},
          {"action", "remove_nodes"},
          {"node_remove",
           boost::json::object{{"node_ids", boost::json::array{"firo-1"}}}}},
      boost::json::object{{"at", "2s"},
                          {"action", "wait_until_height"},
                          {"node", 1U},
                          {"height", 1U},
                          {"timeout_sec", 3U}}};

  const Options options = ParseAndValidateScenario(scenario);
  BOOST_REQUIRE_EQUAL(options.scheduled_events.size(), 2U);
  const ScenarioWorkload& wait =
      std::get<ScenarioWorkload>(options.scheduled_events[1U].action);
  BOOST_CHECK(wait.kind == WorkloadKind::kWaitUntilHeight);
  BOOST_TEST(wait.wait_until_height.node == 1U);
  BOOST_TEST(wait.wait_until_height.node_id == "firo-2");
}

BOOST_AUTO_TEST_CASE(scenario_service_rejects_scheduled_removal_of_role_node) {
  boost::json::object scenario = MinimalScenario();
  scenario["nodes"] = 3U;
  scenario["topology"] =
      boost::json::object{{"node_count", 3U},
                          {"wallet_node_count", 1U},
                          {"miner_node_count", 1U},
                          {"wallet_nodes", boost::json::array{2U}},
                          {"miner_nodes", boost::json::array{3U}}};
  scenario["events"] = boost::json::array{boost::json::object{
      {"at", "1s"},
      {"action", "remove_nodes"},
      {"node_remove",
       boost::json::object{{"node_ids", boost::json::array{"firo-2"}}}}}};

  BOOST_CHECK_EXCEPTION(
      ParseAndValidateScenario(scenario), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "scheduled node.remove requires wallet.remove before removing "
               "wallet node firo-2";
      });
}

}  // namespace bbp
