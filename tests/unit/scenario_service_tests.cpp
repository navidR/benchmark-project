#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <variant>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/scenario_service.h"

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
  BOOST_TEST(options.chain_daemon == std::filesystem::path("/bin/true"));

  const boost::json::object resolved = ResolveScenario(scenario);
  BOOST_TEST(resolved.at("run_id").as_string() == "scenario-service-test");
  BOOST_TEST(resolved.at("chain").as_string() == "firo");
  BOOST_TEST(resolved.at("nodes").as_uint64() == 1U);
  BOOST_TEST(resolved.at("chain_daemon").as_string() == "/bin/true");
  BOOST_TEST(resolved.at("node_configs")
                 .as_array()
                 .front()
                 .as_object()
                 .at("id")
                 .as_string() == "firo-1");
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

}  // namespace bbp
