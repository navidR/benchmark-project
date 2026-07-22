#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <stdexcept>
#include <string>

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
}

}  // namespace bbp
