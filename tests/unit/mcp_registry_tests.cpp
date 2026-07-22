#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <cstddef>
#include <set>
#include <string>
#include <string_view>

#include "bbp/chain_kind.h"
#include "bbp/mcp_registry.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/workload_kind.h"
#include "bbp/tui_command_parser.h"

namespace bbp {
namespace {

const boost::json::array& ArrayField(const boost::json::object& object,
                                     std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  BOOST_REQUIRE(value != nullptr);
  BOOST_REQUIRE(value->is_array());
  return value->as_array();
}

std::set<std::string> StringSet(const boost::json::array& array) {
  std::set<std::string> result;
  for (const boost::json::value& value : array) {
    BOOST_REQUIRE(value.is_string());
    BOOST_REQUIRE(result.emplace(value.as_string()).second);
  }
  return result;
}

std::set<std::string> NamedSet(const boost::json::array& array) {
  std::set<std::string> result;
  for (const boost::json::value& value : array) {
    BOOST_REQUIRE(value.is_object());
    const boost::json::value* name = value.as_object().if_contains("name");
    BOOST_REQUIRE(name != nullptr);
    BOOST_REQUIRE(name->is_string());
    BOOST_REQUIRE(result.emplace(name->as_string()).second);
  }
  return result;
}

}  // namespace

BOOST_AUTO_TEST_CASE(mcp_registry_mechanically_covers_typed_enums) {
  const boost::json::object document = BuildMcpCapabilityDocument();
  const auto chains = StringSet(ArrayField(document, "chains"));
  const auto workloads = StringSet(ArrayField(document, "workloads"));
  const auto commands = StringSet(ArrayField(document, "runtime_commands"));
  const auto events = StringSet(ArrayField(document, "events"));
  const auto local_actions =
      StringSet(ArrayField(document, "tui_local_actions"));

  BOOST_TEST(chains.size() == static_cast<std::size_t>(ChainKind::kCount));
  BOOST_TEST(workloads.size() ==
             static_cast<std::size_t>(WorkloadKind::kCount));
  BOOST_TEST(commands.size() ==
             static_cast<std::size_t>(SimulationCommandKind::kCount));
  BOOST_TEST(events.size() ==
             static_cast<std::size_t>(SimulationEventKind::kCount));
  BOOST_TEST(local_actions.size() ==
             static_cast<std::size_t>(TuiLocalAction::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(SimulationCommandKind::kCount);
       ++index) {
    BOOST_TEST(commands.contains(std::string(
        SimulationCommandKindName(static_cast<SimulationCommandKind>(index)))));
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    BOOST_TEST(workloads.contains(
        std::string(WorkloadKindName(static_cast<WorkloadKind>(index)))));
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(TuiLocalAction::kCount); ++index) {
    BOOST_TEST(local_actions.contains(
        std::string(TuiLocalActionName(static_cast<TuiLocalAction>(index)))));
  }
}

BOOST_AUTO_TEST_CASE(mcp_registry_covers_every_tui_command_and_local_action) {
  const boost::json::object document = BuildMcpCapabilityDocument();
  const auto tui_commands = StringSet(ArrayField(document, "tui_commands"));
  BOOST_TEST(tui_commands.size() == TuiCommandParser::CommandNames().size());
  for (const std::string_view name : TuiCommandParser::CommandNames()) {
    BOOST_TEST(tui_commands.contains(std::string(name)));
  }
  const auto operations = NamedSet(ArrayField(document, "operations"));
  BOOST_TEST(operations.contains("simulation.command"));
  BOOST_TEST(operations.contains("local.firo_qt_launcher"));
  for (const std::string_view operation : {"node.add",
                                           "node.remove",
                                           "node.stop",
                                           "node.kill",
                                           "node.restart",
                                           "node.replace",
                                           "wallet.add",
                                           "wallet.remove",
                                           "role.assign",
                                           "role.remove",
                                           "miner.add",
                                           "miner.remove",
                                           "masternode.add",
                                           "masternode.remove",
                                           "masternode.restart",
                                           "workload.start",
                                           "workload.reconfigure",
                                           "workload.pause",
                                           "workload.resume",
                                           "workload.stop",
                                           "instrumentation.start",
                                           "instrumentation.reconfigure",
                                           "instrumentation.stop",
                                           "evidence.query",
                                           "log.query",
                                           "log.follow",
                                           "artifact.read"}) {
    BOOST_TEST(operations.contains(std::string(operation)));
  }
}

BOOST_AUTO_TEST_CASE(mcp_registry_exposes_every_information_family_and_bound) {
  const boost::json::object document = BuildMcpCapabilityDocument();
  const auto families = NamedSet(ArrayField(document, "information_families"));
  BOOST_TEST(families.size() ==
             static_cast<std::size_t>(McpInformationFamily::kCount));
  BOOST_TEST(BuildMcpResourceRegistry().size() == families.size());
  BOOST_TEST(BuildMcpToolRegistry().size() ==
             static_cast<std::size_t>(McpOperationKind::kCount));
  const auto results = NamedSet(ArrayField(document, "result_families"));
  BOOST_TEST(results.size() ==
             static_cast<std::size_t>(McpResultFamily::kCount));
  BOOST_TEST(results.contains("error"));
  BOOST_TEST(results.contains("cleanup"));
  BOOST_TEST(results.contains("workload"));

  const boost::json::value* limits = document.if_contains("limits");
  BOOST_REQUIRE(limits != nullptr);
  BOOST_REQUIRE(limits->is_object());
  BOOST_TEST(limits->as_object().at("sessions").as_uint64() ==
             kMcpMaximumSessions);
  BOOST_TEST(limits->as_object().at("tasks_per_session").as_uint64() ==
             kMcpMaximumTasksPerSession);
  BOOST_TEST(limits->as_object().at("notifications_per_session").as_uint64() ==
             kMcpMaximumNotificationsPerSession);
}

BOOST_AUTO_TEST_CASE(mcp_scenario_schema_has_unique_authoritative_members) {
  const auto members = McpScenarioMemberRegistry();
  BOOST_TEST(members.size() >= 120U);
  std::set<std::string_view> unique;
  for (const std::string_view member : members) {
    BOOST_TEST(!member.empty());
    BOOST_TEST(unique.emplace(member).second);
  }
  const boost::json::object schema = BuildMcpScenarioSchema();
  BOOST_TEST(schema.at("$schema").as_string() ==
             "https://json-schema.org/draft/2020-12/schema");
  BOOST_TEST(schema.at("additionalProperties").as_bool() == false);
  BOOST_TEST(schema.at("x-bbp-members").as_array().size() == members.size());
  BOOST_TEST(schema.at("x-bbp-workload-kinds").as_array().size() ==
             static_cast<std::size_t>(WorkloadKind::kCount));
}

}  // namespace bbp
