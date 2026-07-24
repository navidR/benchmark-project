#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <cstddef>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/default_peer_topology.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_registry.h"
#include "bbp/scenario_fields.h"
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

std::set<std::string> PropertySet(const boost::json::object& schema) {
  const boost::json::value* properties = schema.if_contains("properties");
  BOOST_REQUIRE(properties != nullptr);
  BOOST_REQUIRE(properties->is_object());
  std::set<std::string> result;
  for (const auto& member : properties->as_object()) {
    BOOST_REQUIRE(result.emplace(member.key()).second);
  }
  return result;
}

std::set<std::string> FieldSet(
    std::span<const std::string_view> fields,
    std::initializer_list<std::string_view> additional = {}) {
  std::set<std::string> result;
  for (const std::string_view field : fields) {
    BOOST_REQUIRE(result.emplace(field).second);
  }
  for (const std::string_view field : additional) {
    BOOST_REQUIRE(result.emplace(field).second);
  }
  return result;
}

void RequireClosedSchemaTree(const boost::json::value& value) {
  if (value.is_array()) {
    for (const boost::json::value& child : value.as_array()) {
      RequireClosedSchemaTree(child);
    }
    return;
  }
  if (!value.is_object()) {
    return;
  }
  const boost::json::object& object = value.as_object();
  const boost::json::value* type = object.if_contains("type");
  if (type != nullptr && type->is_string() && type->as_string() == "object") {
    const boost::json::value* additional =
        object.if_contains("additionalProperties");
    BOOST_REQUIRE(additional != nullptr);
    BOOST_REQUIRE(additional->is_bool());
    BOOST_TEST(additional->as_bool() == false);
  }
  for (const std::string_view keyword :
       {"properties", "patternProperties", "items", "oneOf", "anyOf", "allOf",
        "not", "if", "then", "else"}) {
    const boost::json::value* child = object.if_contains(keyword);
    if (child != nullptr) {
      RequireClosedSchemaTree(*child);
    }
  }
}

const boost::json::object& VariantWithConst(const boost::json::array& variants,
                                            std::string_view discriminator,
                                            std::string_view name) {
  for (const boost::json::value& variant : variants) {
    BOOST_REQUIRE(variant.is_object());
    const boost::json::object& object = variant.as_object();
    const boost::json::value* properties = object.if_contains("properties");
    BOOST_REQUIRE(properties != nullptr);
    BOOST_REQUIRE(properties->is_object());
    const boost::json::value* field =
        properties->as_object().if_contains(discriminator);
    if (field != nullptr && field->is_object()) {
      const boost::json::value* constant =
          field->as_object().if_contains("const");
      if (constant != nullptr && constant->is_string() &&
          constant->as_string() == name) {
        return object;
      }
    }
  }
  BOOST_FAIL("missing schema variant " << name);
  return variants.front().as_object();
}

const boost::json::object& LifecycleOperationConstraint(
    const boost::json::object& operation_schema, std::string_view operation,
    std::string_view state) {
  for (const boost::json::value& constraint :
       operation_schema.at("allOf").as_array()) {
    const boost::json::object& object = constraint.as_object();
    const boost::json::value* condition = object.if_contains("if");
    if (condition == nullptr) {
      continue;
    }
    const boost::json::object& condition_properties =
        condition->as_object().at("properties").as_object();
    const boost::json::value* operation_property =
        condition_properties.if_contains("operation");
    const boost::json::value* state_property =
        condition_properties.if_contains("state");
    if (operation_property != nullptr &&
        operation_property->as_object().if_contains("const") != nullptr &&
        operation_property->as_object().at("const").as_string() == operation &&
        state_property != nullptr &&
        state_property->as_object().if_contains("const") != nullptr &&
        state_property->as_object().at("const").as_string() == state) {
      return object;
    }
  }
  BOOST_FAIL("missing lifecycle operation constraint " << operation << " "
                                                       << state);
  return operation_schema;
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
  const auto notification_methods =
      StringSet(ArrayField(document, "notification_methods"));

  BOOST_TEST(chains.size() == static_cast<std::size_t>(ChainKind::kCount));
  BOOST_TEST(workloads.size() ==
             static_cast<std::size_t>(WorkloadKind::kCount));
  BOOST_TEST(commands.size() ==
             static_cast<std::size_t>(SimulationCommandKind::kCount));
  BOOST_TEST(events.size() ==
             static_cast<std::size_t>(SimulationEventKind::kCount));
  BOOST_TEST(local_actions.size() ==
             static_cast<std::size_t>(TuiLocalAction::kCount));
  BOOST_TEST(notification_methods.size() == kMcpNotificationMethods.size());
  for (const std::string_view method : kMcpNotificationMethods) {
    BOOST_TEST(notification_methods.contains(std::string(method)));
  }
  const boost::json::object& notification_schemas =
      document.at("notification_schemas").as_object();
  BOOST_REQUIRE(notification_schemas.size() == notification_methods.size());
  for (const std::string_view method : kMcpNotificationMethods) {
    const boost::json::object& schema =
        notification_schemas.at(method).as_object();
    BOOST_TEST(schema.at("type").as_string() == "object");
    BOOST_TEST(schema.at("additionalProperties").as_bool() == false);
    RequireClosedSchemaTree(schema);
  }
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
  BOOST_TEST(document.at("protocol_version").as_string() ==
             kMcpProtocolVersion);
  const boost::json::array& supported_versions =
      document.at("supported_protocol_versions").as_array();
  BOOST_REQUIRE_EQUAL(supported_versions.size(), 2U);
  BOOST_TEST(supported_versions[0].as_string() == "2025-11-25");
  BOOST_TEST(supported_versions[1].as_string() == "2025-06-18");
  BOOST_REQUIRE_EQUAL(supported_versions.size(),
                      kMcpSupportedProtocolVersions.size());
  for (std::size_t index = 0U; index < supported_versions.size(); ++index) {
    BOOST_TEST(supported_versions[index].as_string() ==
               kMcpSupportedProtocolVersions[index]);
  }
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
  BOOST_TEST(limits->as_object().at("subscriptions_per_session").as_uint64() ==
             kMcpMaximumSubscriptionsPerSession);
  BOOST_TEST(limits->as_object().at("notifications_per_session").as_uint64() ==
             kMcpMaximumNotificationsPerSession);
  BOOST_TEST(limits->as_object().at("retained_operations").as_uint64() ==
             kMcpMaximumRetainedOperations);
  BOOST_TEST(limits->as_object().at("retained_result_bytes").as_uint64() ==
             kMcpMaximumRetainedResultBytes);
  BOOST_TEST(limits->as_object().at("evidence_text_bytes").as_uint64() ==
             kMcpMaximumEvidenceTextBytes);
  BOOST_TEST(limits->as_object().at("selection_items").as_uint64() ==
             kMcpMaximumSelectionItems);
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

BOOST_AUTO_TEST_CASE(mcp_scenario_object_schemas_match_every_descriptor) {
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ScenarioObjectKind::kCount); ++index) {
    const auto kind = static_cast<ScenarioObjectKind>(index);
    const boost::json::object schema = BuildMcpScenarioObjectSchema(kind);
    BOOST_TEST(schema.at("additionalProperties").as_bool() == false);
    BOOST_TEST(PropertySet(schema) == FieldSet(ScenarioObjectFields(kind)));
  }

  const boost::json::object scenario = BuildMcpScenarioSchema();
  std::set<std::string> expected =
      FieldSet(ScenarioObjectFields(ScenarioObjectKind::kRoot));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
    expected.emplace(ChainDriverSpecFor(static_cast<ChainKind>(index))
                         .daemon_scenario_field);
  }
  BOOST_TEST(PropertySet(scenario) == expected);
  BOOST_REQUIRE(scenario.contains("allOf"));
  BOOST_TEST(scenario.at("allOf").as_array().size() ==
             static_cast<std::size_t>(ChainKind::kCount) * 2U + 2U);
  RequireClosedSchemaTree(scenario);
}

BOOST_AUTO_TEST_CASE(mcp_topology_workload_and_command_schemas_are_exhaustive) {
  const boost::json::object scenario = BuildMcpScenarioSchema();
  const boost::json::object& scenario_properties =
      scenario.at("properties").as_object();
  const boost::json::array& topologies =
      scenario_properties.at("topology").as_object().at("oneOf").as_array();
  BOOST_REQUIRE(topologies.size() ==
                static_cast<std::size_t>(PeerTopologyKind::kCount));
  for (std::size_t index = 0U; index < topologies.size(); ++index) {
    const auto kind = static_cast<PeerTopologyKind>(index);
    const boost::json::object& variant = topologies[index].as_object();
    std::set<std::string> expected = FieldSet(ScenarioTopologyCommonFields());
    for (const std::string_view field : ScenarioTopologyKindFields(kind)) {
      expected.emplace(field);
    }
    BOOST_TEST(PropertySet(variant) == expected);
    BOOST_TEST(variant.at("properties")
                   .as_object()
                   .at("type")
                   .as_object()
                   .at("const")
                   .as_string() == PeerTopologyKindName(kind));
    BOOST_TEST(variant.at("additionalProperties").as_bool() == false);
    const boost::json::value* required = variant.if_contains("required");
    if (kind == PeerTopologyKind::kFullMesh) {
      BOOST_TEST(required == nullptr);
    } else {
      BOOST_REQUIRE(required != nullptr);
      BOOST_TEST(StringSet(required->as_array()).contains("type"));
    }
  }

  const boost::json::object workload_schema = BuildMcpWorkloadSchema();
  const boost::json::array& workloads = workload_schema.at("oneOf").as_array();
  BOOST_REQUIRE(workloads.size() ==
                static_cast<std::size_t>(WorkloadKind::kCount));
  for (std::size_t index = 0U; index < workloads.size(); ++index) {
    const auto kind = static_cast<WorkloadKind>(index);
    const boost::json::object& variant = workloads[index].as_object();
    BOOST_TEST(PropertySet(variant) ==
               FieldSet(ScenarioWorkloadFields(kind), {"type"}));
    BOOST_TEST(variant.at("properties")
                   .as_object()
                   .at("type")
                   .as_object()
                   .at("const")
                   .as_string() == WorkloadKindName(kind));
    BOOST_TEST(variant.at("additionalProperties").as_bool() == false);
  }

  const boost::json::object command_schema = BuildMcpSimulationCommandSchema();
  const boost::json::array& commands = command_schema.at("oneOf").as_array();
  BOOST_REQUIRE(commands.size() ==
                static_cast<std::size_t>(SimulationCommandKind::kCount));
  for (std::size_t index = 0U; index < commands.size(); ++index) {
    const auto kind = static_cast<SimulationCommandKind>(index);
    std::set<std::string> expected =
        FieldSet(ScenarioCommandFields(kind), {"kind"});
    if (ScenarioCommandFieldAllowed(kind, "node")) {
      expected.emplace("node");
    }
    const boost::json::object& variant = commands[index].as_object();
    BOOST_TEST(PropertySet(variant) == expected);
    BOOST_TEST(variant.at("properties")
                   .as_object()
                   .at("kind")
                   .as_object()
                   .at("const")
                   .as_string() == SimulationCommandKindName(kind));
    BOOST_TEST(variant.at("additionalProperties").as_bool() == false);
  }
  RequireClosedSchemaTree(BuildMcpWorkloadSchema());
  RequireClosedSchemaTree(BuildMcpSimulationCommandSchema());
}

BOOST_AUTO_TEST_CASE(mcp_schema_builders_reject_enum_sentinels) {
  BOOST_CHECK_THROW(BuildMcpScenarioObjectSchema(ScenarioObjectKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(BuildMcpOperationInputSchema(McpOperationKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(McpOperationResultFamily(McpOperationKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(BuildMcpOperationOutputSchema(McpOperationKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(BuildMcpResultSchema(McpResultFamily::kCount),
                    std::logic_error);

  const boost::json::object node_profile =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNodeProfile);
  BOOST_TEST(StringSet(node_profile.at("required").as_array()) ==
             std::set<std::string>{"profile"});
}

BOOST_AUTO_TEST_CASE(mcp_scheduled_events_cover_every_registered_action) {
  const boost::json::object scenario = BuildMcpScenarioSchema();
  const boost::json::array& variants = scenario.at("properties")
                                           .as_object()
                                           .at("events")
                                           .as_object()
                                           .at("items")
                                           .as_object()
                                           .at("oneOf")
                                           .as_array();
  BOOST_REQUIRE(variants.size() ==
                static_cast<std::size_t>(WorkloadKind::kCount) +
                    static_cast<std::size_t>(SimulationCommandKind::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    const auto kind = static_cast<WorkloadKind>(index);
    BOOST_TEST(PropertySet(variants[index].as_object()) ==
               FieldSet(ScenarioWorkloadFields(kind), {"at", "action"}));
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(SimulationCommandKind::kCount);
       ++index) {
    const auto kind = static_cast<SimulationCommandKind>(index);
    std::set<std::string> expected =
        FieldSet(ScenarioCommandFields(kind), {"at", "action"});
    if (ScenarioCommandFieldAllowed(kind, "node")) {
      expected.emplace("node");
    }
    BOOST_TEST(
        PropertySet(
            variants[static_cast<std::size_t>(WorkloadKind::kCount) + index]
                .as_object()) == expected);
  }
}

BOOST_AUTO_TEST_CASE(mcp_wallet_and_perf_schemas_preserve_production_types) {
  const boost::json::object wallet_send =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kWalletSend);
  const boost::json::array& amount_choices = wallet_send.at("properties")
                                                 .as_object()
                                                 .at("amount")
                                                 .as_object()
                                                 .at("oneOf")
                                                 .as_array();
  BOOST_TEST(amount_choices.front().as_object().at("maximum").as_uint64() ==
             std::numeric_limits<std::uint64_t>::max());

  const boost::json::object workload_schema = BuildMcpWorkloadSchema();
  const boost::json::array& workloads = workload_schema.at("oneOf").as_array();
  const boost::json::object& wallet_transactions = VariantWithConst(
      workloads, "type", WorkloadKindName(WorkloadKind::kWalletTransactions));
  const boost::json::object& wallet_properties =
      wallet_transactions.at("properties").as_object();
  BOOST_TEST(wallet_properties.at("interval")
                 .as_object()
                 .at("oneOf")
                 .as_array()
                 .size() == 2U);
  BOOST_TEST(wallet_properties.at("sender_wallets")
                 .as_object()
                 .at("items")
                 .as_object()
                 .at("minimum")
                 .as_uint64() == 1U);
  BOOST_TEST(wallet_properties.at("funding_strategy")
                 .as_object()
                 .at("enum")
                 .as_array()
                 .size() == 2U);
  BOOST_TEST(wallet_properties.at("strategy")
                 .as_object()
                 .at("enum")
                 .as_array()
                 .size() == 6U);

  const boost::json::object perf_target =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kPerfTarget);
  BOOST_TEST(perf_target.at("properties")
                 .as_object()
                 .at("kind")
                 .as_object()
                 .at("enum")
                 .as_array()
                 .size() == 4U);
  const boost::json::object command_schema = BuildMcpSimulationCommandSchema();
  const boost::json::object perf_command = VariantWithConst(
      command_schema.at("oneOf").as_array(), "kind",
      SimulationCommandKindName(SimulationCommandKind::kSetPerfCounters));
  BOOST_TEST(perf_command.at("properties")
                 .as_object()
                 .at("perf_counters")
                 .as_object()
                 .at("items")
                 .as_object()
                 .at("enum")
                 .as_array()
                 .size() == 9U);
}

BOOST_AUTO_TEST_CASE(mcp_tool_and_result_schemas_have_mechanical_parity) {
  const boost::json::array tools = BuildMcpToolRegistry();
  BOOST_REQUIRE(tools.size() ==
                static_cast<std::size_t>(McpOperationKind::kCount));
  for (std::size_t index = 0U; index < tools.size(); ++index) {
    const auto operation = static_cast<McpOperationKind>(index);
    const boost::json::object& tool = tools[index].as_object();
    BOOST_TEST(tool.at("name").as_string() == McpOperationKindName(operation));
    const boost::json::object& input = tool.at("inputSchema").as_object();
    BOOST_TEST(input.at("additionalProperties").as_bool() == false);
    RequireClosedSchemaTree(input);
    const boost::json::object& output = tool.at("outputSchema").as_object();
    BOOST_TEST(output.at("type").as_string() == "object");
    BOOST_TEST(tool.if_contains("execution") == nullptr);
    const std::size_t expected_output_choices =
        McpOperationResultFamily(operation) == McpResultFamily::kOperation ? 2U
                                                                           : 3U;
    BOOST_REQUIRE(output.at("oneOf").as_array().size() ==
                  expected_output_choices);
    const boost::json::object& success =
        output.at("oneOf").as_array().front().as_object();
    BOOST_TEST(success.at("properties")
                   .as_object()
                   .at("result_family")
                   .as_object()
                   .at("const")
                   .as_string() ==
               McpResultFamilyName(McpOperationResultFamily(operation)));
    RequireClosedSchemaTree(output.at("oneOf"));
  }

  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(McpResultFamily::kCount); ++index) {
    const auto family = static_cast<McpResultFamily>(index);
    const boost::json::object schema = BuildMcpResultSchema(family);
    BOOST_TEST(schema.at("additionalProperties").as_bool() == false);
    BOOST_TEST(schema.at("properties")
                   .as_object()
                   .at("result_family")
                   .as_object()
                   .at("const")
                   .as_string() == McpResultFamilyName(family));
    RequireClosedSchemaTree(schema);
  }
  const boost::json::object operation_schema =
      BuildMcpResultSchema(McpResultFamily::kOperation);
  BOOST_TEST(operation_schema.at("properties")
                 .as_object()
                 .contains("terminal_result"));
  BOOST_TEST(
      operation_schema.at("properties").as_object().contains("terminal_error"));
  BOOST_TEST(operation_schema.at("allOf").as_array().size() == 9U);

  const boost::json::object mutation_schema =
      BuildMcpResultSchema(McpResultFamily::kMutation);
  const boost::json::object& mutation_properties =
      mutation_schema.at("properties").as_object();
  BOOST_TEST(mutation_properties.contains("affected_node_ids"));
  BOOST_TEST(mutation_properties.contains("action"));
  BOOST_TEST(mutation_properties.contains("state"));
  BOOST_TEST(mutation_properties.contains("command_id"));

  const std::array lifecycle_operations{
      std::pair{McpOperationKind::kStopNode, "stopped"},
      std::pair{McpOperationKind::kKillNode, "killed"},
      std::pair{McpOperationKind::kRestartNode, "running"},
  };
  const std::set<std::string> lifecycle_required{"affected_node_ids", "action",
                                                 "state", "command_id"};
  for (const auto& [operation, expected_state] : lifecycle_operations) {
    const std::string_view operation_name = McpOperationKindName(operation);
    const boost::json::object output = BuildMcpOperationOutputSchema(operation);
    const boost::json::object& direct =
        output.at("oneOf").as_array().front().as_object();
    const std::set<std::string> direct_required =
        StringSet(direct.at("required").as_array());
    BOOST_TEST(std::includes(direct_required.begin(), direct_required.end(),
                             lifecycle_required.begin(),
                             lifecycle_required.end()));
    BOOST_TEST(direct.at("properties")
                   .as_object()
                   .at("action")
                   .as_object()
                   .at("const")
                   .as_string() == operation_name);
    BOOST_TEST(direct.at("properties")
                   .as_object()
                   .at("state")
                   .as_object()
                   .at("const")
                   .as_string() == expected_state);

    const boost::json::object& nested =
        LifecycleOperationConstraint(operation_schema, operation_name,
                                     "succeeded")
            .at("then")
            .as_object()
            .at("properties")
            .as_object()
            .at("terminal_result")
            .as_object();
    BOOST_TEST(StringSet(nested.at("required").as_array()) ==
               lifecycle_required);
    BOOST_TEST(nested.at("properties")
                   .as_object()
                   .at("action")
                   .as_object()
                   .at("const")
                   .as_string() == operation_name);
    BOOST_TEST(nested.at("properties")
                   .as_object()
                   .at("state")
                   .as_object()
                   .at("const")
                   .as_string() == expected_state);

    const boost::json::object& cancelled_diagnostic =
        LifecycleOperationConstraint(operation_schema, operation_name,
                                     "cancelled")
            .at("then")
            .as_object()
            .at("properties")
            .as_object()
            .at("terminal_error")
            .as_object()
            .at("properties")
            .as_object()
            .at("diagnostics")
            .as_object()
            .at("contains")
            .as_object();
    BOOST_TEST(StringSet(cancelled_diagnostic.at("required").as_array()) ==
               std::set<std::string>({"action", "code", "command_id", "message",
                                      "node_id", "state"}));
    BOOST_TEST(cancelled_diagnostic.at("properties")
                   .as_object()
                   .at("action")
                   .as_object()
                   .at("const")
                   .as_string() == operation_name);
  }
}

}  // namespace bbp
