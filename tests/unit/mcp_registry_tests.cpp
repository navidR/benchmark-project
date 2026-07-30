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

void RequireSingleNodeBlockGenerationSchema(const boost::json::object& schema,
                                            std::string_view discriminator) {
  const boost::json::object& block_generation =
      VariantWithConst(schema.at("oneOf").as_array(), discriminator,
                       WorkloadKindName(WorkloadKind::kBlockGeneration));
  const boost::json::object& properties =
      block_generation.at("properties").as_object();
  BOOST_TEST(properties.contains("node"));
  BOOST_TEST(!properties.contains("nodes"));
  const boost::json::object& node = properties.at("node").as_object();
  BOOST_TEST(node.at("type").as_string() == "integer");
  BOOST_TEST(node.at("minimum").as_uint64() == 1U);
}

void RequireSingleNodeHeightWaitSchema(const boost::json::object& schema,
                                       std::string_view discriminator) {
  const boost::json::object& height_wait =
      VariantWithConst(schema.at("oneOf").as_array(), discriminator,
                       WorkloadKindName(WorkloadKind::kWaitUntilHeight));
  const boost::json::object& properties =
      height_wait.at("properties").as_object();
  BOOST_TEST(properties.contains("node"));
  BOOST_TEST(!properties.contains("nodes"));
  const boost::json::object& node = properties.at("node").as_object();
  BOOST_TEST(node.at("type").as_string() == "integer");
  BOOST_TEST(node.at("minimum").as_uint64() == 1U);
  const boost::json::object& timeout = properties.at("timeout_sec").as_object();
  BOOST_TEST(timeout.at("type").as_string() == "integer");
  BOOST_TEST(timeout.at("minimum").as_uint64() == 1U);
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

const boost::json::object& OperationStateSetConstraint(
    const boost::json::object& operation_schema, std::string_view operation,
    const std::set<std::string>& states) {
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
    if (operation_property != nullptr && operation_property->is_object() &&
        operation_property->as_object().if_contains("const") != nullptr &&
        operation_property->as_object().at("const").as_string() == operation &&
        state_property != nullptr && state_property->is_object()) {
      const boost::json::value* state_enum =
          state_property->as_object().if_contains("enum");
      if (state_enum != nullptr && state_enum->is_array() &&
          StringSet(state_enum->as_array()) == states) {
        return object;
      }
    }
  }
  BOOST_FAIL("missing operation state-set constraint " << operation);
  return operation_schema;
}

const boost::json::object& ArrayDiscriminatorConstraint(
    const boost::json::object& schema, std::string_view discriminator,
    std::string_view value) {
  for (const boost::json::value& constraint : schema.at("allOf").as_array()) {
    const boost::json::object& object = constraint.as_object();
    const boost::json::value* condition = object.if_contains("if");
    if (condition == nullptr) {
      continue;
    }
    const boost::json::object& condition_properties =
        condition->as_object().at("properties").as_object();
    const boost::json::value* property =
        condition_properties.if_contains(discriminator);
    if (property == nullptr || !property->is_object()) {
      continue;
    }
    const boost::json::value* constant =
        property->as_object().if_contains("const");
    if (constant != nullptr && constant->is_array() &&
        constant->as_array().size() == 1U &&
        constant->as_array().front().is_string() &&
        constant->as_array().front().as_string() == value) {
      return object;
    }
  }
  BOOST_FAIL("missing array discriminator constraint " << discriminator << " "
                                                       << value);
  return schema;
}

const boost::json::object& StringDiscriminatorConstraint(
    const boost::json::object& schema, std::string_view discriminator,
    std::string_view value, std::string_view excluded_discriminator = {}) {
  for (const boost::json::value& constraint : schema.at("allOf").as_array()) {
    const boost::json::object& object = constraint.as_object();
    const boost::json::value* condition = object.if_contains("if");
    if (condition == nullptr) {
      continue;
    }
    const boost::json::object& condition_properties =
        condition->as_object().at("properties").as_object();
    if (!excluded_discriminator.empty() &&
        condition_properties.contains(excluded_discriminator)) {
      continue;
    }
    const boost::json::value* property =
        condition_properties.if_contains(discriminator);
    if (property != nullptr && property->is_object() &&
        property->as_object().if_contains("const") != nullptr &&
        property->as_object().at("const").is_string() &&
        property->as_object().at("const").as_string() == value) {
      return object;
    }
  }
  BOOST_FAIL("missing string discriminator constraint " << discriminator << " "
                                                        << value);
  return schema;
}

const boost::json::object& WorkloadTypeConstraint(
    const boost::json::object& schema, std::string_view workload_type) {
  for (const boost::json::value& constraint : schema.at("allOf").as_array()) {
    const boost::json::object& object = constraint.as_object();
    const boost::json::value* condition = object.if_contains("if");
    if (condition == nullptr || !condition->is_object()) {
      continue;
    }
    const boost::json::value* configuration = condition->as_object()
                                                  .at("properties")
                                                  .as_object()
                                                  .if_contains("configuration");
    if (configuration == nullptr || !configuration->is_object()) {
      continue;
    }
    const boost::json::value* type = configuration->as_object()
                                         .at("properties")
                                         .as_object()
                                         .if_contains("type");
    if (type != nullptr && type->is_object()) {
      const boost::json::value* constant =
          type->as_object().if_contains("const");
      if (constant != nullptr && constant->is_string() &&
          constant->as_string() == workload_type) {
        return object;
      }
    }
  }
  BOOST_FAIL("missing workload type constraint " << workload_type);
  return schema;
}

bool ContainsRequiredField(const boost::json::value& value,
                           std::string_view field) {
  if (value.is_array()) {
    return std::any_of(value.as_array().begin(), value.as_array().end(),
                       [field](const boost::json::value& child) {
                         return ContainsRequiredField(child, field);
                       });
  }
  if (!value.is_object()) {
    return false;
  }
  const boost::json::object& object = value.as_object();
  if (const boost::json::value* required = object.if_contains("required");
      required != nullptr && required->is_array() &&
      std::any_of(required->as_array().begin(), required->as_array().end(),
                  [field](const boost::json::value& candidate) {
                    return candidate.is_string() &&
                           candidate.as_string() == field;
                  })) {
    return true;
  }
  return std::any_of(object.begin(), object.end(), [field](const auto& member) {
    return member.key() != "required" &&
           ContainsRequiredField(member.value(), field);
  });
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
#ifdef BBP_FIRO_GUI_LAUNCHER
  BOOST_TEST(operations.contains("local.firo_qt_launcher"));
#else
  std::string unavailable_operation = "local";
  unavailable_operation += '.';
  unavailable_operation += "firo_qt_launcher";
  BOOST_TEST(!operations.contains(unavailable_operation));
#endif
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
  const boost::json::object& build_features =
      document.at("build_features").as_object();
  BOOST_REQUIRE_EQUAL(build_features.size(), 1U);
#ifdef BBP_FIRO_GUI_LAUNCHER
  BOOST_TEST(build_features.at("firo_gui_launcher").as_bool());
#else
  BOOST_TEST(!build_features.at("firo_gui_launcher").as_bool());
#endif
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
  const boost::json::object& properties = schema.at("properties").as_object();
  BOOST_TEST(
      properties.at("isolated_network").as_object().at("default").as_bool());
  BOOST_TEST(properties.at("network")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("isolated")
                 .as_object()
                 .at("default")
                 .as_bool());
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

BOOST_AUTO_TEST_CASE(mcp_bandwidth_schema_uses_unsigned_decimal_kilobytes) {
  const boost::json::object condition =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkCondition);
  const boost::json::object& properties =
      condition.at("properties").as_object();
  BOOST_TEST(!properties.contains("bandwidth_mbps"));
  const boost::json::object& bandwidth =
      properties.at("bandwidth_kbps").as_object();
  BOOST_TEST(bandwidth.at("type").as_string() == "integer");
  BOOST_TEST(bandwidth.at("minimum").as_uint64() == 0U);
  BOOST_TEST(bandwidth.at("maximum").as_uint64() ==
             std::numeric_limits<std::uint32_t>::max());
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

BOOST_AUTO_TEST_CASE(
    mcp_block_generation_schemas_match_single_node_production_input) {
  RequireSingleNodeBlockGenerationSchema(BuildMcpWorkloadSchema(), "type");
  const std::vector<std::string> scenario_members =
      BuildScenarioMemberRegistry();
  const bool has_node =
      std::find(scenario_members.begin(), scenario_members.end(),
                "workload.block_generation.node") != scenario_members.end();
  const bool has_nodes =
      std::find(scenario_members.begin(), scenario_members.end(),
                "workload.block_generation.nodes") != scenario_members.end();
  BOOST_TEST(has_node);
  BOOST_TEST(!has_nodes);

  const boost::json::object scenario = BuildMcpScenarioSchema();
  RequireSingleNodeBlockGenerationSchema(scenario.at("properties")
                                             .as_object()
                                             .at("workloads")
                                             .as_object()
                                             .at("items")
                                             .as_object(),
                                         "type");
  RequireSingleNodeBlockGenerationSchema(scenario.at("properties")
                                             .as_object()
                                             .at("events")
                                             .as_object()
                                             .at("items")
                                             .as_object(),
                                         "action");

  for (const McpOperationKind operation :
       {McpOperationKind::kStartWorkload,
        McpOperationKind::kReconfigureWorkload}) {
    RequireSingleNodeBlockGenerationSchema(
        BuildMcpOperationInputSchema(operation)
            .at("properties")
            .as_object()
            .at("workload")
            .as_object(),
        "type");
  }
}

BOOST_AUTO_TEST_CASE(
    mcp_height_wait_schemas_match_single_node_production_input) {
  RequireSingleNodeHeightWaitSchema(BuildMcpWorkloadSchema(), "type");
  const std::vector<std::string> scenario_members =
      BuildScenarioMemberRegistry();
  const bool has_node =
      std::find(scenario_members.begin(), scenario_members.end(),
                "workload.wait_until_height.node") != scenario_members.end();
  const bool has_nodes =
      std::find(scenario_members.begin(), scenario_members.end(),
                "workload.wait_until_height.nodes") != scenario_members.end();
  BOOST_TEST(has_node);
  BOOST_TEST(!has_nodes);

  const boost::json::object scenario = BuildMcpScenarioSchema();
  RequireSingleNodeHeightWaitSchema(scenario.at("properties")
                                        .as_object()
                                        .at("workloads")
                                        .as_object()
                                        .at("items")
                                        .as_object(),
                                    "type");
  RequireSingleNodeHeightWaitSchema(scenario.at("properties")
                                        .as_object()
                                        .at("events")
                                        .as_object()
                                        .at("items")
                                        .as_object(),
                                    "action");

  for (const McpOperationKind operation :
       {McpOperationKind::kStartWorkload,
        McpOperationKind::kReconfigureWorkload}) {
    RequireSingleNodeHeightWaitSchema(BuildMcpOperationInputSchema(operation)
                                          .at("properties")
                                          .as_object()
                                          .at("workload")
                                          .as_object(),
                                      "type");
  }
}

BOOST_AUTO_TEST_CASE(mcp_height_wait_result_schema_matches_production_output) {
  const boost::json::object schema =
      BuildMcpResultSchema(McpResultFamily::kWorkload);
  const boost::json::object& properties = schema.at("properties").as_object();
  const boost::json::array& result_choices =
      properties.at("result").as_object().at("oneOf").as_array();
  BOOST_REQUIRE(result_choices.size() == 2U);
  const boost::json::object& result = result_choices.front().as_object();
  const std::set<std::string> result_fields{"node", "node_id",
                                            "observed_height", "target_height"};
  BOOST_TEST(PropertySet(result) == result_fields);
  BOOST_TEST(StringSet(result.at("required").as_array()) ==
             PropertySet(result));
  BOOST_TEST(result.at("additionalProperties").as_bool() == false);
  BOOST_TEST(result_choices.back().as_object().at("type").as_string() ==
             "null");

  const boost::json::object& constraint =
      WorkloadTypeConstraint(schema, "wait_until_height");
  const boost::json::object& then_schema = constraint.at("then").as_object();
  const std::set<std::string> required_fields{"failure", "result"};
  BOOST_TEST(StringSet(then_schema.at("required").as_array()) ==
             required_fields);
  const std::set<std::string> terminal_outcomes{
      "cancelled", "failed", "height_reached", "none", "stopped"};
  BOOST_TEST(StringSet(then_schema.at("properties")
                           .as_object()
                           .at("terminal_outcome")
                           .as_object()
                           .at("enum")
                           .as_array()) == terminal_outcomes);
  for (const std::string_view forbidden :
       {"accounting", "last_result", "queue_maximum_depth"}) {
    BOOST_TEST(ContainsRequiredField(then_schema.at("not"), forbidden));
  }
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

BOOST_AUTO_TEST_CASE(
    mcp_role_assign_schema_discriminates_role_specific_requirements) {
  const boost::json::object input =
      BuildMcpOperationInputSchema(McpOperationKind::kAssignRole);
  BOOST_TEST(input.at("additionalProperties").as_bool() == false);
  BOOST_TEST(PropertySet(input) ==
             std::set<std::string>({"funding_wallet_id", "mode", "node_ids",
                                    "roles", "run_id", "timeout_sec"}));
  BOOST_TEST(StringSet(ArrayField(input, "required")) ==
             std::set<std::string>({"node_ids", "roles", "run_id"}));

  const boost::json::object& input_properties =
      input.at("properties").as_object();
  const boost::json::object& node_ids =
      input_properties.at("node_ids").as_object();
  BOOST_TEST(node_ids.at("minItems").as_uint64() == 1U);
  BOOST_TEST(node_ids.at("maxItems").as_uint64() == 16U);
  BOOST_TEST(node_ids.at("uniqueItems").as_bool());
  const boost::json::object& node_id = node_ids.at("items").as_object();
  BOOST_TEST(node_id.at("type").as_string() == "string");
  BOOST_TEST(node_id.at("minLength").as_uint64() == 1U);
  BOOST_TEST(node_id.at("maxLength").as_uint64() == 32U);
  BOOST_TEST(node_id.at("pattern").as_string() == "^[A-Za-z0-9_-]{1,32}$");
  BOOST_TEST(input_properties.at("funding_wallet_id").as_object() == node_id);

  const boost::json::object& roles = input_properties.at("roles").as_object();
  BOOST_TEST(roles.at("minItems").as_uint64() == 1U);
  BOOST_TEST(roles.at("maxItems").as_uint64() == 1U);
  BOOST_TEST(roles.at("uniqueItems").as_bool());
  BOOST_TEST(StringSet(roles.at("items").as_object().at("enum").as_array()) ==
             std::set<std::string>({"masternode", "miner", "wallet"}));
  BOOST_TEST(!StringSet(roles.at("items").as_object().at("enum").as_array())
                  .contains("base"));
  BOOST_TEST(
      StringSet(
          input_properties.at("mode").as_object().at("enum").as_array()) ==
      std::set<std::string>({"private", "public"}));
  const boost::json::object& timeout =
      input_properties.at("timeout_sec").as_object();
  BOOST_TEST(timeout.at("minimum").as_uint64() == 1U);
  BOOST_TEST(timeout.at("maximum").as_uint64() == 3600U);

  const boost::json::object& wallet_constraint =
      ArrayDiscriminatorConstraint(input, "roles", "wallet");
  const boost::json::object& wallet_then =
      wallet_constraint.at("then").as_object();
  BOOST_TEST(StringSet(ArrayField(wallet_then, "required")) ==
             std::set<std::string>({"mode"}));
  BOOST_TEST(ContainsRequiredField(wallet_then.at("not"), "funding_wallet_id"));

  const boost::json::object& miner_constraint =
      ArrayDiscriminatorConstraint(input, "roles", "miner");
  const boost::json::object& miner_then =
      miner_constraint.at("then").as_object();
  BOOST_TEST(miner_then.if_contains("required") == nullptr);
  BOOST_TEST(ContainsRequiredField(miner_then.at("not"), "mode"));
  BOOST_TEST(ContainsRequiredField(miner_then.at("not"), "funding_wallet_id"));

  const boost::json::object& masternode_constraint =
      ArrayDiscriminatorConstraint(input, "roles", "masternode");
  const boost::json::object& masternode_then =
      masternode_constraint.at("then").as_object();
  BOOST_TEST(StringSet(ArrayField(masternode_then, "required")) ==
             std::set<std::string>({"funding_wallet_id"}));
  BOOST_TEST(ContainsRequiredField(masternode_then.at("not"), "mode"));

  const boost::json::object result =
      BuildMcpResultSchema(McpResultFamily::kRoleMutation);
  const boost::json::object& result_properties =
      result.at("properties").as_object();
  const std::set<std::string> result_required =
      StringSet(ArrayField(result, "required"));
  BOOST_TEST(result_required.contains("action"));
  BOOST_TEST(result_required.contains("state"));
  for (const std::string_view field :
       {"action", "state", "created_node_ids", "role_generation",
        "inventory_generation", "final_node_count", "final_wallet_count",
        "final_wallet_node_count", "wallets", "final_miner_count",
        "final_masternode_count", "masternodes"}) {
    BOOST_TEST(result_properties.contains(field));
  }

  const boost::json::object& common_constraint = StringDiscriminatorConstraint(
      result, "action", "role.assign", "assigned_roles");
  BOOST_TEST(StringSet(ArrayField(common_constraint.at("then").as_object(),
                                  "required")) ==
             std::set<std::string>({"created_node_ids", "final_node_count",
                                    "inventory_generation", "role_generation",
                                    "state"}));
  const boost::json::object& common_properties =
      common_constraint.at("then").as_object().at("properties").as_object();
  BOOST_TEST(
      common_properties.at("state").as_object().at("const").as_string() ==
      "ready");
  BOOST_TEST(common_properties.at("assigned_roles")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(common_properties.at("assigned_roles")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 1U);
  BOOST_TEST(StringSet(common_properties.at("assigned_roles")
                           .as_object()
                           .at("items")
                           .as_object()
                           .at("enum")
                           .as_array()) ==
             std::set<std::string>({"masternode", "miner", "wallet"}));
  BOOST_TEST(common_properties.at("removed_roles")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(common_properties.at("created_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);

  const std::array role_requirements{
      std::pair{"wallet",
                std::set<std::string>({"final_wallet_count",
                                       "final_wallet_node_count", "wallets"})},
      std::pair{"miner", std::set<std::string>({"final_miner_count"})},
      std::pair{"masternode", std::set<std::string>(
                                  {"final_masternode_count", "masternodes"})},
  };
  for (const auto& [role, expected_required] : role_requirements) {
    const boost::json::object& constraint =
        ArrayDiscriminatorConstraint(result, "assigned_roles", role);
    const boost::json::object& condition_properties =
        constraint.at("if").as_object().at("properties").as_object();
    BOOST_TEST(
        condition_properties.at("action").as_object().at("const").as_string() ==
        "role.assign");
    BOOST_TEST(StringSet(ArrayField(constraint.at("then").as_object(),
                                    "required")) == expected_required);
    BOOST_REQUIRE(constraint.at("then").as_object().contains("not"));
    const boost::json::value& forbidden =
        constraint.at("then").as_object().at("not");
    if (role == std::string_view("wallet")) {
      BOOST_TEST(ContainsRequiredField(forbidden, "final_miner_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "final_masternode_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "masternodes"));
    } else if (role == std::string_view("miner")) {
      BOOST_TEST(ContainsRequiredField(forbidden, "final_wallet_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "final_wallet_node_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "wallets"));
      BOOST_TEST(ContainsRequiredField(forbidden, "final_masternode_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "masternodes"));
    } else {
      BOOST_TEST(ContainsRequiredField(forbidden, "final_wallet_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "final_wallet_node_count"));
      BOOST_TEST(ContainsRequiredField(forbidden, "wallets"));
      BOOST_TEST(ContainsRequiredField(forbidden, "final_miner_count"));
    }
  }

  const boost::json::object output =
      BuildMcpOperationOutputSchema(McpOperationKind::kAssignRole);
  const boost::json::array& output_choices = output.at("oneOf").as_array();
  BOOST_REQUIRE_EQUAL(output_choices.size(), 3U);
  const boost::json::object& direct_result = output_choices.front().as_object();
  BOOST_TEST(direct_result != result);
  const boost::json::object& direct_properties =
      direct_result.at("properties").as_object();
  BOOST_TEST(
      direct_properties.at("action").as_object().at("const").as_string() ==
      "role.assign");
  BOOST_TEST(
      direct_properties.at("state").as_object().at("const").as_string() ==
      "ready");
  BOOST_TEST(direct_properties.at("assigned_roles")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(direct_properties.at("assigned_roles")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 1U);
  BOOST_TEST(direct_properties.at("removed_roles")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(direct_properties.at("created_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(direct_properties.at("wallets")
                 .as_object()
                 .at("items")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("node_id")
                 .as_object() == node_id);
  const boost::json::object& direct_masternode_properties =
      direct_properties.at("masternodes")
          .as_object()
          .at("items")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(direct_masternode_properties.at("node_id").as_object() == node_id);
  BOOST_TEST(
      direct_masternode_properties.at("funding_wallet_node_id").as_object() ==
      node_id);
  for (const auto& [role, expected_required] : role_requirements) {
    const boost::json::object& direct_constraint =
        ArrayDiscriminatorConstraint(direct_result, "assigned_roles", role);
    BOOST_TEST(StringSet(ArrayField(direct_constraint.at("then").as_object(),
                                    "required")) == expected_required);
    BOOST_TEST(direct_constraint.at("then").as_object().contains("not"));
  }

  const boost::json::object operation_result =
      BuildMcpResultSchema(McpResultFamily::kOperation);
  const boost::json::object& nested_result =
      LifecycleOperationConstraint(operation_result, "role.assign", "succeeded")
          .at("then")
          .as_object()
          .at("properties")
          .as_object()
          .at("terminal_result")
          .as_object();
  BOOST_TEST(nested_result.at("properties")
                 .as_object()
                 .at("result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "role_mutation");
  BOOST_TEST(nested_result.at("properties")
                 .as_object()
                 .at("action")
                 .as_object()
                 .at("const")
                 .as_string() == "role.assign");

  const boost::json::object& wrapper = output_choices[1U].as_object();
  BOOST_TEST(StringSet(wrapper.at("properties")
                           .as_object()
                           .at("operation")
                           .as_object()
                           .at("enum")
                           .as_array()) ==
             std::set<std::string>({"role.assign"}));
  BOOST_TEST(StringSet(wrapper.at("properties")
                           .as_object()
                           .at("terminal_result_family")
                           .as_object()
                           .at("enum")
                           .as_array()) ==
             std::set<std::string>({"error", "role_mutation"}));
  const boost::json::object& wrapper_succeeded =
      LifecycleOperationConstraint(wrapper, "role.assign", "succeeded")
          .at("then")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(wrapper_succeeded.at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "role_mutation");
  const std::set<std::string> nonterminal_states{"cancelling", "queued",
                                                 "running"};
  const boost::json::object& wrapper_nonterminal =
      OperationStateSetConstraint(wrapper, "role.assign", nonterminal_states)
          .at("then")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(wrapper_nonterminal.at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "role_mutation");
  const std::set<std::string> failed_states{"cancelled", "failed"};
  const boost::json::object& wrapper_failed =
      OperationStateSetConstraint(wrapper, "role.assign", failed_states)
          .at("then")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(wrapper_failed.at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "error");
}

BOOST_AUTO_TEST_CASE(
    mcp_role_remove_schema_is_one_bounded_mutable_role_transaction) {
  const boost::json::object input =
      BuildMcpOperationInputSchema(McpOperationKind::kRemoveRole);
  BOOST_TEST(input.at("additionalProperties").as_bool() == false);
  BOOST_TEST(
      PropertySet(input) ==
      std::set<std::string>({"node_ids", "roles", "run_id", "timeout_sec"}));
  BOOST_TEST(StringSet(ArrayField(input, "required")) ==
             std::set<std::string>({"node_ids", "roles", "run_id"}));

  const boost::json::object& input_properties =
      input.at("properties").as_object();
  const boost::json::object& node_ids =
      input_properties.at("node_ids").as_object();
  BOOST_TEST(node_ids.at("minItems").as_uint64() == 1U);
  BOOST_TEST(node_ids.at("maxItems").as_uint64() ==
             kSimulationNodeRemoveMaximumCount);
  BOOST_TEST(node_ids.at("uniqueItems").as_bool());
  BOOST_TEST(node_ids.at("items").as_object().at("pattern").as_string() ==
             "^[A-Za-z0-9_-]{1,32}$");
  const boost::json::object& roles = input_properties.at("roles").as_object();
  BOOST_TEST(roles.at("minItems").as_uint64() == 1U);
  BOOST_TEST(roles.at("maxItems").as_uint64() == 1U);
  BOOST_TEST(StringSet(roles.at("items").as_object().at("enum").as_array()) ==
             std::set<std::string>({"masternode", "miner", "wallet"}));
  BOOST_TEST(input_properties.at("timeout_sec")
                 .as_object()
                 .at("minimum")
                 .as_uint64() == 1U);
  BOOST_TEST(input_properties.at("timeout_sec")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == 3600U);

  const boost::json::object output =
      BuildMcpOperationOutputSchema(McpOperationKind::kRemoveRole);
  const boost::json::array& choices = output.at("oneOf").as_array();
  BOOST_REQUIRE_EQUAL(choices.size(), 3U);
  const boost::json::object& direct = choices.front().as_object();
  const boost::json::object& direct_properties =
      direct.at("properties").as_object();
  BOOST_TEST(
      direct_properties.at("action").as_object().at("const").as_string() ==
      "role.remove");
  BOOST_TEST(
      direct_properties.at("state").as_object().at("const").as_string() ==
      "removed");
  BOOST_TEST(direct_properties.at("assigned_roles")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(direct_properties.at("removed_roles")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(direct_properties.at("removed_roles")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 1U);
  BOOST_TEST(direct_properties.at("created_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);

  const std::array role_requirements{
      std::pair{"wallet",
                std::set<std::string>({"final_wallet_count",
                                       "final_wallet_node_count", "wallets"})},
      std::pair{"miner", std::set<std::string>({"final_miner_count"})},
      std::pair{"masternode", std::set<std::string>(
                                  {"final_masternode_count", "masternodes"})},
  };
  for (const auto& [role, expected_required] : role_requirements) {
    const boost::json::object& constraint =
        ArrayDiscriminatorConstraint(direct, "removed_roles", role);
    BOOST_TEST(StringSet(ArrayField(constraint.at("then").as_object(),
                                    "required")) == expected_required);
    BOOST_TEST(constraint.at("then").as_object().contains("not"));
  }

  const boost::json::object& wrapper = choices[1U].as_object();
  BOOST_TEST(StringSet(wrapper.at("properties")
                           .as_object()
                           .at("operation")
                           .as_object()
                           .at("enum")
                           .as_array()) ==
             std::set<std::string>({"role.remove"}));
  const boost::json::object& succeeded =
      LifecycleOperationConstraint(wrapper, "role.remove", "succeeded")
          .at("then")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(succeeded.at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "role_mutation");
  BOOST_TEST(succeeded.at("terminal_result")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("action")
                 .as_object()
                 .at("const")
                 .as_string() == "role.remove");
  const std::set<std::string> nonterminal_states{"cancelling", "queued",
                                                 "running"};
  BOOST_TEST(
      OperationStateSetConstraint(wrapper, "role.remove", nonterminal_states)
          .at("then")
          .as_object()
          .at("properties")
          .as_object()
          .at("terminal_result_family")
          .as_object()
          .at("const")
          .as_string() == "role_mutation");
  const std::set<std::string> failed_states{"cancelled", "failed"};
  BOOST_TEST(OperationStateSetConstraint(wrapper, "role.remove", failed_states)
                 .at("then")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "error");
}

BOOST_AUTO_TEST_CASE(mcp_node_add_schema_is_shared_and_matches_runtime_bounds) {
  const boost::json::object command_schema = BuildMcpSimulationCommandSchema();
  const boost::json::object& generic = VariantWithConst(
      command_schema.at("oneOf").as_array(), "kind", "add_nodes");
  const boost::json::object& generic_request =
      generic.at("properties").as_object().at("node_add").as_object();

  const boost::json::object scenario = BuildMcpScenarioSchema();
  const boost::json::array& scenario_nodes = scenario.at("properties")
                                                 .as_object()
                                                 .at("nodes")
                                                 .as_object()
                                                 .at("oneOf")
                                                 .as_array();
  BOOST_REQUIRE_EQUAL(scenario_nodes.size(), 2U);
  BOOST_TEST(scenario_nodes[0].as_object().at("minimum").as_uint64() == 0U);
  BOOST_TEST(scenario_nodes[1].as_object().at("minItems").as_uint64() == 1U);
  const boost::json::array& scheduled_variants = scenario.at("properties")
                                                     .as_object()
                                                     .at("events")
                                                     .as_object()
                                                     .at("items")
                                                     .as_object()
                                                     .at("oneOf")
                                                     .as_array();
  const boost::json::object& scheduled =
      VariantWithConst(scheduled_variants, "action", "add_nodes");
  const boost::json::object& scheduled_request =
      scheduled.at("properties").as_object().at("node_add").as_object();

  const boost::json::object direct =
      BuildMcpOperationInputSchema(McpOperationKind::kAddNode);
  const boost::json::object& direct_request =
      direct.at("properties").as_object().at("request").as_object();
  const boost::json::object wallet_add =
      BuildMcpOperationInputSchema(McpOperationKind::kAddWallet);
  const boost::json::object& wallet_create_node =
      wallet_add.at("properties").as_object().at("create_node").as_object();
  const boost::json::object wallet_remove =
      BuildMcpOperationInputSchema(McpOperationKind::kRemoveWallet);
  const boost::json::object miner_add =
      BuildMcpOperationInputSchema(McpOperationKind::kAddMiner);
  const boost::json::object& miner_create_nodes =
      miner_add.at("properties").as_object().at("create_nodes").as_object();
  const boost::json::object miner_remove =
      BuildMcpOperationInputSchema(McpOperationKind::kRemoveMiner);
  const boost::json::object& miner_remove_node_ids =
      miner_remove.at("properties").as_object().at("node_ids").as_object();
  BOOST_TEST(wallet_add.at("properties")
                 .as_object()
                 .at("count")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == kSimulationNodeAddMaximumCount);
  const boost::json::object& wallet_node_ids =
      wallet_add.at("properties").as_object().at("node_ids").as_object();
  BOOST_TEST(wallet_node_ids.at("maxItems").as_uint64() ==
             kSimulationNodeAddMaximumCount);
  BOOST_TEST(wallet_node_ids.at("uniqueItems").as_bool());
  BOOST_TEST(
      wallet_node_ids.at("items").as_object().at("maxLength").as_uint64() ==
      32U);
  BOOST_TEST(StringSet(ArrayField(wallet_remove, "required")) ==
             std::set<std::string>({"run_id"}));
  const boost::json::object& wallet_remove_properties =
      wallet_remove.at("properties").as_object();
  BOOST_TEST(wallet_remove_properties.at("node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == kSimulationNodeRemoveMaximumCount);
  BOOST_TEST(wallet_remove.at("oneOf").as_array().size() == 2U);
  BOOST_TEST(ContainsRequiredField(wallet_remove.at("oneOf"), "node_id"));
  BOOST_TEST(ContainsRequiredField(wallet_remove.at("oneOf"), "node_ids"));
  BOOST_TEST(generic_request == scheduled_request);
  BOOST_TEST(generic_request == direct_request);
  BOOST_TEST(generic_request == wallet_create_node);
  BOOST_TEST(generic_request == miner_create_nodes);
  BOOST_TEST(miner_add.at("properties")
                 .as_object()
                 .at("count")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == kSimulationNodeAddMaximumCount);
  BOOST_TEST(miner_add.at("properties")
                 .as_object()
                 .at("node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == kSimulationNodeAddMaximumCount);
  BOOST_TEST(miner_remove_node_ids.at("minItems").as_uint64() == 1U);
  BOOST_TEST(miner_remove_node_ids.at("uniqueItems").as_bool());
  BOOST_TEST(
      StringSet(ArrayField(miner_remove, "required")).contains("node_ids"));
  const boost::json::object& request_properties =
      direct_request.at("properties").as_object();
  BOOST_TEST(
      request_properties.at("count").as_object().at("maximum").as_uint64() ==
      kSimulationNodeAddMaximumCount);
  BOOST_TEST(request_properties.at("node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == kSimulationNodeAddMaximumCount);
  BOOST_TEST(request_properties.at("node_ids")
                 .as_object()
                 .at("items")
                 .as_object()
                 .at("maxLength")
                 .as_uint64() == 32U);
  const boost::json::array& topology_variants =
      request_properties.at("topology").as_object().at("oneOf").as_array();
  const boost::json::object& star =
      VariantWithConst(topology_variants, "type", "star");
  const boost::json::object& center_node =
      star.at("properties").as_object().at("center_node").as_object();
  BOOST_TEST(center_node.at("type").as_string() == "integer");
  BOOST_TEST(center_node.at("minimum").as_uint64() == 1U);
  BOOST_TEST(center_node.at("maximum").as_uint64() ==
             kSimulationNodeAddMaximumCount);
  const boost::json::object& custom =
      VariantWithConst(topology_variants, "type", "custom_edge_list");
  const boost::json::object& edge_properties = custom.at("properties")
                                                   .as_object()
                                                   .at("edges")
                                                   .as_object()
                                                   .at("items")
                                                   .as_object()
                                                   .at("properties")
                                                   .as_object();
  for (const std::string_view selector : {"from", "to"}) {
    const boost::json::object& schema =
        edge_properties.at(selector).as_object();
    BOOST_TEST(schema.at("type").as_string() == "integer");
    BOOST_TEST(schema.at("minimum").as_uint64() == 1U);
    BOOST_TEST(schema.at("maximum").as_uint64() ==
               kSimulationNodeAddMaximumCount);
  }
  const boost::json::object& internet_like =
      VariantWithConst(topology_variants, "type", "internet_like_region_graph");
  const boost::json::object& region_edge_properties =
      internet_like.at("properties")
          .as_object()
          .at("region_edges")
          .as_object()
          .at("items")
          .as_object()
          .at("properties")
          .as_object();
  for (const std::string_view selector : {"from_region", "to_region"}) {
    const boost::json::object& schema =
        region_edge_properties.at(selector).as_object();
    BOOST_TEST(schema.at("type").as_string() == "integer");
    BOOST_TEST(schema.at("minimum").as_uint64() == 1U);
    BOOST_TEST(schema.at("maximum").as_uint64() ==
               kSimulationNodeAddMaximumCount);
  }
  for (const std::string_view rejected :
       {"node_count", "wallet_node_count", "miner_node_count", "wallet_nodes",
        "miner_nodes", "wallet_initialization", "peer_connectivity"}) {
    BOOST_TEST(!star.at("properties").as_object().contains(rejected));
  }

  const boost::json::object direct_output =
      BuildMcpOperationOutputSchema(McpOperationKind::kAddNode)
          .at("oneOf")
          .as_array()
          .front()
          .as_object();
  const boost::json::object& output_properties =
      direct_output.at("properties").as_object();
  BOOST_TEST(
      output_properties.at("action").as_object().at("const").as_string() ==
      "node.add");
  BOOST_TEST(output_properties.at("added_node_ids")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(output_properties.at("removed_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(
      output_properties.at("unchanged").as_object().at("const").as_bool() ==
      false);
  BOOST_TEST(output_properties.at("inventory_generation")
                 .as_object()
                 .at("type")
                 .as_string() == "integer");
  BOOST_TEST(output_properties.at("final_node_count")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == std::numeric_limits<std::uint32_t>::max());
  const boost::json::object wallet_output =
      BuildMcpOperationOutputSchema(McpOperationKind::kAddWallet)
          .at("oneOf")
          .as_array()
          .front()
          .as_object();
  const boost::json::object& wallet_output_properties =
      wallet_output.at("properties").as_object();
  BOOST_TEST(wallet_output_properties.contains("inventory_generation"));
  BOOST_TEST(wallet_output_properties.at("final_node_count")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == std::numeric_limits<std::uint32_t>::max());
  const boost::json::object wallet_remove_schema =
      BuildMcpOperationOutputSchema(McpOperationKind::kRemoveWallet);
  const boost::json::array& wallet_remove_choices =
      wallet_remove_schema.at("oneOf").as_array();
  BOOST_REQUIRE_EQUAL(wallet_remove_choices.size(), 3U);
  const boost::json::object& wallet_remove_output =
      wallet_remove_choices.front().as_object();
  const boost::json::object& wallet_remove_output_properties =
      wallet_remove_output.at("properties").as_object();
  for (const std::string_view field :
       {"affected_node_ids", "action", "state", "wallets", "wallet_generation",
        "final_wallet_count", "final_wallet_node_count", "inventory_generation",
        "final_node_count"}) {
    BOOST_TEST(wallet_remove_output_properties.contains(field));
  }
  BOOST_TEST(wallet_remove_output_properties.at("action")
                 .as_object()
                 .at("const")
                 .as_string() == "wallet.remove");
  BOOST_TEST(wallet_remove_output_properties.at("state")
                 .as_object()
                 .at("const")
                 .as_string() == "removed");
  BOOST_TEST(wallet_remove_output_properties.at("affected_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == kSimulationNodeRemoveMaximumCount);
  BOOST_TEST(wallet_remove_output_properties.at("added_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(wallet_remove_output_properties.at("removed_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_REQUIRE(wallet_remove_output.contains("allOf"));
  const boost::json::object& wallet_remove_wrapper =
      wallet_remove_choices[1U].as_object();
  BOOST_TEST(StringSet(wallet_remove_wrapper.at("properties")
                           .as_object()
                           .at("operation")
                           .as_object()
                           .at("enum")
                           .as_array()) ==
             std::set<std::string>({"wallet.remove"}));
  const boost::json::object& wallet_remove_succeeded =
      LifecycleOperationConstraint(wallet_remove_wrapper, "wallet.remove",
                                   "succeeded")
          .at("then")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(wallet_remove_succeeded.at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "mutation");
  BOOST_TEST(wallet_remove_succeeded.at("terminal_result").as_object() ==
             wallet_remove_output);
  const std::set<std::string> wallet_nonterminal_states{"cancelling", "queued",
                                                        "running"};
  BOOST_TEST(OperationStateSetConstraint(wallet_remove_wrapper, "wallet.remove",
                                         wallet_nonterminal_states)
                 .at("then")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "mutation");
  const std::set<std::string> wallet_failed_states{"cancelled", "failed"};
  BOOST_TEST(OperationStateSetConstraint(wallet_remove_wrapper, "wallet.remove",
                                         wallet_failed_states)
                 .at("then")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "error");
  const boost::json::object miner_output =
      BuildMcpOperationOutputSchema(McpOperationKind::kAddMiner)
          .at("oneOf")
          .as_array()
          .front()
          .as_object();
  const boost::json::object& miner_output_properties =
      miner_output.at("properties").as_object();
  for (const std::string_view field :
       {"action", "state", "created_node_ids", "role_generation",
        "final_miner_count", "inventory_generation", "final_node_count"}) {
    BOOST_TEST(miner_output_properties.contains(field));
  }
  BOOST_TEST(miner_output_properties.at("final_miner_count")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == std::numeric_limits<std::uint32_t>::max());
  BOOST_TEST(miner_output_properties.at("final_node_count")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == std::numeric_limits<std::uint32_t>::max());
  BOOST_REQUIRE(miner_output.contains("allOf"));
  const boost::json::object miner_remove_output =
      BuildMcpOperationOutputSchema(McpOperationKind::kRemoveMiner)
          .at("oneOf")
          .as_array()
          .front()
          .as_object();
  const boost::json::object& miner_remove_output_properties =
      miner_remove_output.at("properties").as_object();
  for (const std::string_view field :
       {"action", "state", "created_node_ids", "role_generation",
        "final_miner_count", "inventory_generation", "final_node_count"}) {
    BOOST_TEST(miner_remove_output_properties.contains(field));
  }
  BOOST_REQUIRE(miner_remove_output.contains("allOf"));

  const boost::json::object runtime_output =
      BuildMcpResultSchema(McpResultFamily::kRuntimeCommand);
  const boost::json::object& runtime_properties =
      runtime_output.at("properties").as_object();
  for (const std::string_view field :
       {"action", "added_node_ids", "affected_node_ids", "inventory_generation",
        "final_node_count"}) {
    BOOST_TEST(runtime_properties.contains(field));
  }
  BOOST_REQUIRE(runtime_output.contains("allOf"));
}

BOOST_AUTO_TEST_CASE(
    mcp_node_replace_schema_preserves_one_identity_without_topology) {
  const boost::json::object command_schema = BuildMcpSimulationCommandSchema();
  const boost::json::object& generic = VariantWithConst(
      command_schema.at("oneOf").as_array(), "kind", "replace_node");
  const boost::json::object& generic_replacement =
      generic.at("properties").as_object().at("node_replace").as_object();
  const boost::json::object direct =
      BuildMcpOperationInputSchema(McpOperationKind::kReplaceNode);
  const boost::json::object& direct_replacement =
      direct.at("properties").as_object().at("replacement").as_object();

  BOOST_TEST(generic_replacement == direct_replacement);
  const boost::json::object& properties =
      direct_replacement.at("properties").as_object();
  BOOST_TEST(!properties.contains("topology"));
  BOOST_TEST(properties.at("count").as_object().at("minimum").as_uint64() ==
             1U);
  BOOST_TEST(properties.at("count").as_object().at("maximum").as_uint64() ==
             1U);
  BOOST_TEST(properties.at("node_ids").as_object().at("minItems").as_uint64() ==
             1U);
  BOOST_TEST(properties.at("node_ids").as_object().at("maxItems").as_uint64() ==
             1U);
  BOOST_TEST(StringSet(ArrayField(direct_replacement, "required")) ==
             std::set<std::string>({"chain", "count"}));

  const boost::json::object output =
      BuildMcpOperationOutputSchema(McpOperationKind::kReplaceNode)
          .at("oneOf")
          .as_array()
          .front()
          .as_object();
  const boost::json::object& output_properties =
      output.at("properties").as_object();
  BOOST_TEST(
      output_properties.at("action").as_object().at("const").as_string() ==
      "node.replace");
  BOOST_TEST(
      output_properties.at("state").as_object().at("const").as_string() ==
      "running");
  BOOST_TEST(output_properties.at("added_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(output_properties.at("removed_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(output_properties.at("affected_node_ids")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(output_properties.at("affected_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 1U);
}

BOOST_AUTO_TEST_CASE(
    mcp_node_remove_schema_is_shared_and_allows_empty_final_inventory) {
  const boost::json::object command_schema = BuildMcpSimulationCommandSchema();
  const boost::json::object& generic = VariantWithConst(
      command_schema.at("oneOf").as_array(), "kind", "remove_nodes");
  const boost::json::object& generic_request =
      generic.at("properties").as_object().at("node_remove").as_object();

  const boost::json::object scenario = BuildMcpScenarioSchema();
  const boost::json::array& scheduled_variants = scenario.at("properties")
                                                     .as_object()
                                                     .at("events")
                                                     .as_object()
                                                     .at("items")
                                                     .as_object()
                                                     .at("oneOf")
                                                     .as_array();
  const boost::json::object& scheduled =
      VariantWithConst(scheduled_variants, "action", "remove_nodes");
  const boost::json::object& scheduled_request =
      scheduled.at("properties").as_object().at("node_remove").as_object();
  BOOST_TEST(generic_request == scheduled_request);
  BOOST_TEST(!scheduled.at("properties").as_object().contains("node"));

  const boost::json::object direct =
      BuildMcpOperationInputSchema(McpOperationKind::kRemoveNode);
  const boost::json::object& direct_properties =
      direct.at("properties").as_object();
  const boost::json::object& request_properties =
      generic_request.at("properties").as_object();
  BOOST_TEST(direct_properties.at("node_ids") ==
             request_properties.at("node_ids"));
  BOOST_TEST(direct_properties.at("timeout_sec") ==
             request_properties.at("timeout_sec"));
  BOOST_TEST(
      direct_properties.at("node_ids").as_object().at("maxItems").as_uint64() ==
      kSimulationNodeRemoveMaximumCount);
  BOOST_TEST(
      direct_properties.at("node_ids").as_object().at("uniqueItems").as_bool());
  BOOST_TEST(direct_properties.at("timeout_sec")
                 .as_object()
                 .at("maximum")
                 .as_uint64() == kSimulationNodeAddMaximumTimeoutSeconds);

  const boost::json::object direct_output =
      BuildMcpOperationOutputSchema(McpOperationKind::kRemoveNode)
          .at("oneOf")
          .as_array()
          .front()
          .as_object();
  const boost::json::object& output_properties =
      direct_output.at("properties").as_object();
  BOOST_TEST(
      output_properties.at("action").as_object().at("const").as_string() ==
      "node.remove");
  BOOST_TEST(output_properties.at("added_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(output_properties.at("removed_node_ids")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(output_properties.at("final_node_count")
                 .as_object()
                 .at("minimum")
                 .as_uint64() == 0U);

  const boost::json::object runtime_output =
      BuildMcpResultSchema(McpResultFamily::kRuntimeCommand);
  BOOST_TEST(
      runtime_output.at("properties").as_object().contains("removed_node_ids"));
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
  BOOST_REQUIRE(operation_schema.contains("allOf"));

  const boost::json::object mutation_schema =
      BuildMcpResultSchema(McpResultFamily::kMutation);
  const boost::json::object& mutation_properties =
      mutation_schema.at("properties").as_object();
  BOOST_TEST(mutation_properties.contains("affected_node_ids"));
  BOOST_TEST(mutation_properties.contains("action"));
  BOOST_TEST(mutation_properties.contains("state"));
  BOOST_TEST(mutation_properties.contains("command_id"));
  BOOST_TEST(mutation_properties.contains("wallets"));
  BOOST_TEST(mutation_properties.contains("wallet_generation"));
  BOOST_TEST(mutation_properties.contains("final_wallet_count"));
  BOOST_TEST(mutation_properties.contains("final_wallet_node_count"));
  const boost::json::object role_mutation_schema =
      BuildMcpResultSchema(McpResultFamily::kRoleMutation);
  const boost::json::object& role_mutation_properties =
      role_mutation_schema.at("properties").as_object();
  for (const std::string_view field :
       {"action", "state", "created_node_ids", "role_generation",
        "final_miner_count", "final_masternode_count", "masternodes",
        "inventory_generation", "final_node_count"}) {
    BOOST_TEST(role_mutation_properties.contains(field));
  }
  const boost::json::object& masternode_identity =
      role_mutation_properties.at("masternodes")
          .as_object()
          .at("items")
          .as_object();
  BOOST_TEST(masternode_identity.at("additionalProperties").as_bool() == false);
  BOOST_TEST(!masternode_identity.at("properties")
                  .as_object()
                  .contains("operator_secret_key"));

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

#ifdef BBP_FIRO_GUI_LAUNCHER
BOOST_AUTO_TEST_CASE(mcp_firo_qt_launcher_schema_is_closed_and_wrapper_exact) {
  const boost::json::object input =
      BuildMcpOperationInputSchema(McpOperationKind::kCreateFiroQtLauncher);
  const std::set<std::string> input_fields{"node_id", "run_id"};
  BOOST_TEST(PropertySet(input) == input_fields);
  BOOST_TEST(StringSet(input.at("required").as_array()) == input_fields);
  BOOST_TEST(!input.at("additionalProperties").as_bool());
  const boost::json::object& node_id =
      input.at("properties").as_object().at("node_id").as_object();
  BOOST_TEST(node_id.at("maxLength").as_uint64() == 32U);
  BOOST_TEST(node_id.at("pattern").as_string() == "^[A-Za-z0-9_-]{1,32}$");

  const boost::json::object output =
      BuildMcpOperationOutputSchema(McpOperationKind::kCreateFiroQtLauncher);
  const boost::json::array& choices = output.at("oneOf").as_array();
  BOOST_REQUIRE_EQUAL(choices.size(), 3U);
  const boost::json::object& direct = choices.front().as_object();
  const std::set<std::string> direct_fields{
      "action",        "added_node_ids",   "affected_node_ids",
      "launcher_path", "operator_command", "removed_node_ids",
      "result_family", "run_id",           "state",
      "unchanged",
  };
  BOOST_TEST(PropertySet(direct) == direct_fields);
  BOOST_TEST(StringSet(direct.at("required").as_array()) == direct_fields);
  BOOST_TEST(!direct.at("additionalProperties").as_bool());
  const boost::json::object& properties = direct.at("properties").as_object();
  BOOST_TEST(
      properties.at("result_family").as_object().at("const").as_string() ==
      "mutation");
  BOOST_TEST(properties.at("action").as_object().at("const").as_string() ==
             "local.firo_qt_launcher");
  BOOST_TEST(properties.at("state").as_object().at("const").as_string() ==
             "ready");
  BOOST_TEST(!properties.at("unchanged").as_object().at("const").as_bool());
  BOOST_TEST(
      properties.at("added_node_ids").as_object().at("maxItems").as_uint64() ==
      0U);
  BOOST_TEST(properties.at("removed_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 0U);
  BOOST_TEST(properties.at("affected_node_ids")
                 .as_object()
                 .at("minItems")
                 .as_uint64() == 1U);
  BOOST_TEST(properties.at("affected_node_ids")
                 .as_object()
                 .at("maxItems")
                 .as_uint64() == 1U);

  const boost::json::object& wrapper = choices[1U].as_object();
  BOOST_TEST(StringSet(wrapper.at("properties")
                           .as_object()
                           .at("operation")
                           .as_object()
                           .at("enum")
                           .as_array()) ==
             std::set<std::string>({"local.firo_qt_launcher"}));
  const boost::json::object& succeeded =
      LifecycleOperationConstraint(wrapper, "local.firo_qt_launcher",
                                   "succeeded")
          .at("then")
          .as_object()
          .at("properties")
          .as_object();
  BOOST_TEST(succeeded.at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "mutation");
  BOOST_TEST(succeeded.at("terminal_result").as_object() == direct);
  BOOST_TEST(OperationStateSetConstraint(
                 wrapper, "local.firo_qt_launcher",
                 std::set<std::string>({"cancelling", "queued", "running"}))
                 .at("then")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "mutation");
  BOOST_TEST(OperationStateSetConstraint(
                 wrapper, "local.firo_qt_launcher",
                 std::set<std::string>({"cancelled", "failed"}))
                 .at("then")
                 .as_object()
                 .at("properties")
                 .as_object()
                 .at("terminal_result_family")
                 .as_object()
                 .at("const")
                 .as_string() == "error");
}
#endif

}  // namespace bbp
