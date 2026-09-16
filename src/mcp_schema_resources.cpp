#include <array>
#include <string>

#include "bbp/mcp_registry.h"

namespace bbp {
namespace {

constexpr std::string_view kSchemaPrefix = "bbp:///schemas/";

struct SchemaResource {
  std::string_view name;
  std::string_view description;
  boost::json::object (*build)();
};

constexpr std::array kSchemas{
    SchemaResource{"scenario", "Complete source scenario JSON Schema",
                   BuildMcpScenarioSchema},
    SchemaResource{"resolved_scenario",
                   "Canonical resolved scenario JSON Schema",
                   BuildMcpResolvedScenarioSchema},
    SchemaResource{"workload", "Registered scenario workload JSON Schema",
                   BuildMcpWorkloadSchema},
    SchemaResource{"simulation_command", "Runtime command JSON Schema",
                   BuildMcpSimulationCommandSchema},
};

boost::json::object Resource(std::string_view name,
                             std::string_view description) {
  return boost::json::object{
      {"uri", std::string(kSchemaPrefix) + std::string(name)},
      {"name", "schemas/" + std::string(name)},
      {"description", description},
      {"mimeType", "application/schema+json"}};
}

}  // namespace

boost::json::array BuildMcpSchemaResourceRegistry(
    std::span<const McpOperationKind> operations) {
  boost::json::array resources;
  for (const SchemaResource& schema : kSchemas) {
    resources.emplace_back(Resource(schema.name, schema.description));
  }
  const auto registry = McpResultFamilyRegistry();
  for (const McpResultFamily family : McpOperationResultFamilies(operations)) {
    const auto& descriptor = registry[static_cast<std::size_t>(family)];
    resources.emplace_back(Resource("results/" + std::string(descriptor.name),
                                    descriptor.description));
  }
  return resources;
}

std::optional<boost::json::object> ReadMcpSchemaResource(
    std::string_view uri, std::span<const McpOperationKind> operations) {
  if (!uri.starts_with(kSchemaPrefix)) {
    return std::nullopt;
  }
  const std::string_view name = uri.substr(kSchemaPrefix.size());
  for (const SchemaResource& schema : kSchemas) {
    if (name == schema.name) {
      return schema.build();
    }
  }
  for (const McpResultFamily family : McpOperationResultFamilies(operations)) {
    if (name == "results/" + std::string(McpResultFamilyName(family))) {
      return BuildMcpResultSchema(family, operations);
    }
  }
  return std::nullopt;
}

boost::json::object BuildMcpSchemaDocument(
    std::span<const McpOperationKind> operations,
    std::span<const McpInformationFamily> information_families) {
  boost::json::object operation_schemas;
  boost::json::array tools =
      BuildMcpToolRegistry(operations, information_families);
  for (const auto& value : tools) {
    const auto& tool = value.as_object();
    operation_schemas[tool.at("name").as_string()] = boost::json::object{
        {"input", tool.at("inputSchema")}, {"output", tool.at("outputSchema")}};
  }
  boost::json::object result_schemas;
  boost::json::array results;
  const auto registry = McpResultFamilyRegistry();
  for (const McpResultFamily family : McpOperationResultFamilies(operations)) {
    const auto& descriptor = registry[static_cast<std::size_t>(family)];
    result_schemas[descriptor.name] = BuildMcpResultSchema(family, operations);
    results.emplace_back(boost::json::object{
        {"name", descriptor.name}, {"description", descriptor.description}});
  }
  boost::json::object document{
      {"operations", std::move(operation_schemas)},
      {"tools", std::move(tools)},
      {"resources", BuildMcpResourceRegistry(operations, information_families)},
      {"results", std::move(results)},
      {"result_schemas", std::move(result_schemas)},
      {"notifications", BuildMcpNotificationDiscovery(operations)},
  };
  for (const SchemaResource& schema : kSchemas) {
    document[schema.name] = schema.build();
  }
  document["error_contract"] = boost::json::object{
      {"schema_uri", "bbp:///schemas/results/error"},
      {"tool_failure",
       "tools/call returns isError=true with the structured error in "
       "structuredContent and JSON text content."},
      {"operation_failure",
       "Accepted operations return their identity immediately. Poll "
       "operation.get; a failed or cancelled operation retains "
       "terminal_error."},
      {"protocol_failure",
       "Invalid JSON-RPC or MCP requests return a JSON-RPC error instead of a "
       "tool result. Unknown or unavailable resources use MCP resource error "
       "-32002; internal resource read failures use -32603."},
  };
  return document;
}

}  // namespace bbp
