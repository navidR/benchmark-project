#include "bbp/mcp_dispatcher.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/string.hpp>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/mcp_registry.h"
#include "bbp/scenario_service.h"

namespace bbp {
namespace {

McpOperationKind RequireOperationKind(std::string_view name) {
  const std::span<const McpNamedCapability> operations = McpOperationRegistry();
  const auto found = std::find_if(operations.begin(), operations.end(),
                                  [name](const McpNamedCapability& operation) {
                                    return operation.name == name;
                                  });
  if (found == operations.end()) {
    throw std::invalid_argument("unknown BBP MCP operation");
  }
  return static_cast<McpOperationKind>(
      static_cast<std::size_t>(found - operations.begin()));
}

McpInformationFamily RequireInformationFamily(std::string_view name) {
  const std::span<const McpNamedCapability> families =
      McpInformationFamilyRegistry();
  const auto found = std::find_if(
      families.begin(), families.end(),
      [name](const McpNamedCapability& family) { return family.name == name; });
  if (found == families.end()) {
    throw std::invalid_argument("unknown BBP MCP information family");
  }
  return static_cast<McpInformationFamily>(
      static_cast<std::size_t>(found - families.begin()));
}

const boost::json::value& RequireMember(const boost::json::object& object,
                                        std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    throw std::invalid_argument("missing MCP tool argument: " +
                                std::string(name));
  }
  return *value;
}

std::string RequireString(const boost::json::object& object,
                          std::string_view name) {
  const boost::json::value& value = RequireMember(object, name);
  if (!value.is_string() || value.as_string().empty()) {
    throw std::invalid_argument("MCP tool argument " + std::string(name) +
                                " must be a non-empty string");
  }
  return std::string(value.as_string());
}

const boost::json::object& RequireObject(const boost::json::object& object,
                                         std::string_view name) {
  const boost::json::value& value = RequireMember(object, name);
  if (!value.is_object()) {
    throw std::invalid_argument("MCP tool argument " + std::string(name) +
                                " must be an object");
  }
  return value.as_object();
}

std::uint64_t OptionalUnsigned(const boost::json::object& object,
                               std::string_view name, std::uint64_t fallback) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return fallback;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  throw std::invalid_argument("MCP tool argument " + std::string(name) +
                              " must be an unsigned integer");
}

std::uint64_t OptionalCursor(const boost::json::object& object,
                             std::string_view name, std::uint64_t fallback) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return fallback;
  }
  if (!value->is_string() || value->as_string().empty()) {
    throw std::invalid_argument("MCP tool argument " + std::string(name) +
                                " must be a decimal cursor string");
  }
  const std::string_view text(value->as_string().data(),
                              value->as_string().size());
  std::uint64_t cursor = 0U;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), cursor, 10);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument("MCP tool argument " + std::string(name) +
                                " must be a decimal cursor string");
  }
  return cursor;
}

void ValidateTopLevelArguments(McpOperationKind kind,
                               const boost::json::object& arguments) {
  const boost::json::object schema = BuildMcpOperationInputSchema(kind);
  const boost::json::object& properties = schema.at("properties").as_object();
  for (const auto& member : arguments) {
    if (properties.if_contains(member.key()) == nullptr) {
      throw std::invalid_argument("unsupported MCP tool argument for " +
                                  std::string(McpOperationKindName(kind)) +
                                  ": " + std::string(member.key()));
    }
  }
  const boost::json::array& required = schema.at("required").as_array();
  for (const boost::json::value& value : required) {
    const std::string_view name(value.as_string().data(),
                                value.as_string().size());
    static_cast<void>(RequireMember(arguments, name));
  }
}

std::vector<std::string> OptionalStringArray(const boost::json::object& object,
                                             std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return {};
  }
  if (!value->is_array()) {
    throw std::invalid_argument("MCP tool argument " + std::string(name) +
                                " must be an array");
  }
  std::vector<std::string> result;
  result.reserve(value->as_array().size());
  for (const boost::json::value& item : value->as_array()) {
    if (!item.is_string() || item.as_string().empty()) {
      throw std::invalid_argument("MCP tool argument " + std::string(name) +
                                  " must contain non-empty strings");
    }
    result.emplace_back(item.as_string());
  }
  return result;
}

std::vector<McpInformationFamily> RequireInformationFamilies(
    const boost::json::object& object) {
  const boost::json::value& value = RequireMember(object, "families");
  if (!value.is_array() || value.as_array().empty()) {
    throw std::invalid_argument(
        "MCP tool argument families must be a non-empty array");
  }
  std::vector<McpInformationFamily> result;
  result.reserve(value.as_array().size());
  for (const boost::json::value& item : value.as_array()) {
    if (!item.is_string()) {
      throw std::invalid_argument(
          "MCP tool argument families must contain strings");
    }
    result.push_back(RequireInformationFamily(
        std::string_view(item.as_string().data(), item.as_string().size())));
  }
  return result;
}

McpTypedResult ValidationResult(boost::json::object scenario) {
  boost::json::array diagnostics;
  bool valid = true;
  try {
    static_cast<void>(ParseAndValidateScenario(scenario));
  } catch (const std::exception& error) {
    valid = false;
    diagnostics.emplace_back(boost::json::object{{"code", "invalid_scenario"},
                                                 {"message", error.what()}});
  }
  return McpTypedResult{
      .family = McpResultFamily::kValidation,
      .value = boost::json::object{{"result_family", "validation"},
                                   {"valid", valid},
                                   {"diagnostics", std::move(diagnostics)}}};
}

McpOperationPlan BuiltInPlan(McpOperationKind kind,
                             const boost::json::object& arguments) {
  if (kind == McpOperationKind::kValidateScenario) {
    boost::json::object scenario = RequireObject(arguments, "scenario");
    return McpOperationPlan{.progress_total = 1U,
                            .executor = [scenario = std::move(scenario)](
                                            McpOperationContext& context) {
                              context.ThrowIfCancelled();
                              McpTypedResult result =
                                  ValidationResult(std::move(scenario));
                              context.ThrowIfCancelled();
                              return result;
                            }};
  }
  if (kind == McpOperationKind::kResolveScenario) {
    boost::json::object scenario = RequireObject(arguments, "scenario");
    return McpOperationPlan{
        .progress_total = 1U,
        .executor = [scenario =
                         std::move(scenario)](McpOperationContext& context) {
          context.ThrowIfCancelled();
          try {
            boost::json::object resolved = ResolveScenario(scenario);
            context.ThrowIfCancelled();
            return McpTypedResult{.family = McpResultFamily::kScenario,
                                  .value = boost::json::object{
                                      {"result_family", "scenario"},
                                      {"scenario", std::move(resolved)}}};
          } catch (const McpOperationCancelled&) {
            throw;
          } catch (const std::exception& error) {
            throw McpOperationFailure("invalid_scenario", error.what(), false);
          }
        }};
  }
  return {};
}

}  // namespace

McpDispatcher::McpDispatcher(McpDispatcherConfig config,
                             McpApplicationOperationFactory operation_factory,
                             McpApplicationResourceReader resource_reader)
    : config_(std::move(config)),
      operation_factory_(std::move(operation_factory)),
      resource_reader_(std::move(resource_reader)),
      operations_(config_.operations,
                  [this](const McpServiceNotification& notification) {
                    DeliverNotification(notification);
                  }) {
  if (config_.session_removal_timeout < std::chrono::milliseconds::zero()) {
    throw std::invalid_argument(
        "MCP dispatcher session removal timeout cannot be negative");
  }
}

McpDispatcher::~McpDispatcher() { Shutdown(); }

McpToolHandler McpDispatcher::ToolHandler() {
  return [this](std::string_view name, const boost::json::object& arguments,
                std::string_view session_id, std::stop_token stop_token) {
    return InvokeTool(name, arguments, session_id, stop_token);
  };
}

McpResourceHandler McpDispatcher::ResourceHandler() {
  return [this](std::string_view uri, std::string_view session_id,
                std::stop_token stop_token) {
    return ReadResource(uri, session_id, stop_token);
  };
}

McpSessionHandler McpDispatcher::SessionHandler() {
  return [this](std::string_view session_id, bool opened,
                std::stop_token stop_token) {
    ChangeSession(session_id, opened, stop_token);
  };
}

void McpDispatcher::SetNotificationHandler(
    McpServiceNotificationHandler handler) {
  std::lock_guard<std::mutex> lock(notification_mutex_);
  notification_handler_ = std::move(handler);
}

void McpDispatcher::Publish(McpEvidenceRecord record) {
  operations_.Publish(std::move(record));
}

McpOperationServiceStats McpDispatcher::Stats() const {
  return operations_.Stats();
}

void McpDispatcher::Shutdown() { operations_.Shutdown(); }

boost::json::value McpDispatcher::InvokeTool(
    std::string_view name, const boost::json::object& arguments,
    std::string_view session_id, std::stop_token stop_token) {
  return operations_.ExecuteSessionRequest(
      session_id,
      [&](std::stop_token request_stop_token) {
        return InvokeToolInSession(name, arguments, session_id,
                                   request_stop_token);
      },
      stop_token);
}

boost::json::value McpDispatcher::InvokeToolInSession(
    std::string_view name, const boost::json::object& arguments,
    std::string_view session_id, std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("MCP request was cancelled before dispatch");
  }
  const McpOperationKind kind = RequireOperationKind(name);
  ValidateTopLevelArguments(kind, arguments);

  if (kind == McpOperationKind::kGetOperation) {
    return McpOperationSnapshotJson(operations_.GetOperation(
        session_id, RequireString(arguments, "operation_id")));
  }
  if (kind == McpOperationKind::kCancelOperation) {
    return McpOperationSnapshotJson(
        operations_
            .CancelOperation(session_id,
                             RequireString(arguments, "operation_id"))
            .operation);
  }
  if (kind == McpOperationKind::kCreateSubscription) {
    static_cast<void>(RequireString(arguments, "run_id"));
    McpSubscriptionRequest request{
        .families = RequireInformationFamilies(arguments),
        .node_ids = OptionalStringArray(arguments, "node_ids"),
        .cursor = OptionalCursor(arguments, "cursor", 0U)};
    return McpSubscriptionSnapshotJson(
        operations_.CreateSubscription(session_id, std::move(request)));
  }
  if (kind == McpOperationKind::kPollSubscription) {
    const std::uint64_t limit = OptionalUnsigned(
        arguments, "limit", config_.operations.maximum_poll_items);
    if (limit == 0U || limit > std::numeric_limits<std::size_t>::max()) {
      throw std::invalid_argument("MCP subscription poll limit is invalid");
    }
    const std::uint64_t timeout_sec =
        OptionalUnsigned(arguments, "timeout_sec", 0U);
    if (timeout_sec > 60U) {
      throw std::invalid_argument(
          "MCP subscription poll timeout exceeds 60 seconds");
    }
    return McpSubscriptionSnapshotJson(operations_.PollSubscription(
        session_id, RequireString(arguments, "subscription_id"),
        OptionalCursor(arguments, "cursor", 0U),
        static_cast<std::size_t>(limit), std::chrono::seconds(timeout_sec),
        stop_token));
  }
  if (kind == McpOperationKind::kCancelSubscription) {
    return McpSubscriptionSnapshotJson(operations_.CancelSubscription(
        session_id, RequireString(arguments, "subscription_id")));
  }

  McpOperationPlan plan = BuiltInPlan(kind, arguments);
  if (!plan.executor && operation_factory_) {
    plan = operation_factory_(kind, arguments, session_id);
  }
  if (!plan.executor) {
    throw std::runtime_error(
        "MCP operation is not available in the current "
        "BBP application state: " +
        std::string(name));
  }
  if (plan.progress_total == 0U) {
    throw std::logic_error(
        "MCP application operation plan has zero progress total");
  }
  return McpOperationSnapshotJson(operations_.Submit(
      session_id, kind, plan.progress_total, std::move(plan.executor)));
}

boost::json::value McpDispatcher::ReadResource(std::string_view uri,
                                               std::string_view session_id,
                                               std::stop_token stop_token) {
  return operations_.ExecuteSessionRequest(
      session_id,
      [&](std::stop_token request_stop_token) {
        return ReadResourceInSession(uri, session_id, request_stop_token);
      },
      stop_token);
}

boost::json::value McpDispatcher::ReadResourceInSession(
    std::string_view uri, std::string_view session_id,
    std::stop_token stop_token) {
  constexpr std::string_view kPrefix = "bbp:///";
  if (!uri.starts_with(kPrefix)) {
    throw std::invalid_argument("invalid BBP MCP resource URI");
  }
  const McpInformationFamily family =
      RequireInformationFamily(uri.substr(kPrefix.size()));
  if (!resource_reader_) {
    throw std::runtime_error(
        "MCP resource is not available in the current BBP application state");
  }
  return resource_reader_(family, session_id, stop_token);
}

void McpDispatcher::ChangeSession(std::string_view session_id, bool opened,
                                  std::stop_token stop_token) {
  if (opened) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    operations_.RegisterSession(std::string(session_id));
    return;
  }
  const std::chrono::milliseconds timeout =
      stop_token.stop_requested() ? std::chrono::milliseconds::zero()
                                  : config_.session_removal_timeout;
  if (!operations_.RemoveSession(session_id, timeout)) {
    throw std::runtime_error(
        "MCP session still owns an operation after its removal deadline");
  }
}

void McpDispatcher::DeliverNotification(
    const McpServiceNotification& notification) {
  McpServiceNotificationHandler handler;
  {
    std::lock_guard<std::mutex> lock(notification_mutex_);
    handler = notification_handler_;
  }
  if (handler) {
    handler(notification);
  }
}

}  // namespace bbp
