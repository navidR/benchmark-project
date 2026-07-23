#pragma once

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <stop_token>
#include <string_view>

#include "bbp/mcp_operation_service.h"
#include "bbp/mcp_protocol.h"

namespace bbp {

struct McpOperationPlan {
  std::uint64_t progress_total = 1U;
  McpOperationExecutor executor;
};

using McpApplicationOperationFactory = std::function<McpOperationPlan(
    McpOperationKind, const boost::json::object&, std::string_view)>;
using McpApplicationResourceReader = std::function<boost::json::value(
    McpInformationFamily, std::string_view, std::stop_token)>;

struct McpDispatcherConfig {
  McpOperationServiceConfig operations;
  std::chrono::milliseconds session_removal_timeout = std::chrono::seconds(5);
};

// Typed bridge between Streamable HTTP and authoritative BBP services. It
// never invokes the CLI, synthesizes terminal input, or parses rendered text.
class McpDispatcher {
 public:
  explicit McpDispatcher(McpDispatcherConfig config = {},
                         McpApplicationOperationFactory operation_factory = {},
                         McpApplicationResourceReader resource_reader = {});
  ~McpDispatcher();

  McpDispatcher(const McpDispatcher&) = delete;
  McpDispatcher& operator=(const McpDispatcher&) = delete;

  McpToolHandler ToolHandler();
  McpResourceHandler ResourceHandler();
  McpSessionHandler SessionHandler();

  void SetNotificationHandler(McpServiceNotificationHandler handler);
  void Publish(McpEvidenceRecord record);
  McpOperationServiceStats Stats() const;
  void Shutdown();

 private:
  boost::json::value InvokeTool(std::string_view name,
                                const boost::json::object& arguments,
                                std::string_view session_id,
                                std::stop_token stop_token);
  boost::json::value InvokeToolInSession(
      std::string_view name, const boost::json::object& arguments,
      std::string_view session_id, std::stop_token stop_token);
  boost::json::value ReadResource(std::string_view uri,
                                  std::string_view session_id,
                                  std::stop_token stop_token);
  boost::json::value ReadResourceInSession(std::string_view uri,
                                           std::string_view session_id,
                                           std::stop_token stop_token);
  void ChangeSession(std::string_view session_id, bool opened,
                     std::stop_token stop_token);
  void DeliverNotification(const McpServiceNotification& notification);

  McpDispatcherConfig config_;
  McpApplicationOperationFactory operation_factory_;
  McpApplicationResourceReader resource_reader_;
  mutable std::mutex notification_mutex_;
  McpServiceNotificationHandler notification_handler_;
  McpOperationService operations_;
};

}  // namespace bbp
