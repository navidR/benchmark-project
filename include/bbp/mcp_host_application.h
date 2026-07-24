#pragma once

#include <boost/json/object.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_dispatcher.h"

namespace bbp {

class McpLiveApplication;

struct McpHostedRunSnapshot {
  std::uint64_t generation = 0U;
  std::string run_id;
  std::string state;
  std::string chain;
  std::uint32_t node_count = 0U;
  std::uint32_t node_capacity = 0U;
  std::uint32_t chain_node_maximum = 0U;
  std::uint32_t available_node_capacity = 0U;
  std::shared_ptr<McpLiveApplication> application;
};

struct McpRunLifecycleResult {
  std::string run_id;
  std::string state;
  std::uint32_t node_count = 0U;
};

// Process-level MCP application. The callbacks reserve controller-owned work
// before waiting; their stop token cancels only that wait, never the run.
class McpHostApplication {
 public:
  struct Config {
    std::string host_id;
    std::function<std::optional<McpHostedRunSnapshot>()> snapshot_run;
    std::function<McpRunLifecycleResult(const boost::json::object&,
                                        std::stop_token)>
        launch_run;
    std::function<McpRunLifecycleResult(std::string_view, std::chrono::seconds,
                                        std::stop_token)>
        stop_run;
  };

  explicit McpHostApplication(Config config);
  ~McpHostApplication();

  McpHostApplication(const McpHostApplication&) = delete;
  McpHostApplication& operator=(const McpHostApplication&) = delete;

  McpApplicationOperationFactory OperationFactory();
  McpApplicationResourceReader ResourceReader();
  std::vector<McpOperationKind> SupportedOperations() const;
  std::vector<McpInformationFamily> SupportedInformationFamilies() const;
  void Shutdown();

 private:
  McpOperationPlan BuildOperation(McpOperationKind kind,
                                  const boost::json::object& arguments,
                                  std::string_view session_id);
  boost::json::value ReadResource(McpInformationFamily family,
                                  std::string_view session_id,
                                  std::stop_token stop_token);
  void RequireRunning() const;

  Config config_;
  mutable std::mutex mutex_;
  bool shutdown_ = false;
};

}  // namespace bbp
