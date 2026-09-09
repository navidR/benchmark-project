#pragma once

#include <boost/json/object.hpp>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>

namespace bbp {

class McpLiveApplication;
class RuntimeNodeInventory;
class RuntimeWalletRegistry;
struct Options;

namespace simulator_app_internal {

struct LiveWalletWorkloadRegistry;

struct LiveWalletRemovalContext {
  const Options& options;
  const std::filesystem::path& events_path;
  McpLiveApplication& mcp_application;
  const RuntimeNodeInventory& node_inventory;
  RuntimeWalletRegistry& runtime_wallet_registry;
  const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads;
  std::timed_mutex& node_mutation_mutex;
  std::function<bool()> request_simulation_stop;
  std::unique_lock<std::timed_mutex> (*acquire_node_mutation_lock)(
      std::timed_mutex&, std::stop_token);
  std::unique_lock<std::timed_mutex> (*acquire_runtime_publication_lock)(
      std::stop_token);
  std::string (*exception_message)(const std::exception_ptr&);
};

boost::json::object RemoveLiveWalletRoles(
    const LiveWalletRemovalContext& context,
    const boost::json::object& arguments, std::stop_token operation_stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
