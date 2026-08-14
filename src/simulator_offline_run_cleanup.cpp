#include "simulator_offline_run_cleanup.h"

#include <linux/capability.h>

#include <algorithm>
#include <boost/json/parse.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/capability.h"
#include "bbp/cgroup.h"
#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/logging.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/network.h"
#include "bbp/network_allocation_lock.h"
#include "bbp/run_ownership.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_json_field_decoding.h"
#include "simulator_resource_profile_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

struct StopForwarder {
  std::stop_source* target = nullptr;

  void operator()() const noexcept { target->request_stop(); }
};

class CombinedStopToken {
 public:
  CombinedStopToken(std::stop_token first, std::stop_token second)
      : first_callback_(first, StopForwarder{&source_}),
        second_callback_(second, StopForwarder{&source_}) {}

  [[nodiscard]] std::stop_token get_token() const noexcept {
    return source_.get_token();
  }

 private:
  std::stop_source source_;
  std::stop_callback<StopForwarder> first_callback_;
  std::stop_callback<StopForwarder> second_callback_;
};

const RunOwnership& RequireRunOwnership(const Options& options) {
  if (!options.run_ownership) {
    throw std::logic_error("run ownership is not initialized");
  }
  return *options.run_ownership;
}

void LoadCleanupMetadata(const std::filesystem::path& run_root,
                         Options* options, std::stop_token stop_token) {
  constexpr std::size_t kMaximumResolvedScenarioBytes = 4U * 1024U * 1024U;
  const std::filesystem::path resolved_path =
      run_root / "resolved-scenario.json";
  if (!std::filesystem::exists(resolved_path)) {
    return;
  }

  const boost::json::value value = boost::json::parse(
      ReadText(resolved_path, kMaximumResolvedScenarioBytes, stop_token));
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             resolved_path.string());
  }
  const boost::json::object& object = value.as_object();
  options->chain = ParseChainKind(JsonOptionalStringField(
      object, "chain", std::string(ChainKindName(options->chain))));
  options->nodes = JsonOptionalUint32Field(object, "nodes", options->nodes);
  const boost::json::value* isolated = object.if_contains("isolated_network");
  if (isolated != nullptr) {
    if (!isolated->is_bool()) {
      throw std::runtime_error(
          "resolved scenario isolated_network is not a boolean");
    }
    options->isolate_network = isolated->as_bool();
  }
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options->chain);
  if (options->nodes > chain_spec.max_nodes) {
    throw std::runtime_error(
        "cleanup currently supports resolved node counts in 0.." +
        std::to_string(chain_spec.max_nodes));
  }
  const boost::json::value* node_configs = object.if_contains("node_configs");
  if (node_configs != nullptr) {
    if (!node_configs->is_array() ||
        node_configs->as_array().size() != options->nodes) {
      throw std::runtime_error(
          "resolved scenario node_configs must match the node count");
    }
    options->node_ids.clear();
    options->node_ids.reserve(options->nodes);
    std::set<std::string> unique_ids;
    for (const boost::json::value& node_value : node_configs->as_array()) {
      if (!node_value.is_object()) {
        throw std::runtime_error(
            "resolved scenario node config is not an object");
      }
      const std::string id =
          JsonOptionalStringField(node_value.as_object(), "id", "");
      RequireSafeScenarioIdentifier(id, "resolved scenario node id");
      if (!unique_ids.insert(id).second) {
        throw std::runtime_error(
            "resolved scenario contains duplicate node ids");
      }
      options->node_ids.push_back(id);
    }
  }
}

void RequireCleanupActive(
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
  if (absolute_deadline &&
      std::chrono::steady_clock::now() >= *absolute_deadline) {
    throw std::runtime_error("run cleanup deadline expired");
  }
}

}  // namespace

McpRunCleanupResult CleanupRun(
    Options options,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token, bool remove_retained_artifacts,
    const RunOwnership* expected_ownership,
    std::optional<OwnedRunRootIdentity> expected_root,
    bool* external_cleanup_complete) {
  McpRunCleanupResult result{
      .run_id = options.run_id,
      .verified_owned = true,
      .processes_remaining = 0U,
      .network_resources_remaining = 0U,
      .cgroups_remaining = 0U,
      .credentials_remaining = 0U,
      .complete = true,
  };
  std::stop_source deadline_stop_source;
  std::optional<std::jthread> deadline_timer;
  if (absolute_deadline) {
    deadline_timer.emplace(
        [deadline = *absolute_deadline,
         &deadline_stop_source](std::stop_token timer_stop_token) {
          try {
            WaitUntil(deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          if (!timer_stop_token.stop_requested()) {
            deadline_stop_source.request_stop();
          }
        });
  }
  CombinedStopToken cleanup_stop_tokens(stop_token,
                                        deadline_stop_source.get_token());
  const std::stop_token cleanup_stop_token = cleanup_stop_tokens.get_token();
  RequireCleanupActive(absolute_deadline, cleanup_stop_token);

  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  RunOwnership ownership;
  try {
    ownership = LoadRunOwnership(options.run_id, run_root, cleanup_stop_token);
  } catch (const std::exception& error) {
    if (cleanup_stop_token.stop_requested() ||
        (absolute_deadline &&
         std::chrono::steady_clock::now() >= *absolute_deadline)) {
      throw;
    }
    if (expected_ownership != nullptr) {
      throw McpOperationFailure("run_cleanup_unverified",
                                "run cleanup could not reverify ownership: " +
                                    std::string(error.what()),
                                false);
    }
    throw;
  }
  if (expected_ownership != nullptr && ownership != *expected_ownership) {
    throw McpOperationFailure(
        "run_cleanup_identity_reused",
        "run ownership changed after cleanup identity verification", false);
  }
  options.run_ownership = std::move(ownership);
  RequireCleanupActive(absolute_deadline, cleanup_stop_token);
  std::optional<RuntimeNodeResourceManifest> manifest;
  try {
    manifest = TryLoadRuntimeNodeResourceManifest(
        RequireRunOwnership(options), expected_root, cleanup_stop_token);
  } catch (const std::exception& error) {
    if (cleanup_stop_token.stop_requested() ||
        (absolute_deadline &&
         std::chrono::steady_clock::now() >= *absolute_deadline)) {
      throw;
    }
    if (expected_ownership != nullptr) {
      throw McpOperationFailure(
          "run_cleanup_unverified",
          "run cleanup could not verify its resource manifest: " +
              std::string(error.what()),
          false);
    }
    throw;
  }
  RequireCleanupActive(absolute_deadline, cleanup_stop_token);
  if (manifest) {
    options.isolate_network = manifest->isolated_network;
  } else {
    LoadCleanupMetadata(run_root, &options, cleanup_stop_token);
    RequireCleanupActive(absolute_deadline, cleanup_stop_token);
  }

  std::unique_ptr<NetworkAllocationLock> network_allocation_lock;
  const bool has_network_resources =
      options.isolate_network &&
      (manifest ? !manifest->nodes.empty() : options.nodes != 0U);
  if (has_network_resources) {
    try {
      RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");
    } catch (const std::exception& error) {
      if (expected_ownership != nullptr) {
        throw McpOperationFailure(
            "run_cleanup_capability_unavailable",
            "run cleanup cannot remove isolated network resources: " +
                std::string(error.what()),
            false);
      }
      throw;
    }
    RequireCleanupActive(absolute_deadline, cleanup_stop_token);
    network_allocation_lock =
        std::make_unique<NetworkAllocationLock>(cleanup_stop_token);
    RequireCleanupActive(absolute_deadline, cleanup_stop_token);
  }

  std::exception_ptr first_failure;
  const auto cleanup_step = [&](std::string_view description, auto&& action) {
    try {
      RequireCleanupActive(absolute_deadline, cleanup_stop_token);
      action();
      RequireCleanupActive(absolute_deadline, cleanup_stop_token);
      return true;
    } catch (...) {
      if (cleanup_stop_token.stop_requested() ||
          (absolute_deadline &&
           std::chrono::steady_clock::now() >= *absolute_deadline)) {
        throw;
      }
      if (!first_failure) {
        first_failure = std::current_exception();
      }
      try {
        std::rethrow_exception(std::current_exception());
      } catch (const std::exception& error) {
        BBP_LOG(error) << description
                       << " failed during offline cleanup: " << error.what();
      } catch (...) {
        BBP_LOG(error) << description << " failed during offline cleanup";
      }
      return false;
    }
  };

  if (manifest) {
    std::exception_ptr ownership_failure;
    for (const RuntimeNodeResourceEntry& entry : manifest->nodes) {
      try {
        RequireCleanupActive(absolute_deadline, cleanup_stop_token);
        const bool root_exists =
            RuntimeNodeRootEntryExists(RequireRunOwnership(options), entry,
                                       expected_root, cleanup_stop_token);
        if (root_exists) {
          VerifyRuntimeNodeRootOwnership(RequireRunOwnership(options), entry,
                                         expected_root, cleanup_stop_token);
        } else if (entry.state == RuntimeNodeResourceState::kLive) {
          throw std::runtime_error(
              "live runtime resource manifest entry has no owned node root: " +
              entry.node_id);
        }
      } catch (...) {
        if (cleanup_stop_token.stop_requested() ||
            (absolute_deadline &&
             std::chrono::steady_clock::now() >= *absolute_deadline)) {
          throw;
        }
        if (!ownership_failure) {
          ownership_failure = std::current_exception();
        }
      }
    }
    if (ownership_failure) {
      if (expected_ownership != nullptr) {
        std::string detail = "unknown ownership failure";
        try {
          std::rethrow_exception(ownership_failure);
        } catch (const std::exception& error) {
          detail = error.what();
        } catch (...) {
        }
        throw McpOperationFailure(
            "run_cleanup_unverified",
            "run cleanup could not verify runtime node ownership: " + detail,
            false);
      }
      std::rethrow_exception(ownership_failure);
    }
  }

  const bool cgroup_cleanup_verified =
      cleanup_step("owned run cgroup removal", [&] {
        if (absolute_deadline) {
          Cgroup::RemoveStaleRun(RequireRunOwnership(options),
                                 *absolute_deadline, cleanup_stop_token);
        } else {
          Cgroup::RemoveStaleRun(RequireRunOwnership(options));
        }
      });
  std::vector<bool> network_cleanup_verified(
      manifest ? manifest->nodes.size() : options.nodes,
      !has_network_resources);
  if (cgroup_cleanup_verified && has_network_resources) {
    if (manifest) {
      for (std::size_t index = 0U; index < manifest->nodes.size(); ++index) {
        const RuntimeNodeResourceEntry& entry = manifest->nodes[index];
        NodeVethConfig config;
        const RunOwnership& ownership = RequireRunOwnership(options);
        config.host_name = RunInterfaceName(ownership, entry.slot, 'h');
        config.peer_name = RunInterfaceName(ownership, entry.slot, 'p');
        config.host_ownership_alias =
            RunInterfaceAlias(ownership, entry.slot, 'h');
        config.peer_ownership_alias =
            RunInterfaceAlias(ownership, entry.slot, 'p');
        network_cleanup_verified[index] = cleanup_step(
            "owned node network removal",
            [&] { DeleteNodeVethNetwork(config, cleanup_stop_token); });
      }
    } else {
      for (uint32_t i = 0; i < options.nodes; ++i) {
        NodeVethConfig config;
        const RunOwnership& ownership = RequireRunOwnership(options);
        config.host_name = RunInterfaceName(ownership, i, 'h');
        config.peer_name = RunInterfaceName(ownership, i, 'p');
        config.host_ownership_alias = RunInterfaceAlias(ownership, i, 'h');
        config.peer_ownership_alias = RunInterfaceAlias(ownership, i, 'p');
        network_cleanup_verified[i] = cleanup_step(
            "owned node network removal",
            [&] { DeleteNodeVethNetwork(config, cleanup_stop_token); });
      }
    }
  }
  if (manifest) {
    std::vector<bool> credential_cleanup_verified(manifest->nodes.size(),
                                                  false);
    for (std::size_t index = 0U; index < manifest->nodes.size(); ++index) {
      if (!cgroup_cleanup_verified || !network_cleanup_verified[index]) {
        continue;
      }
      credential_cleanup_verified[index] =
          cleanup_step("owned node RPC credential removal", [&] {
            CleanupRuntimeNodeRpcCredential(RequireRunOwnership(options),
                                            manifest->nodes[index],
                                            expected_root, cleanup_stop_token);
          });
    }
    RuntimeNodeResourceManifest reconciled = *manifest;
    std::size_t index = 0U;
    std::erase_if(reconciled.nodes, [&](const RuntimeNodeResourceEntry& entry) {
      const std::size_t entry_index = index++;
      if (entry.state != RuntimeNodeResourceState::kPendingAdd) {
        return false;
      }
      if (!cgroup_cleanup_verified || !network_cleanup_verified[entry_index] ||
          !credential_cleanup_verified[entry_index]) {
        return false;
      }
      return cleanup_step("owned pending node root removal", [&] {
        RemoveRuntimeNodeRoot(RequireRunOwnership(options), entry,
                              absolute_deadline, cleanup_stop_token,
                              expected_root);
      });
    });
    if (reconciled != *manifest) {
      cleanup_step("runtime node resource manifest reconciliation", [&] {
        WriteRuntimeNodeResourceManifest(reconciled, expected_root,
                                         cleanup_stop_token);
      });
    }
  } else {
    const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
    for (uint32_t i = 0; i < options.nodes; ++i) {
      if (!cgroup_cleanup_verified || !network_cleanup_verified[i]) {
        continue;
      }
      ChainNodeConfigRequest request;
      request.run_id = options.run_id;
      request.run_root = run_root;
      request.node_index = i;
      if (!options.node_ids.empty()) {
        request.node_id = options.node_ids.at(i);
      }
      cleanup_step("legacy node RPC credential removal", [&] {
        const ChainNodeConfig config = MakeChainNodeConfig(chain_spec, request);
        CleanupLegacyRuntimeNodeRpcCredential(
            RequireRunOwnership(options), config.id, options.chain,
            expected_root, cleanup_stop_token);
      });
    }
  }
  if (first_failure) {
    std::rethrow_exception(first_failure);
  }
  if (external_cleanup_complete != nullptr) {
    *external_cleanup_complete = true;
  }
  if (remove_retained_artifacts) {
    if (!expected_root) {
      throw std::logic_error(
          "artifact-removing cleanup requires a verified root identity");
    }
    RequireCleanupActive(absolute_deadline, cleanup_stop_token);
    DetachRunLogFile(run_root, absolute_deadline, cleanup_stop_token);
    WriteOwnedRunRootCleanupReceipt(RequireRunOwnership(options),
                                    *expected_root, absolute_deadline,
                                    cleanup_stop_token);
    RemoveOwnedRunRoot(RequireRunOwnership(options), absolute_deadline,
                       cleanup_stop_token, expected_root);
  } else {
    RequireCleanupActive(absolute_deadline, cleanup_stop_token);
  }
  try {
    BBP_LOG(info) << "cleanup_run=" << options.run_id << "\n"
                  << "nodes="
                  << (manifest ? manifest->nodes.size()
                               : static_cast<std::size_t>(options.nodes))
                  << "\n"
                  << "run_dir=" << run_root;
  } catch (...) {
    // Logging cannot turn committed cleanup into an ownership-unverifiable
    // failure after the retained root has been removed.
  }
  return result;
}

}  // namespace bbp::simulator_app_internal
