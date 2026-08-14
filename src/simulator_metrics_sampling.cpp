#include "simulator_metrics_sampling.h"

#include <sys/stat.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/perf_counter.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_metrics_json.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_wallet_metrics_json.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {
namespace {

const LinkInfo* FindLinkByName(const std::vector<LinkInfo>& links,
                               std::string_view name) {
  for (const LinkInfo& link : links) {
    if (link.name == name) {
      return &link;
    }
  }
  return nullptr;
}

const QdiscInfo* FindQdiscByInterfaceName(const std::vector<QdiscInfo>& qdiscs,
                                          std::string_view name) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == name) {
      return &qdisc;
    }
  }
  return nullptr;
}

}  // namespace

std::uint32_t WriteMetricsSnapshot(
    const std::filesystem::path& metrics_path, const Options& options,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    RunProcessState& run_process_state,
    MetricsSnapshotSynchronization synchronization,
    const NodeMetricsFailureHandler& node_failure_handler,
    const MetricsStopRequested& stop_requested, std::stop_token stop_token,
    const NodeRoleTopology* runtime_topology,
    const std::set<std::string>* selected_node_ids,
    const NodeMetricsRecordHandler& record_handler) {
  std::uint32_t sample_count = 0U;
  struct NetworkMetricsState {
    std::optional<NodeVethConfig> network;
    std::string profile;
  };
  const std::vector<LinkInfo> links = ListNetworkLinks(stop_token);
  std::vector<QdiscInfo> qdiscs;
  std::vector<NetworkMetricsState> network_states(nodes.size());
  {
    std::lock_guard<std::mutex> lock(synchronization.network_state);
    const bool has_isolated_node =
        std::any_of(nodes.begin(), nodes.end(), [&](const NodeRuntime& node) {
          return (selected_node_ids == nullptr ||
                  selected_node_ids->contains(node.config.id)) &&
                 node.network.has_value();
        });
    if (has_isolated_node) {
      qdiscs = ListQdiscs(stop_token);
    }
    for (std::size_t index = 0; index < nodes.size(); ++index) {
      if (selected_node_ids != nullptr &&
          !selected_node_ids->contains(nodes[index].config.id)) {
        continue;
      }
      network_states[index].network = nodes[index].network;
      network_states[index].profile = nodes[index].network_profile;
    }
  }
  for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
    NodeRuntime& node = nodes[node_index];
    if (selected_node_ids != nullptr &&
        !selected_node_ids->contains(node.config.id)) {
      continue;
    }
    if (stop_requested && stop_requested()) {
      return sample_count;
    }
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    ChainMetrics chain;
    try {
      chain = driver.ReadMetrics(node.config, stop_token);
    } catch (const std::exception& error) {
      if (stop_token.stop_requested() || (stop_requested && stop_requested())) {
        return sample_count;
      }
      if (!node.AllowsChainMetrics()) {
        continue;
      }
      if (!node_failure_handler) {
        throw;
      }
      node_failure_handler(node, error.what());
      continue;
    }
    NodeRuntimeMetrics runtime;
    runtime.node_index = static_cast<std::uint32_t>(node_index + 1U);
    runtime.chain = std::string(ChainKindName(options.chain));
    runtime.role = NodeRoleName(options, static_cast<std::uint32_t>(node_index),
                                runtime_topology);
    runtime.lifecycle = NodeRuntimeLifecycleName(node.Lifecycle());
    runtime.data_dir = node.config.data_dir.string();
    runtime.log_dir = node.config.log_dir.string();
    const RpcEndpoint rpc_endpoint = driver.Endpoint(node.config);
    runtime.rpc_host = rpc_endpoint.host;
    runtime.rpc_port = rpc_endpoint.port;
    {
      auto process_guard = run_process_state.Lock();
      runtime.pid = node.process.pid();
      runtime.pidfd_available = node.process.pidfd() >= 0;
      runtime.process_running = node.process.running();
      runtime.exit_status = node.process.exit_status();
      runtime.perf_counter_kinds = node.perf_counter_kinds;
      runtime.perf_counter_target_kind = node.perf_counter_target_kind;
      runtime.perf_counter_target_id = node.perf_counter_target_id;
      runtime.perf_counter_target_pid = node.perf_counter_target_pid;
      runtime.perf_counter_attached_pid = node.perf_counter_attached_pid;
      runtime.perf_counter_process_generation =
          node.perf_counter_process_generation;
      runtime.perf_counter_cgroup_path = node.perf_counter_cgroup_path.string();
      runtime.perf_counter_cpus = node.perf_counter_cpus;
      runtime.perf_counter_error_kind = node.perf_counter_error_kind;
      runtime.perf_counter_error = node.perf_counter_error;
      if (node.process_perf_counters) {
        try {
          runtime.perf_counter_values = node.process_perf_counters->Read();
          runtime.perf_counters_available = true;
          runtime.perf_counter_error_kind.reset();
          runtime.perf_counter_error.clear();
        } catch (const PerfCounterError& error) {
          runtime.perf_counter_error_kind = error.kind();
          runtime.perf_counter_error = error.what();
        }
      } else if (node.cgroup_perf_counters) {
        try {
          runtime.perf_counter_values = node.cgroup_perf_counters->Read();
          runtime.perf_counters_available = true;
          runtime.perf_counter_error_kind.reset();
          runtime.perf_counter_error.clear();
        } catch (const PerfCounterError& error) {
          runtime.perf_counter_error_kind = error.kind();
          runtime.perf_counter_error = error.what();
        }
      }
      if (node.process_started_at) {
        const auto now = std::chrono::steady_clock::now();
        runtime.uptime_ms =
            now <= *node.process_started_at
                ? 0U
                : static_cast<std::uint64_t>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - *node.process_started_at)
                          .count());
      }
    }
    if (node.cgroup) {
      runtime.cgroup_path = node.cgroup->path().string();
    }
    if (node.network_namespace) {
      struct stat namespace_stat{};
      if (fstat(node.network_namespace->fd(), &namespace_stat) != 0) {
        throw std::runtime_error("fstat node network namespace failed");
      }
      runtime.network_namespace_inode = namespace_stat.st_ino;
      runtime.network_namespace_helper_pid =
          node.network_namespace->helper_pid();
    }
    CgroupMetrics cg;
    std::string resource_profile;
    {
      std::lock_guard<std::mutex> lock(synchronization.resource_state);
      cg = node.cgroup->ReadMetrics();
      resource_profile = node.resource_profile;
    }
    const std::optional<NodeVethConfig>& network =
        network_states[node_index].network;
    if (network) {
      runtime.host_interface = network->host_name;
      runtime.child_interface = network->peer_name;
      runtime.host_address = network->host_address;
      runtime.node_address = network->node_address;
      runtime.prefix_length = network->prefix_len;
      if (node.network_namespace) {
        runtime.routes =
            ListIpv4RoutesInNamespace(node.network_namespace->fd(), stop_token);
      }
    }
    std::optional<DirectionalNetworkPolicyStats> directional_stats;
    if (network && node.network_namespace) {
      std::lock_guard<std::mutex> lock(synchronization.network_state);
      directional_stats = ReadDirectionalNetworkPolicyStatsInNamespace(
          node.network_namespace->fd(), network->peer_name,
          node.directional_network_policies, stop_token);
    }
    const LinkInfo* link =
        network ? FindLinkByName(links, network->host_name) : nullptr;
    std::optional<QdiscInfo> qdisc_summary;
    const QdiscInfo* qdisc = nullptr;
    std::vector<QdiscInfo> qdisc_tree;
    std::vector<TcFilterInfo> filters;
    const std::vector<TcFilterInfo>* filter_metrics = nullptr;
    if (network) {
      for (const QdiscInfo& candidate : qdiscs) {
        if (candidate.if_name == network->host_name) {
          qdisc_tree.push_back(candidate);
        }
      }
      if (network->apply_condition) {
        QdiscInfo candidate;
        if (QdiscsMatchNetworkCondition(qdiscs, network->host_name,
                                        network->condition, &candidate)) {
          qdisc_summary = candidate;
          qdisc = &*qdisc_summary;
        }
      }
      if (qdisc == nullptr) {
        qdisc = FindQdiscByInterfaceName(qdiscs, network->host_name);
      }
      if (link != nullptr) {
        std::lock_guard<std::mutex> lock(synchronization.network_state);
        filters = ListTcFiltersForInterface(network->host_name, stop_token);
        filter_metrics = &filters;
      }
    }
    const std::string record = MetricsJson(
        options.run_id, node.config.id, runtime, chain,
        node.GeneratedBlockCount(), node.MinedTransactionCount(),
        node.MinedTransactionCountComplete(), node.RestartCount(),
        resource_profile, network_states[node_index].profile,
        network && network->apply_condition ? &network->condition : nullptr,
        &cg, link, qdisc, network ? &qdisc_tree : nullptr, filter_metrics,
        directional_stats ? &*directional_stats : nullptr);
    AppendLine(metrics_path, record);
    if (record_handler) {
      record_handler(node, record);
    }
    ++sample_count;
  }
  return sample_count;
}

std::uint32_t WriteWalletMetricsSnapshot(
    const std::filesystem::path& metrics_path, const Options& options,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const SimulationRegistry& registry,
    const WalletMetricsFailureHandler& failure_handler,
    std::stop_token stop_token) {
  std::uint32_t sample_count = 0U;
  constexpr std::uint32_t kTransactionLimit = 256U;
  for (std::size_t index = 0; index < registry.wallets().size(); ++index) {
    const WalletIdentity& wallet = registry.wallets()[index];
    if (wallet.node == 0U) {
      throw std::runtime_error("wallet metrics backing node is zero");
    }
    const std::uint32_t node_index = wallet.node - 1U;
    if (node_index >= nodes.size()) {
      throw std::runtime_error("wallet metrics node is out of range");
    }
    const NodeRuntime& node = nodes[node_index];
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    try {
      const ChainWalletSnapshot snapshot = driver.ReadWalletSnapshot(
          node.config, ToChainWalletMode(registry.wallet_initialization()),
          kTransactionLimit, stop_token);
      AppendLine(
          metrics_path,
          WalletMetricsJson(options, static_cast<std::uint32_t>(index + 1U),
                            node_index + 1U,
                            registry.wallet_initialization().mode, snapshot));
      ++sample_count;
    } catch (const std::exception& error) {
      if (stop_token.stop_requested()) {
        return sample_count;
      }
      if (!node.AllowsChainMetrics()) {
        continue;
      }
      if (!failure_handler) {
        throw;
      }
      failure_handler(static_cast<std::uint32_t>(index + 1U), node,
                      error.what());
    }
  }
  return sample_count;
}

}  // namespace bbp::simulator_app_internal
