#include "benchmark_sim/cgroup.h"
#include "benchmark_sim/firo_driver.h"
#include "benchmark_sim/logging.h"
#include "benchmark_sim/network.h"
#include "benchmark_sim/process.h"
#include "benchmark_sim/util.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/program_options.hpp>

namespace bsim {
namespace {

constexpr const char* kDefaultRewardAddress =
    "TTJW6FsYqLbSiF3ZUwMXRghgQuXK7XTodR";

struct Options {
  std::filesystem::path firod = "/home/navidr/work/firo/build/bin/firod";
  std::filesystem::path output_dir = "runs";
  std::string run_id = MakeRunId();
  uint32_t nodes = 1;
  uint32_t generate_blocks = 1;
  uint32_t ready_timeout_sec = 30;
  bool keep_cgroups = false;
  bool replace_run = false;
  bool probe_address = false;
  bool probe_netns = false;
  bool probe_veth = false;
  bool probe_network = false;
};

struct NodeRuntime {
  FiroNodeConfig config;
  std::optional<Cgroup> cgroup;
  ChildProcess process;
};

Options ParseOptions(int argc, char** argv) {
  namespace po = boost::program_options;
  Options options;

  po::options_description desc("Allowed options");
  desc.add_options()("help", "show this help")(
      "firod", po::value<std::filesystem::path>(&options.firod),
      "explicit firod binary")(
      "output-dir", po::value<std::filesystem::path>(&options.output_dir),
      "run output root")("run-id", po::value<std::string>(&options.run_id),
                         "safe run id")(
      "nodes", po::value<uint32_t>(&options.nodes), "Firo regtest nodes, 1..2")(
      "generate-blocks", po::value<uint32_t>(&options.generate_blocks),
      "blocks generated on node 0")(
      "ready-timeout-sec", po::value<uint32_t>(&options.ready_timeout_sec),
      "RPC startup timeout")("keep-cgroups", po::bool_switch(&options.keep_cgroups),
                             "leave cgroups after exit for inspection")(
      "replace-run", po::bool_switch(&options.replace_run),
      "remove an existing run directory first")(
      "probe-address", po::bool_switch(&options.probe_address),
      "assign and inspect an IPv4 address inside a temporary netns through "
      "libmnl")(
      "probe-netns", po::bool_switch(&options.probe_netns),
      "create a temporary network namespace and inspect it through setns/libmnl")(
      "probe-veth", po::bool_switch(&options.probe_veth),
      "create, move, inspect, and delete a temporary veth pair through libmnl")(
      "probe-network", po::bool_switch(&options.probe_network),
      "list links through rtnetlink/libmnl and exit");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") != 0U) {
    std::cout << "Usage: " << argv[0] << " [options]\n" << desc << "\n";
    std::exit(0);
  }
  if (options.nodes < 1 || options.nodes > 2) {
    throw std::runtime_error("--nodes currently supports 1..2 for MVP smoke");
  }
  RequireSafeRunId(options.run_id);
  if (!options.probe_network && !options.probe_netns && !options.probe_veth &&
      !options.probe_address) {
    RequireExecutable(options.firod);
  }
  return options;
}

boost::json::array LinksJson(const std::vector<LinkInfo>& links) {
  boost::json::array links_json;
  for (const LinkInfo& link : links) {
    boost::json::object link_json;
    link_json["index"] = link.index;
    link_json["name"] = link.name;
    link_json["up"] = link.up;
    links_json.push_back(std::move(link_json));
  }
  return links_json;
}

boost::json::array AddressesJson(const std::vector<AddressInfo>& addresses) {
  boost::json::array addresses_json;
  for (const AddressInfo& address : addresses) {
    boost::json::object address_json;
    address_json["if_index"] = address.if_index;
    address_json["if_name"] = address.if_name;
    address_json["address"] = address.address;
    address_json["prefix_len"] = address.prefix_len;
    addresses_json.push_back(std::move(address_json));
  }
  return addresses_json;
}

std::string NetworkProbeJson() {
  boost::json::object result;
  result["links"] = LinksJson(ListNetworkLinks());
  result["ipv4_addresses"] = AddressesJson(ListIpv4Addresses());
  return boost::json::serialize(result);
}

std::string NetworkNamespaceProbeJson() {
  NetworkNamespaceProbe probe = ProbeIsolatedNetworkNamespace();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["parent_links"] = LinksJson(probe.parent_links);
  result["namespace_links"] = LinksJson(probe.namespace_links);
  return boost::json::serialize(result);
}

std::string VethProbeJson() {
  VethProbe probe = ProbeVethPair();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["parent_before"] = LinksJson(probe.parent_before);
  result["parent_after_create"] = LinksJson(probe.parent_after_create);
  result["parent_after_move"] = LinksJson(probe.parent_after_move);
  result["namespace_after_move"] = LinksJson(probe.namespace_after_move);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string AddressProbeJson() {
  AddressProbe probe = ProbeIpv4AddressAssignment();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["assigned_address"] = probe.assigned_address;
  result["assigned_prefix_len"] = probe.assigned_prefix_len;
  result["parent_after_move"] = LinksJson(probe.parent_after_move);
  result["namespace_links_after_address"] =
      LinksJson(probe.namespace_links_after_address);
  result["namespace_addresses"] = AddressesJson(probe.namespace_addresses);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string MetricsJson(const std::string& run_id, const std::string& node_id,
                        const FiroMetrics& chain,
                        const CgroupMetrics* cgroup) {
  boost::json::object object;
  object["timestamp_ms"] = NowUnixMillis();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["height"] = chain.height;
  object["best_hash"] = chain.best_hash;
  object["peer_count"] = chain.peer_count;
  object["mempool_tx_count"] = chain.mempool_tx_count;
  if (cgroup != nullptr) {
    object["cpu_usage_usec"] = cgroup->cpu_usage_usec;
    object["cpu_throttled_usec"] = cgroup->cpu_throttled_usec;
    object["memory_current"] = cgroup->memory_current;
    object["memory_peak"] = cgroup->memory_peak;
    object["pids_current"] = cgroup->pids_current;
    object["oom"] = cgroup->oom;
    object["oom_kill"] = cgroup->oom_kill;
  }
  return boost::json::serialize(object);
}

void WriteEvent(const std::filesystem::path& events_path,
                const std::string& run_id, const std::string& node_id,
                std::string_view event, std::string_view detail = "") {
  boost::json::object object;
  object["timestamp"] = NowIso8601();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["event"] = event;
  object["detail"] = detail;
  AppendLine(events_path, boost::json::serialize(object));
}

void WriteScenarioFiles(const Options& options,
                        const std::filesystem::path& run_root) {
  WriteText(run_root / "scenario.yaml",
            "simulation:\n"
            "  name: " +
                options.run_id +
                "\n"
                "  output_dir: " +
                run_root.string() +
                "\n"
                "chains:\n"
                "  firo:\n"
                "    driver: firo\n"
                "    default_binary: " +
                options.firod.string() +
                "\n"
                "nodes: " +
                std::to_string(options.nodes) +
                "\n"
                "workloads:\n"
                "  - type: block_generation\n"
                "    count: " +
                std::to_string(options.generate_blocks) + "\n");

  boost::json::object resolved;
  resolved["run_id"] = options.run_id;
  resolved["chain"] = "firo";
  resolved["nodes"] = options.nodes;
  resolved["firod"] = options.firod.string();
  WriteText(run_root / "resolved-scenario.json",
            boost::json::serialize(resolved) + "\n");
}

void StartNodes(const Options& options, const std::filesystem::path& run_root,
                const std::filesystem::path& events_path,
                const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  nodes.reserve(options.nodes);
  for (uint32_t i = 0; i < options.nodes; ++i) {
    const std::string node_id = "firo-" + std::to_string(i + 1);
    const auto node_root = run_root / "nodes" / node_id;
    FiroNodeConfig config;
    config.id = node_id;
    config.binary = options.firod;
    config.data_dir = node_root / "data";
    config.log_dir = node_root;
    config.p2p_port = static_cast<uint16_t>(18168 + i);
    config.rpc_port = static_cast<uint16_t>(18888 + i);
    config.rpc_user = "sim-" + options.run_id;
    config.rpc_password = "pass-" + options.run_id + "-" + std::to_string(i);
    config.listen = true;
    if (i > 0) {
      config.connect_peers.push_back("127.0.0.1:18168");
    }

    NodeRuntime runtime;
    runtime.config = config;
    runtime.cgroup = Cgroup::Create(options.run_id, node_id);
    runtime.cgroup->SetMemoryHigh(1536ULL * 1024ULL * 1024ULL);
    runtime.cgroup->SetMemoryMax(2ULL * 1024ULL * 1024ULL * 1024ULL);
    runtime.cgroup->SetCpuMax(std::nullopt, 100000);
    runtime.cgroup->SetPidsMax(256);

    ProcessSpec process = driver.RenderProcess(runtime.config);
    runtime.process = ChildProcess::Spawn(process, runtime.cgroup->path());
    BSIM_LOG(info) << "started " << node_id << " pid=" << runtime.process.pid();
    WriteEvent(events_path, options.run_id, node_id, "process_started",
               "pid=" + std::to_string(runtime.process.pid()));
    nodes.push_back(std::move(runtime));
  }

  for (auto& node : nodes) {
    driver.WaitReady(node.config,
                     std::chrono::seconds(options.ready_timeout_sec));
    WriteEvent(events_path, options.run_id, node.config.id, "rpc_ready");
  }
}

void StopNodes(const Options& options, const std::filesystem::path& events_path,
               const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  for (auto& node : nodes) {
    WriteEvent(events_path, options.run_id, node.config.id, "rpc_stop");
    driver.Stop(node.config);
  }
  for (auto& node : nodes) {
    if (!node.process.WaitForExit(std::chrono::seconds(15))) {
      WriteEvent(events_path, options.run_id, node.config.id, "sigterm");
      node.process.Terminate(std::chrono::seconds(5));
    }
    if (node.cgroup && !options.keep_cgroups) {
      node.cgroup->KillAll();
      try {
        node.cgroup->Remove();
      } catch (const std::exception& e) {
        WriteEvent(events_path, options.run_id, node.config.id,
                   "cgroup_remove_failed", e.what());
      }
    }
  }
  if (!options.keep_cgroups) {
    try {
      Cgroup::RemoveRun(options.run_id);
    } catch (const std::exception& e) {
      WriteEvent(events_path, options.run_id, "sim", "run_cgroup_remove_failed",
                 e.what());
    }
  }
}

int Run(int argc, char** argv) {
  Options options = ParseOptions(argc, argv);
  if (options.probe_network) {
    std::cout << NetworkProbeJson() << "\n";
    return 0;
  }
  if (options.probe_netns) {
    std::cout << NetworkNamespaceProbeJson() << "\n";
    return 0;
  }
  if (options.probe_veth) {
    std::cout << VethProbeJson() << "\n";
    return 0;
  }
  if (options.probe_address) {
    std::cout << AddressProbeJson() << "\n";
    return 0;
  }

  BSIM_LOG(info) << "starting run " << options.run_id;
  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  if (std::filesystem::exists(run_root)) {
    if (!options.replace_run) {
      throw std::runtime_error("run directory already exists: " +
                               run_root.string() +
                               " (use --replace-run to remove it)");
    }
    std::error_code ec;
    std::filesystem::remove_all(run_root, ec);
    if (ec) {
      throw std::runtime_error("remove existing run directory failed: " +
                               ec.message());
    }
    Cgroup::RemoveRun(options.run_id);
  }
  EnsureDirectory(run_root / "nodes");
  WriteScenarioFiles(options, run_root);

  const auto events_path = run_root / "events.jsonl";
  const auto metrics_path = run_root / "metrics.jsonl";
  WriteEvent(events_path, options.run_id, "sim", "run_started");

  FiroDriver driver(std::chrono::seconds(5));
  std::vector<NodeRuntime> nodes;
  try {
    StartNodes(options, run_root, events_path, driver, nodes);

    for (auto& node : nodes) {
      FiroMetrics chain = driver.ReadMetrics(node.config);
      CgroupMetrics cg = node.cgroup->ReadMetrics();
      AppendLine(metrics_path,
                 MetricsJson(options.run_id, node.config.id, chain, &cg));
    }

    if (options.generate_blocks > 0) {
      std::vector<std::string> hashes = driver.GenerateBlocks(
          nodes.front().config, options.generate_blocks, kDefaultRewardAddress);
      WriteEvent(events_path, options.run_id, nodes.front().config.id,
                 "generated_blocks", std::to_string(hashes.size()));
    }

    for (auto& node : nodes) {
      FiroMetrics chain = driver.ReadMetrics(node.config);
      CgroupMetrics cg = node.cgroup->ReadMetrics();
      AppendLine(metrics_path,
                 MetricsJson(options.run_id, node.config.id, chain, &cg));
    }

    StopNodes(options, events_path, driver, nodes);
    WriteEvent(events_path, options.run_id, "sim", "run_finished");
    BSIM_LOG(info) << "finished run " << options.run_id;
  } catch (...) {
    StopNodes(options, events_path, driver, nodes);
    WriteEvent(events_path, options.run_id, "sim", "run_failed");
    throw;
  }

  std::cout << "run_id=" << options.run_id << "\n"
            << "output_dir=" << run_root << "\n"
            << "metrics=" << metrics_path << "\n"
            << "events=" << events_path << "\n";
  return 0;
}

}  // namespace
}  // namespace bsim

int main(int argc, char** argv) {
  try {
    bsim::InitLogging();
    return bsim::Run(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "benchmark-sim: " << e.what() << "\n";
    return 1;
  }
}
