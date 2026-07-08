#include "benchmark_sim/cgroup.h"
#include "benchmark_sim/firo_driver.h"
#include "benchmark_sim/logging.h"
#include "benchmark_sim/network.h"
#include "benchmark_sim/process.h"
#include "benchmark_sim/util.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
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
  uint32_t sync_timeout_sec = 30;
  bool keep_cgroups = false;
  bool cleanup_run = false;
  bool isolate_network = false;
  bool network_condition_requested = false;
  NetworkCondition network_condition;
  std::vector<std::string> node_network_condition_json;
  std::map<uint32_t, NetworkCondition> node_network_conditions;
  bool replace_run = false;
  bool probe_address = false;
  bool probe_netns = false;
  bool probe_network_condition = false;
  bool probe_qdisc = false;
  bool probe_qdisc_mutation = false;
  bool probe_route = false;
  bool probe_veth = false;
  bool probe_network = false;
};

struct NodeRuntime {
  FiroNodeConfig config;
  std::optional<Cgroup> cgroup;
  std::optional<NetworkNamespace> network_namespace;
  std::optional<NodeVethConfig> network;
  ChildProcess process;
};

uint32_t JsonUint32Field(const boost::json::object& object,
                         const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint32 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint32 JSON field: " +
                           std::string(field));
}

uint32_t JsonOptionalUint32Field(const boost::json::object& object,
                                 const char* field, uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint32 JSON field: " + std::string(field));
}

NetworkCondition ParseNetworkConditionObject(
    const boost::json::object& object) {
  NetworkCondition condition;
  condition.delay_ms =
      JsonOptionalUint32Field(object, "delay_ms", condition.delay_ms);
  condition.jitter_ms =
      JsonOptionalUint32Field(object, "jitter_ms", condition.jitter_ms);
  condition.loss_basis_points = JsonOptionalUint32Field(
      object, "loss_basis_points", condition.loss_basis_points);
  condition.duplicate_basis_points = JsonOptionalUint32Field(
      object, "duplicate_basis_points", condition.duplicate_basis_points);
  condition.limit_packets = JsonOptionalUint32Field(
      object, "limit_packets", condition.limit_packets);
  return condition;
}

void ParseNodeNetworkConditions(Options& options) {
  for (const std::string& text : options.node_network_condition_json) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--node-network-condition-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > options.nodes) {
      throw std::runtime_error(
          "--node-network-condition-json node must be in 1..--nodes");
    }
    options.node_network_conditions[node - 1U] =
        ParseNetworkConditionObject(object);
  }
}

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
      "RPC startup timeout")(
      "sync-timeout-sec", po::value<uint32_t>(&options.sync_timeout_sec),
      "block propagation timeout")(
      "keep-cgroups", po::bool_switch(&options.keep_cgroups),
      "leave cgroups after exit for inspection")(
      "cleanup-run", po::bool_switch(&options.cleanup_run),
      "remove stale simulator-owned veth and cgroup objects for --run-id and "
      "exit")(
      "isolate-network", po::bool_switch(&options.isolate_network),
      "run each Firo node in its own network namespace and veth link")(
      "network-delay-ms",
      po::value<uint32_t>(&options.network_condition.delay_ms),
      "netem delay applied to each isolated node host-side veth")(
      "network-jitter-ms",
      po::value<uint32_t>(&options.network_condition.jitter_ms),
      "netem jitter applied to each isolated node host-side veth")(
      "network-loss-bps",
      po::value<uint32_t>(&options.network_condition.loss_basis_points),
      "netem packet loss in basis points, 10000 = 100%")(
      "network-duplicate-bps",
      po::value<uint32_t>(&options.network_condition.duplicate_basis_points),
      "netem packet duplication in basis points, 10000 = 100%")(
      "network-limit-packets",
      po::value<uint32_t>(&options.network_condition.limit_packets),
      "netem queue limit applied to each isolated node host-side veth")(
      "node-network-condition-json",
      po::value<std::vector<std::string>>(
          &options.node_network_condition_json)
          ->composing(),
      "repeatable JSON object with node plus netem fields for one isolated "
      "node")(
      "replace-run", po::bool_switch(&options.replace_run),
      "remove an existing run directory first")(
      "probe-address", po::bool_switch(&options.probe_address),
      "assign and inspect an IPv4 address inside a temporary netns through "
      "libmnl")(
      "probe-netns", po::bool_switch(&options.probe_netns),
      "create a temporary network namespace and inspect it through setns/libmnl")(
      "probe-network-condition",
      po::bool_switch(&options.probe_network_condition),
      "apply and remove a netem network condition on a temporary veth peer "
      "through libmnl")(
      "probe-qdisc", po::bool_switch(&options.probe_qdisc),
      "dump qdisc state for a temporary veth peer through libmnl")(
      "probe-qdisc-mutation",
      po::bool_switch(&options.probe_qdisc_mutation),
      "replace and delete a root pfifo qdisc on a temporary veth peer through "
      "libmnl")(
      "probe-route", po::bool_switch(&options.probe_route),
      "assign and inspect an IPv4 route inside a temporary netns through libmnl")(
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
  options.network_condition_requested =
      vm.count("network-delay-ms") != 0U ||
      vm.count("network-jitter-ms") != 0U ||
      vm.count("network-loss-bps") != 0U ||
      vm.count("network-duplicate-bps") != 0U ||
      vm.count("network-limit-packets") != 0U;
  if ((options.network_condition_requested ||
       !options.node_network_condition_json.empty()) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network condition options require --isolate-network");
  }
  if (options.nodes < 1 || options.nodes > 2) {
    throw std::runtime_error("--nodes currently supports 1..2 for MVP smoke");
  }
  ParseNodeNetworkConditions(options);
  RequireSafeRunId(options.run_id);
  if (!options.probe_network && !options.probe_netns && !options.probe_veth &&
      !options.probe_address && !options.probe_route && !options.probe_qdisc &&
      !options.probe_qdisc_mutation && !options.probe_network_condition &&
      !options.cleanup_run) {
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
    link_json["has_stats"] = link.has_stats;
    link_json["rx_bytes"] = link.rx_bytes;
    link_json["tx_bytes"] = link.tx_bytes;
    link_json["rx_packets"] = link.rx_packets;
    link_json["tx_packets"] = link.tx_packets;
    link_json["rx_dropped"] = link.rx_dropped;
    link_json["tx_dropped"] = link.tx_dropped;
    link_json["rx_errors"] = link.rx_errors;
    link_json["tx_errors"] = link.tx_errors;
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

boost::json::array RoutesJson(const std::vector<RouteInfo>& routes) {
  boost::json::array routes_json;
  for (const RouteInfo& route : routes) {
    boost::json::object route_json;
    route_json["destination"] = route.destination;
    route_json["prefix_len"] = route.prefix_len;
    route_json["oif_index"] = route.oif_index;
    route_json["oif_name"] = route.oif_name;
    route_json["gateway"] = route.gateway;
    route_json["table"] = route.table;
    route_json["protocol"] = route.protocol;
    route_json["scope"] = route.scope;
    route_json["type"] = route.type;
    routes_json.push_back(std::move(route_json));
  }
  return routes_json;
}

boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs) {
  boost::json::array qdiscs_json;
  for (const QdiscInfo& qdisc : qdiscs) {
    boost::json::object qdisc_json;
    qdisc_json["if_index"] = qdisc.if_index;
    qdisc_json["if_name"] = qdisc.if_name;
    qdisc_json["kind"] = qdisc.kind;
    qdisc_json["handle"] = qdisc.handle;
    qdisc_json["parent"] = qdisc.parent;
    qdisc_json["info"] = qdisc.info;
    qdisc_json["has_netem_options"] = qdisc.has_netem_options;
    qdisc_json["netem_latency_us"] = qdisc.netem_latency_us;
    qdisc_json["netem_jitter_us"] = qdisc.netem_jitter_us;
    qdisc_json["netem_loss"] = qdisc.netem_loss;
    qdisc_json["netem_duplicate"] = qdisc.netem_duplicate;
    qdisc_json["netem_limit_packets"] = qdisc.netem_limit_packets;
    qdiscs_json.push_back(std::move(qdisc_json));
  }
  return qdiscs_json;
}

boost::json::object NetworkConditionJson(const NetworkCondition& condition) {
  boost::json::object object;
  object["delay_ms"] = condition.delay_ms;
  object["jitter_ms"] = condition.jitter_ms;
  object["loss_basis_points"] = condition.loss_basis_points;
  object["duplicate_basis_points"] = condition.duplicate_basis_points;
  object["limit_packets"] = condition.limit_packets;
  return object;
}

std::string NodeHostAddress(uint32_t node_index) {
  return "10.210." + std::to_string(node_index + 1U) + ".1";
}

std::string NodeAddress(uint32_t node_index) {
  return "10.210." + std::to_string(node_index + 1U) + ".2";
}

uint32_t StableRunHash(std::string_view run_id) {
  uint32_t hash = 2166136261U;
  for (const unsigned char c : run_id) {
    hash ^= c;
    hash *= 16777619U;
  }
  return hash;
}

std::string RunInterfaceToken(std::string_view run_id) {
  constexpr char kHex[] = "0123456789abcdef";
  uint32_t value = StableRunHash(run_id) & 0x00FFFFFFU;
  std::string token(6, '0');
  for (int i = 5; i >= 0; --i) {
    token[static_cast<size_t>(i)] = kHex[value & 0x0FU];
    value >>= 4U;
  }
  return token;
}

std::string NodeInterfaceName(std::string_view run_id, uint32_t node_index,
                              char suffix) {
  return "bs" + RunInterfaceToken(run_id) + "n" +
         std::to_string(node_index + 1U) + suffix;
}

NodeVethConfig MakeNodeVethConfig(const Options& options,
                                  uint32_t node_index) {
  NodeVethConfig config;
  config.host_name = NodeInterfaceName(options.run_id, node_index, 'h');
  config.peer_name = NodeInterfaceName(options.run_id, node_index, 'p');
  config.host_address = NodeHostAddress(node_index);
  config.node_address = NodeAddress(node_index);
  config.prefix_len = 30;
  const auto node_condition = options.node_network_conditions.find(node_index);
  if (node_condition != options.node_network_conditions.end()) {
    config.apply_condition = true;
    config.condition = node_condition->second;
  } else {
    config.apply_condition = options.network_condition_requested;
    config.condition = options.network_condition;
  }
  return config;
}

std::string FiroPeerHost(const Options& options, uint32_t node_index) {
  if (options.isolate_network) {
    return NodeAddress(node_index);
  }
  return "127.0.0.1";
}

bool HostIpv4ForwardingEnabled() {
  const std::string value = ReadText("/proc/sys/net/ipv4/ip_forward");
  return !value.empty() && value.front() == '1';
}

const LinkInfo* FindLinkByName(const std::vector<LinkInfo>& links,
                               std::string_view name) {
  for (const LinkInfo& link : links) {
    if (link.name == name) {
      return &link;
    }
  }
  return nullptr;
}

std::string NetworkProbeJson() {
  boost::json::object result;
  result["links"] = LinksJson(ListNetworkLinks());
  result["ipv4_addresses"] = AddressesJson(ListIpv4Addresses());
  result["ipv4_routes"] = RoutesJson(ListIpv4Routes());
  result["qdiscs"] = QdiscsJson(ListQdiscs());
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

std::string NetworkConditionProbeJson() {
  NetworkConditionProbe probe = ProbeNetworkCondition();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] =
      QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string QdiscProbeJson() {
  QdiscProbe probe = ProbeQdiscDump();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["namespace_links"] = LinksJson(probe.namespace_links);
  result["namespace_qdiscs"] = QdiscsJson(probe.namespace_qdiscs);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string QdiscMutationProbeJson() {
  QdiscMutationProbe probe = ProbeQdiscMutation();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["pfifo_limit_packets"] = probe.pfifo_limit_packets;
  result["namespace_qdiscs_before"] =
      QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_replace"] =
      QdiscsJson(probe.namespace_qdiscs_after_replace);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string RouteProbeJson() {
  RouteProbe probe = ProbeIpv4RouteAssignment();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["assigned_address"] = probe.assigned_address;
  result["assigned_prefix_len"] = probe.assigned_prefix_len;
  result["route_destination"] = probe.route_destination;
  result["route_prefix_len"] = probe.route_prefix_len;
  result["namespace_links_after_route"] =
      LinksJson(probe.namespace_links_after_route);
  result["namespace_addresses"] = AddressesJson(probe.namespace_addresses);
  result["namespace_routes"] = RoutesJson(probe.namespace_routes);
  result["namespace_addresses_after_delete"] =
      AddressesJson(probe.namespace_addresses_after_delete);
  result["namespace_routes_after_delete"] =
      RoutesJson(probe.namespace_routes_after_delete);
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
  result["namespace_addresses_after_delete"] =
      AddressesJson(probe.namespace_addresses_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string MetricsJson(const std::string& run_id, const std::string& node_id,
                        const FiroMetrics& chain,
                        const CgroupMetrics* cgroup, const LinkInfo* link) {
  boost::json::object object;
  object["timestamp_ms"] = NowUnixMillis();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["height"] = chain.height;
  object["best_hash"] = chain.best_hash;
  object["peer_count"] = chain.peer_count;
  object["mempool_tx_count"] = chain.mempool_tx_count;
  object["rpc_latency_ms"] = chain.rpc_latency_ms;
  if (cgroup != nullptr) {
    object["cpu_usage_usec"] = cgroup->cpu_usage_usec;
    object["cpu_throttled_usec"] = cgroup->cpu_throttled_usec;
    object["cpu_pressure_some_total_usec"] =
        cgroup->cpu_pressure_some_total_usec;
    object["cpu_pressure_full_total_usec"] =
        cgroup->cpu_pressure_full_total_usec;
    object["memory_current"] = cgroup->memory_current;
    object["memory_peak"] = cgroup->memory_peak;
    object["io_read_bytes"] = cgroup->io_read_bytes;
    object["io_write_bytes"] = cgroup->io_write_bytes;
    object["io_pressure_some_total_usec"] =
        cgroup->io_pressure_some_total_usec;
    object["io_pressure_full_total_usec"] =
        cgroup->io_pressure_full_total_usec;
    object["pids_current"] = cgroup->pids_current;
    object["oom"] = cgroup->oom;
    object["oom_kill"] = cgroup->oom_kill;
  }
  if (link != nullptr) {
    object["network_has_stats"] = link->has_stats;
    object["network_rx_bytes"] = link->rx_bytes;
    object["network_tx_bytes"] = link->tx_bytes;
    object["network_rx_packets"] = link->rx_packets;
    object["network_tx_packets"] = link->tx_packets;
    object["network_rx_dropped"] = link->rx_dropped;
    object["network_tx_dropped"] = link->tx_dropped;
    object["network_rx_errors"] = link->rx_errors;
    object["network_tx_errors"] = link->tx_errors;
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
  std::string network_yaml =
      "network:\n"
      "  isolated: " +
      std::string(options.isolate_network ? "true" : "false") + "\n";
  if (options.network_condition_requested) {
    network_yaml +=
        "  default_condition:\n"
        "    delay_ms: " +
        std::to_string(options.network_condition.delay_ms) +
        "\n"
        "    jitter_ms: " +
        std::to_string(options.network_condition.jitter_ms) +
        "\n"
        "    loss_basis_points: " +
        std::to_string(options.network_condition.loss_basis_points) +
        "\n"
        "    duplicate_basis_points: " +
        std::to_string(options.network_condition.duplicate_basis_points) +
        "\n"
        "    limit_packets: " +
        std::to_string(options.network_condition.limit_packets) + "\n";
  }
  if (!options.node_network_conditions.empty()) {
    network_yaml += "  node_conditions:\n";
    for (const auto& [node_index, condition] :
         options.node_network_conditions) {
      network_yaml +=
          "    firo-" + std::to_string(node_index + 1U) +
          ":\n"
          "      delay_ms: " +
          std::to_string(condition.delay_ms) +
          "\n"
          "      jitter_ms: " +
          std::to_string(condition.jitter_ms) +
          "\n"
          "      loss_basis_points: " +
          std::to_string(condition.loss_basis_points) +
          "\n"
          "      duplicate_basis_points: " +
          std::to_string(condition.duplicate_basis_points) +
          "\n"
          "      limit_packets: " +
          std::to_string(condition.limit_packets) + "\n";
    }
  }

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
                + network_yaml +
                "workloads:\n"
                "  - type: block_generation\n"
                "    count: " +
                std::to_string(options.generate_blocks) +
                "\n"
                "    sync_timeout_sec: " +
                std::to_string(options.sync_timeout_sec) + "\n");

  boost::json::object resolved;
  resolved["run_id"] = options.run_id;
  resolved["chain"] = "firo";
  resolved["nodes"] = options.nodes;
  resolved["firod"] = options.firod.string();
  resolved["isolated_network"] = options.isolate_network;
  resolved["sync_timeout_sec"] = options.sync_timeout_sec;
  if (options.network_condition_requested) {
    resolved["default_network_condition"] =
        NetworkConditionJson(options.network_condition);
  }
  if (!options.node_network_conditions.empty()) {
    boost::json::array node_conditions;
    for (const auto& [node_index, condition] :
         options.node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      node_conditions.push_back(std::move(node_condition));
    }
    resolved["node_network_conditions"] = std::move(node_conditions);
  }
  WriteText(run_root / "resolved-scenario.json",
            boost::json::serialize(resolved) + "\n");
}

void LoadCleanupMetadata(const std::filesystem::path& run_root,
                         Options* options) {
  const std::filesystem::path resolved_path = run_root / "resolved-scenario.json";
  if (!std::filesystem::exists(resolved_path)) {
    return;
  }

  const boost::json::value value = boost::json::parse(ReadText(resolved_path));
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             resolved_path.string());
  }
  const boost::json::object& object = value.as_object();
  options->nodes = JsonOptionalUint32Field(object, "nodes", options->nodes);
  const boost::json::value* isolated = object.if_contains("isolated_network");
  if (isolated != nullptr) {
    if (!isolated->is_bool()) {
      throw std::runtime_error(
          "resolved scenario isolated_network is not a boolean");
    }
    options->isolate_network = isolated->as_bool();
  }
  if (options->nodes < 1 || options->nodes > 2) {
    throw std::runtime_error(
        "cleanup currently supports resolved node counts in 1..2");
  }
}

void CleanupRun(Options options) {
  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  LoadCleanupMetadata(run_root, &options);

  for (uint32_t i = 0; i < options.nodes; ++i) {
    DeleteNodeVethNetwork(MakeNodeVethConfig(options, i));
  }
  Cgroup::RemoveRun(options.run_id);

  std::cout << "cleanup_run=" << options.run_id << "\n"
            << "nodes=" << options.nodes << "\n"
            << "run_dir=" << run_root << "\n";
}

void StartNodes(const Options& options, const std::filesystem::path& run_root,
                const std::filesystem::path& events_path,
                const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  if (options.isolate_network && options.nodes > 1 &&
      !HostIpv4ForwardingEnabled()) {
    throw std::runtime_error(
        "isolated multi-node Firo runs require IPv4 forwarding in the parent "
        "network namespace");
  }
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
      config.connect_peers.push_back(FiroPeerHost(options, 0) + ":18168");
    }

    NodeRuntime runtime;
    try {
      runtime.config = config;
      if (options.isolate_network) {
        runtime.network_namespace = NetworkNamespace::Create();
        runtime.network = MakeNodeVethConfig(options, i);
        SetupNodeVethNetwork(runtime.network_namespace->fd(),
                             *runtime.network);
        runtime.config.rpc_host = runtime.network->node_address;
        runtime.config.rpc_bind = runtime.network->node_address;
        runtime.config.rpc_allow_ips = {runtime.network->host_address};
        runtime.config.p2p_bind = runtime.network->node_address;
        WriteEvent(events_path, options.run_id, node_id, "network_ready",
                   "node_ip=" + runtime.network->node_address +
                       " host_if=" + runtime.network->host_name +
                       " peer_if=" + runtime.network->peer_name);
      }

      runtime.cgroup = Cgroup::Create(options.run_id, node_id);
      runtime.cgroup->SetMemoryHigh(1536ULL * 1024ULL * 1024ULL);
      runtime.cgroup->SetMemoryMax(2ULL * 1024ULL * 1024ULL * 1024ULL);
      runtime.cgroup->SetCpuMax(std::nullopt, 100000);
      runtime.cgroup->SetPidsMax(256);

      ProcessSpec process = driver.RenderProcess(runtime.config);
      if (runtime.network_namespace) {
        process.network_namespace_fd = runtime.network_namespace->fd();
      }
      runtime.process = ChildProcess::Spawn(process, runtime.cgroup->path());
      BSIM_LOG(info) << "started " << node_id
                     << " pid=" << runtime.process.pid();
      WriteEvent(events_path, options.run_id, node_id, "process_started",
                 "pid=" + std::to_string(runtime.process.pid()));
      nodes.push_back(std::move(runtime));
    } catch (...) {
      runtime.process.Kill();
      if (runtime.cgroup) {
        try {
          runtime.cgroup->KillAll();
          runtime.cgroup->Remove();
        } catch (const std::exception&) {
        }
      }
      if (runtime.network) {
        DeleteNodeVethNetwork(*runtime.network);
      }
      throw;
    }
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
    if (node.network) {
      DeleteNodeVethNetwork(*node.network);
      WriteEvent(events_path, options.run_id, node.config.id,
                 "network_removed");
    }
    if (node.network_namespace) {
      node.network_namespace->Stop();
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
  if (options.probe_network_condition) {
    std::cout << NetworkConditionProbeJson() << "\n";
    return 0;
  }
  if (options.probe_address) {
    std::cout << AddressProbeJson() << "\n";
    return 0;
  }
  if (options.probe_route) {
    std::cout << RouteProbeJson() << "\n";
    return 0;
  }
  if (options.probe_qdisc) {
    std::cout << QdiscProbeJson() << "\n";
    return 0;
  }
  if (options.probe_qdisc_mutation) {
    std::cout << QdiscMutationProbeJson() << "\n";
    return 0;
  }
  if (options.cleanup_run) {
    CleanupRun(options);
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
      const std::vector<LinkInfo> links = ListNetworkLinks();
      FiroMetrics chain = driver.ReadMetrics(node.config);
      CgroupMetrics cg = node.cgroup->ReadMetrics();
      const LinkInfo* link = node.network
                                 ? FindLinkByName(links,
                                                  node.network->host_name)
                                 : nullptr;
      AppendLine(metrics_path,
                 MetricsJson(options.run_id, node.config.id, chain, &cg, link));
    }

    if (options.generate_blocks > 0) {
      const uint64_t start_height =
          driver.ReadMetrics(nodes.front().config).height;
      std::vector<std::string> hashes = driver.GenerateBlocks(
          nodes.front().config, options.generate_blocks, kDefaultRewardAddress);
      WriteEvent(events_path, options.run_id, nodes.front().config.id,
                 "generated_blocks", std::to_string(hashes.size()));
      const uint64_t target_height =
          start_height + static_cast<uint64_t>(hashes.size());
      for (auto& node : nodes) {
        driver.WaitForHeight(node.config, target_height,
                             std::chrono::seconds(options.sync_timeout_sec));
        WriteEvent(events_path, options.run_id, node.config.id,
                   "height_reached", std::to_string(target_height));
      }
    }

    for (auto& node : nodes) {
      const std::vector<LinkInfo> links = ListNetworkLinks();
      FiroMetrics chain = driver.ReadMetrics(node.config);
      CgroupMetrics cg = node.cgroup->ReadMetrics();
      const LinkInfo* link = node.network
                                 ? FindLinkByName(links,
                                                  node.network->host_name)
                                 : nullptr;
      AppendLine(metrics_path,
                 MetricsJson(options.run_id, node.config.id, chain, &cg, link));
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
