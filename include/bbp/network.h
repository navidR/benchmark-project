#pragma once

#include <sys/types.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

enum class QdiscKind {
  kUnknown,
  kPfifo,
  kNetem,
  kTbf,
  kPrio,
  kClsact,
  kTbfNetem,
};

std::string_view QdiscKindName(QdiscKind kind);

enum class TcFilterKind {
  kUnknown,
  kFlower,
};

std::string_view TcFilterKindName(TcFilterKind kind);

struct LinkInfo {
  int index = 0;
  std::string name;
  bool up = false;
  bool has_stats = false;
  std::uint64_t rx_bytes = 0;
  std::uint64_t tx_bytes = 0;
  std::uint64_t rx_packets = 0;
  std::uint64_t tx_packets = 0;
  std::uint64_t rx_dropped = 0;
  std::uint64_t tx_dropped = 0;
  std::uint64_t rx_errors = 0;
  std::uint64_t tx_errors = 0;
};

struct AddressInfo {
  int if_index = 0;
  std::string if_name;
  std::string address;
  std::uint8_t prefix_len = 0;
};

struct RouteInfo {
  std::string destination;
  std::uint8_t prefix_len = 0;
  int oif_index = 0;
  std::string oif_name;
  std::string gateway;
  std::uint32_t table = 0;
  std::uint8_t protocol = 0;
  std::uint8_t scope = 0;
  std::uint8_t type = 0;
};

struct QdiscInfo {
  int if_index = 0;
  std::string if_name;
  QdiscKind kind = QdiscKind::kUnknown;
  std::string kernel_kind;
  std::uint32_t handle = 0;
  std::uint32_t parent = 0;
  std::uint32_t info = 0;
  bool has_stats = false;
  std::uint64_t bytes = 0;
  std::uint64_t packets = 0;
  std::uint32_t drops = 0;
  std::uint32_t overlimits = 0;
  std::uint32_t qlen = 0;
  std::uint32_t backlog = 0;
  std::uint32_t requeues = 0;
  bool has_netem_options = false;
  std::uint32_t netem_latency_us = 0;
  std::uint32_t netem_jitter_us = 0;
  std::uint32_t netem_loss = 0;
  std::uint32_t netem_duplicate = 0;
  std::uint32_t netem_corrupt = 0;
  std::uint32_t netem_reorder = 0;
  std::uint32_t netem_limit_packets = 0;
  bool has_tbf_options = false;
  std::uint64_t tbf_rate_bytes_per_sec = 0;
  std::uint32_t tbf_limit_bytes = 0;
  std::uint32_t tbf_buffer_ticks = 0;
  std::uint32_t tbf_mtu_ticks = 0;
  bool has_prio_options = false;
  std::uint32_t prio_bands = 0;
};

struct TcFilterInfo {
  int if_index = 0;
  std::string if_name;
  TcFilterKind kind = TcFilterKind::kUnknown;
  std::string kernel_kind;
  std::uint32_t handle = 0;
  std::uint32_t parent = 0;
  std::uint32_t priority = 0;
  std::uint16_t protocol = 0;
  bool egress = false;
  bool ingress = false;
  bool has_eth_type = false;
  std::uint16_t eth_type = 0;
  bool has_ip_proto = false;
  std::uint8_t ip_proto = 0;
  bool has_ipv4_src = false;
  std::string ipv4_src;
  bool has_ipv4_src_mask = false;
  std::string ipv4_src_mask;
  bool has_ipv4_dst = false;
  std::string ipv4_dst;
  bool has_ipv4_dst_mask = false;
  std::string ipv4_dst_mask;
  bool has_tcp_dst = false;
  std::uint16_t tcp_dst = 0;
  bool has_tcp_dst_mask = false;
  std::uint16_t tcp_dst_mask = 0;
  bool has_class_id = false;
  std::uint32_t class_id = 0;
  bool has_drop_action = false;
  bool has_stats = false;
  std::uint64_t match_bytes = 0;
  std::uint64_t match_packets = 0;
  std::uint64_t drop_packets = 0;
};

struct TcFilterStatsSummary {
  std::uint64_t policy_count = 0;
  std::uint64_t policies_with_stats = 0;
  std::uint64_t match_bytes = 0;
  std::uint64_t match_packets = 0;
  std::uint64_t drop_packets = 0;
};

struct NetworkCondition {
  std::uint32_t bandwidth_mbps = 0;
  std::uint32_t delay_ms = 0;
  std::uint32_t jitter_ms = 0;
  std::uint32_t loss_basis_points = 0;
  std::uint32_t duplicate_basis_points = 0;
  std::uint32_t corrupt_basis_points = 0;
  std::uint32_t reorder_basis_points = 0;
  std::uint32_t limit_packets = 1000;

  bool operator==(const NetworkCondition&) const = default;
};

struct DirectionalNetworkPolicy {
  // Band zero is the unclassified path. Simulator-owned directional policies
  // use bands 1..15, which map to prio classes 2..16.
  std::uint32_t band = 0;
  std::string destination_address;
  NetworkCondition condition;

  bool operator==(const DirectionalNetworkPolicy&) const = default;
};

struct DirectionalNetworkPolicyCounter {
  std::uint32_t band = 0;
  std::string destination_address;
  TcFilterInfo filter;
  std::vector<QdiscInfo> qdiscs;
  std::uint64_t qdisc_bytes = 0;
  std::uint64_t qdisc_packets = 0;
  std::uint64_t qdisc_drops = 0;
  std::uint64_t qdisc_overlimits = 0;
  std::uint64_t qdisc_qlen = 0;
  std::uint64_t qdisc_backlog = 0;
  std::uint64_t qdisc_requeues = 0;
};

struct DirectionalNetworkPolicyStats {
  std::uint64_t policy_count = 0;
  std::uint64_t policies_with_filter_stats = 0;
  std::uint64_t filter_match_bytes = 0;
  std::uint64_t filter_match_packets = 0;
  std::uint64_t qdisc_count = 0;
  std::uint64_t qdiscs_with_stats = 0;
  std::uint64_t qdisc_bytes = 0;
  std::uint64_t qdisc_packets = 0;
  std::uint64_t qdisc_drops = 0;
  std::uint64_t qdisc_overlimits = 0;
  std::uint64_t qdisc_qlen = 0;
  std::uint64_t qdisc_backlog = 0;
  std::uint64_t qdisc_requeues = 0;
  std::vector<DirectionalNetworkPolicyCounter> policies;
};

void ValidateNetworkCondition(const NetworkCondition& condition);
void ValidateIpv4Address(std::string_view address, std::string_view field_name);

class NetworkNamespace {
 public:
  static NetworkNamespace Create();

  NetworkNamespace() = default;
  NetworkNamespace(const NetworkNamespace&) = delete;
  NetworkNamespace& operator=(const NetworkNamespace&) = delete;
  NetworkNamespace(NetworkNamespace&& other) noexcept;
  NetworkNamespace& operator=(NetworkNamespace&& other) noexcept;
  ~NetworkNamespace();

  int fd() const { return fd_; }
  pid_t helper_pid() const { return helper_pid_; }
  void Stop();

 private:
  NetworkNamespace(pid_t helper_pid, int fd)
      : helper_pid_(helper_pid), fd_(fd) {}

  pid_t helper_pid_ = -1;
  int fd_ = -1;
};

struct NodeVethConfig {
  std::string host_name;
  std::string peer_name;
  std::string host_address;
  std::string node_address;
  std::uint8_t prefix_len = 30;
  bool apply_condition = false;
  NetworkCondition condition;
};

#ifdef BBP_ENABLE_TEST_HOOKS
enum class NetlinkFailurePhase {
  kBeforeSend,
  kAfterSend,
};

struct NetlinkFailurePoint {
  std::uint64_t request_number = 0;
  NetlinkFailurePhase phase = NetlinkFailurePhase::kBeforeSend;
};

class ScopedNetlinkFailurePlan {
 public:
  explicit ScopedNetlinkFailurePlan(
      std::vector<NetlinkFailurePoint> failure_points);
  ScopedNetlinkFailurePlan(const ScopedNetlinkFailurePlan&) = delete;
  ScopedNetlinkFailurePlan& operator=(const ScopedNetlinkFailurePlan&) = delete;
  ~ScopedNetlinkFailurePlan();

 private:
  bool installed_ = false;
};

void ValidateNetlinkAcknowledgementForTest(
    const std::vector<std::uint8_t>& reply, std::uint32_t sequence,
    std::uint32_t port_id);
#endif

struct NetworkNamespaceProbe {
  pid_t helper_pid = -1;
  std::vector<LinkInfo> parent_links;
  std::vector<LinkInfo> namespace_links;
};

struct VethProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::vector<LinkInfo> parent_before;
  std::vector<LinkInfo> parent_after_create;
  std::vector<LinkInfo> parent_after_move;
  std::vector<LinkInfo> namespace_after_move;
  std::vector<LinkInfo> parent_after_delete;
};

struct AddressProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::string assigned_address;
  std::uint8_t assigned_prefix_len = 0;
  std::vector<LinkInfo> parent_after_move;
  std::vector<LinkInfo> namespace_links_after_address;
  std::vector<AddressInfo> namespace_addresses;
  std::vector<AddressInfo> namespace_addresses_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct RouteProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::string assigned_address;
  std::uint8_t assigned_prefix_len = 0;
  std::string route_destination;
  std::uint8_t route_prefix_len = 0;
  std::vector<LinkInfo> namespace_links_after_route;
  std::vector<AddressInfo> namespace_addresses;
  std::vector<RouteInfo> namespace_routes;
  std::vector<AddressInfo> namespace_addresses_after_delete;
  std::vector<RouteInfo> namespace_routes_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct QdiscProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::vector<LinkInfo> namespace_links;
  std::vector<QdiscInfo> namespace_qdiscs;
  std::vector<LinkInfo> parent_after_delete;
};

struct QdiscMutationProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::uint32_t pfifo_limit_packets = 0;
  std::vector<QdiscInfo> namespace_qdiscs_before;
  std::vector<QdiscInfo> namespace_qdiscs_after_replace;
  std::vector<QdiscInfo> namespace_qdiscs_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct NetworkConditionProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  NetworkCondition condition;
  std::vector<QdiscInfo> namespace_qdiscs_before;
  std::vector<QdiscInfo> namespace_qdiscs_after_apply;
  std::vector<QdiscInfo> namespace_qdiscs_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct NetworkConditionUpdateProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  NetworkCondition initial_condition;
  NetworkCondition updated_condition;
  std::vector<QdiscInfo> parent_qdiscs_after_initial;
  std::vector<QdiscInfo> parent_qdiscs_after_update;
  std::vector<QdiscInfo> parent_qdiscs_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct BandwidthLimitProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  NetworkCondition condition;
  std::vector<QdiscInfo> namespace_qdiscs_before;
  std::vector<QdiscInfo> namespace_qdiscs_after_apply;
  std::vector<QdiscInfo> namespace_qdiscs_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct DropFilterProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::string dst_address;
  std::uint16_t dst_port = 0;
  std::uint32_t handle = 0;
  std::vector<TcFilterInfo> parent_filters_before;
  std::vector<TcFilterInfo> parent_filters_after_apply;
  std::vector<TcFilterInfo> parent_filters_after_delete;
  std::vector<LinkInfo> parent_after_delete;
};

struct DirectionalNetworkPolicyProbe {
  pid_t helper_pid = -1;
  std::string host_name;
  std::string peer_name;
  std::vector<DirectionalNetworkPolicy> policies;
  std::vector<QdiscInfo> namespace_qdiscs_before;
  std::vector<QdiscInfo> namespace_qdiscs_after_apply;
  std::vector<TcFilterInfo> namespace_filters_after_apply;
  std::vector<QdiscInfo> namespace_qdiscs_after_delete;
  std::vector<TcFilterInfo> namespace_filters_after_delete;
  bool non_owned_root_preserved = false;
  std::vector<LinkInfo> parent_after_delete;
};

std::vector<LinkInfo> ListNetworkLinks();
std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd);
std::vector<AddressInfo> ListIpv4Addresses();
std::vector<AddressInfo> ListIpv4AddressesInNamespace(int netns_fd);
std::vector<RouteInfo> ListIpv4Routes();
std::vector<RouteInfo> ListIpv4RoutesInNamespace(int netns_fd);
std::vector<QdiscInfo> ListQdiscs();
std::vector<QdiscInfo> ListQdiscsInNamespace(int netns_fd);
std::vector<TcFilterInfo> ListTcFilters();
std::vector<TcFilterInfo> ListTcFiltersForInterface(const std::string& if_name);
std::vector<TcFilterInfo> ListTcFiltersForInterfaceParentInNamespace(
    int netns_fd, const std::string& if_name, std::uint32_t parent);
bool QdiscMatchesNetworkCondition(const QdiscInfo& qdisc,
                                  const NetworkCondition& condition);
bool QdiscsMatchNetworkCondition(const std::vector<QdiscInfo>& qdiscs,
                                 const std::string& if_name,
                                 const NetworkCondition& condition,
                                 QdiscInfo* summary);
bool TcFilterMatchesEgressIpv4TcpDrop(const TcFilterInfo& filter,
                                      const std::string& if_name,
                                      const std::string& dst_address,
                                      std::uint16_t dst_port,
                                      std::uint32_t handle);
bool TcFilterIsEgressIpv4TcpDropPolicy(const TcFilterInfo& filter,
                                       const std::string& if_name);
TcFilterStatsSummary SummarizeEgressIpv4TcpDropPolicies(
    const std::vector<TcFilterInfo>& filters, const std::string& if_name);
bool DirectionalNetworkPoliciesMatch(
    const std::vector<QdiscInfo>& qdiscs,
    const std::vector<TcFilterInfo>& filters, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies);
DirectionalNetworkPolicyStats SummarizeDirectionalNetworkPolicyStats(
    const std::vector<QdiscInfo>& qdiscs,
    const std::vector<TcFilterInfo>& filters, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies);
DirectionalNetworkPolicyStats ReadDirectionalNetworkPolicyStatsInNamespace(
    int netns_fd, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies);
bool TcFilterMatchesEgressIpv4TcpDrop(const TcFilterInfo& filter,
                                      const std::string& if_name,
                                      const std::string& src_address,
                                      const std::string& dst_address,
                                      std::uint16_t dst_port,
                                      std::uint32_t handle);
NetworkNamespaceProbe ProbeIsolatedNetworkNamespace();
void CreateVethPair(const std::string& host_name, const std::string& peer_name);
void DeleteLink(const std::string& name);
void MoveLinkToNamespace(const std::string& name, int netns_fd);
void SetLinkUp(const std::string& name, bool up);
void AddIpv4Address(const std::string& if_name, const std::string& address,
                    std::uint8_t prefix_len);
void DeleteIpv4Address(const std::string& if_name, const std::string& address,
                       std::uint8_t prefix_len);
void AddIpv4Route(const std::string& if_name, const std::string& destination,
                  std::uint8_t prefix_len, const std::string& gateway = "");
void DeleteIpv4Route(const std::string& if_name, const std::string& destination,
                     std::uint8_t prefix_len, const std::string& gateway = "");
void ReplaceRootPfifoQdisc(const std::string& if_name,
                           std::uint32_t limit_packets);
void ReplaceRootNetemQdisc(const std::string& if_name,
                           const NetworkCondition& condition);
void ReplaceRootTbfQdisc(const std::string& if_name,
                         const NetworkCondition& condition);
void ReplaceNetworkConditionQdisc(const std::string& if_name,
                                  const NetworkCondition& condition);
void DeleteRootQdisc(const std::string& if_name);
void ReplaceEgressIpv4TcpDropFilter(const std::string& if_name,
                                    const std::string& dst_address,
                                    std::uint16_t dst_port,
                                    std::uint32_t handle);
void ReplaceEgressIpv4TcpDropFilter(const std::string& if_name,
                                    const std::string& src_address,
                                    const std::string& dst_address,
                                    std::uint16_t dst_port,
                                    std::uint32_t handle);
void DeleteEgressIpv4TcpDropFilter(const std::string& if_name,
                                   std::uint32_t handle);
void UpdateDirectionalNetworkPoliciesInNamespace(
    int netns_fd, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& previous,
    const std::vector<DirectionalNetworkPolicy>& desired);
void SetupNodeVethNetwork(int netns_fd, const NodeVethConfig& config);
void DeleteNodeVethNetwork(const NodeVethConfig& config);
VethProbe ProbeVethPair();
AddressProbe ProbeIpv4AddressAssignment();
RouteProbe ProbeIpv4RouteAssignment();
QdiscProbe ProbeQdiscDump();
QdiscMutationProbe ProbeQdiscMutation();
NetworkConditionProbe ProbeNetworkCondition();
NetworkConditionProbe ProbeCombinedNetworkCondition();
NetworkConditionUpdateProbe ProbeNetworkConditionUpdate();
BandwidthLimitProbe ProbeBandwidthLimit();
DropFilterProbe ProbeDropFilter();
DirectionalNetworkPolicyProbe ProbeDirectionalNetworkPolicies();

}  // namespace bbp
