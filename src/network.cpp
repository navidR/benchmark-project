#include "bbp/network.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <libmnl/libmnl.h>
#include <linux/gen_stats.h>
#include <linux/if_ether.h>
#include <linux/if_link.h>
#include <linux/pkt_cls.h>
#include <linux/pkt_sched.h>
#include <linux/rtnetlink.h>
#include <linux/tc_act/tc_gact.h>
#include <linux/veth.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sched.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <exception>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace bbp {

std::string_view QdiscKindName(QdiscKind kind) {
  switch (kind) {
    case QdiscKind::kUnknown:
      return "unknown";
    case QdiscKind::kPfifo:
      return "pfifo";
    case QdiscKind::kNetem:
      return "netem";
    case QdiscKind::kTbf:
      return "tbf";
    case QdiscKind::kPrio:
      return "prio";
    case QdiscKind::kClsact:
      return "clsact";
    case QdiscKind::kTbfNetem:
      return "tbf+netem";
  }
  return "unknown";
}

std::string_view TcFilterKindName(TcFilterKind kind) {
  switch (kind) {
    case TcFilterKind::kUnknown:
      return "unknown";
    case TcFilterKind::kFlower:
      return "flower";
  }
  return "unknown";
}

namespace {

constexpr std::uint32_t kRootQdiscHandle = TC_H_MAKE(1U << 16, 0U);
constexpr std::uint32_t kTbfChildClassId = TC_H_MAKE(1U << 16, 1U);
constexpr std::uint32_t kChildNetemQdiscHandle = TC_H_MAKE(2U << 16, 0U);
constexpr std::uint32_t kClsactQdiscHandle = TC_H_MAKE(TC_H_CLSACT, 0U);
constexpr std::uint32_t kClsactEgressParent =
    TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
constexpr std::uint32_t kClsactIngressParent =
    TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_INGRESS);
constexpr std::uint32_t kDropFilterPriority = 10U;
constexpr std::uint32_t kDirectionalFilterPriority = 11U;
constexpr std::uint32_t kDirectionalRootHandle = TC_H_MAKE(0xBB00U << 16U, 0U);
constexpr std::uint32_t kDirectionalTbfHandleBase = 0xBB10U;
constexpr std::uint32_t kDirectionalNetemHandleBase = 0xBB20U;
constexpr std::uint32_t kDirectionalFilterHandleBase = 0xBB000000U;
constexpr std::uint32_t kDirectionalBandCount = TCQ_PRIO_BANDS;
constexpr std::uint32_t kMaximumDirectionalPolicies =
    kDirectionalBandCount - 1U;

QdiscKind ParseQdiscKind(std::string_view kind) {
  if (kind == "pfifo") {
    return QdiscKind::kPfifo;
  }
  if (kind == "netem") {
    return QdiscKind::kNetem;
  }
  if (kind == "tbf") {
    return QdiscKind::kTbf;
  }
  if (kind == "prio") {
    return QdiscKind::kPrio;
  }
  if (kind == "clsact") {
    return QdiscKind::kClsact;
  }
  return QdiscKind::kUnknown;
}

TcFilterKind ParseTcFilterKind(std::string_view kind) {
  if (kind == "flower") {
    return TcFilterKind::kFlower;
  }
  return TcFilterKind::kUnknown;
}

class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept { *this = std::move(other); }
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
  }

  ~UniqueFd() { Reset(); }

  int get() const { return fd_; }

  int Release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

  void Reset() {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_ = -1;
};

class MnlSocket {
 public:
  explicit MnlSocket(int bus) : socket_(mnl_socket_open(bus)) {
    if (socket_ == nullptr) {
      throw std::runtime_error(std::string("mnl_socket_open failed: ") +
                               std::strerror(errno));
    }
  }

  MnlSocket(const MnlSocket&) = delete;
  MnlSocket& operator=(const MnlSocket&) = delete;

  ~MnlSocket() {
    if (socket_ != nullptr) {
      mnl_socket_close(socket_);
    }
  }

  void Bind(unsigned int groups, pid_t pid) {
    if (mnl_socket_bind(socket_, groups, pid) < 0) {
      throw std::runtime_error(std::string("mnl_socket_bind failed: ") +
                               std::strerror(errno));
    }
  }

  unsigned int PortId() const { return mnl_socket_get_portid(socket_); }

  void SetReceiveTimeout(std::chrono::milliseconds timeout) {
    const auto whole_seconds = timeout.count() / 1000;
    if (timeout.count() <= 0 ||
        whole_seconds >
            static_cast<decltype(timeout.count())>(
                std::numeric_limits<decltype(timeval::tv_sec)>::max())) {
      throw std::runtime_error("invalid netlink receive timeout");
    }
    timeval value{};
    value.tv_sec = static_cast<decltype(value.tv_sec)>(timeout.count() / 1000);
    value.tv_usec =
        static_cast<decltype(value.tv_usec)>((timeout.count() % 1000) * 1000);
    if (setsockopt(mnl_socket_get_fd(socket_), SOL_SOCKET, SO_RCVTIMEO, &value,
                   sizeof(value)) != 0) {
      throw std::runtime_error(std::string("setsockopt(SO_RCVTIMEO) failed: ") +
                               std::strerror(errno));
    }
  }

  void Send(const void* data, size_t size) const {
    const ssize_t sent = mnl_socket_sendto(socket_, data, size);
    if (sent < 0) {
      throw std::runtime_error(std::string("mnl_socket_sendto failed: ") +
                               std::strerror(errno));
    }
    if (static_cast<size_t>(sent) != size) {
      throw std::runtime_error(
          "mnl_socket_sendto wrote a partial netlink message");
    }
  }

  ssize_t Receive(void* data, size_t size) const {
    const ssize_t received = mnl_socket_recvfrom(socket_, data, size);
    if (received < 0) {
      throw std::runtime_error(std::string("mnl_socket_recvfrom failed: ") +
                               std::strerror(errno));
    }
    return received;
  }

 private:
  mnl_socket* socket_ = nullptr;
};

UniqueFd OpenFileDescriptor(const std::string& path);

class ScopedNetworkNamespace {
 public:
  explicit ScopedNetworkNamespace(int netns_fd)
      : parent_(OpenFileDescriptor("/proc/self/ns/net")) {
    if (setns(netns_fd, CLONE_NEWNET) != 0) {
      throw std::runtime_error(std::string("setns(child netns) failed: ") +
                               std::strerror(errno));
    }
  }

  ScopedNetworkNamespace(const ScopedNetworkNamespace&) = delete;
  ScopedNetworkNamespace& operator=(const ScopedNetworkNamespace&) = delete;

  ~ScopedNetworkNamespace() {
    if (parent_.get() >= 0) {
      setns(parent_.get(), CLONE_NEWNET);
    }
  }

 private:
  UniqueFd parent_;
};

UniqueFd OpenFileDescriptor(const std::string& path) {
  UniqueFd fd(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd.get() < 0) {
    throw std::runtime_error("open failed for " + path + ": " +
                             std::strerror(errno));
  }
  return fd;
}

uint32_t NextSequence() {
  static std::atomic<uint32_t> sequence{1U};
  return sequence.fetch_add(1U);
}

void RequireInterfaceName(const std::string& name) {
  if (name.empty() || name.size() >= IFNAMSIZ) {
    throw std::runtime_error("interface name must be 1.." +
                             std::to_string(IFNAMSIZ - 1) + " bytes");
  }
  for (const unsigned char c : name) {
    const bool ok = std::isalnum(c) != 0 || c == '_' || c == '-' || c == '.';
    if (!ok) {
      throw std::runtime_error("interface name contains unsafe character: " +
                               name);
    }
  }
}

bool HasLinkNamed(const std::vector<LinkInfo>& links, const std::string& name) {
  for (const LinkInfo& link : links) {
    if (link.name == name) {
      return true;
    }
  }
  return false;
}

void StoreLinkStats64(const rtnl_link_stats64& stats, LinkInfo* link) {
  link->has_stats = true;
  link->rx_bytes = stats.rx_bytes;
  link->tx_bytes = stats.tx_bytes;
  link->rx_packets = stats.rx_packets;
  link->tx_packets = stats.tx_packets;
  link->rx_dropped = stats.rx_dropped;
  link->tx_dropped = stats.tx_dropped;
  link->rx_errors = stats.rx_errors;
  link->tx_errors = stats.tx_errors;
}

void StoreLinkStats32(const rtnl_link_stats& stats, LinkInfo* link) {
  link->has_stats = true;
  link->rx_bytes = stats.rx_bytes;
  link->tx_bytes = stats.tx_bytes;
  link->rx_packets = stats.rx_packets;
  link->tx_packets = stats.tx_packets;
  link->rx_dropped = stats.rx_dropped;
  link->tx_dropped = stats.tx_dropped;
  link->rx_errors = stats.rx_errors;
  link->tx_errors = stats.tx_errors;
}

bool HasIpv4Address(const std::vector<AddressInfo>& addresses,
                    const std::string& if_name, const std::string& address,
                    std::uint8_t prefix_len) {
  for (const AddressInfo& entry : addresses) {
    if (entry.if_name == if_name && entry.address == address &&
        entry.prefix_len == prefix_len) {
      return true;
    }
  }
  return false;
}

bool HasIpv4Route(const std::vector<RouteInfo>& routes,
                  const std::string& oif_name, const std::string& destination,
                  std::uint8_t prefix_len) {
  for (const RouteInfo& route : routes) {
    if (route.oif_name == oif_name && route.destination == destination &&
        route.prefix_len == prefix_len) {
      return true;
    }
  }
  return false;
}

bool HasQdiscForInterface(const std::vector<QdiscInfo>& qdiscs,
                          const std::string& if_name) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == if_name && !qdisc.kernel_kind.empty()) {
      return true;
    }
  }
  return false;
}

bool HasQdiscKindForInterface(const std::vector<QdiscInfo>& qdiscs,
                              const std::string& if_name, QdiscKind kind) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == if_name && qdisc.kind == kind) {
      return true;
    }
  }
  return false;
}

const QdiscInfo* FindQdiscForInterface(const std::vector<QdiscInfo>& qdiscs,
                                       const std::string& if_name,
                                       QdiscKind kind) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == if_name && qdisc.kind == kind) {
      return &qdisc;
    }
  }
  return nullptr;
}

const QdiscInfo* FindQdiscForInterfaceParent(
    const std::vector<QdiscInfo>& qdiscs, const std::string& if_name,
    QdiscKind kind, std::uint32_t parent) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == if_name && qdisc.kind == kind &&
        qdisc.parent == parent) {
      return &qdisc;
    }
  }
  return nullptr;
}

bool HasNetemCondition(const NetworkCondition& condition) {
  return condition.delay_ms != 0U || condition.jitter_ms != 0U ||
         condition.loss_basis_points != 0U ||
         condition.duplicate_basis_points != 0U ||
         condition.corrupt_basis_points != 0U ||
         condition.reorder_basis_points != 0U;
}

bool HasBandwidthCondition(const NetworkCondition& condition) {
  return condition.bandwidth_mbps != 0U;
}

in_addr ParseIpv4Address(const std::string& address,
                         std::string_view field_name);

std::uint32_t DirectionalClassId(std::uint32_t band) {
  return TC_H_MAKE(TC_H_MAJ(kDirectionalRootHandle), band + 1U);
}

std::uint32_t DirectionalTbfHandle(std::uint32_t band) {
  return TC_H_MAKE((kDirectionalTbfHandleBase + band) << 16U, 0U);
}

std::uint32_t DirectionalNetemHandle(std::uint32_t band) {
  return TC_H_MAKE((kDirectionalNetemHandleBase + band) << 16U, 0U);
}

std::uint32_t DirectionalFilterHandle(std::uint32_t band) {
  return kDirectionalFilterHandleBase + band;
}

void ValidateDirectionalNetworkPolicies(
    const std::vector<DirectionalNetworkPolicy>& policies) {
  if (policies.size() > kMaximumDirectionalPolicies) {
    throw std::runtime_error("directional network policy count exceeds 15");
  }
  std::array<bool, kDirectionalBandCount> bands{};
  std::vector<std::string> destinations;
  destinations.reserve(policies.size());
  for (const DirectionalNetworkPolicy& policy : policies) {
    if (policy.band == 0U || policy.band > kMaximumDirectionalPolicies) {
      throw std::runtime_error("directional network policy band must be 1..15");
    }
    if (bands[policy.band]) {
      throw std::runtime_error("duplicate directional network policy band");
    }
    bands[policy.band] = true;
    ParseIpv4Address(policy.destination_address, "policy destination");
    if (std::find(destinations.begin(), destinations.end(),
                  policy.destination_address) != destinations.end()) {
      throw std::runtime_error(
          "duplicate directional network policy destination");
    }
    destinations.push_back(policy.destination_address);
    ValidateNetworkCondition(policy.condition);
  }
}

std::string DescribeTcFilters(const std::vector<TcFilterInfo>& filters) {
  std::ostringstream output;
  output << "count=" << filters.size();
  for (const TcFilterInfo& filter : filters) {
    const std::string_view kind = filter.kernel_kind.empty()
                                      ? TcFilterKindName(filter.kind)
                                      : std::string_view(filter.kernel_kind);
    output << " [if=" << filter.if_name << " kind=" << kind
           << " handle=" << filter.handle << " parent=" << filter.parent
           << " priority=" << filter.priority << " protocol=" << filter.protocol
           << " egress=" << (filter.egress ? "true" : "false")
           << " eth=" << (filter.has_eth_type ? filter.eth_type : 0U)
           << " ip_proto="
           << (filter.has_ip_proto ? static_cast<unsigned int>(filter.ip_proto)
                                   : 0U)
           << " ipv4_src="
           << (filter.has_ipv4_src ? filter.ipv4_src : std::string(""))
           << " ipv4_dst="
           << (filter.has_ipv4_dst ? filter.ipv4_dst : std::string(""))
           << " tcp_dst=" << (filter.has_tcp_dst ? filter.tcp_dst : 0U)
           << " drop=" << (filter.has_drop_action ? "true" : "false") << "]";
  }
  return output.str();
}

std::uint32_t NetemProbability(std::uint32_t basis_points) {
  constexpr std::uint64_t kScale = 0xFFFFFFFFULL;
  constexpr std::uint64_t kBasisPointsPerWhole = 10000ULL;
  return static_cast<std::uint32_t>(
      (static_cast<std::uint64_t>(basis_points) * kScale) /
      kBasisPointsPerWhole);
}

std::uint64_t TbfRateBytesPerSecond(const NetworkCondition& condition) {
  constexpr std::uint64_t kBytesPerMegabit = 1000000ULL / 8ULL;
  return static_cast<std::uint64_t>(condition.bandwidth_mbps) *
         kBytesPerMegabit;
}

std::string Ipv4ToString(const void* payload) {
  char text[INET_ADDRSTRLEN] = {};
  if (inet_ntop(AF_INET, payload, text, sizeof(text)) == nullptr) {
    throw std::runtime_error(std::string("inet_ntop failed: ") +
                             std::strerror(errno));
  }
  return text;
}

in_addr ParseIpv4Address(const std::string& address,
                         std::string_view field_name) {
  in_addr ipv4_address{};
  if (inet_pton(AF_INET, address.c_str(), &ipv4_address) != 1) {
    throw std::runtime_error("invalid IPv4 " + std::string(field_name) + ": " +
                             address);
  }
  return ipv4_address;
}

std::uint32_t SaturatingUint32(std::uint64_t value) {
  constexpr std::uint64_t kMaxUint32 =
      std::numeric_limits<std::uint32_t>::max();
  if (value > kMaxUint32) {
    return std::numeric_limits<std::uint32_t>::max();
  }
  return static_cast<std::uint32_t>(value);
}

std::uint32_t TbfBurstBytes(const NetworkCondition& condition) {
  constexpr std::uint64_t kMinimumBurstBytes = 1500ULL;
  const std::uint64_t ten_ms_burst = TbfRateBytesPerSecond(condition) / 100ULL;
  const std::uint64_t burst =
      ten_ms_burst > kMinimumBurstBytes ? ten_ms_burst : kMinimumBurstBytes;
  return SaturatingUint32(burst);
}

std::uint32_t TbfLimitBytes(const NetworkCondition& condition) {
  constexpr std::uint64_t kMinimumLimitBytes = 64ULL * 1024ULL;
  const std::uint64_t rate_limit = TbfRateBytesPerSecond(condition) / 10ULL;
  const std::uint64_t burst_limit =
      static_cast<std::uint64_t>(TbfBurstBytes(condition)) * 4ULL;
  std::uint64_t limit = kMinimumLimitBytes;
  if (rate_limit > limit) {
    limit = rate_limit;
  }
  if (burst_limit > limit) {
    limit = burst_limit;
  }
  return SaturatingUint32(limit);
}

bool QdiscMatchesTbfCondition(const QdiscInfo& qdisc,
                              const NetworkCondition& condition) {
  return qdisc.kind == QdiscKind::kTbf && qdisc.has_tbf_options &&
         qdisc.tbf_rate_bytes_per_sec == TbfRateBytesPerSecond(condition) &&
         qdisc.tbf_limit_bytes == TbfLimitBytes(condition);
}

bool QdiscMatchesNetemCondition(const QdiscInfo& qdisc,
                                const NetworkCondition& condition) {
  return qdisc.kind == QdiscKind::kNetem && qdisc.has_netem_options &&
         qdisc.netem_latency_us == condition.delay_ms * 1000U &&
         qdisc.netem_jitter_us == condition.jitter_ms * 1000U &&
         qdisc.netem_loss == NetemProbability(condition.loss_basis_points) &&
         qdisc.netem_duplicate ==
             NetemProbability(condition.duplicate_basis_points) &&
         qdisc.netem_corrupt ==
             NetemProbability(condition.corrupt_basis_points) &&
         qdisc.netem_reorder ==
             NetemProbability(condition.reorder_basis_points) &&
         qdisc.netem_limit_packets == condition.limit_packets;
}

}  // namespace

void ValidateNetworkCondition(const NetworkCondition& condition) {
  constexpr std::uint32_t kMaxBasisPoints = 10000U;
  constexpr std::uint32_t kMaxDelayMs = 4294967U;
  if (condition.delay_ms > kMaxDelayMs || condition.jitter_ms > kMaxDelayMs) {
    throw std::runtime_error("netem delay and jitter must fit in uint32 usec");
  }
  if (condition.loss_basis_points > kMaxBasisPoints) {
    throw std::runtime_error("netem loss basis points must be 0..10000");
  }
  if (condition.duplicate_basis_points > kMaxBasisPoints) {
    throw std::runtime_error("netem duplicate basis points must be 0..10000");
  }
  if (condition.corrupt_basis_points > kMaxBasisPoints) {
    throw std::runtime_error("netem corrupt basis points must be 0..10000");
  }
  if (condition.reorder_basis_points > kMaxBasisPoints) {
    throw std::runtime_error("netem reorder basis points must be 0..10000");
  }
  if (condition.limit_packets == 0U) {
    throw std::runtime_error("netem packet limit must be greater than zero");
  }
}

void ValidateIpv4Address(std::string_view address,
                         std::string_view field_name) {
  const std::string text(address);
  in_addr ipv4_address{};
  if (text.empty() || inet_pton(AF_INET, text.c_str(), &ipv4_address) != 1) {
    throw std::runtime_error("invalid IPv4 " + std::string(field_name) + ": " +
                             text);
  }
}

bool QdiscMatchesNetworkCondition(const QdiscInfo& qdisc,
                                  const NetworkCondition& condition) {
  if (HasBandwidthCondition(condition) && HasNetemCondition(condition)) {
    return qdisc.kind == QdiscKind::kTbfNetem && qdisc.has_tbf_options &&
           qdisc.has_netem_options &&
           qdisc.tbf_rate_bytes_per_sec == TbfRateBytesPerSecond(condition) &&
           qdisc.tbf_limit_bytes == TbfLimitBytes(condition) &&
           qdisc.netem_latency_us == condition.delay_ms * 1000U &&
           qdisc.netem_jitter_us == condition.jitter_ms * 1000U &&
           qdisc.netem_loss == NetemProbability(condition.loss_basis_points) &&
           qdisc.netem_duplicate ==
               NetemProbability(condition.duplicate_basis_points) &&
           qdisc.netem_corrupt ==
               NetemProbability(condition.corrupt_basis_points) &&
           qdisc.netem_reorder ==
               NetemProbability(condition.reorder_basis_points) &&
           qdisc.netem_limit_packets == condition.limit_packets;
  }
  if (HasBandwidthCondition(condition)) {
    return QdiscMatchesTbfCondition(qdisc, condition);
  }
  return QdiscMatchesNetemCondition(qdisc, condition);
}

bool QdiscsMatchNetworkCondition(const std::vector<QdiscInfo>& qdiscs,
                                 const std::string& if_name,
                                 const NetworkCondition& condition,
                                 QdiscInfo* summary) {
  if (HasBandwidthCondition(condition) && HasNetemCondition(condition)) {
    const QdiscInfo* tbf = FindQdiscForInterfaceParent(
        qdiscs, if_name, QdiscKind::kTbf, TC_H_ROOT);
    const QdiscInfo* netem = FindQdiscForInterfaceParent(
        qdiscs, if_name, QdiscKind::kNetem, kTbfChildClassId);
    if (tbf == nullptr || netem == nullptr ||
        !QdiscMatchesTbfCondition(*tbf, condition) ||
        !QdiscMatchesNetemCondition(*netem, condition)) {
      return false;
    }
    if (summary != nullptr) {
      *summary = *tbf;
      summary->kind = QdiscKind::kTbfNetem;
      summary->kernel_kind = std::string(QdiscKindName(summary->kind));
      summary->has_netem_options = true;
      summary->netem_latency_us = netem->netem_latency_us;
      summary->netem_jitter_us = netem->netem_jitter_us;
      summary->netem_loss = netem->netem_loss;
      summary->netem_duplicate = netem->netem_duplicate;
      summary->netem_corrupt = netem->netem_corrupt;
      summary->netem_reorder = netem->netem_reorder;
      summary->netem_limit_packets = netem->netem_limit_packets;
    }
    return true;
  }

  const QdiscInfo* qdisc = nullptr;
  if (HasBandwidthCondition(condition)) {
    qdisc = FindQdiscForInterfaceParent(qdiscs, if_name, QdiscKind::kTbf,
                                        TC_H_ROOT);
  } else {
    qdisc = FindQdiscForInterfaceParent(qdiscs, if_name, QdiscKind::kNetem,
                                        TC_H_ROOT);
  }
  if (qdisc == nullptr || !QdiscMatchesNetworkCondition(*qdisc, condition)) {
    return false;
  }
  if (summary != nullptr) {
    *summary = *qdisc;
  }
  return true;
}

bool TcFilterMatchesEgressIpv4TcpDrop(const TcFilterInfo& filter,
                                      const std::string& if_name,
                                      const std::string& dst_address,
                                      std::uint16_t dst_port,
                                      std::uint32_t handle) {
  return TcFilterMatchesEgressIpv4TcpDrop(filter, if_name, "", dst_address,
                                          dst_port, handle);
}

bool TcFilterMatchesEgressIpv4TcpDrop(const TcFilterInfo& filter,
                                      const std::string& if_name,
                                      const std::string& src_address,
                                      const std::string& dst_address,
                                      std::uint16_t dst_port,
                                      std::uint32_t handle) {
  const bool source_matches =
      src_address.empty()
          ? !filter.has_ipv4_src
          : (filter.has_ipv4_src && filter.ipv4_src == src_address);
  return TcFilterIsEgressIpv4TcpDropPolicy(filter, if_name) &&
         filter.handle == handle && source_matches && filter.has_ipv4_dst &&
         filter.ipv4_dst == dst_address && filter.has_tcp_dst &&
         filter.tcp_dst == dst_port;
}

bool TcFilterIsEgressIpv4TcpDropPolicy(const TcFilterInfo& filter,
                                       const std::string& if_name) {
  const bool source_mask_is_exact =
      !filter.has_ipv4_src ||
      (filter.has_ipv4_src_mask && filter.ipv4_src_mask == "255.255.255.255");
  return filter.if_name == if_name && filter.kind == TcFilterKind::kFlower &&
         filter.handle != 0U && filter.parent == kClsactEgressParent &&
         filter.egress && filter.protocol == ETH_P_IP && filter.has_eth_type &&
         filter.eth_type == ETH_P_IP && filter.has_ip_proto &&
         filter.ip_proto == IPPROTO_TCP && source_mask_is_exact &&
         filter.has_ipv4_dst && filter.has_ipv4_dst_mask &&
         filter.ipv4_dst_mask == "255.255.255.255" && filter.has_tcp_dst &&
         filter.has_tcp_dst_mask && filter.tcp_dst_mask == 0xFFFFU &&
         filter.has_drop_action;
}

TcFilterStatsSummary SummarizeEgressIpv4TcpDropPolicies(
    const std::vector<TcFilterInfo>& filters, const std::string& if_name) {
  TcFilterStatsSummary summary;
  const auto add = [](std::uint64_t value, std::uint64_t* total,
                      std::string_view field) {
    if (value > std::numeric_limits<std::uint64_t>::max() - *total) {
      throw std::runtime_error("tc filter " + std::string(field) +
                               " counter overflow");
    }
    *total += value;
  };
  for (const TcFilterInfo& filter : filters) {
    if (!TcFilterIsEgressIpv4TcpDropPolicy(filter, if_name)) {
      continue;
    }
    ++summary.policy_count;
    if (!filter.has_stats) {
      continue;
    }
    ++summary.policies_with_stats;
    add(filter.match_bytes, &summary.match_bytes, "match bytes");
    add(filter.match_packets, &summary.match_packets, "match packets");
    add(filter.drop_packets, &summary.drop_packets, "drop packets");
  }
  return summary;
}

bool DirectionalNetworkPoliciesMatch(
    const std::vector<QdiscInfo>& qdiscs,
    const std::vector<TcFilterInfo>& filters, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies) {
  ValidateDirectionalNetworkPolicies(policies);
  const auto root =
      std::find_if(qdiscs.begin(), qdiscs.end(), [&](const QdiscInfo& qdisc) {
        return qdisc.if_name == if_name && qdisc.parent == TC_H_ROOT &&
               qdisc.handle == kDirectionalRootHandle;
      });
  if (policies.empty()) {
    return root == qdiscs.end();
  }
  if (root == qdiscs.end() || root->kind != QdiscKind::kPrio ||
      !root->has_prio_options || root->prio_bands != kDirectionalBandCount) {
    return false;
  }

  std::size_t expected_owned_qdisc_count = 0;
  for (const DirectionalNetworkPolicy& policy : policies) {
    ++expected_owned_qdisc_count;
    if (HasBandwidthCondition(policy.condition) &&
        HasNetemCondition(policy.condition)) {
      ++expected_owned_qdisc_count;
    }
  }
  std::size_t owned_qdisc_count = 0;
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name != if_name) {
      continue;
    }
    for (std::uint32_t band = 1U; band <= kMaximumDirectionalPolicies; ++band) {
      if (qdisc.handle == DirectionalTbfHandle(band) ||
          qdisc.handle == DirectionalNetemHandle(band)) {
        ++owned_qdisc_count;
        break;
      }
    }
  }
  if (owned_qdisc_count != expected_owned_qdisc_count) {
    return false;
  }

  std::size_t owned_filter_count = 0;
  for (const TcFilterInfo& filter : filters) {
    if (filter.if_name != if_name || filter.parent != kDirectionalRootHandle) {
      continue;
    }
    // RTM_GETTFILTER emits one handle-zero chain descriptor before the
    // concrete flower classifiers. It is kernel metadata, not a mutable
    // policy object.
    if (filter.handle == 0U) {
      continue;
    }
    if (filter.priority != kDirectionalFilterPriority ||
        filter.handle <= kDirectionalFilterHandleBase ||
        filter.handle >
            kDirectionalFilterHandleBase + kMaximumDirectionalPolicies) {
      return false;
    }
    ++owned_filter_count;
  }
  if (owned_filter_count != policies.size()) {
    return false;
  }

  for (const DirectionalNetworkPolicy& policy : policies) {
    const std::uint32_t class_id = DirectionalClassId(policy.band);
    const auto filter = std::find_if(
        filters.begin(), filters.end(), [&](const TcFilterInfo& candidate) {
          return candidate.if_name == if_name &&
                 candidate.kind == TcFilterKind::kFlower &&
                 candidate.handle == DirectionalFilterHandle(policy.band) &&
                 candidate.parent == kDirectionalRootHandle &&
                 candidate.priority == kDirectionalFilterPriority &&
                 candidate.protocol == ETH_P_IP && candidate.has_eth_type &&
                 candidate.eth_type == ETH_P_IP && !candidate.has_ipv4_src &&
                 candidate.has_ipv4_dst &&
                 candidate.ipv4_dst == policy.destination_address &&
                 candidate.has_ipv4_dst_mask &&
                 candidate.ipv4_dst_mask == "255.255.255.255" &&
                 candidate.has_class_id && candidate.class_id == class_id &&
                 !candidate.has_drop_action;
        });
    if (filter == filters.end()) {
      return false;
    }

    if (HasBandwidthCondition(policy.condition)) {
      const auto tbf = std::find_if(
          qdiscs.begin(), qdiscs.end(), [&](const QdiscInfo& qdisc) {
            return qdisc.if_name == if_name &&
                   qdisc.handle == DirectionalTbfHandle(policy.band) &&
                   qdisc.parent == class_id;
          });
      if (tbf == qdiscs.end() ||
          !QdiscMatchesTbfCondition(*tbf, policy.condition)) {
        return false;
      }
      if (HasNetemCondition(policy.condition)) {
        const std::uint32_t tbf_child =
            TC_H_MAKE(TC_H_MAJ(DirectionalTbfHandle(policy.band)), 1U);
        const auto netem = std::find_if(
            qdiscs.begin(), qdiscs.end(), [&](const QdiscInfo& qdisc) {
              return qdisc.if_name == if_name &&
                     qdisc.handle == DirectionalNetemHandle(policy.band) &&
                     qdisc.parent == tbf_child;
            });
        if (netem == qdiscs.end() ||
            !QdiscMatchesNetemCondition(*netem, policy.condition)) {
          return false;
        }
      }
      continue;
    }

    const auto netem =
        std::find_if(qdiscs.begin(), qdiscs.end(), [&](const QdiscInfo& qdisc) {
          return qdisc.if_name == if_name &&
                 qdisc.handle == DirectionalNetemHandle(policy.band) &&
                 qdisc.parent == class_id;
        });
    if (netem == qdiscs.end() ||
        !QdiscMatchesNetemCondition(*netem, policy.condition)) {
      return false;
    }
  }
  return true;
}

DirectionalNetworkPolicyStats SummarizeDirectionalNetworkPolicyStats(
    const std::vector<QdiscInfo>& qdiscs,
    const std::vector<TcFilterInfo>& filters, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies) {
  if (!DirectionalNetworkPoliciesMatch(qdiscs, filters, if_name, policies)) {
    throw std::runtime_error(
        "directional network policy counter state does not match the "
        "configured kernel model on " +
        if_name);
  }

  DirectionalNetworkPolicyStats stats;
  stats.policy_count = policies.size();
  stats.policies.reserve(policies.size());
  const auto add = [](std::uint64_t value, std::uint64_t* total,
                      std::string_view field) {
    if (value > std::numeric_limits<std::uint64_t>::max() - *total) {
      throw std::runtime_error("directional network " + std::string(field) +
                               " counter overflow");
    }
    *total += value;
  };

  for (const DirectionalNetworkPolicy& policy : policies) {
    const auto filter = std::find_if(
        filters.begin(), filters.end(), [&](const TcFilterInfo& candidate) {
          return candidate.if_name == if_name &&
                 candidate.handle == DirectionalFilterHandle(policy.band) &&
                 candidate.parent == kDirectionalRootHandle;
        });
    if (filter == filters.end()) {
      throw std::runtime_error(
          "directional network policy counter filter disappeared from " +
          if_name);
    }

    DirectionalNetworkPolicyCounter counter;
    counter.band = policy.band;
    counter.destination_address = policy.destination_address;
    counter.filter = *filter;
    if (filter->has_stats) {
      ++stats.policies_with_filter_stats;
      add(filter->match_bytes, &stats.filter_match_bytes, "filter match bytes");
      add(filter->match_packets, &stats.filter_match_packets,
          "filter match packets");
    }

    const auto append_qdisc = [&](std::uint32_t handle) {
      const auto qdisc = std::find_if(
          qdiscs.begin(), qdiscs.end(), [&](const QdiscInfo& candidate) {
            return candidate.if_name == if_name && candidate.handle == handle;
          });
      if (qdisc == qdiscs.end()) {
        throw std::runtime_error(
            "directional network policy counter qdisc disappeared from " +
            if_name);
      }
      counter.qdiscs.push_back(*qdisc);
      ++stats.qdisc_count;
      if (qdisc->has_stats) {
        ++stats.qdiscs_with_stats;
      }
      add(qdisc->drops, &counter.qdisc_drops, "qdisc drops");
      add(qdisc->overlimits, &counter.qdisc_overlimits, "qdisc overlimits");
      add(qdisc->qlen, &counter.qdisc_qlen, "qdisc queue length");
      add(qdisc->backlog, &counter.qdisc_backlog, "qdisc backlog");
      add(qdisc->requeues, &counter.qdisc_requeues, "qdisc requeues");
      add(qdisc->drops, &stats.qdisc_drops, "qdisc drops");
      add(qdisc->overlimits, &stats.qdisc_overlimits, "qdisc overlimits");
      add(qdisc->qlen, &stats.qdisc_qlen, "qdisc queue length");
      add(qdisc->backlog, &stats.qdisc_backlog, "qdisc backlog");
      add(qdisc->requeues, &stats.qdisc_requeues, "qdisc requeues");
    };

    if (HasBandwidthCondition(policy.condition)) {
      append_qdisc(DirectionalTbfHandle(policy.band));
      if (HasNetemCondition(policy.condition)) {
        append_qdisc(DirectionalNetemHandle(policy.band));
      }
    } else {
      append_qdisc(DirectionalNetemHandle(policy.band));
    }

    // The first qdisc is the ingress stage for the edge. Its byte and packet
    // counters represent the policy once; adding a child netem counter would
    // double-count combined TBF-plus-netem traffic.
    const QdiscInfo& primary = counter.qdiscs.front();
    counter.qdisc_bytes = primary.bytes;
    counter.qdisc_packets = primary.packets;
    add(primary.bytes, &stats.qdisc_bytes, "qdisc bytes");
    add(primary.packets, &stats.qdisc_packets, "qdisc packets");
    stats.policies.push_back(std::move(counter));
  }
  return stats;
}

namespace {

constexpr std::chrono::milliseconds kNetlinkAcknowledgementTimeout{5000};

#ifdef BBP_ENABLE_TEST_HOOKS
struct NetlinkFailurePlanState {
  bool installed = false;
  std::uint64_t request_count = 0;
  std::vector<NetlinkFailurePoint> points;
};

std::mutex& NetlinkFailurePlanMutex() {
  static std::mutex mutex;
  return mutex;
}

NetlinkFailurePlanState& MutableNetlinkFailurePlan() {
  static NetlinkFailurePlanState state;
  return state;
}

std::uint64_t BeginNetlinkRequestForTest() {
  std::lock_guard lock(NetlinkFailurePlanMutex());
  NetlinkFailurePlanState& state = MutableNetlinkFailurePlan();
  if (!state.installed) {
    return 0;
  }
  return ++state.request_count;
}

void MaybeInjectNetlinkFailure(std::uint64_t request_number,
                               NetlinkFailurePhase phase) {
  if (request_number == 0) {
    return;
  }
  std::lock_guard lock(NetlinkFailurePlanMutex());
  const NetlinkFailurePlanState& state = MutableNetlinkFailurePlan();
  const auto point =
      std::find_if(state.points.begin(), state.points.end(),
                   [&](const NetlinkFailurePoint& candidate) {
                     return candidate.request_number == request_number &&
                            candidate.phase == phase;
                   });
  if (point == state.points.end()) {
    return;
  }
  const std::string phase_name =
      phase == NetlinkFailurePhase::kBeforeSend ? "before send" : "after send";
  throw std::runtime_error("injected netlink failure " + phase_name +
                           " at request " + std::to_string(request_number));
}
#endif

void ValidateNetlinkAcknowledgement(const void* reply, size_t reply_size,
                                    std::uint32_t sequence,
                                    std::uint32_t port_id) {
  if (reply_size < sizeof(nlmsghdr)) {
    throw std::runtime_error(
        "netlink acknowledgement is shorter than its message header");
  }

  nlmsghdr header{};
  std::memcpy(&header, reply, sizeof(header));
  if (header.nlmsg_len < sizeof(nlmsghdr) || header.nlmsg_len > reply_size ||
      NLMSG_ALIGN(header.nlmsg_len) != reply_size) {
    throw std::runtime_error(
        "netlink acknowledgement is not exactly one complete message");
  }
  if (header.nlmsg_type != NLMSG_ERROR) {
    throw std::runtime_error(
        "netlink mutation reply is not an NLMSG_ERROR acknowledgement");
  }
  if (header.nlmsg_len < NLMSG_LENGTH(sizeof(nlmsgerr))) {
    throw std::runtime_error(
        "netlink acknowledgement has a truncated nlmsgerr payload");
  }
  if (header.nlmsg_seq != sequence) {
    throw std::runtime_error("netlink acknowledgement sequence mismatch");
  }
  if (header.nlmsg_pid != port_id) {
    throw std::runtime_error("netlink acknowledgement port ID mismatch");
  }

  const int status =
      mnl_cb_run(reply, reply_size, sequence, port_id, nullptr, nullptr);
  if (status == MNL_CB_ERROR) {
    throw std::runtime_error(std::string("netlink request failed: ") +
                             std::strerror(errno));
  }
  if (status != MNL_CB_STOP) {
    throw std::runtime_error(
        "netlink mutation reply did not contain a completed acknowledgement");
  }

  nlmsgerr acknowledgement{};
  const auto* bytes = static_cast<const unsigned char*>(reply);
  std::memcpy(&acknowledgement, bytes + NLMSG_HDRLEN, sizeof(acknowledgement));
  if (acknowledgement.error != 0) {
    throw std::runtime_error(
        "netlink error acknowledgement was not rejected by libmnl");
  }
}

template <typename Operation>
auto ExecuteInNetworkNamespace(int netns_fd,
                               Operation operation) -> decltype(operation()) {
  using Result = decltype(operation());
  std::promise<Result> promise;
  std::future<Result> result = promise.get_future();

  std::thread worker([netns_fd, &operation, &promise]() {
    try {
      ScopedNetworkNamespace ns(netns_fd);
      if constexpr (std::is_void_v<Result>) {
        operation();
        promise.set_value();
      } else {
        promise.set_value(operation());
      }
    } catch (...) {
      promise.set_exception(std::current_exception());
    }
  });

  worker.join();
  return result.get();
}

void SendNetlinkRequest(nlmsghdr* nlh, uint32_t sequence) {
#ifdef BBP_ENABLE_TEST_HOOKS
  const std::uint64_t request_number = BeginNetlinkRequestForTest();
  MaybeInjectNetlinkFailure(request_number, NetlinkFailurePhase::kBeforeSend);
#endif
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);
  socket.SetReceiveTimeout(kNetlinkAcknowledgementTimeout);
  const unsigned int port_id = socket.PortId();
  socket.Send(nlh, nlh->nlmsg_len);
#ifdef BBP_ENABLE_TEST_HOOKS
  MaybeInjectNetlinkFailure(request_number, NetlinkFailurePhase::kAfterSend);
#endif

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  const ssize_t received = socket.Receive(buffer.data(), buffer.size());
  ValidateNetlinkAcknowledgement(buffer.data(), static_cast<size_t>(received),
                                 sequence, port_id);
}

void PutRawPayload(nlmsghdr* nlh, const void* data, size_t size) {
  void* tail = mnl_nlmsg_get_payload_tail(nlh);
  std::memcpy(tail, data, size);
  const size_t aligned_size = MNL_ALIGN(size);
  if (aligned_size >
      static_cast<size_t>(std::numeric_limits<__u32>::max() - nlh->nlmsg_len)) {
    throw std::runtime_error("netlink message payload is too large");
  }
  if (aligned_size > size) {
    std::memset(static_cast<char*>(tail) + size, 0, aligned_size - size);
  }
  nlh->nlmsg_len += static_cast<__u32>(aligned_size);
}

void TryDeleteLink(const std::string& name) {
  try {
    DeleteLink(name);
  } catch (const std::exception&) {
  }
}

std::string ProbeName(char suffix) {
  return "bbp" + std::to_string(static_cast<long long>(getpid() % 100000)) +
         suffix;
}

void WriteExact(int fd, const void* data, size_t size) {
  const char* cursor = static_cast<const char*>(data);
  size_t remaining = size;
  while (remaining != 0U) {
    const ssize_t written = write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      _exit(127);
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
}

int ReadNamespaceStatus(int fd) {
  int status = 0;
  char* cursor = reinterpret_cast<char*>(&status);
  size_t remaining = sizeof(status);
  while (remaining != 0U) {
    const ssize_t received = read(fd, cursor, remaining);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("read netns status failed: ") +
                               std::strerror(errno));
    }
    if (received == 0) {
      throw std::runtime_error("netns helper exited before reporting status");
    }
    cursor += received;
    remaining -= static_cast<size_t>(received);
  }
  return status;
}

void WaitForPid(pid_t pid) {
  int status = 0;
  while (waitpid(pid, &status, 0) < 0) {
    if (errno != EINTR) {
      throw std::runtime_error(std::string("waitpid failed: ") +
                               std::strerror(errno));
    }
  }
}

UniqueFd StartNetworkNamespaceHelper(
    pid_t* helper_pid,
    const std::filesystem::path* owner_cgroup_path = nullptr) {
  UniqueFd cgroup_procs;
  if (owner_cgroup_path != nullptr) {
    cgroup_procs = UniqueFd(open(
        ((*owner_cgroup_path) / "cgroup.procs").c_str(), O_WRONLY | O_CLOEXEC));
    if (cgroup_procs.get() < 0) {
      throw std::runtime_error("open network namespace helper cgroup failed: " +
                               std::string(std::strerror(errno)));
    }
  }
  int pipe_fds[2];
  if (pipe2(pipe_fds, O_CLOEXEC) != 0) {
    throw std::runtime_error(std::string("pipe2 failed: ") +
                             std::strerror(errno));
  }
  UniqueFd read_end(pipe_fds[0]);
  UniqueFd write_end(pipe_fds[1]);

  const pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error(std::string("fork failed: ") +
                             std::strerror(errno));
  }

  if (pid == 0) {
    read_end.Reset();
    int status = 0;
    if (cgroup_procs.get() >= 0) {
      std::array<char, 32> own_pid{};
      const auto [end, conversion_error] = std::to_chars(
          own_pid.data(), own_pid.data() + own_pid.size(), getpid());
      if (conversion_error != std::errc()) {
        status = EIO;
      } else {
        const ssize_t written =
            write(cgroup_procs.get(), own_pid.data(),
                  static_cast<std::size_t>(end - own_pid.data()));
        if (written < 0) {
          status = errno;
        } else if (written != end - own_pid.data()) {
          status = EIO;
        }
      }
    }
    cgroup_procs.Reset();
    if (status == 0 && unshare(CLONE_NEWNET) != 0) {
      status = errno;
    }
    WriteExact(write_end.get(), &status, sizeof(status));
    if (status != 0) {
      _exit(127);
    }
    while (true) {
      pause();
    }
  }

  write_end.Reset();
  cgroup_procs.Reset();
  const int status = ReadNamespaceStatus(read_end.get());
  read_end.Reset();
  if (status != 0) {
    WaitForPid(pid);
    const std::string operation = owner_cgroup_path == nullptr
                                      ? "unshare(CLONE_NEWNET)"
                                      : "network namespace helper setup";
    throw std::runtime_error(operation + " failed: " + std::strerror(status));
  }

  try {
    UniqueFd netns =
        OpenFileDescriptor("/proc/" + std::to_string(pid) + "/ns/net");
    *helper_pid = pid;
    return netns;
  } catch (...) {
    kill(pid, SIGKILL);
    WaitForPid(pid);
    throw;
  }
}

template <typename Payload>
Payload CopyAttributePayload(const nlattr* attr) {
  static_assert(std::is_trivially_copyable_v<Payload>);
  Payload payload{};
  std::memcpy(&payload, mnl_attr_get_payload(attr), sizeof(payload));
  return payload;
}

template <typename Payload>
bool CopyMessagePayload(const nlmsghdr* message, Payload* payload) {
  static_assert(std::is_trivially_copyable_v<Payload>);
  if (mnl_nlmsg_get_payload_len(message) < sizeof(*payload)) {
    errno = EINVAL;
    return false;
  }
  std::memcpy(payload, mnl_nlmsg_get_payload(message), sizeof(*payload));
  return true;
}

int ParseLinkAttr(const nlattr* attr, void* data) {
  auto* link = static_cast<LinkInfo*>(data);
  const uint16_t type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, IFLA_MAX) < 0) {
    return MNL_CB_OK;
  }
  if (type == IFLA_IFNAME) {
    if (mnl_attr_validate(attr, MNL_TYPE_STRING) < 0) {
      return MNL_CB_ERROR;
    }
    link->name = mnl_attr_get_str(attr);
  }
  if (type == IFLA_STATS64) {
    if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
        mnl_attr_get_payload_len(attr) < sizeof(rtnl_link_stats64)) {
      errno = EINVAL;
      return MNL_CB_ERROR;
    }
    StoreLinkStats64(CopyAttributePayload<rtnl_link_stats64>(attr), link);
  }
  if (type == IFLA_STATS && !link->has_stats) {
    if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
        mnl_attr_get_payload_len(attr) < sizeof(rtnl_link_stats)) {
      errno = EINVAL;
      return MNL_CB_ERROR;
    }
    StoreLinkStats32(CopyAttributePayload<rtnl_link_stats>(attr), link);
  }
  return MNL_CB_OK;
}

int ParseLinkMessage(const nlmsghdr* nlh, void* data) {
  auto* links = static_cast<std::vector<LinkInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWLINK) {
    return MNL_CB_OK;
  }

  ifinfomsg message{};
  if (!CopyMessagePayload(nlh, &message)) {
    return MNL_CB_ERROR;
  }
  LinkInfo link;
  link.index = message.ifi_index;
  link.up = (message.ifi_flags & IFF_UP) != 0U;
  if (mnl_attr_parse(nlh, sizeof(message), ParseLinkAttr, &link) < 0) {
    return MNL_CB_ERROR;
  }
  links->push_back(std::move(link));
  return MNL_CB_OK;
}

struct AddressParseState {
  AddressInfo address;
  bool has_address = false;
};

int StoreIpv4Address(const nlattr* attr, AddressParseState* state,
                     bool prefer) {
  if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0) {
    return MNL_CB_ERROR;
  }
  if (mnl_attr_get_payload_len(attr) != sizeof(in_addr)) {
    errno = EINVAL;
    return MNL_CB_ERROR;
  }

  char text[INET_ADDRSTRLEN] = {};
  if (inet_ntop(AF_INET, mnl_attr_get_payload(attr), text, sizeof(text)) ==
      nullptr) {
    return MNL_CB_ERROR;
  }
  if (prefer || !state->has_address) {
    state->address.address = text;
    state->has_address = true;
  }
  return MNL_CB_OK;
}

int ParseAddressAttr(const nlattr* attr, void* data) {
  auto* state = static_cast<AddressParseState*>(data);
  const uint16_t type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, IFA_MAX) < 0) {
    return MNL_CB_OK;
  }
  if (type == IFA_LOCAL) {
    return StoreIpv4Address(attr, state, true);
  }
  if (type == IFA_ADDRESS) {
    return StoreIpv4Address(attr, state, false);
  }
  if (type == IFA_LABEL) {
    if (mnl_attr_validate(attr, MNL_TYPE_STRING) < 0) {
      return MNL_CB_ERROR;
    }
    state->address.if_name = mnl_attr_get_str(attr);
  }
  return MNL_CB_OK;
}

int ParseAddressMessage(const nlmsghdr* nlh, void* data) {
  auto* addresses = static_cast<std::vector<AddressInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWADDR) {
    return MNL_CB_OK;
  }

  ifaddrmsg message{};
  if (!CopyMessagePayload(nlh, &message)) {
    return MNL_CB_ERROR;
  }
  if (message.ifa_family != AF_INET) {
    return MNL_CB_OK;
  }

  AddressParseState state;
  state.address.if_index = static_cast<int>(message.ifa_index);
  state.address.prefix_len = message.ifa_prefixlen;
  if (mnl_attr_parse(nlh, sizeof(message), ParseAddressAttr, &state) < 0) {
    return MNL_CB_ERROR;
  }
  if (state.address.if_name.empty()) {
    char if_name[IF_NAMESIZE] = {};
    if (if_indextoname(message.ifa_index, if_name) != nullptr) {
      state.address.if_name = if_name;
    }
  }
  if (state.has_address) {
    addresses->push_back(std::move(state.address));
  }
  return MNL_CB_OK;
}

struct NamespaceAddressState {
  std::vector<LinkInfo> links;
  std::vector<AddressInfo> addresses;
  std::vector<AddressInfo> addresses_after_delete;
};

int StoreRouteIpv4Address(const nlattr* attr, std::string* address) {
  if (mnl_attr_validate(attr, MNL_TYPE_U32) < 0) {
    return MNL_CB_ERROR;
  }

  char text[INET_ADDRSTRLEN] = {};
  if (inet_ntop(AF_INET, mnl_attr_get_payload(attr), text, sizeof(text)) ==
      nullptr) {
    return MNL_CB_ERROR;
  }
  *address = text;
  return MNL_CB_OK;
}

int ParseRouteAttr(const nlattr* attr, void* data) {
  auto* route = static_cast<RouteInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, RTA_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case RTA_DST:
      return StoreRouteIpv4Address(attr, &route->destination);
    case RTA_GATEWAY:
      return StoreRouteIpv4Address(attr, &route->gateway);
    case RTA_OIF:
      if (mnl_attr_validate(attr, MNL_TYPE_U32) < 0) {
        return MNL_CB_ERROR;
      }
      route->oif_index = static_cast<int>(mnl_attr_get_u32(attr));
      return MNL_CB_OK;
    case RTA_TABLE:
      if (mnl_attr_validate(attr, MNL_TYPE_U32) < 0) {
        return MNL_CB_ERROR;
      }
      route->table = mnl_attr_get_u32(attr);
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseRouteMessage(const nlmsghdr* nlh, void* data) {
  auto* routes = static_cast<std::vector<RouteInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWROUTE) {
    return MNL_CB_OK;
  }

  rtmsg message{};
  if (!CopyMessagePayload(nlh, &message)) {
    return MNL_CB_ERROR;
  }
  if (message.rtm_family != AF_INET) {
    return MNL_CB_OK;
  }

  RouteInfo route;
  route.destination = "0.0.0.0";
  route.prefix_len = message.rtm_dst_len;
  route.table = message.rtm_table;
  route.protocol = message.rtm_protocol;
  route.scope = message.rtm_scope;
  route.type = message.rtm_type;
  if (mnl_attr_parse(nlh, sizeof(message), ParseRouteAttr, &route) < 0) {
    return MNL_CB_ERROR;
  }
  if (route.oif_index > 0) {
    char if_name[IF_NAMESIZE] = {};
    if (if_indextoname(static_cast<unsigned int>(route.oif_index), if_name) !=
        nullptr) {
      route.oif_name = if_name;
    }
  }
  routes->push_back(std::move(route));
  return MNL_CB_OK;
}

struct NamespaceRouteState {
  std::vector<LinkInfo> links;
  std::vector<AddressInfo> addresses;
  std::vector<RouteInfo> routes;
  std::vector<AddressInfo> addresses_after_delete;
  std::vector<RouteInfo> routes_after_delete;
};

int ParseTbfOptionAttr(const nlattr* attr, void* data) {
  auto* qdisc = static_cast<QdiscInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_TBF_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_TBF_PARMS:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(tc_tbf_qopt)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const tc_tbf_qopt options = CopyAttributePayload<tc_tbf_qopt>(attr);
        qdisc->has_tbf_options = true;
        qdisc->tbf_rate_bytes_per_sec = options.rate.rate;
        qdisc->tbf_limit_bytes = options.limit;
        qdisc->tbf_buffer_ticks = options.buffer;
        qdisc->tbf_mtu_ticks = options.mtu;
      }
      return MNL_CB_OK;
    case TCA_TBF_RATE64:
      if (mnl_attr_validate(attr, MNL_TYPE_U64) < 0) {
        return MNL_CB_ERROR;
      }
      qdisc->has_tbf_options = true;
      qdisc->tbf_rate_bytes_per_sec = mnl_attr_get_u64(attr);
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseNetemNestedOptionAttr(const nlattr* attr, void* data) {
  auto* qdisc = static_cast<QdiscInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_NETEM_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_NETEM_REORDER:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(tc_netem_reorder)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const tc_netem_reorder options =
            CopyAttributePayload<tc_netem_reorder>(attr);
        qdisc->has_netem_options = true;
        qdisc->netem_reorder = options.probability;
      }
      return MNL_CB_OK;
    case TCA_NETEM_CORRUPT:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(tc_netem_corrupt)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const tc_netem_corrupt options =
            CopyAttributePayload<tc_netem_corrupt>(attr);
        qdisc->has_netem_options = true;
        qdisc->netem_corrupt = options.probability;
      }
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseNetemOptions(const nlattr* attr, QdiscInfo* qdisc) {
  const auto payload_len =
      static_cast<std::size_t>(mnl_attr_get_payload_len(attr));
  if (payload_len < sizeof(tc_netem_qopt)) {
    return MNL_CB_OK;
  }

  const tc_netem_qopt options = CopyAttributePayload<tc_netem_qopt>(attr);
  qdisc->has_netem_options = true;
  qdisc->netem_latency_us = options.latency;
  qdisc->netem_jitter_us = options.jitter;
  qdisc->netem_loss = options.loss;
  qdisc->netem_duplicate = options.duplicate;
  qdisc->netem_limit_packets = options.limit;

  const std::size_t nested_offset = MNL_ALIGN(sizeof(tc_netem_qopt));
  if (payload_len <= nested_offset) {
    return MNL_CB_OK;
  }
  const auto* nested_payload =
      static_cast<const char*>(mnl_attr_get_payload(attr)) + nested_offset;
  const std::size_t nested_len = payload_len - nested_offset;
  return mnl_attr_parse_payload(nested_payload, nested_len,
                                ParseNetemNestedOptionAttr, qdisc);
}

int ParsePrioOptions(const nlattr* attr, QdiscInfo* qdisc) {
  if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
      mnl_attr_get_payload_len(attr) < sizeof(tc_prio_qopt)) {
    errno = EINVAL;
    return MNL_CB_ERROR;
  }
  const tc_prio_qopt options = CopyAttributePayload<tc_prio_qopt>(attr);
  qdisc->has_prio_options = true;
  qdisc->prio_bands = options.bands;
  return MNL_CB_OK;
}

int ParseQdiscAttr(const nlattr* attr, void* data) {
  auto* qdisc = static_cast<QdiscInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_KIND:
      if (mnl_attr_validate(attr, MNL_TYPE_STRING) < 0) {
        return MNL_CB_ERROR;
      }
      qdisc->kernel_kind = mnl_attr_get_str(attr);
      qdisc->kind = ParseQdiscKind(qdisc->kernel_kind);
      return MNL_CB_OK;
    case TCA_STATS:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(tc_stats)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const tc_stats stats = CopyAttributePayload<tc_stats>(attr);
        qdisc->has_stats = true;
        qdisc->bytes = stats.bytes;
        qdisc->packets = stats.packets;
        qdisc->drops = stats.drops;
        qdisc->overlimits = stats.overlimits;
        qdisc->qlen = stats.qlen;
        qdisc->backlog = stats.backlog;
      }
      return MNL_CB_OK;
    case TCA_OPTIONS: {
      if (qdisc->kind == QdiscKind::kTbf) {
        if (mnl_attr_parse_nested(attr, ParseTbfOptionAttr, qdisc) < 0) {
          return MNL_CB_ERROR;
        }
        return MNL_CB_OK;
      }
      if (qdisc->kind == QdiscKind::kNetem) {
        if (ParseNetemOptions(attr, qdisc) < 0) {
          return MNL_CB_ERROR;
        }
        return MNL_CB_OK;
      }
      if (qdisc->kind == QdiscKind::kPrio) {
        if (ParsePrioOptions(attr, qdisc) < 0) {
          return MNL_CB_ERROR;
        }
      }
      return MNL_CB_OK;
    }
    default:
      return MNL_CB_OK;
  }
}

int ParseQdiscStats2Attr(const nlattr* attr, void* data) {
  auto* qdisc = static_cast<QdiscInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_STATS_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_STATS_BASIC:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(gnet_stats_basic)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const gnet_stats_basic stats =
            CopyAttributePayload<gnet_stats_basic>(attr);
        qdisc->has_stats = true;
        qdisc->bytes = stats.bytes;
        qdisc->packets = stats.packets;
      }
      return MNL_CB_OK;
    case TCA_STATS_PKT64:
      if (mnl_attr_validate(attr, MNL_TYPE_U64) < 0) {
        return MNL_CB_ERROR;
      }
      qdisc->has_stats = true;
      qdisc->packets = mnl_attr_get_u64(attr);
      return MNL_CB_OK;
    case TCA_STATS_QUEUE:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(gnet_stats_queue)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const gnet_stats_queue stats =
            CopyAttributePayload<gnet_stats_queue>(attr);
        qdisc->has_stats = true;
        qdisc->qlen = stats.qlen;
        qdisc->backlog = stats.backlog;
        qdisc->drops = stats.drops;
        qdisc->requeues = stats.requeues;
        qdisc->overlimits = stats.overlimits;
      }
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseQdiscTopLevelAttr(const nlattr* attr, void* data) {
  auto* qdisc = static_cast<QdiscInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (attr_type == TCA_STATS2) {
    if (mnl_attr_parse_nested(attr, ParseQdiscStats2Attr, qdisc) < 0) {
      return MNL_CB_ERROR;
    }
    return MNL_CB_OK;
  }
  return ParseQdiscAttr(attr, data);
}

int ParseQdiscMessage(const nlmsghdr* nlh, void* data) {
  auto* qdiscs = static_cast<std::vector<QdiscInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWQDISC) {
    return MNL_CB_OK;
  }

  tcmsg message{};
  if (!CopyMessagePayload(nlh, &message)) {
    return MNL_CB_ERROR;
  }
  QdiscInfo qdisc;
  qdisc.if_index = message.tcm_ifindex;
  qdisc.handle = message.tcm_handle;
  qdisc.parent = message.tcm_parent;
  qdisc.info = message.tcm_info;
  if (qdisc.if_index > 0) {
    char if_name[IF_NAMESIZE] = {};
    if (if_indextoname(static_cast<unsigned int>(qdisc.if_index), if_name) !=
        nullptr) {
      qdisc.if_name = if_name;
    }
  }
  if (mnl_attr_parse(nlh, sizeof(message), ParseQdiscTopLevelAttr, &qdisc) <
      0) {
    return MNL_CB_ERROR;
  }
  if (qdisc.kind != QdiscKind::kNetem) {
    qdisc.has_netem_options = false;
    qdisc.netem_latency_us = 0;
    qdisc.netem_jitter_us = 0;
    qdisc.netem_loss = 0;
    qdisc.netem_duplicate = 0;
    qdisc.netem_corrupt = 0;
    qdisc.netem_reorder = 0;
    qdisc.netem_limit_packets = 0;
  }
  if (qdisc.kind != QdiscKind::kTbf) {
    qdisc.has_tbf_options = false;
    qdisc.tbf_rate_bytes_per_sec = 0;
    qdisc.tbf_limit_bytes = 0;
    qdisc.tbf_buffer_ticks = 0;
    qdisc.tbf_mtu_ticks = 0;
  }
  if (qdisc.kind != QdiscKind::kPrio) {
    qdisc.has_prio_options = false;
    qdisc.prio_bands = 0;
  }
  qdiscs->push_back(std::move(qdisc));
  return MNL_CB_OK;
}

int ParseFilterStats2Attr(const nlattr* attr, void* data) {
  auto* filter = static_cast<TcFilterInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_STATS_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_STATS_BASIC:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) < sizeof(gnet_stats_basic)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      } else {
        const gnet_stats_basic stats =
            CopyAttributePayload<gnet_stats_basic>(attr);
        filter->has_stats = true;
        filter->match_bytes = stats.bytes;
        filter->match_packets = stats.packets;
      }
      return MNL_CB_OK;
    case TCA_STATS_PKT64:
      if (mnl_attr_validate(attr, MNL_TYPE_U64) < 0) {
        return MNL_CB_ERROR;
      }
      filter->has_stats = true;
      filter->match_packets = mnl_attr_get_u64(attr);
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseLegacyFilterStats(const nlattr* attr, TcFilterInfo* filter) {
  if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
      mnl_attr_get_payload_len(attr) < sizeof(tc_stats)) {
    errno = EINVAL;
    return MNL_CB_ERROR;
  }
  const tc_stats stats = CopyAttributePayload<tc_stats>(attr);
  filter->has_stats = true;
  filter->match_bytes = stats.bytes;
  filter->match_packets = stats.packets;
  return MNL_CB_OK;
}

enum class ActionKind {
  kUnknown,
  kGact,
};

ActionKind ParseActionKind(std::string_view kind) {
  return kind == "gact" ? ActionKind::kGact : ActionKind::kUnknown;
}

struct ActionParseState {
  ActionKind kind = ActionKind::kUnknown;
  bool drop = false;
  TcFilterInfo stats;
};

int ParseGactOptionAttr(const nlattr* attr, void* data) {
  auto* action = static_cast<ActionParseState*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_GACT_MAX) < 0) {
    return MNL_CB_OK;
  }

  if (attr_type != TCA_GACT_PARMS) {
    return MNL_CB_OK;
  }
  if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
      mnl_attr_get_payload_len(attr) < sizeof(tc_gact)) {
    errno = EINVAL;
    return MNL_CB_ERROR;
  }

  const tc_gact options = CopyAttributePayload<tc_gact>(attr);
  action->drop = options.action == TC_ACT_SHOT;
  return MNL_CB_OK;
}

int ParseActionEntryAttr(const nlattr* attr, void* data) {
  auto* action = static_cast<ActionParseState*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_ACT_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_ACT_KIND:
      if (mnl_attr_validate(attr, MNL_TYPE_STRING) < 0) {
        return MNL_CB_ERROR;
      }
      action->kind = ParseActionKind(mnl_attr_get_str(attr));
      return MNL_CB_OK;
    case TCA_ACT_OPTIONS:
      if (mnl_attr_parse_nested(attr, ParseGactOptionAttr, action) < 0) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    case TCA_ACT_STATS:
      if (mnl_attr_parse_nested(attr, ParseFilterStats2Attr, &action->stats) <
          0) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseActionListEntry(const nlattr* attr, void* data) {
  auto* filter = static_cast<TcFilterInfo*>(data);
  ActionParseState action;
  if (mnl_attr_parse_nested(attr, ParseActionEntryAttr, &action) < 0) {
    return MNL_CB_ERROR;
  }
  if (action.kind == ActionKind::kGact && action.drop) {
    filter->has_drop_action = true;
    if (action.stats.has_stats) {
      filter->has_stats = true;
      filter->match_bytes = action.stats.match_bytes;
      filter->match_packets = action.stats.match_packets;
    }
  }
  return MNL_CB_OK;
}

int ParseFlowerAttr(const nlattr* attr, void* data) {
  auto* filter = static_cast<TcFilterInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_FLOWER_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_FLOWER_CLASSID:
      if (mnl_attr_validate(attr, MNL_TYPE_U32) < 0) {
        return MNL_CB_ERROR;
      }
      filter->has_class_id = true;
      filter->class_id = mnl_attr_get_u32(attr);
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_ETH_TYPE:
      if (mnl_attr_validate(attr, MNL_TYPE_U16) < 0) {
        return MNL_CB_ERROR;
      }
      filter->has_eth_type = true;
      filter->eth_type = ntohs(mnl_attr_get_u16(attr));
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_IP_PROTO:
      if (mnl_attr_validate(attr, MNL_TYPE_U8) < 0) {
        return MNL_CB_ERROR;
      }
      filter->has_ip_proto = true;
      filter->ip_proto = mnl_attr_get_u8(attr);
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_IPV4_SRC:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) != sizeof(in_addr)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      }
      filter->has_ipv4_src = true;
      try {
        filter->ipv4_src = Ipv4ToString(mnl_attr_get_payload(attr));
      } catch (...) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_IPV4_SRC_MASK:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) != sizeof(in_addr)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      }
      filter->has_ipv4_src_mask = true;
      try {
        filter->ipv4_src_mask = Ipv4ToString(mnl_attr_get_payload(attr));
      } catch (...) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_IPV4_DST:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) != sizeof(in_addr)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      }
      filter->has_ipv4_dst = true;
      try {
        filter->ipv4_dst = Ipv4ToString(mnl_attr_get_payload(attr));
      } catch (...) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_IPV4_DST_MASK:
      if (mnl_attr_validate(attr, MNL_TYPE_BINARY) < 0 ||
          mnl_attr_get_payload_len(attr) != sizeof(in_addr)) {
        errno = EINVAL;
        return MNL_CB_ERROR;
      }
      filter->has_ipv4_dst_mask = true;
      try {
        filter->ipv4_dst_mask = Ipv4ToString(mnl_attr_get_payload(attr));
      } catch (...) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_TCP_DST:
      if (mnl_attr_validate(attr, MNL_TYPE_U16) < 0) {
        return MNL_CB_ERROR;
      }
      filter->has_tcp_dst = true;
      filter->tcp_dst = ntohs(mnl_attr_get_u16(attr));
      return MNL_CB_OK;
    case TCA_FLOWER_KEY_TCP_DST_MASK:
      if (mnl_attr_validate(attr, MNL_TYPE_U16) < 0) {
        return MNL_CB_ERROR;
      }
      filter->has_tcp_dst_mask = true;
      filter->tcp_dst_mask = ntohs(mnl_attr_get_u16(attr));
      return MNL_CB_OK;
    case TCA_FLOWER_ACT:
      if (mnl_attr_parse_nested(attr, ParseActionListEntry, filter) < 0) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseFilterAttr(const nlattr* attr, void* data) {
  auto* filter = static_cast<TcFilterInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_MAX) < 0) {
    return MNL_CB_OK;
  }

  switch (attr_type) {
    case TCA_KIND:
      if (mnl_attr_validate(attr, MNL_TYPE_STRING) < 0) {
        return MNL_CB_ERROR;
      }
      filter->kernel_kind = mnl_attr_get_str(attr);
      filter->kind = ParseTcFilterKind(filter->kernel_kind);
      return MNL_CB_OK;
    case TCA_OPTIONS:
      if (mnl_attr_parse_nested(attr, ParseFlowerAttr, filter) < 0) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    case TCA_STATS:
      return ParseLegacyFilterStats(attr, filter);
    case TCA_STATS2:
      if (mnl_attr_parse_nested(attr, ParseFilterStats2Attr, filter) < 0) {
        return MNL_CB_ERROR;
      }
      return MNL_CB_OK;
    default:
      return MNL_CB_OK;
  }
}

int ParseFilterMessage(const nlmsghdr* nlh, void* data) {
  auto* filters = static_cast<std::vector<TcFilterInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWTFILTER) {
    return MNL_CB_OK;
  }

  tcmsg message{};
  if (!CopyMessagePayload(nlh, &message)) {
    return MNL_CB_ERROR;
  }
  TcFilterInfo filter;
  filter.if_index = message.tcm_ifindex;
  filter.handle = message.tcm_handle;
  filter.parent = message.tcm_parent;
  filter.priority = TC_H_MAJ(message.tcm_info) >> 16U;
  filter.protocol =
      ntohs(static_cast<std::uint16_t>(TC_H_MIN(message.tcm_info)));
  filter.egress = message.tcm_parent == kClsactEgressParent;
  filter.ingress = message.tcm_parent == kClsactIngressParent;
  if (filter.if_index > 0) {
    char if_name[IF_NAMESIZE] = {};
    if (if_indextoname(static_cast<unsigned int>(filter.if_index), if_name) !=
        nullptr) {
      filter.if_name = if_name;
    }
  }
  if (mnl_attr_parse(nlh, sizeof(message), ParseFilterAttr, &filter) < 0) {
    return MNL_CB_ERROR;
  }
  if (filter.has_drop_action && filter.has_stats) {
    filter.drop_packets = filter.match_packets;
  }
  filters->push_back(std::move(filter));
  return MNL_CB_OK;
}

struct NamespaceQdiscState {
  std::vector<LinkInfo> links;
  std::vector<QdiscInfo> qdiscs;
};

struct NamespaceQdiscMutationState {
  std::vector<QdiscInfo> qdiscs_before;
  std::vector<QdiscInfo> qdiscs_after_replace;
  std::vector<QdiscInfo> qdiscs_after_delete;
};

struct NamespaceNetworkConditionState {
  std::vector<QdiscInfo> qdiscs_before;
  std::vector<QdiscInfo> qdiscs_after_apply;
  std::vector<QdiscInfo> qdiscs_after_delete;
};

struct NamespaceBandwidthLimitState {
  std::vector<QdiscInfo> qdiscs_before;
  std::vector<QdiscInfo> qdiscs_after_apply;
  std::vector<QdiscInfo> qdiscs_after_delete;
};

}  // namespace

#ifdef BBP_ENABLE_TEST_HOOKS
ScopedNetlinkFailurePlan::ScopedNetlinkFailurePlan(
    std::vector<NetlinkFailurePoint> failure_points) {
  for (const NetlinkFailurePoint& point : failure_points) {
    if (point.request_number == 0) {
      throw std::invalid_argument(
          "netlink failure request number must be greater than zero");
    }
  }
  std::sort(
      failure_points.begin(), failure_points.end(),
      [](const NetlinkFailurePoint& left, const NetlinkFailurePoint& right) {
        if (left.request_number != right.request_number) {
          return left.request_number < right.request_number;
        }
        return left.phase < right.phase;
      });
  if (std::adjacent_find(failure_points.begin(), failure_points.end(),
                         [](const NetlinkFailurePoint& left,
                            const NetlinkFailurePoint& right) {
                           return left.request_number == right.request_number &&
                                  left.phase == right.phase;
                         }) != failure_points.end()) {
    throw std::invalid_argument("duplicate netlink failure point");
  }

  std::lock_guard lock(NetlinkFailurePlanMutex());
  NetlinkFailurePlanState& state = MutableNetlinkFailurePlan();
  if (state.installed) {
    throw std::logic_error("a netlink failure plan is already installed");
  }
  state.installed = true;
  state.request_count = 0;
  state.points = std::move(failure_points);
  installed_ = true;
}

ScopedNetlinkFailurePlan::~ScopedNetlinkFailurePlan() {
  if (!installed_) {
    return;
  }
  std::lock_guard lock(NetlinkFailurePlanMutex());
  NetlinkFailurePlanState& state = MutableNetlinkFailurePlan();
  state.installed = false;
  state.request_count = 0;
  state.points.clear();
}

void ValidateNetlinkAcknowledgementForTest(
    const std::vector<std::uint8_t>& reply, std::uint32_t sequence,
    std::uint32_t port_id) {
  ValidateNetlinkAcknowledgement(reply.data(), reply.size(), sequence, port_id);
}
#endif

NetworkNamespace NetworkNamespace::Create() {
  pid_t helper_pid = -1;
  UniqueFd netns = StartNetworkNamespaceHelper(&helper_pid);
  return NetworkNamespace(helper_pid, netns.Release());
}

NetworkNamespace NetworkNamespace::Create(
    const std::filesystem::path& owner_cgroup_path) {
  pid_t helper_pid = -1;
  UniqueFd netns = StartNetworkNamespaceHelper(&helper_pid, &owner_cgroup_path);
  return NetworkNamespace(helper_pid, netns.Release());
}

NetworkNamespace::NetworkNamespace(NetworkNamespace&& other) noexcept {
  *this = std::move(other);
}

NetworkNamespace& NetworkNamespace::operator=(
    NetworkNamespace&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Stop();
  helper_pid_ = other.helper_pid_;
  fd_ = other.fd_;
  other.helper_pid_ = -1;
  other.fd_ = -1;
  return *this;
}

NetworkNamespace::~NetworkNamespace() { Stop(); }

void NetworkNamespace::Stop() {
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
  if (helper_pid_ > 0) {
    kill(helper_pid_, SIGKILL);
    int status = 0;
    while (waitpid(helper_pid_, &status, 0) < 0 && errno == EINTR) {
    }
    helper_pid_ = -1;
  }
}

std::vector<LinkInfo> ListNetworkLinks() {
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_GETLINK;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  message->ifi_family = AF_PACKET;

  socket.Send(nlh, nlh->nlmsg_len);

  std::vector<LinkInfo> links;
  while (true) {
    const ssize_t received = socket.Receive(buffer.data(), buffer.size());
    const int status =
        mnl_cb_run(buffer.data(), static_cast<size_t>(received), sequence,
                   socket.PortId(), ParseLinkMessage, &links);
    if (status == MNL_CB_ERROR) {
      throw std::runtime_error(std::string("mnl_cb_run failed: ") +
                               std::strerror(errno));
    }
    if (status == MNL_CB_STOP) {
      break;
    }
  }

  return links;
}

std::vector<AddressInfo> ListIpv4Addresses() {
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_GETADDR;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<rtgenmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(rtgenmsg)));
  message->rtgen_family = AF_INET;

  socket.Send(nlh, nlh->nlmsg_len);

  std::vector<AddressInfo> addresses;
  while (true) {
    const ssize_t received = socket.Receive(buffer.data(), buffer.size());
    const int status =
        mnl_cb_run(buffer.data(), static_cast<size_t>(received), sequence,
                   socket.PortId(), ParseAddressMessage, &addresses);
    if (status == MNL_CB_ERROR) {
      throw std::runtime_error(std::string("mnl_cb_run failed: ") +
                               std::strerror(errno));
    }
    if (status == MNL_CB_STOP) {
      break;
    }
  }

  return addresses;
}

std::vector<RouteInfo> ListIpv4Routes() {
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_GETROUTE;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<rtmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(rtmsg)));
  message->rtm_family = AF_INET;

  socket.Send(nlh, nlh->nlmsg_len);

  std::vector<RouteInfo> routes;
  while (true) {
    const ssize_t received = socket.Receive(buffer.data(), buffer.size());
    const int status =
        mnl_cb_run(buffer.data(), static_cast<size_t>(received), sequence,
                   socket.PortId(), ParseRouteMessage, &routes);
    if (status == MNL_CB_ERROR) {
      throw std::runtime_error(std::string("mnl_cb_run failed: ") +
                               std::strerror(errno));
    }
    if (status == MNL_CB_STOP) {
      break;
    }
  }

  return routes;
}

std::vector<QdiscInfo> ListQdiscs() {
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_GETQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;

  socket.Send(nlh, nlh->nlmsg_len);

  std::vector<QdiscInfo> qdiscs;
  while (true) {
    const ssize_t received = socket.Receive(buffer.data(), buffer.size());
    const int status =
        mnl_cb_run(buffer.data(), static_cast<size_t>(received), sequence,
                   socket.PortId(), ParseQdiscMessage, &qdiscs);
    if (status == MNL_CB_ERROR) {
      throw std::runtime_error(std::string("mnl_cb_run failed: ") +
                               std::strerror(errno));
    }
    if (status == MNL_CB_STOP) {
      break;
    }
  }

  return qdiscs;
}

namespace {

void AppendTcFiltersForLinkParent(const LinkInfo& link, std::uint32_t parent,
                                  std::vector<TcFilterInfo>* filters) {
  if (link.index <= 0) {
    return;
  }
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_GETTFILTER;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = link.index;
  message->tcm_parent = parent;

  socket.Send(nlh, nlh->nlmsg_len);
  while (true) {
    const ssize_t received = socket.Receive(buffer.data(), buffer.size());
    const int status =
        mnl_cb_run(buffer.data(), static_cast<size_t>(received), sequence,
                   socket.PortId(), ParseFilterMessage, filters);
    if (status == MNL_CB_ERROR) {
      throw std::runtime_error(std::string("mnl_cb_run failed: ") +
                               std::strerror(errno));
    }
    if (status == MNL_CB_STOP) {
      break;
    }
  }
}

void AppendTcFiltersForLink(const LinkInfo& link,
                            const std::vector<QdiscInfo>& qdiscs,
                            std::vector<TcFilterInfo>* filters) {
  const std::array<std::uint32_t, 2> clsact_parents = {kClsactIngressParent,
                                                       kClsactEgressParent};
  if (link.index <= 0 ||
      !HasQdiscKindForInterface(qdiscs, link.name, QdiscKind::kClsact)) {
    return;
  }
  for (const std::uint32_t parent : clsact_parents) {
    AppendTcFiltersForLinkParent(link, parent, filters);
  }
}

}  // namespace

std::vector<TcFilterInfo> ListTcFilters() {
  std::vector<TcFilterInfo> filters;
  const std::vector<QdiscInfo> qdiscs = ListQdiscs();
  for (const LinkInfo& link : ListNetworkLinks()) {
    AppendTcFiltersForLink(link, qdiscs, &filters);
  }
  return filters;
}

std::vector<TcFilterInfo> ListTcFiltersForInterface(
    const std::string& if_name) {
  RequireInterfaceName(if_name);
  const std::vector<LinkInfo> links = ListNetworkLinks();
  const auto link = std::find_if(
      links.begin(), links.end(),
      [&](const LinkInfo& candidate) { return candidate.name == if_name; });
  if (link == links.end()) {
    throw std::runtime_error("network interface not found: " + if_name);
  }
  std::vector<TcFilterInfo> filters;
  AppendTcFiltersForLink(*link, ListQdiscs(), &filters);
  return filters;
}

std::vector<TcFilterInfo> ListTcFiltersForInterfaceParentInNamespace(
    int netns_fd, const std::string& if_name, std::uint32_t parent) {
  if (netns_fd < 0) {
    throw std::runtime_error("invalid network namespace fd");
  }
  return ExecuteInNetworkNamespace(netns_fd, [&]() {
    RequireInterfaceName(if_name);
    const std::vector<LinkInfo> links = ListNetworkLinks();
    const auto link = std::find_if(
        links.begin(), links.end(),
        [&](const LinkInfo& candidate) { return candidate.name == if_name; });
    if (link == links.end()) {
      throw std::runtime_error("network interface not found: " + if_name);
    }
    std::vector<TcFilterInfo> filters;
    AppendTcFiltersForLinkParent(*link, parent, &filters);
    return filters;
  });
}

void CreateVethPair(const std::string& host_name,
                    const std::string& peer_name) {
  RequireInterfaceName(host_name);
  RequireInterfaceName(peer_name);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWLINK;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  message->ifi_family = AF_UNSPEC;

  mnl_attr_put_str(nlh, IFLA_IFNAME, host_name.c_str());
  nlattr* link_info = mnl_attr_nest_start(nlh, IFLA_LINKINFO);
  mnl_attr_put_strz(nlh, IFLA_INFO_KIND, "veth");
  nlattr* info_data = mnl_attr_nest_start(nlh, IFLA_INFO_DATA);
  nlattr* peer = mnl_attr_nest_start(nlh, VETH_INFO_PEER);
  auto* peer_message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  peer_message->ifi_family = AF_UNSPEC;
  mnl_attr_put_str(nlh, IFLA_IFNAME, peer_name.c_str());
  mnl_attr_nest_end(nlh, peer);
  mnl_attr_nest_end(nlh, info_data);
  mnl_attr_nest_end(nlh, link_info);

  SendNetlinkRequest(nlh, sequence);
}

void DeleteLink(const std::string& name) {
  RequireInterfaceName(name);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_DELLINK;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  message->ifi_family = AF_UNSPEC;
  mnl_attr_put_str(nlh, IFLA_IFNAME, name.c_str());

  SendNetlinkRequest(nlh, sequence);
}

void MoveLinkToNamespace(const std::string& name, int netns_fd) {
  RequireInterfaceName(name);
  if (netns_fd < 0) {
    throw std::runtime_error("invalid network namespace fd");
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWLINK;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  message->ifi_family = AF_UNSPEC;
  mnl_attr_put_str(nlh, IFLA_IFNAME, name.c_str());
  mnl_attr_put_u32(nlh, IFLA_NET_NS_FD, static_cast<uint32_t>(netns_fd));

  SendNetlinkRequest(nlh, sequence);
}

void SetLinkUp(const std::string& name, bool up) {
  RequireInterfaceName(name);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWLINK;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  message->ifi_family = AF_UNSPEC;
  message->ifi_change = IFF_UP;
  message->ifi_flags = up ? static_cast<unsigned int>(IFF_UP) : 0U;
  mnl_attr_put_str(nlh, IFLA_IFNAME, name.c_str());

  SendNetlinkRequest(nlh, sequence);
}

void AddIpv4Address(const std::string& if_name, const std::string& address,
                    std::uint8_t prefix_len) {
  RequireInterfaceName(if_name);
  if (prefix_len > 32U) {
    throw std::runtime_error("IPv4 prefix length must be 0..32");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  in_addr ipv4_address{};
  if (inet_pton(AF_INET, address.c_str(), &ipv4_address) != 1) {
    throw std::runtime_error("invalid IPv4 address: " + address);
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWADDR;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifaddrmsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifaddrmsg)));
  message->ifa_family = AF_INET;
  message->ifa_prefixlen = prefix_len;
  message->ifa_flags = IFA_F_PERMANENT;
  message->ifa_scope = RT_SCOPE_UNIVERSE;
  message->ifa_index = if_index;

  mnl_attr_put_u32(nlh, IFA_LOCAL, ipv4_address.s_addr);
  mnl_attr_put_u32(nlh, IFA_ADDRESS, ipv4_address.s_addr);

  SendNetlinkRequest(nlh, sequence);
}

void DeleteIpv4Address(const std::string& if_name, const std::string& address,
                       std::uint8_t prefix_len) {
  RequireInterfaceName(if_name);
  if (prefix_len > 32U) {
    throw std::runtime_error("IPv4 prefix length must be 0..32");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  in_addr ipv4_address{};
  if (inet_pton(AF_INET, address.c_str(), &ipv4_address) != 1) {
    throw std::runtime_error("invalid IPv4 address: " + address);
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_DELADDR;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message = static_cast<ifaddrmsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifaddrmsg)));
  message->ifa_family = AF_INET;
  message->ifa_prefixlen = prefix_len;
  message->ifa_flags = IFA_F_PERMANENT;
  message->ifa_scope = RT_SCOPE_UNIVERSE;
  message->ifa_index = if_index;

  mnl_attr_put_u32(nlh, IFA_LOCAL, ipv4_address.s_addr);
  mnl_attr_put_u32(nlh, IFA_ADDRESS, ipv4_address.s_addr);

  SendNetlinkRequest(nlh, sequence);
}

void AddIpv4Route(const std::string& if_name, const std::string& destination,
                  std::uint8_t prefix_len, const std::string& gateway) {
  RequireInterfaceName(if_name);
  if (prefix_len > 32U) {
    throw std::runtime_error("IPv4 route prefix length must be 0..32");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  in_addr destination_address{};
  if (inet_pton(AF_INET, destination.c_str(), &destination_address) != 1) {
    throw std::runtime_error("invalid IPv4 route destination: " + destination);
  }

  in_addr gateway_address{};
  if (!gateway.empty() &&
      inet_pton(AF_INET, gateway.c_str(), &gateway_address) != 1) {
    throw std::runtime_error("invalid IPv4 route gateway: " + gateway);
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWROUTE;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<rtmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(rtmsg)));
  message->rtm_family = AF_INET;
  message->rtm_dst_len = prefix_len;
  message->rtm_src_len = 0;
  message->rtm_tos = 0;
  message->rtm_protocol = RTPROT_STATIC;
  message->rtm_table = RT_TABLE_MAIN;
  message->rtm_type = RTN_UNICAST;
  message->rtm_scope = gateway.empty() ? RT_SCOPE_LINK : RT_SCOPE_UNIVERSE;
  message->rtm_flags = 0;

  mnl_attr_put_u32(nlh, RTA_DST, destination_address.s_addr);
  mnl_attr_put_u32(nlh, RTA_OIF, if_index);
  if (!gateway.empty()) {
    mnl_attr_put_u32(nlh, RTA_GATEWAY, gateway_address.s_addr);
  }

  SendNetlinkRequest(nlh, sequence);
}

void DeleteIpv4Route(const std::string& if_name, const std::string& destination,
                     std::uint8_t prefix_len, const std::string& gateway) {
  RequireInterfaceName(if_name);
  if (prefix_len > 32U) {
    throw std::runtime_error("IPv4 route prefix length must be 0..32");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  in_addr destination_address{};
  if (inet_pton(AF_INET, destination.c_str(), &destination_address) != 1) {
    throw std::runtime_error("invalid IPv4 route destination: " + destination);
  }

  in_addr gateway_address{};
  if (!gateway.empty() &&
      inet_pton(AF_INET, gateway.c_str(), &gateway_address) != 1) {
    throw std::runtime_error("invalid IPv4 route gateway: " + gateway);
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_DELROUTE;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<rtmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(rtmsg)));
  message->rtm_family = AF_INET;
  message->rtm_dst_len = prefix_len;
  message->rtm_src_len = 0;
  message->rtm_tos = 0;
  message->rtm_protocol = RTPROT_STATIC;
  message->rtm_table = RT_TABLE_MAIN;
  message->rtm_type = RTN_UNICAST;
  message->rtm_scope = gateway.empty() ? RT_SCOPE_LINK : RT_SCOPE_UNIVERSE;
  message->rtm_flags = 0;

  mnl_attr_put_u32(nlh, RTA_DST, destination_address.s_addr);
  mnl_attr_put_u32(nlh, RTA_OIF, if_index);
  if (!gateway.empty()) {
    mnl_attr_put_u32(nlh, RTA_GATEWAY, gateway_address.s_addr);
  }

  SendNetlinkRequest(nlh, sequence);
}

void ReplaceRootPfifoQdisc(const std::string& if_name,
                           std::uint32_t limit_packets) {
  RequireInterfaceName(if_name);
  if (limit_packets == 0U) {
    throw std::runtime_error("pfifo limit must be greater than zero");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = kRootQdiscHandle;
  message->tcm_parent = TC_H_ROOT;
  message->tcm_info = 0;

  tc_fifo_qopt options{};
  options.limit = limit_packets;
  mnl_attr_put_strz(nlh, TCA_KIND, "pfifo");
  mnl_attr_put(nlh, TCA_OPTIONS, sizeof(options), &options);

  SendNetlinkRequest(nlh, sequence);
}

void ReplaceNetemQdisc(const std::string& if_name, std::uint32_t parent,
                       std::uint32_t handle,
                       const NetworkCondition& condition) {
  RequireInterfaceName(if_name);
  ValidateNetworkCondition(condition);
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = handle;
  message->tcm_parent = parent;
  message->tcm_info = 0;

  tc_netem_qopt options{};
  options.latency = condition.delay_ms * 1000U;
  options.jitter = condition.jitter_ms * 1000U;
  options.loss = NetemProbability(condition.loss_basis_points);
  options.duplicate = NetemProbability(condition.duplicate_basis_points);
  options.limit = condition.limit_packets;
  options.gap = condition.reorder_basis_points == 0U ? 0U : 1U;

  mnl_attr_put_strz(nlh, TCA_KIND, "netem");
  nlattr* netem_options = mnl_attr_nest_start(nlh, TCA_OPTIONS);
  PutRawPayload(nlh, &options, sizeof(options));
  if (condition.reorder_basis_points != 0U) {
    tc_netem_reorder reorder{};
    reorder.probability = NetemProbability(condition.reorder_basis_points);
    reorder.correlation = 0;
    mnl_attr_put(nlh, TCA_NETEM_REORDER, sizeof(reorder), &reorder);
  }
  if (condition.corrupt_basis_points != 0U) {
    tc_netem_corrupt corrupt{};
    corrupt.probability = NetemProbability(condition.corrupt_basis_points);
    corrupt.correlation = 0;
    mnl_attr_put(nlh, TCA_NETEM_CORRUPT, sizeof(corrupt), &corrupt);
  }
  mnl_attr_nest_end(nlh, netem_options);

  SendNetlinkRequest(nlh, sequence);
}

void ReplaceRootNetemQdisc(const std::string& if_name,
                           const NetworkCondition& condition) {
  ReplaceNetemQdisc(if_name, TC_H_ROOT, kRootQdiscHandle, condition);
}

void ReplaceTbfQdisc(const std::string& if_name, std::uint32_t parent,
                     std::uint32_t handle, const NetworkCondition& condition) {
  RequireInterfaceName(if_name);
  ValidateNetworkCondition(condition);
  if (condition.bandwidth_mbps == 0U) {
    throw std::runtime_error("TBF bandwidth must be greater than zero");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  const std::uint64_t rate_bytes_per_second = TbfRateBytesPerSecond(condition);
  const std::uint32_t burst_bytes = TbfBurstBytes(condition);
  const std::uint32_t limit_bytes = TbfLimitBytes(condition);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = handle;
  message->tcm_parent = parent;
  message->tcm_info = 0;

  tc_tbf_qopt options{};
  options.rate.linklayer = TC_LINKLAYER_ETHERNET;
  options.rate.rate = SaturatingUint32(rate_bytes_per_second);
  options.limit = limit_bytes;

  mnl_attr_put_strz(nlh, TCA_KIND, "tbf");
  nlattr* tbf_options = mnl_attr_nest_start(nlh, TCA_OPTIONS);
  mnl_attr_put(nlh, TCA_TBF_PARMS, sizeof(options), &options);
  if (rate_bytes_per_second >
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max())) {
    mnl_attr_put_u64(nlh, TCA_TBF_RATE64, rate_bytes_per_second);
  }
  mnl_attr_put_u32(nlh, TCA_TBF_BURST, burst_bytes);
  mnl_attr_nest_end(nlh, tbf_options);

  SendNetlinkRequest(nlh, sequence);
}

void ReplaceRootTbfQdisc(const std::string& if_name,
                         const NetworkCondition& condition) {
  ReplaceTbfQdisc(if_name, TC_H_ROOT, kRootQdiscHandle, condition);
}

void DeleteRootQdisc(const std::string& if_name) {
  RequireInterfaceName(if_name);
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_DELQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = 0;
  message->tcm_parent = TC_H_ROOT;
  message->tcm_info = 0;

  SendNetlinkRequest(nlh, sequence);
}

namespace {

void ReplaceDirectionalPrioQdisc(const std::string& if_name) {
  RequireInterfaceName(if_name);
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = kDirectionalRootHandle;
  message->tcm_parent = TC_H_ROOT;

  tc_prio_qopt options{};
  options.bands = kDirectionalBandCount;
  mnl_attr_put_strz(nlh, TCA_KIND, "prio");
  mnl_attr_put(nlh, TCA_OPTIONS, sizeof(options), &options);
  SendNetlinkRequest(nlh, sequence);
}

void ReplaceDirectionalFlowerFilter(const std::string& if_name,
                                    const DirectionalNetworkPolicy& policy) {
  RequireInterfaceName(if_name);
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }
  const in_addr destination =
      ParseIpv4Address(policy.destination_address, "policy destination");
  const std::uint32_t exact_mask = 0xFFFFFFFFU;

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWTFILTER;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = DirectionalFilterHandle(policy.band);
  message->tcm_parent = kDirectionalRootHandle;
  message->tcm_info = TC_H_MAKE(
      kDirectionalFilterPriority << 16U,
      static_cast<std::uint32_t>(htons(static_cast<std::uint16_t>(ETH_P_IP))));

  mnl_attr_put_strz(nlh, TCA_KIND, "flower");
  nlattr* options = mnl_attr_nest_start(nlh, TCA_OPTIONS);
  mnl_attr_put_u32(nlh, TCA_FLOWER_CLASSID, DirectionalClassId(policy.band));
  mnl_attr_put_u16(nlh, TCA_FLOWER_KEY_ETH_TYPE,
                   htons(static_cast<std::uint16_t>(ETH_P_IP)));
  mnl_attr_put_u32(nlh, TCA_FLOWER_KEY_IPV4_DST, destination.s_addr);
  mnl_attr_put_u32(nlh, TCA_FLOWER_KEY_IPV4_DST_MASK, exact_mask);
  mnl_attr_nest_end(nlh, options);
  SendNetlinkRequest(nlh, sequence);
}

std::vector<TcFilterInfo> ListDirectionalFilters(const std::string& if_name) {
  const std::vector<LinkInfo> links = ListNetworkLinks();
  const auto link = std::find_if(
      links.begin(), links.end(),
      [&](const LinkInfo& candidate) { return candidate.name == if_name; });
  if (link == links.end()) {
    throw std::runtime_error("network interface not found: " + if_name);
  }
  std::vector<TcFilterInfo> filters;
  AppendTcFiltersForLinkParent(*link, kDirectionalRootHandle, &filters);
  return filters;
}

bool HasOwnedDirectionalRoot(const std::vector<QdiscInfo>& qdiscs,
                             const std::string& if_name) {
  bool found = false;
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name != if_name || qdisc.parent != TC_H_ROOT ||
        qdisc.handle == 0U) {
      continue;
    }
    if (qdisc.handle != kDirectionalRootHandle ||
        qdisc.kind != QdiscKind::kPrio || !qdisc.has_prio_options ||
        qdisc.prio_bands != kDirectionalBandCount) {
      throw std::runtime_error(
          "refusing to replace a non-owned root qdisc on " + if_name);
    }
    found = true;
  }
  return found;
}

void ApplyDirectionalNetworkPolicies(
    const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies) {
  ValidateDirectionalNetworkPolicies(policies);
  const bool had_owned_root = HasOwnedDirectionalRoot(ListQdiscs(), if_name);
  if (had_owned_root) {
    DeleteRootQdisc(if_name);
  }
  if (policies.empty()) {
    return;
  }

  ReplaceDirectionalPrioQdisc(if_name);
  for (const DirectionalNetworkPolicy& policy : policies) {
    const std::uint32_t class_id = DirectionalClassId(policy.band);
    if (HasBandwidthCondition(policy.condition)) {
      const std::uint32_t tbf_handle = DirectionalTbfHandle(policy.band);
      ReplaceTbfQdisc(if_name, class_id, tbf_handle, policy.condition);
      if (HasNetemCondition(policy.condition)) {
        ReplaceNetemQdisc(if_name, TC_H_MAKE(TC_H_MAJ(tbf_handle), 1U),
                          DirectionalNetemHandle(policy.band),
                          policy.condition);
      }
    } else {
      ReplaceNetemQdisc(if_name, class_id, DirectionalNetemHandle(policy.band),
                        policy.condition);
    }
    ReplaceDirectionalFlowerFilter(if_name, policy);
  }

  const std::vector<QdiscInfo> qdiscs = ListQdiscs();
  const std::vector<TcFilterInfo> filters = ListDirectionalFilters(if_name);
  if (!DirectionalNetworkPoliciesMatch(qdiscs, filters, if_name, policies)) {
    std::ostringstream detail;
    detail << "directional network policy kernel read-back mismatch on "
           << if_name << "; filters=" << filters.size();
    for (const TcFilterInfo& filter : filters) {
      detail << " [kind=" << filter.kernel_kind << " handle=" << filter.handle
             << " parent=" << filter.parent << " priority=" << filter.priority
             << " protocol=" << filter.protocol << ']';
    }
    throw std::runtime_error(detail.str());
  }
}

}  // namespace

void UpdateDirectionalNetworkPoliciesInNamespace(
    int netns_fd, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& previous,
    const std::vector<DirectionalNetworkPolicy>& desired) {
  if (netns_fd < 0) {
    throw std::runtime_error("invalid network namespace fd");
  }
  RequireInterfaceName(if_name);
  ValidateDirectionalNetworkPolicies(previous);
  ValidateDirectionalNetworkPolicies(desired);
  ExecuteInNetworkNamespace(netns_fd, [&]() {
    const std::vector<QdiscInfo> qdiscs = ListQdiscs();
    static_cast<void>(HasOwnedDirectionalRoot(qdiscs, if_name));
    const std::vector<TcFilterInfo> filters = ListDirectionalFilters(if_name);
    if (!DirectionalNetworkPoliciesMatch(qdiscs, filters, if_name, previous)) {
      throw std::runtime_error(
          "directional network policy state does not match expected prior "
          "state on " +
          if_name);
    }
    try {
      ApplyDirectionalNetworkPolicies(if_name, desired);
    } catch (const std::exception& apply_error) {
      const std::string apply_message = apply_error.what();
      try {
        ApplyDirectionalNetworkPolicies(if_name, previous);
      } catch (const std::exception& rollback_error) {
        throw std::runtime_error(apply_message +
                                 "; rollback failed: " + rollback_error.what());
      }
      throw std::runtime_error(apply_message + "; prior state restored");
    }
  });
}

void EnsureClsactQdisc(const std::string& if_name) {
  RequireInterfaceName(if_name);
  if (HasQdiscKindForInterface(ListQdiscs(), if_name, QdiscKind::kClsact)) {
    return;
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWQDISC;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = kClsactQdiscHandle;
  message->tcm_parent = TC_H_CLSACT;
  message->tcm_info = 0;

  mnl_attr_put_strz(nlh, TCA_KIND, "clsact");

  SendNetlinkRequest(nlh, sequence);
}

void RequireDropFilterHandle(std::uint32_t handle) {
  if (handle == 0U) {
    throw std::runtime_error("drop filter handle must be greater than zero");
  }
}

void ReplaceEgressIpv4TcpDropFilter(const std::string& if_name,
                                    const std::string& dst_address,
                                    std::uint16_t dst_port,
                                    std::uint32_t handle) {
  ReplaceEgressIpv4TcpDropFilter(if_name, "", dst_address, dst_port, handle);
}

void ReplaceEgressIpv4TcpDropFilter(const std::string& if_name,
                                    const std::string& src_address,
                                    const std::string& dst_address,
                                    std::uint16_t dst_port,
                                    std::uint32_t handle) {
  RequireInterfaceName(if_name);
  RequireDropFilterHandle(handle);
  if (dst_port == 0U) {
    throw std::runtime_error("drop filter TCP destination port must be > 0");
  }
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }
  std::optional<in_addr> ipv4_source;
  if (!src_address.empty()) {
    ipv4_source = ParseIpv4Address(src_address, "source");
  }
  const in_addr ipv4_address = ParseIpv4Address(dst_address, "destination");
  const std::uint32_t ipv4_mask = 0xFFFFFFFFU;

  EnsureClsactQdisc(if_name);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_NEWTFILTER;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = handle;
  message->tcm_parent = kClsactEgressParent;
  message->tcm_info = TC_H_MAKE(
      kDropFilterPriority << 16U,
      static_cast<std::uint32_t>(htons(static_cast<std::uint16_t>(ETH_P_IP))));

  mnl_attr_put_strz(nlh, TCA_KIND, "flower");
  nlattr* flower_options = mnl_attr_nest_start(nlh, TCA_OPTIONS);
  mnl_attr_put_u16(nlh, TCA_FLOWER_KEY_ETH_TYPE,
                   htons(static_cast<std::uint16_t>(ETH_P_IP)));
  mnl_attr_put_u8(nlh, TCA_FLOWER_KEY_IP_PROTO,
                  static_cast<std::uint8_t>(IPPROTO_TCP));
  if (ipv4_source) {
    mnl_attr_put_u32(nlh, TCA_FLOWER_KEY_IPV4_SRC, ipv4_source->s_addr);
    mnl_attr_put_u32(nlh, TCA_FLOWER_KEY_IPV4_SRC_MASK, ipv4_mask);
  }
  mnl_attr_put_u32(nlh, TCA_FLOWER_KEY_IPV4_DST, ipv4_address.s_addr);
  mnl_attr_put_u32(nlh, TCA_FLOWER_KEY_IPV4_DST_MASK, ipv4_mask);
  mnl_attr_put_u16(nlh, TCA_FLOWER_KEY_TCP_DST, htons(dst_port));
  mnl_attr_put_u16(nlh, TCA_FLOWER_KEY_TCP_DST_MASK,
                   htons(static_cast<std::uint16_t>(0xFFFFU)));

  nlattr* actions = mnl_attr_nest_start(nlh, TCA_FLOWER_ACT);
  nlattr* action = mnl_attr_nest_start(nlh, 1U);
  mnl_attr_put_strz(nlh, TCA_ACT_KIND, "gact");
  nlattr* action_options = mnl_attr_nest_start(nlh, TCA_ACT_OPTIONS);
  tc_gact gact{};
  gact.action = TC_ACT_SHOT;
  mnl_attr_put(nlh, TCA_GACT_PARMS, sizeof(gact), &gact);
  mnl_attr_nest_end(nlh, action_options);
  mnl_attr_nest_end(nlh, action);
  mnl_attr_nest_end(nlh, actions);
  mnl_attr_nest_end(nlh, flower_options);

  SendNetlinkRequest(nlh, sequence);
}

void DeleteEgressIpv4TcpDropFilter(const std::string& if_name,
                                   std::uint32_t handle) {
  RequireInterfaceName(if_name);
  RequireDropFilterHandle(handle);
  const unsigned int if_index = if_nametoindex(if_name.c_str());
  if (if_index == 0U) {
    throw std::runtime_error("if_nametoindex failed for " + if_name + ": " +
                             std::strerror(errno));
  }

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_DELTFILTER;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
  const uint32_t sequence = NextSequence();
  nlh->nlmsg_seq = sequence;

  auto* message =
      static_cast<tcmsg*>(mnl_nlmsg_put_extra_header(nlh, sizeof(tcmsg)));
  message->tcm_family = AF_UNSPEC;
  message->tcm_ifindex = static_cast<int>(if_index);
  message->tcm_handle = handle;
  message->tcm_parent = kClsactEgressParent;
  message->tcm_info = TC_H_MAKE(
      kDropFilterPriority << 16U,
      static_cast<std::uint32_t>(htons(static_cast<std::uint16_t>(ETH_P_IP))));
  mnl_attr_put_strz(nlh, TCA_KIND, "flower");

  SendNetlinkRequest(nlh, sequence);
}

void ReplaceNetworkConditionQdisc(const std::string& if_name,
                                  const NetworkCondition& condition) {
  ValidateNetworkCondition(condition);
  try {
    DeleteRootQdisc(if_name);
  } catch (const std::exception&) {
  }

  if (HasBandwidthCondition(condition)) {
    ReplaceRootTbfQdisc(if_name, condition);
    if (HasNetemCondition(condition)) {
      ReplaceNetemQdisc(if_name, kTbfChildClassId, kChildNetemQdiscHandle,
                        condition);
    }
    return;
  }

  ReplaceRootNetemQdisc(if_name, condition);
}

void SetupNodeVethNetwork(int netns_fd, const NodeVethConfig& config) {
  if (netns_fd < 0) {
    throw std::runtime_error("invalid network namespace fd");
  }
  RequireInterfaceName(config.host_name);
  RequireInterfaceName(config.peer_name);
  if (config.prefix_len > 32U) {
    throw std::runtime_error("node network prefix length must be 0..32");
  }
  if (config.host_name == config.peer_name) {
    throw std::runtime_error("node veth endpoint names must be distinct");
  }

  const std::vector<LinkInfo> existing_links = ListNetworkLinks();
  if (HasLinkNamed(existing_links, config.host_name) ||
      HasLinkNamed(existing_links, config.peer_name)) {
    throw std::runtime_error(
        "refusing to replace a pre-existing, non-owned veth endpoint");
  }

  bool pair_created = false;
  try {
    CreateVethPair(config.host_name, config.peer_name);
    pair_created = true;
    AddIpv4Address(config.host_name, config.host_address, config.prefix_len);
    SetLinkUp(config.host_name, true);
    if (config.apply_condition) {
      ReplaceNetworkConditionQdisc(config.host_name, config.condition);
    }
    MoveLinkToNamespace(config.peer_name, netns_fd);

    ExecuteInNetworkNamespace(netns_fd, [&config]() {
      SetLinkUp("lo", true);
      AddIpv4Address(config.peer_name, config.node_address, config.prefix_len);
      SetLinkUp(config.peer_name, true);
      AddIpv4Route(config.peer_name, "0.0.0.0", 0, config.host_address);
    });
  } catch (...) {
    if (pair_created) {
      TryDeleteLink(config.host_name);
    }
    throw;
  }
}

void DeleteNodeVethNetwork(const NodeVethConfig& config) {
  TryDeleteLink(config.host_name);
  TryDeleteLink(config.peer_name);
}

std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd,
                                   []() { return ListNetworkLinks(); });
}

std::vector<AddressInfo> ListIpv4AddressesInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd,
                                   []() { return ListIpv4Addresses(); });
}

std::vector<RouteInfo> ListIpv4RoutesInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd, []() { return ListIpv4Routes(); });
}

std::vector<QdiscInfo> ListQdiscsInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd, []() { return ListQdiscs(); });
}

DirectionalNetworkPolicyStats ReadDirectionalNetworkPolicyStatsInNamespace(
    int netns_fd, const std::string& if_name,
    const std::vector<DirectionalNetworkPolicy>& policies) {
  if (netns_fd < 0) {
    throw std::runtime_error("invalid network namespace fd");
  }
  RequireInterfaceName(if_name);
  ValidateDirectionalNetworkPolicies(policies);
  return ExecuteInNetworkNamespace(netns_fd, [&]() {
    const std::vector<QdiscInfo> qdiscs = ListQdiscs();
    const std::vector<TcFilterInfo> filters = ListDirectionalFilters(if_name);
    return SummarizeDirectionalNetworkPolicyStats(qdiscs, filters, if_name,
                                                  policies);
  });
}

NetworkNamespaceProbe ProbeIsolatedNetworkNamespace() {
  NetworkNamespaceProbe probe;
  probe.parent_links = ListNetworkLinks();
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&probe.helper_pid);

  try {
    probe.namespace_links = ListNetworkLinksInNamespace(namespace_fd.get());
  } catch (...) {
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

NetworkConditionProbe ProbeNetworkCondition() {
  NetworkConditionProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.condition.delay_ms = 80;
  probe.condition.jitter_ms = 10;
  probe.condition.loss_basis_points = 25;
  probe.condition.duplicate_basis_points = 10;
  probe.condition.corrupt_basis_points = 15;
  probe.condition.reorder_basis_points = 20;
  probe.condition.limit_packets = 1000;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());

    NamespaceNetworkConditionState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          NamespaceNetworkConditionState state;
          state.qdiscs_before = ListQdiscs();
          ReplaceNetworkConditionQdisc(probe.peer_name, probe.condition);
          state.qdiscs_after_apply = ListQdiscs();
          DeleteRootQdisc(probe.peer_name);
          state.qdiscs_after_delete = ListQdiscs();
          return state;
        });
    probe.namespace_qdiscs_before = std::move(namespace_state.qdiscs_before);
    probe.namespace_qdiscs_after_apply =
        std::move(namespace_state.qdiscs_after_apply);
    probe.namespace_qdiscs_after_delete =
        std::move(namespace_state.qdiscs_after_delete);

    if (!QdiscsMatchNetworkCondition(probe.namespace_qdiscs_after_apply,
                                     probe.peer_name, probe.condition,
                                     nullptr)) {
      throw std::runtime_error("netem qdisc options did not match condition");
    }
    if (HasQdiscKindForInterface(probe.namespace_qdiscs_after_delete,
                                 probe.peer_name, QdiscKind::kNetem)) {
      throw std::runtime_error("netem qdisc remained after delete");
    }
    if (!HasQdiscForInterface(probe.namespace_qdiscs_after_delete,
                              probe.peer_name)) {
      throw std::runtime_error("qdisc dump lost veth peer after netem delete");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

NetworkConditionProbe ProbeCombinedNetworkCondition() {
  NetworkConditionProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.condition.bandwidth_mbps = 20;
  probe.condition.delay_ms = 40;
  probe.condition.jitter_ms = 5;
  probe.condition.loss_basis_points = 10;
  probe.condition.duplicate_basis_points = 5;
  probe.condition.corrupt_basis_points = 5;
  probe.condition.reorder_basis_points = 10;
  probe.condition.limit_packets = 1000;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());

    NamespaceNetworkConditionState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          NamespaceNetworkConditionState state;
          state.qdiscs_before = ListQdiscs();
          ReplaceNetworkConditionQdisc(probe.peer_name, probe.condition);
          state.qdiscs_after_apply = ListQdiscs();
          DeleteRootQdisc(probe.peer_name);
          state.qdiscs_after_delete = ListQdiscs();
          return state;
        });
    probe.namespace_qdiscs_before = std::move(namespace_state.qdiscs_before);
    probe.namespace_qdiscs_after_apply =
        std::move(namespace_state.qdiscs_after_apply);
    probe.namespace_qdiscs_after_delete =
        std::move(namespace_state.qdiscs_after_delete);

    QdiscInfo summary;
    if (!QdiscsMatchNetworkCondition(probe.namespace_qdiscs_after_apply,
                                     probe.peer_name, probe.condition,
                                     &summary)) {
      throw std::runtime_error(
          "combined TBF/netem qdisc options did not match condition");
    }
    if (summary.kind != QdiscKind::kTbfNetem) {
      throw std::runtime_error("combined qdisc summary kind was not tbf+netem");
    }
    if (HasQdiscKindForInterface(probe.namespace_qdiscs_after_delete,
                                 probe.peer_name, QdiscKind::kTbf) ||
        HasQdiscKindForInterface(probe.namespace_qdiscs_after_delete,
                                 probe.peer_name, QdiscKind::kNetem)) {
      throw std::runtime_error("combined qdisc remained after delete");
    }
    if (!HasQdiscForInterface(probe.namespace_qdiscs_after_delete,
                              probe.peer_name)) {
      throw std::runtime_error(
          "qdisc dump lost veth peer after combined qdisc delete");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

BandwidthLimitProbe ProbeBandwidthLimit() {
  BandwidthLimitProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.condition.bandwidth_mbps = 20;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());

    NamespaceBandwidthLimitState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          NamespaceBandwidthLimitState state;
          state.qdiscs_before = ListQdiscs();
          ReplaceRootTbfQdisc(probe.peer_name, probe.condition);
          state.qdiscs_after_apply = ListQdiscs();
          DeleteRootQdisc(probe.peer_name);
          state.qdiscs_after_delete = ListQdiscs();
          return state;
        });
    probe.namespace_qdiscs_before = std::move(namespace_state.qdiscs_before);
    probe.namespace_qdiscs_after_apply =
        std::move(namespace_state.qdiscs_after_apply);
    probe.namespace_qdiscs_after_delete =
        std::move(namespace_state.qdiscs_after_delete);

    const QdiscInfo* tbf = FindQdiscForInterface(
        probe.namespace_qdiscs_after_apply, probe.peer_name, QdiscKind::kTbf);
    if (tbf == nullptr) {
      throw std::runtime_error("TBF qdisc was not visible after apply");
    }
    if (!QdiscMatchesNetworkCondition(*tbf, probe.condition)) {
      throw std::runtime_error("TBF qdisc options did not match condition");
    }
    if (HasQdiscKindForInterface(probe.namespace_qdiscs_after_delete,
                                 probe.peer_name, QdiscKind::kTbf)) {
      throw std::runtime_error("TBF qdisc remained after delete");
    }
    if (!HasQdiscForInterface(probe.namespace_qdiscs_after_delete,
                              probe.peer_name)) {
      throw std::runtime_error("qdisc dump lost veth peer after TBF delete");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

NetworkConditionUpdateProbe ProbeNetworkConditionUpdate() {
  NetworkConditionUpdateProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.initial_condition.delay_ms = 20;
  probe.initial_condition.jitter_ms = 1;
  probe.initial_condition.loss_basis_points = 5;
  probe.initial_condition.duplicate_basis_points = 0;
  probe.initial_condition.limit_packets = 512;
  probe.updated_condition.delay_ms = 75;
  probe.updated_condition.jitter_ms = 10;
  probe.updated_condition.loss_basis_points = 25;
  probe.updated_condition.duplicate_basis_points = 10;
  probe.updated_condition.limit_packets = 2048;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());
    ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
      SetLinkUp("lo", true);
      SetLinkUp(probe.peer_name, true);
    });

    ReplaceNetworkConditionQdisc(probe.host_name, probe.initial_condition);
    probe.parent_qdiscs_after_initial = ListQdiscs();
    const QdiscInfo* initial = FindQdiscForInterface(
        probe.parent_qdiscs_after_initial, probe.host_name, QdiscKind::kNetem);
    if (initial == nullptr ||
        !QdiscMatchesNetworkCondition(*initial, probe.initial_condition)) {
      throw std::runtime_error("initial live netem condition was not visible");
    }

    ReplaceNetworkConditionQdisc(probe.host_name, probe.updated_condition);
    probe.parent_qdiscs_after_update = ListQdiscs();
    const QdiscInfo* updated = FindQdiscForInterface(
        probe.parent_qdiscs_after_update, probe.host_name, QdiscKind::kNetem);
    if (updated == nullptr ||
        !QdiscMatchesNetworkCondition(*updated, probe.updated_condition)) {
      throw std::runtime_error("updated live netem condition was not visible");
    }

    DeleteRootQdisc(probe.host_name);
    probe.parent_qdiscs_after_delete = ListQdiscs();
    if (HasQdiscKindForInterface(probe.parent_qdiscs_after_delete,
                                 probe.host_name, QdiscKind::kNetem)) {
      throw std::runtime_error("live netem condition remained after delete");
    }
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);
  probe.parent_after_delete = ListNetworkLinks();
  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

DropFilterProbe ProbeDropFilter() {
  DropFilterProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.dst_address = "198.51.100.7";
  probe.dst_port = 18168;
  probe.handle = 1001;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    SetLinkUp(probe.peer_name, true);

    probe.parent_filters_before = ListTcFiltersForInterface(probe.host_name);
    ReplaceEgressIpv4TcpDropFilter(probe.host_name, probe.dst_address,
                                   probe.dst_port, probe.handle);
    probe.parent_filters_after_apply =
        ListTcFiltersForInterface(probe.host_name);

    bool found = false;
    for (const TcFilterInfo& filter : probe.parent_filters_after_apply) {
      if (TcFilterMatchesEgressIpv4TcpDrop(filter, probe.host_name,
                                           probe.dst_address, probe.dst_port,
                                           probe.handle)) {
        if (!filter.has_stats) {
          throw std::runtime_error(
              "flower gact drop filter did not expose kernel statistics");
        }
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error(
          "flower gact drop filter was not visible after apply: " +
          DescribeTcFilters(probe.parent_filters_after_apply));
    }

    DeleteEgressIpv4TcpDropFilter(probe.host_name, probe.handle);
    probe.parent_filters_after_delete =
        ListTcFiltersForInterface(probe.host_name);
    for (const TcFilterInfo& filter : probe.parent_filters_after_delete) {
      if (TcFilterMatchesEgressIpv4TcpDrop(filter, probe.host_name,
                                           probe.dst_address, probe.dst_port,
                                           probe.handle)) {
        throw std::runtime_error(
            "flower gact drop filter remained after delete");
      }
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    throw;
  }

  return probe;
}

DirectionalNetworkPolicyProbe ProbeDirectionalNetworkPolicies() {
  DirectionalNetworkPolicyProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  DirectionalNetworkPolicy delay_policy;
  delay_policy.band = 1;
  delay_policy.destination_address = "198.51.100.7";
  delay_policy.condition.delay_ms = 40;
  delay_policy.condition.jitter_ms = 5;
  delay_policy.condition.loss_basis_points = 10;
  DirectionalNetworkPolicy combined_policy;
  combined_policy.band = 2;
  combined_policy.destination_address = "198.51.100.11";
  combined_policy.condition.bandwidth_mbps = 20;
  combined_policy.condition.delay_ms = 15;
  combined_policy.condition.limit_packets = 512;
  probe.policies = {delay_policy, combined_policy};

  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&probe.helper_pid);
  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);
  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());
    ExecuteInNetworkNamespace(namespace_fd.get(), [&]() {
      SetLinkUp("lo", true);
      SetLinkUp(probe.peer_name, true);
    });

    probe.namespace_qdiscs_before = ListQdiscsInNamespace(namespace_fd.get());
    UpdateDirectionalNetworkPoliciesInNamespace(
        namespace_fd.get(), probe.peer_name, {}, probe.policies);
    probe.namespace_qdiscs_after_apply =
        ListQdiscsInNamespace(namespace_fd.get());
    probe.namespace_filters_after_apply =
        ListTcFiltersForInterfaceParentInNamespace(
            namespace_fd.get(), probe.peer_name, kDirectionalRootHandle);
    if (!DirectionalNetworkPoliciesMatch(probe.namespace_qdiscs_after_apply,
                                         probe.namespace_filters_after_apply,
                                         probe.peer_name, probe.policies)) {
      throw std::runtime_error(
          "directional network policies did not match kernel state");
    }

    UpdateDirectionalNetworkPoliciesInNamespace(
        namespace_fd.get(), probe.peer_name, probe.policies, {});
    probe.namespace_qdiscs_after_delete =
        ListQdiscsInNamespace(namespace_fd.get());
    probe.namespace_filters_after_delete =
        ListTcFiltersForInterfaceParentInNamespace(
            namespace_fd.get(), probe.peer_name, kDirectionalRootHandle);
    if (!DirectionalNetworkPoliciesMatch(probe.namespace_qdiscs_after_delete,
                                         probe.namespace_filters_after_delete,
                                         probe.peer_name, {})) {
      throw std::runtime_error(
          "directional network policies remained after delete");
    }

    ExecuteInNetworkNamespace(namespace_fd.get(), [&]() {
      ReplaceRootPfifoQdisc(probe.peer_name, 64);
    });
    bool rejected_non_owned_root = false;
    try {
      UpdateDirectionalNetworkPoliciesInNamespace(
          namespace_fd.get(), probe.peer_name, {}, probe.policies);
    } catch (const std::exception&) {
      rejected_non_owned_root = true;
    }
    const std::vector<QdiscInfo> after_rejection =
        ListQdiscsInNamespace(namespace_fd.get());
    probe.non_owned_root_preserved =
        rejected_non_owned_root &&
        HasQdiscKindForInterface(after_rejection, probe.peer_name,
                                 QdiscKind::kPfifo);
    if (!probe.non_owned_root_preserved) {
      throw std::runtime_error(
          "directional network update did not preserve a non-owned root "
          "qdisc");
    }
    ExecuteInNetworkNamespace(namespace_fd.get(),
                              [&]() { DeleteRootQdisc(probe.peer_name); });

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

QdiscMutationProbe ProbeQdiscMutation() {
  QdiscMutationProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.pfifo_limit_packets = 64;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());

    NamespaceQdiscMutationState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          NamespaceQdiscMutationState state;
          state.qdiscs_before = ListQdiscs();
          ReplaceRootPfifoQdisc(probe.peer_name, probe.pfifo_limit_packets);
          state.qdiscs_after_replace = ListQdiscs();
          DeleteRootQdisc(probe.peer_name);
          state.qdiscs_after_delete = ListQdiscs();
          return state;
        });
    probe.namespace_qdiscs_before = std::move(namespace_state.qdiscs_before);
    probe.namespace_qdiscs_after_replace =
        std::move(namespace_state.qdiscs_after_replace);
    probe.namespace_qdiscs_after_delete =
        std::move(namespace_state.qdiscs_after_delete);

    if (!HasQdiscKindForInterface(probe.namespace_qdiscs_after_replace,
                                  probe.peer_name, QdiscKind::kPfifo)) {
      throw std::runtime_error("pfifo qdisc was not visible after replace");
    }
    if (HasQdiscKindForInterface(probe.namespace_qdiscs_after_delete,
                                 probe.peer_name, QdiscKind::kPfifo)) {
      throw std::runtime_error("pfifo qdisc remained after delete");
    }
    if (!HasQdiscForInterface(probe.namespace_qdiscs_after_delete,
                              probe.peer_name)) {
      throw std::runtime_error("qdisc dump lost veth peer after delete");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

QdiscProbe ProbeQdiscDump() {
  QdiscProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());

    NamespaceQdiscState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          NamespaceQdiscState state;
          state.links = ListNetworkLinks();
          state.qdiscs = ListQdiscs();
          return state;
        });
    probe.namespace_links = std::move(namespace_state.links);
    probe.namespace_qdiscs = std::move(namespace_state.qdiscs);

    if (!HasLinkNamed(probe.namespace_links, probe.peer_name)) {
      throw std::runtime_error("veth peer was not visible before qdisc probe");
    }
    if (!HasQdiscForInterface(probe.namespace_qdiscs, probe.peer_name)) {
      throw std::runtime_error("qdisc dump did not include veth peer");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

RouteProbe ProbeIpv4RouteAssignment() {
  RouteProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.assigned_address = "10.20.0.2";
  probe.assigned_prefix_len = 24;
  probe.route_destination = "10.30.0.0";
  probe.route_prefix_len = 24;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());

    NamespaceRouteState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          AddIpv4Address(probe.peer_name, probe.assigned_address,
                         probe.assigned_prefix_len);
          AddIpv4Route(probe.peer_name, probe.route_destination,
                       probe.route_prefix_len);
          NamespaceRouteState state;
          state.links = ListNetworkLinks();
          state.addresses = ListIpv4Addresses();
          state.routes = ListIpv4Routes();
          DeleteIpv4Route(probe.peer_name, probe.route_destination,
                          probe.route_prefix_len);
          DeleteIpv4Address(probe.peer_name, probe.assigned_address,
                            probe.assigned_prefix_len);
          state.addresses_after_delete = ListIpv4Addresses();
          state.routes_after_delete = ListIpv4Routes();
          return state;
        });
    probe.namespace_links_after_route = std::move(namespace_state.links);
    probe.namespace_addresses = std::move(namespace_state.addresses);
    probe.namespace_routes = std::move(namespace_state.routes);
    probe.namespace_addresses_after_delete =
        std::move(namespace_state.addresses_after_delete);
    probe.namespace_routes_after_delete =
        std::move(namespace_state.routes_after_delete);

    if (!HasLinkNamed(probe.namespace_links_after_route, probe.peer_name)) {
      throw std::runtime_error("veth peer was not visible before route probe");
    }
    if (!HasIpv4Address(probe.namespace_addresses, probe.peer_name,
                        probe.assigned_address, probe.assigned_prefix_len)) {
      throw std::runtime_error(
          "IPv4 address was not visible before route probe");
    }
    if (!HasIpv4Route(probe.namespace_routes, probe.peer_name,
                      probe.route_destination, probe.route_prefix_len)) {
      throw std::runtime_error("IPv4 route was not visible in child netns");
    }
    if (HasIpv4Route(probe.namespace_routes_after_delete, probe.peer_name,
                     probe.route_destination, probe.route_prefix_len)) {
      throw std::runtime_error("IPv4 route remained after delete");
    }
    if (HasIpv4Address(probe.namespace_addresses_after_delete, probe.peer_name,
                       probe.assigned_address, probe.assigned_prefix_len)) {
      throw std::runtime_error(
          "IPv4 address remained after route probe cleanup");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

AddressProbe ProbeIpv4AddressAssignment() {
  AddressProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.assigned_address = "10.20.0.2";
  probe.assigned_prefix_len = 24;

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());
    probe.parent_after_move = ListNetworkLinks();

    NamespaceAddressState namespace_state =
        ExecuteInNetworkNamespace(namespace_fd.get(), [&probe]() {
          SetLinkUp("lo", true);
          SetLinkUp(probe.peer_name, true);
          AddIpv4Address(probe.peer_name, probe.assigned_address,
                         probe.assigned_prefix_len);
          NamespaceAddressState state;
          state.links = ListNetworkLinks();
          state.addresses = ListIpv4Addresses();
          DeleteIpv4Address(probe.peer_name, probe.assigned_address,
                            probe.assigned_prefix_len);
          state.addresses_after_delete = ListIpv4Addresses();
          return state;
        });
    probe.namespace_links_after_address = std::move(namespace_state.links);
    probe.namespace_addresses = std::move(namespace_state.addresses);
    probe.namespace_addresses_after_delete =
        std::move(namespace_state.addresses_after_delete);

    if (!HasLinkNamed(probe.parent_after_move, probe.host_name) ||
        HasLinkNamed(probe.parent_after_move, probe.peer_name) ||
        !HasLinkNamed(probe.namespace_links_after_address, probe.peer_name)) {
      throw std::runtime_error(
          "veth peer was not isolated before address probe");
    }
    if (!HasIpv4Address(probe.namespace_addresses, probe.peer_name,
                        probe.assigned_address, probe.assigned_prefix_len)) {
      throw std::runtime_error("IPv4 address was not visible in child netns");
    }
    if (HasIpv4Address(probe.namespace_addresses_after_delete, probe.peer_name,
                       probe.assigned_address, probe.assigned_prefix_len)) {
      throw std::runtime_error("IPv4 address remained after delete");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

VethProbe ProbeVethPair() {
  VethProbe probe;
  probe.host_name = ProbeName('h');
  probe.peer_name = ProbeName('p');
  probe.parent_before = ListNetworkLinks();

  pid_t helper_pid = -1;
  UniqueFd namespace_fd = StartNetworkNamespaceHelper(&helper_pid);
  probe.helper_pid = helper_pid;

  TryDeleteLink(probe.host_name);
  TryDeleteLink(probe.peer_name);

  try {
    CreateVethPair(probe.host_name, probe.peer_name);
    SetLinkUp(probe.host_name, true);
    probe.parent_after_create = ListNetworkLinks();
    if (!HasLinkNamed(probe.parent_after_create, probe.host_name) ||
        !HasLinkNamed(probe.parent_after_create, probe.peer_name)) {
      throw std::runtime_error(
          "created veth pair was not visible in parent netns");
    }

    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());
    probe.parent_after_move = ListNetworkLinks();
    probe.namespace_after_move =
        ListNetworkLinksInNamespace(namespace_fd.get());
    if (!HasLinkNamed(probe.parent_after_move, probe.host_name) ||
        HasLinkNamed(probe.parent_after_move, probe.peer_name) ||
        !HasLinkNamed(probe.namespace_after_move, probe.peer_name)) {
      throw std::runtime_error("veth peer did not move to child netns");
    }

    DeleteLink(probe.host_name);
    probe.parent_after_delete = ListNetworkLinks();
  } catch (...) {
    TryDeleteLink(probe.host_name);
    TryDeleteLink(probe.peer_name);
    kill(probe.helper_pid, SIGKILL);
    WaitForPid(probe.helper_pid);
    throw;
  }

  kill(probe.helper_pid, SIGKILL);
  WaitForPid(probe.helper_pid);
  return probe;
}

}  // namespace bbp
