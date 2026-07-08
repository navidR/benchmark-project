#include "benchmark_sim/network.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <linux/if_link.h>
#include <linux/pkt_sched.h>
#include <linux/rtnetlink.h>
#include <linux/veth.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sched.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <exception>
#include <future>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <thread>
#include <utility>
#include <vector>

#include <libmnl/libmnl.h>

namespace bsim {
namespace {

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
                  const std::string& oif_name,
                  const std::string& destination,
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
    if (qdisc.if_name == if_name && !qdisc.kind.empty()) {
      return true;
    }
  }
  return false;
}

bool HasQdiscKindForInterface(const std::vector<QdiscInfo>& qdiscs,
                              const std::string& if_name,
                              const std::string& kind) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == if_name && qdisc.kind == kind) {
      return true;
    }
  }
  return false;
}

template <typename Operation>
auto ExecuteInNetworkNamespace(int netns_fd, Operation operation)
    -> decltype(operation()) {
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
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);
  const unsigned int port_id = socket.PortId();
  socket.Send(nlh, nlh->nlmsg_len);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  const ssize_t received = socket.Receive(buffer.data(), buffer.size());
  const int status =
      mnl_cb_run(buffer.data(), static_cast<size_t>(received), sequence, port_id,
                 nullptr, nullptr);
  if (status == MNL_CB_ERROR) {
    throw std::runtime_error(std::string("netlink request failed: ") +
                             std::strerror(errno));
  }
}

void TryDeleteLink(const std::string& name) {
  try {
    DeleteLink(name);
  } catch (const std::exception&) {
  }
}

std::string ProbeName(char suffix) {
  return "bs" + std::to_string(static_cast<long long>(getpid() % 100000)) +
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

UniqueFd StartNetworkNamespaceHelper(pid_t* helper_pid) {
  int pipe_fds[2];
  if (pipe2(pipe_fds, O_CLOEXEC) != 0) {
    throw std::runtime_error(std::string("pipe2 failed: ") +
                             std::strerror(errno));
  }
  UniqueFd read_end(pipe_fds[0]);
  UniqueFd write_end(pipe_fds[1]);

  const pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error(std::string("fork failed: ") + std::strerror(errno));
  }

  if (pid == 0) {
    read_end.Reset();
    int status = 0;
    if (unshare(CLONE_NEWNET) != 0) {
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
  const int status = ReadNamespaceStatus(read_end.get());
  read_end.Reset();
  if (status != 0) {
    WaitForPid(pid);
    throw std::runtime_error(std::string("unshare(CLONE_NEWNET) failed: ") +
                             std::strerror(status));
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
  return MNL_CB_OK;
}

int ParseLinkMessage(const nlmsghdr* nlh, void* data) {
  auto* links = static_cast<std::vector<LinkInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWLINK) {
    return MNL_CB_OK;
  }

  const auto* message = static_cast<const ifinfomsg*>(mnl_nlmsg_get_payload(nlh));
  LinkInfo link;
  link.index = message->ifi_index;
  link.up = (message->ifi_flags & IFF_UP) != 0U;
  if (mnl_attr_parse(nlh, sizeof(*message), ParseLinkAttr, &link) < 0) {
    return MNL_CB_ERROR;
  }
  links->push_back(std::move(link));
  return MNL_CB_OK;
}

struct AddressParseState {
  AddressInfo address;
  bool has_address = false;
};

int StoreIpv4Address(const nlattr* attr, AddressParseState* state, bool prefer) {
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

  const auto* message =
      static_cast<const ifaddrmsg*>(mnl_nlmsg_get_payload(nlh));
  if (message->ifa_family != AF_INET) {
    return MNL_CB_OK;
  }

  AddressParseState state;
  state.address.if_index = static_cast<int>(message->ifa_index);
  state.address.prefix_len = message->ifa_prefixlen;
  if (mnl_attr_parse(nlh, sizeof(*message), ParseAddressAttr, &state) < 0) {
    return MNL_CB_ERROR;
  }
  if (state.address.if_name.empty()) {
    char if_name[IF_NAMESIZE] = {};
    if (if_indextoname(message->ifa_index, if_name) != nullptr) {
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

  const auto* message = static_cast<const rtmsg*>(mnl_nlmsg_get_payload(nlh));
  if (message->rtm_family != AF_INET) {
    return MNL_CB_OK;
  }

  RouteInfo route;
  route.destination = "0.0.0.0";
  route.prefix_len = message->rtm_dst_len;
  route.table = message->rtm_table;
  route.protocol = message->rtm_protocol;
  route.scope = message->rtm_scope;
  route.type = message->rtm_type;
  if (mnl_attr_parse(nlh, sizeof(*message), ParseRouteAttr, &route) < 0) {
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

int ParseQdiscAttr(const nlattr* attr, void* data) {
  auto* qdisc = static_cast<QdiscInfo*>(data);
  const uint16_t attr_type = mnl_attr_get_type(attr);
  if (mnl_attr_type_valid(attr, TCA_MAX) < 0) {
    return MNL_CB_OK;
  }

  if (attr_type == TCA_KIND) {
    if (mnl_attr_validate(attr, MNL_TYPE_STRING) < 0) {
      return MNL_CB_ERROR;
    }
    qdisc->kind = mnl_attr_get_str(attr);
  }
  return MNL_CB_OK;
}

int ParseQdiscMessage(const nlmsghdr* nlh, void* data) {
  auto* qdiscs = static_cast<std::vector<QdiscInfo>*>(data);
  if (nlh->nlmsg_type != RTM_NEWQDISC) {
    return MNL_CB_OK;
  }

  const auto* message = static_cast<const tcmsg*>(mnl_nlmsg_get_payload(nlh));
  QdiscInfo qdisc;
  qdisc.if_index = message->tcm_ifindex;
  qdisc.handle = message->tcm_handle;
  qdisc.parent = message->tcm_parent;
  qdisc.info = message->tcm_info;
  if (qdisc.if_index > 0) {
    char if_name[IF_NAMESIZE] = {};
    if (if_indextoname(static_cast<unsigned int>(qdisc.if_index), if_name) !=
        nullptr) {
      qdisc.if_name = if_name;
    }
  }
  if (mnl_attr_parse(nlh, sizeof(*message), ParseQdiscAttr, &qdisc) < 0) {
    return MNL_CB_ERROR;
  }
  qdiscs->push_back(std::move(qdisc));
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

}  // namespace

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

void CreateVethPair(const std::string& host_name, const std::string& peer_name) {
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
  message->tcm_handle = TC_H_MAKE(1U << 16, 0U);
  message->tcm_parent = TC_H_ROOT;
  message->tcm_info = 0;

  tc_fifo_qopt options{};
  options.limit = limit_packets;
  mnl_attr_put_strz(nlh, TCA_KIND, "pfifo");
  mnl_attr_put(nlh, TCA_OPTIONS, sizeof(options), &options);

  SendNetlinkRequest(nlh, sequence);
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

std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd, []() {
    return ListNetworkLinks();
  });
}

std::vector<AddressInfo> ListIpv4AddressesInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd, []() {
    return ListIpv4Addresses();
  });
}

std::vector<RouteInfo> ListIpv4RoutesInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd, []() {
    return ListIpv4Routes();
  });
}

std::vector<QdiscInfo> ListQdiscsInNamespace(int netns_fd) {
  return ExecuteInNetworkNamespace(netns_fd, []() {
    return ListQdiscs();
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
                                  probe.peer_name, "pfifo")) {
      throw std::runtime_error("pfifo qdisc was not visible after replace");
    }
    if (HasQdiscKindForInterface(probe.namespace_qdiscs_after_delete,
                                 probe.peer_name, "pfifo")) {
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
      throw std::runtime_error("IPv4 address was not visible before route probe");
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
      throw std::runtime_error("IPv4 address remained after route probe cleanup");
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
      throw std::runtime_error("veth peer was not isolated before address probe");
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
      throw std::runtime_error("created veth pair was not visible in parent netns");
    }

    MoveLinkToNamespace(probe.peer_name, namespace_fd.get());
    probe.parent_after_move = ListNetworkLinks();
    probe.namespace_after_move = ListNetworkLinksInNamespace(namespace_fd.get());
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

}  // namespace bsim
