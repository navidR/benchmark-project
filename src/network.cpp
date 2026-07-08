#include "benchmark_sim/network.h"

#include <fcntl.h>
#include <linux/if_link.h>
#include <linux/rtnetlink.h>
#include <net/if.h>
#include <sched.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <exception>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
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

}  // namespace

std::vector<LinkInfo> ListNetworkLinks() {
  MnlSocket socket(NETLINK_ROUTE);
  socket.Bind(0, MNL_SOCKET_AUTOPID);

  std::array<char, MNL_SOCKET_DUMP_SIZE> buffer{};
  nlmsghdr* nlh = mnl_nlmsg_put_header(buffer.data());
  nlh->nlmsg_type = RTM_GETLINK;
  nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
  nlh->nlmsg_seq = 1U;

  auto* message = static_cast<ifinfomsg*>(
      mnl_nlmsg_put_extra_header(nlh, sizeof(ifinfomsg)));
  message->ifi_family = AF_PACKET;

  socket.Send(nlh, nlh->nlmsg_len);

  std::vector<LinkInfo> links;
  while (true) {
    const ssize_t received = socket.Receive(buffer.data(), buffer.size());
    const int status =
        mnl_cb_run(buffer.data(), static_cast<size_t>(received), nlh->nlmsg_seq,
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

std::vector<LinkInfo> ListNetworkLinksInNamespace(int netns_fd) {
  std::promise<std::vector<LinkInfo>> promise;
  std::future<std::vector<LinkInfo>> result = promise.get_future();

  std::thread worker([netns_fd, &promise]() {
    try {
      std::vector<LinkInfo> links;
      {
        ScopedNetworkNamespace ns(netns_fd);
        links = ListNetworkLinks();
      }
      promise.set_value(std::move(links));
    } catch (...) {
      promise.set_exception(std::current_exception());
    }
  });

  worker.join();
  return result.get();
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

}  // namespace bsim
