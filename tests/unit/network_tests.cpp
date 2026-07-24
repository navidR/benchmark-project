#include <libmnl/libmnl.h>
#include <linux/if_ether.h>
#include <linux/pkt_sched.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/network.h"

namespace {

constexpr std::uint32_t kDirectionalRootHandle = TC_H_MAKE(0xBB00U << 16U, 0U);

struct NamespaceIdentity {
  dev_t device = 0;
  ino_t inode = 0;

  bool operator==(const NamespaceIdentity&) const = default;
};

NamespaceIdentity CurrentThreadNetworkNamespace() {
  const long thread_id = syscall(SYS_gettid);
  if (thread_id <= 0) {
    throw std::runtime_error("gettid failed");
  }
  const std::string path =
      "/proc/self/task/" + std::to_string(thread_id) + "/ns/net";
  struct stat status {};
  if (stat(path.c_str(), &status) != 0) {
    throw std::runtime_error("stat failed for " + path + ": " +
                             std::strerror(errno));
  }
  return {status.st_dev, status.st_ino};
}

bbp::NodeVethConfig UniqueVethConfig() {
  static std::atomic<unsigned int> next{1U};
  const unsigned int ordinal = next.fetch_add(1U);
  const std::string token =
      std::to_string(static_cast<unsigned long long>(getpid()) % 10000ULL) +
      std::to_string(ordinal % 1000U);
  bbp::NodeVethConfig config;
  config.host_name = "bbpt" + token + "h";
  config.peer_name = "bbpt" + token + "p";
  config.host_ownership_alias = "bbp:test:" + token + ":h";
  config.peer_ownership_alias = "bbp:test:" + token + ":p";
  const unsigned int subnet = 32U + (ordinal % 192U);
  config.host_address = "198.18." + std::to_string(subnet) + ".1";
  config.node_address = "198.18." + std::to_string(subnet) + ".2";
  config.prefix_len = 30;
  return config;
}

bool ExplicitPrivilegeFailure(const std::exception& error) {
  const std::string message = error.what();
  return message.find(std::strerror(EPERM)) != std::string::npos ||
         message.find(std::strerror(EACCES)) != std::string::npos;
}

void RenameLinkForTest(const std::string& current,
                       const std::string& replacement) {
  const int descriptor = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (descriptor < 0) {
    throw std::runtime_error("open link rename socket failed");
  }
  ifreq request{};
  std::strncpy(request.ifr_name, current.c_str(), IFNAMSIZ - 1);
  std::strncpy(request.ifr_newname, replacement.c_str(), IFNAMSIZ - 1);
  if (ioctl(descriptor, SIOCSIFNAME, &request) != 0) {
    const int error = errno;
    close(descriptor);
    throw std::runtime_error("rename test link failed: " +
                             std::string(std::strerror(error)));
  }
  close(descriptor);
}

class ScopedNamespaceVeth {
 public:
  ScopedNamespaceVeth()
      : network_namespace_(bbp::NetworkNamespace::Create()),
        config_(UniqueVethConfig()) {
    std::optional<bbp::NodeVethIdentity> acquired;
    bbp::SetupNodeVethNetwork(network_namespace_.fd(), config_, &acquired);
    if (!acquired) {
      throw std::runtime_error("test veth setup returned no acquired identity");
    }
    identity_ = std::move(*acquired);
    setup_ = true;
  }

  ScopedNamespaceVeth(const ScopedNamespaceVeth&) = delete;
  ScopedNamespaceVeth& operator=(const ScopedNamespaceVeth&) = delete;

  ~ScopedNamespaceVeth() {
    if (setup_) {
      bbp::DeleteNodeVethNetwork(config_, identity_);
    }
  }

  int namespace_fd() const { return network_namespace_.fd(); }
  const bbp::NodeVethConfig& config() const { return config_; }

 private:
  bbp::NetworkNamespace network_namespace_;
  bbp::NodeVethConfig config_;
  bbp::NodeVethIdentity identity_;
  bool setup_ = false;
};

class ScopedParentVeth {
 public:
  explicit ScopedParentVeth(bbp::NodeVethConfig config)
      : config_(std::move(config)) {
    identity_ = bbp::CreateVethPair(
        config_.host_name, config_.peer_name, config_.host_ownership_alias,
        config_.peer_ownership_alias);
    created_ = true;
  }

  ScopedParentVeth(const ScopedParentVeth&) = delete;
  ScopedParentVeth& operator=(const ScopedParentVeth&) = delete;

  ~ScopedParentVeth() {
    if (created_) {
      try {
        bbp::DeleteNodeVethNetwork(config_, identity_);
      } catch (const std::exception&) {
      }
    }
  }

  const bbp::NodeVethIdentity& identity() const { return identity_; }

 private:
  bbp::NodeVethConfig config_;
  bbp::NodeVethIdentity identity_;
  bool created_ = false;
};

std::vector<std::uint8_t> NetlinkAcknowledgement(
    std::uint32_t sequence, std::uint32_t port_id, int error = 0,
    std::uint16_t message_type = NLMSG_ERROR) {
  std::vector<std::uint8_t> reply(NLMSG_LENGTH(sizeof(nlmsgerr)));
  nlmsghdr header{};
  header.nlmsg_len = static_cast<std::uint32_t>(reply.size());
  header.nlmsg_type = message_type;
  header.nlmsg_seq = sequence;
  header.nlmsg_pid = port_id;
  std::memcpy(reply.data(), &header, sizeof(header));
  nlmsgerr acknowledgement{};
  acknowledgement.error = error;
  std::memcpy(reply.data() + NLMSG_HDRLEN, &acknowledgement,
              sizeof(acknowledgement));
  return reply;
}

bbp::DirectionalNetworkPolicy DelayPolicy() {
  bbp::DirectionalNetworkPolicy policy;
  policy.band = 1;
  policy.destination_address = "198.51.100.7";
  policy.condition.delay_ms = 5;
  return policy;
}

bool HasQdiscHandle(const std::vector<bbp::QdiscInfo>& qdiscs,
                    const std::string& if_name, std::uint32_t handle) {
  return std::any_of(
      qdiscs.begin(), qdiscs.end(), [&](const bbp::QdiscInfo& qdisc) {
        return qdisc.if_name == if_name && qdisc.handle == handle;
      });
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    netlink_mutations_require_an_exact_success_acknowledgement) {
  constexpr std::uint32_t sequence = 41U;
  constexpr std::uint32_t port_id = 73U;
  const std::vector<std::uint8_t> success =
      NetlinkAcknowledgement(sequence, port_id);
  BOOST_CHECK_NO_THROW(
      bbp::ValidateNetlinkAcknowledgementForTest(success, sequence, port_id));

  const std::vector<std::uint8_t> done =
      NetlinkAcknowledgement(sequence, port_id, 0, NLMSG_DONE);
  BOOST_CHECK_THROW(
      bbp::ValidateNetlinkAcknowledgementForTest(done, sequence, port_id),
      std::runtime_error);

  std::vector<std::uint8_t> truncated(sizeof(nlmsghdr));
  nlmsghdr truncated_header{};
  truncated_header.nlmsg_len = static_cast<std::uint32_t>(truncated.size());
  truncated_header.nlmsg_type = NLMSG_ERROR;
  truncated_header.nlmsg_seq = sequence;
  truncated_header.nlmsg_pid = port_id;
  std::memcpy(truncated.data(), &truncated_header, sizeof(truncated_header));
  BOOST_CHECK_THROW(
      bbp::ValidateNetlinkAcknowledgementForTest(truncated, sequence, port_id),
      std::runtime_error);

  std::vector<std::uint8_t> trailing = success;
  trailing.resize(trailing.size() + NLMSG_ALIGN(sizeof(nlmsghdr)));
  BOOST_CHECK_THROW(
      bbp::ValidateNetlinkAcknowledgementForTest(trailing, sequence, port_id),
      std::runtime_error);

  BOOST_CHECK_THROW(bbp::ValidateNetlinkAcknowledgementForTest(
                        success, sequence + 1U, port_id),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ValidateNetlinkAcknowledgementForTest(
                        success, sequence, port_id + 1U),
                    std::runtime_error);

  const std::vector<std::uint8_t> kernel_error =
      NetlinkAcknowledgement(sequence, port_id, -EPERM);
  BOOST_CHECK_EXCEPTION(
      bbp::ValidateNetlinkAcknowledgementForTest(kernel_error, sequence,
                                                 port_id),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what()).find(std::strerror(EPERM)) !=
               std::string::npos;
      });
}

BOOST_AUTO_TEST_CASE(rtnetlink_dump_cancellation_has_bounded_latency) {
  std::stop_source stop_source;
  stop_source.request_stop();
  const auto started = std::chrono::steady_clock::now();
  BOOST_CHECK_EXCEPTION(
      bbp::ListNetworkLinks(stop_source.get_token()), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find("cancelled") != std::string::npos;
      });
  const auto elapsed = std::chrono::steady_clock::now() - started;
  BOOST_TEST(
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() <
      250);
}

BOOST_AUTO_TEST_CASE(rtnetlink_dump_timeout_has_bounded_latency) {
  const auto started = std::chrono::steady_clock::now();
  {
    bbp::ScopedRtnetlinkDumpFailure failure(
        bbp::RtnetlinkDumpFailure::kTimeout);
    BOOST_CHECK_EXCEPTION(
        bbp::ListNetworkLinks(), std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what()).find("deadline exceeded") !=
                 std::string::npos;
        });
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;
  BOOST_TEST(
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() <
      250);
}

BOOST_AUTO_TEST_CASE(rtnetlink_incomplete_dump_is_never_returned) {
  bbp::ScopedRtnetlinkDumpFailure failure(
      bbp::RtnetlinkDumpFailure::kIncomplete);
  BOOST_CHECK_EXCEPTION(
      bbp::ListNetworkLinks(), std::runtime_error,
      [](const std::runtime_error& error) {
        const std::string message = error.what();
        return message.find("incomplete") != std::string::npos &&
               message.find("NLMSG_DONE") != std::string::npos;
      });
}

BOOST_AUTO_TEST_CASE(
    veth_deletion_resolver_accepts_rename_but_rejects_replacement_and_reuse) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  const bbp::NodeVethIdentity acquired{
      .host =
          {
              .index = 101,
              .linked_index = 102,
              .name = config.host_name,
              .ownership_alias = config.host_ownership_alias,
              .kind = "veth",
          },
      .peer =
          {
              .index = 102,
              .linked_index = 101,
              .name = config.peer_name,
              .ownership_alias = config.peer_ownership_alias,
              .kind = "veth",
          },
  };
  const std::vector<bbp::LinkInfo> replacements{
      {
          .index = 201,
          .linked_index = 202,
          .name = config.host_name,
          .ownership_alias = config.host_ownership_alias,
          .kind = "veth",
      },
      {
          .index = 202,
          .linked_index = 201,
          .name = config.peer_name,
          .ownership_alias = config.peer_ownership_alias,
          .kind = "veth",
      },
  };

  std::vector<bbp::LinkInfo> renamed{
      {
          .index = acquired.host.index,
          .linked_index = acquired.host.linked_index,
          .name = config.host_name + "r",
          .ownership_alias = config.host_ownership_alias,
          .kind = "veth",
      },
      {
          .index = acquired.peer.index,
          .linked_index = acquired.peer.linked_index,
          .name = config.peer_name + "r",
          .ownership_alias = config.peer_ownership_alias,
          .kind = "veth",
      },
      {
          .index = 401,
          .linked_index = 0,
          .name = config.host_name,
          .ownership_alias = "foreign-owner",
          .kind = "dummy",
      },
  };
  const std::optional<int> renamed_index =
      bbp::ResolveNodeVethDeletionIndexForTest(config, acquired, renamed);
  BOOST_REQUIRE(renamed_index.has_value());
  BOOST_TEST(*renamed_index == acquired.host.index);

  BOOST_CHECK_EXCEPTION(
      bbp::ResolveNodeVethDeletionIndexForTest(config, acquired, replacements),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what()).find("replacement") !=
               std::string::npos;
      });

  std::vector<bbp::LinkInfo> reused_ifindex = replacements;
  reused_ifindex.front().index = acquired.host.index;
  BOOST_CHECK_THROW(
      bbp::ResolveNodeVethDeletionIndexForTest(config, acquired, reused_ifindex),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    veth_intent_only_stale_cleanup_refuses_a_live_reused_name) {
  const bbp::NodeVethConfig stale = UniqueVethConfig();
  const std::vector<bbp::LinkInfo> foreign_reuse{
      {
          .index = 301,
          .linked_index = 0,
          .name = stale.host_name,
          .ownership_alias = "foreign-owner",
          .kind = "dummy",
      },
  };

  BOOST_CHECK_EXCEPTION(
      bbp::RequireIntentOnlyNodeVethAbsentForTest(stale, foreign_reuse),
      std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find(
                   "exact acquired veth identity is required") !=
               std::string::npos;
      });
  BOOST_CHECK_NO_THROW(
      bbp::RequireIntentOnlyNodeVethAbsentForTest(stale, {}));
}

BOOST_AUTO_TEST_CASE(
    veth_cleanup_surfaces_deletion_failure_and_preserves_link) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  try {
    ScopedParentVeth pair(config);
    {
      bbp::ScopedNetlinkFailurePlan plan(
          {{1U, bbp::NetlinkFailurePhase::kBeforeSend}});
      BOOST_CHECK_EXCEPTION(
          bbp::DeleteNodeVethNetwork(config, pair.identity()),
          std::runtime_error,
          [](const std::runtime_error& error) {
            return std::string(error.what())
                       .find(
                           "injected netlink failure before send at request "
                           "1") != std::string::npos;
          });
    }
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(
        std::any_of(links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.host_name &&
                 link.ownership_alias == config.host_ownership_alias;
        }));
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged deletion-error test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(veth_cleanup_returns_only_after_kernel_absence_readback) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  try {
    const bbp::NodeVethIdentity identity = bbp::CreateVethPair(
        config.host_name, config.peer_name, config.host_ownership_alias,
        config.peer_ownership_alias);
    bbp::DeleteNodeVethNetwork(config, identity);
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(std::none_of(
        links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.host_name || link.name == config.peer_name;
        }));
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged deletion-readback test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(veth_cleanup_deletes_a_renamed_acquired_endpoint) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  const std::string renamed = config.host_name.substr(0U, 13U) + "r";
  try {
    ScopedParentVeth pair(config);
    RenameLinkForTest(config.host_name, renamed);
    bbp::DeleteNodeVethNetwork(config, pair.identity());
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(std::none_of(
        links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.index == pair.identity().host.index ||
                 link.ownership_alias == config.host_ownership_alias ||
                 link.ownership_alias == config.peer_ownership_alias;
        }));
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged renamed-link cleanup test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(
    veth_creation_post_send_failure_rolls_back_verified_owned_pair) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  try {
    std::string failure;
    {
      bbp::ScopedNetlinkFailurePlan plan(
          {{1U, bbp::NetlinkFailurePhase::kAfterSend}});
      try {
        static_cast<void>(bbp::CreateVethPair(
            config.host_name, config.peer_name, config.host_ownership_alias,
            config.peer_ownership_alias));
        BOOST_FAIL("injected post-send veth creation failure did not fail");
      } catch (const std::runtime_error& error) {
        if (ExplicitPrivilegeFailure(error)) {
          BOOST_TEST_MESSAGE(
              "skipping privileged creation-rollback test: " << error.what());
          return;
        }
        failure = error.what();
      }
    }
    BOOST_TEST(
        failure.find("injected netlink failure after send at request 1") !=
        std::string::npos);
    BOOST_TEST(failure.find("rollback failed") == std::string::npos);
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(std::none_of(
        links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.host_name || link.name == config.peer_name;
        }));
  } catch (const std::exception&) {
    throw;
  }
}

BOOST_AUTO_TEST_CASE(veth_setup_reports_original_and_rollback_failures) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  try {
    bbp::NetworkNamespace network_namespace = bbp::NetworkNamespace::Create();
    std::optional<bbp::NodeVethIdentity> acquired;
    std::string failure;
    {
      bbp::ScopedNetlinkFailurePlan plan(
          {{4U, bbp::NetlinkFailurePhase::kBeforeSend},
           {5U, bbp::NetlinkFailurePhase::kBeforeSend}});
      try {
        bbp::SetupNodeVethNetwork(network_namespace.fd(), config, &acquired);
        BOOST_FAIL("injected setup and rollback failures did not fail setup");
      } catch (const std::runtime_error& error) {
        failure = error.what();
      }
    }
    BOOST_TEST(
        failure.find("injected netlink failure before send at request 4") !=
        std::string::npos);
    BOOST_TEST(failure.find("veth rollback failed") != std::string::npos);
    BOOST_TEST(
        failure.find("injected netlink failure before send at request 5") !=
        std::string::npos);
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(
        std::any_of(links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.host_name;
        }));
    BOOST_REQUIRE(acquired.has_value());
    bbp::DeleteNodeVethNetwork(config, *acquired);
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged setup-rollback test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(veth_setup_failure_completes_verified_rollback) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  try {
    bbp::NetworkNamespace network_namespace = bbp::NetworkNamespace::Create();
    std::optional<bbp::NodeVethIdentity> acquired;
    {
      bbp::ScopedNetlinkFailurePlan plan(
          {{4U, bbp::NetlinkFailurePhase::kBeforeSend}});
      BOOST_CHECK_EXCEPTION(
          bbp::SetupNodeVethNetwork(network_namespace.fd(), config, &acquired),
          std::runtime_error, [](const std::runtime_error& error) {
            const std::string message = error.what();
            return message.find(
                       "injected netlink failure before send at request 4") !=
                       std::string::npos &&
                   message.find("rollback failed") == std::string::npos;
          });
    }
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(std::none_of(
        links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.host_name || link.name == config.peer_name;
        }));
    BOOST_TEST(!acquired.has_value());
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged setup-rollback test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(directional_netlink_failure_rolls_back_applied_mutations) {
  std::optional<ScopedNamespaceVeth> topology;
  try {
    topology.emplace();
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE("skipping privileged rollback test: " << error.what());
      return;
    }
    throw;
  }

  std::string failure;
  {
    bbp::ScopedNetlinkFailurePlan plan(
        {{2U, bbp::NetlinkFailurePhase::kAfterSend}});
    try {
      bbp::UpdateDirectionalNetworkPoliciesInNamespace(
          topology->namespace_fd(), topology->config().peer_name, {},
          {DelayPolicy()});
      BOOST_FAIL("injected post-send netlink failure did not fail the update");
    } catch (const std::runtime_error& error) {
      failure = error.what();
    }
  }

  BOOST_TEST(failure.find("injected netlink failure after send at request 2") !=
             std::string::npos);
  BOOST_TEST(failure.find("prior state restored") != std::string::npos);
  const std::vector<bbp::QdiscInfo> after =
      bbp::ListQdiscsInNamespace(topology->namespace_fd());
  BOOST_TEST(!HasQdiscHandle(after, topology->config().peer_name,
                             kDirectionalRootHandle));
}

BOOST_AUTO_TEST_CASE(directional_netlink_reports_a_failed_rollback) {
  std::optional<ScopedNamespaceVeth> topology;
  try {
    topology.emplace();
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE("skipping privileged rollback test: " << error.what());
      return;
    }
    throw;
  }

  std::string failure;
  {
    bbp::ScopedNetlinkFailurePlan plan(
        {{2U, bbp::NetlinkFailurePhase::kAfterSend},
         {3U, bbp::NetlinkFailurePhase::kBeforeSend}});
    try {
      bbp::UpdateDirectionalNetworkPoliciesInNamespace(
          topology->namespace_fd(), topology->config().peer_name, {},
          {DelayPolicy()});
      BOOST_FAIL("injected rollback failure did not fail the update");
    } catch (const std::runtime_error& error) {
      failure = error.what();
    }
  }

  BOOST_TEST(failure.find("injected netlink failure after send at request 2") !=
             std::string::npos);
  BOOST_TEST(failure.find("; rollback failed: injected netlink failure before "
                          "send at request 3") != std::string::npos);
  const std::vector<bbp::QdiscInfo> after =
      bbp::ListQdiscsInNamespace(topology->namespace_fd());
  BOOST_TEST(HasQdiscHandle(after, topology->config().peer_name,
                            kDirectionalRootHandle));
}

BOOST_AUTO_TEST_CASE(main_thread_never_enters_a_node_network_namespace) {
  const NamespaceIdentity before = CurrentThreadNetworkNamespace();
  std::optional<ScopedNamespaceVeth> topology;
  try {
    topology.emplace();
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged namespace test: " << error.what());
      return;
    }
    throw;
  }
  NamespaceIdentity current = CurrentThreadNetworkNamespace();
  BOOST_TEST(current.device == before.device);
  BOOST_TEST(current.inode == before.inode);

  {
    bbp::ScopedNetlinkFailurePlan plan(
        {{1U, bbp::NetlinkFailurePhase::kBeforeSend}});
    BOOST_CHECK_THROW(bbp::UpdateDirectionalNetworkPoliciesInNamespace(
                          topology->namespace_fd(),
                          topology->config().peer_name, {}, {DelayPolicy()}),
                      std::runtime_error);
  }
  current = CurrentThreadNetworkNamespace();
  BOOST_TEST(current.device == before.device);
  BOOST_TEST(current.inode == before.inode);
}

BOOST_AUTO_TEST_CASE(veth_setup_refuses_and_preserves_name_collisions) {
  const bbp::NodeVethConfig config = UniqueVethConfig();
  try {
    bbp::NetworkNamespace network_namespace = bbp::NetworkNamespace::Create();
    ScopedParentVeth foreign(config);
    std::optional<bbp::NodeVethIdentity> acquired;
    BOOST_CHECK_EXCEPTION(
        bbp::SetupNodeVethNetwork(network_namespace.fd(), config, &acquired),
        std::runtime_error, [](const std::runtime_error& error) {
          return std::string(error.what()).find("non-owned veth") !=
                 std::string::npos;
        });
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    BOOST_TEST(
        std::any_of(links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.host_name;
        }));
    BOOST_TEST(
        std::any_of(links.begin(), links.end(), [&](const bbp::LinkInfo& link) {
          return link.name == config.peer_name;
        }));
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged ownership test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(veth_cleanup_preserves_a_colliding_foreign_owner) {
  const bbp::NodeVethConfig foreign = UniqueVethConfig();
  bbp::NodeVethConfig colliding = foreign;
  colliding.host_ownership_alias += ":different";
  colliding.peer_ownership_alias += ":different";
  try {
    ScopedParentVeth pair(foreign);
    BOOST_CHECK_EXCEPTION(
        bbp::DeleteNodeVethNetwork(colliding, pair.identity()),
        std::runtime_error,
        [](const std::runtime_error& error) {
          return std::string(error.what()).find("does not match requested") !=
                 std::string::npos;
        });
    const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();
    const auto host = std::find_if(links.begin(), links.end(),
                                   [&](const bbp::LinkInfo& link) {
                                     return link.name == foreign.host_name;
                                   });
    const auto peer = std::find_if(links.begin(), links.end(),
                                   [&](const bbp::LinkInfo& link) {
                                     return link.name == foreign.peer_name;
                                   });
    BOOST_REQUIRE(host != links.end());
    BOOST_REQUIRE(peer != links.end());
    BOOST_TEST(host->ownership_alias == foreign.host_ownership_alias);
    BOOST_TEST(peer->ownership_alias == foreign.peer_ownership_alias);
  } catch (const std::exception& error) {
    if (ExplicitPrivilegeFailure(error)) {
      BOOST_TEST_MESSAGE(
          "skipping privileged ownership collision test: " << error.what());
      return;
    }
    throw;
  }
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_loopback_with_libmnl) {
  const std::vector<bbp::LinkInfo> links = bbp::ListNetworkLinks();

  BOOST_TEST(!links.empty());
  const auto loopback =
      std::find_if(links.begin(), links.end(),
                   [](const bbp::LinkInfo& link) { return link.name == "lo"; });
  BOOST_REQUIRE(loopback != links.end());
  BOOST_TEST(loopback->index > 0);
  BOOST_TEST(loopback->has_stats);
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_ipv4_loopback_with_libmnl) {
  const std::vector<bbp::AddressInfo> addresses = bbp::ListIpv4Addresses();

  BOOST_TEST(!addresses.empty());
  const auto loopback = std::find_if(
      addresses.begin(), addresses.end(), [](const bbp::AddressInfo& address) {
        return address.if_name == "lo" && address.address == "127.0.0.1";
      });
  BOOST_REQUIRE(loopback != addresses.end());
  BOOST_TEST(loopback->if_index > 0);
  BOOST_TEST(loopback->prefix_len == 8U);
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_ipv4_routes_with_libmnl) {
  const std::vector<bbp::RouteInfo> routes = bbp::ListIpv4Routes();

  BOOST_TEST(!routes.empty());
  const auto valid_route = std::find_if(
      routes.begin(), routes.end(), [](const bbp::RouteInfo& route) {
        return !route.destination.empty() && route.prefix_len <= 32U;
      });
  BOOST_REQUIRE(valid_route != routes.end());
}

BOOST_AUTO_TEST_CASE(rtnetlink_lists_qdiscs_with_libmnl) {
  const std::vector<bbp::QdiscInfo> qdiscs = bbp::ListQdiscs();

  BOOST_TEST(!qdiscs.empty());
  const auto parsed_qdisc = std::find_if(
      qdiscs.begin(), qdiscs.end(), [](const bbp::QdiscInfo& qdisc) {
        return qdisc.if_index > 0 && !qdisc.kernel_kind.empty();
      });
  BOOST_REQUIRE(parsed_qdisc != qdiscs.end());
}

BOOST_AUTO_TEST_CASE(qdisc_summary_matches_combined_tbf_netem_condition) {
  bbp::NetworkCondition condition;
  condition.bandwidth_mbps = 20;
  condition.delay_ms = 40;
  condition.jitter_ms = 5;
  condition.loss_basis_points = 10;
  condition.duplicate_basis_points = 5;
  condition.corrupt_basis_points = 5;
  condition.reorder_basis_points = 10;
  condition.limit_packets = 1000;

  bbp::QdiscInfo tbf;
  tbf.if_name = "veth0";
  tbf.kind = bbp::QdiscKind::kTbf;
  tbf.kernel_kind = "tbf";
  tbf.handle = TC_H_MAKE(1U << 16, 0U);
  tbf.parent = TC_H_ROOT;
  tbf.has_tbf_options = true;
  tbf.tbf_rate_bytes_per_sec = 2500000;
  tbf.tbf_limit_bytes = 250000;

  bbp::QdiscInfo netem;
  netem.if_name = "veth0";
  netem.kind = bbp::QdiscKind::kNetem;
  netem.kernel_kind = "netem";
  netem.handle = TC_H_MAKE(2U << 16, 0U);
  netem.parent = TC_H_MAKE(1U << 16, 1U);
  netem.has_netem_options = true;
  netem.netem_latency_us = 40000;
  netem.netem_jitter_us = 5000;
  netem.netem_loss = 4294967;
  netem.netem_duplicate = 2147483;
  netem.netem_corrupt = 2147483;
  netem.netem_reorder = 4294967;
  netem.netem_limit_packets = 1000;

  const std::vector<bbp::QdiscInfo> qdiscs = {tbf, netem};
  bbp::QdiscInfo summary;

  BOOST_TEST(
      bbp::QdiscsMatchNetworkCondition(qdiscs, "veth0", condition, &summary));
  BOOST_CHECK(summary.kind == bbp::QdiscKind::kTbfNetem);
  BOOST_TEST(summary.has_tbf_options);
  BOOST_TEST(summary.has_netem_options);
  BOOST_TEST(summary.tbf_rate_bytes_per_sec == 2500000U);
  BOOST_TEST(summary.netem_latency_us == 40000U);
  BOOST_TEST(bbp::QdiscMatchesNetworkCondition(summary, condition));
}

BOOST_AUTO_TEST_CASE(qdisc_summary_rejects_missing_child_netem) {
  bbp::NetworkCondition condition;
  condition.bandwidth_mbps = 20;
  condition.delay_ms = 40;

  bbp::QdiscInfo tbf;
  tbf.if_name = "veth0";
  tbf.kind = bbp::QdiscKind::kTbf;
  tbf.kernel_kind = "tbf";
  tbf.handle = TC_H_MAKE(1U << 16, 0U);
  tbf.parent = TC_H_ROOT;
  tbf.has_tbf_options = true;
  tbf.tbf_rate_bytes_per_sec = 2500000;
  tbf.tbf_limit_bytes = 250000;

  bbp::QdiscInfo summary;
  BOOST_TEST(
      !bbp::QdiscsMatchNetworkCondition({tbf}, "veth0", condition, &summary));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_matches_egress_ipv4_tcp_drop) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18168, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_matches_source_aware_tcp_drop) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_src = true;
  filter.ipv4_src = "198.51.100.11";
  filter.has_ipv4_src_mask = true;
  filter.ipv4_src_mask = "255.255.255.255";
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.11", 0U, "198.51.100.7", 18168, 1001));
  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.12", 0U, "198.51.100.7", 18168, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_matches_source_port_aware_tcp_drop) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_tcp_src = true;
  filter.tcp_src = 43120;
  filter.has_tcp_src_mask = true;
  filter.tcp_src_mask = 0xFFFFU;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "", 43120U, "198.51.100.7", 18168, 1001));
  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "", 43121U, "198.51.100.7", 18168, 1001));
  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "", 0U, "198.51.100.7", 18168, 1001));
  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18168, 1001));

  filter.tcp_src_mask = 0xFFF0U;
  BOOST_TEST(!bbp::TcFilterIsEgressIpv4TcpDropPolicy(filter, "veth0"));
  filter.tcp_src_mask = 0xFFFFU;
  filter.has_tcp_src_mask = false;
  BOOST_TEST(!bbp::TcFilterIsEgressIpv4TcpDropPolicy(filter, "veth0"));
  filter.has_tcp_src = false;
  filter.has_tcp_src_mask = true;
  BOOST_TEST(!bbp::TcFilterIsEgressIpv4TcpDropPolicy(filter, "veth0"));
  filter.has_tcp_src = true;
  filter.tcp_src = 0U;
  BOOST_TEST(!bbp::TcFilterIsEgressIpv4TcpDropPolicy(filter, "veth0"));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_rejects_source_filter_for_dst_only) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_src = true;
  filter.ipv4_src = "198.51.100.11";
  filter.has_ipv4_src_mask = true;
  filter.ipv4_src_mask = "255.255.255.255";
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18168, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_summary_rejects_wrong_port) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = "198.51.100.7";
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_tcp_dst = true;
  filter.tcp_dst = 18168;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(!bbp::TcFilterMatchesEgressIpv4TcpDrop(
      filter, "veth0", "198.51.100.7", 18169, 1001));
}

BOOST_AUTO_TEST_CASE(tc_filter_policy_summary_aggregates_owned_drop_counters) {
  bbp::TcFilterInfo first;
  first.if_name = "veth0";
  first.kind = bbp::TcFilterKind::kFlower;
  first.handle = 1001;
  first.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  first.protocol = ETH_P_IP;
  first.egress = true;
  first.has_eth_type = true;
  first.eth_type = ETH_P_IP;
  first.has_ip_proto = true;
  first.ip_proto = IPPROTO_TCP;
  first.has_ipv4_dst = true;
  first.ipv4_dst = "198.51.100.7";
  first.has_ipv4_dst_mask = true;
  first.ipv4_dst_mask = "255.255.255.255";
  first.has_tcp_dst = true;
  first.tcp_dst = 18168;
  first.has_tcp_dst_mask = true;
  first.tcp_dst_mask = 0xFFFFU;
  first.has_drop_action = true;
  first.has_stats = true;
  first.match_bytes = 100;
  first.match_packets = 2;
  first.drop_packets = 2;

  bbp::TcFilterInfo second = first;
  second.handle = 1002;
  second.match_bytes = 250;
  second.match_packets = 3;
  second.drop_packets = 3;
  bbp::TcFilterInfo unrelated = first;
  unrelated.if_name = "veth1";

  const bbp::TcFilterStatsSummary summary =
      bbp::SummarizeEgressIpv4TcpDropPolicies({first, second, unrelated},
                                              "veth0");
  BOOST_TEST(summary.policy_count == 2U);
  BOOST_TEST(summary.policies_with_stats == 2U);
  BOOST_TEST(summary.match_bytes == 350U);
  BOOST_TEST(summary.match_packets == 5U);
  BOOST_TEST(summary.drop_packets == 5U);
}

BOOST_AUTO_TEST_CASE(tc_filter_policy_summary_rejects_counter_overflow) {
  bbp::TcFilterInfo first;
  first.if_name = "veth0";
  first.kind = bbp::TcFilterKind::kFlower;
  first.handle = 1001;
  first.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  first.protocol = ETH_P_IP;
  first.egress = true;
  first.has_eth_type = true;
  first.eth_type = ETH_P_IP;
  first.has_ip_proto = true;
  first.ip_proto = IPPROTO_TCP;
  first.has_ipv4_dst = true;
  first.has_ipv4_dst_mask = true;
  first.ipv4_dst_mask = "255.255.255.255";
  first.has_tcp_dst = true;
  first.has_tcp_dst_mask = true;
  first.tcp_dst_mask = 0xFFFFU;
  first.has_drop_action = true;
  first.has_stats = true;
  first.match_bytes = std::numeric_limits<std::uint64_t>::max();
  bbp::TcFilterInfo second = first;
  second.handle = 1002;
  second.match_bytes = 1;

  BOOST_CHECK_THROW(
      bbp::SummarizeEgressIpv4TcpDropPolicies({first, second}, "veth0"),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(tc_filter_policy_requires_exact_masks) {
  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.handle = 1001;
  filter.parent = TC_H_MAKE(TC_H_CLSACT, TC_H_MIN_EGRESS);
  filter.protocol = ETH_P_IP;
  filter.egress = true;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ip_proto = true;
  filter.ip_proto = IPPROTO_TCP;
  filter.has_ipv4_dst = true;
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.0";
  filter.has_tcp_dst = true;
  filter.has_tcp_dst_mask = true;
  filter.tcp_dst_mask = 0xFFFFU;
  filter.has_drop_action = true;

  BOOST_TEST(!bbp::TcFilterIsEgressIpv4TcpDropPolicy(filter, "veth0"));
}

BOOST_AUTO_TEST_CASE(directional_network_policy_matches_exact_kernel_model) {
  constexpr std::uint32_t root_handle = TC_H_MAKE(0xBB00U << 16U, 0U);
  bbp::DirectionalNetworkPolicy policy;
  policy.band = 1;
  policy.destination_address = "198.51.100.7";
  policy.condition.delay_ms = 40;
  policy.condition.jitter_ms = 5;

  bbp::QdiscInfo root;
  root.if_name = "veth0";
  root.kind = bbp::QdiscKind::kPrio;
  root.kernel_kind = "prio";
  root.handle = root_handle;
  root.parent = TC_H_ROOT;
  root.has_prio_options = true;
  root.prio_bands = TCQ_PRIO_BANDS;

  bbp::QdiscInfo netem;
  netem.if_name = "veth0";
  netem.kind = bbp::QdiscKind::kNetem;
  netem.kernel_kind = "netem";
  netem.handle = TC_H_MAKE(0xBB21U << 16U, 0U);
  netem.parent = TC_H_MAKE(0xBB00U << 16U, 2U);
  netem.has_netem_options = true;
  netem.netem_latency_us = 40000;
  netem.netem_jitter_us = 5000;
  netem.netem_limit_packets = 1000;

  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 0xBB000001U;
  filter.parent = root_handle;
  filter.priority = 11;
  filter.protocol = ETH_P_IP;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = policy.destination_address;
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_class_id = true;
  filter.class_id = netem.parent;

  BOOST_TEST(bbp::DirectionalNetworkPoliciesMatch({root, netem}, {filter},
                                                  "veth0", {policy}));
  filter.class_id = TC_H_MAKE(0xBB00U << 16U, 3U);
  BOOST_TEST(!bbp::DirectionalNetworkPoliciesMatch({root, netem}, {filter},
                                                   "veth0", {policy}));
  filter.class_id = netem.parent;
  bbp::TcFilterInfo foreign = filter;
  foreign.handle = 0x12345678U;
  BOOST_TEST(!bbp::DirectionalNetworkPoliciesMatch(
      {root, netem}, {filter, foreign}, "veth0", {policy}));
}

BOOST_AUTO_TEST_CASE(
    directional_network_policy_stats_keep_stages_without_double_counting) {
  constexpr std::uint32_t root_handle = TC_H_MAKE(0xBB00U << 16U, 0U);
  constexpr std::uint32_t class_id = TC_H_MAKE(0xBB00U << 16U, 2U);
  constexpr std::uint32_t tbf_handle = TC_H_MAKE(0xBB11U << 16U, 0U);
  bbp::DirectionalNetworkPolicy policy;
  policy.band = 1;
  policy.destination_address = "198.51.100.7";
  policy.condition.bandwidth_mbps = 10;
  policy.condition.delay_ms = 2;

  bbp::QdiscInfo root;
  root.if_name = "veth0";
  root.kind = bbp::QdiscKind::kPrio;
  root.kernel_kind = "prio";
  root.handle = root_handle;
  root.parent = TC_H_ROOT;
  root.has_prio_options = true;
  root.prio_bands = TCQ_PRIO_BANDS;

  bbp::QdiscInfo tbf;
  tbf.if_name = "veth0";
  tbf.kind = bbp::QdiscKind::kTbf;
  tbf.kernel_kind = "tbf";
  tbf.handle = tbf_handle;
  tbf.parent = class_id;
  tbf.has_tbf_options = true;
  tbf.tbf_rate_bytes_per_sec = 1250000U;
  tbf.tbf_limit_bytes = 125000U;
  tbf.has_stats = true;
  tbf.bytes = 100U;
  tbf.packets = 10U;
  tbf.drops = 1U;
  tbf.overlimits = 3U;
  tbf.qlen = 2U;
  tbf.backlog = 20U;
  tbf.requeues = 4U;

  bbp::QdiscInfo netem;
  netem.if_name = "veth0";
  netem.kind = bbp::QdiscKind::kNetem;
  netem.kernel_kind = "netem";
  netem.handle = TC_H_MAKE(0xBB21U << 16U, 0U);
  netem.parent = TC_H_MAKE(TC_H_MAJ(tbf_handle), 1U);
  netem.has_netem_options = true;
  netem.netem_latency_us = 2000U;
  netem.netem_limit_packets = 1000U;
  netem.has_stats = true;
  netem.bytes = 90U;
  netem.packets = 9U;
  netem.drops = 2U;
  netem.overlimits = 5U;
  netem.qlen = 1U;
  netem.backlog = 10U;
  netem.requeues = 6U;

  bbp::TcFilterInfo filter;
  filter.if_name = "veth0";
  filter.kind = bbp::TcFilterKind::kFlower;
  filter.kernel_kind = "flower";
  filter.handle = 0xBB000001U;
  filter.parent = root_handle;
  filter.priority = 11U;
  filter.protocol = ETH_P_IP;
  filter.has_eth_type = true;
  filter.eth_type = ETH_P_IP;
  filter.has_ipv4_dst = true;
  filter.ipv4_dst = policy.destination_address;
  filter.has_ipv4_dst_mask = true;
  filter.ipv4_dst_mask = "255.255.255.255";
  filter.has_class_id = true;
  filter.class_id = class_id;
  filter.has_stats = true;
  filter.match_bytes = 100U;
  filter.match_packets = 10U;

  const bbp::DirectionalNetworkPolicyStats stats =
      bbp::SummarizeDirectionalNetworkPolicyStats({root, tbf, netem}, {filter},
                                                  "veth0", {policy});
  BOOST_TEST(stats.policy_count == 1U);
  BOOST_TEST(stats.policies_with_filter_stats == 1U);
  BOOST_TEST(stats.filter_match_bytes == 100U);
  BOOST_TEST(stats.filter_match_packets == 10U);
  BOOST_TEST(stats.qdisc_count == 2U);
  BOOST_TEST(stats.qdiscs_with_stats == 2U);
  BOOST_TEST(stats.qdisc_bytes == 100U);
  BOOST_TEST(stats.qdisc_packets == 10U);
  BOOST_TEST(stats.qdisc_drops == 3U);
  BOOST_TEST(stats.qdisc_overlimits == 8U);
  BOOST_TEST(stats.qdisc_qlen == 3U);
  BOOST_TEST(stats.qdisc_backlog == 30U);
  BOOST_TEST(stats.qdisc_requeues == 10U);
  BOOST_REQUIRE_EQUAL(stats.policies.size(), 1U);
  const bbp::DirectionalNetworkPolicyCounter& counter = stats.policies.front();
  BOOST_TEST(counter.band == 1U);
  BOOST_TEST(counter.destination_address == "198.51.100.7");
  BOOST_TEST(counter.qdisc_bytes == 100U);
  BOOST_TEST(counter.qdisc_packets == 10U);
  BOOST_TEST(counter.qdisc_drops == 3U);
  BOOST_REQUIRE_EQUAL(counter.qdiscs.size(), 2U);
  BOOST_TEST(counter.qdiscs[0].kernel_kind == "tbf");
  BOOST_TEST(counter.qdiscs[1].kernel_kind == "netem");

  BOOST_CHECK_THROW(bbp::SummarizeDirectionalNetworkPolicyStats(
                        {root, tbf}, {filter}, "veth0", {policy}),
                    std::runtime_error);

  bbp::QdiscInfo stale = netem;
  stale.handle = TC_H_MAKE(0xBB22U << 16U, 0U);
  stale.parent = TC_H_MAKE(0xBB00U << 16U, 3U);
  BOOST_CHECK_THROW(bbp::SummarizeDirectionalNetworkPolicyStats(
                        {root, tbf, netem, stale}, {filter}, "veth0", {policy}),
                    std::runtime_error);

  bbp::DirectionalNetworkPolicy second_policy;
  second_policy.band = 2;
  second_policy.destination_address = "198.51.100.8";
  second_policy.condition.delay_ms = 3;
  bbp::QdiscInfo second_netem = netem;
  second_netem.handle = TC_H_MAKE(0xBB22U << 16U, 0U);
  second_netem.parent = TC_H_MAKE(0xBB00U << 16U, 3U);
  second_netem.netem_latency_us = 3000U;
  second_netem.bytes = 1U;
  bbp::TcFilterInfo second_filter = filter;
  second_filter.handle = 0xBB000002U;
  second_filter.ipv4_dst = second_policy.destination_address;
  second_filter.class_id = TC_H_MAKE(0xBB00U << 16U, 3U);
  tbf.bytes = std::numeric_limits<std::uint64_t>::max();
  BOOST_CHECK_THROW(
      bbp::SummarizeDirectionalNetworkPolicyStats(
          {root, tbf, netem, second_netem}, {filter, second_filter}, "veth0",
          {policy, second_policy}),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(directional_network_policy_rejects_duplicate_bands) {
  bbp::DirectionalNetworkPolicy first;
  first.band = 1;
  first.destination_address = "198.51.100.7";
  first.condition.delay_ms = 1;
  bbp::DirectionalNetworkPolicy second = first;
  second.destination_address = "198.51.100.8";

  BOOST_CHECK_THROW(
      bbp::DirectionalNetworkPoliciesMatch({}, {}, "veth0", {first, second}),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(directional_network_policy_rejects_invalid_destination) {
  bbp::DirectionalNetworkPolicy policy;
  policy.band = 1;
  policy.destination_address = "not-an-address";
  policy.condition.delay_ms = 1;

  BOOST_CHECK_THROW(
      bbp::DirectionalNetworkPoliciesMatch({}, {}, "veth0", {policy}),
      std::runtime_error);
}
