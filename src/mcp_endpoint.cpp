#include "bbp/mcp_endpoint.h"

#include <fcntl.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <cstring>
#include <exception>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/logging.h"
#include "bbp/mcp_registry.h"

namespace bbp {
namespace {

std::runtime_error SystemError(std::string_view action,
                               const std::filesystem::path& path,
                               int error = errno) {
  return std::runtime_error(std::string(action) + " " + path.string() +
                            " failed: " + std::strerror(error));
}

std::string RandomToken() {
  std::array<unsigned char, 32> bytes{};
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t received =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("getrandom for MCP bearer token failed: " +
                               std::string(std::strerror(errno)));
    }
    if (received == 0) {
      throw std::runtime_error(
          "getrandom for MCP bearer token made no progress");
    }
    offset += static_cast<std::size_t>(received);
  }
  constexpr std::array<char, 16> kHex = {'0', '1', '2', '3', '4', '5',
                                         '6', '7', '8', '9', 'a', 'b',
                                         'c', 'd', 'e', 'f'};
  std::string token;
  token.reserve(bytes.size() * 2U);
  for (const unsigned char byte : bytes) {
    token.push_back(kHex[byte >> 4U]);
    token.push_back(kHex[byte & 0x0fU]);
  }
  return token;
}

struct FileIdentity {
  dev_t device = 0;
  ino_t inode = 0;
};

FileIdentity Identity(const struct stat& status) {
  return FileIdentity{.device = status.st_dev, .inode = status.st_ino};
}

bool SameIdentity(const struct stat& status, const FileIdentity& identity) {
  return status.st_dev == identity.device && status.st_ino == identity.inode;
}

FileIdentity RequireOwnedDirectory(const std::filesystem::path& path,
                                   mode_t forbidden_permissions) {
  struct stat status{};
  if (lstat(path.c_str(), &status) != 0) {
    throw SystemError("inspect MCP directory", path);
  }
  if (!S_ISDIR(status.st_mode)) {
    throw std::runtime_error("MCP path is not a directory: " + path.string());
  }
  if (status.st_uid != geteuid()) {
    throw std::runtime_error(
        "MCP directory is not owned by the effective user: " + path.string());
  }
  if ((status.st_mode & forbidden_permissions) != 0) {
    throw std::runtime_error("MCP directory permissions are unsafe: " +
                             path.string());
  }
  return Identity(status);
}

FileIdentity WriteCredentialFile(int directory_descriptor,
                                 std::string_view name,
                                 std::string_view contents) {
  const std::string temporary_name = "." + std::string(name) + ".tmp";
  if (unlinkat(directory_descriptor, temporary_name.c_str(), 0) != 0 &&
      errno != ENOENT) {
    throw std::runtime_error("remove stale temporary MCP credential " +
                             temporary_name +
                             " failed: " + std::strerror(errno));
  }
  const int descriptor = openat(
      directory_descriptor, temporary_name.c_str(),
      O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    throw std::runtime_error("create temporary MCP credential " +
                             temporary_name +
                             " failed: " + std::strerror(errno));
  }
  int failure = 0;
  std::size_t offset = 0U;
  while (offset < contents.size()) {
    const ssize_t written =
        write(descriptor, contents.data() + offset, contents.size() - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      failure = errno;
      break;
    }
    if (written == 0) {
      failure = EIO;
      break;
    }
    offset += static_cast<std::size_t>(written);
  }
  if (failure == 0 && fsync(descriptor) != 0) {
    failure = errno;
  }
  struct stat status{};
  if (failure == 0 && fstat(descriptor, &status) != 0) {
    failure = errno;
  }
  if (failure == 0 &&
      (!S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
       (status.st_mode & (S_IRWXG | S_IRWXO)) != 0 ||
       (status.st_mode & (S_IRUSR | S_IWUSR)) != (S_IRUSR | S_IWUSR))) {
    failure = EPERM;
  }
  if (close(descriptor) != 0 && failure == 0) {
    failure = errno;
  }
  bool published = false;
  if (failure == 0 &&
      renameat(directory_descriptor, temporary_name.c_str(),
               directory_descriptor, std::string(name).c_str()) != 0) {
    failure = errno;
  } else if (failure == 0) {
    published = true;
  }
  if (!published) {
    static_cast<void>(
        unlinkat(directory_descriptor, temporary_name.c_str(), 0));
  }
  if (failure == 0 && fsync(directory_descriptor) != 0) {
    failure = errno;
  }
  struct stat published_status{};
  if (failure == 0 && fstatat(directory_descriptor, std::string(name).c_str(),
                              &published_status, AT_SYMLINK_NOFOLLOW) != 0) {
    failure = errno;
  }
  if (failure == 0 && (!SameIdentity(published_status, Identity(status)) ||
                       !S_ISREG(published_status.st_mode) ||
                       published_status.st_uid != geteuid() ||
                       (published_status.st_mode & (S_IRWXG | S_IRWXO)) != 0)) {
    failure = EPERM;
  }
  if (failure != 0) {
    if (published) {
      static_cast<void>(
          unlinkat(directory_descriptor, std::string(name).c_str(), 0));
    }
    throw std::runtime_error("publish MCP credential " + std::string(name) +
                             " failed: " + std::strerror(failure));
  }
  return Identity(status);
}

void RemoveAndVerifyFile(int directory_descriptor, std::string_view name,
                         const std::optional<FileIdentity>& identity) {
  struct stat status{};
  if (fstatat(directory_descriptor, std::string(name).c_str(), &status,
              AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return;
    }
    throw std::runtime_error("inspect MCP credential " + std::string(name) +
                             " failed: " + std::strerror(errno));
  }
  if (!identity || !SameIdentity(status, *identity) ||
      !S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
      (status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
    throw std::runtime_error(
        "refusing to remove replaced or unsafe MCP "
        "credential: " +
        std::string(name));
  }
  if (unlinkat(directory_descriptor, std::string(name).c_str(), 0) != 0) {
    throw std::runtime_error("remove MCP credential " + std::string(name) +
                             " failed: " + std::strerror(errno));
  }
  if (fstatat(directory_descriptor, std::string(name).c_str(), &status,
              AT_SYMLINK_NOFOLLOW) == 0 ||
      errno != ENOENT) {
    throw std::runtime_error("MCP credential survived cleanup: " +
                             std::string(name));
  }
}

void RemoveStalePublicationFile(int directory_descriptor,
                                std::string_view name) {
  struct stat status{};
  if (fstatat(directory_descriptor, std::string(name).c_str(), &status,
              AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return;
    }
    throw std::runtime_error("inspect stale MCP publication " +
                             std::string(name) +
                             " failed: " + std::strerror(errno));
  }
  if (status.st_uid != geteuid() ||
      (!S_ISREG(status.st_mode) && !S_ISLNK(status.st_mode))) {
    throw std::runtime_error(
        "refusing to remove foreign or unsafe stale MCP publication: " +
        std::string(name));
  }
  if (unlinkat(directory_descriptor, std::string(name).c_str(), 0) != 0) {
    throw std::runtime_error("remove stale MCP publication " +
                             std::string(name) +
                             " failed: " + std::strerror(errno));
  }
}

void RemoveStalePublications(int directory_descriptor,
                             const std::filesystem::path& directory) {
  RemoveStalePublicationFile(directory_descriptor, kMcpClientConfigFile);
  RemoveStalePublicationFile(directory_descriptor, kMcpTokenFile);
  RemoveStalePublicationFile(directory_descriptor,
                             "." + std::string(kMcpClientConfigFile) + ".tmp");
  RemoveStalePublicationFile(directory_descriptor,
                             "." + std::string(kMcpTokenFile) + ".tmp");
  if (fsync(directory_descriptor) != 0) {
    throw SystemError("sync stale MCP publication cleanup", directory);
  }
}

std::string EscapeTomlBasicString(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const char character : value) {
    if (character == '\\' || character == '"') {
      result.push_back('\\');
    }
    result.push_back(character);
  }
  return result;
}

boost::json::object BuildClientConfig(std::string_view run_id,
                                      std::string_view endpoint,
                                      std::string_view token,
                                      const std::filesystem::path& token_file) {
  const std::string authorization = "Bearer " + std::string(token);
  const std::string codex_toml =
      "[mcp_servers.bbp]\nurl = \"" + EscapeTomlBasicString(endpoint) +
      "\"\nhttp_headers = { Authorization = \"" +
      EscapeTomlBasicString(authorization) + "\" }\n";
  boost::json::object opencode_server{
      {"type", "remote"},
      {"url", endpoint},
      {"headers", boost::json::object{{"Authorization", authorization}}}};
  boost::json::array supported_protocol_versions;
  supported_protocol_versions.reserve(kMcpSupportedProtocolVersions.size());
  for (const std::string_view version : kMcpSupportedProtocolVersions) {
    supported_protocol_versions.emplace_back(version);
  }
  return boost::json::object{
      {"run_id", run_id},
      {"endpoint", endpoint},
      {"protocol_version", kMcpProtocolVersion},
      {"supported_protocol_versions", std::move(supported_protocol_versions)},
      {"transport", "streamable_http"},
      {"authentication",
       boost::json::object{{"type", "bearer"},
                           {"token", token},
                           {"token_file", token_file.string()}}},
      {"codex_config_toml", codex_toml},
      {"opencode_config",
       boost::json::object{
           {"$schema", "https://opencode.ai/config.json"},
           {"mcp", boost::json::object{{"bbp", std::move(opencode_server)}}}}}};
}

}  // namespace

struct McpEndpoint::Impl {
  Impl(McpEndpointConfig config_value,
       McpApplicationOperationFactory operation_factory,
       McpApplicationResourceReader resource_reader)
      : config(std::move(config_value)),
        dispatcher(config.dispatcher, std::move(operation_factory),
                   std::move(resource_reader)) {
    if (config.state_directory.empty()) {
      throw std::runtime_error("MCP endpoint requires a state directory");
    }
    if (config.run_id.empty()) {
      throw std::runtime_error("MCP endpoint requires a run id");
    }
  }

  ~Impl() { StopNoThrow(); }

  void Start() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (started || stopped || services_stopped) {
      throw std::runtime_error("MCP endpoint cannot be started in this state");
    }
    static_cast<void>(
        RequireOwnedDirectory(config.state_directory, S_IRWXG | S_IRWXO));
    publication_directory = config.state_directory / kMcpEndpointDirectory;
    if (mkdir(publication_directory.c_str(), S_IRWXU) != 0 && errno != EEXIST) {
      throw SystemError("create MCP publication directory",
                        publication_directory);
    }
    try {
      static_cast<void>(RequireOwnedDirectory(publication_directory, 0U));
      const int descriptor =
          open(publication_directory.c_str(),
               O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
      if (descriptor < 0) {
        throw SystemError("open MCP publication directory",
                          publication_directory);
      }
      if (fchmod(descriptor, S_IRWXU) != 0) {
        const int error = errno;
        close(descriptor);
        throw SystemError("secure MCP publication directory",
                          publication_directory, error);
      }
      struct stat directory_status{};
      if (fstat(descriptor, &directory_status) != 0) {
        const int error = errno;
        close(descriptor);
        throw SystemError("inspect MCP publication directory",
                          publication_directory, error);
      }
      if (!S_ISDIR(directory_status.st_mode) ||
          directory_status.st_uid != geteuid() ||
          (directory_status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
        close(descriptor);
        throw SystemError("verify MCP publication directory",
                          publication_directory, EPERM);
      }
      publication_directory_descriptor = descriptor;
      publication_directory_identity = Identity(directory_status);
      publication_directory_opened = true;
      RemoveStalePublications(publication_directory_descriptor,
                              publication_directory);
      token = RandomToken();
      McpProtocolConfig protocol_config{
          .bearer_token = token,
          .endpoint_path = "/mcp",
          .endpoint_port = 0U,
          .allowed_operations = config.allowed_operations,
          .allowed_information_families =
              config.allowed_information_families,
          .read_only = config.read_only};
      server = std::make_unique<McpServer>(
          config.server, std::move(protocol_config), dispatcher.ToolHandler(),
          dispatcher.ResourceHandler(), dispatcher.SessionHandler());
      server->Start();
      dispatcher.SetNotificationHandler(
          [this](const McpServiceNotification& notification) {
            std::lock_guard<std::mutex> notification_lock(notification_mutex);
            if (server) {
              server->protocol().EnqueueNotification(notification.session_id,
                                                     notification.method,
                                                     notification.params);
            }
          });
      current_publication.endpoint = server->endpoint();
      current_publication.port = server->port();
      current_publication.token_file = publication_directory / kMcpTokenFile;
      current_publication.client_config_file =
          publication_directory / kMcpClientConfigFile;
      token_identity = WriteCredentialFile(publication_directory_descriptor,
                                           kMcpTokenFile, token + "\n");
      const boost::json::object client_config =
          BuildClientConfig(config.run_id, current_publication.endpoint, token,
                            current_publication.token_file);
      client_config_identity = WriteCredentialFile(
          publication_directory_descriptor, kMcpClientConfigFile,
          boost::json::serialize(client_config) + "\n");
      const FileIdentity visible_identity =
          RequireOwnedDirectory(publication_directory, S_IRWXG | S_IRWXO);
      if (!publication_directory_identity ||
          visible_identity.device != publication_directory_identity->device ||
          visible_identity.inode != publication_directory_identity->inode) {
        throw std::runtime_error(
            "MCP publication directory changed during startup");
      }
      started = true;
    } catch (...) {
      StopUnlockedNoThrow();
      throw;
    }
  }

  void Stop() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (stopped) {
      return;
    }
    std::exception_ptr failure;
    if (!services_stopped) {
      try {
        StopServicesUnlocked();
      } catch (...) {
        failure = std::current_exception();
      }
    }
    if (services_stopped) {
      try {
        RemovePublicationsUnlocked();
      } catch (...) {
        if (!failure) {
          failure = std::current_exception();
        }
      }
    }
    started = false;
    stopped =
        services_stopped && !publication_directory_opened && failure == nullptr;
    std::fill(token.begin(), token.end(), '\0');
    token.clear();
    if (failure) {
      std::rethrow_exception(failure);
    }
  }

  void StopAdmissionAndDrain() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (services_stopped || stopped) {
      return;
    }
    StopServicesUnlocked();
  }

  void StopServicesUnlocked() {
    if (services_stopped) {
      return;
    }
    std::exception_ptr failure;
    dispatcher.SetNotificationHandler({});
    if (!server) {
      server_stopped = true;
    }
    if (!server_stopped && server) {
      try {
        server->Stop();
        const McpServerStats stats = server->Stats();
        if (stats.running || stats.active_connections != 0U ||
            stats.queued_connections != 0U) {
          throw std::runtime_error("MCP server did not drain every connection");
        }
        server_stopped = true;
      } catch (...) {
        failure = std::current_exception();
      }
    }
    if (!dispatcher_stopped) {
      try {
        dispatcher.Shutdown();
        if (dispatcher.Stats().active_workers != 0U) {
          throw std::runtime_error(
              "MCP dispatcher did not drain every operation worker");
        }
        dispatcher_stopped = true;
      } catch (...) {
        if (!failure) {
          failure = std::current_exception();
        }
      }
    }
    if (server_stopped) {
      std::lock_guard<std::mutex> notification_lock(notification_mutex);
      server.reset();
    }
    started = false;
    services_stopped = server_stopped && dispatcher_stopped;
    if (failure) {
      std::rethrow_exception(failure);
    }
  }

  void RemovePublicationsUnlocked() {
    if (!publication_directory_opened) {
      return;
    }
    std::exception_ptr failure;
    const auto attempt = [&](auto&& action) {
      try {
        action();
      } catch (...) {
        if (!failure) {
          failure = std::current_exception();
        }
      }
    };
    if (publication_directory_descriptor >= 0) {
      attempt([&] {
        RemoveAndVerifyFile(publication_directory_descriptor,
                            kMcpClientConfigFile, client_config_identity);
      });
      attempt([&] {
        RemoveAndVerifyFile(publication_directory_descriptor, kMcpTokenFile,
                            token_identity);
      });
      attempt([&] {
        if (fsync(publication_directory_descriptor) != 0) {
          throw SystemError("sync MCP publication directory",
                            publication_directory);
        }
      });
      if (!failure) {
        attempt([&] {
          if (close(publication_directory_descriptor) != 0) {
            throw SystemError("close MCP publication directory",
                              publication_directory);
          }
          publication_directory_descriptor = -1;
        });
      }
    }
    if (!failure) {
      attempt([&] {
        const FileIdentity visible_identity =
            RequireOwnedDirectory(publication_directory, S_IRWXG | S_IRWXO);
        if (!publication_directory_identity ||
            visible_identity.device != publication_directory_identity->device ||
            visible_identity.inode != publication_directory_identity->inode) {
          throw std::runtime_error(
              "MCP publication directory changed during cleanup: " +
              publication_directory.string());
        }
        publication_directory_opened = false;
      });
    }
    if (failure) {
      std::rethrow_exception(failure);
    }
  }

  void StopUnlockedNoThrow() noexcept {
    if (!services_stopped) {
      try {
        StopServicesUnlocked();
      } catch (const std::exception& error) {
        BBP_LOG(error) << "MCP service cleanup failed: " << error.what();
      } catch (...) {
        BBP_LOG(error) << "MCP service cleanup failed";
      }
    }
    if (services_stopped) {
      try {
        RemovePublicationsUnlocked();
      } catch (const std::exception& error) {
        BBP_LOG(error) << "MCP credential cleanup failed: " << error.what();
      } catch (...) {
        BBP_LOG(error) << "MCP credential cleanup failed";
      }
    }
    started = false;
    stopped = services_stopped && !publication_directory_opened;
    std::fill(token.begin(), token.end(), '\0');
    token.clear();
  }

  void StopNoThrow() noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (!stopped) {
      StopUnlockedNoThrow();
    }
  }

  McpEndpointConfig config;
  McpDispatcher dispatcher;
  mutable std::mutex lifecycle_mutex;
  mutable std::mutex notification_mutex;
  std::unique_ptr<McpServer> server;
  std::filesystem::path publication_directory;
  int publication_directory_descriptor = -1;
  std::optional<FileIdentity> publication_directory_identity;
  std::optional<FileIdentity> token_identity;
  std::optional<FileIdentity> client_config_identity;
  McpEndpointPublication current_publication;
  std::string token;
  bool publication_directory_opened = false;
  bool server_stopped = false;
  bool dispatcher_stopped = false;
  bool services_stopped = false;
  bool started = false;
  bool stopped = false;
};

McpEndpoint::McpEndpoint(McpEndpointConfig config,
                         McpApplicationOperationFactory operation_factory,
                         McpApplicationResourceReader resource_reader)
    : impl_(std::make_unique<Impl>(std::move(config),
                                   std::move(operation_factory),
                                   std::move(resource_reader))) {}

McpEndpoint::~McpEndpoint() = default;

void McpEndpoint::Start() { impl_->Start(); }

void McpEndpoint::StopAdmissionAndDrain() { impl_->StopAdmissionAndDrain(); }

void McpEndpoint::Stop() { impl_->Stop(); }

bool McpEndpoint::running() const {
  std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
  return impl_->started && impl_->server && impl_->server->Stats().running;
}

McpEndpointPublication McpEndpoint::publication() const {
  std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
  if (!impl_->started || !impl_->server) {
    throw std::runtime_error("MCP endpoint is not running");
  }
  return impl_->current_publication;
}

McpServerStats McpEndpoint::ServerStats() const {
  std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
  if (!impl_->started || !impl_->server) {
    return {};
  }
  return impl_->server->Stats();
}

McpProtocolStats McpEndpoint::ProtocolStats() const {
  std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
  if (!impl_->started || !impl_->server) {
    return {};
  }
  return impl_->server->protocol().Stats();
}

McpOperationServiceStats McpEndpoint::DispatcherStats() const {
  return impl_->dispatcher.Stats();
}

void McpEndpoint::PublishEvidence(McpEvidenceRecord record) {
  if (impl_->dispatcher.Stats().accepting) {
    impl_->dispatcher.Publish(std::move(record));
  }
}

void McpEndpoint::CloseRunSubscriptions(std::string_view run_id) {
  if (impl_->dispatcher.Stats().accepting) {
    impl_->dispatcher.CloseRunSubscriptions(run_id);
  }
}

McpDispatcher& McpEndpoint::dispatcher() { return impl_->dispatcher; }

}  // namespace bbp
