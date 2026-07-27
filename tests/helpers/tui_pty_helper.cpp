#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pty.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

inline constexpr auto kSlowNodeReplacementPhaseDelay =
    std::chrono::milliseconds(17750);

class PtyProcess {
 public:
  PtyProcess(const std::filesystem::path& command,
             std::vector<std::string> arguments, unsigned short rows,
             unsigned short cols,
             const std::filesystem::path& home_directory = {}) {
    struct winsize size{};
    size.ws_row = rows;
    size.ws_col = cols;
    pid_ = forkpty(&master_fd_, nullptr, nullptr, &size);
    if (pid_ < 0) {
      throw std::system_error(errno, std::generic_category(), "forkpty");
    }
    if (pid_ == 0) {
      static_cast<void>(setenv("TERM", "xterm", 1));
      static_cast<void>(setenv("ESCDELAY", "25", 1));
      if (!home_directory.empty()) {
        static_cast<void>(setenv("HOME", home_directory.c_str(), 1));
      }
      std::vector<char*> argv;
      argv.reserve(arguments.size() + 2U);
      argv.push_back(const_cast<char*>(command.c_str()));
      for (std::string& argument : arguments) {
        argv.push_back(argument.data());
      }
      argv.push_back(nullptr);
      execv(command.c_str(), argv.data());
      _exit(127);
    }
  }

  PtyProcess(const PtyProcess&) = delete;
  PtyProcess& operator=(const PtyProcess&) = delete;

  ~PtyProcess() {
    if (pid_ > 0) {
      static_cast<void>(kill(pid_, SIGINT));
      const auto deadline = std::chrono::steady_clock::now() + 5s;
      while (pid_ > 0 && std::chrono::steady_clock::now() < deadline) {
        const pid_t result = waitpid(pid_, nullptr, WNOHANG);
        if (result == pid_ || (result < 0 && errno == ECHILD)) {
          pid_ = -1;
          break;
        }
        std::this_thread::sleep_for(10ms);
      }
      if (pid_ > 0) {
        static_cast<void>(kill(pid_, SIGKILL));
        static_cast<void>(waitpid(pid_, nullptr, 0));
      }
    }
    if (master_fd_ >= 0) {
      static_cast<void>(close(master_fd_));
    }
  }

  std::string ReadFor(std::chrono::milliseconds duration) const {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    std::string output;
    while (std::chrono::steady_clock::now() < deadline) {
      const auto remaining =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              deadline - std::chrono::steady_clock::now());
      const int timeout =
          static_cast<int>(std::min<std::int64_t>(remaining.count(), 50));
      pollfd descriptor{
          .fd = master_fd_,
          .events = POLLIN,
          .revents = 0,
      };
      const int poll_result = poll(&descriptor, 1, timeout);
      if (poll_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(), "poll PTY");
      }
      if (poll_result == 0) {
        continue;
      }
      char buffer[8192];
      const ssize_t count = read(master_fd_, buffer, sizeof(buffer));
      if (count > 0) {
        output.append(buffer, static_cast<std::size_t>(count));
      } else if (count == 0 || (count < 0 && errno == EIO)) {
        break;
      } else if (errno != EINTR) {
        throw std::system_error(errno, std::generic_category(), "read PTY");
      }
    }
    return output;
  }

  std::string ReadUntil(std::string_view expected,
                        std::chrono::milliseconds timeout,
                        std::string_view context) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string output;
    while (std::chrono::steady_clock::now() < deadline) {
      output += ReadFor(100ms);
      if (output.find(expected) != std::string::npos) {
        return output;
      }
    }
    constexpr std::size_t kDiagnosticTail = 8192U;
    const std::size_t diagnostic_begin =
        output.size() > kDiagnosticTail ? output.size() - kDiagnosticTail : 0U;
    throw std::runtime_error(
        std::string(context) + " did not render: " + std::string(expected) +
        "\nPTY output tail:\n" + output.substr(diagnostic_begin));
  }

  void Write(std::string_view input) const {
    std::size_t written = 0U;
    while (written < input.size()) {
      const ssize_t count =
          write(master_fd_, input.data() + written, input.size() - written);
      if (count > 0) {
        written += static_cast<std::size_t>(count);
      } else if (count < 0 && errno != EINTR) {
        throw std::system_error(errno, std::generic_category(), "write PTY");
      }
    }
  }

  void Resize(unsigned short rows, unsigned short cols) const {
    struct winsize size{};
    size.ws_row = rows;
    size.ws_col = cols;
    if (ioctl(master_fd_, TIOCSWINSZ, &size) != 0) {
      throw std::system_error(errno, std::generic_category(), "resize PTY");
    }
    if (kill(pid_, SIGWINCH) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "signal PTY resize");
    }
  }

  bool Running() const {
    return pid_ > 0 && (kill(pid_, 0) == 0 || errno == EPERM);
  }

  int Wait(std::chrono::milliseconds timeout = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    int status = 0;
    while (std::chrono::steady_clock::now() < deadline) {
      const pid_t result = waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        if (WIFEXITED(status)) {
          return WEXITSTATUS(status);
        }
        return 128 + WTERMSIG(status);
      }
      if (result < 0 && errno != EINTR) {
        throw std::system_error(errno, std::generic_category(), "waitpid");
      }
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("PTY child did not exit before the deadline");
  }

 private:
  pid_t pid_ = -1;
  int master_fd_ = -1;
};

class OwnedTemporaryDirectory {
 public:
  explicit OwnedTemporaryDirectory(std::string_view label) {
    const std::filesystem::path temporary_root =
        std::filesystem::temp_directory_path();
    for (unsigned int attempt = 0U; attempt < 100U; ++attempt) {
      root_ = temporary_root /
              ("bbp-tui-pty-" + std::string(label) + "-" +
               std::to_string(getpid()) + "-" + std::to_string(attempt));
      if (std::filesystem::create_directory(root_)) {
        return;
      }
    }
    throw std::runtime_error("could not create owned PTY test directory");
  }

  OwnedTemporaryDirectory(const OwnedTemporaryDirectory&) = delete;
  OwnedTemporaryDirectory& operator=(const OwnedTemporaryDirectory&) = delete;

  ~OwnedTemporaryDirectory() {
    std::error_code error;
    std::filesystem::remove_all(root_, error);
  }

  const std::filesystem::path& root() const { return root_; }

 private:
  std::filesystem::path root_;
};

class TcpListener {
 public:
  TcpListener(std::string_view address, std::uint16_t port) {
    descriptor_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    if (descriptor_ < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "create TCP collision listener");
    }
    sockaddr_in endpoint{};
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    if (inet_pton(AF_INET, std::string(address).c_str(), &endpoint.sin_addr) !=
        1) {
      close(descriptor_);
      descriptor_ = -1;
      throw std::runtime_error("invalid TCP collision-listener address");
    }
    if (bind(descriptor_, reinterpret_cast<const sockaddr*>(&endpoint),
             sizeof(endpoint)) != 0 ||
        listen(descriptor_, 1) != 0) {
      const int error = errno;
      close(descriptor_);
      descriptor_ = -1;
      throw std::system_error(error, std::generic_category(),
                              "bind TCP collision listener");
    }
  }

  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;

  ~TcpListener() {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
    }
  }

 private:
  int descriptor_ = -1;
};

class OwnedRunCopy {
 public:
  OwnedRunCopy(const std::filesystem::path& source, std::string_view label)
      : directory_(label), run_root_(directory_.root() / "run") {
    std::filesystem::copy(source, run_root_,
                          std::filesystem::copy_options::recursive);
  }

  const std::filesystem::path& run_root() const { return run_root_; }

  void AppendEvent(std::string_view event) const {
    std::ofstream stream(run_root_ / "events.jsonl", std::ios::app);
    if (!stream) {
      throw std::runtime_error("could not append PTY test event");
    }
    stream << event << '\n';
    if (!stream) {
      throw std::runtime_error("could not flush PTY test event");
    }
  }

  void WriteSimulatorLog(const std::vector<std::string>& records) const {
    std::ofstream stream(run_root_ / "simulator.log",
                         std::ios::binary | std::ios::trunc);
    if (!stream) {
      throw std::runtime_error("could not create PTY simulator log");
    }
    for (const std::string& record : records) {
      stream << record << '\n';
    }
    if (!stream) {
      throw std::runtime_error("could not flush PTY simulator log");
    }
  }

 private:
  OwnedTemporaryDirectory directory_;
  std::filesystem::path run_root_;
};

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream stream(path);
  if (!stream) {
    return {};
  }
  return std::string(std::istreambuf_iterator<char>(stream),
                     std::istreambuf_iterator<char>());
}

std::string WaitForFileText(const std::filesystem::path& path,
                            std::string_view expected,
                            std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string contents;
  while (std::chrono::steady_clock::now() < deadline) {
    contents = ReadFile(path);
    if (contents.find(expected) != std::string::npos) {
      return contents;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("timed out waiting for " + path.string() +
                           " to contain " + std::string(expected));
}

std::size_t CountOccurrences(std::string_view text, std::string_view expected) {
  std::size_t count = 0U;
  std::size_t offset = 0U;
  while ((offset = text.find(expected, offset)) != std::string_view::npos) {
    ++count;
    offset += expected.size();
  }
  return count;
}

std::string WaitForFileOccurrences(const std::filesystem::path& path,
                                   std::string_view expected,
                                   std::size_t minimum_count,
                                   std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string contents;
  while (std::chrono::steady_clock::now() < deadline) {
    contents = ReadFile(path);
    if (CountOccurrences(contents, expected) >= minimum_count) {
      return contents;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("timed out waiting for " + path.string() +
                           " occurrence count for " + std::string(expected));
}

http::response<http::string_body> McpExchange(
    std::uint16_t port, std::string_view token, std::string body,
    std::string_view session_id = {}, std::string_view protocol_version = {}) {
  asio::io_context io_context;
  beast::tcp_stream stream(io_context);
  stream.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port));
  http::request<http::string_body> request{http::verb::post, "/mcp", 11};
  request.set(http::field::host, "127.0.0.1:" + std::to_string(port));
  request.set(http::field::origin, "http://127.0.0.1:" + std::to_string(port));
  request.set(http::field::content_type, "application/json");
  request.set(http::field::accept, "application/json, text/event-stream");
  request.set(http::field::authorization, "Bearer " + std::string(token));
  request.set(http::field::connection, "close");
  if (!session_id.empty()) {
    request.set("Mcp-Session-Id", session_id);
    request.set("MCP-Protocol-Version", protocol_version);
  }
  request.body() = std::move(body);
  request.prepare_payload();
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  return response;
}

boost::json::object McpStructuredContent(
    const http::response<http::string_body>& response,
    std::string_view context) {
  if (response.result() != http::status::ok) {
    throw std::runtime_error(std::string(context) + " returned HTTP " +
                             std::to_string(response.result_int()) + ": " +
                             response.body());
  }
  const boost::json::value decoded = boost::json::parse(response.body());
  if (!decoded.is_object()) {
    throw std::runtime_error(std::string(context) +
                             " returned a non-object JSON-RPC response");
  }
  const boost::json::value* result = decoded.as_object().if_contains("result");
  if (result == nullptr || !result->is_object()) {
    throw std::runtime_error(std::string(context) +
                             " returned no JSON-RPC result");
  }
  const boost::json::value* structured =
      result->as_object().if_contains("structuredContent");
  if (structured == nullptr || !structured->is_object()) {
    throw std::runtime_error(std::string(context) +
                             " returned no structured content");
  }
  return structured->as_object();
}

boost::json::object McpToolCall(std::uint16_t port, std::string_view token,
                                std::string_view session_id,
                                std::string_view protocol_version,
                                std::uint64_t request_id, std::string_view name,
                                boost::json::object arguments) {
  boost::json::object result = McpStructuredContent(
      McpExchange(port, token,
                  boost::json::serialize(boost::json::object{
                      {"jsonrpc", "2.0"},
                      {"id", request_id},
                      {"method", "tools/call"},
                      {"params", boost::json::object{{"name", name},
                                                     {"arguments",
                                                      std::move(arguments)}}}}),
                  session_id, protocol_version),
      name);
  if (const boost::json::value* error = result.if_contains("isError");
      error != nullptr && error->is_bool() && error->as_bool()) {
    throw std::runtime_error(
        std::string(name) +
        " returned an MCP tool error: " + boost::json::serialize(result));
  }
  return result;
}

struct McpTestSession {
  std::uint16_t port = 0U;
  std::string token;
  std::string protocol_version;
  std::string session_id;
};

McpTestSession ConnectMcpTestSession(
    const std::filesystem::path& publication_directory) {
  const boost::json::value client = boost::json::parse(WaitForFileText(
      publication_directory / "client.json", "\"endpoint\"", 5s));
  if (!client.is_object()) {
    throw std::runtime_error("MCP client publication is not an object");
  }
  const boost::json::object& object = client.as_object();
  const std::string endpoint(object.at("endpoint").as_string());
  constexpr std::string_view kEndpointPrefix = "http://127.0.0.1:";
  constexpr std::string_view kEndpointSuffix = "/mcp";
  if (!endpoint.starts_with(kEndpointPrefix) ||
      !endpoint.ends_with(kEndpointSuffix)) {
    throw std::runtime_error("MCP client endpoint is not loopback HTTP");
  }
  const std::string port_text = endpoint.substr(
      kEndpointPrefix.size(),
      endpoint.size() - kEndpointPrefix.size() - kEndpointSuffix.size());
  const unsigned long parsed_port = std::stoul(port_text);
  if (parsed_port == 0U || parsed_port > 65535U) {
    throw std::runtime_error("MCP client endpoint port is invalid");
  }
  McpTestSession session{
      .port = static_cast<std::uint16_t>(parsed_port),
      .token = ReadFile(publication_directory / "token"),
      .protocol_version =
          std::string(object.at("protocol_version").as_string()),
      .session_id = {},
  };
  if (!session.token.empty() && session.token.back() == '\n') {
    session.token.pop_back();
  }
  const http::response<http::string_body> initialized = McpExchange(
      session.port, session.token,
      boost::json::serialize(boost::json::object{
          {"jsonrpc", "2.0"},
          {"id", 1U},
          {"method", "initialize"},
          {"params",
           boost::json::object{
               {"protocolVersion", session.protocol_version},
               {"capabilities", boost::json::object{}},
               {"clientInfo",
                boost::json::object{{"name", "bbp-pty-node-replace-test"},
                                    {"version", "1"}}}}}}));
  if (initialized.result() != http::status::ok ||
      initialized.find("Mcp-Session-Id") == initialized.end()) {
    throw std::runtime_error("MCP initialization failed: " +
                             initialized.body());
  }
  session.session_id = initialized.at("Mcp-Session-Id");
  const http::response<http::string_body> notification =
      McpExchange(session.port, session.token,
                  boost::json::serialize(boost::json::object{
                      {"jsonrpc", "2.0"},
                      {"method", "notifications/initialized"},
                      {"params", boost::json::object{}}}),
                  session.session_id, session.protocol_version);
  if (notification.result() != http::status::accepted) {
    throw std::runtime_error("MCP initialized notification failed");
  }
  return session;
}

void RequireContains(std::string_view text, std::string_view expected,
                     std::string_view context) {
  if (text.find(expected) == std::string_view::npos) {
    throw std::runtime_error(std::string(context) +
                             " did not contain: " + std::string(expected));
  }
}

void RequireNotContains(std::string_view text, std::string_view unexpected,
                        std::string_view context) {
  if (text.find(unexpected) != std::string_view::npos) {
    throw std::runtime_error(
        std::string(context) +
        " unexpectedly contained: " + std::string(unexpected));
  }
}

std::string McpEndpointFromClientConfig(std::string_view client_config,
                                        std::string_view context) {
  const boost::json::value decoded = boost::json::parse(client_config);
  if (!decoded.is_object()) {
    throw std::runtime_error(std::string(context) +
                             " client configuration is not an object");
  }
  const boost::json::value* endpoint =
      decoded.as_object().if_contains("endpoint");
  if (endpoint == nullptr || !endpoint->is_string()) {
    throw std::runtime_error(std::string(context) +
                             " client configuration has no endpoint");
  }
  return std::string(endpoint->as_string());
}

std::string McpTokenValue(std::string token, std::string_view context) {
  if (!token.empty() && token.back() == '\n') {
    token.pop_back();
  }
  if (token.empty()) {
    throw std::runtime_error(std::string(context) + " token is empty");
  }
  return token;
}

std::string OpenMcpConnectionPane(const PtyProcess& process,
                                  std::string_view activation_key,
                                  std::string_view endpoint,
                                  const std::filesystem::path& token_path,
                                  const std::filesystem::path& client_path,
                                  std::string_view token,
                                  std::string_view context) {
  process.Write(activation_key);
  const std::string output =
      process.ReadUntil("Enter, Esc, or i dismisses this pane.", 3s, context);
  RequireContains(output, "MCP connection", context);
  RequireContains(output, endpoint, context);
  RequireContains(output, "Authorization: Bearer <contents of token file>",
                  context);
  RequireContains(output, token_path.string(), context);
  RequireContains(output, client_path.string(), context);
  RequireContains(output, "Codex entry: codex_config_toml", context);
  RequireContains(output, "OpenCode entry: opencode_config", context);
  RequireContains(output, "Credential values are not displayed", context);
  RequireNotContains(output, token, context);
  return output;
}

void RequireExitZero(PtyProcess* process, std::string_view context) {
  const int result = process->Wait();
  if (result != 0) {
    throw std::runtime_error(std::string(context) + " exited " +
                             std::to_string(result));
  }
}

std::string DaemonArgumentValue(int argc, char** argv,
                                std::string_view prefix) {
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (argument.starts_with(prefix)) {
      return std::string(argument.substr(prefix.size()));
    }
  }
  throw std::runtime_error("ready daemon missing argument " +
                           std::string(prefix));
}

std::vector<std::string> DaemonArgumentValues(int argc, char** argv,
                                              std::string_view prefix) {
  std::vector<std::string> values;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (argument.starts_with(prefix)) {
      values.emplace_back(argument.substr(prefix.size()));
    }
  }
  return values;
}

void SendAll(int descriptor, std::string_view text) {
  while (!text.empty()) {
    const ssize_t sent =
        send(descriptor, text.data(), text.size(), MSG_NOSIGNAL);
    if (sent < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(),
                              "send ready-daemon response");
    }
    text.remove_prefix(static_cast<std::size_t>(sent));
  }
}

std::string ReadHttpRequest(int descriptor) {
  constexpr std::size_t kMaximumRequestBytes = 64U * 1024U;
  std::string request;
  std::size_t expected_size = 0U;
  while (expected_size == 0U || request.size() < expected_size) {
    char buffer[4096];
    const ssize_t received = recv(descriptor, buffer, sizeof(buffer), 0);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(),
                              "read ready-daemon request");
    }
    if (received == 0) {
      throw std::runtime_error("ready-daemon request ended early");
    }
    request.append(buffer, static_cast<std::size_t>(received));
    if (request.size() > kMaximumRequestBytes) {
      throw std::runtime_error("ready-daemon request is too large");
    }
    const std::size_t header_end = request.find("\r\n\r\n");
    if (header_end != std::string::npos && expected_size == 0U) {
      constexpr std::string_view kContentLength = "\r\nContent-Length: ";
      const std::size_t field = request.find(kContentLength);
      if (field == std::string::npos || field >= header_end) {
        throw std::runtime_error("ready-daemon request has no content length");
      }
      const std::size_t value_begin = field + kContentLength.size();
      const std::size_t value_end = request.find("\r\n", value_begin);
      const std::size_t content_length =
          std::stoul(request.substr(value_begin, value_end - value_begin));
      expected_size = header_end + 4U + content_length;
      if (expected_size > kMaximumRequestBytes) {
        throw std::runtime_error("ready-daemon request is too large");
      }
    }
  }
  return request;
}

std::string RpcMethod(std::string_view request) {
  constexpr std::string_view kMethod = "\"method\":\"";
  const std::size_t begin_field = request.find(kMethod);
  if (begin_field == std::string_view::npos) {
    throw std::runtime_error("ready-daemon request has no RPC method");
  }
  const std::size_t begin = begin_field + kMethod.size();
  const std::size_t end = request.find('"', begin);
  if (end == std::string_view::npos) {
    throw std::runtime_error("ready-daemon RPC method is malformed");
  }
  return std::string(request.substr(begin, end - begin));
}

std::string RpcFirstStringParameter(std::string_view request,
                                    std::string_view method) {
  const std::size_t body_begin = request.find("\r\n\r\n");
  if (body_begin == std::string_view::npos) {
    throw std::runtime_error("ready daemon RPC has no body");
  }
  const boost::json::value decoded =
      boost::json::parse(request.substr(body_begin + 4U));
  const boost::json::value* parameters =
      decoded.as_object().if_contains("params");
  if (parameters == nullptr || !parameters->is_array() ||
      parameters->as_array().empty() ||
      !parameters->as_array().front().is_string()) {
    throw std::runtime_error("ready daemon " + std::string(method) +
                             " RPC has no string target");
  }
  return std::string(parameters->as_array().front().as_string());
}

void WriteReadyDaemonPeerState(const std::filesystem::path& path,
                               const std::set<std::string>& peers) {
  std::ofstream stream(path);
  if (!stream) {
    throw std::runtime_error("could not record ready-daemon peer state");
  }
  for (const std::string& peer : peers) {
    stream << peer << '\n';
  }
}

void AppendReadyDaemonRpcAudit(const std::filesystem::path& path,
                               std::string_view method, std::string_view target,
                               std::string_view phase) {
  std::ofstream stream(path, std::ios::app);
  if (!stream) {
    throw std::runtime_error("could not append ready-daemon RPC audit");
  }
  stream << method << ' ' << target << ' ' << phase << '\n';
  if (!stream) {
    throw std::runtime_error("could not flush ready-daemon RPC audit");
  }
}

void AppendReadyDaemonTimingAudit(const std::filesystem::path& run_root,
                                  std::string_view node_root_name,
                                  std::string_view phase) {
  std::ofstream stream(run_root / "bbp-test-slow-node-replace-audit",
                       std::ios::app);
  if (!stream) {
    throw std::runtime_error("could not append slow ready-daemon timing audit");
  }
  stream << node_root_name << ' ' << phase << '\n';
  if (!stream) {
    throw std::runtime_error("could not flush slow ready-daemon timing audit");
  }
}

std::string RpcResult(std::string_view method, std::string_view request,
                      std::set<std::string>* peers,
                      const std::filesystem::path& peer_state_path,
                      bool slow_replacement,
                      std::optional<std::chrono::steady_clock::time_point>*
                          slow_synchronization_started,
                      bool* slow_synchronization_completed) {
  const std::filesystem::path node_root =
      peer_state_path.parent_path().parent_path();
  const std::filesystem::path run_root = node_root.parent_path().parent_path();
  const std::filesystem::path directed_peer_drop_control =
      run_root / "bbp-test-drop-directed-peer-on-target-stop";
  const std::filesystem::path directed_peer_drop_trigger =
      run_root / "bbp-test-directed-peer-target-stopped";
  if (method == "getblockchaininfo") {
    if (slow_replacement) {
      const auto now = std::chrono::steady_clock::now();
      if (!*slow_synchronization_started) {
        *slow_synchronization_started = now;
        AppendReadyDaemonTimingAudit(run_root, node_root.filename().string(),
                                     "synchronization-delay-started");
      } else if (now < **slow_synchronization_started +
                           kSlowNodeReplacementPhaseDelay) {
        return R"({"blocks":0,"headers":0,"bestblockhash":"00","initialblockdownload":true,"verificationprogress":0.5,"difficulty":1.0,"mediantime":0,"chainwork":"00"})";
      } else if (!*slow_synchronization_completed) {
        AppendReadyDaemonTimingAudit(run_root, node_root.filename().string(),
                                     "synchronization-delay-completed");
        *slow_synchronization_completed = true;
      }
    }
    return R"({"blocks":0,"headers":0,"bestblockhash":"00","initialblockdownload":false,"verificationprogress":1.0,"difficulty":1.0,"mediantime":0,"chainwork":"00"})";
  }
  if (method == "getnetworkinfo") {
    return boost::json::serialize(
        boost::json::object{{"version", 1U},
                            {"protocolversion", 1U},
                            {"subversion", "/bbp-test/"},
                            {"connections", peers->size()}});
  }
  if (method == "getmempoolinfo") {
    return R"({"size":0,"bytes":0})";
  }
  if (method == "getblockheader") {
    return R"({"time":0})";
  }
  if (method == "getnetworkhashps") {
    return "0";
  }
  if (method == "getpeerinfo") {
    const std::filesystem::path directed_peer_drop_consumed =
        peer_state_path.parent_path() / "bbp-test-directed-peer-drop-consumed";
    if (node_root.filename() != "firo-1" &&
        std::filesystem::exists(directed_peer_drop_trigger) &&
        !std::filesystem::exists(directed_peer_drop_consumed)) {
      peers->clear();
      WriteReadyDaemonPeerState(peer_state_path, *peers);
      std::ofstream consumed(directed_peer_drop_consumed);
      if (!consumed) {
        throw std::runtime_error(
            "could not record directed peer-drop consumption");
      }
      consumed << "target peer dropped\n";
    }
    boost::json::array result;
    for (const std::string& peer : *peers) {
      result.emplace_back(boost::json::object{
          {"addr", peer},
          {"bytesrecv_per_msg", boost::json::object{{"verack", 1U}}}});
    }
    return boost::json::serialize(result);
  }
  if (method == "listbanned") {
    return "[]";
  }
  if (method == "setban") {
    return "null";
  }
  if (method == "addnode") {
    const std::string target = RpcFirstStringParameter(request, method);
    const std::filesystem::path staged_events =
        run_root / ".runtime-node-replace-events.pending";
    std::string_view phase = "outside-replace";
    if (std::filesystem::exists(staged_events)) {
      phase = ReadFile(staged_events).find("\"event\":\"height_reached\"") ==
                      std::string::npos
                  ? "before-height"
                  : "after-height";
    }
    AppendReadyDaemonRpcAudit(
        peer_state_path.parent_path() / "bbp-test-rpc-audit", method, target,
        phase);
    peers->insert(target);
    WriteReadyDaemonPeerState(peer_state_path, *peers);
    return "null";
  }
  if (method == "disconnectnode") {
    peers->erase(RpcFirstStringParameter(request, method));
    WriteReadyDaemonPeerState(peer_state_path, *peers);
    return "null";
  }
  if (method == "stop") {
    if (node_root.filename() == "firo-1" &&
        std::filesystem::exists(directed_peer_drop_control)) {
      std::ofstream trigger(directed_peer_drop_trigger);
      if (!trigger) {
        throw std::runtime_error("could not create directed peer-drop trigger");
      }
      trigger << "target stopped\n";
    }
    return "null";
  }
  throw std::runtime_error("ready daemon received unsupported RPC method " +
                           std::string(method));
}

int RunReadyFiroDaemon(int argc, char** argv) {
  const std::filesystem::path data_directory =
      DaemonArgumentValue(argc, argv, "-datadir=");
  const std::filesystem::path node_root = data_directory.parent_path();
  const std::filesystem::path run_root = node_root.parent_path().parent_path();
  const bool slow_replacement =
      std::filesystem::exists(run_root / "bbp-test-slow-node-replace");
  if (node_root.filename().string().starts_with("bbpr-") &&
      std::filesystem::exists(run_root / "bbp-test-stall-final-replacement")) {
    std::ofstream marker(run_root / "nodes" / "firo-1" / "data" /
                         "bbp-test-stall-readiness");
    if (!marker) {
      throw std::runtime_error(
          "could not create final replacement readiness marker");
    }
    marker << "stall final replacement readiness\n";
  }
  if (std::filesystem::exists(data_directory / "bbp-test-stall-readiness")) {
    for (;;) {
      pause();
    }
  }
  const std::filesystem::path cookie =
      DaemonArgumentValue(argc, argv, "-rpccookiefile=");
  const std::string bind_address = DaemonArgumentValue(argc, argv, "-rpcbind=");
  const unsigned long parsed_port =
      std::stoul(DaemonArgumentValue(argc, argv, "-rpcport="));
  if (parsed_port == 0U || parsed_port > 65535U) {
    throw std::runtime_error("ready-daemon RPC port is out of range");
  }
  {
    std::ofstream stream(cookie);
    if (!stream) {
      throw std::runtime_error("could not create ready-daemon RPC cookie");
    }
    stream << "bbp-test:bbp-test";
  }
  if (chmod(cookie.c_str(), 0600) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "protect ready-daemon RPC cookie");
  }
  if (slow_replacement) {
    AppendReadyDaemonTimingAudit(run_root, node_root.filename().string(),
                                 "readiness-delay-started");
    std::this_thread::sleep_for(kSlowNodeReplacementPhaseDelay);
    AppendReadyDaemonTimingAudit(run_root, node_root.filename().string(),
                                 "readiness-delay-completed");
  }

  const int listener = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (listener < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "create ready-daemon listener");
  }
  const auto close_listener = [&] { static_cast<void>(close(listener)); };
  int reuse = 1;
  if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) !=
      0) {
    const int error = errno;
    close_listener();
    throw std::system_error(error, std::generic_category(),
                            "configure ready-daemon listener");
  }
  sockaddr_in endpoint{};
  endpoint.sin_family = AF_INET;
  endpoint.sin_port = htons(static_cast<std::uint16_t>(parsed_port));
  if (inet_pton(AF_INET, bind_address.c_str(), &endpoint.sin_addr) != 1) {
    close_listener();
    throw std::runtime_error("ready-daemon RPC bind address is invalid");
  }
  if (bind(listener, reinterpret_cast<const sockaddr*>(&endpoint),
           sizeof(endpoint)) != 0 ||
      listen(listener, 16) != 0) {
    const int error = errno;
    close_listener();
    throw std::system_error(error, std::generic_category(),
                            "bind ready-daemon listener");
  }

  const std::vector<std::string> configured_peers =
      DaemonArgumentValues(argc, argv, "-connect=");
  std::set<std::string> peers(configured_peers.begin(), configured_peers.end());
  const std::filesystem::path peer_state_path =
      data_directory / "bbp-test-configured-peers";
  if (!peers.empty()) {
    WriteReadyDaemonPeerState(peer_state_path, peers);
  }
  std::optional<std::chrono::steady_clock::time_point>
      slow_synchronization_started;
  bool slow_synchronization_completed = false;
  bool stop = false;
  while (!stop) {
    int connection;
    do {
      connection = accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
    } while (connection < 0 && errno == EINTR);
    if (connection < 0) {
      const int error = errno;
      close_listener();
      throw std::system_error(error, std::generic_category(),
                              "accept ready-daemon request");
    }
    try {
      const std::string request = ReadHttpRequest(connection);
      const std::string method = RpcMethod(request);
      const std::string body =
          "{\"result\":" +
          RpcResult(method, request, &peers, peer_state_path, slow_replacement,
                    &slow_synchronization_started,
                    &slow_synchronization_completed) +
          ",\"error\":null,\"id\":\"bbp\"}";
      const std::string response =
          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
          "Connection: close\r\nContent-Length: " +
          std::to_string(body.size()) + "\r\n\r\n" + body;
      SendAll(connection, response);
      stop = method == "stop";
    } catch (...) {
      static_cast<void>(close(connection));
      close_listener();
      throw;
    }
    static_cast<void>(close(connection));
  }
  close_listener();
  return 0;
}

std::filesystem::path LauncherPathFromOutput(std::string_view output) {
  constexpr std::string_view prefix = "/tmp/bbp-firo-qt-";
  constexpr std::size_t random_length = 6U;
  constexpr std::string_view suffix = ".sh";
  const std::size_t begin = output.find(prefix);
  if (begin == std::string_view::npos) {
    return {};
  }
  const std::size_t length = prefix.size() + random_length + suffix.size();
  if (begin + length > output.size() ||
      output.substr(begin + prefix.size() + random_length, suffix.size()) !=
          suffix) {
    return {};
  }
  return std::string(output.substr(begin, length));
}

std::pair<std::string, std::filesystem::path> ReadLauncherDialog(
    const PtyProcess& process, std::string_view context) {
  std::string output =
      process.ReadUntil("Native Firo-Qt launcher", 3s, context);
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  std::filesystem::path path = LauncherPathFromOutput(output);
  while (path.empty() && std::chrono::steady_clock::now() < deadline) {
    output += process.ReadFor(100ms);
    path = LauncherPathFromOutput(output);
  }
  if (path.empty()) {
    throw std::runtime_error(std::string(context) +
                             " did not show its complete script path");
  }
  RequireContains(output, "Script path:", context);
  RequireContains(output, "Complete command:", context);
  RequireContains(output, "'-regtest'", context);
  RequireContains(output, "'-connect=", context);
  RequireContains(output, "'-maxconnections=1'", context);
  RequireContains(output, "'-upnp=0'", context);
  RequireContains(output, "BBP has not launched Firo-Qt", context);
  return {std::move(output), std::move(path)};
}

void CheckCanonicalExitModal(const std::filesystem::path& command,
                             const std::filesystem::path& run_root) {
  PtyProcess process(command, {"--run", run_root.string()}, 24, 80);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 3s,
                                      "canonical report"));
  process.Write("\x1b");
  const std::string popup =
      process.ReadUntil("Confirm exit", 3s, "canonical exit modal");
  RequireContains(popup, "Press y to exit; n or Esc cancels.",
                  "canonical exit modal");
  process.Write("n");
  if (process.ReadFor(500ms).empty()) {
    throw std::runtime_error("canonical cancel path did not refresh");
  }
  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "canonical second exit modal"));
  process.Write("y");
  RequireExitZero(&process, "canonical exit modal");
}

void CorruptNextRefresh(const OwnedRunCopy& run) {
  run.AppendEvent("{malformed-event");
}

void CheckPaletteOnErrorFrame(const std::filesystem::path& command,
                              const std::filesystem::path& source_run) {
  OwnedRunCopy run(source_run, "palette");
  PtyProcess process(command, {"--run", run.run_root().string()}, 30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 3s,
                                      "palette report"));
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Live command", 3s, "palette before report error"));
  CorruptNextRefresh(run);
  static_cast<void>(
      process.ReadUntil("error:", 3s, "palette report-error frame"));
  process.Write("x");
  if (process.ReadFor(500ms).empty()) {
    throw std::runtime_error("palette was not interactive on the error frame");
  }
  process.Write("\x1bq");
  RequireExitZero(&process, "palette report-error frame");
}

void CheckCommandErrorOnErrorFrame(const std::filesystem::path& command,
                                   const std::filesystem::path& source_run) {
  OwnedRunCopy run(source_run, "command-error");
  PtyProcess process(command, {"--run", run.run_root().string()}, 30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 3s,
                                      "command-error report"));
  run.AppendEvent(
      "{\"run_id\":\"tui-fixture\",\"node_id\":\"sim\","
      "\"timestamp\":\"2026-07-22T00:00:00Z\","
      "\"event\":\"operator_command_completed\","
      "\"detail\":\"{\\\"sequence\\\":9000,"
      "\\\"kind\\\":\\\"add_nodes\\\","
      "\\\"added_node_ids\\\":[\\\"firo-2\\\"],"
      "\\\"inventory_generation\\\":2,"
      "\\\"final_node_count\\\":2}\"}");
  static_cast<void>(process.ReadUntil("generation 2, 2 total nodes", 3s,
                                      "numeric node-add completion status"));
  run.AppendEvent(
      "{\"run_id\":\"tui-fixture\",\"node_id\":\"firo-1\","
      "\"timestamp\":\"2026-07-22T00:00:00Z\","
      "\"event\":\"operator_command_failed\","
      "\"detail\":\"{\\\"sequence\\\":9001,"
      "\\\"kind\\\":\\\"kill\\\","
      "\\\"error\\\":\\\"forced command failure\\\"}\"}");
  static_cast<void>(process.ReadUntil("Command error", 3s,
                                      "command error before report error"));
  CorruptNextRefresh(run);
  static_cast<void>(
      process.ReadUntil("error:", 3s, "command-error report-error frame"));
  process.Write("\x1bq");
  RequireExitZero(&process, "command-error report-error frame");
}

pid_t ProcessStartedPid(std::string_view events) {
  constexpr std::string_view marker = "\\\"pid\\\":";
  const std::size_t process_event =
      events.rfind("\"event\":\"process_started\"");
  const std::size_t pid_field = events.find(marker, process_event);
  if (process_event == std::string_view::npos ||
      pid_field == std::string_view::npos) {
    throw std::runtime_error("process_started event did not contain a pid");
  }
  const std::size_t begin = pid_field + marker.size();
  const std::size_t end = events.find_first_not_of("0123456789", begin);
  const std::string text(events.substr(begin, end - begin));
  const long parsed = std::stol(text);
  if (parsed <= 0) {
    throw std::runtime_error("process_started pid was not positive");
  }
  return static_cast<pid_t>(parsed);
}

bool ProcessExists(pid_t pid) { return kill(pid, 0) == 0 || errno == EPERM; }

std::vector<pid_t> EventProcessPids(std::string_view events) {
  constexpr std::string_view event_marker = "\"event\":\"process_started\"";
  constexpr std::string_view pid_marker = "\\\"pid\\\":";
  std::vector<pid_t> pids;
  std::size_t offset = 0U;
  while ((offset = events.find(event_marker, offset)) !=
         std::string_view::npos) {
    const std::size_t line_end = events.find('\n', offset);
    const std::size_t pid_field = events.find(pid_marker, offset);
    if (pid_field == std::string_view::npos ||
        (line_end != std::string_view::npos && pid_field >= line_end)) {
      throw std::runtime_error("process_started event did not contain a pid");
    }
    const std::size_t begin = pid_field + pid_marker.size();
    const std::size_t end = events.find_first_not_of("0123456789", begin);
    const long parsed =
        std::stol(std::string(events.substr(begin, end - begin)));
    if (parsed <= 0) {
      throw std::runtime_error("process_started pid was not positive");
    }
    pids.push_back(static_cast<pid_t>(parsed));
    offset = line_end == std::string_view::npos ? events.size() : line_end + 1U;
  }
  return pids;
}

std::vector<pid_t> MetricNamespacePids(std::string_view metrics) {
  constexpr std::string_view marker = "\"network_namespace_helper_pid\":";
  std::set<pid_t> unique_pids;
  std::size_t offset = 0U;
  while ((offset = metrics.find(marker, offset)) != std::string_view::npos) {
    const std::size_t begin = offset + marker.size();
    if (metrics.substr(begin, 4U) != "null") {
      const std::size_t end = metrics.find_first_not_of("0123456789", begin);
      const long parsed =
          std::stol(std::string(metrics.substr(begin, end - begin)));
      if (parsed > 0) {
        unique_pids.insert(static_cast<pid_t>(parsed));
      }
    }
    offset = begin;
  }
  return {unique_pids.begin(), unique_pids.end()};
}

void WaitForProcessExit(pid_t pid, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!ProcessExists(pid)) {
      return;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("active-run daemon survived confirmed TUI exit");
}

std::string MarkerResourceId(const std::filesystem::path& run_root) {
  const std::string marker = ReadFile(run_root / ".bbp-run");
  constexpr std::string_view field = "\"resource_id\":\"";
  const std::size_t begin_field = marker.find(field);
  if (begin_field == std::string::npos) {
    throw std::runtime_error("run ownership marker has no resource id");
  }
  const std::size_t begin = begin_field + field.size();
  const std::size_t end = marker.find('"', begin);
  if (end == std::string::npos || end - begin != 32U) {
    throw std::runtime_error("run ownership marker resource id is invalid");
  }
  return marker.substr(begin, end - begin);
}

void RequireNoRpcCredentials(const std::filesystem::path& run_root) {
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(run_root)) {
    if (entry.path().filename() == ".bbp-rpc-cookie") {
      throw std::runtime_error("RPC credential survived run cleanup: " +
                               entry.path().string());
    }
  }
}

void RequireOwnedResourcesRemoved(const std::filesystem::path& run_root,
                                  std::uint32_t node_count,
                                  const std::vector<pid_t>& daemon_pids,
                                  const std::vector<pid_t>& namespace_pids) {
  for (const pid_t pid : daemon_pids) {
    WaitForProcessExit(pid, 5s);
  }
  for (const pid_t pid : namespace_pids) {
    WaitForProcessExit(pid, 5s);
  }
  const std::string resource_id = MarkerResourceId(run_root);
  const std::filesystem::path cgroup =
      std::filesystem::path("/sys/fs/cgroup/bbp") / resource_id;
  if (std::filesystem::exists(cgroup)) {
    throw std::runtime_error("owned run cgroup survived cleanup: " +
                             cgroup.string());
  }
  const std::string interface_token = resource_id.substr(0U, 8U);
  for (std::uint32_t node = 1U; node <= node_count; ++node) {
    for (const char suffix : {'h', 'p'}) {
      const std::filesystem::path interface =
          std::filesystem::path("/sys/class/net") /
          ("bbp" + interface_token + "n" + std::to_string(node) + suffix);
      if (std::filesystem::exists(interface)) {
        throw std::runtime_error("owned run interface survived cleanup: " +
                                 interface.string());
      }
    }
  }
  RequireNoRpcCredentials(run_root);
}

void RequirePrivateMcpPath(const std::filesystem::path& path, mode_t type,
                           mode_t permissions) {
  struct stat status{};
  if (lstat(path.c_str(), &status) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "inspect MCP publication");
  }
  if ((status.st_mode & S_IFMT) != type ||
      (status.st_mode & 0777) != permissions || status.st_uid != geteuid()) {
    throw std::runtime_error("MCP publication type, mode, or owner mismatch: " +
                             path.string());
  }
}

void CheckEmptyControlPlane(const std::filesystem::path& command) {
  OwnedTemporaryDirectory directory("empty-control-plane");
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::filesystem::path home_directory = directory.root() / "home";
  std::filesystem::create_directory(home_directory);
  const std::string run_id =
      "empty-control-plane-" + std::to_string(static_cast<long long>(getpid()));
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  const std::filesystem::path mcp_path = home_directory / ".bbp" / "mcp";
  const std::filesystem::path token_path = mcp_path / "token";
  const std::filesystem::path client_path = mcp_path / "client.json";
  PtyProcess process(
      command,
      {"--benchmark-root", benchmark_root.string(), "--run-id", run_id,
       "--refresh-ms", "50", "--metrics-interval", "50ms"},
      30, 120, home_directory);
  std::string initial = process.ReadUntil("Blockchain Benchmark Project TUI",
                                          5s, "empty control plane");
  if (initial.find("No active run.") == std::string::npos) {
    initial +=
        process.ReadUntil("No active run.", 3s, "empty control-plane state");
  }
  if (initial.find("MCP [i].") == std::string::npos) {
    initial += process.ReadUntil("MCP [i].", 3s,
                                 "empty control-plane connection shortcut");
  }
  const std::string client =
      WaitForFileText(client_path, "\"codex_config_toml\"", 5s);
  const std::string token = ReadFile(token_path);
  if (token.size() != 65U || token.back() != '\n' ||
      !std::all_of(token.begin(), token.end() - 1, [](unsigned char character) {
        return std::isxdigit(character) != 0;
      })) {
    throw std::runtime_error("empty control-plane MCP token is invalid");
  }
  RequirePrivateMcpPath(mcp_path, S_IFDIR, 0700);
  RequirePrivateMcpPath(token_path, S_IFREG, 0600);
  RequirePrivateMcpPath(client_path, S_IFREG, 0600);
  RequireContains(client,
                  "http://127.0.0.1:", "empty control-plane MCP endpoint");
  RequireContains(client, "Bearer ", "empty control-plane MCP authentication");
  RequireContains(client, "[mcp_servers.bbp]",
                  "empty control-plane Codex configuration");
  RequireContains(client, "\"opencode_config\"",
                  "empty control-plane OpenCode configuration");
  const std::string endpoint =
      McpEndpointFromClientConfig(client, "empty control-plane MCP connection");
  const std::string token_value =
      McpTokenValue(token, "empty control-plane MCP connection");

  if (std::filesystem::exists(run_root) ||
      std::filesystem::exists(events_path)) {
    throw std::runtime_error(
        "empty control plane created a synthetic benchmark run");
  }
  if (!process.Running()) {
    throw std::runtime_error("empty control plane exited without a request");
  }

  static_cast<void>(OpenMcpConnectionPane(
      process, "i", endpoint, token_path, client_path, token_value,
      "empty control-plane MCP connection pane"));
  const McpTestSession session = ConnectMcpTestSession(mcp_path);
  if (session.session_id.empty()) {
    throw std::runtime_error(
        "MCP session did not initialize while the connection pane was open");
  }
  process.Resize(32, 120);
  static_cast<void>(process.ReadUntil(
      "MCP connection", 3s,
      "empty control-plane MCP pane after initialized handshake"));
  process.Write("\n");
  static_cast<void>(process.ReadFor(250ms));

  static_cast<void>(OpenMcpConnectionPane(
      process, "I", endpoint, token_path, client_path, token_value,
      "uppercase empty control-plane MCP connection pane"));
  process.Write("\x1b");
  static_cast<void>(process.ReadFor(250ms));
  if (!process.Running()) {
    throw std::runtime_error("Esc dismissal stopped the empty control plane");
  }

  static_cast<void>(OpenMcpConnectionPane(
      process, "i", endpoint, token_path, client_path, token_value,
      "repeat empty control-plane MCP connection pane"));
  process.Write("I");
  static_cast<void>(process.ReadFor(250ms));

  process.Write("c");
  static_cast<void>(process.ReadUntil("add-nodes <chain> <count> [binary]", 3s,
                                      "empty control-plane command palette"));
  process.Write("\x1b");
  static_cast<void>(process.ReadFor(100ms));
  if (!process.Running() || !std::filesystem::exists(client_path)) {
    throw std::runtime_error(
        "closing the zero-node command palette stopped the control plane");
  }

  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "empty control-plane exit modal"));
  process.Write("n");
  static_cast<void>(process.ReadFor(250ms));
  if (!process.Running() || !std::filesystem::exists(client_path)) {
    throw std::runtime_error(
        "Esc,n stopped the empty control plane or its MCP endpoint");
  }
  if (std::filesystem::exists(run_root)) {
    throw std::runtime_error(
        "empty control-plane cancel path created a benchmark run");
  }

  process.Write("\x1b");
  static_cast<void>(process.ReadUntil(
      "Confirm exit", 3s, "empty control-plane confirmed exit modal"));
  process.Write("y");
  RequireExitZero(&process, "empty editor TUI exit");
  if (std::filesystem::exists(token_path) ||
      std::filesystem::exists(client_path)) {
    throw std::runtime_error(
        "MCP credentials survived empty control-plane cleanup");
  }
  RequirePrivateMcpPath(mcp_path, S_IFDIR, 0700);
  if (std::filesystem::exists(run_root)) {
    throw std::runtime_error("empty editor TUI left a synthetic benchmark run");
  }
}

void CheckZeroToOnePublication(const std::filesystem::path& command,
                               const std::filesystem::path& helper_binary) {
  OwnedTemporaryDirectory directory("zero-to-one");
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::filesystem::path home_directory = directory.root() / "home";
  std::filesystem::create_directory(home_directory);
  const std::filesystem::path ready_daemon = directory.root() / "ready-firod";
  std::filesystem::copy_file(helper_binary, ready_daemon);
  std::filesystem::permissions(ready_daemon,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);
  const std::string active_run_id =
      "active-zero-" + std::to_string(static_cast<long long>(getpid()));
  const std::filesystem::path active_run_root = benchmark_root / active_run_id;
  const std::filesystem::path active_events = active_run_root / "events.jsonl";
  PtyProcess active_process(
      command,
      {"--benchmark-root", benchmark_root.string(), "--run-id", active_run_id,
       "--nodes", "0", "--node-capacity", "2", "--node-binary",
       ready_daemon.string(), "--refresh-ms", "50", "--metrics-interval",
       "50ms"},
      30, 120, home_directory);
  static_cast<void>(active_process.ReadUntil("Blockchain Benchmark Project TUI",
                                             5s,
                                             "active zero-node control plane"));
  static_cast<void>(
      WaitForFileText(active_events, "\"event\":\"run_started\"", 5s));

  active_process.Write("c");
  static_cast<void>(
      active_process.ReadUntil("add-nodes <chain> <count> [binary]", 3s,
                               "active zero-node command palette"));
  active_process.Write("add-nodes firo 1\n");
  static_cast<void>(active_process.ReadUntil("generation 2, 1 total nodes", 10s,
                                             "zero-to-one publication status"));
  const std::string published_events = WaitForFileText(
      active_events, "\"event\":\"runtime_generation_published\"", 3s);
  RequireContains(published_events, "\\\"generation\\\":2",
                  "zero-to-one published generation");
  RequireContains(published_events, "\\\"node_count\\\":1",
                  "zero-to-one published node count");
  RequireContains(published_events, "\\\"node_ids\\\":[\\\"firo-1\\\"]",
                  "zero-to-one published node IDs");
  RequireContains(published_events, "\"event\":\"operator_command_completed\"",
                  "zero-to-one command completion");
  RequireContains(published_events, "\\\"final_node_count\\\":1",
                  "zero-to-one command outcome");
  const pid_t added_daemon_pid = ProcessStartedPid(published_events);
  if (!active_process.Running() || !ProcessExists(added_daemon_pid)) {
    throw std::runtime_error(
        "zero-to-one publication did not retain its live node");
  }

  active_process.Write("\x1b");
  static_cast<void>(active_process.ReadUntil(
      "Confirm exit", 3s, "active zero-node confirmed exit modal"));
  active_process.Write("y");
  RequireExitZero(&active_process, "active zero-node TUI exit");
  WaitForProcessExit(added_daemon_pid, 3s);
}

void CheckNodeReplacementPublication(
    const std::filesystem::path& command,
    const std::filesystem::path& helper_binary) {
  OwnedTemporaryDirectory directory("node-replace");
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::filesystem::path home_directory = directory.root() / "home";
  std::filesystem::create_directory(home_directory);
  const std::filesystem::path ready_daemon = directory.root() / "ready-firod";
  const std::filesystem::path firo_qt = directory.root() / "firo-qt";
  std::filesystem::copy_file(helper_binary, ready_daemon);
  std::filesystem::copy_file(helper_binary, firo_qt);
  std::filesystem::permissions(ready_daemon,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);
  std::filesystem::permissions(firo_qt,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);
  const std::string run_id =
      "replace-" + std::to_string(static_cast<long long>(getpid()));
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  const std::filesystem::path node_root = run_root / "nodes" / "firo-1";
  const std::filesystem::path sentinel = node_root / "data" / "sentinel";
  const std::filesystem::path clone_failure =
      node_root / "data" / "clone-failure.fifo";
  const std::filesystem::path manifest_path =
      run_root / "runtime-node-resources.json";
  PtyProcess process(
      command,
      {"--benchmark-root", benchmark_root.string(), "--run-id", run_id,
       "--nodes", "1", "--node-capacity", "1", "--node-binary",
       ready_daemon.string(), "--no-isolate-network", "--no-mining",
       "--refresh-ms", "50", "--metrics-interval", "50ms"},
      30, 120, home_directory);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 5s,
                                      "node replacement TUI"));
  static_cast<void>(
      WaitForFileText(events_path, "\"event\":\"run_started\"", 5s));
  static_cast<void>(
      WaitForFileText(events_path, "\"event\":\"process_started\"", 5s));
  {
    std::ofstream stream(sentinel);
    if (!stream) {
      throw std::runtime_error("could not create node-replacement sentinel");
    }
    stream << "preserve-root-and-data\n";
  }
  struct stat root_before{};
  struct stat sentinel_before{};
  if (stat(node_root.c_str(), &root_before) != 0 ||
      stat(sentinel.c_str(), &sentinel_before) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "inspect node replacement identities");
  }
  const std::string manifest_before =
      WaitForFileText(manifest_path, "\"state\":\"live\"", 5s);
  if (mkfifo(clone_failure.c_str(), 0600) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "create node-replacement clone failure");
  }

  process.Write("c");
  static_cast<void>(process.ReadUntil("add-nodes <chain> <count> [binary]", 3s,
                                      "node replacement command palette"));
  static_cast<void>(process.ReadFor(100ms));
  process.Write("replace-node firo\n");
  static_cast<void>(process.ReadUntil("Confirm destructive action", 3s,
                                      "node replacement confirmation"));
  process.Write("y");
  const std::string failed_events = WaitForFileText(
      events_path, "\"event\":\"operator_command_failed\"", 10s);
  RequireNotContains(failed_events, "\"event\":\"run_cancelled\"",
                     "recoverable node replacement clone failure");
  if (ReadFile(manifest_path) != manifest_before) {
    throw std::runtime_error(
        "node replacement clone failure changed the live resource manifest");
  }
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(run_root / "nodes")) {
    if (entry.path().filename().string().starts_with("bbpr-")) {
      throw std::runtime_error(
          "node replacement clone failure retained a staging root");
    }
  }
  struct stat root_after_failure{};
  struct stat sentinel_after_failure{};
  if (stat(node_root.c_str(), &root_after_failure) != 0 ||
      stat(sentinel.c_str(), &sentinel_after_failure) != 0 ||
      root_before.st_dev != root_after_failure.st_dev ||
      root_before.st_ino != root_after_failure.st_ino ||
      sentinel_before.st_dev != sentinel_after_failure.st_dev ||
      sentinel_before.st_ino != sentinel_after_failure.st_ino) {
    throw std::runtime_error(
        "node replacement clone failure changed live filesystem identity");
  }
  static_cast<void>(
      process.ReadUntil("Command error", 5s, "node replacement clone failure"));
  process.Write("\n");
  if (!std::filesystem::remove(clone_failure)) {
    throw std::runtime_error(
        "could not remove node-replacement clone failure fixture");
  }

  const McpTestSession mcp =
      ConnectMcpTestSession(home_directory / ".bbp" / "mcp");
  const std::filesystem::path stall_control =
      run_root / "bbp-test-stall-final-replacement";
  const std::filesystem::path stall_readiness =
      node_root / "data" / "bbp-test-stall-readiness";
  {
    std::ofstream stream(stall_control);
    if (!stream) {
      throw std::runtime_error(
          "could not create final replacement stall control");
    }
    stream << "stall final replacement only\n";
  }
  const boost::json::object submitted = McpToolCall(
      mcp.port, mcp.token, mcp.session_id, mcp.protocol_version, 2U,
      "node.replace",
      boost::json::object{
          {"run_id", run_id},
          {"node_id", "firo-1"},
          {"replacement",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"firo-1"}},
                               {"ready_timeout_sec", 10U},
                               {"sync_timeout_sec", 10U}}}});
  const std::string operation_id(submitted.at("operation_id").as_string());
  static_cast<void>(
      WaitForFileText(run_root / ".runtime-node-replace-events.pending",
                      "\\\"reason\\\":\\\"runtime_node_replace\\\"", 15s));
  if (!std::filesystem::remove(stall_readiness) ||
      !std::filesystem::remove(stall_control)) {
    throw std::runtime_error(
        "could not release final replacement readiness fixture");
  }
  const boost::json::object cancellation = McpToolCall(
      mcp.port, mcp.token, mcp.session_id, mcp.protocol_version, 3U,
      "operation.cancel", boost::json::object{{"operation_id", operation_id}});
  if (!cancellation.at("cancel_requested").as_bool()) {
    throw std::runtime_error(
        "MCP did not accept final replacement cancellation");
  }
  boost::json::object cancelled;
  const auto cancellation_deadline = std::chrono::steady_clock::now() + 15s;
  std::uint64_t request_id = 4U;
  while (std::chrono::steady_clock::now() < cancellation_deadline) {
    cancelled = McpToolCall(
        mcp.port, mcp.token, mcp.session_id, mcp.protocol_version, request_id++,
        "operation.get", boost::json::object{{"operation_id", operation_id}});
    if (cancelled.at("state").as_string() == "cancelled") {
      break;
    }
    if (cancelled.at("state").as_string() == "failed" ||
        cancelled.at("state").as_string() == "succeeded") {
      throw std::runtime_error(
          "final replacement cancellation produced terminal state " +
          std::string(cancelled.at("state").as_string()));
    }
    std::this_thread::sleep_for(20ms);
  }
  if (cancelled.empty() || cancelled.at("state").as_string() != "cancelled") {
    throw std::runtime_error(
        "final replacement cancellation did not become terminal");
  }
  const std::string after_cancellation = ReadFile(events_path);
  RequireNotContains(after_cancellation, "\"event\":\"run_cancelled\"",
                     "cancelled node replacement");
  if (ReadFile(manifest_path) != manifest_before ||
      std::filesystem::exists(run_root /
                              ".runtime-node-replace-events.pending")) {
    throw std::runtime_error(
        "cancelled node replacement did not restore its manifest/evidence");
  }
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(run_root / "nodes")) {
    if (entry.path().filename().string().starts_with("bbpr-")) {
      throw std::runtime_error(
          "cancelled node replacement retained a staging root");
    }
  }
  struct stat root_after_cancellation{};
  struct stat sentinel_after_cancellation{};
  if (stat(node_root.c_str(), &root_after_cancellation) != 0 ||
      stat(sentinel.c_str(), &sentinel_after_cancellation) != 0 ||
      root_before.st_dev != root_after_cancellation.st_dev ||
      root_before.st_ino != root_after_cancellation.st_ino ||
      sentinel_before.st_dev != sentinel_after_cancellation.st_dev ||
      sentinel_before.st_ino != sentinel_after_cancellation.st_ino ||
      ReadFile(sentinel) != "preserve-root-and-data\n") {
    throw std::runtime_error(
        "cancelled node replacement changed preserved filesystem identity");
  }

  process.Write("c");
  static_cast<void>(process.ReadUntil("add-nodes <chain> <count> [binary]", 3s,
                                      "node replacement retry palette"));
  static_cast<void>(process.ReadFor(100ms));
  process.Write("replace-node firo\n");
  static_cast<void>(process.ReadUntil("Confirm destructive action", 3s,
                                      "node replacement retry confirmation"));
  process.Write("y");
  const std::string events = WaitForFileText(
      events_path, "\"event\":\"operator_command_completed\"", 30s);
  static_cast<void>(process.ReadUntil("Command #3 completed for firo-1.", 5s,
                                      "node replacement completion"));
  RequireContains(events, "\"event\":\"runtime_generation_published\"",
                  "node replacement publication");
  RequireContains(events, "\\\"generation\\\":2",
                  "node replacement generation");
  RequireContains(events, "\\\"node_count\\\":1", "node replacement count");
  RequireContains(events, "\\\"replaced_node_id\\\":\\\"firo-1\\\"",
                  "node replacement stable identity");
  RequireContains(events, "\\\"topology_restore_request_sequence\\\":1",
                  "node replacement topology restoration request");
  RequireNotContains(events, "node_replace_outcome_unconfirmed",
                     "node replacement outcome");
  RequireContains(events,
                  "\\\"reason\\\":\\\"runtime_node_replace_candidate\\\"",
                  "node replacement candidate start");
  RequireContains(events, "\\\"reason\\\":\\\"runtime_node_replace\\\"",
                  "node replacement final start");

  struct stat root_after{};
  struct stat sentinel_after{};
  if (stat(node_root.c_str(), &root_after) != 0 ||
      stat(sentinel.c_str(), &sentinel_after) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "reinspect node replacement identities");
  }
  if (root_before.st_dev != root_after.st_dev ||
      root_before.st_ino != root_after.st_ino ||
      sentinel_before.st_dev != sentinel_after.st_dev ||
      sentinel_before.st_ino != sentinel_after.st_ino ||
      ReadFile(sentinel) != "preserve-root-and-data\n") {
    throw std::runtime_error(
        "compatible node replacement changed its live root or sentinel "
        "identity");
  }
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(run_root / "nodes")) {
    if (entry.path().filename().string().starts_with("bbpr-")) {
      throw std::runtime_error(
          "successful node replacement retained a staging root");
    }
  }

  process.Write("\x1b");
  static_cast<void>(process.ReadUntil("Confirm exit", 3s,
                                      "node replacement confirmed exit modal"));
  process.Write("y");
  RequireExitZero(&process, "node replacement TUI exit");
}

void CheckRetainedMcpLifecycle(const std::filesystem::path& command,
                               const std::filesystem::path& run_root) {
  OwnedTemporaryDirectory directory("retained-mcp");
  const std::filesystem::path home_directory = directory.root() / "home";
  std::filesystem::create_directory(home_directory);
  const std::filesystem::path mcp_path = home_directory / ".bbp" / "mcp";
  const std::filesystem::path token_path = mcp_path / "token";
  const std::filesystem::path client_path = mcp_path / "client.json";

  PtyProcess process(command,
                     {"--run", run_root.string(), "--refresh-ms", "50"}, 30,
                     120, home_directory);
  std::string initial = process.ReadUntil("Blockchain Benchmark Project TUI",
                                          5s, "retained MCP TUI");
  if (initial.find("MCP [i].") == std::string::npos) {
    initial +=
        process.ReadUntil("MCP [i].", 3s, "retained MCP connection shortcut");
  }
  const std::string client =
      WaitForFileText(client_path, "\"codex_config_toml\"", 5s);
  const std::string token = ReadFile(token_path);
  RequireContains(client, "\"run_id\":\"tui-fixture\"",
                  "retained MCP run identity");
  RequirePrivateMcpPath(mcp_path, S_IFDIR, 0700);
  RequirePrivateMcpPath(token_path, S_IFREG, 0600);
  RequirePrivateMcpPath(client_path, S_IFREG, 0600);
  if (!process.Running()) {
    throw std::runtime_error("retained TUI exited while MCP was published");
  }

  const std::string endpoint =
      McpEndpointFromClientConfig(client, "retained MCP connection");
  const std::string token_value =
      McpTokenValue(token, "retained MCP connection");
  static_cast<void>(OpenMcpConnectionPane(process, "i", endpoint, token_path,
                                          client_path, token_value,
                                          "retained MCP connection pane"));
  process.Write("\x1b");
  static_cast<void>(process.ReadFor(250ms));
  if (!process.Running()) {
    throw std::runtime_error("retained MCP pane dismissal exited the TUI");
  }

  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "retained MCP exit modal"));
  process.Write("y");
  RequireExitZero(&process, "retained MCP TUI exit");
  if (std::filesystem::exists(token_path) ||
      std::filesystem::exists(client_path)) {
    throw std::runtime_error("MCP credentials survived retained TUI cleanup");
  }
  RequirePrivateMcpPath(mcp_path, S_IFDIR, 0700);
}

void CheckFiniteDirectLoadOption(const std::filesystem::path& command,
                                 const std::filesystem::path& daemon,
                                 const std::filesystem::path& benchmark_root) {
  const std::string run_id = "direct-load-finite-" + std::to_string(getpid());
  const std::filesystem::path run_root = benchmark_root / run_id;
  PtyProcess process(command,
                     {"--firod",
                      daemon.string(),
                      "--benchmark-root",
                      benchmark_root.string(),
                      "--run-id",
                      run_id,
                      "--nodes",
                      "2",
                      "--wallet-node-count",
                      "2",
                      "--transaction-load-strategy",
                      "random_bruteforce",
                      "--metrics-sample-count",
                      "1",
                      "--no-tui",
                      "--metrics-interval",
                      "100ms",
                      "--block-production-period-ms",
                      "100",
                      "--block-production-probability",
                      "1",
                      "--keep-artifacts"},
                     24, 80);
  const int result = process.Wait(60s);
  if (result != 0) {
    throw std::runtime_error("explicit finite direct load exited " +
                             std::to_string(result));
  }
  const std::string resolved = ReadFile(run_root / "resolved-scenario.json");
  RequireContains(resolved, "\"metrics_sample_count\":1",
                  "explicit finite direct load options");
  const std::string events = ReadFile(run_root / "events.jsonl");
  RequireContains(events, "\"event\":\"run_finished\"",
                  "explicit finite direct load");
  RequireNotContains(events, "\"event\":\"run_cancelled\"",
                     "explicit finite direct load");
  const std::vector<pid_t> daemon_pids = EventProcessPids(events);
  if (daemon_pids.size() != 2U) {
    throw std::runtime_error("finite direct load did not start two daemons");
  }
  RequireOwnedResourcesRemoved(
      run_root, 2U, daemon_pids,
      MetricNamespacePids(ReadFile(run_root / "metrics.jsonl")));
}

void CheckIndefiniteDirectLoadLifecycle(
    const std::filesystem::path& command, const std::filesystem::path& daemon,
    const std::filesystem::path& benchmark_root) {
  const std::string run_id =
      "direct-load-indefinite-" + std::to_string(getpid());
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  PtyProcess process(command,
                     {"--firod",
                      daemon.string(),
                      "--benchmark-root",
                      benchmark_root.string(),
                      "--run-id",
                      run_id,
                      "--nodes",
                      "2",
                      "--wallet-node-count",
                      "2",
                      "--transaction-load-strategy",
                      "random_bruteforce",
                      "--metrics-interval",
                      "100ms",
                      "--refresh-ms",
                      "50",
                      "--block-production-period-ms",
                      "100",
                      "--block-production-probability",
                      "1",
                      "--keep-artifacts"},
                     30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 10s,
                                      "indefinite direct load"));
  const std::string resolved = WaitForFileText(
      run_root / "resolved-scenario.json", "\"metrics_sample_count\":0", 10s);
  RequireContains(resolved, "\"transaction_count\":null",
                  "indefinite direct load options");
  std::string after_workload =
      WaitForFileText(events_path, "\\\"transaction_index\\\":2", 30s);
  const std::size_t metrics_before =
      CountOccurrences(after_workload, "\"event\":\"metrics_sample\"");
  const std::size_t blocks_before = CountOccurrences(
      after_workload, "\"event\":\"scheduled_block_produced\"");
  after_workload = WaitForFileOccurrences(
      events_path, "\"event\":\"metrics_sample\"", metrics_before + 2U, 20s);
  after_workload = WaitForFileOccurrences(
      events_path, "\"event\":\"scheduled_block_produced\"", blocks_before + 2U,
      20s);
  RequireNotContains(after_workload, "\"event\":\"run_cancelled\"",
                     "indefinite direct load after workload");
  RequireNotContains(after_workload, "\"event\":\"run_finished\"",
                     "indefinite direct load after workload");
  const std::vector<pid_t> daemon_pids = EventProcessPids(after_workload);
  if (!process.Running() || daemon_pids.size() != 2U) {
    throw std::runtime_error(
        "default direct load was not running after workload completion");
  }
  for (const pid_t pid : daemon_pids) {
    if (!ProcessExists(pid)) {
      throw std::runtime_error(
          "direct-load daemon stopped after workload completion");
    }
  }

  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "direct-load exit modal"));
  process.Write("y");
  const int result = process.Wait(30s);
  if (result != 0) {
    throw std::runtime_error("explicit direct-load exit returned " +
                             std::to_string(result));
  }
  const std::string finished =
      WaitForFileText(events_path, "\"event\":\"run_finished\"", 5s);
  RequireContains(finished, "\"event\":\"run_cancelled\"",
                  "explicit direct-load exit");
  RequireContains(finished, "\"event\":\"wallet_workload_state\"",
                  "explicit direct-load exit");
  RequireContains(finished, "\\\"state\\\":\\\"cancelled\\\"",
                  "explicit direct-load exit");
  RequireContains(finished, "\\\"outstanding\\\":0",
                  "explicit direct-load exit");
  RequireOwnedResourcesRemoved(
      run_root, 2U, daemon_pids,
      MetricNamespacePids(ReadFile(run_root / "metrics.jsonl")));
}

void CheckDirectLoadLifecycle(const std::filesystem::path& command,
                              const std::filesystem::path& daemon,
                              const std::filesystem::path& benchmark_root) {
  std::filesystem::create_directories(benchmark_root);
  CheckFiniteDirectLoadOption(command, daemon, benchmark_root);
  CheckIndefiniteDirectLoadLifecycle(command, daemon, benchmark_root);
}

void WriteActiveScenario(const std::filesystem::path& path) {
  std::ofstream stream(path);
  if (!stream) {
    throw std::runtime_error("could not create active-run scenario");
  }
  stream << R"({
  "simulation": {
    "duration": "120s",
    "metrics_interval": "100ms"
  },
  "isolated_network": false,
  "nodes": [
    {
      "id": "firo-active",
      "chain": "firo",
      "role": "base"
    }
  ],
  "block_production": {
    "enabled": false
  },
  "ready_timeout_sec": 120
})";
  if (!stream) {
    throw std::runtime_error("could not write active-run scenario");
  }
}

std::string ActiveOperatorConnectionCommand(
    const std::filesystem::path& run_root,
    const std::filesystem::path& qt_binary) {
  const std::filesystem::path data_dir = run_root / "operator" / "firo-qt";
  return "'" + std::filesystem::canonical(qt_binary).string() +
         "' '-regtest' '-datadir=" + data_dir.string() +
         "' '-connect=127.0.0.1:18168' '-dns=0' '-dnsseed=0' "
         "'-forcednsseed=0' '-maxconnections=1' '-listen=0' '-discover=0' "
         "'-listenonion=0' '-torsetup=0' '-upnp=0'";
}

void CheckSimulatorLogWrapping(const std::filesystem::path& command,
                               const std::filesystem::path& source_run) {
  OwnedRunCopy run(source_run, "simulator-log-wrap");
  OwnedTemporaryDirectory home("simulator-log-wrap-home");
  std::filesystem::create_directory(home.root() / "home");
  const std::string generated_command =
      "[2026-07-25 11:20:00.000000] [info] manual Firo GUI command: "
      "'/home/navidr/work/firo/build/src/qt/firo-qt' '-regtest' "
      "'-datadir=" +
      (run.run_root() / "operator" / "firo-qt").string() +
      "' '-connect=127.0.0.1:18168' '-dns=0' '-dnsseed=0' "
      "'-forcednsseed=0' '-maxconnections=1' '-listen=0' '-discover=0' "
      "'-listenonion=0' '-torsetup=0' '-upnp=0' LOG_WRAP_COMMAND_END";
  run.WriteSimulatorLog({"[2026-07-25 11:19:59.000000] [info] simulator ready",
                         generated_command});

  PtyProcess process(command,
                     {"--run", run.run_root().string(), "--refresh-ms", "50"},
                     24, 80, home.root() / "home");
  std::string rendered =
      process.ReadUntil("LOG_WRAP_COMMAND_END", 5s, "80x24 simulator-log tail");
  RequireContains(rendered, "Simulator Logs [",
                  "80x24 simulator-log pane title");

  process.Write(std::string(96U, '['));
  rendered += process.ReadUntil("manual Firo GUI command:", 5s,
                                "80x24 simulator-log upward scroll");
  for (const std::string_view expected :
       {"manual Firo GUI command:", "firo/build/src/qt/firo-qt", "'-regtest'",
        "'-datadir=", "operator/firo-qt'", "'-connect=127.0.0.1:18168'",
        "'-dnsseed=0'", "'-forcednsseed=0'", "'-maxconnections=1'",
        "'-listen=0'", "'-discover=0'", "'-listenonion=0'", "'-torsetup=0'",
        "'-upnp=0'", "LOG_WRAP_COMMAND_END"}) {
    RequireContains(rendered, expected, "80x24 scrolled Firo-Qt command");
  }

  process.Resize(30, 120);
  std::string resized = process.ReadUntil(
      "manual Firo GUI command:", 5s, "120x30 resized simulator-log anchor");
  resized += process.ReadFor(200ms);
  RequireContains(resized, "Blockchain Benchmark Project TUI",
                  "120x30 resized title");
  RequireContains(resized, "Simulator Logs [",
                  "120x30 resized simulator-log pane title");
  RequireContains(resized, "Logs [/] row, PgUp/PgDn page, Home/End.",
                  "120x30 resized simulator-log footer");

  process.Write(std::string(96U, ']'));
  resized += process.ReadFor(2s);
  RequireContains(resized, "LOG_WRAP_COMMAND_END",
                  "120x30 simulator-log downward scroll");
  RequireContains(resized, "'-maxconnections=1'",
                  "120x30 reflowed Firo-Qt command");
  RequireContains(resized, "'-upnp=0'", "120x30 reflowed Firo-Qt command");
  process.Write("q");
  RequireExitZero(&process, "simulator-log wrapping TUI");
}

std::filesystem::path CopyActiveDaemonFixtures(
    const std::filesystem::path& helper_binary,
    const std::filesystem::path& directory) {
  const std::filesystem::path bin = directory / "bin";
  std::filesystem::create_directory(bin);
  const std::filesystem::path daemon = bin / "ready-firod";
  const std::filesystem::path qt = bin / "firo-qt";
  std::filesystem::copy_file(helper_binary, daemon);
  std::filesystem::copy_file(helper_binary, qt);
  constexpr std::filesystem::perms kOwnerExecutable =
      std::filesystem::perms::owner_read | std::filesystem::perms::owner_write |
      std::filesystem::perms::owner_exec;
  std::filesystem::permissions(daemon, kOwnerExecutable,
                               std::filesystem::perm_options::replace);
  std::filesystem::permissions(qt, kOwnerExecutable,
                               std::filesystem::perm_options::replace);
  return daemon;
}

void CheckSlowNodeReplacementDefaultTimeout(
    const std::filesystem::path& command,
    const std::filesystem::path& helper_binary) {
  OwnedTemporaryDirectory directory("slow-node-replace");
  const std::filesystem::path daemon =
      CopyActiveDaemonFixtures(helper_binary, directory.root());
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::filesystem::path home_directory = directory.root() / "home";
  std::filesystem::create_directory(home_directory);
  const std::string run_id =
      "slow-replace-" + std::to_string(static_cast<long long>(getpid()));
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  const std::filesystem::path node_root = run_root / "nodes" / "firo-1";
  const std::filesystem::path sentinel = node_root / "data" / "sentinel";
  const std::filesystem::path timing_audit =
      run_root / "bbp-test-slow-node-replace-audit";

  PtyProcess process(
      command,
      {"--benchmark-root", benchmark_root.string(), "--run-id", run_id,
       "--nodes", "1", "--node-capacity", "1", "--node-binary", daemon.string(),
       "--no-isolate-network", "--no-mining", "--refresh-ms", "50",
       "--metrics-interval", "50ms"},
      30, 120, home_directory);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 5s,
                                      "slow node replacement TUI"));
  static_cast<void>(
      WaitForFileText(events_path, "\"event\":\"rpc_ready\"", 30s));
  {
    std::ofstream stream(sentinel);
    if (!stream) {
      throw std::runtime_error(
          "could not create slow node-replacement sentinel");
    }
    stream << "preserve-slow-replacement-root\n";
  }
  struct stat root_before{};
  struct stat sentinel_before{};
  if (stat(node_root.c_str(), &root_before) != 0 ||
      stat(sentinel.c_str(), &sentinel_before) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "inspect slow node replacement identities");
  }
  {
    std::ofstream stream(run_root / "bbp-test-slow-node-replace");
    if (!stream) {
      throw std::runtime_error(
          "could not enable slow node-replacement fixture");
    }
    stream << "delay candidate and final readiness and synchronization\n";
  }

  const McpTestSession mcp =
      ConnectMcpTestSession(home_directory / ".bbp" / "mcp");
  const auto replacement_started = std::chrono::steady_clock::now();
  const boost::json::object submitted = McpToolCall(
      mcp.port, mcp.token, mcp.session_id, mcp.protocol_version, 2U,
      "node.replace",
      boost::json::object{
          {"run_id", run_id},
          {"node_id", "firo-1"},
          {"replacement",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"firo-1"}},
                               {"ready_timeout_sec", 20U},
                               {"sync_timeout_sec", 20U}}}});
  const std::string operation_id(submitted.at("operation_id").as_string());
  boost::json::object terminal;
  std::uint64_t request_id = 3U;
  const auto terminal_wait_deadline = replacement_started + 135s;
  while (std::chrono::steady_clock::now() < terminal_wait_deadline) {
    terminal = McpToolCall(mcp.port, mcp.token, mcp.session_id,
                           mcp.protocol_version, request_id++, "operation.get",
                           boost::json::object{{"operation_id", operation_id}});
    const std::string_view state = terminal.at("state").as_string();
    if (state == "succeeded" || state == "failed" || state == "cancelled") {
      break;
    }
    std::this_thread::sleep_for(100ms);
  }
  const auto replacement_elapsed =
      std::chrono::steady_clock::now() - replacement_started;
  if (terminal.empty() || terminal.at("state").as_string() != "succeeded") {
    throw std::runtime_error("slow node replacement did not succeed: " +
                             (terminal.empty()
                                  ? std::string("no terminal result")
                                  : boost::json::serialize(terminal)));
  }
  constexpr auto kOldAggregateDefault = 70s;
  constexpr auto kNewAggregateDefault = 130s;
  if (replacement_elapsed <= kOldAggregateDefault) {
    throw std::runtime_error(
        "slow node replacement did not exceed the old 70-second default");
  }
  if (replacement_elapsed >= kNewAggregateDefault) {
    throw std::runtime_error(
        "slow node replacement exceeded the new 130-second default");
  }

  const boost::json::object& result =
      terminal.at("terminal_result").as_object();
  if (result.at("action").as_string() != "node.replace" ||
      result.at("state").as_string() != "running" ||
      result.at("inventory_generation").to_number<std::uint64_t>() != 2U ||
      result.at("final_node_count").to_number<std::uint64_t>() != 1U ||
      result.at("affected_node_ids").as_array().size() != 1U ||
      result.at("affected_node_ids").as_array().front().as_string() !=
          "firo-1") {
    throw std::runtime_error(
        "slow node replacement returned inconsistent inventory evidence");
  }

  const std::string timing = WaitForFileOccurrences(
      timing_audit, "synchronization-delay-completed", 2U, 5s);
  if (CountOccurrences(timing, "readiness-delay-started") != 2U ||
      CountOccurrences(timing, "readiness-delay-completed") != 2U ||
      CountOccurrences(timing, "synchronization-delay-started") != 2U ||
      CountOccurrences(timing, "synchronization-delay-completed") != 2U) {
    throw std::runtime_error(
        "slow node replacement did not exercise exactly four delayed phases");
  }
  RequireContains(timing, "bbpr-", "slow replacement candidate timing audit");
  RequireContains(timing, "firo-1", "slow replacement final timing audit");

  const std::string events = ReadFile(events_path);
  RequireContains(events, "\"event\":\"runtime_generation_published\"",
                  "slow node replacement publication");
  RequireNotContains(events, "node_replace_outcome_unconfirmed",
                     "slow node replacement outcome");
  RequireNotContains(events, "\"event\":\"run_cancelled\"",
                     "slow node replacement run state");
  RequireNotContains(events, "\"event\":\"run_failed\"",
                     "slow node replacement run state");

  struct stat root_after{};
  struct stat sentinel_after{};
  if (stat(node_root.c_str(), &root_after) != 0 ||
      stat(sentinel.c_str(), &sentinel_after) != 0 ||
      root_before.st_dev != root_after.st_dev ||
      root_before.st_ino != root_after.st_ino ||
      sentinel_before.st_dev != sentinel_after.st_dev ||
      sentinel_before.st_ino != sentinel_after.st_ino ||
      ReadFile(sentinel) != "preserve-slow-replacement-root\n") {
    throw std::runtime_error(
        "slow compatible replacement changed preserved filesystem identity");
  }
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(run_root / "nodes")) {
    if (entry.path().filename().string().starts_with("bbpr-")) {
      throw std::runtime_error(
          "slow successful replacement retained a staging root");
    }
  }

  process.Write("\x1b");
  static_cast<void>(process.ReadUntil(
      "Confirm exit", 3s, "slow node replacement confirmed exit modal"));
  process.Write("y");
  RequireExitZero(&process, "slow node replacement TUI exit");
  std::cout << "slow node replacement succeeded after "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   replacement_elapsed)
                   .count()
            << "ms across four delayed phases\n";
}

void CheckDirectedNodeReplacementPublication(
    const std::filesystem::path& command,
    const std::filesystem::path& helper_binary) {
  OwnedTemporaryDirectory directory("directed-node-replace");
  const std::filesystem::path daemon =
      CopyActiveDaemonFixtures(helper_binary, directory.root());
  const std::filesystem::path scenario = directory.root() / "scenario.json";
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::filesystem::path home_directory = directory.root() / "home";
  std::filesystem::create_directory(home_directory);
  const std::string run_id =
      "directed-replace-" + std::to_string(static_cast<long long>(getpid()));
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  {
    std::ofstream stream(scenario);
    if (!stream) {
      throw std::runtime_error(
          "could not create directed node-replacement scenario");
    }
    stream << boost::json::serialize(boost::json::object{
                  {"chain", "firo"},
                  {"chain_daemon", daemon.string()},
                  {"nodes", 3U},
                  {"node_capacity", 3U},
                  {"isolated_network", false},
                  {"topology",
                   boost::json::object{
                       {"node_count", 3U},
                       {"type", "custom_edge_list"},
                       {"edges",
                        boost::json::array{
                            boost::json::object{{"from", 2U},
                                                {"to", 1U},
                                                {"bidirectional", false},
                                                {"active", true}},
                            boost::json::object{{"from", 3U},
                                                {"to", 1U},
                                                {"bidirectional", false},
                                                {"active", true}}}}}},
                  {"block_production", boost::json::object{{"enabled", false}}},
                  {"ready_timeout_sec", 10U},
                  {"sync_timeout_sec", 10U},
                  {"metrics_interval_ms", 50U},
                  {"workloads", boost::json::array{}}})
           << '\n';
    if (!stream) {
      throw std::runtime_error(
          "could not write directed node-replacement scenario");
    }
  }

  PtyProcess process(
      command,
      {"--scenario", scenario.string(), "--node-binary", daemon.string(),
       "--benchmark-root", benchmark_root.string(), "--run-id", run_id,
       "--refresh-ms", "50"},
      30, 120, home_directory);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 5s,
                                      "directed node replacement TUI"));
  static_cast<void>(WaitForFileOccurrences(
      events_path, "\"event\":\"process_started\"", 3U, 10s));
  const std::string node_two_configured_peers = WaitForFileText(
      run_root / "nodes" / "firo-2" / "data" / "bbp-test-configured-peers",
      "127.0.0.1:18168", 5s);
  const std::string node_three_configured_peers = WaitForFileText(
      run_root / "nodes" / "firo-3" / "data" / "bbp-test-configured-peers",
      "127.0.0.1:18168", 5s);
  RequireContains(node_two_configured_peers, "127.0.0.1:18168",
                  "directed replacement source peer");
  RequireContains(node_three_configured_peers, "127.0.0.1:18168",
                  "directed replacement second source peer");
  if (std::filesystem::exists(run_root / "nodes" / "firo-1" / "data" /
                              "bbp-test-configured-peers")) {
    throw std::runtime_error(
        "directed replacement target received a reverse startup peer");
  }
  const std::filesystem::path source_rpc_audit =
      run_root / "nodes" / "firo-2" / "data" / "bbp-test-rpc-audit";
  const std::filesystem::path second_source_rpc_audit =
      run_root / "nodes" / "firo-3" / "data" / "bbp-test-rpc-audit";
  const std::filesystem::path target_rpc_audit =
      run_root / "nodes" / "firo-1" / "data" / "bbp-test-rpc-audit";
  const std::string source_rpc_audit_before =
      std::filesystem::exists(source_rpc_audit) ? ReadFile(source_rpc_audit)
                                                : std::string{};
  const std::string second_source_rpc_audit_before =
      std::filesystem::exists(second_source_rpc_audit)
          ? ReadFile(second_source_rpc_audit)
          : std::string{};
  const std::string target_rpc_audit_before =
      std::filesystem::exists(target_rpc_audit) ? ReadFile(target_rpc_audit)
                                                : std::string{};
  {
    std::ofstream control(run_root /
                          "bbp-test-drop-directed-peer-on-target-stop");
    if (!control) {
      throw std::runtime_error("could not create directed peer-drop control");
    }
    control << "drop the preserved incoming peer once\n";
  }

  static_cast<void>(process.ReadFor(200ms));
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Enter submits. Tab completes. Esc closes.", 3s,
                        "directed replacement palette"));
  static_cast<void>(process.ReadFor(200ms));
  process.Write("replace-node firo\n");
  static_cast<void>(process.ReadUntil("Confirm destructive action", 3s,
                                      "directed replacement confirmation"));
  process.Write("y");
  const std::string events = WaitForFileText(
      events_path, "\"event\":\"operator_command_completed\"", 30s);
  static_cast<void>(process.ReadUntil("Command #1 completed for firo-1.", 5s,
                                      "directed replacement completion"));
  RequireContains(
      events,
      "\\\"topology_current_edges\\\":[{\\\"from\\\":2,\\\"to\\\":1,"
      "\\\"band\\\":1,\\\"active\\\":true",
      "directed replacement published topology");
  RequireContains(
      events, "{\\\"from\\\":3,\\\"to\\\":1,\\\"band\\\":1,\\\"active\\\":true",
      "directed replacement second published topology edge");
  RequireNotContains(events, "\\\"from\\\":1,\\\"to\\\":2",
                     "directed replacement published topology");
  RequireNotContains(events, "\\\"from\\\":1,\\\"to\\\":3",
                     "directed replacement published topology");
  RequireContains(events, "\\\"topology_restore_request_sequence\\\":1",
                  "directed replacement topology restoration request");
  RequireNotContains(events, "\"event\":\"run_cancelled\"",
                     "directed replacement run state");
  constexpr std::string_view kSourceRestorationRpc =
      "addnode 127.0.0.1:18168 before-height";
  const std::string source_rpc_audit_after = WaitForFileOccurrences(
      source_rpc_audit, kSourceRestorationRpc,
      CountOccurrences(source_rpc_audit_before, kSourceRestorationRpc) + 1U,
      5s);
  if (!source_rpc_audit_after.starts_with(source_rpc_audit_before)) {
    throw std::runtime_error(
        "directed replacement rewrote its source RPC audit");
  }
  RequireContains(std::string_view(source_rpc_audit_after)
                      .substr(source_rpc_audit_before.size()),
                  kSourceRestorationRpc,
                  "directed replacement source-oriented restoration RPC");
  const std::string second_source_rpc_audit_after = WaitForFileOccurrences(
      second_source_rpc_audit, kSourceRestorationRpc,
      CountOccurrences(second_source_rpc_audit_before, kSourceRestorationRpc) +
          1U,
      5s);
  if (!second_source_rpc_audit_after.starts_with(
          second_source_rpc_audit_before)) {
    throw std::runtime_error(
        "directed replacement rewrote its second source RPC audit");
  }
  RequireContains(
      std::string_view(second_source_rpc_audit_after)
          .substr(second_source_rpc_audit_before.size()),
      kSourceRestorationRpc,
      "directed replacement second source-oriented restoration RPC");
  const std::string target_rpc_audit_after =
      std::filesystem::exists(target_rpc_audit) ? ReadFile(target_rpc_audit)
                                                : std::string{};
  if (!target_rpc_audit_after.starts_with(target_rpc_audit_before)) {
    throw std::runtime_error(
        "directed replacement rewrote its target RPC audit");
  }
  RequireNotContains(std::string_view(target_rpc_audit_after)
                         .substr(target_rpc_audit_before.size()),
                     "addnode 127.0.0.1:18169",
                     "directed replacement reverse restoration RPC");
  RequireNotContains(std::string_view(target_rpc_audit_after)
                         .substr(target_rpc_audit_before.size()),
                     "addnode 127.0.0.1:18170",
                     "directed replacement second reverse restoration RPC");

  process.Write("\x1b");
  static_cast<void>(process.ReadUntil(
      "Confirm exit", 3s, "directed replacement confirmed exit modal"));
  process.Write("y");
  RequireExitZero(&process, "directed replacement TUI exit");
}

void AppendMalformedEvent(const std::filesystem::path& events_path) {
  std::ofstream stream(events_path, std::ios::app);
  if (!stream) {
    throw std::runtime_error("could not append active-run malformed event");
  }
  stream << "{malformed-event\n";
  if (!stream) {
    throw std::runtime_error("could not flush active-run malformed event");
  }
}

void CheckActiveRunLifecycle(const std::filesystem::path& command,
                             const std::filesystem::path& helper_binary) {
  OwnedTemporaryDirectory directory("active");
  const std::filesystem::path daemon =
      CopyActiveDaemonFixtures(helper_binary, directory.root());
  const std::filesystem::path scenario = directory.root() / "scenario.json";
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::string run_id = "tui-active-" + std::to_string(getpid());
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  const std::filesystem::path qt_execution_marker =
      directory.root() / "firo-qt-was-executed";
  WriteActiveScenario(scenario);

  if (setenv("BBP_TUI_FIRO_QT_EXECUTION_MARKER", qt_execution_marker.c_str(),
             1) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "set Firo-Qt execution marker");
  }

  PtyProcess process(
      command,
      {"--scenario", scenario.string(), "--node-binary", daemon.string(),
       "--benchmark-root", benchmark_root.string(), "--run-id", run_id,
       "--refresh-ms", "50"},
      30, 100);
  if (unsetenv("BBP_TUI_FIRO_QT_EXECUTION_MARKER") != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "clear Firo-Qt execution marker");
  }
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 5s,
                                      "active benchmark"));
  const std::string startup_events =
      WaitForFileText(events_path, "\"event\":\"process_started\"", 10s);
  const pid_t daemon_pid = ProcessStartedPid(startup_events);
  if (!process.Running() || !ProcessExists(daemon_pid)) {
    throw std::runtime_error("active benchmark was not running before Esc");
  }
  static_cast<void>(WaitForFileText(
      events_path, "\"event\":\"operator_connection_command\"", 10s));

  const std::string expected_qt_command = ActiveOperatorConnectionCommand(
      run_root, daemon.parent_path() / "firo-qt");
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Live command", 3s, "active Firo-Qt palette"));
  process.Write("firo-qt\n");
  const auto first_launcher_result = [&] {
    try {
      return ReadLauncherDialog(process, "active Firo-Qt launcher dialog");
    } catch (const std::exception& error) {
      throw std::runtime_error(std::string(error.what()) +
                               "\nactive events:\n" + ReadFile(events_path) +
                               "\nactive simulator log:\n" +
                               ReadFile(run_root / "simulator.log"));
    }
  }();
  const auto& [first_dialog, first_launcher] = first_launcher_result;
  static_cast<void>(first_dialog);
  if (ReadFile(first_launcher) !=
      "#!/bin/bash\nexec " + expected_qt_command + "\n") {
    throw std::runtime_error(
        "active Firo-Qt launcher did not preserve the complete command");
  }
  if (!process.Running() || !ProcessExists(daemon_pid) ||
      std::filesystem::exists(qt_execution_marker)) {
    throw std::runtime_error(
        "creating the Firo-Qt launcher executed it or changed the active run");
  }

  process.Write("\n");
  static_cast<void>(process.ReadFor(200ms));
  if (!std::filesystem::exists(first_launcher) || !process.Running() ||
      !ProcessExists(daemon_pid)) {
    throw std::runtime_error(
        "dismissing the Firo-Qt dialog changed the active run or launcher");
  }
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Live command", 3s, "repeated Firo-Qt palette"));
  process.Write("firo-qt\n");
  auto [second_dialog, second_launcher] =
      ReadLauncherDialog(process, "repeated Firo-Qt launcher dialog");
  static_cast<void>(second_dialog);
  if (first_launcher == second_launcher ||
      std::filesystem::exists(first_launcher) ||
      !std::filesystem::exists(second_launcher)) {
    throw std::runtime_error(
        "repeated Firo-Qt command did not retain exactly one launcher");
  }
  process.Write("\n");
  static_cast<void>(process.ReadFor(200ms));
  const std::string after_launcher = ReadFile(events_path);
  RequireNotContains(after_launcher, "\"event\":\"run_cancelled\"",
                     "active Firo-Qt dialog dismissal");
  RequireNotContains(after_launcher, "\"event\":\"run_finished\"",
                     "active Firo-Qt dialog dismissal");
  if (!process.Running() || !ProcessExists(daemon_pid) ||
      std::filesystem::exists(qt_execution_marker)) {
    throw std::runtime_error(
        "Firo-Qt dialog dismissal stopped the active worker or ran Firo-Qt");
  }

  std::this_thread::sleep_for(200ms);
  static_cast<void>(process.ReadFor(100ms));
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Live command", 3s, "active destructive palette"));
  process.Write("kill\n");
  static_cast<void>(process.ReadUntil("Confirm destructive action", 3s,
                                      "active destructive confirmation"));
  AppendMalformedEvent(events_path);
  static_cast<void>(process.ReadUntil(
      "error:", 3s, "active destructive confirmation error frame"));
  process.Write("n");
  static_cast<void>(process.ReadFor(100ms));

  static_cast<void>(process.ReadFor(100ms));
  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "active-run exit modal"));
  process.Write("n");
  if (process.ReadFor(500ms).empty()) {
    throw std::runtime_error("active-run cancel path did not refresh");
  }
  std::this_thread::sleep_for(150ms);
  const std::string after_cancel = ReadFile(events_path);
  RequireNotContains(after_cancel, "\"event\":\"run_cancelled\"",
                     "active-run cancel path");
  RequireNotContains(after_cancel, "\"event\":\"run_finished\"",
                     "active-run cancel path");
  if (!process.Running() || !ProcessExists(daemon_pid)) {
    throw std::runtime_error("Esc,n stopped the active worker or its daemon");
  }

  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "active-run confirmed exit modal"));
  process.Write("y");
  RequireExitZero(&process, "active RunBenchmarkWithTui exit");
  const std::string finished_events =
      WaitForFileText(events_path, "\"event\":\"run_finished\"", 3s);
  RequireContains(finished_events, "\"event\":\"run_cancelled\"",
                  "active-run confirmed exit");
  WaitForProcessExit(daemon_pid, 3s);
  struct stat launcher_status{};
  errno = 0;
  if (lstat(second_launcher.c_str(), &launcher_status) == 0 ||
      errno != ENOENT) {
    throw std::runtime_error(
        "owned Firo-Qt launcher survived confirmed active-run cleanup");
  }
  if (std::filesystem::exists(qt_execution_marker)) {
    throw std::runtime_error("BBP executed the generated Firo-Qt launcher");
  }
}

int RunIdleDaemon() {
  while (true) {
    pause();
  }
}

}  // namespace

int main(int argc, char** argv) {
  const std::filesystem::path executable_name =
      std::filesystem::path(argv[0]).filename();
  if (executable_name == "firo-qt") {
    const char* marker = std::getenv("BBP_TUI_FIRO_QT_EXECUTION_MARKER");
    if (marker != nullptr) {
      std::ofstream stream(marker);
      stream << "executed\n";
    }
    return 0;
  }
  if (executable_name == "ready-firod") {
    try {
      return RunReadyFiroDaemon(argc, argv);
    } catch (const std::exception& error) {
      std::cerr << "ready Firo daemon failed: " << error.what() << '\n';
      return 1;
    }
  }
  if (argc == 5 && std::string_view(argv[1]) == "--direct-load-lifecycle") {
    try {
      CheckDirectLoadLifecycle(argv[2], argv[3], argv[4]);
      std::cout << "direct no-JSON finite option and indefinite active-run "
                   "lifecycle checks passed\n";
      return 0;
    } catch (const std::exception& error) {
      std::cerr << "direct-load lifecycle regression failed: " << error.what()
                << '\n';
      return 1;
    }
  }
  if (argc == 3 && std::string_view(argv[1]) == "--empty-control-plane") {
    try {
      CheckEmptyControlPlane(argv[2]);
      std::cout << "empty zero-node TUI and MCP lifecycle checks passed\n";
      return 0;
    } catch (const std::exception& error) {
      std::cerr << "empty control-plane regression failed: " << error.what()
                << '\n';
      return 1;
    }
  }
  if (argc == 4 && std::string_view(argv[1]) == "--simulator-log-wrap") {
    try {
      CheckSimulatorLogWrapping(argv[2], argv[3]);
      std::cout << "simulator-log visual-row wrapping, scrolling, and resize "
                   "checks passed\n";
      return 0;
    } catch (const std::exception& error) {
      std::cerr << "simulator-log wrapping regression failed: " << error.what()
                << '\n';
      return 1;
    }
  }
  if (argc == 4 && std::string_view(argv[1]) == "--retained-mcp") {
    try {
      CheckRetainedMcpLifecycle(argv[2], argv[3]);
      std::cout << "retained TUI and MCP lifecycle checks passed\n";
      return 0;
    } catch (const std::exception& error) {
      std::cerr << "retained MCP regression failed: " << error.what() << '\n';
      return 1;
    }
  }
  if (argc == 3 && std::string_view(argv[1]) == "--slow-node-replace") {
    try {
      CheckSlowNodeReplacementDefaultTimeout(
          argv[2], std::filesystem::canonical(argv[0]));
      return 0;
    } catch (const std::exception& error) {
      std::cerr << "slow node replacement regression failed: " << error.what()
                << '\n';
      return 1;
    }
  }
  if (argc > 1 && argv[1][0] == '-') {
    return RunIdleDaemon();
  }
  if (argc != 4) {
    std::cerr << "usage: " << argv[0]
              << " BBP LIVE_RUN_ROOT COMPLETE_RUN_ROOT\n";
    return 2;
  }
  try {
    const std::filesystem::path command = argv[1];
    const std::filesystem::path live_run = argv[2];
    const std::filesystem::path complete_run = argv[3];
    CheckCanonicalExitModal(command, live_run);
    CheckPaletteOnErrorFrame(command, complete_run);
    CheckCommandErrorOnErrorFrame(command, complete_run);
    CheckActiveRunLifecycle(command, std::filesystem::canonical(argv[0]));
    CheckZeroToOnePublication(command, std::filesystem::canonical(argv[0]));
    CheckNodeReplacementPublication(command,
                                    std::filesystem::canonical(argv[0]));
    CheckDirectedNodeReplacementPublication(
        command, std::filesystem::canonical(argv[0]));
    std::cout << "canonical modal, shared error-frame overlays, and active "
                 "RunBenchmarkWithTui lifecycle checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "tui PTY regression failed: " << error.what() << '\n';
    return 1;
  }
}
