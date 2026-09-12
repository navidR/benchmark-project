// Public capacity acceptance using the existing ready-firod executable fixture.
// PTY and MCP transport follow tui_pty_helper.cpp; no application internals
// linked.
#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <poll.h>
#include <pty.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
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
#include <initializer_list>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

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
      const auto deadline = std::chrono::steady_clock::now() + 30s;
      while (pid_ > 0 && std::chrono::steady_clock::now() < deadline) {
        const pid_t result = waitpid(pid_, nullptr, WNOHANG);
        if (result == pid_ || (result < 0 && errno == ECHILD)) {
          pid_ = -1;
          break;
        }
        try {
          static_cast<void>(ReadFor(10ms));
        } catch (...) {
          std::this_thread::sleep_for(10ms);
        }
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
    if (!verified_success_) {
      std::cerr << "Preserved failed capacity fixture and ownership evidence: "
                << root_ << '\n';
      return;
    }
    std::error_code error;
    std::filesystem::remove_all(root_, error);
  }

  const std::filesystem::path& root() const { return root_; }
  void MarkVerifiedSuccess() { verified_success_ = true; }

 private:
  std::filesystem::path root_;
  bool verified_success_ = false;
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

boost::json::object WaitForMcpOperation(
    const McpTestSession& session, std::uint64_t* request_id,
    const boost::json::object& submitted,
    std::chrono::steady_clock::duration timeout, std::string_view context) {
  const boost::json::value* operation_id =
      submitted.if_contains("operation_id");
  if (operation_id == nullptr || !operation_id->is_string()) {
    throw std::runtime_error(
        std::string(context) +
        " returned no operation_id: " + boost::json::serialize(submitted));
  }
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  boost::json::object operation;
  while (std::chrono::steady_clock::now() < deadline) {
    operation =
        McpToolCall(session.port, session.token, session.session_id,
                    session.protocol_version, (*request_id)++, "operation.get",
                    boost::json::object{{"operation_id", *operation_id}});
    const std::string_view state = operation.at("state").as_string();
    if (state == "succeeded") {
      const boost::json::value* terminal =
          operation.if_contains("terminal_result");
      if (terminal == nullptr || !terminal->is_object()) {
        throw std::runtime_error(std::string(context) +
                                 " succeeded without a terminal result");
      }
      return terminal->as_object();
    }
    if (state == "failed" || state == "cancelled") {
      throw std::runtime_error(std::string(context) + " ended in state " +
                               std::string(state) + ": " +
                               boost::json::serialize(operation));
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error(std::string(context) +
                           " did not finish before its test deadline");
}

boost::json::object SubmitMcpOperation(const McpTestSession& session,
                                       std::uint64_t* request_id,
                                       std::string_view tool,
                                       boost::json::object arguments) {
  return McpToolCall(session.port, session.token, session.session_id,
                     session.protocol_version, (*request_id)++, tool,
                     std::move(arguments));
}

boost::json::object InvokeMcpOperation(
    const McpTestSession& session, std::uint64_t* request_id,
    std::string_view tool, boost::json::object arguments,
    std::chrono::steady_clock::duration timeout, std::string_view context) {
  const boost::json::object submitted =
      SubmitMcpOperation(session, request_id, tool, std::move(arguments));
  return WaitForMcpOperation(session, request_id, submitted, timeout, context);
}

boost::json::value ReadMcpResourceData(const McpTestSession& session,
                                       std::uint64_t* request_id,
                                       std::string_view uri,
                                       std::string_view context) {
  const http::response<http::string_body> response =
      McpExchange(session.port, session.token,
                  boost::json::serialize(boost::json::object{
                      {"jsonrpc", "2.0"},
                      {"id", (*request_id)++},
                      {"method", "resources/read"},
                      {"params", boost::json::object{{"uri", uri}}}}),
                  session.session_id, session.protocol_version);
  if (response.result() != http::status::ok) {
    throw std::runtime_error(std::string(context) + " returned HTTP " +
                             std::to_string(response.result_int()) + ": " +
                             response.body());
  }
  const boost::json::object decoded =
      boost::json::parse(response.body()).as_object();
  const boost::json::array& contents =
      decoded.at("result").as_object().at("contents").as_array();
  if (contents.size() != 1U || !contents.front().is_object()) {
    throw std::runtime_error(std::string(context) +
                             " returned invalid resource contents");
  }
  const boost::json::object& content = contents.front().as_object();
  if (content.at("uri").as_string() != uri ||
      content.at("mimeType").as_string() != "application/json") {
    throw std::runtime_error(std::string(context) +
                             " returned incorrect resource metadata");
  }
  const boost::json::object envelope =
      boost::json::parse(content.at("text").as_string()).as_object();
  return envelope.at("data");
}

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

void Require(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

void WriteFile(const std::filesystem::path& path, std::string_view contents) {
  std::ofstream output(path);
  output << contents;
  Require(static_cast<bool>(output), "could not write " + path.string());
}

// A deterministic daemon fixture. Real BBP processes, cgroups, RPC, namespace
// setup, role registration, persistence, replay, and cleanup stay on their
// public production paths; only the blockchain responses are simulated.
int RunCapacityDaemon(int argc, char** argv) {
  static_cast<void>(umask(0077));
  const auto argument = [&](std::string_view prefix) {
    for (int index = 1; index < argc; ++index) {
      const std::string_view value(argv[index]);
      if (value.starts_with(prefix)) {
        return std::string(value.substr(prefix.size()));
      }
    }
    return std::string{};
  };
  const std::filesystem::path data = argument("-datadir=");
  const std::filesystem::path node = data.parent_path();
  const std::filesystem::path run = node.parent_path().parent_path();
  const std::string id = node.filename();
  if (id == "failed-ready") {
    WriteFile(run / "fixture-readiness-failed", id);
    return 9;
  }
  if (id == "race-a") {
    WriteFile(run / "fixture-admission-held", id);
    static_cast<void>(
        WaitForFileText(run / "fixture-admission-release", "release", 15s));
  }
  WriteFile(argument("-rpccookiefile="), "bbp-test:bbp-test");
  const unsigned long port = std::stoul(argument("-rpcport="));
  Require(port > 0U && port <= 65535U, "invalid fixture RPC port");
  asio::io_context context;
  tcp::acceptor acceptor(context);
  acceptor.open(tcp::v4());
  acceptor.set_option(tcp::acceptor::reuse_address(true));
  acceptor.bind(tcp::endpoint(asio::ip::make_address_v4(argument("-rpcbind=")),
                              static_cast<std::uint16_t>(port)));
  acceptor.listen();
  std::set<std::string> peers;
  for (int index = 1; index < argc; ++index) {
    const std::string_view value(argv[index]);
    if (value.starts_with("-connect=")) {
      peers.insert(std::string(value.substr(9U)));
    }
  }
  std::uint64_t address_index = 0U;
  std::uint64_t height = 600U;
  bool stop = false;
  while (!stop) {
    tcp::socket socket(context);
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    boost::system::error_code error;
    http::read(socket, buffer, request, error);
    if (error) {
      continue;
    }
    const boost::json::object decoded =
        boost::json::parse(request.body()).as_object();
    const std::string_view method = decoded.at("method").as_string();
    const boost::json::array& parameters = decoded.at("params").as_array();
    boost::json::value result;
    boost::json::value rpc_error;
    if (method == "getblockchaininfo") {
      result = boost::json::object{{"blocks", height},
                                   {"headers", height},
                                   {"bestblockhash", "block-600"},
                                   {"initialblockdownload", false},
                                   {"verificationprogress", 1.0},
                                   {"difficulty", 1.0},
                                   {"mediantime", 0U},
                                   {"chainwork", "00"}};
    } else if (method == "getnetworkinfo") {
      result = boost::json::object{{"version", 1U},
                                   {"protocolversion", 1U},
                                   {"subversion", "/bbp-capacity-test/"},
                                   {"connections", peers.size()}};
    } else if (method == "getmempoolinfo") {
      result = boost::json::object{{"size", 0U}, {"bytes", 0U}};
    } else if (method == "getblockcount") {
      result = height;
    } else if (method == "getnetworkhashps") {
      result = 0U;
    } else if (method == "getnewaddress") {
      if (id == "failed-wallet") {
        WriteFile(run / "fixture-wallet-failed", id);
        rpc_error = boost::json::object{{"code", -4},
                                        {"message", "fixture wallet failure"}};
      } else {
        result = id + "-address-" + std::to_string(++address_index);
      }
    } else if (method == "getbalance") {
      result = 100000.0;
    } else if (method == "getwalletinfo") {
      result = boost::json::object{{"balance", 100000.0},
                                   {"unconfirmed_balance", 0.0},
                                   {"immature_balance", 0.0},
                                   {"txcount", 0U}};
    } else if (method == "listtransactions" || method == "getrawmempool" ||
               method == "listbanned") {
      result = boost::json::array{};
    } else if (method == "getpeerinfo") {
      boost::json::array entries;
      for (const std::string& peer : peers) {
        entries.emplace_back(boost::json::object{
            {"addr", peer},
            {"bytesrecv_per_msg", boost::json::object{{"verack", 1U}}}});
      }
      result = std::move(entries);
    } else if (method == "addnode") {
      peers.insert(std::string(parameters.front().as_string()));
    } else if (method == "disconnectnode") {
      peers.erase(std::string(parameters.front().as_string()));
    } else if (method == "settxfee") {
      result = true;
    } else if (method == "setban") {
      result = nullptr;
    } else if (method == "bls") {
      result = boost::json::object{{"secret", std::string(64U, '1')},
                                   {"public", std::string(96U, '2')}};
    } else if (method == "protx") {
      result = parameters.front().as_string() == "revoke"
                   ? "revocation-transaction"
                   : "registration-transaction";
    } else if (method == "evoznode") {
      result = boost::json::object{{"proTxHash", "registration-transaction"},
                                   {"service", argument("-externalip=")},
                                   {"collateralHash", "collateral-transaction"},
                                   {"collateralIndex", 0U},
                                   {"state", "READY"},
                                   {"status", "Ready"}};
    } else if (method == "getrawtransaction") {
      result = boost::json::object{{"txid", parameters.front()},
                                   {"blockhash", "block-600"},
                                   {"height", 600U},
                                   {"confirmations", 1U}};
    } else if (method == "getblockheader") {
      result = boost::json::object{
          {"hash", parameters.front()}, {"height", 600U}, {"time", 0U}};
    } else if (method == "getblock") {
      result = boost::json::object{{"tx", boost::json::array{"coinbase"}}};
    } else if (method == "generatetoaddress") {
      boost::json::array hashes;
      for (std::uint64_t index = 0U;
           index < parameters.front().to_number<std::uint64_t>(); ++index) {
        hashes.emplace_back("block-" + std::to_string(++height));
      }
      result = std::move(hashes);
    } else if (method == "stop") {
      stop = true;
    } else {
      rpc_error = boost::json::object{
          {"code", -32601},
          {"message", "unsupported fixture RPC " + std::string(method)}};
    }
    http::response<http::string_body> response{http::status::ok, 11};
    response.set(http::field::content_type, "application/json");
    response.set(http::field::connection, "close");
    response.body() = boost::json::serialize(
        boost::json::object{{"result", std::move(result)},
                            {"error", std::move(rpc_error)},
                            {"id", decoded.at("id")}});
    response.prepare_payload();
    http::write(socket, response, error);
  }
  return 0;
}

boost::json::object WaitForTerminal(const McpTestSession& session,
                                    std::uint64_t* request_id,
                                    const boost::json::object& submitted) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    boost::json::object operation = McpToolCall(
        session.port, session.token, session.session_id,
        session.protocol_version, (*request_id)++, "operation.get",
        boost::json::object{{"operation_id", submitted.at("operation_id")}});
    const std::string_view state = operation.at("state").as_string();
    if (state == "succeeded" || state == "failed" || state == "cancelled") {
      return operation;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("capacity operation did not terminate");
}

boost::json::object RunRegistryEntry(const McpTestSession& session,
                                     std::uint64_t* request_id,
                                     std::string_view run_id) {
  const boost::json::value registry = ReadMcpResourceData(
      session, request_id, "bbp:///run_registry", "capacity run registry");
  for (const boost::json::value& value : registry.as_array()) {
    if (value.as_object().at("run_id").as_string() == run_id) {
      return value.as_object();
    }
  }
  throw std::runtime_error("run missing from capacity run registry");
}

boost::json::object JsonFile(const std::filesystem::path& path) {
  return boost::json::parse(ReadFile(path)).as_object();
}

std::set<std::string> DirectoryNames(const std::filesystem::path& path) {
  std::set<std::string> result;
  if (std::filesystem::exists(path)) {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      if (entry.is_directory()) {
        result.insert(entry.path().filename().string());
      }
    }
  }
  return result;
}

std::map<std::string, std::string> NodeEndpoints(
    const std::filesystem::path& run) {
  std::map<std::string, std::string> endpoints;
  std::istringstream events(ReadFile(run / "events.jsonl"));
  for (std::string line; std::getline(events, line);) {
    if (line.empty()) {
      continue;
    }
    const boost::json::object event = boost::json::parse(line).as_object();
    if (event.at("event").as_string() == "network_ready") {
      endpoints.insert_or_assign(std::string(event.at("node_id").as_string()),
                                 std::string(event.at("detail").as_string()));
    }
  }
  return endpoints;
}

void CheckCapacity(const McpTestSession& session, std::uint64_t* request_id,
                   const std::filesystem::path& run, std::uint32_t count,
                   std::uint32_t capacity, bool isolated,
                   const boost::json::array& original_links = {}) {
  const boost::json::object entry =
      RunRegistryEntry(session, request_id, run.filename().string());
  Require(entry.at("node_count").to_number<std::uint32_t>() == count &&
              entry.at("node_capacity").to_number<std::uint32_t>() == capacity,
          "run registry did not publish exact capacity/inventory");
  const boost::json::object manifest =
      JsonFile(run / "runtime-node-resources.json");
  Require(manifest.at("node_capacity").to_number<std::uint32_t>() == capacity &&
              manifest.at("nodes").as_array().size() == count,
          "resource manifest did not persist capacity/inventory");
  Require(DirectoryNames(run / "nodes").size() == count,
          "node roots differ from the published inventory");
  const boost::json::object source = JsonFile(run / "source-scenario.json");
  const boost::json::object resolved = JsonFile(run / "resolved-scenario.json");
  Require(
      source.at("node_capacity").to_number<std::uint32_t>() == capacity &&
          resolved.at("node_capacity").to_number<std::uint32_t>() == capacity,
      "source/resolved scenarios did not persist enlarged capacity");
  if (isolated) {
    const boost::json::value& allocation = entry.at("network_allocation");
    Require(allocation == manifest.at("network_allocation") &&
                allocation == resolved.at("network_allocation"),
            "published and persisted address allocations disagree");
    const boost::json::array& links =
        allocation.as_object().at("link_cidrs").as_array();
    Require(links.size() == capacity && links.size() >= original_links.size(),
            "address reservation differs from capacity");
    std::set<std::string> unique;
    for (std::size_t index = 0U; index < links.size(); ++index) {
      const std::string cidr(links[index].as_string());
      Require(cidr.ends_with("/31") && unique.insert(cidr).second,
              "address reservation has duplicate or non-/31 links");
      if (index < original_links.size()) {
        Require(links[index] == original_links[index],
                "growth renumbered an existing node link");
      }
    }
  } else {
    Require(entry.at("network_allocation").is_null() &&
                (!manifest.if_contains("network_allocation") ||
                 manifest.at("network_allocation").is_null()) &&
                resolved.at("network_allocation").is_null(),
            "shared-network run published a fictional address reservation");
  }
}

void CheckRuntimeCapacity(const std::filesystem::path& command,
                          const std::filesystem::path& helper, bool isolated) {
  OwnedTemporaryDirectory temporary(isolated ? "capacity-isolated"
                                             : "capacity-shared");
  const std::filesystem::path root = temporary.root();
  const std::filesystem::path runs = root / "runs";
  const std::filesystem::path home = root / "home";
  const std::filesystem::path bin = root / "bin";
  std::filesystem::create_directory(home);
  std::filesystem::create_directory(bin);
  const std::filesystem::path daemon = bin / "capacity-firod";
  std::filesystem::copy_file("/proc/self/exe", daemon);
  std::filesystem::copy_file(helper, bin / "firo-qt");
  const std::string run_id = "capacity-growth";
  const std::filesystem::path run = runs / run_id;
  PtyProcess process(command,
                     {"--benchmark-root", runs.string(), "--refresh-ms", "50"},
                     30, 120, home);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 5s,
                                      "capacity editor"));
  const McpTestSession session = ConnectMcpTestSession(home / ".bbp" / "mcp");
  // Keep the terminal flowing while public MCP requests drive the editor.
  std::jthread terminal_drain([&](std::stop_token stop) {
    while (!stop.stop_requested()) {
      static_cast<void>(process.ReadFor(50ms));
    }
  });
  std::uint64_t request_id = 2U;
  const auto invoke = [&](std::string_view tool,
                          boost::json::object arguments) {
    return InvokeMcpOperation(session, &request_id, tool, std::move(arguments),
                              30s, tool);
  };
  const auto node_request = [](std::uint32_t count,
                               boost::json::array ids = {}) {
    boost::json::object request{{"chain", "firo"},
                                {"count", count},
                                {"ready_timeout_sec", 5U},
                                {"sync_timeout_sec", 5U}};
    if (!ids.empty()) {
      request["node_ids"] = std::move(ids);
    }
    return request;
  };
  boost::json::object scenario{
      {"run_id", run_id},
      {"chain", "firo"},
      {"chain_daemon", daemon.string()},
      {"nodes", 11U},
      {"node_capacity", 16U},
      {"isolated_network", isolated},
      {"network_address_pool", "10.253.0.0/20"},
      {"topology",
       boost::json::object{{"node_count", 11U},
                           {"wallet_node_count", 1U},
                           {"miner_node_count", 1U},
                           {"wallet_nodes", boost::json::array{1U}},
                           {"miner_nodes", boost::json::array{2U}},
                           {"wallet_initialization",
                            boost::json::object{{"strategy", "driver_rpc"},
                                                {"mode", "public"}}}}},
      {"block_production", boost::json::object{{"enabled", false}}},
      {"ready_timeout_sec", 5U},
      {"sync_timeout_sec", 5U},
      {"metrics_interval_ms", 10000U},
      {"workloads", boost::json::array{}}};
  static_cast<void>(invoke("run.launch", {{"scenario", scenario}}));
  CheckCapacity(session, &request_id, run, 11U, 16U, isolated);
  if (!isolated) {
    // Firo requires namespaces to identify individual runtime-added peers.
    // Its documented driver refusal must not leak an expanded reservation.
    const auto initial_roots = DirectoryNames(run / "nodes");
    const std::map<std::string, std::string> initial_documents{
        {"runtime-node-resources.json",
         ReadFile(run / "runtime-node-resources.json")},
        {"source-scenario.json", ReadFile(run / "source-scenario.json")},
        {"resolved-scenario.json", ReadFile(run / "resolved-scenario.json")},
        {"scenario.yaml", ReadFile(run / "scenario.yaml")}};
    const boost::json::object submitted =
        SubmitMcpOperation(session, &request_id, "wallet.add",
                           {{"run_id", run_id},
                            {"count", 10U},
                            {"mode", "public"},
                            {"create_node", node_request(10U)}});
    const boost::json::object failed =
        WaitForTerminal(session, &request_id, submitted);
    Require(failed.at("state").as_string() == "failed" &&
                boost::json::serialize(failed).find(
                    "per-node peer identity without isolated networking") !=
                    std::string::npos,
            "shared-network growth omitted its truthful driver refusal: " +
                boost::json::serialize(failed));
    CheckCapacity(session, &request_id, run, 11U, 16U, false);
    Require(DirectoryNames(run / "nodes") == initial_roots,
            "shared-network refusal retained candidate node roots");
    for (const auto& [name, contents] : initial_documents) {
      Require(ReadFile(run / name) == contents,
              "shared-network refusal changed reservation document " + name);
    }
    static_cast<void>(
        invoke("run.stop", {{"run_id", run_id}, {"timeout_sec", 10U}}));
    static_cast<void>(
        invoke("run.clean", {{"run_id", run_id},
                             {"timeout_sec", 10U},
                             {"remove_retained_artifacts", true}}));
    Require(!std::filesystem::exists(run),
            "shared-network cleanup retained its run");
    terminal_drain.request_stop();
    terminal_drain.join();
    process.Write("\x1b");
    static_cast<void>(
        process.ReadUntil("Confirm exit", 3s, "capacity editor exit"));
    process.Write("y");
    Require(process.Wait(10s) == 0, "capacity editor did not exit cleanly");
    temporary.MarkVerifiedSuccess();
    return;
  }
  const boost::json::array original_links =
      isolated ? JsonFile(run / "runtime-node-resources.json")
                     .at("network_allocation")
                     .as_object()
                     .at("link_cidrs")
                     .as_array()
               : boost::json::array{};
  const auto original_endpoints = NodeEndpoints(run);
  Require(original_endpoints.size() == (isolated ? 11U : 0U),
          "initial network endpoint evidence has the wrong cardinality");
  const auto original_node_roots = DirectoryNames(run / "nodes");
  const auto check_success = [&](const boost::json::object& result,
                                 std::uint32_t count) {
    Require(result.at("node_capacity").to_number<std::uint32_t>() == count,
            "creation result omitted enlarged node capacity: " +
                boost::json::serialize(result));
    CheckCapacity(session, &request_id, run, count, count, isolated,
                  original_links);
    const auto endpoints = NodeEndpoints(run);
    for (const auto& [id, endpoint] : original_endpoints) {
      Require(endpoints.contains(id) && endpoints.at(id) == endpoint,
              "growth changed an existing node network endpoint");
    }
    const auto current_roots = DirectoryNames(run / "nodes");
    Require(
        std::includes(current_roots.begin(), current_roots.end(),
                      original_node_roots.begin(), original_node_roots.end()),
        "growth lost an original node root");
  };

  check_success(invoke("wallet.add", {{"run_id", run_id},
                                      {"count", 10U},
                                      {"mode", "public"},
                                      {"create_node", node_request(10U)}}),
                21U);
  check_success(
      invoke("node.add", {{"run_id", run_id}, {"request", node_request(1U)}}),
      22U);
  check_success(invoke("miner.add", {{"run_id", run_id},
                                     {"count", 1U},
                                     {"create_nodes", node_request(1U)}}),
                23U);
  check_success(invoke("masternode.add", {{"run_id", run_id},
                                          {"count", 1U},
                                          {"funding_wallet_id", "firo-1"},
                                          {"create_nodes", node_request(1U)}}),
                24U);

  const auto reject = [&](std::string_view tool, boost::json::object arguments,
                          std::string_view expected) {
    const boost::json::object submitted =
        SubmitMcpOperation(session, &request_id, tool, std::move(arguments));
    boost::json::object terminal = submitted;
    if (submitted.if_contains("operation_id")) {
      terminal = WaitForTerminal(session, &request_id, submitted);
      Require(terminal.at("state").as_string() == "failed",
              "invalid creation unexpectedly succeeded: " +
                  boost::json::serialize(terminal));
    }
    Require(
        boost::json::serialize(terminal).find(expected) != std::string::npos,
        "creation rejection omitted expected cause " + std::string(expected) +
            ": " + boost::json::serialize(terminal));
  };
  reject("node.add", {{"run_id", run_id}, {"request", node_request(17U)}},
         "16");
  const std::map<std::string, std::string> persisted_before{
      {"runtime-node-resources.json",
       ReadFile(run / "runtime-node-resources.json")},
      {"source-scenario.json", ReadFile(run / "source-scenario.json")},
      {"resolved-scenario.json", ReadFile(run / "resolved-scenario.json")},
      {"scenario.yaml", ReadFile(run / "scenario.yaml")}};
  const auto before_failure = DirectoryNames(run / "nodes");
  const auto check_rollback = [&] {
    CheckCapacity(session, &request_id, run, 24U, 24U, isolated,
                  original_links);
    Require(DirectoryNames(run / "nodes") == before_failure,
            "failed creation left candidate node roots");
    for (const auto& [name, contents] : persisted_before) {
      Require(ReadFile(run / name) == contents,
              "failed creation changed capacity document " + name);
    }
  };
  reject("node.add",
         {{"run_id", run_id}, {"request", node_request(1U, {"failed-ready"})}},
         "failed");
  Require(ReadFile(run / "fixture-readiness-failed") == "failed-ready",
          "readiness failure did not reach daemon startup");
  check_rollback();
  reject("wallet.add",
         {{"run_id", run_id},
          {"count", 1U},
          {"mode", "public"},
          {"create_node", node_request(1U, {"failed-wallet"})}},
         "fixture wallet failure");
  Require(ReadFile(run / "fixture-wallet-failed") == "failed-wallet",
          "wallet failure did not reach role preparation");
  check_rollback();
  const std::filesystem::path blocked =
      run / ".resolved-scenario.json.capacity.tmp";
  WriteFile(blocked, "owned collision sentinel\n");
  reject(
      "node.add",
      {{"run_id", run_id}, {"request", node_request(1U, {"failed-persist"})}},
      "exist");
  Require(ReadFile(blocked) == "owned collision sentinel\n",
          "capacity rollback removed the foreign publication collision");
  Require(std::filesystem::remove(blocked), "remove owned collision fixture");
  check_rollback();

  const boost::json::object first = SubmitMcpOperation(
      session, &request_id, "node.add",
      {{"run_id", run_id}, {"request", node_request(1U, {"race-a"})}});
  static_cast<void>(
      WaitForFileText(run / "fixture-admission-held", "race-a", 5s));
  const boost::json::object second =
      SubmitMcpOperation(session, &request_id, "wallet.add",
                         {{"run_id", run_id},
                          {"count", 1U},
                          {"mode", "public"},
                          {"create_node", node_request(1U, {"race-b"})}});
  std::this_thread::sleep_for(100ms);
  const boost::json::object held =
      RunRegistryEntry(session, &request_id, run_id);
  Require(held.at("node_capacity").to_number<std::uint32_t>() == 24U &&
              held.at("node_count").to_number<std::uint32_t>() == 24U &&
              !std::filesystem::exists(run / "nodes" / "race-b"),
          "concurrent creation crossed unpublished mutation admission");
  WriteFile(run / "fixture-admission-release", "release");
  const boost::json::object first_result = WaitForMcpOperation(
      session, &request_id, first, 30s, "first concurrent add");
  const boost::json::object second_result = WaitForMcpOperation(
      session, &request_id, second, 30s, "second concurrent add");
  Require(
      first_result.at("node_capacity").to_number<std::uint32_t>() == 25U &&
          second_result.at("node_capacity").to_number<std::uint32_t>() == 26U,
      "concurrent creation did not serialize capacity growth");
  check_success(second_result, 26U);

  static_cast<void>(
      invoke("run.stop", {{"run_id", run_id}, {"timeout_sec", 10U}}));
  const boost::json::object retained =
      RunRegistryEntry(session, &request_id, run_id);
  Require(retained.at("node_count").to_number<std::uint32_t>() == 26U &&
              retained.at("node_capacity").to_number<std::uint32_t>() == 26U,
          "retained run lost final capacity/inventory");
  const boost::json::object summary =
      JsonFile(run / "run-registry-summary.json");
  Require(summary.at("node_count").to_number<std::uint32_t>() == 26U &&
              summary.at("node_capacity").to_number<std::uint32_t>() == 26U,
          "persisted retained summary lost grown capacity/inventory");
  const std::string ownership_resource =
      std::string(JsonFile(run / ".bbp-run").at("resource_id").as_string());
  Require(!std::filesystem::exists(std::filesystem::path("/sys/fs/cgroup/bbp") /
                                   ownership_resource),
          "stopped run retained its owned cgroup");
  static_cast<void>(invoke("run.replay", {{"source_run_id", run_id},
                                          {"run_id", "capacity-replay"}}));
  // Replay reuses the source launch scenario and its enlarged reservation;
  // interactive node additions are not scenario events and are not replayed.
  CheckCapacity(session, &request_id, runs / "capacity-replay", 11U, 26U,
                isolated);
  static_cast<void>(invoke(
      "run.stop", {{"run_id", "capacity-replay"}, {"timeout_sec", 10U}}));
  for (const std::string_view id : {"capacity-replay", "capacity-growth"}) {
    static_cast<void>(
        invoke("run.clean", {{"run_id", id},
                             {"timeout_sec", 10U},
                             {"remove_retained_artifacts", true}}));
    Require(!std::filesystem::exists(runs / id),
            "cleanup retained a grown/replayed run");
  }

  if (isolated) {
    scenario["run_id"] = "capacity-exhaustion";
    scenario["nodes"] = 1U;
    scenario["node_capacity"] = 1U;
    scenario["network_address_pool"] = "10.253.240.0/31";
    scenario.erase("topology");
    static_cast<void>(invoke("run.launch", {{"scenario", scenario}}));
    reject("node.add",
           {{"run_id", "capacity-exhaustion"}, {"request", node_request(1U)}},
           "requested 1, available 0");
    CheckCapacity(session, &request_id, runs / "capacity-exhaustion", 1U, 1U,
                  true);
    static_cast<void>(invoke(
        "run.stop", {{"run_id", "capacity-exhaustion"}, {"timeout_sec", 10U}}));
    static_cast<void>(
        invoke("run.clean", {{"run_id", "capacity-exhaustion"},
                             {"timeout_sec", 10U},
                             {"remove_retained_artifacts", true}}));
  }
  Require(ReadMcpResourceData(session, &request_id, "bbp:///run_registry",
                              "clean capacity registry")
              .as_array()
              .empty(),
          "cleanup left a retained capacity run");
  terminal_drain.request_stop();
  terminal_drain.join();
  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "capacity editor exit"));
  process.Write("y");
  Require(process.Wait(10s) == 0, "capacity editor did not exit cleanly");
  temporary.MarkVerifiedSuccess();
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (std::filesystem::path(argv[0]).filename() == "capacity-firod") {
      return RunCapacityDaemon(argc, argv);
    }
    if (argc != 4 || (std::string_view(argv[1]) != "--shared-network" &&
                      std::string_view(argv[1]) != "--isolated")) {
      std::cerr << "usage: bbp-runtime-capacity-helper "
                   "--shared-network|--isolated BBP TUI_HELPER\n";
      return 2;
    }
    const bool isolated = std::string_view(argv[1]) == "--isolated";
    if (access("/sys/fs/cgroup", W_OK) != 0) {
      std::cout << "SKIP: writable cgroup v2 is required\n";
      return 77;
    }
    if (isolated) {
      std::istringstream status(ReadFile("/proc/self/status"));
      std::string line;
      std::uint64_t capabilities = 0U;
      while (std::getline(status, line)) {
        if (line.starts_with("CapEff:")) {
          capabilities = std::stoull(line.substr(7U), nullptr, 16);
        }
      }
      constexpr std::uint64_t kRequiredCapabilities =
          (std::uint64_t{1U} << 12U) | (std::uint64_t{1U} << 21U);
      if ((capabilities & kRequiredCapabilities) != kRequiredCapabilities) {
        std::cout << "SKIP: CAP_NET_ADMIN and CAP_SYS_ADMIN are required\n";
        return 77;
      }
    }
    CheckRuntimeCapacity(std::filesystem::absolute(argv[2]),
                         std::filesystem::absolute(argv[3]), isolated);
    std::cout
        << (isolated
                ? "public runtime capacity, roles, rollback, concurrent "
                  "growth, "
                  "persistence, replay, and cleanup checks passed\n"
                : "shared-network driver refusal, reservation rollback, and "
                  "cleanup checks passed\n");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "runtime capacity acceptance failed: " << error.what() << '\n';
    return 1;
  }
}
