#include "bbp/mcp_server.h"

#include <poll.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/system/error_code.hpp>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace bbp {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

void CloseSocket(tcp::socket* socket) {
  boost::system::error_code ignored;
  socket->cancel(ignored);
  socket->shutdown(tcp::socket::shutdown_both, ignored);
  socket->close(ignored);
}

enum class SocketWaitResult { kReady, kTimedOut, kCancelled, kFailed };

SocketWaitResult WaitForSocket(tcp::socket& socket, short events,
                               std::chrono::steady_clock::time_point deadline,
                               std::stop_token stop_token) {
  constexpr std::chrono::milliseconds kCancellationPollInterval{10};
  while (!stop_token.stop_requested()) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return SocketWaitResult::kTimedOut;
    }
    const auto remaining =
        std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
    const auto wait = std::min(remaining, kCancellationPollInterval);
    pollfd descriptor{
        .fd = socket.native_handle(), .events = events, .revents = 0};
    const int result = ::poll(&descriptor, 1U, static_cast<int>(wait.count()));
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      return SocketWaitResult::kFailed;
    }
    if (result == 0) {
      continue;
    }
    if ((descriptor.revents & (events | POLLERR | POLLHUP)) != 0) {
      return SocketWaitResult::kReady;
    }
    return SocketWaitResult::kFailed;
  }
  return SocketWaitResult::kCancelled;
}

enum class RequestReadResult {
  kComplete,
  kBodyLimit,
  kTimedOut,
  kCancelled,
  kFailed
};

RequestReadResult ReadRequest(beast::tcp_stream* stream,
                              beast::flat_buffer* buffer,
                              http::request_parser<http::string_body>* parser,
                              std::chrono::steady_clock::time_point deadline,
                              std::stop_token stop_token) {
  boost::system::error_code error;
  stream->socket().non_blocking(true, error);
  if (error) {
    return RequestReadResult::kFailed;
  }
  while (!parser->is_done()) {
    http::read_some(*stream, *buffer, *parser, error);
    if (!error) {
      continue;
    }
    if (error == http::error::body_limit) {
      return RequestReadResult::kBodyLimit;
    }
    if (error != asio::error::would_block && error != asio::error::try_again) {
      return RequestReadResult::kFailed;
    }
    error.clear();
    switch (WaitForSocket(stream->socket(), POLLIN, deadline, stop_token)) {
      case SocketWaitResult::kReady:
        break;
      case SocketWaitResult::kTimedOut:
        return RequestReadResult::kTimedOut;
      case SocketWaitResult::kCancelled:
        return RequestReadResult::kCancelled;
      case SocketWaitResult::kFailed:
        return RequestReadResult::kFailed;
    }
  }
  return RequestReadResult::kComplete;
}

bool WriteResponse(beast::tcp_stream* stream,
                   http::response<http::string_body>* response,
                   std::chrono::steady_clock::time_point deadline,
                   std::stop_token stop_token = {}) {
  boost::system::error_code error;
  stream->socket().non_blocking(true, error);
  if (error) {
    return false;
  }
  http::response_serializer<http::string_body> serializer(*response);
  while (!serializer.is_done()) {
    http::write_some(*stream, serializer, error);
    if (!error) {
      continue;
    }
    if (error != asio::error::would_block && error != asio::error::try_again) {
      return false;
    }
    error.clear();
    if (WaitForSocket(stream->socket(), POLLOUT, deadline, stop_token) !=
        SocketWaitResult::kReady) {
      return false;
    }
  }
  return true;
}

}  // namespace

struct McpServer::Impl {
  Impl(McpServerConfig server_config, McpProtocolConfig protocol_config,
       McpToolHandler tool_handler, McpResourceHandler resource_handler)
      : config(std::move(server_config)),
        protocol_config_(std::move(protocol_config)),
        protocol(protocol_config_, std::move(tool_handler),
                 std::move(resource_handler)),
        acceptor(io_context) {
    if (config.worker_count == 0U || config.worker_count > 64U) {
      throw std::runtime_error("MCP worker count must be in 1..64");
    }
    if (config.pending_connection_capacity == 0U ||
        config.pending_connection_capacity > 1024U) {
      throw std::runtime_error(
          "MCP pending connection capacity must be in 1..1024");
    }
    if (config.request_timeout <= std::chrono::milliseconds::zero()) {
      throw std::runtime_error("MCP request timeout must be positive");
    }
  }

  ~Impl() { Stop(); }

  void Start() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex);
    if (running.load(std::memory_order_acquire)) {
      throw std::runtime_error("MCP server is already running");
    }
    const asio::ip::address address =
        asio::ip::make_address(config.bind_address);
    if (!address.is_loopback()) {
      throw std::runtime_error("MCP server must bind a loopback address");
    }
    boost::system::error_code error;
    acceptor.open(address.is_v6() ? tcp::v6() : tcp::v4(), error);
    if (error) {
      throw std::runtime_error("open MCP listener failed: " + error.message());
    }
    acceptor.set_option(asio::socket_base::reuse_address(true), error);
    if (error) {
      acceptor.close();
      throw std::runtime_error("configure MCP listener failed: " +
                               error.message());
    }
    acceptor.bind(tcp::endpoint(address, config.port), error);
    if (error) {
      acceptor.close();
      throw std::runtime_error("bind MCP listener failed: " + error.message());
    }
    acceptor.listen(
        static_cast<int>(std::min<std::size_t>(
            config.pending_connection_capacity,
            static_cast<std::size_t>(std::numeric_limits<int>::max()))),
        error);
    if (error) {
      acceptor.close();
      throw std::runtime_error("listen on MCP endpoint failed: " +
                               error.message());
    }
    const std::uint16_t actual_port = acceptor.local_endpoint().port();
    bound_port.store(actual_port, std::memory_order_release);
    protocol_config_.endpoint_port = actual_port;
    protocol.SetEndpointPort(actual_port);
    running.store(true, std::memory_order_release);
    try {
      workers.reserve(config.worker_count);
      for (std::size_t index = 0U; index < config.worker_count; ++index) {
        workers.emplace_back(
            [this](std::stop_token stop_token) { WorkerLoop(stop_token); });
      }
      accept_thread = std::jthread(
          [this](std::stop_token stop_token) { AcceptLoop(stop_token); });
    } catch (...) {
      StopUnlocked();
      throw;
    }
  }

  void Stop() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex);
    StopUnlocked();
  }

  void StopUnlocked() {
    if (!running.exchange(false, std::memory_order_acq_rel) &&
        !accept_thread.joinable() && workers.empty()) {
      return;
    }
    if (accept_thread.joinable()) {
      accept_thread.request_stop();
    }
    for (std::jthread& worker : workers) {
      worker.request_stop();
    }
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      for (tcp::socket& socket : pending_connections) {
        CloseSocket(&socket);
      }
      pending_connections.clear();
    }
    queue_ready.notify_all();
    WakeAcceptor();
    if (accept_thread.joinable()) {
      accept_thread.join();
    }
    boost::system::error_code ignored;
    acceptor.cancel(ignored);
    acceptor.close(ignored);
    for (std::jthread& worker : workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers.clear();
    bound_port.store(0U, std::memory_order_release);
  }

  void WakeAcceptor() {
    const std::uint16_t actual_port =
        bound_port.load(std::memory_order_acquire);
    if (!acceptor.is_open() || actual_port == 0U) {
      return;
    }
    boost::system::error_code ignored;
    tcp::socket wake_socket(io_context);
    wake_socket.connect(
        tcp::endpoint(asio::ip::make_address(config.bind_address, ignored),
                      actual_port),
        ignored);
    CloseSocket(&wake_socket);
  }

  void RejectOverload(tcp::socket socket) {
    http::response<http::string_body> response{
        http::status::service_unavailable, 11};
    response.set(http::field::server, "bbp");
    response.set(http::field::content_type, "text/plain");
    response.set(http::field::retry_after, "1");
    response.keep_alive(false);
    response.body() = "MCP connection queue capacity reached";
    response.prepare_payload();
    beast::tcp_stream stream(std::move(socket));
    static_cast<void>(WriteResponse(
        &stream, &response,
        std::chrono::steady_clock::now() + config.request_timeout));
    CloseSocket(&stream.socket());
  }

  void AcceptLoop(std::stop_token stop_token) {
    while (!stop_token.stop_requested() &&
           running.load(std::memory_order_acquire)) {
      tcp::socket socket(io_context);
      boost::system::error_code error;
      acceptor.accept(socket, error);
      if (error) {
        if (!running.load(std::memory_order_acquire) ||
            stop_token.stop_requested() ||
            error == asio::error::operation_aborted ||
            error == asio::error::bad_descriptor) {
          break;
        }
        continue;
      }
      if (!running.load(std::memory_order_acquire) ||
          stop_token.stop_requested()) {
        CloseSocket(&socket);
        break;
      }
      accepted_connections.fetch_add(1U, std::memory_order_relaxed);
      bool queued = false;
      {
        std::lock_guard<std::mutex> lock(queue_mutex);
        if (pending_connections.size() < config.pending_connection_capacity) {
          pending_connections.push_back(std::move(socket));
          maximum_queued_connections =
              std::max(maximum_queued_connections, pending_connections.size());
          queued = true;
        }
      }
      if (queued) {
        queue_ready.notify_one();
      } else {
        rejected_connections.fetch_add(1U, std::memory_order_relaxed);
        RejectOverload(std::move(socket));
      }
    }
  }

  std::optional<tcp::socket> PopConnection(std::stop_token stop_token) {
    std::unique_lock<std::mutex> lock(queue_mutex);
    queue_ready.wait(lock, stop_token, [this] {
      return !pending_connections.empty() ||
             !running.load(std::memory_order_acquire);
    });
    if (pending_connections.empty()) {
      return std::nullopt;
    }
    tcp::socket socket = std::move(pending_connections.front());
    pending_connections.pop_front();
    return socket;
  }

  void WorkerLoop(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
      std::optional<tcp::socket> socket = PopConnection(stop_token);
      if (!socket) {
        if (!running.load(std::memory_order_acquire)) {
          break;
        }
        continue;
      }
      const std::uint64_t connection_id =
          next_connection_id.fetch_add(1U, std::memory_order_relaxed);
      auto stream = std::make_shared<beast::tcp_stream>(std::move(*socket));
      {
        std::lock_guard<std::mutex> lock(active_mutex);
        active_connections.emplace(connection_id, stream);
      }
      try {
        const auto deadline =
            std::chrono::steady_clock::now() + config.request_timeout;
        beast::flat_buffer buffer;
        http::request_parser<http::string_body> parser;
        parser.body_limit(kMcpMaximumRequestBytes);
        const RequestReadResult read_result =
            ReadRequest(stream.get(), &buffer, &parser, deadline, stop_token);
        if (read_result == RequestReadResult::kComplete &&
            running.load(std::memory_order_acquire) &&
            !stop_token.stop_requested()) {
          http::request<http::string_body> request = parser.release();
          http::response<http::string_body> response =
              protocol.Handle(request, stop_token);
          static_cast<void>(
              WriteResponse(stream.get(), &response, deadline, stop_token));
        } else if (read_result == RequestReadResult::kBodyLimit) {
          http::response<http::string_body> response{
              http::status::payload_too_large, 11};
          response.set(http::field::server, "bbp");
          response.set(http::field::content_type, "text/plain");
          response.keep_alive(false);
          response.body() = "MCP request exceeds retained byte limit";
          response.prepare_payload();
          static_cast<void>(
              WriteResponse(stream.get(), &response, deadline, stop_token));
        }
      } catch (const std::exception&) {
      }
      CloseSocket(&stream->socket());
      {
        std::lock_guard<std::mutex> lock(active_mutex);
        active_connections.erase(connection_id);
      }
      completed_connections.fetch_add(1U, std::memory_order_relaxed);
    }
  }

  McpServerStats GetStats() const {
    McpServerStats result;
    result.running = running.load(std::memory_order_acquire);
    result.accepted_connections =
        accepted_connections.load(std::memory_order_relaxed);
    result.rejected_connections =
        rejected_connections.load(std::memory_order_relaxed);
    result.completed_connections =
        completed_connections.load(std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      result.queued_connections = pending_connections.size();
      result.maximum_queued_connections = maximum_queued_connections;
    }
    {
      std::lock_guard<std::mutex> lock(active_mutex);
      result.active_connections = active_connections.size();
    }
    return result;
  }

  McpServerConfig config;
  McpProtocolConfig protocol_config_;
  McpProtocol protocol;
  asio::io_context io_context;
  tcp::acceptor acceptor;
  mutable std::mutex lifecycle_mutex;
  std::atomic<bool> running = false;
  std::atomic<std::uint16_t> bound_port = 0U;
  std::jthread accept_thread;
  std::vector<std::jthread> workers;
  mutable std::mutex queue_mutex;
  std::condition_variable_any queue_ready;
  std::deque<tcp::socket> pending_connections;
  std::size_t maximum_queued_connections = 0U;
  mutable std::mutex active_mutex;
  std::map<std::uint64_t, std::shared_ptr<beast::tcp_stream>>
      active_connections;
  std::atomic<std::uint64_t> next_connection_id = 1U;
  std::atomic<std::uint64_t> accepted_connections = 0U;
  std::atomic<std::uint64_t> rejected_connections = 0U;
  std::atomic<std::uint64_t> completed_connections = 0U;
};

McpServer::McpServer(McpServerConfig config, McpProtocolConfig protocol_config,
                     McpToolHandler tool_handler,
                     McpResourceHandler resource_handler)
    : impl_(std::make_unique<Impl>(
          std::move(config), std::move(protocol_config),
          std::move(tool_handler), std::move(resource_handler))) {}

McpServer::~McpServer() = default;

void McpServer::Start() { impl_->Start(); }

void McpServer::Stop() { impl_->Stop(); }

std::uint16_t McpServer::port() const {
  const std::uint16_t result =
      impl_->bound_port.load(std::memory_order_acquire);
  if (!impl_->running.load(std::memory_order_acquire) || result == 0U) {
    throw std::runtime_error("MCP server is not running");
  }
  return result;
}

std::string McpServer::endpoint() const {
  return "http://" + impl_->config.bind_address + ":" + std::to_string(port()) +
         impl_->protocol_config_.endpoint_path;
}

McpServerStats McpServer::Stats() const { return impl_->GetStats(); }

McpProtocol& McpServer::protocol() { return impl_->protocol; }

}  // namespace bbp
