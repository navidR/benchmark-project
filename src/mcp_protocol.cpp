#include "bbp/mcp_protocol.h"

#include <sys/random.h>

#include <algorithm>
#include <array>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/mcp_registry.h"

namespace bbp {
namespace {

namespace http = boost::beast::http;

constexpr std::string_view kJsonContentType = "application/json";
constexpr std::string_view kSseContentType = "text/event-stream";
constexpr std::string_view kSessionHeader = "Mcp-Session-Id";
constexpr std::string_view kProtocolHeader = "MCP-Protocol-Version";
constexpr std::size_t kMaximumPendingSessionCleanups = 2U * kMcpMaximumSessions;

struct McpNotification {
  std::uint64_t sequence = 0U;
  std::string json;
};

struct McpActiveRequest {
  std::stop_source stop_source;
};

enum class McpSessionState {
  kOpening,
  kAwaitingInitialization,
  kInitialized,
  kClosing,
};

struct McpSession {
  McpSessionState state = McpSessionState::kOpening;
  std::chrono::steady_clock::time_point initialization_deadline =
      std::chrono::steady_clock::time_point::max();
  std::string client_name;
  std::deque<McpNotification> notifications;
  std::uint64_t next_notification_sequence = 1U;
  std::map<std::string, std::shared_ptr<McpActiveRequest>, std::less<>>
      active_requests;
};

struct McpPendingSessionCleanup {
  std::string session_id;
  std::chrono::steady_clock::time_point retry_at;
  std::size_t failed_attempts = 0U;
  bool in_progress = false;
};

struct JsonRpcRequest {
  boost::json::value id;
  bool has_id = false;
  std::string method;
  boost::json::object params;
};

std::string HeaderValue(const http::request<http::string_body>& request,
                        std::string_view name) {
  const auto field = request.find(name);
  if (field == request.end()) {
    return {};
  }
  return std::string(field->value());
}

std::string LowerAscii(std::string_view value) {
  std::string lower(value);
  std::transform(lower.begin(), lower.end(), lower.begin(), [](char character) {
    if (character >= 'A' && character <= 'Z') {
      return static_cast<char>(character - 'A' + 'a');
    }
    return character;
  });
  return lower;
}

bool ContainsMediaType(std::string_view value, std::string_view media_type) {
  const std::string expected = LowerAscii(media_type);
  while (!value.empty()) {
    const std::size_t comma = value.find(',');
    std::string_view entry = value.substr(0U, comma);
    if (comma == std::string_view::npos) {
      value = {};
    } else {
      value.remove_prefix(comma + 1U);
    }
    const std::size_t semicolon = entry.find(';');
    entry = entry.substr(0U, semicolon);
    while (!entry.empty() && (entry.front() == ' ' || entry.front() == '\t')) {
      entry.remove_prefix(1U);
    }
    while (!entry.empty() && (entry.back() == ' ' || entry.back() == '\t')) {
      entry.remove_suffix(1U);
    }
    if (LowerAscii(entry) == expected) {
      return true;
    }
  }
  return false;
}

bool ConstantTimeEqual(std::string_view left, std::string_view right) {
  const std::size_t length = std::max(left.size(), right.size());
  std::size_t difference = left.size() ^ right.size();
  for (std::size_t index = 0U; index < length; ++index) {
    const unsigned char left_byte =
        index < left.size() ? static_cast<unsigned char>(left[index]) : 0U;
    const unsigned char right_byte =
        index < right.size() ? static_cast<unsigned char>(right[index]) : 0U;
    difference |= static_cast<std::size_t>(left_byte ^ right_byte);
  }
  return difference == 0U;
}

std::string RandomHex(std::size_t byte_count) {
  std::vector<unsigned char> bytes(byte_count);
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t received =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("getrandom for MCP session failed: " +
                               std::string(std::strerror(errno)));
    }
    if (received == 0) {
      throw std::runtime_error("getrandom for MCP session made no progress");
    }
    offset += static_cast<std::size_t>(received);
  }
  constexpr std::array<char, 16> kHex = {'0', '1', '2', '3', '4', '5',
                                         '6', '7', '8', '9', 'a', 'b',
                                         'c', 'd', 'e', 'f'};
  std::string result;
  result.reserve(bytes.size() * 2U);
  for (const unsigned char byte : bytes) {
    result.push_back(kHex[byte >> 4U]);
    result.push_back(kHex[byte & 0x0fU]);
  }
  return result;
}

std::string LoopbackOrigin(std::string_view host, std::uint16_t port) {
  std::string origin = "http://" + std::string(host);
  if (port != 80U) {
    origin += ":" + std::to_string(port);
  }
  return origin;
}

bool IsAllowedOrigin(std::string_view origin, std::uint16_t port) {
  if (origin.empty()) {
    return true;
  }
  if (port != 0U) {
    return origin == LoopbackOrigin("127.0.0.1", port) ||
           origin == LoopbackOrigin("localhost", port) ||
           origin == LoopbackOrigin("[::1]", port);
  }
  constexpr std::array<std::string_view, 3> kPrefixes = {
      "http://127.0.0.1:", "http://localhost:", "http://[::1]:"};
  return std::any_of(kPrefixes.begin(), kPrefixes.end(), [origin](auto prefix) {
    if (!origin.starts_with(prefix)) {
      return false;
    }
    const std::string_view port_text = origin.substr(prefix.size());
    std::uint16_t parsed = 0U;
    const auto [end, error] = std::from_chars(
        port_text.data(), port_text.data() + port_text.size(), parsed, 10);
    return error == std::errc{} && end == port_text.data() + port_text.size() &&
           parsed != 0U;
  });
}

http::response<http::string_body> TextResponse(
    const http::request<http::string_body>& request, http::status status,
    std::string body, std::string_view content_type = "text/plain") {
  http::response<http::string_body> response{status, request.version()};
  response.set(http::field::server, "bbp");
  response.set(http::field::content_type, content_type);
  response.keep_alive(false);
  response.body() = std::move(body);
  response.prepare_payload();
  return response;
}

boost::json::object JsonRpcError(boost::json::value id, int code,
                                 std::string message,
                                 boost::json::value data = nullptr) {
  boost::json::object error{{"code", code}, {"message", std::move(message)}};
  if (!data.is_null()) {
    error["data"] = std::move(data);
  }
  return boost::json::object{
      {"jsonrpc", "2.0"}, {"id", std::move(id)}, {"error", std::move(error)}};
}

boost::json::object JsonRpcResult(boost::json::value id,
                                  boost::json::value result) {
  return boost::json::object{
      {"jsonrpc", "2.0"}, {"id", std::move(id)}, {"result", std::move(result)}};
}

http::response<http::string_body> JsonResponse(
    const http::request<http::string_body>& request, boost::json::value body,
    http::status status = http::status::ok) {
  return TextResponse(request, status, boost::json::serialize(body),
                      kJsonContentType);
}

JsonRpcRequest ParseJsonRpcRequest(const boost::json::value& value) {
  if (!value.is_object()) {
    throw std::invalid_argument("JSON-RPC request must be an object");
  }
  const boost::json::object& object = value.as_object();
  const boost::json::value* version = object.if_contains("jsonrpc");
  const boost::json::value* method = object.if_contains("method");
  if (version == nullptr || !version->is_string() ||
      version->as_string() != "2.0" || method == nullptr ||
      !method->is_string() || method->as_string().empty()) {
    throw std::invalid_argument("invalid JSON-RPC 2.0 request");
  }
  JsonRpcRequest request;
  request.method = std::string(method->as_string());
  if (const boost::json::value* id = object.if_contains("id")) {
    if (!id->is_string() && !id->is_int64() && !id->is_uint64()) {
      throw std::invalid_argument("JSON-RPC id must be a string or integer");
    }
    request.id = *id;
    request.has_id = true;
  }
  if (const boost::json::value* params = object.if_contains("params")) {
    if (!params->is_object()) {
      throw std::invalid_argument("JSON-RPC params must be an object");
    }
    request.params = params->as_object();
  }
  return request;
}

bool IsJsonRpcNotificationEnvelope(const boost::json::value& value) {
  if (!value.is_object()) {
    return false;
  }
  const boost::json::object& object = value.as_object();
  const boost::json::value* version = object.if_contains("jsonrpc");
  const boost::json::value* method = object.if_contains("method");
  return version != nullptr && version->is_string() &&
         version->as_string() == "2.0" && method != nullptr &&
         method->is_string() && !method->as_string().empty() &&
         object.if_contains("id") == nullptr;
}

std::string JsonRpcRequestKey(const boost::json::value& id) {
  if (!id.is_string() && !id.is_int64() && !id.is_uint64()) {
    throw std::invalid_argument("JSON-RPC request id is invalid");
  }
  return boost::json::serialize(id);
}

std::optional<std::size_t> ParseCursor(const boost::json::object& params) {
  const boost::json::value* cursor = params.if_contains("cursor");
  if (cursor == nullptr) {
    return std::nullopt;
  }
  if (!cursor->is_string() || cursor->as_string().empty()) {
    throw std::invalid_argument("cursor must be a non-empty decimal string");
  }
  const std::string_view text(cursor->as_string().data(),
                              cursor->as_string().size());
  std::size_t value = 0U;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value, 10);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument("cursor must be a decimal list offset");
  }
  return value;
}

bool OperationAllowed(const McpProtocolConfig& config,
                      McpOperationKind operation) {
  return config.allowed_operations.empty() ||
         std::find(config.allowed_operations.begin(),
                   config.allowed_operations.end(),
                   operation) != config.allowed_operations.end();
}

bool InformationFamilyAllowed(const McpProtocolConfig& config,
                              McpInformationFamily family) {
  return config.allowed_information_families.empty() ||
         std::find(config.allowed_information_families.begin(),
                   config.allowed_information_families.end(),
                   family) != config.allowed_information_families.end();
}

std::vector<McpOperationKind> EffectiveOperations(
    const McpProtocolConfig& config) {
  if (!config.allowed_operations.empty()) {
    return config.allowed_operations;
  }
  std::vector<McpOperationKind> operations;
  operations.reserve(static_cast<std::size_t>(McpOperationKind::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(McpOperationKind::kCount); ++index) {
    operations.push_back(static_cast<McpOperationKind>(index));
  }
  return operations;
}

std::vector<McpInformationFamily> EffectiveInformationFamilies(
    const McpProtocolConfig& config) {
  if (!config.allowed_information_families.empty()) {
    return config.allowed_information_families;
  }
  std::vector<McpInformationFamily> information_families;
  information_families.reserve(
      static_cast<std::size_t>(McpInformationFamily::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(McpInformationFamily::kCount);
       ++index) {
    information_families.push_back(
        static_cast<McpInformationFamily>(index));
  }
  return information_families;
}

boost::json::object CapabilityDocument(const McpProtocolConfig& config) {
  const std::vector<McpOperationKind> operations = EffectiveOperations(config);
  const std::vector<McpInformationFamily> information_families =
      EffectiveInformationFamilies(config);
  boost::json::object document =
      BuildMcpCapabilityDocument(operations, information_families);
  document["access_mode"] = config.read_only ? "read_only" : "read_write";
  return document;
}

boost::json::object ToolPage(const boost::json::object& params,
                             const McpProtocolConfig& config) {
  const std::vector<McpOperationKind> operations = EffectiveOperations(config);
  const std::vector<McpInformationFamily> information_families =
      EffectiveInformationFamilies(config);
  const boost::json::array all =
      BuildMcpToolRegistry(operations, information_families);
  const std::size_t begin = ParseCursor(params).value_or(0U);
  if (begin > all.size()) {
    throw std::invalid_argument("tool cursor is out of range");
  }
  const std::size_t end = std::min(all.size(), begin + kMcpListPageSize);
  boost::json::array page;
  page.reserve(end - begin);
  for (std::size_t index = begin; index < end; ++index) {
    page.push_back(all[index]);
  }
  boost::json::object result{{"tools", std::move(page)}};
  if (end < all.size()) {
    result["nextCursor"] = std::to_string(end);
  }
  return result;
}

boost::json::object ResourcePage(const boost::json::object& params,
                                 const McpProtocolConfig& config) {
  const std::vector<McpInformationFamily> information_families =
      EffectiveInformationFamilies(config);
  const boost::json::array all =
      BuildMcpResourceRegistry(information_families);
  const std::size_t begin = ParseCursor(params).value_or(0U);
  if (begin > all.size()) {
    throw std::invalid_argument("resource cursor is out of range");
  }
  const std::size_t end = std::min(all.size(), begin + kMcpListPageSize);
  boost::json::array page;
  page.reserve(end - begin);
  for (std::size_t index = begin; index < end; ++index) {
    page.push_back(all[index]);
  }
  boost::json::object result{{"resources", std::move(page)}};
  if (end < all.size()) {
    result["nextCursor"] = std::to_string(end);
  }
  return result;
}

std::optional<McpOperationKind> RegisteredTool(std::string_view name) {
  const std::span<const McpNamedCapability> operations = McpOperationRegistry();
  const auto found = std::find_if(operations.begin(), operations.end(),
                                  [name](const McpNamedCapability& operation) {
                                    return operation.name == name;
                                  });
  if (found == operations.end()) {
    return std::nullopt;
  }
  return static_cast<McpOperationKind>(
      static_cast<std::size_t>(found - operations.begin()));
}

std::optional<McpInformationFamily> RegisteredResource(std::string_view uri) {
  constexpr std::string_view kPrefix = "bbp:///";
  if (!uri.starts_with(kPrefix)) {
    return std::nullopt;
  }
  const std::string_view name = uri.substr(kPrefix.size());
  const std::span<const McpNamedCapability> information_families =
      McpInformationFamilyRegistry();
  const auto found = std::find_if(
      information_families.begin(), information_families.end(),
      [name](const McpNamedCapability& family) { return family.name == name; });
  if (found == information_families.end()) {
    return std::nullopt;
  }
  return static_cast<McpInformationFamily>(
      static_cast<std::size_t>(found - information_families.begin()));
}

void ValidateSelectedInformationFamilies(
    McpOperationKind operation, const boost::json::object& arguments,
    const McpProtocolConfig& config) {
  if (operation != McpOperationKind::kQueryEvidence &&
      operation != McpOperationKind::kCreateSubscription) {
    return;
  }
  const boost::json::value* families = arguments.if_contains("families");
  if (families == nullptr || !families->is_array()) {
    return;
  }
  for (const boost::json::value& value : families->as_array()) {
    if (!value.is_string()) {
      continue;
    }
    const std::string uri =
        "bbp:///" + std::string(value.as_string().data(),
                                value.as_string().size());
    const std::optional<McpInformationFamily> family =
        RegisteredResource(uri);
    if (family && !InformationFamilyAllowed(config, *family)) {
      throw std::invalid_argument(
          "requested BBP information family is unavailable in the current "
          "endpoint");
    }
  }
}

std::string RequiredString(const boost::json::object& params,
                           std::string_view field) {
  const boost::json::value* value = params.if_contains(field);
  if (value == nullptr || !value->is_string() || value->as_string().empty()) {
    throw std::invalid_argument(std::string(field) +
                                " must be a non-empty string");
  }
  return std::string(value->as_string());
}

std::string RequiredClientName(const boost::json::object& params) {
  const boost::json::value* capabilities = params.if_contains("capabilities");
  if (capabilities == nullptr || !capabilities->is_object()) {
    throw std::invalid_argument("capabilities must be an object");
  }
  const boost::json::value* client = params.if_contains("clientInfo");
  if (client == nullptr || !client->is_object()) {
    throw std::invalid_argument("clientInfo must be an object");
  }
  const std::string name = RequiredString(client->as_object(), "name");
  const std::string version = RequiredString(client->as_object(), "version");
  if (name.size() > kMcpMaximumClientNameBytes ||
      version.size() > kMcpMaximumClientNameBytes) {
    throw std::invalid_argument("clientInfo name or version is too long");
  }
  return name;
}

}  // namespace

struct McpProtocol::Impl {
  class RequestRegistration {
   public:
    RequestRegistration(Impl* implementation, std::string session_id,
                        std::string request_key,
                        std::shared_ptr<McpActiveRequest> request)
        : implementation_(implementation),
          session_id_(std::move(session_id)),
          request_key_(std::move(request_key)),
          request_(std::move(request)) {}

    ~RequestRegistration() {
      implementation_->FinishRequest(session_id_, request_key_, request_);
    }

    RequestRegistration(const RequestRegistration&) = delete;
    RequestRegistration& operator=(const RequestRegistration&) = delete;

    std::stop_token stop_token() const {
      return request_->stop_source.get_token();
    }

    void RequestStop() { request_->stop_source.request_stop(); }

   private:
    Impl* implementation_;
    std::string session_id_;
    std::string request_key_;
    std::shared_ptr<McpActiveRequest> request_;
  };

  Impl(McpProtocolConfig config_value, McpToolHandler tool_handler_value,
       McpResourceHandler resource_handler_value,
       McpSessionHandler session_handler_value)
      : config(std::move(config_value)),
        tool_handler(std::move(tool_handler_value)),
        resource_handler(std::move(resource_handler_value)),
        session_handler(std::move(session_handler_value)) {
    if (config.bearer_token.size() < 32U || config.bearer_token.size() > 512U) {
      throw std::runtime_error("MCP bearer token must contain 32..512 bytes");
    }
    if (config.endpoint_path.empty() || config.endpoint_path.front() != '/' ||
        config.endpoint_path.find('?') != std::string::npos ||
        config.endpoint_path.find('#') != std::string::npos) {
      throw std::runtime_error("MCP endpoint path must be an absolute path");
    }
    if (config.uninitialized_session_timeout <=
        std::chrono::milliseconds::zero()) {
      throw std::runtime_error(
          "MCP uninitialized session timeout must be positive");
    }
    std::array<bool, static_cast<std::size_t>(McpOperationKind::kCount)> seen{};
    for (const McpOperationKind operation : config.allowed_operations) {
      const std::size_t index = static_cast<std::size_t>(operation);
      if (index >= seen.size() || seen[index]) {
        throw std::runtime_error(
            "MCP allowed operation list is invalid or contains duplicates");
      }
      seen[index] = true;
    }
    std::array<bool, static_cast<std::size_t>(McpInformationFamily::kCount)>
        seen_information{};
    for (const McpInformationFamily family :
         config.allowed_information_families) {
      const std::size_t index = static_cast<std::size_t>(family);
      if (index >= seen_information.size() || seen_information[index]) {
        throw std::runtime_error(
            "MCP allowed information family list is invalid or contains "
            "duplicates");
      }
      seen_information[index] = true;
    }
    if (config.read_only && config.allowed_operations.empty()) {
      throw std::runtime_error(
          "read-only MCP protocol requires an explicit operation list");
    }
    deadline_worker = std::jthread(
        [this](std::stop_token stop_token) { ExpirationLoop(stop_token); });
    cleanup_worker = std::jthread(
        [this](std::stop_token stop_token) { CleanupLoop(stop_token); });
  }

  ~Impl() {
    deadline_worker.request_stop();
    cleanup_worker.request_stop();
    state_changed.notify_all();
    if (deadline_worker.joinable()) {
      deadline_worker.join();
    }
    if (cleanup_worker.joinable()) {
      cleanup_worker.join();
    }

    std::vector<std::string> session_ids;
    {
      std::lock_guard<std::mutex> lock(mutex);
      session_ids.reserve(sessions.size() + pending_cleanup.size());
      for (const auto& [session_id, session] : sessions) {
        static_cast<void>(session);
        session_ids.push_back(session_id);
      }
      for (const McpPendingSessionCleanup& cleanup : pending_cleanup) {
        session_ids.push_back(cleanup.session_id);
      }
      session_ids.insert(session_ids.end(), failed_cleanup.begin(),
                         failed_cleanup.end());
      sessions.clear();
      pending_cleanup.clear();
      failed_cleanup.clear();
    }
    if (!session_handler) {
      return;
    }
    std::stop_source shutdown;
    shutdown.request_stop();
    for (const std::string& session_id : session_ids) {
      try {
        session_handler(session_id, false, shutdown.get_token());
      } catch (...) {
      }
    }
  }

  bool InvokeSessionClose(std::string_view session_id,
                          std::stop_token stop_token,
                          std::string* error_message = nullptr) {
    try {
      if (session_handler) {
        session_handler(session_id, false, stop_token);
      }
    } catch (const std::exception& error) {
      if (error_message != nullptr) {
        *error_message = error.what();
      }
      return false;
    } catch (...) {
      if (error_message != nullptr) {
        *error_message = "non-standard exception";
      }
      return false;
    }
    return true;
  }

  std::unique_ptr<RequestRegistration> RegisterRequest(
      std::string_view session_id, const boost::json::value& request_id) {
    const std::string request_key = JsonRpcRequestKey(request_id);
    auto request = std::make_shared<McpActiveRequest>();
    std::lock_guard<std::mutex> lock(mutex);
    const auto session = sessions.find(session_id);
    if (session == sessions.end() ||
        session->second.state != McpSessionState::kInitialized) {
      return {};
    }
    if (!session->second.active_requests
             .emplace(request_key, request)
             .second) {
      throw std::invalid_argument(
          "JSON-RPC request id is already active in this session");
    }
    return std::make_unique<RequestRegistration>(
        this, std::string(session_id), request_key, std::move(request));
  }

  void FinishRequest(
      std::string_view session_id, std::string_view request_key,
      const std::shared_ptr<McpActiveRequest>& expected_request) noexcept {
    try {
      std::lock_guard<std::mutex> lock(mutex);
      const auto session = sessions.find(session_id);
      if (session == sessions.end()) {
        return;
      }
      const auto request =
          session->second.active_requests.find(request_key);
      if (request != session->second.active_requests.end() &&
          request->second == expected_request) {
        session->second.active_requests.erase(request);
      }
      state_changed.notify_all();
    } catch (...) {
    }
  }

  void CancelRequest(std::string_view session_id,
                     const boost::json::object& params) {
    const boost::json::value* request_id = params.if_contains("requestId");
    if (request_id == nullptr) {
      throw std::invalid_argument(
          "notifications/cancelled requires requestId");
    }
    const std::string request_key = JsonRpcRequestKey(*request_id);
    std::shared_ptr<McpActiveRequest> request;
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto session = sessions.find(session_id);
      if (session == sessions.end()) {
        return;
      }
      const auto active = session->second.active_requests.find(request_key);
      if (active != session->second.active_requests.end()) {
        request = active->second;
      }
    }
    if (request) {
      request->stop_source.request_stop();
    }
  }

  bool FinishExplicitSessionClose(std::string_view session_id,
                                  std::stop_token stop_token,
                                  std::string* error_message) {
    if (!InvokeSessionClose(session_id, stop_token, error_message)) {
      std::lock_guard<std::mutex> lock(mutex);
      const auto session = sessions.find(session_id);
      if (session != sessions.end() &&
          session->second.state == McpSessionState::kClosing) {
        if (pending_cleanup.size() < kMaximumPendingSessionCleanups) {
          pending_cleanup.push_back(McpPendingSessionCleanup{
              .session_id = std::string(session_id),
              .retry_at =
                  std::chrono::steady_clock::now() + std::chrono::seconds(1),
              .failed_attempts = 1U,
              .in_progress = false});
        } else {
          ++stats.failed_session_cleanups;
          if (failed_cleanup.size() == kMcpMaximumSessions) {
            failed_cleanup.pop_front();
          }
          failed_cleanup.emplace_back(session_id);
        }
        sessions.erase(session);
        ++stats.terminated_sessions;
      }
      state_changed.notify_all();
      return false;
    }
    std::lock_guard<std::mutex> lock(mutex);
    const auto session = sessions.find(session_id);
    if (session == sessions.end()) {
      return false;
    }
    sessions.erase(session);
    ++stats.terminated_sessions;
    state_changed.notify_all();
    return true;
  }

  void ExpireSessionLocked(
      std::map<std::string, McpSession, std::less<>>::iterator session,
      std::chrono::steady_clock::time_point now) {
    if (session_handler) {
      if (pending_cleanup.size() < kMaximumPendingSessionCleanups) {
        pending_cleanup.push_back(
            McpPendingSessionCleanup{.session_id = session->first,
                                     .retry_at = now,
                                     .failed_attempts = 0U,
                                     .in_progress = false});
      } else {
        ++stats.failed_session_cleanups;
        if (failed_cleanup.size() == kMcpMaximumSessions) {
          failed_cleanup.pop_front();
        }
        failed_cleanup.push_back(session->first);
      }
    }
    sessions.erase(session);
    ++stats.terminated_sessions;
    ++stats.expired_sessions;
  }

  void ExpirationLoop(std::stop_token stop_token) {
    std::unique_lock<std::mutex> lock(mutex);
    while (!stop_token.stop_requested()) {
      const auto now = std::chrono::steady_clock::now();
      auto next_deadline = std::chrono::steady_clock::time_point::max();
      bool expired = false;
      for (auto session = sessions.begin(); session != sessions.end();) {
        if (session->second.state != McpSessionState::kAwaitingInitialization) {
          ++session;
          continue;
        }
        if (session->second.initialization_deadline > now) {
          next_deadline =
              std::min(next_deadline, session->second.initialization_deadline);
          ++session;
          continue;
        }
        auto current = session++;
        ExpireSessionLocked(current, now);
        expired = true;
      }
      if (expired) {
        state_changed.notify_all();
        continue;
      }
      if (next_deadline == std::chrono::steady_clock::time_point::max()) {
        static_cast<void>(state_changed.wait(lock, stop_token, [this] {
          return std::any_of(sessions.begin(), sessions.end(),
                             [](const auto& entry) {
                               return entry.second.state ==
                                      McpSessionState::kAwaitingInitialization;
                             });
        }));
      } else {
        const auto wake_at = next_deadline;
        static_cast<void>(state_changed.wait_until(
            lock, stop_token, wake_at, [this, wake_at] {
              return std::any_of(
                  sessions.begin(), sessions.end(),
                  [wake_at](const auto& entry) {
                    return entry.second.state ==
                               McpSessionState::kAwaitingInitialization &&
                           entry.second.initialization_deadline < wake_at;
                  });
            }));
      }
    }
  }

  void CleanupLoop(std::stop_token stop_token) {
    constexpr std::size_t kMaximumRuntimeAttempts = 2U;
    constexpr std::chrono::seconds kRetryInterval{1};
    std::unique_lock<std::mutex> lock(mutex);
    while (!stop_token.stop_requested()) {
      if (pending_cleanup.empty()) {
        static_cast<void>(state_changed.wait(
            lock, stop_token, [this] { return !pending_cleanup.empty(); }));
        continue;
      }
      const auto next =
          std::min_element(pending_cleanup.begin(), pending_cleanup.end(),
                           [](const McpPendingSessionCleanup& left,
                              const McpPendingSessionCleanup& right) {
                             if (left.in_progress != right.in_progress) {
                               return !left.in_progress;
                             }
                             return left.retry_at < right.retry_at;
                           });
      if (next == pending_cleanup.end() || next->in_progress) {
        static_cast<void>(state_changed.wait(lock, stop_token, [this] {
          return std::any_of(pending_cleanup.begin(), pending_cleanup.end(),
                             [](const McpPendingSessionCleanup& cleanup) {
                               return !cleanup.in_progress;
                             });
        }));
        continue;
      }
      const auto now = std::chrono::steady_clock::now();
      if (next->retry_at > now) {
        const auto wake_at = next->retry_at;
        static_cast<void>(state_changed.wait_until(
            lock, stop_token, wake_at, [this, wake_at] {
              return std::any_of(
                  pending_cleanup.begin(), pending_cleanup.end(),
                  [wake_at](const McpPendingSessionCleanup& cleanup) {
                    return !cleanup.in_progress && cleanup.retry_at < wake_at;
                  });
            }));
        continue;
      }
      next->in_progress = true;
      const std::string session_id = next->session_id;
      lock.unlock();
      const bool removed = InvokeSessionClose(session_id, stop_token);
      lock.lock();
      const auto cleanup =
          std::find_if(pending_cleanup.begin(), pending_cleanup.end(),
                       [&](const McpPendingSessionCleanup& entry) {
                         return entry.session_id == session_id;
                       });
      if (cleanup == pending_cleanup.end()) {
        continue;
      }
      if (removed) {
        pending_cleanup.erase(cleanup);
        continue;
      }
      ++cleanup->failed_attempts;
      cleanup->in_progress = false;
      if (stop_token.stop_requested()) {
        continue;
      }
      if (cleanup->failed_attempts < kMaximumRuntimeAttempts) {
        cleanup->retry_at = std::chrono::steady_clock::now() + kRetryInterval;
        continue;
      }
      ++stats.failed_session_cleanups;
      std::optional<std::string> final_attempt;
      if (failed_cleanup.size() == kMcpMaximumSessions) {
        final_attempt = std::move(failed_cleanup.front());
        failed_cleanup.pop_front();
      }
      failed_cleanup.push_back(session_id);
      pending_cleanup.erase(cleanup);
      if (final_attempt) {
        lock.unlock();
        static_cast<void>(InvokeSessionClose(*final_attempt, stop_token));
        lock.lock();
      }
    }
  }

  bool Authenticate(const http::request<http::string_body>& request) {
    const std::string authorization = HeaderValue(request, "Authorization");
    const std::string expected = "Bearer " + config.bearer_token;
    if (!ConstantTimeEqual(authorization, expected)) {
      std::lock_guard<std::mutex> lock(mutex);
      ++stats.authentication_failures;
      return false;
    }
    return true;
  }

  bool ValidateOrigin(const http::request<http::string_body>& request) {
    std::uint16_t endpoint_port = 0U;
    {
      std::lock_guard<std::mutex> lock(mutex);
      endpoint_port = config.endpoint_port;
    }
    if (!IsAllowedOrigin(HeaderValue(request, "Origin"), endpoint_port)) {
      std::lock_guard<std::mutex> lock(mutex);
      ++stats.origin_failures;
      return false;
    }
    return true;
  }

  http::response<http::string_body> RejectAuthentication(
      const http::request<http::string_body>& request) {
    auto response = TextResponse(request, http::status::unauthorized,
                                 "MCP bearer authentication required");
    response.set(http::field::www_authenticate, "Bearer");
    return response;
  }

  std::optional<http::response<http::string_body>> ValidateHttpRequest(
      const http::request<http::string_body>& request) {
    if (request.target() != config.endpoint_path) {
      return TextResponse(request, http::status::not_found,
                          "MCP endpoint not found");
    }
    if (!ValidateOrigin(request)) {
      return TextResponse(
          request, http::status::forbidden,
          "MCP request Origin is not the bound loopback endpoint");
    }
    if (!Authenticate(request)) {
      return RejectAuthentication(request);
    }
    return std::nullopt;
  }

  std::optional<std::string> RequireSession(
      const http::request<http::string_body>& request,
      bool require_initialized) {
    const std::string session_id = HeaderValue(request, kSessionHeader);
    if (session_id.empty()) {
      return std::nullopt;
    }
    std::unique_lock<std::mutex> lock(mutex);
    const auto session = sessions.find(session_id);
    if (session == sessions.end() ||
        session->second.state == McpSessionState::kOpening ||
        session->second.state == McpSessionState::kClosing) {
      return std::nullopt;
    }
    if (session->second.state == McpSessionState::kAwaitingInitialization &&
        std::chrono::steady_clock::now() >=
            session->second.initialization_deadline) {
      ExpireSessionLocked(session, std::chrono::steady_clock::now());
      lock.unlock();
      state_changed.notify_all();
      return std::nullopt;
    }
    if (require_initialized &&
        session->second.state != McpSessionState::kInitialized) {
      return std::nullopt;
    }
    return session_id;
  }

  bool ValidProtocolVersion(
      const http::request<http::string_body>& request) const {
    return HeaderValue(request, kProtocolHeader) == kMcpProtocolVersion;
  }

  std::optional<std::string> CreateSession(std::string client_name) {
    std::lock_guard<std::mutex> lock(mutex);
    if (sessions.size() >= kMcpMaximumSessions ||
        pending_cleanup.size() > kMcpMaximumSessions) {
      ++stats.rejected_sessions;
      return std::nullopt;
    }
    std::string id;
    do {
      id = RandomHex(24U);
    } while (sessions.contains(id) ||
             std::any_of(pending_cleanup.begin(), pending_cleanup.end(),
                         [&](const McpPendingSessionCleanup& cleanup) {
                           return cleanup.session_id == id;
                         }) ||
             std::find(failed_cleanup.begin(), failed_cleanup.end(), id) !=
                 failed_cleanup.end());
    sessions.emplace(
        id, McpSession{.state = McpSessionState::kOpening,
                       .initialization_deadline =
                           std::chrono::steady_clock::time_point::max(),
                       .client_name = std::move(client_name),
                       .notifications = {},
                       .next_notification_sequence = 1U,
                       .active_requests = {}});
    stats.maximum_sessions = std::max(stats.maximum_sessions, sessions.size());
    return id;
  }

  void ArmInitializationDeadline(std::string_view session_id) {
    std::lock_guard<std::mutex> lock(mutex);
    const auto session = sessions.find(session_id);
    if (session == sessions.end() ||
        session->second.state != McpSessionState::kOpening) {
      throw std::logic_error("MCP session ended while opening");
    }
    session->second.state = McpSessionState::kAwaitingInitialization;
    session->second.initialization_deadline =
        std::chrono::steady_clock::now() + config.uninitialized_session_timeout;
    state_changed.notify_all();
  }

  bool MarkInitialized(std::string_view session_id) {
    std::unique_lock<std::mutex> lock(mutex);
    const auto session = sessions.find(session_id);
    if (session == sessions.end()) {
      return false;
    }
    if (session->second.state == McpSessionState::kInitialized) {
      return true;
    }
    if (session->second.state != McpSessionState::kAwaitingInitialization) {
      return false;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= session->second.initialization_deadline) {
      ExpireSessionLocked(session, now);
      lock.unlock();
      state_changed.notify_all();
      return false;
    }
    session->second.state = McpSessionState::kInitialized;
    ++stats.initialized_sessions;
    lock.unlock();
    state_changed.notify_all();
    return true;
  }

  boost::json::object InitializeResult() const {
    boost::json::object capabilities;
    capabilities["logging"] = boost::json::object{};
    capabilities["prompts"] = boost::json::object{{"listChanged", false}};
    capabilities["resources"] =
        boost::json::object{{"subscribe", false}, {"listChanged", false}};
    capabilities["tools"] = boost::json::object{{"listChanged", false}};
    capabilities["completions"] = boost::json::object{};
    capabilities["experimental"] =
        boost::json::object{{"bbp", CapabilityDocument(config)}};
    return boost::json::object{
        {"protocolVersion", kMcpProtocolVersion},
        {"capabilities", std::move(capabilities)},
        {"serverInfo", boost::json::object{{"name", "bbp"}, {"version", "1"}}},
        {"instructions",
         "Use typed BBP tools and resources; direct host or daemon access is "
         "outside this endpoint."}};
  }

  boost::json::value ReadResource(std::string_view uri,
                                  std::string_view session_id,
                                  std::stop_token stop_token) {
    if (uri == "bbp:///capabilities") {
      return CapabilityDocument(config);
    }
    if (uri == "bbp:///schemas") {
      const boost::json::object capabilities = CapabilityDocument(config);
      return boost::json::object{
          {"scenario", BuildMcpScenarioSchema()},
          {"tools",
           BuildMcpToolRegistry(EffectiveOperations(config),
                                EffectiveInformationFamilies(config))},
          {"resources",
           BuildMcpResourceRegistry(EffectiveInformationFamilies(config))},
          {"results", capabilities.at("result_families")}};
    }
    if (!resource_handler) {
      throw std::runtime_error("resource is not available in the current run");
    }
    return resource_handler(uri, session_id, stop_token);
  }

  boost::json::value Dispatch(const JsonRpcRequest& request,
                              std::string_view session_id,
                              std::stop_token stop_token) {
    if (request.method == "ping") {
      return boost::json::object{};
    }
    if (request.method == "tools/list") {
      return ToolPage(request.params, config);
    }
    if (request.method == "resources/list") {
      return ResourcePage(request.params, config);
    }
    if (request.method == "prompts/list") {
      return boost::json::object{{"prompts", boost::json::array{}}};
    }
    if (request.method == "completion/complete") {
      return boost::json::object{
          {"completion", boost::json::object{{"values", boost::json::array{}},
                                             {"total", 0U},
                                             {"hasMore", false}}}};
    }
    if (request.method == "logging/setLevel") {
      static_cast<void>(RequiredString(request.params, "level"));
      return boost::json::object{};
    }
    if (request.method == "resources/read") {
      const std::string uri = RequiredString(request.params, "uri");
      const std::optional<McpInformationFamily> family =
          RegisteredResource(uri);
      if (!family) {
        throw std::invalid_argument("unknown BBP resource URI");
      }
      if (!InformationFamilyAllowed(config, *family)) {
        throw std::invalid_argument(
            "BBP resource is unavailable in the current endpoint");
      }
      boost::json::value value = ReadResource(uri, session_id, stop_token);
      return boost::json::object{
          {"contents", boost::json::array{boost::json::object{
                           {"uri", uri},
                           {"mimeType", "application/json"},
                           {"text", boost::json::serialize(value)}}}}};
    }
    if (request.method == "tools/call") {
      const std::string name = RequiredString(request.params, "name");
      const std::optional<McpOperationKind> operation = RegisteredTool(name);
      if (!operation) {
        throw std::invalid_argument("unknown BBP tool");
      }
      if (!OperationAllowed(config, *operation)) {
        throw std::invalid_argument(
            config.read_only
                ? "BBP tool is unavailable for a retained read-only run"
                : "BBP tool is unavailable in the current endpoint");
      }
      boost::json::object arguments;
      if (const boost::json::value* value =
              request.params.if_contains("arguments")) {
        if (!value->is_object()) {
          throw std::invalid_argument("tool arguments must be an object");
        }
        arguments = value->as_object();
      }
      ValidateSelectedInformationFamilies(*operation, arguments, config);
      boost::json::value result;
      try {
        if (!tool_handler) {
          throw std::runtime_error("tool is not available in the current run");
        }
        result = tool_handler(name, arguments, session_id, stop_token);
      } catch (const std::exception& error) {
        boost::json::object structured_error{{"kind", "tool_error"},
                                             {"message", error.what()}};
        return boost::json::object{
            {"content",
             boost::json::array{boost::json::object{
                 {"type", "text"},
                 {"text", boost::json::serialize(structured_error)}}}},
            {"structuredContent", std::move(structured_error)},
            {"isError", true}};
      }
      return boost::json::object{
          {"content",
           boost::json::array{boost::json::object{
               {"type", "text"}, {"text", boost::json::serialize(result)}}}},
          {"structuredContent", std::move(result)},
          {"isError", false}};
    }
    throw std::out_of_range("method not found");
  }

  http::response<http::string_body> HandleInitialize(
      const http::request<http::string_body>& http_request,
      const JsonRpcRequest& request, std::stop_token stop_token) {
    if (!request.has_id) {
      return JsonResponse(
          http_request, JsonRpcError(nullptr, -32600,
                                     "initialize must be a JSON-RPC request"));
    }
    if (!HeaderValue(http_request, kSessionHeader).empty()) {
      return JsonResponse(http_request,
                          JsonRpcError(request.id, -32600,
                                       "initialize must not reuse a session"));
    }
    const std::string requested_version =
        RequiredString(request.params, "protocolVersion");
    if (requested_version != kMcpProtocolVersion) {
      return JsonResponse(
          http_request,
          JsonRpcError(request.id, -32602, "unsupported MCP protocol version",
                       boost::json::object{{"supported", kMcpProtocolVersion},
                                           {"requested", requested_version}}));
    }
    const std::string client_name = RequiredClientName(request.params);
    const std::optional<std::string> session_id = CreateSession(client_name);
    if (!session_id) {
      return JsonResponse(
          http_request,
          JsonRpcError(request.id, -32001, "MCP session capacity reached"),
          http::status::service_unavailable);
    }
    if (session_handler) {
      try {
        session_handler(*session_id, true, stop_token);
      } catch (const std::exception& error) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          sessions.erase(*session_id);
        }
        return JsonResponse(
            http_request,
            JsonRpcError(request.id, -32001,
                         "MCP application session capacity reached",
                         error.what()),
            http::status::service_unavailable);
      } catch (...) {
        {
          std::lock_guard<std::mutex> lock(mutex);
          sessions.erase(*session_id);
        }
        return JsonResponse(
            http_request,
            JsonRpcError(request.id, -32603, "internal error",
                         "non-standard callback exception"));
      }
    }
    ArmInitializationDeadline(*session_id);
    auto response = JsonResponse(http_request,
                                 JsonRpcResult(request.id, InitializeResult()));
    response.set(kSessionHeader, *session_id);
    response.set(kProtocolHeader, kMcpProtocolVersion);
    return response;
  }

  http::response<http::string_body> HandlePost(
      const http::request<http::string_body>& http_request,
      std::stop_token stop_token) {
    if (!ContainsMediaType(HeaderValue(http_request, "Content-Type"),
                           kJsonContentType)) {
      return TextResponse(http_request, http::status::unsupported_media_type,
                          "MCP POST requires application/json");
    }
    const std::string accept = HeaderValue(http_request, "Accept");
    if (!ContainsMediaType(accept, kJsonContentType) ||
        !ContainsMediaType(accept, kSseContentType)) {
      return TextResponse(http_request, http::status::not_acceptable,
                          "MCP POST Accept must include application/json and "
                          "text/event-stream");
    }
    boost::json::value parsed;
    try {
      parsed = boost::json::parse(http_request.body());
    } catch (const std::exception&) {
      std::lock_guard<std::mutex> lock(mutex);
      ++stats.malformed_requests;
      return JsonResponse(http_request,
                          JsonRpcError(nullptr, -32700, "parse error"));
    }
    JsonRpcRequest request;
    try {
      request = ParseJsonRpcRequest(parsed);
    } catch (const std::exception& error) {
      {
        std::lock_guard<std::mutex> lock(mutex);
        ++stats.malformed_requests;
      }
      if (IsJsonRpcNotificationEnvelope(parsed)) {
        return TextResponse(http_request, http::status::accepted, "");
      }
      return JsonResponse(
          http_request,
          JsonRpcError(nullptr, -32600, "invalid request", error.what()));
    }
    if (request.method == "initialize") {
      try {
        return HandleInitialize(http_request, request, stop_token);
      } catch (const std::invalid_argument& error) {
        return JsonResponse(
            http_request,
            JsonRpcError(request.id, -32602, "invalid params", error.what()));
      }
    }
    if (!ValidProtocolVersion(http_request)) {
      return TextResponse(http_request, http::status::bad_request,
                          "missing or unsupported MCP-Protocol-Version");
    }
    const std::optional<std::string> session_id =
        RequireSession(http_request, false);
    if (!session_id) {
      return TextResponse(http_request, http::status::not_found,
                          "unknown MCP session");
    }
    if (request.method == "notifications/initialized") {
      if (request.has_id) {
        return JsonResponse(
            http_request,
            JsonRpcError(request.id, -32600,
                         "notifications/initialized must not have an id"));
      }
      if (!MarkInitialized(*session_id)) {
        return TextResponse(http_request, http::status::not_found,
                            "unknown MCP session");
      }
      return TextResponse(http_request, http::status::accepted, "");
    }
    if (!RequireSession(http_request, true)) {
      if (!RequireSession(http_request, false)) {
        return TextResponse(http_request, http::status::not_found,
                            "unknown MCP session");
      }
      if (!request.has_id) {
        return TextResponse(http_request, http::status::accepted, "");
      }
      return JsonResponse(
          http_request,
          JsonRpcError(
              request.id, -32002,
              "MCP session has not received notifications/initialized"));
    }
    if (request.method == "notifications/cancelled") {
      if (request.has_id) {
        return JsonResponse(
            http_request,
            JsonRpcError(request.id, -32600,
                         "notifications/cancelled must not have an id"));
      }
      try {
        CancelRequest(*session_id, request.params);
      } catch (const std::invalid_argument&) {
      }
      return TextResponse(http_request, http::status::accepted, "");
    }
    if (!request.has_id) {
      return TextResponse(http_request, http::status::accepted, "");
    }
    std::unique_ptr<RequestRegistration> active_request;
    try {
      active_request = RegisterRequest(*session_id, request.id);
    } catch (const std::invalid_argument& error) {
      return JsonResponse(
          http_request,
          JsonRpcError(request.id, -32600, "invalid request", error.what()));
    }
    if (!active_request) {
      return TextResponse(http_request, http::status::not_found,
                          "unknown MCP session");
    }
    std::stop_callback stop_on_transport(
        stop_token, [&] { active_request->RequestStop(); });
    try {
      return JsonResponse(http_request,
                          JsonRpcResult(request.id,
                                        Dispatch(request, *session_id,
                                                 active_request->stop_token())));
    } catch (const std::invalid_argument& error) {
      return JsonResponse(
          http_request,
          JsonRpcError(request.id, -32602, "invalid params", error.what()));
    } catch (const std::out_of_range&) {
      return JsonResponse(http_request,
                          JsonRpcError(request.id, -32601, "method not found"));
    } catch (const std::exception& error) {
      return JsonResponse(
          http_request,
          JsonRpcError(request.id, -32603, "internal error", error.what()));
    } catch (...) {
      return JsonResponse(
          http_request,
          JsonRpcError(request.id, -32603, "internal error",
                       "non-standard callback exception"));
    }
  }

  http::response<http::string_body> HandleGet(
      const http::request<http::string_body>& request) {
    if (!ContainsMediaType(HeaderValue(request, "Accept"), kSseContentType)) {
      return TextResponse(request, http::status::not_acceptable,
                          "MCP GET requires Accept: text/event-stream");
    }
    if (!ValidProtocolVersion(request)) {
      return TextResponse(request, http::status::bad_request,
                          "missing or unsupported MCP-Protocol-Version");
    }
    const std::optional<std::string> session_id = RequireSession(request, true);
    if (!session_id) {
      return TextResponse(request, http::status::not_found,
                          "unknown or uninitialized MCP session");
    }
    std::uint64_t after = 0U;
    const std::string last_event = HeaderValue(request, "Last-Event-ID");
    if (!last_event.empty()) {
      const auto [end, error] = std::from_chars(
          last_event.data(), last_event.data() + last_event.size(), after, 10);
      if (error != std::errc{} ||
          end != last_event.data() + last_event.size()) {
        return TextResponse(request, http::status::bad_request,
                            "Last-Event-ID must be a decimal sequence");
      }
    }
    std::string body;
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto session = sessions.find(*session_id);
      if (session == sessions.end()) {
        return TextResponse(request, http::status::not_found,
                            "unknown MCP session");
      }
      for (const McpNotification& notification :
           session->second.notifications) {
        if (notification.sequence <= after) {
          continue;
        }
        body += "id: " + std::to_string(notification.sequence) + "\n";
        body += "event: message\n";
        body += "data: " + notification.json + "\n\n";
      }
    }
    if (body.empty()) {
      body = ": bbp keepalive\n\n";
    }
    auto response = TextResponse(request, http::status::ok, std::move(body),
                                 kSseContentType);
    response.set(http::field::cache_control, "no-cache");
    return response;
  }

  http::response<http::string_body> HandleDelete(
      const http::request<http::string_body>& request,
      std::stop_token stop_token) {
    if (!ValidProtocolVersion(request)) {
      return TextResponse(request, http::status::bad_request,
                          "missing or unsupported MCP-Protocol-Version");
    }
    const std::string session_id = HeaderValue(request, kSessionHeader);
    if (session_id.empty()) {
      return TextResponse(request, http::status::not_found,
                          "unknown MCP session");
    }
    std::vector<std::shared_ptr<McpActiveRequest>> active_requests;
    {
      std::unique_lock<std::mutex> lock(mutex);
      const auto session = sessions.find(session_id);
      if (session == sessions.end() ||
          session->second.state == McpSessionState::kOpening ||
          session->second.state == McpSessionState::kClosing) {
        return TextResponse(request, http::status::not_found,
                            "unknown MCP session");
      }
      if (session->second.state == McpSessionState::kAwaitingInitialization &&
          std::chrono::steady_clock::now() >=
              session->second.initialization_deadline) {
        ExpireSessionLocked(session, std::chrono::steady_clock::now());
        lock.unlock();
        state_changed.notify_all();
        return TextResponse(request, http::status::not_found,
                            "unknown MCP session");
      }
      session->second.state = McpSessionState::kClosing;
      active_requests.reserve(session->second.active_requests.size());
      for (const auto& [request_key, active_request] :
           session->second.active_requests) {
        static_cast<void>(request_key);
        active_requests.push_back(active_request);
      }
    }
    for (const auto& active_request : active_requests) {
      active_request->stop_source.request_stop();
    }
    std::string cleanup_error;
    if (!FinishExplicitSessionClose(session_id, stop_token, &cleanup_error)) {
      if (!cleanup_error.empty()) {
        return TextResponse(request, http::status::service_unavailable,
                            "MCP session cleanup incomplete: " + cleanup_error);
      }
      return TextResponse(request, http::status::not_found,
                          "unknown MCP session");
    }
    return TextResponse(request, http::status::ok, "MCP session terminated");
  }

  McpProtocolConfig config;
  McpToolHandler tool_handler;
  McpResourceHandler resource_handler;
  McpSessionHandler session_handler;
  mutable std::mutex mutex;
  std::condition_variable_any state_changed;
  std::map<std::string, McpSession, std::less<>> sessions;
  std::vector<McpPendingSessionCleanup> pending_cleanup;
  std::deque<std::string> failed_cleanup;
  McpProtocolStats stats;
  std::jthread deadline_worker;
  std::jthread cleanup_worker;
};

McpProtocol::McpProtocol(McpProtocolConfig config, McpToolHandler tool_handler,
                         McpResourceHandler resource_handler,
                         McpSessionHandler session_handler)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(tool_handler),
                                   std::move(resource_handler),
                                   std::move(session_handler))) {}

McpProtocol::~McpProtocol() = default;

http::response<http::string_body> McpProtocol::Handle(
    const http::request<http::string_body>& request,
    std::stop_token stop_token) {
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ++impl_->stats.requests;
  }
  std::optional<http::response<http::string_body>> rejection =
      impl_->ValidateHttpRequest(request);
  if (rejection) {
    return std::move(*rejection);
  }
  switch (request.method()) {
    case http::verb::post:
      return impl_->HandlePost(request, stop_token);
    case http::verb::get:
      return impl_->HandleGet(request);
    case http::verb::delete_:
      return impl_->HandleDelete(request, stop_token);
    default:
      return TextResponse(request, http::status::method_not_allowed,
                          "MCP endpoint supports POST, GET, and DELETE");
  }
}

void McpProtocol::EnqueueNotification(std::string_view session_id,
                                      std::string_view method,
                                      boost::json::value params) {
  if (method.empty()) {
    throw std::runtime_error("MCP notification method must not be empty");
  }
  boost::json::object notification{
      {"jsonrpc", "2.0"}, {"method", method}, {"params", std::move(params)}};
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto session = impl_->sessions.find(session_id);
  if (session == impl_->sessions.end()) {
    throw std::runtime_error("unknown MCP notification session");
  }
  McpSession& target = session->second;
  if (target.notifications.size() == kMcpMaximumNotificationsPerSession) {
    target.notifications.pop_front();
    ++impl_->stats.notifications_dropped;
  }
  target.notifications.push_back(
      McpNotification{.sequence = target.next_notification_sequence++,
                      .json = boost::json::serialize(notification)});
  ++impl_->stats.notifications_enqueued;
}

void McpProtocol::BroadcastNotification(std::string_view method,
                                        const boost::json::value& params) {
  if (method.empty()) {
    throw std::runtime_error("MCP notification method must not be empty");
  }
  boost::json::object notification{
      {"jsonrpc", "2.0"}, {"method", method}, {"params", params}};
  const std::string json = boost::json::serialize(notification);
  std::lock_guard<std::mutex> lock(impl_->mutex);
  for (auto& [id, session] : impl_->sessions) {
    static_cast<void>(id);
    if (session.state != McpSessionState::kInitialized) {
      continue;
    }
    if (session.notifications.size() == kMcpMaximumNotificationsPerSession) {
      session.notifications.pop_front();
      ++impl_->stats.notifications_dropped;
    }
    session.notifications.push_back(McpNotification{
        .sequence = session.next_notification_sequence++, .json = json});
    ++impl_->stats.notifications_enqueued;
  }
}

void McpProtocol::SetEndpointPort(std::uint16_t endpoint_port) {
  if (endpoint_port == 0U) {
    throw std::runtime_error("MCP endpoint port must be nonzero");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->config.endpoint_port = endpoint_port;
}

McpProtocolStats McpProtocol::Stats() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  McpProtocolStats result = impl_->stats;
  result.sessions = impl_->sessions.size();
  return result;
}

}  // namespace bbp
