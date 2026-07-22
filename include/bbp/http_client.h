#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

struct HttpResponse {
  int status = 0;
  std::string body;
};

enum class RpcAuthenticationMode {
  kBasic,
  kCookieFile,
  kDigest,
};

struct RpcEndpoint {
  std::string host = "127.0.0.1";
  uint16_t port = 0;
  RpcAuthenticationMode authentication = RpcAuthenticationMode::kBasic;
  std::string user;
  std::string password;
  std::filesystem::path cookie_file;
};

class HttpClient {
 public:
  explicit HttpClient(std::chrono::milliseconds timeout) : timeout_(timeout) {}

  HttpResponse PostJson(const RpcEndpoint& endpoint, std::string_view path,
                        std::string_view body,
                        std::stop_token stop_token = {}) const;
  HttpResponse PostJsonUntil(const RpcEndpoint& endpoint, std::string_view path,
                             std::string_view body,
                             std::chrono::steady_clock::time_point deadline,
                             std::stop_token stop_token = {}) const;

 private:
  HttpResponse PostJsonWithDeadline(
      const RpcEndpoint& endpoint, std::string_view path, std::string_view body,
      std::chrono::steady_clock::time_point deadline,
      std::stop_token stop_token) const;

  std::chrono::milliseconds timeout_;
  mutable std::mutex digest_mutex_;
};

}  // namespace bbp
