#pragma once

#include <chrono>
#include <cstdint>
#include <stop_token>
#include <string>
#include <string_view>

namespace bsim {

struct HttpResponse {
  int status = 0;
  std::string body;
};

struct RpcEndpoint {
  std::string host = "127.0.0.1";
  uint16_t port = 0;
  std::string user;
  std::string password;
};

class HttpClient {
 public:
  explicit HttpClient(std::chrono::milliseconds timeout) : timeout_(timeout) {}

  HttpResponse PostJson(const RpcEndpoint& endpoint, std::string_view path,
                        std::string_view body,
                        std::stop_token stop_token = {}) const;

 private:
  std::chrono::milliseconds timeout_;
};

}  // namespace bsim
