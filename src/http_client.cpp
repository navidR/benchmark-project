#include "bbp/http_client.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/system/system_error.hpp>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/simulation_cancelled.h"

namespace bbp {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

std::string Base64Encode(const std::string& input) {
  std::string output;
  output.resize(beast::detail::base64::encoded_size(input.size()));
  const auto size =
      beast::detail::base64::encode(output.data(), input.data(), input.size());
  output.resize(size);
  return output;
}

class UniqueFd {
 public:
  explicit UniqueFd(int fd) : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  ~UniqueFd() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  int get() const { return fd_; }

 private:
  int fd_ = -1;
};

struct RpcCredentials {
  std::string user;
  std::string password;
};

std::runtime_error CredentialFileError(const std::filesystem::path& path,
                                       std::string_view detail) {
  return std::runtime_error("RPC credential file " + path.string() + " " +
                            std::string(detail));
}

RpcCredentials ReadCookieCredentials(const std::filesystem::path& path) {
  if (path.empty()) {
    throw std::runtime_error("RPC cookie authentication requires a file");
  }
  const int raw_fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (raw_fd < 0) {
    throw CredentialFileError(
        path, std::string("open failed: ") + std::strerror(errno));
  }
  const UniqueFd fd(raw_fd);
  struct stat status {};
  if (fstat(fd.get(), &status) != 0) {
    throw CredentialFileError(
        path, std::string("stat failed: ") + std::strerror(errno));
  }
  if (!S_ISREG(status.st_mode)) {
    throw CredentialFileError(path, "is not a regular file");
  }
  if (status.st_uid != geteuid()) {
    throw CredentialFileError(path, "is not owned by the effective user");
  }
  if ((status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
    throw CredentialFileError(path, "has group or other permissions");
  }

  constexpr std::size_t kMaximumCredentialBytes = 1024U;
  std::string credential;
  char buffer[256];
  while (true) {
    const ssize_t received = read(fd.get(), buffer, sizeof(buffer));
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw CredentialFileError(
          path, std::string("read failed: ") + std::strerror(errno));
    }
    if (received == 0) {
      break;
    }
    const std::size_t size = static_cast<std::size_t>(received);
    if (size > kMaximumCredentialBytes - credential.size()) {
      throw CredentialFileError(path, "exceeds 1024 bytes");
    }
    credential.append(buffer, size);
  }

  const std::size_t separator = credential.find(':');
  if (separator == std::string::npos || separator == 0U ||
      separator + 1U == credential.size()) {
    throw CredentialFileError(path, "has malformed contents");
  }
  for (const unsigned char character : credential) {
    if (character < 0x20U || character == 0x7fU) {
      throw CredentialFileError(path, "has malformed contents");
    }
  }
  return RpcCredentials{.user = credential.substr(0U, separator),
                        .password = credential.substr(separator + 1U)};
}

RpcCredentials ResolveCredentials(const RpcEndpoint& endpoint) {
  switch (endpoint.authentication) {
    case RpcAuthenticationMode::kBasic:
      if (endpoint.user.empty() || endpoint.password.empty()) {
        throw std::runtime_error(
            "RPC basic authentication requires a user and password");
      }
      if (!endpoint.cookie_file.empty()) {
        throw std::runtime_error(
            "RPC basic authentication must not specify a cookie file");
      }
      return RpcCredentials{.user = endpoint.user,
                            .password = endpoint.password};
    case RpcAuthenticationMode::kCookieFile:
      if (!endpoint.user.empty() || !endpoint.password.empty()) {
        throw std::runtime_error(
            "RPC cookie authentication must not specify inline credentials");
      }
      return ReadCookieCredentials(endpoint.cookie_file);
  }
  throw std::runtime_error("unsupported RPC authentication mode");
}

}  // namespace

HttpResponse HttpClient::PostJson(const RpcEndpoint& endpoint,
                                  std::string_view path, std::string_view body,
                                  std::stop_token stop_token) const {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
  const RpcCredentials credentials = ResolveCredentials(endpoint);

  asio::io_context ioc;
  tcp::resolver resolver(ioc);
  beast::tcp_stream stream(ioc);
  tcp::resolver::results_type endpoints;

  http::request<http::string_body> request{http::verb::post, std::string(path),
                                           11};
  request.set(http::field::host,
              endpoint.host + ":" + std::to_string(endpoint.port));
  request.set(http::field::content_type, "text/plain");
  request.set(
      http::field::authorization,
      "Basic " + Base64Encode(credentials.user + ":" + credentials.password));
  request.body() = std::string(body);
  request.prepare_payload();

  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  beast::error_code operation_error;
  bool completed = false;

  resolver.async_resolve(
      endpoint.host, std::to_string(endpoint.port),
      [&](beast::error_code resolve_error,
          tcp::resolver::results_type resolved_endpoints) {
        if (resolve_error) {
          operation_error = resolve_error;
          completed = true;
          return;
        }
        endpoints = std::move(resolved_endpoints);
        stream.expires_after(timeout_);
        stream.async_connect(endpoints, [&](beast::error_code connect_error,
                                            const tcp::endpoint&) {
          if (connect_error) {
            operation_error = connect_error;
            completed = true;
            return;
          }
          stream.expires_after(timeout_);
          http::async_write(
              stream, request, [&](beast::error_code write_error, std::size_t) {
                if (write_error) {
                  operation_error = write_error;
                  completed = true;
                  return;
                }
                stream.expires_after(timeout_);
                http::async_read(
                    stream, buffer, response,
                    [&](beast::error_code read_error, std::size_t) {
                      operation_error = read_error;
                      completed = true;
                    });
              });
        });
      });

  std::stop_callback cancellation(stop_token, [&ioc] { ioc.stop(); });
  ioc.run();
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
  if (!completed) {
    throw std::runtime_error("HTTP JSON POST stopped before completion");
  }
  if (operation_error) {
    throw boost::system::system_error(operation_error, "HTTP JSON POST");
  }

  beast::error_code shutdown_error;
  stream.socket().shutdown(tcp::socket::shutdown_both, shutdown_error);

  return HttpResponse{.status = static_cast<int>(response.result_int()),
                      .body = response.body()};
}

}  // namespace bbp
