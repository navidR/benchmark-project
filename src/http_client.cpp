#include "bbp/http_client.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/hash2/md5.hpp>
#include <boost/system/system_error.hpp>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

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

struct HttpExchange {
  HttpResponse response;
  std::vector<std::string> authentication_challenges;
};

struct DigestChallenge {
  std::string realm;
  std::string nonce;
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
    case RpcAuthenticationMode::kDigest:
      if (endpoint.user.empty() || endpoint.password.empty()) {
        throw std::runtime_error(
            "RPC digest authentication requires a user and password");
      }
      if (!endpoint.cookie_file.empty()) {
        throw std::runtime_error(
            "RPC digest authentication must not specify a cookie file");
      }
      return RpcCredentials{.user = endpoint.user,
                            .password = endpoint.password};
  }
  throw std::runtime_error("unsupported RPC authentication mode");
}

bool AsciiCaseEqual(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t i = 0U; i < left.size(); ++i) {
    const unsigned char left_character = static_cast<unsigned char>(left[i]);
    const unsigned char right_character = static_cast<unsigned char>(right[i]);
    if (std::tolower(left_character) != std::tolower(right_character)) {
      return false;
    }
  }
  return true;
}

bool IsHttpTokenCharacter(unsigned char character) {
  if ((character >= 'a' && character <= 'z') ||
      (character >= 'A' && character <= 'Z') ||
      (character >= '0' && character <= '9')) {
    return true;
  }
  constexpr std::string_view kOtherTokenCharacters = "!#$%&'*+-.^_`|~";
  return kOtherTokenCharacters.find(static_cast<char>(character)) !=
         std::string_view::npos;
}

void SkipOptionalWhitespace(std::string_view input, std::size_t* position) {
  while (*position < input.size() &&
         (input[*position] == ' ' || input[*position] == '\t')) {
    ++*position;
  }
}

std::string ParseHttpToken(std::string_view input, std::size_t* position) {
  const std::size_t begin = *position;
  while (*position < input.size() &&
         IsHttpTokenCharacter(static_cast<unsigned char>(input[*position]))) {
    ++*position;
  }
  if (*position == begin) {
    throw std::runtime_error(
        "RPC digest authentication challenge has an invalid token");
  }
  return std::string(input.substr(begin, *position - begin));
}

std::string ParseDigestValue(std::string_view input, std::size_t* position) {
  if (*position >= input.size()) {
    throw std::runtime_error(
        "RPC digest authentication challenge has a missing value");
  }
  if (input[*position] != '"') {
    return ParseHttpToken(input, position);
  }

  ++*position;
  std::string value;
  while (*position < input.size()) {
    const unsigned char character =
        static_cast<unsigned char>(input[*position]);
    ++*position;
    if (character == '"') {
      return value;
    }
    if (character == '\\') {
      if (*position >= input.size()) {
        break;
      }
      const unsigned char escaped =
          static_cast<unsigned char>(input[*position]);
      ++*position;
      if (escaped < 0x20U || escaped == 0x7fU) {
        break;
      }
      value.push_back(static_cast<char>(escaped));
      continue;
    }
    if (character < 0x20U || character == 0x7fU) {
      break;
    }
    value.push_back(static_cast<char>(character));
  }
  throw std::runtime_error(
      "RPC digest authentication challenge has an invalid quoted value");
}

std::optional<DigestChallenge> ParseDigestChallenge(std::string_view input) {
  constexpr std::size_t kMaximumChallengeBytes = 4096U;
  if (input.empty() || input.size() > kMaximumChallengeBytes) {
    throw std::runtime_error(
        "RPC digest authentication challenge has an invalid size");
  }

  std::size_t position = 0U;
  SkipOptionalWhitespace(input, &position);
  const std::string scheme = ParseHttpToken(input, &position);
  if (!AsciiCaseEqual(scheme, "Digest")) {
    return std::nullopt;
  }
  if (position >= input.size() ||
      (input[position] != ' ' && input[position] != '\t')) {
    throw std::runtime_error(
        "RPC digest authentication challenge has no parameters");
  }

  std::map<std::string, std::string, std::less<>> parameters;
  while (position < input.size()) {
    SkipOptionalWhitespace(input, &position);
    if (position >= input.size()) {
      break;
    }
    std::string name = ParseHttpToken(input, &position);
    std::transform(name.begin(), name.end(), name.begin(), [](char character) {
      return static_cast<char>(
          std::tolower(static_cast<unsigned char>(character)));
    });
    SkipOptionalWhitespace(input, &position);
    if (position >= input.size() || input[position] != '=') {
      throw std::runtime_error(
          "RPC digest authentication challenge has a malformed parameter");
    }
    ++position;
    SkipOptionalWhitespace(input, &position);
    std::string value = ParseDigestValue(input, &position);
    if (!parameters.emplace(std::move(name), std::move(value)).second) {
      throw std::runtime_error(
          "RPC digest authentication challenge repeats a parameter");
    }
    SkipOptionalWhitespace(input, &position);
    if (position >= input.size()) {
      break;
    }
    if (input[position] != ',') {
      throw std::runtime_error(
          "RPC digest authentication challenge has invalid separators");
    }
    ++position;
  }

  const auto realm = parameters.find("realm");
  const auto nonce = parameters.find("nonce");
  const auto algorithm = parameters.find("algorithm");
  const auto qop = parameters.find("qop");
  if (realm == parameters.end() || realm->second.empty() ||
      nonce == parameters.end() || nonce->second.empty() ||
      (algorithm != parameters.end() &&
       !AsciiCaseEqual(algorithm->second, "MD5")) ||
      qop == parameters.end() || !AsciiCaseEqual(qop->second, "auth")) {
    return std::nullopt;
  }
  return DigestChallenge{.realm = realm->second, .nonce = nonce->second};
}

DigestChallenge SelectDigestChallenge(
    const std::vector<std::string>& challenges) {
  for (const std::string& challenge : challenges) {
    const std::optional<DigestChallenge> parsed =
        ParseDigestChallenge(challenge);
    if (parsed) {
      return *parsed;
    }
  }
  throw std::runtime_error(
      "RPC server returned no supported MD5 qop=auth digest challenge");
}

std::string Md5Hex(std::string_view input) {
  boost::hash2::md5_128 hash;
  hash.update(input.data(), input.size());
  return to_string(hash.result());
}

std::string QuotedDigestValue(std::string_view value) {
  std::string output;
  output.reserve(value.size() + 2U);
  output.push_back('"');
  for (const unsigned char character : value) {
    if (character < 0x20U || character == 0x7fU) {
      throw std::runtime_error(
          "RPC digest authentication value contains a control character");
    }
    if (character == '"' || character == '\\') {
      output.push_back('\\');
    }
    output.push_back(static_cast<char>(character));
  }
  output.push_back('"');
  return output;
}

std::string DigestAuthorization(const RpcCredentials& credentials,
                                const DigestChallenge& challenge,
                                std::string_view path) {
  constexpr std::string_view kNonceCount = "00000001";
  const std::string first = Md5Hex(credentials.user + ":" + challenge.realm +
                                   ":" + credentials.password);
  const std::string second = Md5Hex("POST:" + std::string(path));
  const std::string response =
      Md5Hex(first + ":" + challenge.nonce + ":" + std::string(kNonceCount) +
             "::auth:" + second);
  return "Digest algorithm=MD5,nonce=" + QuotedDigestValue(challenge.nonce) +
         ",realm=" + QuotedDigestValue(challenge.realm) +
         ",response=" + QuotedDigestValue(response) +
         ",uri=" + QuotedDigestValue(path) +
         ",username=" + QuotedDigestValue(credentials.user) +
         ",qop=auth,nc=" + std::string(kNonceCount);
}

HttpExchange SendJson(const RpcEndpoint& endpoint, std::string_view path,
                      std::string_view body,
                      const std::optional<std::string>& authorization,
                      std::chrono::milliseconds timeout,
                      std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }

  asio::io_context ioc;
  tcp::resolver resolver(ioc);
  beast::tcp_stream stream(ioc);
  tcp::resolver::results_type endpoints;

  http::request<http::string_body> request{http::verb::post, std::string(path),
                                           11};
  request.set(http::field::host,
              endpoint.host + ":" + std::to_string(endpoint.port));
  request.set(http::field::content_type, "text/plain");
  if (authorization) {
    request.set(http::field::authorization, *authorization);
  }
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
        stream.expires_after(timeout);
        stream.async_connect(endpoints, [&](beast::error_code connect_error,
                                            const tcp::endpoint&) {
          if (connect_error) {
            operation_error = connect_error;
            completed = true;
            return;
          }
          stream.expires_after(timeout);
          http::async_write(
              stream, request, [&](beast::error_code write_error, std::size_t) {
                if (write_error) {
                  operation_error = write_error;
                  completed = true;
                  return;
                }
                stream.expires_after(timeout);
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

  HttpExchange exchange;
  exchange.response.status = static_cast<int>(response.result_int());
  exchange.response.body = std::move(response.body());
  const auto challenge_range =
      response.base().equal_range(http::field::www_authenticate);
  for (auto field = challenge_range.first; field != challenge_range.second;
       ++field) {
    exchange.authentication_challenges.emplace_back(field->value());
  }
  return exchange;
}

}  // namespace

HttpResponse HttpClient::PostJson(const RpcEndpoint& endpoint,
                                  std::string_view path, std::string_view body,
                                  std::stop_token stop_token) const {
  const RpcCredentials credentials = ResolveCredentials(endpoint);
  if (endpoint.authentication != RpcAuthenticationMode::kDigest) {
    const std::string authorization =
        "Basic " + Base64Encode(credentials.user + ":" + credentials.password);
    return SendJson(endpoint, path, body, authorization, timeout_, stop_token)
        .response;
  }

  std::lock_guard lock(digest_mutex_);
  HttpExchange challenge_response =
      SendJson(endpoint, path, body, std::nullopt, timeout_, stop_token);
  if (challenge_response.response.status != 401) {
    throw std::runtime_error(
        "RPC digest endpoint accepted a request without authentication");
  }

  DigestChallenge challenge =
      SelectDigestChallenge(challenge_response.authentication_challenges);
  for (std::uint32_t attempt = 0U; attempt < 2U; ++attempt) {
    HttpExchange authenticated = SendJson(
        endpoint, path, body, DigestAuthorization(credentials, challenge, path),
        timeout_, stop_token);
    if (authenticated.response.status != 401) {
      return authenticated.response;
    }
    if (attempt == 0U) {
      challenge =
          SelectDigestChallenge(authenticated.authentication_challenges);
    }
  }
  throw std::runtime_error("RPC digest authentication was rejected");
}

}  // namespace bbp
