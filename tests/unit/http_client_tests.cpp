#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <future>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "bbp/http_client.h"
#include "bbp/simulation_cancelled.h"

namespace {

std::filesystem::path MakeTestDirectory(std::string_view name) {
  const std::filesystem::path directory =
      std::filesystem::temp_directory_path() /
      ("bbp-http-client-" + std::string(name) + "-" + std::to_string(getpid()));
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  return directory;
}

void WriteFile(const std::filesystem::path& path, std::string_view contents,
               mode_t mode = 0600) {
  const int fd =
      open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW,
           mode);
  if (fd < 0) {
    throw std::runtime_error("test credential file open failed");
  }
  std::size_t offset = 0U;
  while (offset < contents.size()) {
    const ssize_t written =
        write(fd, contents.data() + offset, contents.size() - offset);
    if (written <= 0) {
      close(fd);
      throw std::runtime_error("test credential file write failed");
    }
    offset += static_cast<std::size_t>(written);
  }
  if (fchmod(fd, mode) != 0 || close(fd) != 0) {
    throw std::runtime_error("test credential file close failed");
  }
}

bbp::RpcEndpoint CookieEndpoint(const std::filesystem::path& cookie_file,
                                std::uint16_t port) {
  return bbp::RpcEndpoint{
      .host = "127.0.0.1",
      .port = port,
      .authentication = bbp::RpcAuthenticationMode::kCookieFile,
      .user = {},
      .password = {},
      .cookie_file = cookie_file,
  };
}

bbp::RpcEndpoint DigestEndpoint(std::uint16_t port) {
  return bbp::RpcEndpoint{
      .host = "127.0.0.1",
      .port = port,
      .authentication = bbp::RpcAuthenticationMode::kDigest,
      .user = "bbp-user",
      .password = "digest-secret",
      .cookie_file = {},
  };
}

struct DigestReply {
  boost::beast::http::status status;
  std::vector<std::string> challenges;
  std::string body;
};

struct DigestConversation {
  std::vector<std::string> authorizations;
  std::optional<bbp::HttpResponse> response;
  std::optional<std::string> error;
};

std::vector<std::string> ServeDigestReplies(
    boost::asio::ip::tcp::acceptor& acceptor,
    const std::vector<DigestReply>& replies) {
  namespace beast = boost::beast;
  namespace http = beast::http;

  boost::asio::ip::tcp::socket socket(acceptor.get_executor());
  acceptor.accept(socket);
  beast::flat_buffer buffer;
  std::vector<std::string> authorizations;
  authorizations.reserve(replies.size());
  for (const DigestReply& reply : replies) {
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    const auto authorization = request.find(http::field::authorization);
    authorizations.push_back(authorization == request.end()
                                 ? ""
                                 : std::string(authorization->value()));

    http::response<http::string_body> response{reply.status, 11};
    for (const std::string& challenge : reply.challenges) {
      response.insert(http::field::www_authenticate, challenge);
    }
    response.keep_alive(true);
    response.body() = reply.body;
    response.prepare_payload();
    http::write(socket, response);
  }
  return authorizations;
}

DigestConversation RunDigestConversation(
    const std::vector<DigestReply>& replies, std::string user = "bbp-user",
    std::string password = "digest-secret") {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestReplies(acceptor, replies); });
  bbp::RpcEndpoint endpoint = DigestEndpoint(acceptor.local_endpoint().port());
  endpoint.user = std::move(user);
  endpoint.password = std::move(password);

  DigestConversation result;
  try {
    result.response = bbp::HttpClient(std::chrono::seconds(1))
                          .PostJson(endpoint, "/json_rpc", "{}");
  } catch (const std::exception& error) {
    result.error = error.what();
  }
  result.authorizations = served.get();
  return result;
}

}  // namespace

BOOST_AUTO_TEST_CASE(http_client_uses_secure_cookie_credentials) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  const std::filesystem::path directory = MakeTestDirectory("cookie");
  const std::filesystem::path cookie = directory / ".cookie";
  WriteFile(cookie, "__cookie__:secret-cookie-value");

  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<std::string> authorization = std::async(std::launch::async, [&] {
    tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    http::response<http::string_body> response{http::status::ok, 11};
    response.body() = "{}";
    response.prepare_payload();
    http::write(socket, response);
    return std::string(request.at(http::field::authorization));
  });

  const bbp::HttpClient client(std::chrono::seconds(1));
  const bbp::HttpResponse response = client.PostJson(
      CookieEndpoint(cookie, acceptor.local_endpoint().port()), "/", "{}");

  BOOST_TEST(response.status == 200);
  BOOST_TEST(response.body == "{}");
  BOOST_TEST(authorization.get() ==
             "Basic X19jb29raWVfXzpzZWNyZXQtY29va2llLXZhbHVl");
  std::filesystem::remove_all(directory);
}

BOOST_AUTO_TEST_CASE(http_client_rejects_unsafe_cookie_files_without_leaking) {
  const std::filesystem::path directory = MakeTestDirectory("unsafe");
  const std::string secret = "secret-must-not-leak";
  const bbp::HttpClient client(std::chrono::milliseconds(20));

  const auto expect_rejection = [&](const std::filesystem::path& path) {
    try {
      static_cast<void>(client.PostJson(CookieEndpoint(path, 1U), "/", "{}"));
      BOOST_FAIL("unsafe RPC cookie was accepted");
    } catch (const std::exception& error) {
      BOOST_TEST(std::string(error.what()).find(secret) == std::string::npos);
    }
  };

  const std::filesystem::path permissive = directory / "permissive";
  WriteFile(permissive, "user:" + secret, 0640);
  expect_rejection(permissive);

  const std::filesystem::path target = directory / "target";
  WriteFile(target, "user:" + secret);
  const std::filesystem::path link = directory / "link";
  std::filesystem::create_symlink(target, link);
  expect_rejection(link);

  const std::filesystem::path malformed = directory / "malformed";
  WriteFile(malformed, secret);
  expect_rejection(malformed);

  const std::filesystem::path oversized = directory / "oversized";
  WriteFile(oversized, "user:" + std::string(1024U, 'x'));
  expect_rejection(oversized);

  std::filesystem::remove_all(directory);
}

BOOST_AUTO_TEST_CASE(http_client_transport_errors_do_not_expose_credentials) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  const std::filesystem::path directory = MakeTestDirectory("timeout");
  const std::string secret = "timeout-secret-must-not-leak";
  const std::filesystem::path cookie = directory / ".cookie";
  WriteFile(cookie, "user:" + secret);

  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<void> server = std::async(std::launch::async, [&] {
    tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  const bbp::HttpClient client(std::chrono::milliseconds(20));
  try {
    static_cast<void>(client.PostJson(
        CookieEndpoint(cookie, acceptor.local_endpoint().port()), "/", "{}"));
    BOOST_FAIL("timed-out RPC request unexpectedly completed");
  } catch (const std::exception& error) {
    BOOST_TEST(std::string(error.what()).find(secret) == std::string::npos);
  }
  server.get();
  std::filesystem::remove_all(directory);
}

BOOST_AUTO_TEST_CASE(http_client_rejects_conflicting_authentication_sources) {
  const bbp::HttpClient client(std::chrono::milliseconds(20));
  bbp::RpcEndpoint endpoint;
  endpoint.host = "127.0.0.1";
  endpoint.port = 1U;
  endpoint.authentication = bbp::RpcAuthenticationMode::kCookieFile;
  endpoint.user = "inline-user";
  endpoint.password = "inline-password";
  endpoint.cookie_file = "/tmp/not-read";
  BOOST_CHECK_THROW(client.PostJson(endpoint, "/", "{}"), std::runtime_error);

  endpoint.authentication = bbp::RpcAuthenticationMode::kBasic;
  endpoint.cookie_file = "/tmp/must-not-be-present";
  BOOST_CHECK_THROW(client.PostJson(endpoint, "/", "{}"), std::runtime_error);

  endpoint.authentication = bbp::RpcAuthenticationMode::kDigest;
  BOOST_CHECK_THROW(client.PostJson(endpoint, "/", "{}"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(http_client_completes_monero_digest_authentication) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<std::string> authorization = std::async(std::launch::async, [&] {
    tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    for (std::uint32_t request_index = 0U; request_index < 2U;
         ++request_index) {
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      if (request_index == 0U) {
        BOOST_CHECK(request.find(http::field::authorization) == request.end());
        http::response<http::string_body> response{http::status::unauthorized,
                                                   11};
        response.insert(
            http::field::www_authenticate,
            "Digest qop=\"auth\",algorithm=MD5-sess,realm=\"monero-rpc\","
            "nonce=\"fixed-nonce\",stale=false");
        response.insert(
            http::field::www_authenticate,
            "Digest qop=\"auth\",algorithm=MD5,realm=\"monero-rpc\","
            "nonce=\"fixed-nonce\",stale=false");
        response.keep_alive(true);
        response.body() = "unauthorized";
        response.prepare_payload();
        http::write(socket, response);
        continue;
      }
      const std::string header(request.at(http::field::authorization));
      http::response<http::string_body> response{http::status::ok, 11};
      response.body() = "{\"ok\":true}";
      response.prepare_payload();
      http::write(socket, response);
      return header;
    }
    throw std::runtime_error("digest test did not receive authentication");
  });

  const bbp::HttpClient client(std::chrono::seconds(1));
  const bbp::HttpResponse response = client.PostJson(
      DigestEndpoint(acceptor.local_endpoint().port()), "/json_rpc", "{}");

  BOOST_TEST(response.status == 200);
  BOOST_TEST(response.body == "{\"ok\":true}");
  const std::string header = authorization.get();
  BOOST_TEST(header.find("Digest algorithm=MD5") == 0U);
  BOOST_TEST(header.find("nonce=\"fixed-nonce\"") != std::string::npos);
  BOOST_TEST(header.find("realm=\"monero-rpc\"") != std::string::npos);
  BOOST_TEST(header.find("uri=\"/json_rpc\"") != std::string::npos);
  BOOST_TEST(header.find("username=\"bbp-user\"") != std::string::npos);
  BOOST_TEST(header.find("qop=auth,nc=00000001") != std::string::npos);
  BOOST_TEST(header.find("response=\"a7ecb5f91bb89b778c32d8b1a58ce427\"") !=
             std::string::npos);
  BOOST_TEST(header.find("digest-secret") == std::string::npos);
}

BOOST_AUTO_TEST_CASE(http_client_rejects_malformed_digest_challenges) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<void> server = std::async(std::launch::async, [&] {
    tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    http::response<http::string_body> response{http::status::unauthorized, 11};
    response.insert(http::field::www_authenticate,
                    "Digest qop=\"auth\",algorithm=MD5,realm=\"monero-rpc\"");
    response.prepare_payload();
    http::write(socket, response);
  });

  const bbp::HttpClient client(std::chrono::seconds(1));
  try {
    static_cast<void>(client.PostJson(
        DigestEndpoint(acceptor.local_endpoint().port()), "/json_rpc", "{}"));
    BOOST_FAIL("malformed digest challenge was accepted");
  } catch (const std::exception& error) {
    BOOST_TEST(std::string(error.what()).find("digest-secret") ==
               std::string::npos);
  }
  server.get();
}

BOOST_AUTO_TEST_CASE(http_client_rejects_unsafe_digest_credentials) {
  const bbp::HttpClient client(std::chrono::milliseconds(20));
  const auto expect_unsafe = [&](std::string user, std::string password) {
    bbp::RpcEndpoint endpoint = DigestEndpoint(1U);
    endpoint.user = std::move(user);
    endpoint.password = std::move(password);
    try {
      static_cast<void>(client.PostJson(endpoint, "/json_rpc", "{}"));
      BOOST_FAIL("unsafe Digest credentials were accepted");
    } catch (const std::exception& error) {
      const std::string message = error.what();
      BOOST_TEST(message.find(endpoint.user) == std::string::npos);
      BOOST_TEST(message.find(endpoint.password) == std::string::npos);
    }
  };

  expect_unsafe("unsafe:user", "safe-password");
  expect_unsafe("safe-user", "unsafe:password");
  expect_unsafe("unsafe\nuser", "safe-password");
  expect_unsafe("safe-user", "unsafe\rpassword");
  expect_unsafe(std::string(257U, 'u'), "safe-password");
  expect_unsafe("safe-user", std::string(257U, 'p'));
}

BOOST_AUTO_TEST_CASE(http_client_rejects_multiple_supported_digest_challenges) {
  const DigestConversation result = RunDigestConversation({
      {.status = boost::beast::http::status::unauthorized,
       .challenges = {"Digest qop=auth,algorithm=MD5,realm=first,nonce=one",
                      "Digest qop=auth,algorithm=MD5,realm=second,nonce=two"},
       .body = "unauthorized"},
  });

  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("multiple supported") != std::string::npos);
  BOOST_REQUIRE_EQUAL(result.authorizations.size(), 1U);
  BOOST_TEST(result.authorizations.front().empty());
}

BOOST_AUTO_TEST_CASE(http_client_rejects_invalid_digest_stale_state) {
  const DigestConversation result = RunDigestConversation({
      {.status = boost::beast::http::status::unauthorized,
       .challenges =
           {"Digest qop=auth,algorithm=MD5,realm=test,nonce=one,stale=maybe"},
       .body = "unauthorized"},
  });

  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("invalid stale state") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(
    http_client_rejects_authenticated_digest_401_without_stale_refresh) {
  const DigestConversation result = RunDigestConversation({
      {.status = boost::beast::http::status::unauthorized,
       .challenges =
           {"Digest qop=auth,algorithm=MD5,realm=test,nonce=one,stale=false"},
       .body = "unauthorized"},
      {.status = boost::beast::http::status::unauthorized,
       .challenges =
           {"Digest qop=auth,algorithm=MD5,realm=test,nonce=two,stale=false"},
       .body = "rejected"},
  });

  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("without a stale challenge") !=
             std::string::npos);
  BOOST_REQUIRE_EQUAL(result.authorizations.size(), 2U);
  BOOST_TEST(result.authorizations.front().empty());
  BOOST_TEST(result.authorizations.back().find("nonce=\"one\"") !=
             std::string::npos);
}

BOOST_AUTO_TEST_CASE(http_client_rejects_digest_stale_nonce_reuse) {
  const DigestConversation result = RunDigestConversation({
      {.status = boost::beast::http::status::unauthorized,
       .challenges = {"Digest qop=auth,algorithm=MD5,realm=test,nonce=one"},
       .body = "unauthorized"},
      {.status = boost::beast::http::status::unauthorized,
       .challenges =
           {"Digest qop=auth,algorithm=MD5,realm=test,nonce=one,stale=true"},
       .body = "stale"},
  });

  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("reused its nonce") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(http_client_rejects_digest_stale_realm_change) {
  const DigestConversation result = RunDigestConversation({
      {.status = boost::beast::http::status::unauthorized,
       .challenges = {"Digest qop=auth,algorithm=MD5,realm=first,nonce=one"},
       .body = "unauthorized"},
      {.status = boost::beast::http::status::unauthorized,
       .challenges =
           {"Digest qop=auth,algorithm=MD5,realm=second,nonce=two,stale=true"},
       .body = "stale"},
  });

  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("changed realm") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(http_client_retries_one_valid_digest_stale_nonce) {
  const DigestConversation result = RunDigestConversation({
      {.status = boost::beast::http::status::unauthorized,
       .challenges = {"Digest qop=auth,algorithm=MD5,realm=test,nonce=one"},
       .body = "unauthorized"},
      {.status = boost::beast::http::status::unauthorized,
       .challenges =
           {"Digest qop=auth,algorithm=MD5,realm=test,nonce=two,stale=true"},
       .body = "stale"},
      {.status = boost::beast::http::status::ok,
       .challenges = {},
       .body = "{\"ok\":true}"},
  });

  BOOST_REQUIRE(!result.error.has_value());
  BOOST_REQUIRE(result.response.has_value());
  BOOST_TEST(result.response->status == 200);
  BOOST_TEST(result.response->body == "{\"ok\":true}");
  BOOST_REQUIRE_EQUAL(result.authorizations.size(), 3U);
  BOOST_TEST(result.authorizations[0].empty());
  BOOST_TEST(result.authorizations[1].find("nonce=\"one\"") !=
             std::string::npos);
  BOOST_TEST(result.authorizations[2].find("nonce=\"two\"") !=
             std::string::npos);
  BOOST_TEST(result.authorizations[1].find("digest-secret") ==
             std::string::npos);
  BOOST_TEST(result.authorizations[2].find("digest-secret") ==
             std::string::npos);
}

BOOST_AUTO_TEST_CASE(http_client_digest_errors_do_not_expose_credentials) {
  const std::string user = "diagnostic-user-secret";
  const std::string password = "diagnostic-password-secret";
  const DigestConversation result = RunDigestConversation(
      {{.status = boost::beast::http::status::unauthorized,
        .challenges = {"Digest qop=auth,algorithm=MD5,realm=test"},
        .body = "unauthorized"}},
      user, password);

  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find(user) == std::string::npos);
  BOOST_TEST(result.error->find(password) == std::string::npos);
}

BOOST_AUTO_TEST_CASE(
    http_client_digest_wait_honors_stop_while_server_is_silent) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::promise<void> request_received;
  std::future<void> request_seen = request_received.get_future();
  std::future<void> server = std::async(std::launch::async, [&] {
    tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    request_received.set_value();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  });

  std::stop_source stop;
  const bbp::HttpClient client(std::chrono::seconds(5));
  std::future<void> rpc = std::async(std::launch::async, [&] {
    static_cast<void>(
        client.PostJson(DigestEndpoint(acceptor.local_endpoint().port()),
                        "/json_rpc", "{}", stop.get_token()));
  });
  BOOST_REQUIRE(request_seen.wait_for(std::chrono::seconds(1)) ==
                std::future_status::ready);

  const auto cancellation_started = std::chrono::steady_clock::now();
  stop.request_stop();
  BOOST_REQUIRE(rpc.wait_for(std::chrono::milliseconds(250)) ==
                std::future_status::ready);
  BOOST_CHECK_THROW(rpc.get(), bbp::SimulationCancelled);
  const auto elapsed = std::chrono::steady_clock::now() - cancellation_started;
  BOOST_TEST(elapsed < std::chrono::milliseconds(250));
  server.get();
}
