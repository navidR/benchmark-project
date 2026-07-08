#include "benchmark_sim/http_client.h"

#include <string>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

namespace bsim {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

std::string Base64Encode(const std::string& input) {
  std::string output;
  output.resize(beast::detail::base64::encoded_size(input.size()));
  const auto size = beast::detail::base64::encode(output.data(), input.data(),
                                                 input.size());
  output.resize(size);
  return output;
}

}  // namespace

HttpResponse HttpClient::PostJson(const RpcEndpoint& endpoint,
                                  std::string_view path,
                                  std::string_view body) const {
  asio::io_context ioc;
  tcp::resolver resolver(ioc);
  beast::tcp_stream stream(ioc);
  stream.expires_after(timeout_);

  auto results = resolver.resolve(endpoint.host, std::to_string(endpoint.port));
  stream.connect(results);

  http::request<http::string_body> request{http::verb::post,
                                           std::string(path), 11};
  request.set(http::field::host,
              endpoint.host + ":" + std::to_string(endpoint.port));
  request.set(http::field::content_type, "text/plain");
  request.set(http::field::authorization,
              "Basic " +
                  Base64Encode(endpoint.user + ":" + endpoint.password));
  request.body() = std::string(body);
  request.prepare_payload();

  http::write(stream, request);

  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  beast::error_code ec;
  stream.socket().shutdown(tcp::socket::shutdown_both, ec);

  return HttpResponse{.status = static_cast<int>(response.result_int()),
                      .body = response.body()};
}

}  // namespace bsim
