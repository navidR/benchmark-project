#include "bbp/http_client.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/system/system_error.hpp>
#include <string>

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

}  // namespace

HttpResponse HttpClient::PostJson(const RpcEndpoint& endpoint,
                                  std::string_view path, std::string_view body,
                                  std::stop_token stop_token) const {
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
  request.set(http::field::authorization,
              "Basic " + Base64Encode(endpoint.user + ":" + endpoint.password));
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
