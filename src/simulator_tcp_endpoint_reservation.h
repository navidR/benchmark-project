#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <cstdint>
#include <memory>
#include <string_view>

namespace bbp::simulator_app_internal {

std::unique_ptr<boost::asio::ip::tcp::acceptor> ReserveTcpEndpoint(
    boost::asio::io_context& io_context, std::string_view address,
    std::uint16_t port, std::string_view resource_kind,
    std::string_view node_id, std::string_view purpose);

}  // namespace bbp::simulator_app_internal
