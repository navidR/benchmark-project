#include "simulator_tcp_endpoint_reservation.h"

#include <boost/asio/ip/address.hpp>
#include <boost/system/error_code.hpp>
#include <string>
#include <utility>

#include "bbp/simulation_node_add.h"

namespace bbp::simulator_app_internal {

std::unique_ptr<boost::asio::ip::tcp::acceptor> ReserveTcpEndpoint(
    boost::asio::io_context& io_context, std::string_view address,
    std::uint16_t port, std::string_view resource_kind,
    std::string_view node_id, std::string_view purpose) {
  const auto unavailable = [&](std::string message) {
    throw SimulationNodeResourceUnavailable(
        std::move(message), SimulationNodeResourceFailure{
                                .resource_kind = std::string(resource_kind),
                                .node_id = std::string(node_id),
                                .address = std::string(address),
                                .port = port,
                                .purpose = std::string(purpose),
                                .mutation_started = false,
                            });
  };
  boost::system::error_code error;
  const boost::asio::ip::address parsed_address =
      boost::asio::ip::make_address(address, error);
  if (error) {
    unavailable("node-add " + std::string(purpose) +
                " bind address is invalid: " + std::string(address));
  }
  const boost::asio::ip::tcp::endpoint endpoint(parsed_address, port);
  auto reservation =
      std::make_unique<boost::asio::ip::tcp::acceptor>(io_context);
  reservation->open(endpoint.protocol(), error);
  if (error) {
    unavailable("node-add could not open " + std::string(purpose) +
                " port reservation: " + error.message());
  }
  reservation->bind(endpoint, error);
  if (error) {
    unavailable("node-add " + std::string(purpose) + " endpoint " +
                std::string(address) + ":" + std::to_string(port) +
                " is unavailable: " + error.message());
  }
  return reservation;
}

}  // namespace bbp::simulator_app_internal
