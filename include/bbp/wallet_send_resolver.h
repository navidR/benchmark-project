#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <string>

#include "bbp/simulation_wallet_send.h"

namespace bbp {

struct ResolvedWalletSend {
  SimulationWalletSend send;
  std::string sender_node_id;
  std::string target_text;
};

ResolvedWalletSend ResolveSelectedWalletSend(const boost::json::object& report,
                                             std::size_t selected_wallet,
                                             SimulationWalletSend send);

}  // namespace bbp
