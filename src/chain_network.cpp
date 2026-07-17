#include "bbp/chain_network.h"

#include <stdexcept>
#include <string>

namespace bbp {

ChainNetwork ParseChainNetwork(std::string_view name) {
  if (name == "regtest") {
    return ChainNetwork::kRegtest;
  }
  throw std::runtime_error("unsupported chain network: " + std::string(name));
}

std::string_view ChainNetworkName(ChainNetwork network) {
  switch (network) {
    case ChainNetwork::kRegtest:
      return "regtest";
  }
  throw std::runtime_error("unknown chain network enum value");
}

}  // namespace bbp
