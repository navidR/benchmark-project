#pragma once

#include <string_view>

namespace bbp {

enum class ChainNetwork {
  kRegtest,
};

ChainNetwork ParseChainNetwork(std::string_view name);
std::string_view ChainNetworkName(ChainNetwork network);

}  // namespace bbp
