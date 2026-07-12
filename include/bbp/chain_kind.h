#pragma once

#include <string_view>

namespace bbp {

enum class ChainKind {
  kFiro,
  kBitcoin,
  kMonero,
};

ChainKind ParseChainKind(std::string_view value);
std::string_view ChainKindName(ChainKind chain);

}  // namespace bbp
