#include "bbp/chain_kind.h"

#include <stdexcept>
#include <string>

namespace bbp {

ChainKind ParseChainKind(std::string_view value) {
  if (value == "firo") {
    return ChainKind::kFiro;
  }
  if (value == "bitcoin") {
    return ChainKind::kBitcoin;
  }
  if (value == "monero") {
    return ChainKind::kMonero;
  }
  throw std::runtime_error("unsupported chain: " + std::string(value));
}

std::string_view ChainKindName(ChainKind chain) {
  switch (chain) {
    case ChainKind::kFiro:
      return "firo";
    case ChainKind::kBitcoin:
      return "bitcoin";
    case ChainKind::kMonero:
      return "monero";
    case ChainKind::kCount:
      break;
  }
  throw std::logic_error("unknown chain kind");
}

}  // namespace bbp
