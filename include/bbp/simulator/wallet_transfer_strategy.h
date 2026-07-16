#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class WalletTransferStrategy {
  kRoundRobin,
  kRandom,
  kFanout,
  kHotspot,
};

constexpr std::string_view WalletTransferStrategyName(
    WalletTransferStrategy strategy) {
  switch (strategy) {
    case WalletTransferStrategy::kRoundRobin:
      return "round_robin";
    case WalletTransferStrategy::kRandom:
      return "random";
    case WalletTransferStrategy::kFanout:
      return "fanout";
    case WalletTransferStrategy::kHotspot:
      return "hotspot";
  }
  return "unknown";
}

constexpr std::optional<WalletTransferStrategy> WalletTransferStrategyFromName(
    std::string_view name) {
  if (name == "round_robin") {
    return WalletTransferStrategy::kRoundRobin;
  }
  if (name == "random") {
    return WalletTransferStrategy::kRandom;
  }
  if (name == "fanout") {
    return WalletTransferStrategy::kFanout;
  }
  if (name == "hotspot") {
    return WalletTransferStrategy::kHotspot;
  }
  return std::nullopt;
}

}  // namespace bbp
