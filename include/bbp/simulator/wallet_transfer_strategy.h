#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class WalletTransferStrategy {
  kRoundRobin,
  kRandom,
  kFanout,
  kHotspot,
  kRandomBruteforce,
  kEqualFanout,
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
    case WalletTransferStrategy::kRandomBruteforce:
      return "random_bruteforce";
    case WalletTransferStrategy::kEqualFanout:
      return "equal_fanout";
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
  if (name == "random_bruteforce") {
    return WalletTransferStrategy::kRandomBruteforce;
  }
  if (name == "equal_fanout") {
    return WalletTransferStrategy::kEqualFanout;
  }
  return std::nullopt;
}

constexpr bool IsTransactionLoadStrategy(WalletTransferStrategy strategy) {
  return strategy == WalletTransferStrategy::kRandomBruteforce ||
         strategy == WalletTransferStrategy::kEqualFanout;
}

}  // namespace bbp
