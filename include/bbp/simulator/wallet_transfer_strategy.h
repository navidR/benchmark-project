#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class WalletTransferStrategy {
  kRoundRobin,
  kRandom,
};

constexpr std::string_view WalletTransferStrategyName(
    WalletTransferStrategy strategy) {
  switch (strategy) {
    case WalletTransferStrategy::kRoundRobin:
      return "round_robin";
    case WalletTransferStrategy::kRandom:
      return "random";
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
  return std::nullopt;
}

}  // namespace bbp
