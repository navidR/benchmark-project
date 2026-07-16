#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace bbp {

enum class WalletFundingStrategy {
  kRoundRobin,
  kRandom,
};

std::string_view WalletFundingStrategyName(WalletFundingStrategy strategy);
std::optional<WalletFundingStrategy> WalletFundingStrategyFromName(
    std::string_view name);
std::vector<std::uint32_t> WalletFundingMinerNodes(
    std::span<const std::uint32_t> miner_nodes, std::size_t wallet_count,
    WalletFundingStrategy strategy, std::uint64_t seed);

}  // namespace bbp
