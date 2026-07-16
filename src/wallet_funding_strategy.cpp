#include "bbp/simulator/wallet_funding_strategy.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <stdexcept>

namespace bbp {

std::string_view WalletFundingStrategyName(WalletFundingStrategy strategy) {
  switch (strategy) {
    case WalletFundingStrategy::kRoundRobin:
      return "round_robin";
    case WalletFundingStrategy::kRandom:
      return "random";
  }
  throw std::runtime_error("unknown wallet funding strategy");
}

std::optional<WalletFundingStrategy> WalletFundingStrategyFromName(
    std::string_view name) {
  if (name == "round_robin") {
    return WalletFundingStrategy::kRoundRobin;
  }
  if (name == "random") {
    return WalletFundingStrategy::kRandom;
  }
  return std::nullopt;
}

std::vector<std::uint32_t> WalletFundingMinerNodes(
    std::span<const std::uint32_t> miner_nodes, std::size_t wallet_count,
    WalletFundingStrategy strategy, std::uint64_t seed) {
  if (wallet_count == 0U) {
    return {};
  }
  if (miner_nodes.empty() && wallet_count != 0U) {
    throw std::runtime_error("wallet funding requires at least one miner");
  }

  std::vector<std::uint32_t> plan;
  plan.reserve(wallet_count);
  switch (strategy) {
    case WalletFundingStrategy::kRoundRobin:
      for (std::size_t index = 0; index < wallet_count; ++index) {
        plan.push_back(miner_nodes[index % miner_nodes.size()]);
      }
      break;
    case WalletFundingStrategy::kRandom: {
      boost::random::mt19937_64 generator(seed);
      boost::random::uniform_int_distribution<std::size_t> distribution(
          0U, miner_nodes.size() - 1U);
      for (std::size_t index = 0; index < wallet_count; ++index) {
        plan.push_back(miner_nodes[distribution(generator)]);
      }
      break;
    }
  }
  return plan;
}

}  // namespace bbp
