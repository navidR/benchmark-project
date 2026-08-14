#pragma once

#include <cstdint>
#include <string>

namespace bbp {

struct ChainWalletSnapshot;
struct Options;
enum class WalletPrivacyMode;

namespace simulator_app_internal {

std::string WalletMetricsJson(const Options& options,
                              std::uint32_t wallet_index,
                              std::uint32_t one_based_node,
                              WalletPrivacyMode wallet_mode,
                              const ChainWalletSnapshot& snapshot);

}  // namespace simulator_app_internal
}  // namespace bbp
