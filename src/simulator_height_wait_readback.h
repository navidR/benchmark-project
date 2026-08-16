#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <stop_token>

namespace bbp {

class ChainDriver;
struct ChainNodeConfig;

namespace simulator_app_internal {

std::optional<std::uint64_t> WaitForHeightReadback(
    const ChainDriver& driver, const ChainNodeConfig& config,
    std::uint64_t target_height, std::chrono::seconds timeout,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
