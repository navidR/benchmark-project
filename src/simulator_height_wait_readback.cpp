#include "simulator_height_wait_readback.h"

#include <chrono>
#include <cstdint>
#include <optional>
#include <stop_token>

#include "bbp/drivers/chain_driver.h"

namespace bbp::simulator_app_internal {

std::optional<std::uint64_t> WaitForHeightReadback(
    const ChainDriver& driver, const ChainNodeConfig& config,
    std::uint64_t target_height, std::chrono::seconds timeout,
    std::stop_token stop_token) {
  driver.WaitForHeight(config, target_height, timeout, stop_token);
  const std::uint64_t observed_height =
      driver.ReadMetrics(config, stop_token).height;
  if (observed_height < target_height) {
    return std::nullopt;
  }
  return observed_height;
}

}  // namespace bbp::simulator_app_internal
