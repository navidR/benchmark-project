#include "benchmark_sim/block_production_policy.h"

#include <cmath>
#include <stdexcept>

namespace bsim {

BlockProductionPolicy::BlockProductionPolicy(std::chrono::milliseconds period,
                                             double probability,
                                             std::uint64_t seed)
    : period_(period), probability_(probability), seed_(seed) {
  if (period_.count() <= 0) {
    throw std::runtime_error("block production period must be positive");
  }
  if (!std::isfinite(probability_) || probability_ < 0.0 ||
      probability_ > 1.0) {
    throw std::runtime_error(
        "block production probability must be finite and in [0, 1]");
  }
}

}  // namespace bsim
