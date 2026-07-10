#include "benchmark_sim/mining_difficulty.h"

#include <cmath>
#include <stdexcept>

namespace bsim {

MiningDifficulty::MiningDifficulty(double value) : value_(value) {
  if (!std::isfinite(value_) || value_ <= 0.0) {
    throw std::runtime_error("mining difficulty must be finite and positive");
  }
}

}  // namespace bsim
