#pragma once

#include <chrono>
#include <cstdint>

namespace bsim {

class BlockProductionPolicy {
 public:
  BlockProductionPolicy(std::chrono::milliseconds period, double probability,
                        std::uint64_t seed);

  [[nodiscard]] std::chrono::milliseconds period() const { return period_; }
  [[nodiscard]] double probability() const { return probability_; }
  [[nodiscard]] std::uint64_t seed() const { return seed_; }

 private:
  std::chrono::milliseconds period_;
  double probability_;
  std::uint64_t seed_;
};

}  // namespace bsim
