#pragma once

#include <chrono>
#include <cstdint>

namespace bbp {

class SimulationTimeScale {
 public:
  static SimulationTimeScale FromDouble(double value);

  [[nodiscard]] double value() const;
  [[nodiscard]] std::uint64_t millionths() const;
  [[nodiscard]] std::chrono::milliseconds WallDuration(
      std::chrono::milliseconds simulation_duration) const;

 private:
  explicit SimulationTimeScale(std::uint64_t millionths);

  std::uint64_t millionths_ = 1'000'000U;
};

}  // namespace bbp
