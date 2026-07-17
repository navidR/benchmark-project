#include "bbp/simulation_time_scale.h"

#include <cmath>
#include <limits>
#include <stdexcept>

namespace bbp {
namespace {

constexpr std::uint64_t kScaleUnitsPerOne = 1'000'000U;
constexpr std::uint64_t kMaximumScaleUnits = 1'000'000U * kScaleUnitsPerOne;

}  // namespace

SimulationTimeScale SimulationTimeScale::FromDouble(double value) {
  if (!std::isfinite(value) || value < 0.000001 || value > 1'000'000.0) {
    throw std::runtime_error(
        "simulation time_scale must be finite and in 0.000001..1000000");
  }
  const double scaled = value * static_cast<double>(kScaleUnitsPerOne);
  const double rounded = std::round(scaled);
  if (std::fabs(scaled - rounded) > 1e-6) {
    throw std::runtime_error(
        "simulation time_scale supports at most six decimal places");
  }
  const auto millionths = static_cast<std::uint64_t>(rounded);
  if (millionths == 0U || millionths > kMaximumScaleUnits) {
    throw std::runtime_error("simulation time_scale is outside its range");
  }
  return SimulationTimeScale(millionths);
}

double SimulationTimeScale::value() const {
  return static_cast<double>(millionths_) /
         static_cast<double>(kScaleUnitsPerOne);
}

std::uint64_t SimulationTimeScale::millionths() const { return millionths_; }

std::chrono::milliseconds SimulationTimeScale::WallDuration(
    std::chrono::milliseconds simulation_duration) const {
  if (simulation_duration.count() < 0) {
    throw std::runtime_error("simulation duration must not be negative");
  }
  const auto simulation_ms =
      static_cast<std::uint64_t>(simulation_duration.count());
  const std::uint64_t whole = simulation_ms / millionths_;
  const std::uint64_t remainder = simulation_ms % millionths_;
  const std::uint64_t maximum = static_cast<std::uint64_t>(
      std::numeric_limits<std::chrono::milliseconds::rep>::max());
  if (whole > maximum / kScaleUnitsPerOne) {
    throw std::runtime_error(
        "scaled simulation duration exceeds milliseconds range");
  }
  std::uint64_t wall_ms = whole * kScaleUnitsPerOne;
  if (remainder != 0U) {
    const std::uint64_t partial_numerator = remainder * kScaleUnitsPerOne;
    const std::uint64_t partial =
        partial_numerator / millionths_ +
        (partial_numerator % millionths_ == 0U ? 0U : 1U);
    if (wall_ms > maximum - partial) {
      throw std::runtime_error(
          "scaled simulation duration exceeds milliseconds range");
    }
    wall_ms += partial;
  }
  return std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(wall_ms));
}

SimulationTimeScale::SimulationTimeScale(std::uint64_t millionths)
    : millionths_(millionths) {}

}  // namespace bbp
