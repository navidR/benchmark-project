#include "bbp/positive_duration.h"

#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

std::uint64_t UnitMilliseconds(std::string_view suffix) {
  if (suffix == "ms") {
    return 1U;
  }
  if (suffix == "s") {
    return 1000U;
  }
  if (suffix == "m") {
    return 60U * 1000U;
  }
  if (suffix == "h") {
    return 60U * 60U * 1000U;
  }
  throw std::runtime_error("duration unit must be ms, s, m, or h: " +
                           std::string(suffix));
}

}  // namespace

PositiveDuration PositiveDuration::Parse(std::string_view value) {
  const std::size_t suffix_start = value.find_first_not_of("0123456789");
  if (suffix_start == 0U || suffix_start == std::string_view::npos) {
    throw std::runtime_error(
        "duration must contain a positive integer and explicit unit");
  }
  std::uint64_t amount = 0;
  const std::string_view amount_text = value.substr(0U, suffix_start);
  const auto [end, error] = std::from_chars(
      amount_text.data(), amount_text.data() + amount_text.size(), amount);
  if (error != std::errc() || end != amount_text.data() + amount_text.size()) {
    throw std::runtime_error("invalid duration amount: " +
                             std::string(amount_text));
  }
  const std::uint64_t multiplier = UnitMilliseconds(value.substr(suffix_start));
  const std::uint64_t maximum = static_cast<std::uint64_t>(
      std::numeric_limits<std::chrono::milliseconds::rep>::max());
  if (amount == 0U || amount > maximum / multiplier) {
    throw std::runtime_error("duration is zero or exceeds milliseconds range");
  }
  return FromMilliseconds(amount * multiplier);
}

PositiveDuration PositiveDuration::FromMilliseconds(
    std::uint64_t milliseconds) {
  const std::uint64_t maximum = static_cast<std::uint64_t>(
      std::numeric_limits<std::chrono::milliseconds::rep>::max());
  if (milliseconds == 0U || milliseconds > maximum) {
    throw std::runtime_error("duration is zero or exceeds milliseconds range");
  }
  return PositiveDuration(std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(milliseconds)));
}

PositiveDuration::PositiveDuration(std::chrono::milliseconds value)
    : value_(value) {}

std::chrono::milliseconds PositiveDuration::value() const { return value_; }

}  // namespace bbp
