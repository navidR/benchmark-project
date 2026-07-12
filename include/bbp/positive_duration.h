#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

namespace bbp {

class PositiveDuration {
 public:
  static PositiveDuration Parse(std::string_view value);
  static PositiveDuration FromMilliseconds(std::uint64_t milliseconds);

  std::chrono::milliseconds value() const;

 private:
  explicit PositiveDuration(std::chrono::milliseconds value);

  std::chrono::milliseconds value_;
};

}  // namespace bbp
