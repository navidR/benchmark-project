#pragma once

#include <cstdint>

namespace bbp {

class PeerCountPolicy {
 public:
  PeerCountPolicy(std::uint32_t minimum, std::uint32_t maximum);

  std::uint32_t minimum() const { return minimum_; }
  std::uint32_t maximum() const { return maximum_; }

 private:
  std::uint32_t minimum_;
  std::uint32_t maximum_;
};

}  // namespace bbp
