#pragma once

#include <stdexcept>

namespace bsim {

class SimulationCancelled final : public std::runtime_error {
 public:
  SimulationCancelled() : std::runtime_error("simulation stop requested") {}
};

}  // namespace bsim
