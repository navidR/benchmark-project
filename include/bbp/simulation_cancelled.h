#pragma once

#include <stdexcept>

namespace bbp {

class SimulationCancelled final : public std::runtime_error {
 public:
  SimulationCancelled() : std::runtime_error("simulation stop requested") {}
};

}  // namespace bbp
