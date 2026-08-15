#include "simulator_scenario_identifier.h"

#include <stdexcept>
#include <string>

namespace bbp::simulator_app_internal {

void RequireSafeScenarioIdentifier(std::string_view value,
                                   std::string_view field) {
  if (value.empty() || value.size() > 32U) {
    throw std::runtime_error(std::string(field) + " must be 1..32 characters");
  }
  for (const char c : value) {
    const bool safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9') || c == '-' || c == '_';
    if (!safe) {
      throw std::runtime_error(std::string(field) +
                               " contains an unsafe character");
    }
  }
}

}  // namespace bbp::simulator_app_internal
