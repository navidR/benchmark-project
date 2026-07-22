#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace bbp {

struct TuiCommandAdmissionResult {
  bool accepted = false;
  std::uint64_t sequence = 0U;
  std::string feedback;
};

std::string TuiCommandRejectionMessage(std::string_view detail);
TuiCommandAdmissionResult AdmitTuiCommand(
    const std::function<std::uint64_t()>& submit);

}  // namespace bbp
