#include "bbp/tui_command_admission.h"

#include <exception>

namespace bbp {

std::string TuiCommandRejectionMessage(std::string_view detail) {
  return "Command rejected: " + std::string(detail);
}

TuiCommandAdmissionResult AdmitTuiCommand(
    const std::function<std::uint64_t()>& submit) {
  try {
    return TuiCommandAdmissionResult{
        .accepted = true,
        .sequence = submit(),
        .feedback = {},
    };
  } catch (const std::exception& error) {
    return TuiCommandAdmissionResult{
        .accepted = false,
        .sequence = 0U,
        .feedback = TuiCommandRejectionMessage(error.what()),
    };
  }
}

}  // namespace bbp
