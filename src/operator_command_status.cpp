#include "bbp/operator_command_status.h"

#include <stdexcept>

namespace bbp {

std::string_view OperatorCommandStatusName(OperatorCommandStatus status) {
  switch (status) {
    case OperatorCommandStatus::kCompleted:
      return "completed";
    case OperatorCommandStatus::kFailed:
      return "failed";
  }
  throw std::runtime_error("unknown operator command status");
}

std::optional<OperatorCommandStatus> OperatorCommandStatusFromName(
    std::string_view name) {
  if (name == "completed") {
    return OperatorCommandStatus::kCompleted;
  }
  if (name == "failed") {
    return OperatorCommandStatus::kFailed;
  }
  return std::nullopt;
}

}  // namespace bbp
