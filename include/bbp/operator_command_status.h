#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class OperatorCommandStatus {
  kCompleted,
  kFailed,
};

std::string_view OperatorCommandStatusName(OperatorCommandStatus status);
std::optional<OperatorCommandStatus> OperatorCommandStatusFromName(
    std::string_view name);

}  // namespace bbp
