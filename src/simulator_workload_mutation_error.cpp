#include "simulator_workload_mutation_error.h"

#include <exception>
#include <string>

namespace bbp::simulator_app_internal {
namespace {

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

}  // namespace

[[noreturn]] void ThrowWorkloadMutationOutcomeUnconfirmed(
    std::string_view context, const std::exception_ptr& original_error,
    const std::vector<std::string>& rollback_errors) {
  std::string message(context);
  if (original_error) {
    message += ": " + ExceptionMessage(original_error);
  }
  if (!rollback_errors.empty()) {
    message += "; rollback failed:";
    for (const std::string& rollback_error : rollback_errors) {
      message += " [" + rollback_error + "]";
    }
  }
  throw WorkloadMutationOutcomeUnconfirmed(message);
}

}  // namespace bbp::simulator_app_internal
