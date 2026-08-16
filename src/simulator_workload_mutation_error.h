#pragma once

#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace bbp::simulator_app_internal {

class WorkloadMutationCancelledAfterRollback final : public std::runtime_error {
 public:
  WorkloadMutationCancelledAfterRollback()
      : std::runtime_error("workload mutation cancellation was rolled back") {}
};

class WorkloadMutationFailedAfterRollback final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class WorkloadMutationOutcomeUnconfirmed final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

[[noreturn]] void RethrowWorkloadMutationAfterVerifiedRollback(
    const std::exception_ptr& error);

[[noreturn]] void ThrowWorkloadMutationOutcomeUnconfirmed(
    std::string_view context, const std::exception_ptr& original_error,
    const std::vector<std::string>& rollback_errors = {});

}  // namespace bbp::simulator_app_internal
