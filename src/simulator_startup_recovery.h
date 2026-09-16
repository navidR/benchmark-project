#pragma once

#include <filesystem>
#include <memory>

#include "bbp/run_ownership.h"

namespace bbp::simulator_app_internal {

// The process lock is released by the kernel on death and is not inherited by
// fork children. Keep the record after failure; remove it after orderly
// cleanup.
class RunRecoveryLease {
 public:
  explicit RunRecoveryLease(const RunOwnership& ownership);
  ~RunRecoveryLease();
  RunRecoveryLease(const RunRecoveryLease&) = delete;
  RunRecoveryLease& operator=(const RunRecoveryLease&) = delete;

  void Complete();

 private:
  class State;
  std::unique_ptr<State> state_;

  friend void RecoverStaleRuns(const std::filesystem::path& benchmark_root);
};

void RecoverStaleRuns(const std::filesystem::path& benchmark_root);

}  // namespace bbp::simulator_app_internal
