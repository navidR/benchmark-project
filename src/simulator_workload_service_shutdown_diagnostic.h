#pragma once

#include <boost/json/object.hpp>
#include <chrono>
#include <cstddef>
#include <stdexcept>

namespace bbp {

struct McpLiveWorkloadDrainResult;

namespace simulator_app_internal {

inline constexpr auto kWorkloadServiceShutdownBound = std::chrono::seconds(15);

class WorkloadServiceShutdownTimeout final : public std::runtime_error {
 public:
  explicit WorkloadServiceShutdownTimeout(
      const McpLiveWorkloadDrainResult& result);

  boost::json::object Diagnostic() const;

 private:
  std::size_t active_callback_count_;
  std::size_t active_worker_count_;
  bool admission_closed_;
  bool cancellation_requested_;
};

}  // namespace simulator_app_internal
}  // namespace bbp
