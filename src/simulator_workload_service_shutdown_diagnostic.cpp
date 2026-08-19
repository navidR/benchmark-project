#include "simulator_workload_service_shutdown_diagnostic.h"

#include <boost/json/object.hpp>
#include <chrono>
#include <cstdint>

#include "bbp/mcp_live_application.h"

namespace bbp::simulator_app_internal {

WorkloadServiceShutdownTimeout::WorkloadServiceShutdownTimeout(
    const McpLiveWorkloadDrainResult& result)
    : std::runtime_error(
          "workload service did not drain within the 15000 ms shutdown "
          "bound; referenced simulator state was retained until every "
          "callback and worker exited"),
      active_callback_count_(result.active_callback_count),
      active_worker_count_(result.active_worker_count),
      admission_closed_(result.admission_closed),
      cancellation_requested_(result.cancellation_requested) {}

boost::json::object WorkloadServiceShutdownTimeout::Diagnostic() const {
  return boost::json::object{
      {"code", "workload_service_shutdown_timeout"},
      {"message", what()},
      {"severity", "critical"},
      {"active_callback_count",
       static_cast<std::uint64_t>(active_callback_count_)},
      {"active_worker_count", static_cast<std::uint64_t>(active_worker_count_)},
      {"shutdown_bound_ms",
       static_cast<std::uint64_t>(
           std::chrono::duration_cast<std::chrono::milliseconds>(
               kWorkloadServiceShutdownBound)
               .count())},
      {"admission_closed", admission_closed_},
      {"cancellation_requested", cancellation_requested_},
      {"safe_to_destroy", false},
  };
}

}  // namespace bbp::simulator_app_internal
