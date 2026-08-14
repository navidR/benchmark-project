#pragma once

#include <boost/json/object.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

struct ScheduledScenarioEvent;
struct SimulationCommand;
struct SimulationCommandOutcome;

namespace simulator_app_internal {

boost::json::object ScheduledEventLifecycleDetail(
    const ScheduledScenarioEvent& event,
    std::chrono::milliseconds scheduled_wall_delay,
    std::chrono::steady_clock::time_point epoch,
    std::chrono::steady_clock::time_point started,
    std::optional<std::chrono::steady_clock::time_point> finished,
    std::optional<std::string_view> error = std::nullopt);
std::string RestartNodeWorkloadDetail(std::uint32_t workload_index,
                                      std::uint32_t workload_count,
                                      std::uint32_t node,
                                      std::uint64_t restart_count);
std::string FreezeNodeWorkloadDetail(std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     std::uint32_t node,
                                     std::uint32_t duration_ms);
std::string CheckpointWorkloadDetail(std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     std::string_view name,
                                     std::uint32_t node_metric_samples,
                                     std::uint32_t wallet_metric_samples);
std::string ScheduledBlockDetail(const std::vector<std::string>& hashes);
std::filesystem::path NodeReportRelativePath(const SimulationCommand& command);
std::string SimulationCommandDetail(
    const SimulationCommand& command, std::string_view error = {},
    const SimulationCommandOutcome* outcome = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
