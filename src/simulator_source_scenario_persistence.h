#pragma once

#include <boost/json/object.hpp>
#include <filesystem>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

struct Options;

namespace simulator_app_internal {

void WriteSourceScenarioFile(const boost::json::object& source_scenario,
                             const std::string& run_id,
                             const std::filesystem::path& run_root,
                             std::optional<int> reserved_run_root_fd,
                             const Options* options = nullptr);

boost::json::object LoadRetainedSourceScenario(
    const std::filesystem::path& source_root, std::string_view source_run_id,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
