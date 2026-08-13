#pragma once

#include <boost/json/value.hpp>
#include <filesystem>
#include <string>

namespace bbp::simulator_app_internal {

boost::json::value ParseYamlDocument(std::string input,
                                     const std::filesystem::path& source);

}  // namespace bbp::simulator_app_internal
