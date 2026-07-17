#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <string_view>
#include <vector>

#include "bbp/simulator/freeze_request.h"

namespace bbp {

struct ProcessControlConfig {
  std::vector<std::uint32_t> restart_node_indexes;
  std::vector<FreezeRequest> freezes;
};

ProcessControlConfig ParseProcessControlConfig(
    const boost::json::object& object, std::uint32_t node_count,
    std::string_view context = "scenario process");

boost::json::object ProcessControlConfigJson(
    const ProcessControlConfig& config);

}  // namespace bbp
