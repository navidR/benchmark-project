#include "bbp/simulator/process_control_config.h"

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/scenario_fields.h"

namespace bbp {
namespace {

std::uint32_t RequireUint32(const boost::json::object& object,
                            std::string_view field, std::string_view context) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<std::uint32_t>::max()) {
    return static_cast<std::uint32_t>(value->as_uint64());
  }
  if (value != nullptr && value->is_int64() && value->as_int64() >= 0 &&
      static_cast<std::uint64_t>(value->as_int64()) <=
          std::numeric_limits<std::uint32_t>::max()) {
    return static_cast<std::uint32_t>(value->as_int64());
  }
  throw std::runtime_error(std::string(context) + " has no valid " +
                           std::string(field));
}

const boost::json::array* OptionalArray(const boost::json::object& object,
                                        std::string_view field,
                                        std::string_view context) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return nullptr;
  }
  if (!value->is_array()) {
    throw std::runtime_error(std::string(context) + "." + std::string(field) +
                             " must be a JSON array");
  }
  return &value->as_array();
}

const boost::json::object& RequireEntryObject(const boost::json::value& value,
                                              std::string_view field,
                                              std::string_view context) {
  if (!value.is_object()) {
    throw std::runtime_error(std::string(context) + "." + std::string(field) +
                             " entries must be JSON objects");
  }
  return value.as_object();
}

std::uint32_t ParseNodeIndex(const boost::json::object& object,
                             std::uint32_t node_count, std::string_view field,
                             std::string_view context) {
  const std::uint32_t node =
      RequireUint32(object, "node",
                    std::string(context) + "." + std::string(field) + " entry");
  if (node == 0U || node > node_count) {
    throw std::runtime_error(std::string(context) + "." + std::string(field) +
                             " node must be in 1.." +
                             std::to_string(node_count));
  }
  return node - 1U;
}

std::uint64_t OneBasedNodeIndex(std::uint32_t node_index) {
  if (node_index == std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("process node index exceeds canonical uint32");
  }
  return static_cast<std::uint64_t>(node_index) + 1U;
}

}  // namespace

ProcessControlConfig ParseProcessControlConfig(
    const boost::json::object& object, std::uint32_t node_count,
    std::string_view context) {
  if (node_count == 0U) {
    throw std::runtime_error(std::string(context) +
                             " requires at least one node");
  }

  for (const auto& member : object) {
    if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kProcess,
                                    member.key())) {
      throw std::runtime_error(
          std::string(context) +
          " has unsupported field: " + std::string(member.key()));
    }
  }

  ProcessControlConfig config;
  if (const boost::json::array* restarts =
          OptionalArray(object, "runtime_node_restarts", context)) {
    config.restart_node_indexes.reserve(restarts->size());
    for (const boost::json::value& value : *restarts) {
      const boost::json::object& restart =
          RequireEntryObject(value, "runtime_node_restarts", context);
      for (const auto& member : restart) {
        if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kProcessRestart,
                                        member.key())) {
          throw std::runtime_error(
              std::string(context) +
              ".runtime_node_restarts entry has unsupported field: " +
              std::string(member.key()));
        }
      }
      config.restart_node_indexes.push_back(ParseNodeIndex(
          restart, node_count, "runtime_node_restarts", context));
    }
  }

  if (const boost::json::array* freezes =
          OptionalArray(object, "runtime_node_freezes", context)) {
    config.freezes.reserve(freezes->size());
    for (const boost::json::value& value : *freezes) {
      const boost::json::object& freeze =
          RequireEntryObject(value, "runtime_node_freezes", context);
      for (const auto& member : freeze) {
        if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kProcessFreeze,
                                        member.key())) {
          throw std::runtime_error(
              std::string(context) +
              ".runtime_node_freezes entry has unsupported field: " +
              std::string(member.key()));
        }
      }
      const std::uint32_t node_index =
          ParseNodeIndex(freeze, node_count, "runtime_node_freezes", context);
      const std::uint32_t duration_ms =
          RequireUint32(freeze, "duration_ms",
                        std::string(context) + ".runtime_node_freezes entry");
      if (duration_ms == 0U) {
        throw std::runtime_error(
            std::string(context) +
            ".runtime_node_freezes duration_ms must be greater than zero");
      }
      config.freezes.push_back(
          FreezeRequest{.node_index = node_index, .duration_ms = duration_ms});
    }
  }
  return config;
}

boost::json::object ProcessControlConfigJson(
    const ProcessControlConfig& config) {
  boost::json::object object;
  if (!config.restart_node_indexes.empty()) {
    boost::json::array restarts;
    restarts.reserve(config.restart_node_indexes.size());
    for (const std::uint32_t node_index : config.restart_node_indexes) {
      boost::json::object restart;
      restart["node"] = OneBasedNodeIndex(node_index);
      restarts.push_back(std::move(restart));
    }
    object["runtime_node_restarts"] = std::move(restarts);
  }
  if (!config.freezes.empty()) {
    boost::json::array freezes;
    freezes.reserve(config.freezes.size());
    for (const FreezeRequest& freeze : config.freezes) {
      if (freeze.duration_ms == 0U) {
        throw std::runtime_error(
            "process runtime_node_freezes duration_ms must be greater than "
            "zero");
      }
      boost::json::object entry;
      entry["node"] = OneBasedNodeIndex(freeze.node_index);
      entry["duration_ms"] = freeze.duration_ms;
      freezes.push_back(std::move(entry));
    }
    object["runtime_node_freezes"] = std::move(freezes);
  }
  return object;
}

}  // namespace bbp
