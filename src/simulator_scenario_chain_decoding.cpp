#include "simulator_scenario_chain_decoding.h"

#include <boost/json/value.hpp>
#include <stdexcept>
#include <string>

#include "bbp/chain_kind.h"
#include "bbp/scenario_chain.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulator/options.h"
#include "simulator_json_field_decoding.h"
#include "simulator_resource_profile_decoding.h"

namespace bbp::simulator_app_internal {

void ParseScenarioChains(const boost::json::object& scenario,
                         Options* options) {
  const boost::json::value* value = scenario.if_contains("chains");
  if (value == nullptr) {
    return;
  }
  if (!value->is_object()) {
    throw std::runtime_error("scenario chains must be an object");
  }
  const boost::json::object& chains = value->as_object();
  if (chains.empty()) {
    throw std::runtime_error("scenario chains must not be empty");
  }
  for (const auto& [name_json, definition_value] : chains) {
    const std::string name(name_json);
    RequireSafeScenarioIdentifier(name, "scenario chain name");
    const ChainKind chain = ParseChainKind(name);
    if (!definition_value.is_object()) {
      throw std::runtime_error("scenario chain " + name +
                               " definition must be an object");
    }
    const boost::json::object& definition = definition_value.as_object();
    RejectUnsupportedFields(
        definition, ScenarioObjectFields(ScenarioObjectKind::kChainDefinition),
        "scenario chain " + name + " definition");
    const ChainKind driver =
        ParseChainKind(JsonStringField(definition, "driver"));
    if (driver != chain) {
      throw std::runtime_error("scenario chain " + name +
                               " driver must match the registry key");
    }
    const std::string default_binary =
        JsonStringField(definition, "default_binary");
    if (default_binary.empty()) {
      throw std::runtime_error("scenario chain " + name +
                               " default_binary must not be empty");
    }
    if (default_binary.find('\0') != std::string::npos) {
      throw std::runtime_error("scenario chain " + name +
                               " default_binary must not contain NUL");
    }
    const auto [unused, inserted] = options->chains.emplace(
        name,
        ScenarioChain{.driver = driver, .default_binary = default_binary});
    static_cast<void>(unused);
    if (!inserted) {
      throw std::runtime_error("scenario chains contains duplicate name: " +
                               name);
    }
  }
}

}  // namespace bbp::simulator_app_internal
