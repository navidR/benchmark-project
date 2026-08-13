#include "simulator_wallet_configuration_decoding.h"

#include <stdexcept>
#include <string>

#include "bbp/scenario_fields.h"
#include "simulator_json_field_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

WalletInitializationStrategy ParseWalletInitializationStrategy(
    std::string_view value) {
  const std::optional<WalletInitializationStrategy> strategy =
      WalletInitializationStrategyFromName(value);
  if (strategy) {
    return *strategy;
  }
  throw std::runtime_error(
      "scenario topology.wallet_initialization strategy must be driver_rpc");
}

WalletPrivacyMode ParseWalletPrivacyMode(std::string_view value) {
  const std::optional<WalletPrivacyMode> mode =
      WalletPrivacyModeFromName(value);
  if (mode) {
    return *mode;
  }
  throw std::runtime_error(
      "scenario topology.wallet_initialization mode must be public or private");
}

}  // namespace

std::optional<ScenarioNodeWalletConfig> ParseScenarioNodeWalletConfig(
    const boost::json::object& node, std::string_view node_id, bool wallet_role,
    const WalletInitialization& global_initialization) {
  const boost::json::value* value = node.if_contains("wallet");
  if (value == nullptr) {
    return std::nullopt;
  }
  if (!value->is_object()) {
    throw std::runtime_error("scenario node " + std::string(node_id) +
                             " wallet must be an object");
  }
  const boost::json::object& object = value->as_object();
  for (const auto& member : object) {
    if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kNodeWallet,
                                    member.key())) {
      throw std::runtime_error(
          "scenario node " + std::string(node_id) +
          " has unsupported wallet field: " + std::string(member.key()));
    }
  }

  ScenarioNodeWalletConfig config;
  config.enabled = JsonOptionalBoolField(object, "enabled", wallet_role);
  config.strategy = global_initialization.strategy;
  config.mode = global_initialization.mode;
  if (config.enabled != wallet_role) {
    throw std::runtime_error(
        "scenario node " + std::string(node_id) +
        " wallet.enabled must match whether the node has a wallet role");
  }

  const boost::json::value* initialization =
      object.if_contains("initialization");
  if (initialization == nullptr) {
    return config;
  }
  if (!config.enabled) {
    throw std::runtime_error(
        "scenario node " + std::string(node_id) +
        " disabled wallet must not specify initialization");
  }
  if (!initialization->is_object()) {
    throw std::runtime_error("scenario node " + std::string(node_id) +
                             " wallet.initialization must be an object");
  }
  const boost::json::object& initialization_object =
      initialization->as_object();
  for (const auto& member : initialization_object) {
    if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kWalletInitialization,
                                    member.key())) {
      throw std::runtime_error(
          "scenario node " + std::string(node_id) +
          " has unsupported wallet.initialization field: " +
          std::string(member.key()));
    }
  }

  if (const boost::json::value* strategy =
          initialization_object.if_contains("strategy")) {
    if (!strategy->is_string()) {
      throw std::runtime_error(
          "scenario node " + std::string(node_id) +
          " wallet.initialization.strategy must be a string");
    }
    const std::optional<WalletInitializationStrategy> parsed =
        WalletInitializationStrategyFromName(strategy->as_string());
    if (!parsed) {
      throw std::runtime_error(
          "scenario node " + std::string(node_id) +
          " wallet.initialization.strategy must be driver_rpc");
    }
    config.strategy = *parsed;
  }
  if (const boost::json::value* mode =
          initialization_object.if_contains("mode")) {
    if (!mode->is_string()) {
      throw std::runtime_error("scenario node " + std::string(node_id) +
                               " wallet.initialization.mode must be a string");
    }
    const std::optional<WalletPrivacyMode> parsed =
        WalletPrivacyModeFromName(mode->as_string());
    if (!parsed) {
      throw std::runtime_error(
          "scenario node " + std::string(node_id) +
          " wallet.initialization.mode must be public or private");
    }
    config.mode = *parsed;
  }
  if (config.strategy != global_initialization.strategy) {
    throw std::runtime_error(
        "scenario node " + std::string(node_id) +
        " wallet initialization strategy must match topology");
  }
  if (config.mode != global_initialization.mode) {
    throw std::runtime_error("scenario node " + std::string(node_id) +
                             " wallet initialization mode must match topology");
  }
  return config;
}

WalletInitialization ParseWalletInitializationObject(
    const boost::json::object& topology) {
  WalletInitialization initialization;
  const boost::json::value* value =
      topology.if_contains("wallet_initialization");
  if (value == nullptr) {
    return initialization;
  }
  if (!value->is_object()) {
    throw std::runtime_error(
        "scenario topology.wallet_initialization must be a JSON object");
  }
  const boost::json::object& object = value->as_object();
  RejectUnsupportedFields(
      object, ScenarioObjectFields(ScenarioObjectKind::kWalletInitialization),
      "scenario topology.wallet_initialization");
  initialization.strategy =
      ParseWalletInitializationStrategy(JsonOptionalStringField(
          object, "strategy",
          WalletInitializationStrategyName(initialization.strategy)));
  initialization.mode = ParseWalletPrivacyMode(JsonOptionalStringField(
      object, "mode", WalletPrivacyModeName(initialization.mode)));
  return initialization;
}

}  // namespace bbp::simulator_app_internal
