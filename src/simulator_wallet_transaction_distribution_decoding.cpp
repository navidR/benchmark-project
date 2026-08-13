#include "simulator_wallet_transaction_distribution_decoding.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <string>
#include <string_view>

#include "bbp/positive_duration.h"
#include "bbp/scenario_fields.h"
#include "bbp/util.h"
#include "simulator_json_field_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

ValueDistributionKind ParseValueDistributionKind(std::string_view value,
                                                 std::string_view field) {
  if (value == ValueDistributionKindName(ValueDistributionKind::kFixed)) {
    return ValueDistributionKind::kFixed;
  }
  if (value == ValueDistributionKindName(ValueDistributionKind::kUniform)) {
    return ValueDistributionKind::kUniform;
  }
  throw std::runtime_error(std::string(field) +
                           " distribution must be fixed or uniform");
}

void ValidateDistributionObjectFields(const boost::json::object& object,
                                      std::string_view field) {
  for (const auto& [name, unused] : object) {
    static_cast<void>(unused);
    if (!ScenarioObjectFieldAllowed(ScenarioObjectKind::kDistribution, name)) {
      throw std::runtime_error(
          std::string(field) +
          " distribution contains unsupported field: " + std::string(name));
    }
  }
}

std::chrono::milliseconds ParseIntervalValue(const boost::json::value& value,
                                             std::string_view field) {
  if (!value.is_string()) {
    throw std::runtime_error(std::string(field) + " must be a duration string");
  }
  return PositiveDuration::Parse(std::string_view(value.as_string())).value();
}

}  // namespace

AmountDistribution ParseAmountDistribution(const boost::json::object& object,
                                           const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing wallet transaction amount");
  }
  if (!value->is_object()) {
    const std::uint64_t amount = JsonFixed8Amount(*value, field);
    return AmountDistribution{
        .kind = ValueDistributionKind::kFixed,
        .minimum_satoshis = amount,
        .maximum_satoshis = amount,
    };
  }

  const boost::json::object& distribution = value->as_object();
  ValidateDistributionObjectFields(distribution, field);
  const ValueDistributionKind kind = ParseValueDistributionKind(
      JsonStringField(distribution, "distribution"), field);
  const boost::json::value* minimum = distribution.if_contains("min");
  const boost::json::value* maximum = distribution.if_contains("max");
  if (minimum == nullptr || maximum == nullptr) {
    throw std::runtime_error(std::string(field) +
                             " distribution requires min and max");
  }
  AmountDistribution result{
      .kind = kind,
      .minimum_satoshis =
          JsonFixed8Amount(*minimum, std::string(field) + ".min"),
      .maximum_satoshis =
          JsonFixed8Amount(*maximum, std::string(field) + ".max"),
  };
  if (result.minimum_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions amount must be greater than zero");
  }
  if (result.minimum_satoshis > result.maximum_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions amount distribution min must be <= max");
  }
  if (result.kind == ValueDistributionKind::kFixed &&
      result.minimum_satoshis != result.maximum_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions fixed amount distribution requires "
        "equal min and max");
  }
  return result;
}

IntervalDistribution ParseIntervalDistribution(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return IntervalDistribution{};
  }
  if (!value->is_object()) {
    const std::chrono::milliseconds interval =
        ParseIntervalValue(*value, field);
    return IntervalDistribution{
        .kind = ValueDistributionKind::kFixed,
        .minimum = interval,
        .maximum = interval,
    };
  }

  const boost::json::object& distribution = value->as_object();
  ValidateDistributionObjectFields(distribution, field);
  const ValueDistributionKind kind = ParseValueDistributionKind(
      JsonStringField(distribution, "distribution"), field);
  const boost::json::value* minimum = distribution.if_contains("min");
  const boost::json::value* maximum = distribution.if_contains("max");
  if (minimum == nullptr || maximum == nullptr) {
    throw std::runtime_error(std::string(field) +
                             " distribution requires min and max");
  }
  IntervalDistribution result{
      .kind = kind,
      .minimum = ParseIntervalValue(*minimum, std::string(field) + ".min"),
      .maximum = ParseIntervalValue(*maximum, std::string(field) + ".max"),
  };
  if (result.minimum > result.maximum) {
    throw std::runtime_error(
        "scenario wallet_transactions interval distribution min must be <= "
        "max");
  }
  if (result.kind == ValueDistributionKind::kFixed &&
      result.minimum != result.maximum) {
    throw std::runtime_error(
        "scenario wallet_transactions fixed interval distribution requires "
        "equal min and max");
  }
  return result;
}

std::vector<std::uint32_t> ParseWalletIndexList(
    const boost::json::object& object, const char* field,
    std::size_t wallet_count) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return {};
  }
  if (!value->is_array()) {
    throw std::runtime_error(std::string("scenario wallet_transactions ") +
                             field + " must be a JSON array");
  }
  std::vector<std::uint32_t> wallets;
  for (const boost::json::value& entry : value->as_array()) {
    const std::uint32_t wallet = JsonUint32Value(entry, field);
    if (wallet == 0U || static_cast<std::size_t>(wallet) > wallet_count) {
      throw std::runtime_error(std::string("scenario wallet_transactions ") +
                               field + " values must be in 1..wallet_count");
    }
    if (std::find(wallets.begin(), wallets.end(), wallet) != wallets.end()) {
      throw std::runtime_error(std::string("scenario wallet_transactions ") +
                               field + " contains a duplicate");
    }
    wallets.push_back(wallet);
  }
  return wallets;
}

}  // namespace bbp::simulator_app_internal
