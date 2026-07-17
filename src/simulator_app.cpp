#include "bbp/simulator_app.h"

#include <linux/capability.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <yaml.h>

#include <algorithm>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/program_options.hpp>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/capability.h"
#include "bbp/cgroup.h"
#include "bbp/default_peer_topology.h"
#include "bbp/drivers/chain_command_executor.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/log_tail.h"
#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/network_allocation_lock.h"
#include "bbp/node_log_collector.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/perf_counter.h"
#include "bbp/periodic_metrics_collector.h"
#include "bbp/positive_duration.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/process.h"
#include "bbp/run_report.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/signal_stop_monitor.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_processor.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/legacy_cli_inputs.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/process_control_config.h"
#include "bbp/simulator/wallet_transaction_plan.h"
#include "bbp/simulator/yaml_helpers.h"
#include "bbp/tui.h"
#include "bbp/util.h"

namespace bbp {
namespace {

std::mutex node_network_state_mutex;
std::mutex node_resource_state_mutex;

void ThrowIfStopRequested(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
}

void WaitForDuration(std::chrono::milliseconds duration,
                     std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  std::condition_variable_any condition;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  condition.wait_for(lock, stop_token, duration, [] { return false; });
  ThrowIfStopRequested(stop_token);
}

void WaitUntil(std::chrono::steady_clock::time_point deadline,
               std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (std::chrono::steady_clock::now() >= deadline) {
    return;
  }
  std::condition_variable_any condition;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  condition.wait_until(lock, stop_token, deadline, [] { return false; });
  ThrowIfStopRequested(stop_token);
}

std::chrono::steady_clock::time_point SteadyDeadline(
    std::chrono::steady_clock::time_point epoch,
    std::chrono::milliseconds delay) {
  const auto remaining = std::chrono::steady_clock::time_point::max() - epoch;
  const auto maximum_delay =
      std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
  if (delay > maximum_delay) {
    throw std::runtime_error(
        "scheduled event time exceeds monotonic clock range");
  }
  return epoch +
         std::chrono::duration_cast<std::chrono::steady_clock::duration>(delay);
}

std::uint64_t ElapsedMilliseconds(
    std::chrono::steady_clock::time_point epoch,
    std::chrono::steady_clock::time_point timestamp) {
  if (timestamp <= epoch) {
    return 0U;
  }
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(timestamp - epoch)
          .count();
  return static_cast<std::uint64_t>(elapsed);
}

boost::json::object ScheduledEventLifecycleDetail(
    const ScheduledScenarioEvent& event,
    std::chrono::milliseconds scheduled_wall_delay,
    std::chrono::steady_clock::time_point epoch,
    std::chrono::steady_clock::time_point started,
    std::optional<std::chrono::steady_clock::time_point> finished,
    std::optional<std::string_view> error = std::nullopt) {
  const std::uint64_t scheduled_at_ms =
      static_cast<std::uint64_t>(event.at.count());
  const std::uint64_t scheduled_wall_at_ms =
      static_cast<std::uint64_t>(scheduled_wall_delay.count());
  const std::uint64_t started_at_ms = ElapsedMilliseconds(epoch, started);
  boost::json::object detail;
  detail["sequence"] = event.sequence;
  detail["action"] = std::string(WorkloadKindName(event.action.kind));
  detail["scheduled_at_ms"] = scheduled_at_ms;
  detail["scheduled_wall_at_ms"] = scheduled_wall_at_ms;
  detail["started_at_ms"] = started_at_ms;
  detail["lateness_ms"] = started_at_ms > scheduled_wall_at_ms
                              ? started_at_ms - scheduled_wall_at_ms
                              : 0U;
  if (finished) {
    const std::uint64_t finished_at_ms = ElapsedMilliseconds(epoch, *finished);
    detail["finished_at_ms"] = finished_at_ms;
    detail["duration_ms"] =
        finished_at_ms > started_at_ms ? finished_at_ms - started_at_ms : 0U;
  }
  if (error) {
    detail["error"] = *error;
  }
  return detail;
}

uint32_t JsonUint32Field(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint32 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint32 JSON field: " +
                           std::string(field));
}

uint32_t JsonOptionalUint32Field(const boost::json::object& object,
                                 const char* field, uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint32 JSON field: " + std::string(field));
}

uint32_t JsonOptionalNullableUint32Field(const boost::json::object& object,
                                         const char* field,
                                         uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_null()) {
    return default_value;
  }
  return JsonOptionalUint32Field(object, field, default_value);
}

uint64_t JsonOptionalUint64Field(const boost::json::object& object,
                                 const char* field, uint64_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint64 JSON field: " + std::string(field));
}

double JsonOptionalDoubleField(const boost::json::object& object,
                               const char* field, double default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_double()) {
    return value->as_double();
  }
  if (value->is_uint64()) {
    return static_cast<double>(value->as_uint64());
  }
  if (value->is_int64()) {
    return static_cast<double>(value->as_int64());
  }
  throw std::runtime_error("invalid numeric JSON field: " + std::string(field));
}

std::optional<double> JsonOptionalNullableDoubleField(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || value->is_null()) {
    return std::nullopt;
  }
  return JsonOptionalDoubleField(object, field, 0.0);
}

uint32_t JsonPercentBasisPoints(const boost::json::object& object,
                                const char* field) {
  const double percent = JsonOptionalDoubleField(object, field, 0.0);
  if (!std::isfinite(percent) || percent < 0.0 || percent > 100.0) {
    throw std::runtime_error(std::string(field) + " must be in 0..100");
  }
  const double scaled = percent * 100.0;
  const double integral = std::round(scaled);
  if (std::fabs(scaled - integral) > 1e-9) {
    throw std::runtime_error(std::string(field) +
                             " must use at most 0.01 percent resolution");
  }
  return static_cast<uint32_t>(integral);
}

uint64_t JsonUint64Field(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint64 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint64 JSON field: " +
                           std::string(field));
}

uint64_t JsonUint64Value(const boost::json::value& value,
                         std::string_view field) {
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<uint64_t>(value.as_int64());
  }
  throw std::runtime_error("invalid uint64 JSON field: " + std::string(field));
}

uint32_t JsonUint32Value(const boost::json::value& value,
                         std::string_view field) {
  const uint64_t parsed = JsonUint64Value(value, field);
  if (parsed > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("invalid uint32 JSON field: " +
                             std::string(field));
  }
  return static_cast<uint32_t>(parsed);
}

std::optional<uint64_t> JsonOptionalUint64FieldValue(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  return JsonUint64Value(*value, field);
}

bool JsonOptionalBoolField(const boost::json::object& object, const char* field,
                           bool default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (!value->is_bool()) {
    throw std::runtime_error("invalid bool JSON field: " + std::string(field));
  }
  return value->as_bool();
}

std::filesystem::path JsonOptionalPathField(
    const boost::json::object& object, const char* field,
    const std::filesystem::path& default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (!value->is_string()) {
    throw std::runtime_error("invalid path JSON field: " + std::string(field));
  }
  return std::filesystem::path(std::string(value->as_string()));
}

std::string JsonStringField(const boost::json::object& object,
                            const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    throw std::runtime_error("missing or invalid string JSON field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

std::string JsonOptionalStringField(const boost::json::object& object,
                                    const char* field,
                                    std::string_view default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::string(default_value);
  }
  if (!value->is_string()) {
    throw std::runtime_error("invalid string JSON field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

uint64_t JsonAmountField(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid fixed-8 amount JSON field: " +
                             std::string(field));
  }
  return JsonFixed8Amount(*value, field);
}

uint64_t JsonOptionalAmountField(const boost::json::object& object,
                                 const char* field, uint64_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  return JsonFixed8Amount(*value, field);
}

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
    if (name != "distribution" && name != "min" && name != "max") {
      throw std::runtime_error(
          std::string(field) +
          " distribution contains unsupported field: " + std::string(name));
    }
  }
}

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

std::chrono::milliseconds ParseIntervalValue(const boost::json::value& value,
                                             std::string_view field) {
  if (!value.is_string()) {
    throw std::runtime_error(std::string(field) + " must be a duration string");
  }
  return PositiveDuration::Parse(std::string_view(value.as_string())).value();
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

std::vector<uint32_t> JsonNodeGroupField(const boost::json::object& object,
                                         const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("missing or invalid node group JSON field: " +
                             std::string(field));
  }
  std::vector<uint32_t> nodes;
  for (const boost::json::value& node_value : value->as_array()) {
    const uint64_t raw_node = JsonUint64Value(node_value, field);
    if (raw_node > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error("partition node value exceeds uint32");
    }
    const uint32_t node = static_cast<uint32_t>(raw_node);
    if (node == 0U) {
      throw std::runtime_error(
          "partition node values must be greater than zero");
    }
    for (uint32_t existing : nodes) {
      if (existing == node - 1U) {
        throw std::runtime_error("partition node group contains a duplicate");
      }
    }
    nodes.push_back(node - 1U);
  }
  if (nodes.empty()) {
    throw std::runtime_error("partition node groups must not be empty");
  }
  return nodes;
}

std::optional<std::vector<uint32_t>> JsonOptionalNodeIndexListField(
    const boost::json::object& object, const char* field,
    std::string_view source) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (!value->is_array()) {
    throw std::runtime_error(std::string(source) + " must be a JSON array");
  }

  std::vector<uint32_t> nodes;
  for (const boost::json::value& node_value : value->as_array()) {
    const uint64_t raw_node = JsonUint64Value(node_value, field);
    if (raw_node > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error(std::string(source) +
                               " node value exceeds uint32");
    }
    const uint32_t node = static_cast<uint32_t>(raw_node);
    if (node == 0U) {
      throw std::runtime_error(std::string(source) +
                               " node values must be greater than zero");
    }
    for (uint32_t existing : nodes) {
      if (existing == node - 1U) {
        throw std::runtime_error(std::string(source) + " contains a duplicate");
      }
    }
    nodes.push_back(node - 1U);
  }
  return nodes;
}

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

std::string YamlScalarText(const yaml_event_t& event) {
  return std::string(reinterpret_cast<const char*>(event.data.scalar.value),
                     event.data.scalar.length);
}

std::string LowerAscii(std::string_view text) {
  std::string lower;
  lower.reserve(text.size());
  for (const char c : text) {
    if (c >= 'A' && c <= 'Z') {
      lower.push_back(static_cast<char>(c - 'A' + 'a'));
    } else {
      lower.push_back(c);
    }
  }
  return lower;
}

bool IsDecimalInteger(std::string_view text) {
  if (text.empty()) {
    return false;
  }
  size_t offset = 0;
  if (text.front() == '-') {
    if (text.size() == 1U) {
      return false;
    }
    offset = 1U;
  }
  for (size_t i = offset; i < text.size(); ++i) {
    if (text[i] < '0' || text[i] > '9') {
      return false;
    }
  }
  return true;
}

boost::json::value ParseYamlPlainScalar(std::string_view text) {
  const std::string lower = LowerAscii(text);
  if (text.empty() || text == "~" || lower == "null") {
    return nullptr;
  }
  if (lower == "true") {
    return true;
  }
  if (lower == "false") {
    return false;
  }
  if (!IsDecimalInteger(text)) {
    double value = 0.0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(),
                                        value, std::chars_format::general);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size() &&
        std::isfinite(value)) {
      return value;
    }
    return boost::json::string(text);
  }
  if (text.front() == '-') {
    int64_t value = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size()) {
      return value;
    }
  } else {
    uint64_t value = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size()) {
      return value;
    }
  }
  return boost::json::string(text);
}

boost::json::value ParseYamlValue(YamlParser* parser, YamlEvent event);

boost::json::array ParseYamlSequence(YamlParser* parser) {
  boost::json::array array;
  while (true) {
    YamlEvent event = parser->Next();
    if (event.Type() == YAML_SEQUENCE_END_EVENT) {
      break;
    }
    array.push_back(ParseYamlValue(parser, std::move(event)));
  }
  return array;
}

boost::json::object ParseYamlMapping(YamlParser* parser) {
  boost::json::object object;
  while (true) {
    YamlEvent key_event = parser->Next();
    if (key_event.Type() == YAML_MAPPING_END_EVENT) {
      break;
    }
    if (key_event.Type() != YAML_SCALAR_EVENT) {
      throw std::runtime_error("YAML mapping keys must be scalars");
    }
    const std::string key = YamlScalarText(key_event.Raw());
    if (object.if_contains(key) != nullptr) {
      throw std::runtime_error("duplicate YAML mapping key: " + key);
    }
    object[key] = ParseYamlValue(parser, parser->Next());
  }
  return object;
}

boost::json::value ParseYamlValue(YamlParser* parser, YamlEvent event) {
  if (event.Type() == YAML_SCALAR_EVENT) {
    const std::string text = YamlScalarText(event.Raw());
    if (event.Raw().data.scalar.style != YAML_PLAIN_SCALAR_STYLE) {
      return boost::json::string(text);
    }
    return ParseYamlPlainScalar(text);
  }
  if (event.Type() == YAML_SEQUENCE_START_EVENT) {
    return ParseYamlSequence(parser);
  }
  if (event.Type() == YAML_MAPPING_START_EVENT) {
    return ParseYamlMapping(parser);
  }
  if (event.Type() == YAML_ALIAS_EVENT) {
    throw std::runtime_error("YAML aliases are not supported");
  }
  throw std::runtime_error("unexpected YAML event while parsing scenario");
}

void RequireYamlEvent(const YamlEvent& event, yaml_event_type_t expected,
                      std::string_view message) {
  if (event.Type() != expected) {
    throw std::runtime_error(std::string(message));
  }
}

boost::json::value ParseYamlDocument(std::string input,
                                     const std::filesystem::path& source) {
  YamlParser parser(std::move(input), source.string());
  RequireYamlEvent(parser.Next(), YAML_STREAM_START_EVENT,
                   "YAML stream did not start");
  YamlEvent document_start = parser.Next();
  if (document_start.Type() == YAML_STREAM_END_EVENT) {
    return nullptr;
  }
  RequireYamlEvent(document_start, YAML_DOCUMENT_START_EVENT,
                   "YAML document did not start");

  boost::json::value root;
  YamlEvent root_event = parser.Next();
  if (root_event.Type() == YAML_DOCUMENT_END_EVENT) {
    root = nullptr;
  } else {
    root = ParseYamlValue(&parser, std::move(root_event));
    RequireYamlEvent(parser.Next(), YAML_DOCUMENT_END_EVENT,
                     "YAML document did not end");
  }
  RequireYamlEvent(parser.Next(), YAML_STREAM_END_EVENT,
                   "YAML scenario must contain exactly one document");
  return root;
}

NetworkCondition ParseNetworkConditionObject(
    const boost::json::object& object) {
  NetworkCondition condition;
  condition.bandwidth_mbps = JsonOptionalUint32Field(object, "bandwidth_mbps",
                                                     condition.bandwidth_mbps);
  condition.delay_ms =
      JsonOptionalUint32Field(object, "delay_ms", condition.delay_ms);
  condition.jitter_ms =
      JsonOptionalUint32Field(object, "jitter_ms", condition.jitter_ms);
  const bool loss_basis_points_present =
      object.if_contains("loss_basis_points") != nullptr;
  const bool loss_percent_present =
      object.if_contains("loss_percent") != nullptr;
  if (loss_basis_points_present && loss_percent_present) {
    throw std::runtime_error(
        "network condition loss_percent and loss_basis_points must not both "
        "be specified");
  }
  condition.loss_basis_points =
      loss_percent_present
          ? JsonPercentBasisPoints(object, "loss_percent")
          : JsonOptionalUint32Field(object, "loss_basis_points",
                                    condition.loss_basis_points);
  condition.duplicate_basis_points = JsonOptionalUint32Field(
      object, "duplicate_basis_points", condition.duplicate_basis_points);
  condition.corrupt_basis_points = JsonOptionalUint32Field(
      object, "corrupt_basis_points", condition.corrupt_basis_points);
  condition.reorder_basis_points = JsonOptionalUint32Field(
      object, "reorder_basis_points", condition.reorder_basis_points);
  condition.limit_packets =
      JsonOptionalUint32Field(object, "limit_packets", condition.limit_packets);
  return condition;
}

uint32_t StableRuleHandle(const NetworkBlockRule& rule) {
  uint32_t hash = 2166136261U;
  const auto mix_byte = [&hash](unsigned char value) {
    hash ^= value;
    hash *= 16777619U;
  };
  const auto mix_uint32 = [&mix_byte](uint32_t value) {
    for (uint32_t shift = 0; shift < 32U; shift += 8U) {
      mix_byte(static_cast<unsigned char>((value >> shift) & 0xFFU));
    }
  };
  mix_uint32(rule.node_index + 1U);
  for (const unsigned char c : rule.src_address) {
    mix_byte(c);
  }
  for (const unsigned char c : rule.dst_address) {
    mix_byte(c);
  }
  mix_uint32(rule.dst_port);
  hash &= 0x00FFFFFFU;
  return hash == 0U ? 1U : hash;
}

NetworkBlockRule ParseNetworkBlockRuleObject(
    const boost::json::object& object) {
  const uint32_t node = JsonUint32Field(object, "node");
  if (node == 0U) {
    throw std::runtime_error(
        "network block rule node must be greater than zero");
  }
  const uint32_t dst_port = JsonUint32Field(object, "dst_port");
  if (dst_port == 0U || dst_port > 65535U) {
    throw std::runtime_error("network block rule dst_port must be 1..65535");
  }

  NetworkBlockRule rule;
  rule.node_index = node - 1U;
  const boost::json::value* src_address = object.if_contains("src_address");
  if (src_address != nullptr) {
    if (!src_address->is_string()) {
      throw std::runtime_error(
          "network block rule src_address must be a string");
    }
    rule.src_address = std::string(src_address->as_string());
  }
  rule.dst_address = JsonStringField(object, "dst_address");
  ValidateIpv4Address(rule.dst_address, "network block destination");
  if (!rule.src_address.empty()) {
    ValidateIpv4Address(rule.src_address, "network block source");
  }
  rule.dst_port = static_cast<uint16_t>(dst_port);
  rule.handle = JsonOptionalUint32Field(object, "handle", 0U);
  if (rule.handle == 0U) {
    rule.handle = StableRuleHandle(rule);
  }
  return rule;
}

NetworkPartitionRule ParseNetworkPartitionRuleObject(
    const boost::json::object& object) {
  NetworkPartitionRule rule;
  rule.group_a = JsonNodeGroupField(object, "group_a");
  rule.group_b = JsonNodeGroupField(object, "group_b");
  for (uint32_t a : rule.group_a) {
    for (uint32_t b : rule.group_b) {
      if (a == b) {
        throw std::runtime_error(
            "partition groups must not contain the same node");
      }
    }
  }
  return rule;
}

void ValidateNetworkPartitionRule(const NetworkPartitionRule& rule,
                                  uint32_t nodes, std::string_view source) {
  for (uint32_t node_index : rule.group_a) {
    if (node_index >= nodes) {
      throw std::runtime_error(std::string(source) +
                               " group_a node must be in 1.." +
                               std::to_string(nodes));
    }
  }
  for (uint32_t node_index : rule.group_b) {
    if (node_index >= nodes) {
      throw std::runtime_error(std::string(source) +
                               " group_b node must be in 1.." +
                               std::to_string(nodes));
    }
  }
}

std::vector<uint32_t> ConsecutiveNodeIndexes(uint32_t first_index,
                                             uint32_t count) {
  std::vector<uint32_t> nodes;
  nodes.reserve(count);
  for (uint32_t offset = 0; offset < count; ++offset) {
    nodes.push_back(first_index + offset);
  }
  return nodes;
}

void ValidateRoleNodeList(const std::vector<uint32_t>& role_nodes,
                          uint32_t node_count, std::string_view source) {
  for (uint32_t node_index : role_nodes) {
    if (node_index >= node_count) {
      throw std::runtime_error(std::string(source) + " must be in 1.." +
                               std::to_string(node_count));
    }
  }
}

bool NodeListsOverlap(const std::vector<uint32_t>& left,
                      const std::vector<uint32_t>& right) {
  for (uint32_t left_node : left) {
    for (uint32_t right_node : right) {
      if (left_node == right_node) {
        return true;
      }
    }
  }
  return false;
}

bool NodeListContains(const std::vector<uint32_t>& nodes, uint32_t node_index) {
  for (uint32_t candidate : nodes) {
    if (candidate == node_index) {
      return true;
    }
  }
  return false;
}

std::string NodeRoleName(const Options& options, std::uint32_t node_index) {
  if (!options.node_roles.empty()) {
    return options.node_roles.at(node_index);
  }
  const bool wallet =
      NodeListContains(options.topology.wallet_nodes, node_index);
  const bool miner = NodeListContains(options.topology.miner_nodes, node_index);
  return wallet && miner ? "wallet_miner"
         : wallet        ? "wallet"
         : miner         ? "miner"
                         : "base";
}

const PeerConnectivityPolicy* FindPeerConnectivityPolicy(
    const NodeRoleTopology& topology, uint32_t node_index) {
  for (const PeerConnectivityPolicy& policy : topology.peer_connectivity) {
    if (policy.node == node_index) {
      return &policy;
    }
  }
  return nullptr;
}

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

WalletFundingStrategy ParseWalletFundingStrategy(std::string_view value) {
  const std::optional<WalletFundingStrategy> strategy =
      WalletFundingStrategyFromName(value);
  if (strategy) {
    return *strategy;
  }
  throw std::runtime_error(
      "scenario wallet_transactions funding_strategy must be round_robin or "
      "random");
}

WalletTransferStrategy ParseWalletTransferStrategy(std::string_view value) {
  const std::optional<WalletTransferStrategy> strategy =
      WalletTransferStrategyFromName(value);
  if (strategy) {
    return *strategy;
  }
  throw std::runtime_error(
      "scenario wallet_transactions strategy must be round_robin, random, "
      "fanout, or hotspot");
}

std::string_view PeerConnectivityModeName(PeerConnectivityMode mode) {
  switch (mode) {
    case PeerConnectivityMode::kFixedCount:
      return "fixed_count";
    case PeerConnectivityMode::kAllPeers:
      return "all_peers";
  }
  throw std::runtime_error("unknown peer connectivity mode");
}

ChainWalletMode ToChainWalletMode(const WalletInitialization& initialization) {
  switch (initialization.mode) {
    case WalletPrivacyMode::kPublic:
      return ChainWalletMode::kPublic;
    case WalletPrivacyMode::kPrivate:
      return ChainWalletMode::kPrivate;
  }
  throw std::runtime_error("unknown wallet privacy mode");
}

PeerConnectivityPolicy ParsePeerConnectivityPolicyObject(
    const boost::json::object& object, uint32_t node_count) {
  const uint32_t node = JsonUint32Field(object, "node");
  if (node == 0U || node > node_count) {
    throw std::runtime_error(
        "scenario topology.peer_connectivity node must be in 1..node_count");
  }
  PeerConnectivityPolicy policy;
  policy.node = node - 1U;
  const bool all_peers = JsonOptionalBoolField(object, "all_peers", false);
  const bool min_peer_count_present =
      object.if_contains("min_peer_count") != nullptr;
  const bool max_peer_count_present =
      object.if_contains("max_peer_count") != nullptr;
  if (all_peers && (min_peer_count_present || max_peer_count_present)) {
    throw std::runtime_error(
        "scenario topology.peer_connectivity cannot combine all_peers with "
        "minimum or maximum peer counts");
  }
  if (all_peers) {
    policy.mode = PeerConnectivityMode::kAllPeers;
    policy.peer_count = PeerCountPolicy(node_count - 1U, node_count - 1U);
    return policy;
  }
  if (!max_peer_count_present) {
    throw std::runtime_error(
        "scenario topology.peer_connectivity requires all_peers or "
        "max_peer_count");
  }
  policy.mode = PeerConnectivityMode::kFixedCount;
  const uint32_t maximum = JsonUint32Field(object, "max_peer_count");
  const uint32_t minimum =
      JsonOptionalUint32Field(object, "min_peer_count", maximum);
  if (maximum >= node_count) {
    throw std::runtime_error(
        "scenario topology.peer_connectivity max_peer_count must be less than "
        "node_count");
  }
  if (minimum > maximum) {
    throw std::runtime_error(
        "scenario topology.peer_connectivity min_peer_count must not exceed "
        "max_peer_count");
  }
  policy.peer_count = PeerCountPolicy(minimum, maximum);
  return policy;
}

std::vector<PeerConnectivityPolicy> ParsePeerConnectivityPolicies(
    const boost::json::object& object, uint32_t node_count) {
  const boost::json::value* value = object.if_contains("peer_connectivity");
  if (value == nullptr) {
    return {};
  }
  if (!value->is_array()) {
    throw std::runtime_error(
        "scenario topology.peer_connectivity must be a JSON array");
  }
  std::vector<PeerConnectivityPolicy> policies;
  for (const boost::json::value& item : value->as_array()) {
    if (!item.is_object()) {
      throw std::runtime_error(
          "scenario topology.peer_connectivity entries must be objects");
    }
    PeerConnectivityPolicy policy =
        ParsePeerConnectivityPolicyObject(item.as_object(), node_count);
    for (const PeerConnectivityPolicy& existing : policies) {
      if (existing.node == policy.node) {
        throw std::runtime_error(
            "scenario topology.peer_connectivity contains duplicate node");
      }
    }
    policies.push_back(policy);
  }
  return policies;
}

PeerTopologyKind ParsePeerTopologyKind(std::string_view name) {
  const std::optional<PeerTopologyKind> kind = PeerTopologyKindFromName(name);
  if (!kind) {
    throw std::runtime_error(
        "scenario topology.type must be full_mesh, ring, star, random_graph, "
        "scale_free_graph, latency_matrix, custom_edge_list, "
        "partitioned_groups, or internet_like_region_graph");
  }
  return *kind;
}

std::vector<std::vector<uint32_t>> ParseTopologyNodeGroups(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("scenario topology." + std::string(field) +
                             " must be a JSON array");
  }
  std::vector<std::vector<uint32_t>> groups;
  for (const boost::json::value& group_value : value->as_array()) {
    if (!group_value.is_array()) {
      throw std::runtime_error("scenario topology." + std::string(field) +
                               " entries must be JSON arrays");
    }
    boost::json::object wrapper;
    wrapper["nodes"] = group_value;
    groups.push_back(JsonNodeGroupField(wrapper, "nodes"));
  }
  return groups;
}

uint32_t ParseTopologyNode(const boost::json::object& object, const char* field,
                           uint32_t node_count) {
  const uint32_t node = JsonUint32Field(object, field);
  if (node == 0U || node > node_count) {
    throw std::runtime_error("scenario topology " + std::string(field) +
                             " must be in 1..node_count");
  }
  return node - 1U;
}

PeerTopologyEdge ParsePeerTopologyEdge(const boost::json::object& object,
                                       uint32_t node_count) {
  PeerTopologyEdge edge;
  edge.from = ParseTopologyNode(object, "from", node_count);
  edge.to = ParseTopologyNode(object, "to", node_count);
  edge.bidirectional =
      JsonOptionalBoolField(object, "bidirectional", edge.bidirectional);
  edge.active = JsonOptionalBoolField(object, "active", edge.active);
  const boost::json::value* latency = object.if_contains("latency_ms");
  if (latency != nullptr) {
    edge.latency_ms = JsonUint32Value(*latency, "latency_ms");
  }
  constexpr std::string_view kConditionFields[] = {
      "bandwidth_mbps",
      "delay_ms",
      "jitter_ms",
      "loss_basis_points",
      "duplicate_basis_points",
      "corrupt_basis_points",
      "reorder_basis_points",
      "limit_packets",
  };
  bool condition_present = edge.latency_ms.has_value();
  for (std::string_view field : kConditionFields) {
    condition_present =
        condition_present || object.if_contains(field) != nullptr;
  }
  if (condition_present) {
    NetworkCondition condition = ParseNetworkConditionObject(object);
    if (edge.latency_ms) {
      const boost::json::value* delay = object.if_contains("delay_ms");
      if (delay != nullptr && condition.delay_ms != *edge.latency_ms) {
        throw std::runtime_error(
            "scenario topology edge latency_ms and delay_ms must match");
      }
      condition.delay_ms = *edge.latency_ms;
    }
    ValidateNetworkCondition(condition);
    edge.condition = condition;
  }
  return edge;
}

constexpr std::string_view kTopologyEdgeConditionFields[] = {
    "latency_ms",
    "bandwidth_mbps",
    "delay_ms",
    "jitter_ms",
    "loss_basis_points",
    "duplicate_basis_points",
    "corrupt_basis_points",
    "reorder_basis_points",
    "limit_packets",
};

bool HasTopologyEdgeConditionField(const boost::json::object& object) {
  return std::any_of(std::begin(kTopologyEdgeConditionFields),
                     std::end(kTopologyEdgeConditionFields),
                     [&](std::string_view field) {
                       return object.if_contains(field) != nullptr;
                     });
}

NetworkCondition ParseTopologyEdgeWorkloadCondition(
    const boost::json::object& object) {
  if (!HasTopologyEdgeConditionField(object)) {
    throw std::runtime_error(
        "scenario set_edge_condition requires condition fields");
  }
  NetworkCondition condition = ParseNetworkConditionObject(object);
  const boost::json::value* latency = object.if_contains("latency_ms");
  if (latency != nullptr) {
    const std::uint32_t latency_ms = JsonUint32Value(*latency, "latency_ms");
    const boost::json::value* delay = object.if_contains("delay_ms");
    if (delay != nullptr && condition.delay_ms != latency_ms) {
      throw std::runtime_error(
          "scenario topology edge action latency_ms and delay_ms must match");
    }
    condition.delay_ms = latency_ms;
  }
  ValidateNetworkCondition(condition);
  return condition;
}

void RejectTopologyEdgeConditionFields(const boost::json::object& object,
                                       std::string_view action) {
  if (HasTopologyEdgeConditionField(object)) {
    throw std::runtime_error("scenario " + std::string(action) +
                             " does not accept condition fields");
  }
}

std::vector<PeerTopologyEdge> ParsePeerTopologyEdges(
    const boost::json::object& object, uint32_t node_count) {
  const boost::json::value* value = object.if_contains("edges");
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("scenario topology.edges must be a JSON array");
  }
  std::vector<PeerTopologyEdge> edges;
  for (const boost::json::value& edge_value : value->as_array()) {
    if (!edge_value.is_object()) {
      throw std::runtime_error(
          "scenario topology.edges entries must be JSON objects");
    }
    edges.push_back(ParsePeerTopologyEdge(edge_value.as_object(), node_count));
  }
  return edges;
}

std::vector<std::vector<std::optional<uint32_t>>> ParseLatencyMatrix(
    const boost::json::object& object) {
  const boost::json::value* value = object.if_contains("latency_matrix_ms");
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error(
        "scenario topology.latency_matrix_ms must be a JSON array");
  }
  std::vector<std::vector<std::optional<uint32_t>>> matrix;
  for (const boost::json::value& row_value : value->as_array()) {
    if (!row_value.is_array()) {
      throw std::runtime_error(
          "scenario topology.latency_matrix_ms rows must be JSON arrays");
    }
    std::vector<std::optional<uint32_t>> row;
    for (const boost::json::value& cell : row_value.as_array()) {
      if (cell.is_null()) {
        row.push_back(std::nullopt);
      } else {
        row.push_back(JsonUint32Value(cell, "latency_matrix_ms"));
      }
    }
    matrix.push_back(std::move(row));
  }
  return matrix;
}

std::vector<PeerTopologyRegionEdge> ParsePeerTopologyRegionEdges(
    const boost::json::object& object, uint32_t region_count) {
  const boost::json::value* value = object.if_contains("region_edges");
  if (value == nullptr) {
    return {};
  }
  if (!value->is_array()) {
    throw std::runtime_error(
        "scenario topology.region_edges must be a JSON array");
  }
  std::vector<PeerTopologyRegionEdge> edges;
  for (const boost::json::value& edge_value : value->as_array()) {
    if (!edge_value.is_object()) {
      throw std::runtime_error(
          "scenario topology.region_edges entries must be JSON objects");
    }
    const boost::json::object& edge_object = edge_value.as_object();
    const uint32_t from = JsonUint32Field(edge_object, "from_region");
    const uint32_t to = JsonUint32Field(edge_object, "to_region");
    if (from == 0U || from > region_count || to == 0U || to > region_count) {
      throw std::runtime_error(
          "scenario topology region edge must reference a configured region");
    }
    PeerTopologyRegionEdge edge;
    edge.from_region = from - 1U;
    edge.to_region = to - 1U;
    edge.bidirectional =
        JsonOptionalBoolField(edge_object, "bidirectional", edge.bidirectional);
    edge.active = JsonOptionalBoolField(edge_object, "active", edge.active);
    edges.push_back(edge);
  }
  return edges;
}

PeerTopologyConfig ParsePeerTopologyConfig(const boost::json::object& object,
                                           uint32_t node_count,
                                           std::uint64_t default_seed) {
  PeerTopologyConfig topology;
  topology.kind = ParsePeerTopologyKind(
      JsonOptionalStringField(object, "type", "full_mesh"));
  topology.seed = JsonOptionalUint64Field(object, "seed", default_seed);
  switch (topology.kind) {
    case PeerTopologyKind::kFullMesh:
    case PeerTopologyKind::kRing:
      break;
    case PeerTopologyKind::kStar: {
      const uint32_t center_node =
          JsonOptionalUint32Field(object, "center_node", 1U);
      if (center_node == 0U || center_node > node_count) {
        throw std::runtime_error(
            "scenario topology center_node must be in 1..node_count");
      }
      topology.star_center = center_node - 1U;
    } break;
    case PeerTopologyKind::kRandomGraph:
      topology.average_degree = JsonUint32Field(object, "average_degree");
      break;
    case PeerTopologyKind::kScaleFreeGraph:
      topology.average_degree =
          JsonOptionalUint32Field(object, "average_degree", 0U);
      topology.attachment_count =
          JsonOptionalUint32Field(object, "attachment_count", 0U);
      break;
    case PeerTopologyKind::kLatencyMatrix:
      topology.latency_matrix_ms = ParseLatencyMatrix(object);
      break;
    case PeerTopologyKind::kCustomEdgeList:
      topology.edges = ParsePeerTopologyEdges(object, node_count);
      break;
    case PeerTopologyKind::kPartitionedGroups:
      topology.groups = ParseTopologyNodeGroups(object, "groups");
      break;
    case PeerTopologyKind::kInternetLikeRegionGraph:
      topology.regions = ParseTopologyNodeGroups(object, "regions");
      if (topology.regions.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(
            "scenario topology region count exceeds uint32");
      }
      topology.region_edges = ParsePeerTopologyRegionEdges(
          object, static_cast<uint32_t>(topology.regions.size()));
      break;
  }
  ResolvePeerTopologyEdges(topology, node_count);
  return topology;
}

void ResolvePeerPolicyEligibility(NodeRoleTopology* topology) {
  for (PeerConnectivityPolicy& policy : topology->peer_connectivity) {
    const std::vector<uint32_t> eligible = ResolvePeerTopologyPeerIndexes(
        topology->peer_topology, topology->node_count, policy.node);
    if (policy.mode == PeerConnectivityMode::kAllPeers) {
      if (eligible.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("eligible peer count exceeds uint32");
      }
      const uint32_t count = static_cast<uint32_t>(eligible.size());
      policy.peer_count = PeerCountPolicy(count, count);
    } else if (policy.peer_count.minimum() > eligible.size()) {
      throw std::runtime_error(
          "scenario topology.peer_connectivity min_peer_count exceeds the "
          "node's eligible logical topology peers");
    }
  }
}

NodeRoleTopology ParseNodeRoleTopologyObject(const boost::json::object& object,
                                             uint32_t nodes,
                                             std::uint64_t default_seed) {
  NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = JsonOptionalUint32Field(object, "node_count", nodes);
  if (topology.node_count != nodes) {
    throw std::runtime_error(
        "scenario topology.node_count must match node count");
  }

  std::optional<std::vector<uint32_t>> wallet_nodes =
      JsonOptionalNodeIndexListField(object, "wallet_nodes",
                                     "scenario topology.wallet_nodes");
  std::optional<std::vector<uint32_t>> miner_nodes =
      JsonOptionalNodeIndexListField(object, "miner_nodes",
                                     "scenario topology.miner_nodes");
  const bool wallet_count_present =
      object.if_contains("wallet_node_count") != nullptr;
  const bool miner_count_present =
      object.if_contains("miner_node_count") != nullptr;
  const uint32_t wallet_nodes_size =
      wallet_nodes ? static_cast<uint32_t>(wallet_nodes->size()) : 0U;
  const uint32_t miner_nodes_size =
      miner_nodes ? static_cast<uint32_t>(miner_nodes->size()) : 0U;

  topology.wallet_node_count = JsonOptionalUint32Field(
      object, "wallet_node_count", wallet_nodes ? wallet_nodes_size : 0U);
  topology.miner_node_count = JsonOptionalUint32Field(
      object, "miner_node_count", miner_nodes ? miner_nodes_size : 0U);
  topology.allow_miner_wallet_overlap =
      JsonOptionalBoolField(object, "allow_miner_wallet_overlap",
                            topology.allow_miner_wallet_overlap);

  if (wallet_nodes && wallet_count_present &&
      topology.wallet_node_count != wallet_nodes_size) {
    throw std::runtime_error(
        "scenario topology wallet_node_count must match wallet_nodes size");
  }
  if (miner_nodes && miner_count_present &&
      topology.miner_node_count != miner_nodes_size) {
    throw std::runtime_error(
        "scenario topology miner_node_count must match miner_nodes size");
  }
  if (topology.wallet_node_count > topology.node_count) {
    throw std::runtime_error(
        "scenario topology wallet_node_count must be <= node_count");
  }
  if (topology.miner_node_count > topology.node_count) {
    throw std::runtime_error(
        "scenario topology miner_node_count must be <= node_count");
  }
  if (!topology.allow_miner_wallet_overlap &&
      topology.wallet_node_count >
          topology.node_count - topology.miner_node_count) {
    throw std::runtime_error(
        "scenario topology wallet_node_count plus miner_node_count must be <= "
        "node_count when overlap is disabled");
  }

  topology.wallet_nodes =
      wallet_nodes ? *wallet_nodes
                   : ConsecutiveNodeIndexes(0U, topology.wallet_node_count);
  const uint32_t first_miner_index =
      topology.allow_miner_wallet_overlap ? 0U : topology.wallet_node_count;
  topology.miner_nodes =
      miner_nodes ? *miner_nodes
                  : ConsecutiveNodeIndexes(first_miner_index,
                                           topology.miner_node_count);

  ValidateRoleNodeList(topology.wallet_nodes, topology.node_count,
                       "scenario topology.wallet_nodes");
  ValidateRoleNodeList(topology.miner_nodes, topology.node_count,
                       "scenario topology.miner_nodes");
  if (!topology.allow_miner_wallet_overlap &&
      NodeListsOverlap(topology.wallet_nodes, topology.miner_nodes)) {
    throw std::runtime_error(
        "scenario topology wallet_nodes and miner_nodes overlap but "
        "allow_miner_wallet_overlap is false");
  }
  topology.peer_topology =
      ParsePeerTopologyConfig(object, topology.node_count, default_seed);
  topology.peer_connectivity =
      ParsePeerConnectivityPolicies(object, topology.node_count);
  ResolvePeerPolicyEligibility(&topology);
  return topology;
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
  initialization.strategy =
      ParseWalletInitializationStrategy(JsonOptionalStringField(
          object, "strategy",
          WalletInitializationStrategyName(initialization.strategy)));
  initialization.mode = ParseWalletPrivacyMode(JsonOptionalStringField(
      object, "mode", WalletPrivacyModeName(initialization.mode)));
  initialization.seed =
      JsonOptionalStringField(object, "seed", initialization.seed);
  return initialization;
}

void ValidateWalletTransactionsWorkload(
    const WalletTransactionsWorkload& workload, const Options& options) {
  if (!options.topology.configured) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires topology");
  }
  if (options.topology.miner_nodes.empty()) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires at least one "
        "MinerNode");
  }
  const size_t wallet_count = options.topology.wallet_nodes.size();
  if (wallet_count < 2U) {
    throw std::runtime_error(
        "scenario wallet_transactions workload requires at least two "
        "WalletNode roles");
  }
  if (workload.transaction_count == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions transaction_count must be greater than "
        "zero");
  }
  const std::uint64_t coinbase_confirmations =
      ChainDriverSpecFor(options.chain).coinbase_spendable_confirmations;
  if (workload.funding_blocks_per_wallet < coinbase_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_blocks_per_wallet must be at "
        "least " +
        std::to_string(coinbase_confirmations));
  }
  if (workload.readiness_confirmations < coinbase_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions readiness_confirmations must be at "
        "least " +
        std::to_string(coinbase_confirmations));
  }
  if (workload.funding_blocks_per_wallet < workload.readiness_confirmations) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_blocks_per_wallet must be >= "
        "readiness_confirmations");
  }
  if (workload.amount.minimum_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions amount must be greater than zero");
  }
  if (workload.amount.minimum_satoshis > workload.amount.maximum_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions amount distribution min must be <= max");
  }
  if (workload.amount.kind == ValueDistributionKind::kFixed &&
      workload.amount.minimum_satoshis != workload.amount.maximum_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions fixed amount distribution requires "
        "equal min and max");
  }
  if (workload.interval.minimum.count() < 0 ||
      workload.interval.minimum > workload.interval.maximum) {
    throw std::runtime_error(
        "scenario wallet_transactions interval distribution min must be <= "
        "max");
  }
  if (workload.interval.kind == ValueDistributionKind::kFixed &&
      workload.interval.minimum != workload.interval.maximum) {
    throw std::runtime_error(
        "scenario wallet_transactions fixed interval distribution requires "
        "equal min and max");
  }
  if (workload.fee_satoshis == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions fee must be greater than zero");
  }
  if (workload.amount.maximum_satoshis >
      std::numeric_limits<uint64_t>::max() - workload.fee_satoshis) {
    throw std::runtime_error(
        "scenario wallet_transactions amount plus fee overflows uint64");
  }
  const std::uint64_t minimum_funding_threshold =
      workload.amount.maximum_satoshis + workload.fee_satoshis;
  if (workload.funding_threshold_satoshis < minimum_funding_threshold) {
    throw std::runtime_error(
        "scenario wallet_transactions funding_threshold must cover amount "
        "plus fee");
  }
  if (workload.timeout_sec == 0U) {
    throw std::runtime_error(
        "scenario wallet_transactions timeout_sec must be greater than zero");
  }

  switch (workload.strategy) {
    case WalletTransferStrategy::kRoundRobin:
    case WalletTransferStrategy::kRandom:
      if (!workload.sender_wallets.empty() ||
          !workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions wallet selectors require fanout "
            "or hotspot strategy");
      }
      break;
    case WalletTransferStrategy::kFanout:
      if (workload.sender_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout requires sender_wallets");
      }
      if (!workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout does not accept "
            "receiver_wallets");
      }
      if (workload.sender_wallets.size() >= wallet_count) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout sender_wallets must leave "
            "at least one receiver");
      }
      break;
    case WalletTransferStrategy::kHotspot:
      if (workload.receiver_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot requires receiver_wallets");
      }
      if (!workload.sender_wallets.empty()) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot does not accept "
            "sender_wallets");
      }
      if (workload.receiver_wallets.size() >= wallet_count) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot receiver_wallets must "
            "leave at least one sender");
      }
      break;
  }

  static_cast<void>(BuildWalletTransactionPlan(wallet_count, workload));

  SimulationRegistry::FromTopology(options.topology,
                                   options.wallet_initialization);
}

void RequireNonZero(uint64_t value, std::string_view field) {
  if (value == 0U) {
    throw std::runtime_error(std::string(field) + " must be greater than zero");
  }
}

void RequireCgroupWeight(uint64_t value, std::string_view field) {
  if (value < 1U || value > 10000U) {
    throw std::runtime_error(std::string(field) + " must be in 1..10000");
  }
}

std::optional<uint64_t> JsonRequiredNullablePositiveUint64(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing io.max field: " + std::string(field));
  }
  if (value->is_null()) {
    return std::nullopt;
  }
  const uint64_t parsed = JsonUint64Value(*value, field);
  RequireNonZero(parsed, field);
  return parsed;
}

std::vector<IoLimit> ParseIoLimits(const boost::json::value& value,
                                   std::string_view field) {
  if (!value.is_array()) {
    throw std::runtime_error(std::string(field) + " must be a JSON array");
  }
  std::vector<IoLimit> limits;
  std::set<BlockDeviceId> devices;
  for (const boost::json::value& entry : value.as_array()) {
    if (!entry.is_object()) {
      throw std::runtime_error(std::string(field) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = entry.as_object();
    IoLimit limit;
    limit.device = ParseBlockDeviceId(JsonStringField(object, "device"));
    if (!devices.insert(limit.device).second) {
      throw std::runtime_error(std::string(field) +
                               " contains a duplicate block device: " +
                               BlockDeviceIdText(limit.device));
    }
    limit.read_bytes_per_sec =
        JsonRequiredNullablePositiveUint64(object, "read_bytes_per_sec");
    limit.write_bytes_per_sec =
        JsonRequiredNullablePositiveUint64(object, "write_bytes_per_sec");
    limit.read_operations_per_sec =
        JsonRequiredNullablePositiveUint64(object, "read_operations_per_sec");
    limit.write_operations_per_sec =
        JsonRequiredNullablePositiveUint64(object, "write_operations_per_sec");
    limits.push_back(std::move(limit));
  }
  return limits;
}

ResourceLimitPatch ParseResourceLimitPatchObject(
    const boost::json::object& object) {
  ResourceLimitPatch patch;
  patch.memory_high_bytes =
      JsonOptionalUint64FieldValue(object, "memory_high_bytes");
  patch.memory_max_bytes =
      JsonOptionalUint64FieldValue(object, "memory_max_bytes");
  const boost::json::value* quota = object.if_contains("cpu_quota_us");
  if (quota != nullptr) {
    patch.cpu_quota_present = true;
    if (!quota->is_null()) {
      patch.cpu_quota_us = JsonUint64Value(*quota, "cpu_quota_us");
    }
  }
  patch.cpu_period_us = JsonOptionalUint64FieldValue(object, "cpu_period_us");
  patch.cpu_weight = JsonOptionalUint64FieldValue(object, "cpu_weight");
  patch.io_weight = JsonOptionalUint64FieldValue(object, "io_weight");
  const boost::json::value* io_limits = object.if_contains("io_max");
  if (io_limits != nullptr) {
    patch.io_limits_present = true;
    patch.io_limits = ParseIoLimits(*io_limits, "io_max");
  }
  patch.pids_max = JsonOptionalUint64FieldValue(object, "pids_max");

  ValidateResourceLimitPatch(patch);
  return patch;
}

ResourceLimits InitialResourceLimits(const Options& options);
ResourceLimits ApplyResourceLimitPatch(const ResourceLimits& current,
                                       const ResourceLimitPatch& patch,
                                       const std::string& node_id);

void RequireSafeScenarioIdentifier(std::string_view value,
                                   std::string_view field) {
  if (value.empty() || value.size() > 32U) {
    throw std::runtime_error(std::string(field) + " must be 1..32 characters");
  }
  for (const char c : value) {
    const bool safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9') || c == '-' || c == '_';
    if (!safe) {
      throw std::runtime_error(std::string(field) +
                               " contains an unsafe character");
    }
  }
}

uint64_t ParsePositiveUint64Text(std::string_view text,
                                 std::string_view field) {
  uint64_t value = 0U;
  const char* const begin = text.data();
  const char* const end = begin + text.size();
  const auto [next, error] = std::from_chars(begin, end, value);
  if (text.empty() || error != std::errc() || next != end || value == 0U) {
    throw std::runtime_error(std::string(field) + " must be a positive uint64");
  }
  return value;
}

uint64_t ParseBinaryByteSize(const boost::json::value& value,
                             std::string_view field) {
  if (!value.is_string()) {
    throw std::runtime_error(std::string(field) +
                             " must be a binary byte-size string");
  }
  const std::string text(value.as_string());
  const std::pair<std::string_view, uint64_t> suffixes[] = {
      {"TiB", 1ULL << 40U},
      {"GiB", 1ULL << 30U},
      {"MiB", 1ULL << 20U},
      {"KiB", 1ULL << 10U},
      {"B", 1U}};
  for (const auto& [suffix, multiplier] : suffixes) {
    if (text.size() <= suffix.size() ||
        std::string_view(text).substr(text.size() - suffix.size()) != suffix) {
      continue;
    }
    const std::string_view magnitude(text.data(), text.size() - suffix.size());
    const uint64_t parsed = ParsePositiveUint64Text(magnitude, field);
    if (parsed > std::numeric_limits<uint64_t>::max() / multiplier) {
      throw std::runtime_error(std::string(field) + " overflows uint64 bytes");
    }
    return parsed * multiplier;
  }
  throw std::runtime_error(std::string(field) +
                           " must use B, KiB, MiB, GiB, or TiB");
}

void ApplyCpuMaxAlias(const boost::json::value& value, std::string_view field,
                      boost::json::object* canonical) {
  if (!value.is_string()) {
    throw std::runtime_error(std::string(field) + " must be a string");
  }
  std::istringstream input(std::string(value.as_string()));
  std::string quota_text;
  std::string period_text;
  std::string extra;
  if (!(input >> quota_text >> period_text) || (input >> extra)) {
    throw std::runtime_error(std::string(field) +
                             " must contain exactly quota-or-max and period");
  }
  if (quota_text == "max") {
    (*canonical)["cpu_quota_us"] = nullptr;
  } else {
    (*canonical)["cpu_quota_us"] = ParsePositiveUint64Text(quota_text, field);
  }
  (*canonical)["cpu_period_us"] = ParsePositiveUint64Text(period_text, field);
}

ResourceLimits ParseResourceProfile(const boost::json::object& object,
                                    const ResourceLimits& defaults,
                                    std::string_view profile_name) {
  boost::json::object canonical = object;
  const bool memory_high_alias = object.if_contains("memory_high") != nullptr;
  const bool memory_max_alias = object.if_contains("memory_max") != nullptr;
  if (memory_high_alias && object.if_contains("memory_high_bytes") != nullptr) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " specifies both memory_high and "
                             "memory_high_bytes");
  }
  if (memory_max_alias && object.if_contains("memory_max_bytes") != nullptr) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " specifies both memory_max and "
                             "memory_max_bytes");
  }
  if (memory_high_alias) {
    canonical["memory_high_bytes"] = ParseBinaryByteSize(
        object.at("memory_high"), "resource profile memory_high");
    canonical.erase("memory_high");
  }
  if (memory_max_alias) {
    canonical["memory_max_bytes"] = ParseBinaryByteSize(
        object.at("memory_max"), "resource profile memory_max");
    canonical.erase("memory_max");
  }

  const bool cpu_quota_alias = object.if_contains("cpu_quota") != nullptr;
  const bool cpu_max_alias = object.if_contains("cpu_max") != nullptr;
  if (cpu_quota_alias && cpu_max_alias) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " specifies both cpu_quota and cpu_max");
  }
  if ((cpu_quota_alias || cpu_max_alias) &&
      (object.if_contains("cpu_quota_us") != nullptr ||
       object.if_contains("cpu_period_us") != nullptr)) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " CPU aliases conflict with cpu_quota_us or "
                             "cpu_period_us");
  }
  if (cpu_quota_alias || cpu_max_alias) {
    const char* const alias = cpu_quota_alias ? "cpu_quota" : "cpu_max";
    ApplyCpuMaxAlias(object.at(alias), "resource profile " + std::string(alias),
                     &canonical);
    canonical.erase(alias);
  }

  ResourceLimitPatch patch = ParseResourceLimitPatchObject(canonical);
  if (patch.memory_max_bytes && !patch.memory_high_bytes &&
      defaults.memory_high_bytes > *patch.memory_max_bytes) {
    patch.memory_high_bytes = *patch.memory_max_bytes;
  }
  return ApplyResourceLimitPatch(
      defaults, patch, "resource profile " + std::string(profile_name));
}

void ParseResourceProfiles(const boost::json::object& scenario,
                           Options* options) {
  const boost::json::value* value = scenario.if_contains("resource_profiles");
  if (value == nullptr) {
    return;
  }
  if (!value->is_object()) {
    throw std::runtime_error("scenario resource_profiles must be an object");
  }
  const ResourceLimits defaults = InitialResourceLimits(*options);
  for (const auto& [name_json, profile_value] : value->as_object()) {
    const std::string name(name_json);
    RequireSafeScenarioIdentifier(name, "resource profile name");
    if (!profile_value.is_object()) {
      throw std::runtime_error("scenario resource profile " + name +
                               " must be an object");
    }
    options->resource_profiles.emplace(
        name, ParseResourceProfile(profile_value.as_object(), defaults, name));
  }
}

void ParseNetworkProfiles(const boost::json::object& scenario,
                          Options* options) {
  const boost::json::value* value = scenario.if_contains("network_profiles");
  if (value == nullptr) {
    return;
  }
  if (!value->is_object()) {
    throw std::runtime_error("scenario network_profiles must be an object");
  }
  for (const auto& [name_json, profile_value] : value->as_object()) {
    const std::string name(name_json);
    RequireSafeScenarioIdentifier(name, "network profile name");
    if (!profile_value.is_object()) {
      throw std::runtime_error("scenario network profile " + name +
                               " must be an object");
    }
    const NetworkCondition condition =
        ParseNetworkConditionObject(profile_value.as_object());
    ValidateNetworkCondition(condition);
    options->network_profiles.emplace(name, condition);
  }
}

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

struct ScenarioNodeRoles {
  bool configured = false;
  std::vector<uint32_t> wallet_nodes;
  std::vector<uint32_t> miner_nodes;
};

ScenarioNodeRoles ParseScenarioNodes(
    const boost::json::object& scenario,
    const boost::program_options::variables_map& vm, Options* options) {
  ScenarioNodeRoles roles;
  const boost::json::value* nodes_value = scenario.if_contains("nodes");
  if (nodes_value == nullptr || !nodes_value->is_array()) {
    if (!OptionProvided(vm, "nodes")) {
      const bool nodes_present = nodes_value != nullptr;
      const bool node_count_present =
          scenario.if_contains("node_count") != nullptr;
      const uint32_t scenario_nodes =
          JsonOptionalUint32Field(scenario, "nodes", options->nodes);
      const uint32_t scenario_node_count =
          JsonOptionalUint32Field(scenario, "node_count", scenario_nodes);
      if (nodes_present && node_count_present &&
          scenario_nodes != scenario_node_count) {
        throw std::runtime_error("scenario nodes and node_count must match");
      }
      options->nodes =
          node_count_present ? scenario_node_count : scenario_nodes;
    }
    return roles;
  }

  const boost::json::array& nodes = nodes_value->as_array();
  if (nodes.empty()) {
    throw std::runtime_error("scenario nodes array must not be empty");
  }
  if (nodes.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("scenario nodes array exceeds uint32 size");
  }
  const uint32_t node_count = static_cast<uint32_t>(nodes.size());
  if (OptionProvided(vm, "nodes") && options->nodes != node_count) {
    throw std::runtime_error("--nodes must match scenario nodes array size");
  }
  if (scenario.if_contains("node_count") != nullptr &&
      JsonUint32Field(scenario, "node_count") != node_count) {
    throw std::runtime_error(
        "scenario node_count must match scenario nodes array size");
  }
  options->nodes = node_count;
  options->node_ids.reserve(nodes.size());
  options->node_roles.reserve(nodes.size());
  std::set<std::string> node_ids;
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    const boost::json::value& node_value = nodes[index];
    if (!node_value.is_object()) {
      throw std::runtime_error("scenario nodes entries must be objects");
    }
    const boost::json::object& node = node_value.as_object();
    const std::string id = JsonStringField(node, "id");
    RequireSafeScenarioIdentifier(id, "scenario node id");
    if (!node_ids.insert(id).second) {
      throw std::runtime_error("scenario nodes contains duplicate id: " + id);
    }
    if (ParseChainKind(JsonStringField(node, "chain")) != options->chain) {
      throw std::runtime_error("scenario node " + id +
                               " chain must match the active chain");
    }
    std::string role = JsonStringField(node, "role");
    if (role == "node") {
      role = "base";
    }
    if (role == "wallet") {
      roles.wallet_nodes.push_back(static_cast<uint32_t>(index));
    } else if (role == "miner") {
      roles.miner_nodes.push_back(static_cast<uint32_t>(index));
    } else if (role != "base") {
      throw std::runtime_error("scenario node " + id +
                               " role must be base, node, wallet, or miner");
    }

    const auto read_profile = [&](const char* section,
                                  std::map<uint32_t, std::string>* output) {
      const boost::json::value* section_value = node.if_contains(section);
      if (section_value == nullptr) {
        return;
      }
      if (!section_value->is_object()) {
        throw std::runtime_error("scenario node " + id + " " + section +
                                 " must be an object");
      }
      const std::string profile =
          JsonStringField(section_value->as_object(), "profile");
      RequireSafeScenarioIdentifier(profile,
                                    std::string(section) + " profile name");
      output->emplace(static_cast<uint32_t>(index), profile);
    };
    read_profile("resources", &options->node_resource_profiles);
    read_profile("network", &options->node_network_profiles);
    options->node_ids.push_back(id);
    options->node_roles.push_back(role);
  }
  roles.configured = true;
  return roles;
}

bool SameNodeSet(std::vector<uint32_t> left, std::vector<uint32_t> right) {
  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());
  return left == right;
}

void ResolveNodeProfileAssignments(Options* options) {
  for (const auto& [node_index, profile_name] :
       options->node_resource_profiles) {
    const auto profile = options->resource_profiles.find(profile_name);
    if (profile == options->resource_profiles.end()) {
      throw std::runtime_error(
          "scenario node " + options->node_ids.at(node_index) +
          " references unknown resource profile: " + profile_name);
    }
    options->node_resource_limits.emplace(node_index, profile->second);
  }
  for (const auto& [node_index, profile_name] :
       options->node_network_profiles) {
    const auto profile = options->network_profiles.find(profile_name);
    if (profile == options->network_profiles.end()) {
      throw std::runtime_error(
          "scenario node " + options->node_ids.at(node_index) +
          " references unknown network profile: " + profile_name);
    }
    if (options->node_network_conditions.contains(node_index)) {
      throw std::runtime_error(
          "scenario node " + options->node_ids.at(node_index) +
          " cannot combine network.profile with network.node_conditions");
    }
    options->node_network_conditions.emplace(node_index, profile->second);
    options->isolate_network = true;
  }
}

void ValidateProfileSwitchReferences(Options* options) {
  const auto validate = [&](const ScenarioWorkload& workload) {
    if (workload.kind == WorkloadKind::kSetResourceProfile) {
      if (!options->resource_profiles.contains(
              workload.profile_switch.profile)) {
        throw std::runtime_error(
            "scenario set_resource_profile references unknown profile: " +
            workload.profile_switch.profile);
      }
    } else if (workload.kind == WorkloadKind::kSetNetworkProfile) {
      if (!options->network_profiles.contains(
              workload.profile_switch.profile)) {
        throw std::runtime_error(
            "scenario set_network_profile references unknown profile: " +
            workload.profile_switch.profile);
      }
      options->isolate_network = true;
    }
  };
  for (const ScenarioWorkload& workload : options->workloads) {
    validate(workload);
  }
  for (const ScheduledScenarioEvent& event : options->scheduled_events) {
    validate(event.action);
  }
}

void ApplyNodeConditions(const boost::json::array& conditions, uint32_t nodes,
                         std::string_view source,
                         std::map<uint32_t, NetworkCondition>& output) {
  for (const boost::json::value& value : conditions) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output[node - 1U] = ParseNetworkConditionObject(object);
  }
}

void ApplyNetworkBlockRules(const boost::json::array& rules, uint32_t nodes,
                            std::string_view source,
                            std::vector<NetworkBlockRule>& output) {
  for (const boost::json::value& value : rules) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    NetworkBlockRule rule = ParseNetworkBlockRuleObject(value.as_object());
    if (rule.node_index >= nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output.push_back(std::move(rule));
  }
}

void ApplyNetworkPartitionRules(const boost::json::array& rules, uint32_t nodes,
                                std::string_view source,
                                std::vector<NetworkPartitionRule>& output) {
  for (const boost::json::value& value : rules) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    NetworkPartitionRule rule =
        ParseNetworkPartitionRuleObject(value.as_object());
    ValidateNetworkPartitionRule(rule, nodes, source);
    output.push_back(std::move(rule));
  }
}

void ApplyResourceLimitPatches(const boost::json::array& updates,
                               uint32_t nodes, std::string_view source,
                               std::map<uint32_t, ResourceLimitPatch>& output) {
  for (const boost::json::value& value : updates) {
    if (!value.is_object()) {
      throw std::runtime_error(std::string(source) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(source) + " node must be in 1.." +
                               std::to_string(nodes));
    }
    output[node - 1U] = ParseResourceLimitPatchObject(object);
  }
}

std::string ScenarioNodeId(const Options& options, uint32_t node_index) {
  if (node_index >= options.nodes) {
    throw std::runtime_error("scenario node index is out of range");
  }
  if (!options.node_ids.empty()) {
    return options.node_ids.at(node_index);
  }
  return ChainDriverSpecFor(options.chain).node_id_prefix + "-" +
         std::to_string(node_index + 1U);
}

bool NodeHasScenarioRole(const Options& options, uint32_t node_index,
                         std::string_view role) {
  if (role == "miner") {
    return NodeListContains(options.topology.miner_nodes, node_index);
  }
  if (role == "wallet") {
    return NodeListContains(options.topology.wallet_nodes, node_index);
  }
  if (role == "base" || role == "node") {
    return !NodeListContains(options.topology.miner_nodes, node_index) &&
           !NodeListContains(options.topology.wallet_nodes, node_index);
  }
  throw std::runtime_error(
      "profile switch role selector must be role:base, role:node, "
      "role:wallet, or role:miner");
}

ProfileSwitchWorkload ParseProfileSwitchWorkload(
    const boost::json::object& object, const Options& options,
    WorkloadKind kind) {
  ProfileSwitchWorkload workload;
  workload.profile = JsonStringField(object, "profile");
  RequireSafeScenarioIdentifier(workload.profile, "profile switch name");
  const boost::json::value* targets = object.if_contains("nodes");
  if (targets == nullptr) {
    throw std::runtime_error("scenario " + std::string(WorkloadKindName(kind)) +
                             " requires nodes");
  }

  std::vector<std::string> requested_ids;
  if (targets->is_array()) {
    if (targets->as_array().empty()) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " nodes must not be empty");
    }
    for (const boost::json::value& target : targets->as_array()) {
      if (!target.is_string()) {
        throw std::runtime_error("scenario " +
                                 std::string(WorkloadKindName(kind)) +
                                 " nodes entries must be node ID strings");
      }
      requested_ids.emplace_back(target.as_string());
    }
  } else if (targets->is_string()) {
    const std::string selector(targets->as_string());
    constexpr std::string_view kRolePrefix = "role:";
    if (!std::string_view(selector).starts_with(kRolePrefix)) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " string nodes selector must start with role:");
    }
    const std::string_view role =
        std::string_view(selector).substr(kRolePrefix.size());
    for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
      if (NodeHasScenarioRole(options, node_index, role)) {
        requested_ids.push_back(ScenarioNodeId(options, node_index));
      }
    }
    if (requested_ids.empty()) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " role selector resolved no nodes");
    }
  } else {
    throw std::runtime_error("scenario " + std::string(WorkloadKindName(kind)) +
                             " nodes must be an ID array or role selector");
  }

  std::set<std::string> unique_ids;
  for (const std::string& requested_id : requested_ids) {
    if (!unique_ids.insert(requested_id).second) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " nodes contains duplicate ID: " + requested_id);
    }
    bool found = false;
    for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
      if (ScenarioNodeId(options, node_index) == requested_id) {
        workload.nodes.push_back(node_index + 1U);
        workload.node_ids.push_back(requested_id);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error("scenario " +
                               std::string(WorkloadKindName(kind)) +
                               " references unknown node ID: " + requested_id);
    }
  }
  return workload;
}

void ApplyScenarioWorkloads(const boost::json::array& workloads,
                            const boost::program_options::variables_map& vm,
                            Options& options) {
  for (const boost::json::value& value : workloads) {
    if (!value.is_object()) {
      throw std::runtime_error(
          "scenario workloads entries must be JSON objects");
    }
    const boost::json::object& workload = value.as_object();
    const std::string type_name = JsonStringField(workload, "type");
    const std::optional<WorkloadKind> kind = ParseWorkloadKind(type_name);
    if (!kind) {
      throw std::runtime_error("unsupported scenario workload type: " +
                               type_name);
    }
    if (*kind == WorkloadKind::kBlockGeneration) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP block_generation workload uses node, not nodes");
      }
      BlockGenerationWorkload block_generation;
      block_generation.count = JsonUint32Field(workload, "count");
      block_generation.node =
          OptionProvided(vm, "generate-node")
              ? options.generate_node
              : JsonOptionalUint32Field(workload, "node",
                                        options.generate_node);
      block_generation.sync_timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "sync_timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kBlockGeneration;
      scenario_workload.block_generation = block_generation;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kWaitUntilHeight) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP wait_until_height workload uses node, not nodes");
      }
      WaitUntilHeightWorkload wait;
      wait.node = JsonOptionalUint32Field(workload, "node", wait.node);
      wait.height = JsonUint64Field(workload, "height");
      wait.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWaitUntilHeight;
      scenario_workload.wait_until_height = wait;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kWaitForPeers) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP wait_for_peers workload uses node, not nodes");
      }
      WaitForPeersWorkload wait;
      wait.node = JsonOptionalUint32Field(workload, "node", wait.node);
      wait.peer_count = JsonUint64Field(workload, "peer_count");
      wait.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWaitForPeers;
      scenario_workload.wait_for_peers = wait;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kConnectPeer) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP connect_peer workload uses node, not nodes");
      }
      ConnectPeerWorkload connect;
      connect.node = JsonOptionalUint32Field(workload, "node", connect.node);
      connect.peer = JsonUint32Field(workload, "peer");
      connect.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kConnectPeer;
      scenario_workload.connect_peer = connect;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kDisconnectPeer) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP disconnect_peer workload uses node, not nodes");
      }
      DisconnectPeerWorkload disconnect;
      disconnect.node =
          JsonOptionalUint32Field(workload, "node", disconnect.node);
      disconnect.peer = JsonUint32Field(workload, "peer");
      disconnect.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kDisconnectPeer;
      scenario_workload.disconnect_peer = disconnect;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kRestartNode) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP restart_node workload uses node, not nodes");
      }
      RestartNodeWorkload restart;
      restart.node = JsonOptionalUint32Field(workload, "node", restart.node);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kRestartNode;
      scenario_workload.restart_node = restart;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kFreezeNode) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP freeze_node workload uses node, not nodes");
      }
      FreezeNodeWorkload freeze;
      freeze.node = JsonOptionalUint32Field(workload, "node", freeze.node);
      freeze.duration_ms = JsonUint32Field(workload, "duration_ms");
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kFreezeNode;
      scenario_workload.freeze_node = freeze;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kUpdateResourceLimits) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP update_resource_limits workload uses node, not "
            "nodes");
      }
      ResourceLimitUpdateWorkload update;
      update.node = JsonOptionalUint32Field(workload, "node", update.node);
      update.patch = ParseResourceLimitPatchObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kUpdateResourceLimits;
      scenario_workload.update_resource_limits = update;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kSetResourceProfile ||
               *kind == WorkloadKind::kSetNetworkProfile) {
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = *kind;
      scenario_workload.profile_switch =
          ParseProfileSwitchWorkload(workload, options, *kind);
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kResourcePressure) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP resource_pressure workload uses node, not nodes");
      }
      ResourcePressureWorkload pressure;
      pressure.node = JsonOptionalUint32Field(workload, "node", pressure.node);
      pressure.patch = ParseResourceLimitPatchObject(workload);
      pressure.duration_ms = JsonUint32Field(workload, "duration_ms");
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kResourcePressure;
      scenario_workload.resource_pressure = pressure;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kSetNetworkCondition) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP set_network_condition workload uses node, not "
            "nodes");
      }
      NetworkConditionWorkload update;
      update.node = JsonOptionalUint32Field(workload, "node", update.node);
      update.condition = ParseNetworkConditionObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kSetNetworkCondition;
      scenario_workload.network_condition = update;
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kBlockNetworkFlow ||
               *kind == WorkloadKind::kUnblockNetworkFlow) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP network flow workload uses node, not nodes");
      }
      NetworkBlockWorkload flow;
      flow.rule = ParseNetworkBlockRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = *kind;
      scenario_workload.network_block = std::move(flow);
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kPartitionNodes) {
      NetworkPartitionWorkload partition;
      partition.partition = ParseNetworkPartitionRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kPartitionNodes;
      scenario_workload.network_partition = partition;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kHealPartition) {
      NetworkPartitionWorkload partition;
      partition.partition = ParseNetworkPartitionRuleObject(workload);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kHealPartition;
      scenario_workload.network_partition = partition;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kSetEdgeCondition ||
               *kind == WorkloadKind::kActivateEdge ||
               *kind == WorkloadKind::kDeactivateEdge ||
               *kind == WorkloadKind::kRestoreEdge) {
      TopologyEdgeWorkload edge;
      edge.from = JsonUint32Field(workload, "from");
      edge.to = JsonUint32Field(workload, "to");
      if (*kind == WorkloadKind::kSetEdgeCondition) {
        if (workload.if_contains("timeout_sec") != nullptr) {
          throw std::runtime_error(
              "scenario set_edge_condition does not accept timeout_sec");
        }
        edge.condition = ParseTopologyEdgeWorkloadCondition(workload);
      } else {
        RejectTopologyEdgeConditionFields(workload, WorkloadKindName(*kind));
        edge.timeout_sec =
            OptionProvided(vm, "sync-timeout-sec")
                ? options.sync_timeout_sec
                : JsonOptionalUint32Field(workload, "timeout_sec",
                                          options.sync_timeout_sec);
      }
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = *kind;
      scenario_workload.topology_edge = edge;
      options.workloads.push_back(std::move(scenario_workload));
    } else if (*kind == WorkloadKind::kSendRawTransaction) {
      if (workload.if_contains("nodes") != nullptr) {
        throw std::runtime_error(
            "current MVP send_raw_transaction workload uses "
            "funding_node and submit_node, not nodes");
      }
      SendRawTransactionWorkload transaction;
      transaction.funding_node = JsonOptionalUint32Field(
          workload, "funding_node", transaction.funding_node);
      transaction.submit_node = JsonOptionalUint32Field(
          workload, "submit_node", transaction.submit_node);
      transaction.source_address = JsonStringField(workload, "source_address");
      transaction.source_private_key =
          JsonStringField(workload, "source_private_key");
      transaction.destination_address =
          JsonStringField(workload, "destination_address");
      transaction.funding_blocks = JsonOptionalUint32Field(
          workload, "funding_blocks", transaction.funding_blocks);
      transaction.amount_satoshis = JsonAmountField(workload, "amount");
      transaction.fee_satoshis = JsonAmountField(workload, "fee");
      transaction.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kSendRawTransaction;
      scenario_workload.send_raw_transaction = transaction;
      options.workloads.push_back(scenario_workload);
    } else if (*kind == WorkloadKind::kWalletTransactions) {
      if (workload.if_contains("wallets") != nullptr ||
          workload.if_contains("private_key") != nullptr ||
          workload.if_contains("source_private_key") != nullptr ||
          workload.if_contains("address") != nullptr ||
          workload.if_contains("source_address") != nullptr ||
          workload.if_contains("destination_address") != nullptr) {
        throw std::runtime_error(
            "scenario wallet_transactions workload must use "
            "topology.wallet_initialization instead of wallet keys or "
            "addresses");
      }
      WalletTransactionsWorkload transactions;
      const std::uint32_t coinbase_confirmations =
          ChainDriverSpecFor(options.chain).coinbase_spendable_confirmations;
      transactions.funding_blocks_per_wallet = coinbase_confirmations;
      transactions.readiness_confirmations = coinbase_confirmations;
      transactions.funding_strategy =
          ParseWalletFundingStrategy(JsonOptionalStringField(
              workload, "funding_strategy",
              WalletFundingStrategyName(transactions.funding_strategy)));
      transactions.strategy =
          ParseWalletTransferStrategy(JsonOptionalStringField(
              workload, "strategy",
              WalletTransferStrategyName(transactions.strategy)));
      transactions.funding_blocks_per_wallet =
          JsonOptionalUint32Field(workload, "funding_blocks_per_wallet",
                                  transactions.funding_blocks_per_wallet);
      transactions.readiness_confirmations =
          JsonOptionalUint64Field(workload, "readiness_confirmations",
                                  transactions.readiness_confirmations);
      transactions.transaction_count = JsonOptionalUint32Field(
          workload, "transaction_count",
          options.topology.configured
              ? static_cast<uint32_t>(options.topology.wallet_nodes.size())
              : 0U);
      transactions.amount = ParseAmountDistribution(workload, "amount");
      transactions.interval = ParseIntervalDistribution(workload, "interval");
      transactions.fee_satoshis = JsonAmountField(workload, "fee");
      const std::uint64_t default_funding_threshold =
          transactions.amount.maximum_satoshis <=
                  std::numeric_limits<std::uint64_t>::max() -
                      transactions.fee_satoshis
              ? transactions.amount.maximum_satoshis + transactions.fee_satoshis
              : 0U;
      transactions.funding_threshold_satoshis = JsonOptionalAmountField(
          workload, "funding_threshold", default_funding_threshold);
      transactions.random_seed =
          JsonOptionalUint64Field(workload, "seed", options.simulation_seed);
      const std::size_t wallet_count = options.topology.wallet_nodes.size();
      transactions.sender_wallets =
          ParseWalletIndexList(workload, "sender_wallets", wallet_count);
      transactions.receiver_wallets =
          ParseWalletIndexList(workload, "receiver_wallets", wallet_count);
      const bool sender_wallets_present =
          workload.if_contains("sender_wallets") != nullptr;
      const bool receiver_wallets_present =
          workload.if_contains("receiver_wallets") != nullptr;
      if ((transactions.strategy == WalletTransferStrategy::kRoundRobin ||
           transactions.strategy == WalletTransferStrategy::kRandom) &&
          (sender_wallets_present || receiver_wallets_present)) {
        throw std::runtime_error(
            "scenario wallet_transactions wallet selectors require fanout "
            "or hotspot strategy");
      }
      if (transactions.strategy == WalletTransferStrategy::kFanout &&
          receiver_wallets_present) {
        throw std::runtime_error(
            "scenario wallet_transactions fanout does not accept "
            "receiver_wallets");
      }
      if (transactions.strategy == WalletTransferStrategy::kHotspot &&
          sender_wallets_present) {
        throw std::runtime_error(
            "scenario wallet_transactions hotspot does not accept "
            "sender_wallets");
      }
      transactions.timeout_sec =
          OptionProvided(vm, "sync-timeout-sec")
              ? options.sync_timeout_sec
              : JsonOptionalUint32Field(workload, "timeout_sec",
                                        options.sync_timeout_sec);
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kWalletTransactions;
      scenario_workload.wallet_transactions = std::move(transactions);
      options.workloads.push_back(std::move(scenario_workload));
      options.wallet_backed_workload_requested = true;
    } else if (*kind == WorkloadKind::kCheckpoint) {
      CheckpointWorkload checkpoint;
      if (workload.if_contains("name") != nullptr) {
        checkpoint.name = JsonStringField(workload, "name");
        RequireSafeScenarioIdentifier(checkpoint.name, "checkpoint name");
      }
      ScenarioWorkload scenario_workload;
      scenario_workload.kind = WorkloadKind::kCheckpoint;
      scenario_workload.checkpoint = std::move(checkpoint);
      options.workloads.push_back(std::move(scenario_workload));
    }
  }
}

void ApplyScheduledScenarioEvents(
    const boost::json::array& events,
    const boost::program_options::variables_map& vm, Options& options) {
  if (events.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("scenario events exceeds uint32 sequence range");
  }
  for (std::size_t index = 0; index < events.size(); ++index) {
    const boost::json::value& value = events[index];
    if (!value.is_object()) {
      throw std::runtime_error("scenario events entries must be JSON objects");
    }
    const boost::json::object& event = value.as_object();
    const std::string at_text = JsonStringField(event, "at");
    const std::string action_name = JsonStringField(event, "action");
    if (!ParseWorkloadKind(action_name)) {
      throw std::runtime_error("unsupported scheduled event action: " +
                               action_name);
    }
    if (event.if_contains("type") != nullptr) {
      throw std::runtime_error("scenario events entries use action, not type");
    }

    boost::json::object action_object = event;
    action_object.erase("at");
    action_object.erase("action");
    action_object["type"] = action_name;
    boost::json::array action_array;
    action_array.push_back(std::move(action_object));
    const std::size_t workload_count = options.workloads.size();
    ApplyScenarioWorkloads(action_array, vm, options);
    if (options.workloads.size() != workload_count + 1U) {
      throw std::runtime_error("scheduled event did not produce one action");
    }

    ScheduledScenarioEvent scheduled;
    scheduled.at = PositiveDuration::Parse(at_text).value();
    const std::chrono::milliseconds scheduled_wall_at =
        options.time_scale.WallDuration(scheduled.at);
    const auto maximum_monotonic_delay =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::duration::max());
    if (scheduled_wall_at > maximum_monotonic_delay) {
      throw std::runtime_error(
          "scheduled event time exceeds monotonic clock range");
    }
    if (options.simulation_duration &&
        scheduled.at >= *options.simulation_duration) {
      throw std::runtime_error(
          "scheduled event time must be less than simulation duration");
    }
    scheduled.sequence = static_cast<std::uint32_t>(index + 1U);
    scheduled.action = std::move(options.workloads.back());
    options.workloads.pop_back();
    options.scheduled_events.push_back(std::move(scheduled));
  }
}

void ApplyScenarioJson(const boost::json::object& scenario,
                       const boost::program_options::variables_map& vm,
                       Options& options) {
  if (scenario.if_contains("generate_blocks") != nullptr) {
    throw std::runtime_error(
        "scenario generate_blocks was removed; use block_production.enabled "
        "or an explicit block_generation workload");
  }
  const boost::json::value* simulation_value =
      scenario.if_contains("simulation");
  const boost::json::object* simulation = nullptr;
  if (simulation_value != nullptr) {
    if (!simulation_value->is_object()) {
      throw std::runtime_error("scenario simulation must be a JSON object");
    }
    simulation = &simulation_value->as_object();
    if (simulation->if_contains("name") != nullptr) {
      options.simulation_name = JsonStringField(*simulation, "name");
      RequireSafeScenarioIdentifier(options.simulation_name, "simulation name");
    }
    options.simulation_seed =
        JsonOptionalUint64Field(*simulation, "seed", options.simulation_seed);
    const boost::json::value* duration = simulation->if_contains("duration");
    if (duration != nullptr) {
      if (!duration->is_string()) {
        throw std::runtime_error(
            "scenario simulation.duration must be a duration string");
      }
      options.simulation_duration =
          PositiveDuration::Parse(
              std::string_view(duration->as_string().data(),
                               duration->as_string().size()))
              .value();
    }
    options.time_scale =
        SimulationTimeScale::FromDouble(JsonOptionalDoubleField(
            *simulation, "time_scale", options.time_scale.value()));

    if (!OptionProvided(vm, "keep-cgroups")) {
      const std::string cleanup_policy = JsonOptionalStringField(
          *simulation, "cleanup_policy",
          std::string(CleanupPolicyName(options.cleanup_policy)));
      const std::optional<CleanupPolicy> parsed_cleanup_policy =
          CleanupPolicyFromName(cleanup_policy);
      if (!parsed_cleanup_policy) {
        throw std::runtime_error(
            "scenario simulation.cleanup_policy must be automatic or "
            "retain_cgroups");
      }
      options.cleanup_policy = *parsed_cleanup_policy;
    }

    const std::string privilege_mode = JsonOptionalStringField(
        *simulation, "privilege_mode",
        std::string(PrivilegeModeName(options.privilege_mode)));
    const std::optional<PrivilegeMode> parsed_privilege_mode =
        PrivilegeModeFromName(privilege_mode);
    if (!parsed_privilege_mode) {
      throw std::runtime_error(
          "scenario simulation.privilege_mode currently supports only "
          "direct");
    }
    options.privilege_mode = *parsed_privilege_mode;

    const std::string log_retention_policy = JsonOptionalStringField(
        *simulation, "log_retention_policy",
        std::string(LogRetentionPolicyName(options.log_retention_policy)));
    const std::optional<LogRetentionPolicy> parsed_log_retention_policy =
        LogRetentionPolicyFromName(log_retention_policy);
    if (!parsed_log_retention_policy) {
      throw std::runtime_error(
          "scenario simulation.log_retention_policy currently supports only "
          "preserve");
    }
    options.log_retention_policy = *parsed_log_retention_policy;

    const bool has_metrics_interval =
        simulation->if_contains("metrics_interval") != nullptr;
    const bool has_tick_interval =
        simulation->if_contains("tick_interval") != nullptr;
    if (has_metrics_interval && has_tick_interval) {
      throw std::runtime_error(
          "scenario simulation.metrics_interval and tick_interval are "
          "aliases and must not both be provided");
    }
    if ((has_metrics_interval || has_tick_interval) &&
        scenario.if_contains("metrics_interval_ms") != nullptr) {
      throw std::runtime_error(
          "scenario simulation metrics interval and top-level "
          "metrics_interval_ms must not be combined");
    }
    if (!OptionProvided(vm, "metrics-interval") &&
        !OptionProvided(vm, "metrics-interval-ms") &&
        (has_metrics_interval || has_tick_interval)) {
      const char* field =
          has_metrics_interval ? "metrics_interval" : "tick_interval";
      options.metrics_interval =
          PositiveDuration::Parse(JsonStringField(*simulation, field)).value();
    }

    if (simulation->if_contains("output_dir") != nullptr &&
        scenario.if_contains("output_dir") != nullptr) {
      throw std::runtime_error(
          "scenario simulation.output_dir and top-level output_dir must not "
          "be combined");
    }
    if (!OptionProvided(vm, "benchmark-root") &&
        !OptionProvided(vm, "output-dir")) {
      options.output_dir =
          JsonOptionalPathField(*simulation, "output_dir", options.output_dir);
    }

    if (simulation->if_contains("tui_refresh_interval") != nullptr &&
        !OptionProvided(vm, "refresh-ms")) {
      const auto refresh = PositiveDuration::Parse(
          JsonStringField(*simulation, "tui_refresh_interval"));
      if (refresh.value().count() >
          static_cast<std::chrono::milliseconds::rep>(
              std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error(
            "scenario simulation.tui_refresh_interval exceeds uint32 "
            "milliseconds");
      }
      options.tui_refresh_ms =
          static_cast<std::uint32_t>(refresh.value().count());
    }
  }
  if (!OptionProvided(vm, "block-production-seed")) {
    options.block_production.policy = BlockProductionPolicy(
        options.block_production.policy.period(),
        options.block_production.policy.probability(), options.simulation_seed);
  }
  ParseScenarioChains(scenario, &options);
  if (!OptionProvided(vm, "chain")) {
    if (scenario.if_contains("chain") != nullptr) {
      options.chain = ParseChainKind(JsonStringField(scenario, "chain"));
    } else if (options.chains.size() == 1U) {
      options.chain = options.chains.begin()->second.driver;
    } else if (!options.chains.empty()) {
      throw std::runtime_error(
          "scenario with multiple chains requires an active chain");
    }
  }
  const std::string active_chain_name(ChainKindName(options.chain));
  const auto active_chain = options.chains.find(active_chain_name);
  if (!options.chains.empty() && active_chain == options.chains.end()) {
    throw std::runtime_error("scenario chains does not define active chain: " +
                             active_chain_name);
  }
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  const bool chain_daemon_provided =
      OptionProvided(vm, "node-binary") || OptionProvided(vm, "chain-daemon") ||
      OptionProvided(vm, chain_spec.daemon_option_name.c_str());
  const bool legacy_chain_daemon =
      scenario.if_contains("chain_daemon") != nullptr;
  const bool legacy_driver_daemon =
      scenario.if_contains(chain_spec.daemon_scenario_field) != nullptr;
  if (!options.chains.empty() &&
      (legacy_chain_daemon || legacy_driver_daemon)) {
    throw std::runtime_error(
        "scenario chains default_binary must not be combined with legacy "
        "daemon fields");
  }
  if (!chain_daemon_provided) {
    if (active_chain != options.chains.end()) {
      options.chain_daemon = active_chain->second.default_binary;
    } else {
      if (legacy_chain_daemon && legacy_driver_daemon) {
        throw std::runtime_error(
            "scenario chain_daemon and its driver-specific alias must not "
            "both be provided");
      }
      options.chain_daemon =
          JsonOptionalPathField(scenario, "chain_daemon", options.chain_daemon);
      options.chain_daemon = JsonOptionalPathField(
          scenario, chain_spec.daemon_scenario_field.c_str(),
          options.chain_daemon);
    }
  }
  if (!OptionProvided(vm, "benchmark-root") &&
      !OptionProvided(vm, "output-dir") &&
      (simulation == nullptr ||
       simulation->if_contains("output_dir") == nullptr)) {
    options.output_dir =
        JsonOptionalPathField(scenario, "output_dir", options.output_dir);
  }
  if (!OptionProvided(vm, "run-id")) {
    const boost::json::value* run_id = scenario.if_contains("run_id");
    if (run_id != nullptr) {
      if (!run_id->is_string()) {
        throw std::runtime_error("scenario run_id must be a string");
      }
      options.run_id = std::string(run_id->as_string());
    }
  }
  if (options.simulation_name.empty()) {
    options.simulation_name = options.run_id;
  }
  const ScenarioNodeRoles node_roles =
      ParseScenarioNodes(scenario, vm, &options);
  const boost::json::value* topology = scenario.if_contains("topology");
  if (topology != nullptr) {
    if (!topology->is_object()) {
      throw std::runtime_error("scenario topology must be a JSON object");
    }
    options.topology = ParseNodeRoleTopologyObject(
        topology->as_object(), options.nodes, options.simulation_seed);
    options.wallet_initialization =
        ParseWalletInitializationObject(topology->as_object());
    if (node_roles.configured &&
        (!SameNodeSet(options.topology.wallet_nodes, node_roles.wallet_nodes) ||
         !SameNodeSet(options.topology.miner_nodes, node_roles.miner_nodes))) {
      throw std::runtime_error(
          "scenario nodes roles must match topology wallet_nodes and "
          "miner_nodes");
    }
    if (!OptionProvided(vm, "generate-node") &&
        !options.topology.miner_nodes.empty()) {
      options.generate_node = options.topology.miner_nodes.front() + 1U;
    }
  } else if (node_roles.configured) {
    boost::json::object derived_topology;
    derived_topology["node_count"] = options.nodes;
    boost::json::array wallet_nodes;
    for (const uint32_t node_index : node_roles.wallet_nodes) {
      wallet_nodes.push_back(node_index + 1U);
    }
    derived_topology["wallet_nodes"] = std::move(wallet_nodes);
    boost::json::array miner_nodes;
    for (const uint32_t node_index : node_roles.miner_nodes) {
      miner_nodes.push_back(node_index + 1U);
    }
    derived_topology["miner_nodes"] = std::move(miner_nodes);
    derived_topology["type"] = "full_mesh";
    options.topology = ParseNodeRoleTopologyObject(
        derived_topology, options.nodes, options.simulation_seed);
    if (!OptionProvided(vm, "generate-node") &&
        !options.topology.miner_nodes.empty()) {
      options.generate_node = options.topology.miner_nodes.front() + 1U;
    }
  }
  if (!OptionProvided(vm, "generate-node")) {
    options.generate_node = JsonOptionalNullableUint32Field(
        scenario, "generate_node", options.generate_node);
  }
  const boost::json::value* block_production =
      scenario.if_contains("block_production");
  if (block_production != nullptr) {
    if (!block_production->is_object()) {
      throw std::runtime_error(
          "scenario block_production must be a JSON object");
    }
    const boost::json::object& object = block_production->as_object();
    if (!OptionProvided(vm, "no-mining")) {
      options.block_production.enabled = JsonOptionalBoolField(
          object, "enabled", options.block_production.enabled);
    }
    if (!OptionProvided(vm, "native-mining")) {
      options.block_production.mode =
          JsonOptionalBoolField(object, "native_mining", false)
              ? MiningMode::kNativeMining
              : MiningMode::kScheduledBlockProduction;
    }
    const std::uint32_t period_ms =
        OptionProvided(vm, "block-production-period-ms")
            ? static_cast<std::uint32_t>(
                  options.block_production.policy.period().count())
            : JsonOptionalUint32Field(
                  object, "period_ms",
                  static_cast<std::uint32_t>(
                      options.block_production.policy.period().count()));
    const double probability =
        OptionProvided(vm, "block-production-probability")
            ? options.block_production.policy.probability()
            : JsonOptionalDoubleField(
                  object, "probability",
                  options.block_production.policy.probability());
    const std::uint64_t seed =
        OptionProvided(vm, "block-production-seed")
            ? options.block_production.policy.seed()
            : JsonOptionalUint64Field(object, "seed",
                                      options.block_production.policy.seed());
    options.block_production.policy = BlockProductionPolicy(
        std::chrono::milliseconds(period_ms), probability, seed);
    if (!OptionProvided(vm, "mining-difficulty") &&
        object.if_contains("difficulty") != nullptr) {
      const std::optional<double> difficulty =
          JsonOptionalNullableDoubleField(object, "difficulty");
      options.block_production.difficulty =
          difficulty
              ? std::optional<MiningDifficulty>(MiningDifficulty(*difficulty))
              : std::nullopt;
    }
  }
  if (!OptionProvided(vm, "ready-timeout-sec")) {
    options.ready_timeout_sec = JsonOptionalUint32Field(
        scenario, "ready_timeout_sec", options.ready_timeout_sec);
  }
  if (!OptionProvided(vm, "sync-timeout-sec")) {
    options.sync_timeout_sec = JsonOptionalNullableUint32Field(
        scenario, "sync_timeout_sec", options.sync_timeout_sec);
  }
  if (!OptionProvided(vm, "metrics-sample-count")) {
    options.metrics_sample_count = JsonOptionalUint32Field(
        scenario, "metrics_sample_count", options.metrics_sample_count);
  }
  if (!OptionProvided(vm, "metrics-interval") &&
      !OptionProvided(vm, "metrics-interval-ms") &&
      (simulation == nullptr ||
       (simulation->if_contains("metrics_interval") == nullptr &&
        simulation->if_contains("tick_interval") == nullptr))) {
    options.metrics_interval =
        PositiveDuration::FromMilliseconds(
            JsonOptionalUint32Field(
                scenario, "metrics_interval_ms",
                static_cast<std::uint32_t>(options.metrics_interval.count())))
            .value();
  }
  if (!OptionProvided(vm, "isolate-network")) {
    options.isolate_network = JsonOptionalBoolField(
        scenario, "isolated_network", options.isolate_network);
  }
  const boost::json::value* workloads = scenario.if_contains("workloads");
  if (workloads != nullptr) {
    if (!workloads->is_array()) {
      throw std::runtime_error("scenario workloads must be a JSON array");
    }
    options.workloads_configured = true;
    ApplyScenarioWorkloads(workloads->as_array(), vm, options);
  }
  const boost::json::value* events = scenario.if_contains("events");
  if (events != nullptr) {
    if (!events->is_array()) {
      throw std::runtime_error("scenario events must be a JSON array");
    }
    ApplyScheduledScenarioEvents(events->as_array(), vm, options);
  }

  const boost::json::value* resources = scenario.if_contains("resources");
  if (resources != nullptr) {
    if (!resources->is_object()) {
      throw std::runtime_error("scenario resources must be a JSON object");
    }
    const boost::json::object& object = resources->as_object();
    if (!OptionProvided(vm, "memory-high-bytes")) {
      options.memory_high_bytes = JsonOptionalUint64Field(
          object, "memory_high_bytes", options.memory_high_bytes);
    }
    if (!OptionProvided(vm, "memory-max-bytes")) {
      options.memory_max_bytes = JsonOptionalUint64Field(
          object, "memory_max_bytes", options.memory_max_bytes);
    }
    if (!OptionProvided(vm, "cpu-period-us")) {
      options.cpu_period_us = JsonOptionalUint64Field(object, "cpu_period_us",
                                                      options.cpu_period_us);
    }
    if (!OptionProvided(vm, "cpu-weight")) {
      options.cpu_weight =
          JsonOptionalUint64Field(object, "cpu_weight", options.cpu_weight);
    }
    if (!OptionProvided(vm, "io-weight")) {
      options.io_weight =
          JsonOptionalUint64Field(object, "io_weight", options.io_weight);
    }
    const boost::json::value* initial_io_limits = object.if_contains("io_max");
    if (initial_io_limits != nullptr) {
      options.io_limits =
          ParseIoLimits(*initial_io_limits, "scenario resources.io_max");
    }
    if (!OptionProvided(vm, "pids-max")) {
      options.pids_max =
          JsonOptionalUint64Field(object, "pids_max", options.pids_max);
    }
    if (!OptionProvided(vm, "cpu-quota-us")) {
      const boost::json::value* quota = object.if_contains("cpu_quota_us");
      if (quota != nullptr && !quota->is_null()) {
        if (!quota->is_uint64() &&
            !(quota->is_int64() && quota->as_int64() >= 0)) {
          throw std::runtime_error(
              "scenario resources.cpu_quota_us must be uint or null");
        }
        options.cpu_quota_us = quota->is_uint64()
                                   ? quota->as_uint64()
                                   : static_cast<uint64_t>(quota->as_int64());
        options.cpu_quota_requested = true;
      }
    }
    const boost::json::value* runtime_node_limits =
        object.if_contains("runtime_node_limits");
    if (runtime_node_limits != nullptr) {
      if (!runtime_node_limits->is_array()) {
        throw std::runtime_error(
            "scenario resources.runtime_node_limits must be a JSON array");
      }
      ApplyResourceLimitPatches(runtime_node_limits->as_array(), options.nodes,
                                "scenario resources.runtime_node_limits",
                                options.runtime_node_resource_updates);
    }
  }
  ParseResourceProfiles(scenario, &options);

  const boost::json::value* process = scenario.if_contains("process");
  if (process != nullptr) {
    if (!process->is_object()) {
      throw std::runtime_error("scenario process must be a JSON object");
    }
    ProcessControlConfig config =
        ParseProcessControlConfig(process->as_object(), options.nodes);
    options.runtime_node_restarts.insert(options.runtime_node_restarts.end(),
                                         config.restart_node_indexes.begin(),
                                         config.restart_node_indexes.end());
    options.runtime_node_freezes.insert(options.runtime_node_freezes.end(),
                                        config.freezes.begin(),
                                        config.freezes.end());
  }

  ParseNetworkProfiles(scenario, &options);
  const boost::json::value* network = scenario.if_contains("network");
  if (network != nullptr) {
    if (!network->is_object()) {
      throw std::runtime_error("scenario network must be a JSON object");
    }
    const boost::json::object& object = network->as_object();
    if (!OptionProvided(vm, "isolate-network")) {
      options.isolate_network =
          JsonOptionalBoolField(object, "isolated", options.isolate_network);
    }
    const boost::json::value* default_condition =
        object.if_contains("default_condition");
    if (default_condition != nullptr) {
      if (!default_condition->is_object()) {
        throw std::runtime_error(
            "scenario network.default_condition must be a JSON object");
      }
      const NetworkCondition scenario_condition =
          ParseNetworkConditionObject(default_condition->as_object());
      if (!OptionProvided(vm, "network-bandwidth-mbps")) {
        options.network_condition.bandwidth_mbps =
            scenario_condition.bandwidth_mbps;
      }
      if (!OptionProvided(vm, "network-delay-ms")) {
        options.network_condition.delay_ms = scenario_condition.delay_ms;
      }
      if (!OptionProvided(vm, "network-jitter-ms")) {
        options.network_condition.jitter_ms = scenario_condition.jitter_ms;
      }
      if (!OptionProvided(vm, "network-loss-bps")) {
        options.network_condition.loss_basis_points =
            scenario_condition.loss_basis_points;
      }
      if (!OptionProvided(vm, "network-duplicate-bps")) {
        options.network_condition.duplicate_basis_points =
            scenario_condition.duplicate_basis_points;
      }
      if (!OptionProvided(vm, "network-corrupt-bps")) {
        options.network_condition.corrupt_basis_points =
            scenario_condition.corrupt_basis_points;
      }
      if (!OptionProvided(vm, "network-reorder-bps")) {
        options.network_condition.reorder_basis_points =
            scenario_condition.reorder_basis_points;
      }
      if (!OptionProvided(vm, "network-limit-packets")) {
        options.network_condition.limit_packets =
            scenario_condition.limit_packets;
      }
      options.network_condition_requested = true;
    }
    const boost::json::value* node_conditions =
        object.if_contains("node_conditions");
    if (node_conditions != nullptr) {
      if (!node_conditions->is_array()) {
        throw std::runtime_error(
            "scenario network.node_conditions must be a JSON array");
      }
      ApplyNodeConditions(node_conditions->as_array(), options.nodes,
                          "scenario network.node_conditions",
                          options.node_network_conditions);
    }
    const boost::json::value* runtime_node_conditions =
        object.if_contains("runtime_node_conditions");
    if (runtime_node_conditions != nullptr) {
      if (!runtime_node_conditions->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_conditions must be a JSON array");
      }
      ApplyNodeConditions(runtime_node_conditions->as_array(), options.nodes,
                          "scenario network.runtime_node_conditions",
                          options.runtime_node_network_conditions);
    }
    const boost::json::value* runtime_node_blocks =
        object.if_contains("runtime_node_blocks");
    if (runtime_node_blocks != nullptr) {
      if (!runtime_node_blocks->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_blocks must be a JSON array");
      }
      ApplyNetworkBlockRules(runtime_node_blocks->as_array(), options.nodes,
                             "scenario network.runtime_node_blocks",
                             options.runtime_node_blocks);
    }
    const boost::json::value* runtime_node_unblocks =
        object.if_contains("runtime_node_unblocks");
    if (runtime_node_unblocks != nullptr) {
      if (!runtime_node_unblocks->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_unblocks must be a JSON array");
      }
      ApplyNetworkBlockRules(runtime_node_unblocks->as_array(), options.nodes,
                             "scenario network.runtime_node_unblocks",
                             options.runtime_node_unblocks);
    }
    const boost::json::value* runtime_partitions =
        object.if_contains("runtime_partitions");
    if (runtime_partitions != nullptr) {
      if (!runtime_partitions->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_partitions must be a JSON array");
      }
      ApplyNetworkPartitionRules(runtime_partitions->as_array(), options.nodes,
                                 "scenario network.runtime_partitions",
                                 options.runtime_partitions);
    }
    const boost::json::value* runtime_partition_heals =
        object.if_contains("runtime_partition_heals");
    if (runtime_partition_heals != nullptr) {
      if (!runtime_partition_heals->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_partition_heals must be a JSON array");
      }
      ApplyNetworkPartitionRules(runtime_partition_heals->as_array(),
                                 options.nodes,
                                 "scenario network.runtime_partition_heals",
                                 options.runtime_partition_heals);
    }
  }
  ResolveNodeProfileAssignments(&options);
  ValidateProfileSwitchReferences(&options);
}

bool WorkloadsRequireIsolatedNetwork(const Options& options) {
  for (const ScenarioWorkload& workload : options.workloads) {
    if (workload.kind == WorkloadKind::kPartitionNodes ||
        workload.kind == WorkloadKind::kHealPartition ||
        workload.kind == WorkloadKind::kSetNetworkCondition ||
        workload.kind == WorkloadKind::kBlockNetworkFlow ||
        workload.kind == WorkloadKind::kUnblockNetworkFlow ||
        workload.kind == WorkloadKind::kSetNetworkProfile) {
      return true;
    }
  }
  for (const ScheduledScenarioEvent& event : options.scheduled_events) {
    if (event.action.kind == WorkloadKind::kPartitionNodes ||
        event.action.kind == WorkloadKind::kHealPartition ||
        event.action.kind == WorkloadKind::kSetNetworkCondition ||
        event.action.kind == WorkloadKind::kBlockNetworkFlow ||
        event.action.kind == WorkloadKind::kUnblockNetworkFlow ||
        event.action.kind == WorkloadKind::kSetNetworkProfile) {
      return true;
    }
  }
  return false;
}

std::vector<const ScenarioWorkload*> ConfiguredScenarioActions(
    const Options& options) {
  std::vector<const ScenarioWorkload*> actions;
  actions.reserve(options.workloads.size() + options.scheduled_events.size());
  for (const ScenarioWorkload& workload : options.workloads) {
    actions.push_back(&workload);
  }
  for (const ScheduledScenarioEvent& event : options.scheduled_events) {
    actions.push_back(&event.action);
  }
  return actions;
}

bool IsTopologyEdgeAction(WorkloadKind kind) {
  return kind == WorkloadKind::kSetEdgeCondition ||
         kind == WorkloadKind::kActivateEdge ||
         kind == WorkloadKind::kDeactivateEdge ||
         kind == WorkloadKind::kRestoreEdge;
}

std::vector<ScenarioWorkload> OrderedConfiguredScenarioActions(
    const Options& options) {
  std::vector<ScenarioWorkload> actions = options.workloads;
  for (const ScheduledScenarioEvent& event :
       OrderScheduledScenarioEvents(options.scheduled_events)) {
    actions.push_back(event.action);
  }
  return actions;
}

void ValidateRuntimeTopologyPeerPolicy(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    std::uint32_t node_index) {
  const PeerConnectivityPolicy* policy =
      FindPeerConnectivityPolicy(options.topology, node_index);
  if (policy != nullptr &&
      policy->peer_count.minimum() >
          runtime_topology.ActivePeerIndexes(node_index).size()) {
    throw std::runtime_error(
        "peer policy minimum exceeds allowed logical peers after topology "
        "edge action");
  }
}

bool ValidateRuntimeTopologyActionSequence(
    const Options& options, RuntimePeerTopology* runtime_topology) {
  bool requires_isolated_network = false;
  for (const ScenarioWorkload& action :
       OrderedConfiguredScenarioActions(options)) {
    if (IsTopologyEdgeAction(action.kind)) {
      const TopologyEdgeWorkload& edge = action.topology_edge;
      const std::uint32_t from = edge.from - 1U;
      const std::uint32_t to = edge.to - 1U;
      if (action.kind == WorkloadKind::kSetEdgeCondition) {
        if (!edge.condition) {
          throw std::runtime_error(
              "set_edge_condition action is missing its typed condition");
        }
        static_cast<void>(
            runtime_topology->SetCondition(from, to, *edge.condition));
      } else if (action.kind == WorkloadKind::kActivateEdge) {
        static_cast<void>(runtime_topology->SetActive(from, to, true));
      } else if (action.kind == WorkloadKind::kDeactivateEdge) {
        static_cast<void>(runtime_topology->SetActive(from, to, false));
      } else {
        static_cast<void>(runtime_topology->RestoreBaseline(from, to));
      }
      ValidateRuntimeTopologyPeerPolicy(options, *runtime_topology, from);
      const RuntimePeerTopologyEdge& current = runtime_topology->Edge(from, to);
      requires_isolated_network =
          requires_isolated_network || (current.active && current.condition);
    } else if (action.kind == WorkloadKind::kConnectPeer) {
      const std::uint32_t from = action.connect_peer.node - 1U;
      const std::uint32_t to = action.connect_peer.peer - 1U;
      if (!runtime_topology->Edge(from, to).active) {
        throw std::runtime_error(
            "scenario connect_peer target is not active at this point in "
            "the topology action sequence");
      }
    }
  }
  return requires_isolated_network;
}

void ParseNodeNetworkConditionTexts(
    const std::vector<std::string>& texts, uint32_t nodes,
    std::string_view option_name,
    std::map<uint32_t, NetworkCondition>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(std::string(option_name) +
                               " node must be in 1..--nodes");
    }
    output[node - 1U] = ParseNetworkConditionObject(object);
  }
}

void ParseRuntimeNodeResourceTexts(
    const std::vector<std::string>& texts, uint32_t nodes,
    std::map<uint32_t, ResourceLimitPatch>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-resource-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-resource-json node must be in 1..--nodes");
    }
    output[node - 1U] = ParseResourceLimitPatchObject(object);
  }
}

void ParseRuntimeNodeBlockTexts(const std::vector<std::string>& texts,
                                uint32_t nodes, std::string_view option_name,
                                std::vector<NetworkBlockRule>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    NetworkBlockRule rule = ParseNetworkBlockRuleObject(value.as_object());
    if (rule.node_index >= nodes) {
      throw std::runtime_error(std::string(option_name) +
                               " node must be in 1..--nodes");
    }
    output.push_back(std::move(rule));
  }
}

void ParseRuntimePartitionTexts(const std::vector<std::string>& texts,
                                uint32_t nodes, std::string_view option_name,
                                std::vector<NetworkPartitionRule>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(std::string(option_name) +
                               " must be a JSON object");
    }
    NetworkPartitionRule rule =
        ParseNetworkPartitionRuleObject(value.as_object());
    for (uint32_t node_index : rule.group_a) {
      if (node_index >= nodes) {
        throw std::runtime_error(std::string(option_name) +
                                 " group_a node must be in 1..--nodes");
      }
    }
    for (uint32_t node_index : rule.group_b) {
      if (node_index >= nodes) {
        throw std::runtime_error(std::string(option_name) +
                                 " group_b node must be in 1..--nodes");
      }
    }
    output.push_back(std::move(rule));
  }
}

void ParseRuntimeNodeRestartTexts(const std::vector<std::string>& texts,
                                  uint32_t nodes,
                                  std::vector<uint32_t>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-restart-json must be a JSON object");
    }
    const uint32_t node = JsonUint32Field(value.as_object(), "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-restart-json node must be in 1..--nodes");
    }
    output.push_back(node - 1U);
  }
}

void ParseRuntimeNodeFreezeTexts(const std::vector<std::string>& texts,
                                 uint32_t nodes,
                                 std::vector<FreezeRequest>& output) {
  for (const std::string& text : texts) {
    boost::json::value value = boost::json::parse(text);
    if (!value.is_object()) {
      throw std::runtime_error(
          "--runtime-node-freeze-json must be a JSON object");
    }
    const boost::json::object& object = value.as_object();
    const uint32_t node = JsonUint32Field(object, "node");
    if (node == 0 || node > nodes) {
      throw std::runtime_error(
          "--runtime-node-freeze-json node must be in 1..--nodes");
    }
    const uint32_t duration_ms = JsonUint32Field(object, "duration_ms");
    if (duration_ms == 0U) {
      throw std::runtime_error(
          "--runtime-node-freeze-json duration_ms must be greater than zero");
    }
    output.push_back(
        FreezeRequest{.node_index = node - 1U, .duration_ms = duration_ms});
  }
}

void ParseLegacyCliInputs(const LegacyCliInputs& inputs, Options& options) {
  ParseNodeNetworkConditionTexts(inputs.node_network_conditions, options.nodes,
                                 "--node-network-condition-json",
                                 options.node_network_conditions);
  ParseNodeNetworkConditionTexts(inputs.runtime_node_network_conditions,
                                 options.nodes,
                                 "--runtime-node-network-condition-json",
                                 options.runtime_node_network_conditions);
  ParseRuntimeNodeBlockTexts(inputs.runtime_node_blocks, options.nodes,
                             "--runtime-node-block-json",
                             options.runtime_node_blocks);
  ParseRuntimeNodeBlockTexts(inputs.runtime_node_unblocks, options.nodes,
                             "--runtime-node-unblock-json",
                             options.runtime_node_unblocks);
  ParseRuntimePartitionTexts(inputs.runtime_partitions, options.nodes,
                             "--runtime-partition-json",
                             options.runtime_partitions);
  ParseRuntimePartitionTexts(inputs.runtime_partition_heals, options.nodes,
                             "--runtime-heal-partition-json",
                             options.runtime_partition_heals);
  ParseRuntimeNodeResourceTexts(inputs.runtime_node_resources, options.nodes,
                                options.runtime_node_resource_updates);
  ParseRuntimeNodeRestartTexts(inputs.runtime_node_restarts, options.nodes,
                               options.runtime_node_restarts);
  ParseRuntimeNodeFreezeTexts(inputs.runtime_node_freezes, options.nodes,
                              options.runtime_node_freezes);
}

bool TopologyHasDirectionalNetworkConditions(const Options& options);

Options ParseOptions(int argc, char** argv) {
  namespace po = boost::program_options;
  Options options;
  LegacyCliInputs legacy_inputs;
  const ChainDriverSpec& default_chain_spec = DefaultChainDriverSpec();
  std::string chain_name = std::string(ChainKindName(options.chain));
  std::string log_level_name = std::string(LogLevelName(options.log_level));
  std::string metrics_interval_text = "1s";
  std::uint32_t legacy_metrics_interval_ms = 1000U;
  std::uint32_t block_production_period_ms = 1000U;
  double block_production_probability = 0.5;
  std::uint64_t block_production_seed = 0U;
  double mining_difficulty = 1.0;
  bool no_mining = false;
  bool native_mining = false;
  bool keep_cgroups = false;
  std::string cleanup_run_id;

  const std::string nodes_help =
      "chain regtest nodes, 1.." + std::to_string(default_chain_spec.max_nodes);
  po::options_description canonical_options(
      "Blockchain Benchmark Project options");
  canonical_options.add_options()("help", "show this help")(
      "scenario", po::value<std::filesystem::path>(),
      "JSON or YAML scenario file")("chain", po::value<std::string>(),
                                    "firo, bitcoin, or monero")(
      "node-binary", po::value<std::filesystem::path>(),
      "daemon binary for the selected chain")(
      "benchmark-root", po::value<std::filesystem::path>(),
      "root directory for all benchmark artifacts")(
      "run-id", po::value<std::string>(), "optional stable run identifier")(
      "replace-run", "replace a validated simulator-owned run directory")(
      "cleanup-run", po::value<std::string>()->implicit_value(""),
      "clean a previous run, optionally by run id")(
      "report-run", po::value<std::filesystem::path>(),
      "report a previous run id or directory")(
      "run", po::value<std::filesystem::path>(),
      "view a previous run id or directory in the TUI")(
      "no-tui", "run a new simulation headlessly")(
      "log-level", po::value<std::string>(),
      "trace, debug, info, warning, error, or fatal")(
      "metrics-interval", po::value<std::string>(),
      "metrics interval with an explicit unit, such as 250ms or 1s")(
      "keep-artifacts", "preserve run artifacts")(
      "once", "render one frame when viewing a previous run");
  po::options_description desc("Allowed options");
  desc.add_options()("help", "show this help")(
      "scenario", po::value<std::filesystem::path>(&options.scenario),
      "JSON or YAML scenario file selected by extension")(
      "scenario-json", po::value<std::filesystem::path>(&options.scenario_json),
      "legacy Boost.JSON scenario file option")(
      "scenario-yaml", po::value<std::filesystem::path>(&options.scenario_yaml),
      "legacy libyaml scenario file option")(
      "chain", po::value<std::string>(&chain_name),
      "chain driver: firo, bitcoin, or monero")(
      "log-level", po::value<std::string>(&log_level_name),
      "minimum Boost.Log severity: trace, debug, info, warning, error, or "
      "fatal")("node-binary",
               po::value<std::filesystem::path>(&options.chain_daemon),
               "daemon binary for the selected chain")(
      "chain-daemon", po::value<std::filesystem::path>(&options.chain_daemon),
      "legacy alias for --node-binary")(
      default_chain_spec.daemon_option_name.c_str(),
      po::value<std::filesystem::path>(&options.chain_daemon),
      "legacy Firo daemon binary alias")(
      "benchmark-root", po::value<std::filesystem::path>(&options.output_dir),
      "root directory for run data, node directories, metrics, events, and "
      "logs")("output-dir",
              po::value<std::filesystem::path>(&options.output_dir),
              "legacy alias for --benchmark-root")(
      "run-id", po::value<std::string>(&options.run_id), "safe run id")(
      "report-run", po::value<std::filesystem::path>(&options.report_run),
      "summarize an existing run directory as JSON and exit")(
      "run", po::value<std::filesystem::path>(&options.tui_run),
      "view an existing run directory in the integrated ncurses TUI")(
      "no-tui", po::bool_switch(&options.no_tui),
      "run a new benchmark headlessly with Boost.Log output instead of the "
      "integrated ncurses TUI")(
      "once", po::bool_switch(&options.tui_once),
      "render one integrated TUI frame and exit; requires --run")(
      "refresh-ms", po::value<std::uint32_t>(&options.tui_refresh_ms),
      "milliseconds between integrated TUI report refreshes")(
      "nodes", po::value<uint32_t>(&options.nodes), nodes_help.c_str())(
      "generate-node", po::value<uint32_t>(&options.generate_node),
      "default 1-based miner node when no topology is configured")(
      "no-mining", po::bool_switch(&no_mining),
      "disable scheduled block production")(
      "native-mining", po::bool_switch(&native_mining),
      "use the chain's native continuous miner instead of scheduled block "
      "production")(
      "block-production-period-ms",
      po::value<std::uint32_t>(&block_production_period_ms),
      "milliseconds between global Bernoulli block-production draws")(
      "block-production-probability",
      po::value<double>(&block_production_probability),
      "probability in [0,1] of producing one block at each draw")(
      "block-production-seed", po::value<std::uint64_t>(&block_production_seed),
      "reproducible global Bernoulli scheduler seed")(
      "mining-difficulty", po::value<double>(&mining_difficulty),
      "chain-specific mining difficulty requested for configured miners")(
      "ready-timeout-sec", po::value<uint32_t>(&options.ready_timeout_sec),
      "RPC startup timeout")("sync-timeout-sec",
                             po::value<uint32_t>(&options.sync_timeout_sec),
                             "block propagation timeout")(
      "metrics-sample-count",
      po::value<uint32_t>(&options.metrics_sample_count),
      "periodic metric samples collected concurrently with workloads and "
      "block production")(
      "metrics-interval", po::value<std::string>(&metrics_interval_text),
      "typed interval between metric and node-log samples, such as 250ms or "
      "1s")("metrics-interval-ms",
            po::value<uint32_t>(&legacy_metrics_interval_ms),
            "legacy millisecond alias for --metrics-interval")(
      "memory-high-bytes", po::value<uint64_t>(&options.memory_high_bytes),
      "cgroup memory.high soft pressure threshold in bytes")(
      "memory-max-bytes", po::value<uint64_t>(&options.memory_max_bytes),
      "cgroup memory.max hard limit in bytes")(
      "cpu-quota-us", po::value<uint64_t>(&options.cpu_quota_us),
      "optional cgroup cpu.max quota in microseconds per period")(
      "cpu-period-us", po::value<uint64_t>(&options.cpu_period_us),
      "cgroup cpu.max period in microseconds")(
      "cpu-weight", po::value<uint64_t>(&options.cpu_weight),
      "cgroup cpu.weight proportional share in 1..10000")(
      "io-weight", po::value<uint64_t>(&options.io_weight),
      "cgroup default io.weight proportional share in 1..10000")(
      "pids-max", po::value<uint64_t>(&options.pids_max),
      "cgroup pids.max process limit")(
      "keep-artifacts",
      po::value<bool>(&options.keep_artifacts)
          ->zero_tokens()
          ->implicit_value(true)
          ->default_value(true),
      "preserve run data, logs, metrics, events, and resolved scenarios")(
      "keep-cgroups", po::bool_switch(&keep_cgroups),
      "leave cgroups after exit for inspection")(
      "cleanup-run",
      po::value<std::string>(&cleanup_run_id)->implicit_value(""),
      "remove stale simulator-owned objects for the optional run id and exit")(
      "isolate-network", po::bool_switch(&options.isolate_network),
      "run each chain node in its own network namespace and veth link")(
      "network-bandwidth-mbps",
      po::value<uint32_t>(&options.network_condition.bandwidth_mbps),
      "TBF bandwidth limit in megabits per second for each isolated node "
      "host-side veth")(
      "network-delay-ms",
      po::value<uint32_t>(&options.network_condition.delay_ms),
      "netem delay applied to each isolated node host-side veth")(
      "network-jitter-ms",
      po::value<uint32_t>(&options.network_condition.jitter_ms),
      "netem jitter applied to each isolated node host-side veth")(
      "network-loss-bps",
      po::value<uint32_t>(&options.network_condition.loss_basis_points),
      "netem packet loss in basis points, 10000 = 100%")(
      "network-duplicate-bps",
      po::value<uint32_t>(&options.network_condition.duplicate_basis_points),
      "netem packet duplication in basis points, 10000 = 100%")(
      "network-corrupt-bps",
      po::value<uint32_t>(&options.network_condition.corrupt_basis_points),
      "netem packet corruption in basis points, 10000 = 100%")(
      "network-reorder-bps",
      po::value<uint32_t>(&options.network_condition.reorder_basis_points),
      "netem packet reordering in basis points, 10000 = 100%")(
      "network-limit-packets",
      po::value<uint32_t>(&options.network_condition.limit_packets),
      "netem queue limit applied to each isolated node host-side veth")(
      "node-network-condition-json",
      po::value<std::vector<std::string>>(
          &legacy_inputs.node_network_conditions)
          ->composing(),
      "repeatable JSON object with node plus network condition fields for one "
      "isolated "
      "node")(
      "runtime-node-network-condition-json",
      po::value<std::vector<std::string>>(
          &legacy_inputs.runtime_node_network_conditions)
          ->composing(),
      "repeatable JSON object with node plus live network condition fields to "
      "apply after isolated nodes are running")(
      "runtime-node-block-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_blocks)
          ->composing(),
      "repeatable JSON object with node, optional src_address, dst_address, "
      "dst_port, and optional handle for one live host-side TCP drop filter")(
      "runtime-node-unblock-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_unblocks)
          ->composing(),
      "repeatable JSON object with node, optional src_address, dst_address, "
      "dst_port, and optional handle for one live host-side TCP drop filter "
      "removal")(
      "runtime-partition-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_partitions)
          ->composing(),
      "repeatable JSON object with group_a and group_b arrays for one live "
      "source-aware group partition")(
      "runtime-heal-partition-json",
      po::value<std::vector<std::string>>(
          &legacy_inputs.runtime_partition_heals)
          ->composing(),
      "repeatable JSON object with group_a and group_b arrays for one live "
      "source-aware group partition heal")(
      "runtime-node-resource-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_resources)
          ->composing(),
      "repeatable JSON object with node plus live cgroup limit fields to apply "
      "after nodes are running")(
      "runtime-node-restart-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_restarts)
          ->composing(),
      "repeatable JSON object with node field for one live node restart after "
      "nodes are running")(
      "runtime-node-freeze-json",
      po::value<std::vector<std::string>>(&legacy_inputs.runtime_node_freezes)
          ->composing(),
      "repeatable JSON object with node and duration_ms for one live cgroup "
      "freeze/thaw after nodes are running")(
      "replace-run", po::bool_switch(&options.replace_run),
      "remove an existing run directory first")(
      "probe-address", po::bool_switch(&options.probe_address),
      "assign and inspect an IPv4 address inside a temporary netns through "
      "libmnl")(
      "probe-bandwidth-limit", po::bool_switch(&options.probe_bandwidth_limit),
      "apply and remove a TBF bandwidth limit on a temporary veth peer through "
      "libmnl")(
      "probe-capabilities", po::bool_switch(&options.probe_capabilities),
      "report effective Linux capabilities needed by privileged simulator "
      "paths")(
      "probe-cgroup-freeze", po::bool_switch(&options.probe_cgroup_freeze),
      "attach a child process to a cgroup and verify cgroup.freeze/thaw "
      "paths")(
      "probe-drop-filter", po::bool_switch(&options.probe_drop_filter),
      "apply and remove a flower/gact TCP drop filter on a temporary veth "
      "through libmnl")(
      "probe-directional-network-condition",
      po::bool_switch(&options.probe_directional_network_condition),
      "apply and remove exact-destination prio/flower/TBF/netem policies "
      "inside a temporary network namespace through libmnl")(
      "probe-netns", po::bool_switch(&options.probe_netns),
      "create a temporary network namespace and inspect it "
      "through setns/libmnl")(
      "probe-network-condition",
      po::bool_switch(&options.probe_network_condition),
      "apply and remove a netem network condition on a temporary veth peer "
      "through libmnl")(
      "probe-combined-network-condition",
      po::bool_switch(&options.probe_combined_network_condition),
      "apply and remove a combined TBF/netem condition on a temporary veth "
      "peer through libmnl")(
      "probe-network-condition-update",
      po::bool_switch(&options.probe_network_condition_update),
      "replace a live host-side netem network condition on a temporary veth "
      "through libmnl")(
      "probe-qdisc", po::bool_switch(&options.probe_qdisc),
      "dump qdisc state for a temporary veth peer through libmnl")(
      "probe-qdisc-mutation", po::bool_switch(&options.probe_qdisc_mutation),
      "replace and delete a root pfifo qdisc on a temporary veth peer through "
      "libmnl")("probe-route", po::bool_switch(&options.probe_route),
                "assign and inspect an IPv4 route inside a temporary netns "
                "through libmnl")(
      "probe-veth", po::bool_switch(&options.probe_veth),
      "create, move, inspect, and delete a temporary veth pair through libmnl")(
      "probe-network", po::bool_switch(&options.probe_network),
      "list links through rtnetlink/libmnl and exit");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  options.chain = ParseChainKind(chain_name);
  options.log_level = ParseLogLevel(log_level_name);
  if (keep_cgroups) {
    options.cleanup_policy = CleanupPolicy::kRetainCgroups;
  }
  if (OptionProvided(vm, "metrics-interval") &&
      OptionProvided(vm, "metrics-interval-ms")) {
    throw std::runtime_error(
        "--metrics-interval and --metrics-interval-ms must not be combined");
  }
  if (OptionProvided(vm, "metrics-interval")) {
    options.metrics_interval =
        PositiveDuration::Parse(metrics_interval_text).value();
  } else if (OptionProvided(vm, "metrics-interval-ms")) {
    options.metrics_interval =
        PositiveDuration::FromMilliseconds(legacy_metrics_interval_ms).value();
  }
  if (OptionProvided(vm, "cleanup-run")) {
    options.cleanup_run = true;
    if (!cleanup_run_id.empty()) {
      if (OptionProvided(vm, "run-id") && options.run_id != cleanup_run_id) {
        throw std::runtime_error(
            "--cleanup-run id and --run-id must match when both are provided");
      }
      options.run_id = cleanup_run_id;
    }
  }

  if (OptionProvided(vm, "scenario")) {
    if (OptionProvided(vm, "scenario-json") ||
        OptionProvided(vm, "scenario-yaml")) {
      throw std::runtime_error(
          "--scenario cannot be combined with its legacy format-specific "
          "aliases");
    }
    const std::string extension = options.scenario.extension().string();
    if (extension == ".json") {
      options.scenario_json = options.scenario;
    } else if (extension == ".yaml" || extension == ".yml") {
      options.scenario_yaml = options.scenario;
    } else {
      throw std::runtime_error(
          "--scenario must use a .json, .yaml, or .yml extension");
    }
  }

  options.block_production.enabled = !no_mining;
  options.block_production.mode = native_mining
                                      ? MiningMode::kNativeMining
                                      : MiningMode::kScheduledBlockProduction;
  options.block_production.policy = BlockProductionPolicy(
      std::chrono::milliseconds(block_production_period_ms),
      block_production_probability, block_production_seed);
  if (OptionProvided(vm, "mining-difficulty")) {
    options.block_production.difficulty = MiningDifficulty(mining_difficulty);
  }

  if (vm.count("help") != 0U) {
    BBP_LOG(info) << "Usage: " << argv[0] << " [options]\n"
                  << canonical_options;
    std::exit(0);
  }
  if (!options.scenario_json.empty() && !options.scenario_yaml.empty()) {
    throw std::runtime_error(
        "--scenario-json and --scenario-yaml are mutually exclusive");
  }
  if (OptionProvided(vm, "benchmark-root") &&
      OptionProvided(vm, "output-dir")) {
    throw std::runtime_error(
        "--benchmark-root and --output-dir are aliases and must not both be "
        "provided");
  }
  const std::uint32_t node_binary_option_count =
      static_cast<std::uint32_t>(OptionProvided(vm, "node-binary")) +
      static_cast<std::uint32_t>(OptionProvided(vm, "chain-daemon")) +
      static_cast<std::uint32_t>(
          OptionProvided(vm, default_chain_spec.daemon_option_name.c_str()));
  if (node_binary_option_count > 1U) {
    throw std::runtime_error(
        "--node-binary and its legacy aliases must not be combined");
  }
  const std::uint32_t stored_run_operation_count =
      static_cast<std::uint32_t>(OptionProvided(vm, "run")) +
      static_cast<std::uint32_t>(OptionProvided(vm, "report-run")) +
      static_cast<std::uint32_t>(options.cleanup_run);
  if (stored_run_operation_count > 1U) {
    throw std::runtime_error(
        "--run, --report-run, and --cleanup-run are mutually exclusive");
  }
  if (vm.count("run") == 0U && OptionProvided(vm, "once")) {
    throw std::runtime_error("--once requires --run");
  }
  if (no_mining && native_mining) {
    throw std::runtime_error(
        "--no-mining and --native-mining are mutually exclusive");
  }
  if (options.tui_refresh_ms == 0U) {
    throw std::runtime_error("--refresh-ms must be greater than zero");
  }
  if (!options.scenario_json.empty()) {
    const boost::json::value scenario =
        boost::json::parse(ReadText(options.scenario_json));
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-json root must be a JSON object");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
  }
  if (!options.scenario_yaml.empty()) {
    const boost::json::value scenario = ParseYamlDocument(
        ReadText(options.scenario_yaml), options.scenario_yaml);
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-yaml root must be a YAML mapping");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
  }
  if (options.simulation_name.empty()) {
    options.simulation_name = options.run_id;
  }
  const std::string active_chain_name(ChainKindName(options.chain));
  auto [active_chain, inserted] = options.chains.try_emplace(
      active_chain_name, ScenarioChain{.driver = options.chain,
                                       .default_binary = options.chain_daemon});
  static_cast<void>(inserted);
  active_chain->second.driver = options.chain;
  active_chain->second.default_binary = options.chain_daemon;
  if (options.simulation_duration) {
    if (options.metrics_sample_count != 0U) {
      throw std::runtime_error(
          "simulation duration and metrics_sample_count must not be "
          "combined");
    }
    const std::chrono::milliseconds wall_duration =
        options.time_scale.WallDuration(*options.simulation_duration);
    const auto maximum_monotonic_delay =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::duration::max());
    if (wall_duration > maximum_monotonic_delay) {
      throw std::runtime_error(
          "scaled simulation duration exceeds monotonic clock range");
    }
  }
  options.network_condition_requested =
      options.network_condition_requested ||
      vm.count("network-bandwidth-mbps") != 0U ||
      vm.count("network-delay-ms") != 0U ||
      vm.count("network-jitter-ms") != 0U ||
      vm.count("network-loss-bps") != 0U ||
      vm.count("network-duplicate-bps") != 0U ||
      vm.count("network-corrupt-bps") != 0U ||
      vm.count("network-reorder-bps") != 0U ||
      vm.count("network-limit-packets") != 0U;
  options.cpu_quota_requested =
      options.cpu_quota_requested || vm.count("cpu-quota-us") != 0U;
  ParseLegacyCliInputs(legacy_inputs, options);
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  if (options.memory_high_bytes > options.memory_max_bytes) {
    throw std::runtime_error(
        "--memory-high-bytes must be less than or equal to --memory-max-bytes");
  }
  if (options.cpu_period_us == 0U) {
    throw std::runtime_error("--cpu-period-us must be greater than zero");
  }
  if (options.cpu_quota_requested && options.cpu_quota_us == 0U) {
    throw std::runtime_error("--cpu-quota-us must be greater than zero");
  }
  RequireCgroupWeight(options.cpu_weight, "--cpu-weight");
  RequireCgroupWeight(options.io_weight, "--io-weight");
  if (options.pids_max == 0U) {
    throw std::runtime_error("--pids-max must be greater than zero");
  }
  if ((options.network_condition_requested ||
       !options.node_network_conditions.empty() ||
       !options.runtime_node_network_conditions.empty() ||
       !options.runtime_node_blocks.empty() ||
       !options.runtime_node_unblocks.empty() ||
       !options.runtime_partitions.empty() ||
       !options.runtime_partition_heals.empty() ||
       TopologyHasDirectionalNetworkConditions(options) ||
       WorkloadsRequireIsolatedNetwork(options)) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network runtime options require --isolate-network");
  }
  if (options.nodes < 1 || options.nodes > chain_spec.max_nodes) {
    throw std::runtime_error("--nodes currently supports 1.." +
                             std::to_string(chain_spec.max_nodes) +
                             " for chain smoke runs");
  }
  RuntimePeerTopology validated_runtime_topology(options.topology.peer_topology,
                                                 options.nodes);
  if (options.generate_node == 0U || options.generate_node > options.nodes) {
    throw std::runtime_error("--generate-node must be in 1..--nodes");
  }
  if (options.workloads.size() > std::numeric_limits<std::uint32_t>::max() -
                                     options.scheduled_events.size()) {
    throw std::runtime_error("scenario action count exceeds uint32 range");
  }
  for (const ScenarioWorkload* configured_workload :
       ConfiguredScenarioActions(options)) {
    const ScenarioWorkload& workload = *configured_workload;
    if (workload.kind == WorkloadKind::kBlockGeneration) {
      if (workload.block_generation.node == 0U ||
          workload.block_generation.node > options.nodes) {
        throw std::runtime_error(
            "scenario block_generation workload node must be in 1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kWaitUntilHeight) {
      if (workload.wait_until_height.node == 0U ||
          workload.wait_until_height.node > options.nodes) {
        throw std::runtime_error(
            "scenario wait_until_height workload node must be in 1..--nodes");
      }
      if (workload.wait_until_height.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario wait_until_height timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kWaitForPeers) {
      if (workload.wait_for_peers.node == 0U ||
          workload.wait_for_peers.node > options.nodes) {
        throw std::runtime_error(
            "scenario wait_for_peers workload node must be in 1..--nodes");
      }
      if (workload.wait_for_peers.peer_count == 0U) {
        throw std::runtime_error(
            "scenario wait_for_peers peer_count must be greater than zero");
      }
      if (workload.wait_for_peers.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario wait_for_peers timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kConnectPeer) {
      if (workload.connect_peer.node == 0U ||
          workload.connect_peer.node > options.nodes) {
        throw std::runtime_error(
            "scenario connect_peer workload node must be in 1..--nodes");
      }
      if (workload.connect_peer.peer == 0U ||
          workload.connect_peer.peer > options.nodes) {
        throw std::runtime_error(
            "scenario connect_peer workload peer must be in 1..--nodes");
      }
      if (workload.connect_peer.node == workload.connect_peer.peer) {
        throw std::runtime_error(
            "scenario connect_peer workload node and peer must differ");
      }
      if (workload.connect_peer.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario connect_peer timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kDisconnectPeer) {
      if (workload.disconnect_peer.node == 0U ||
          workload.disconnect_peer.node > options.nodes) {
        throw std::runtime_error(
            "scenario disconnect_peer workload node must be in 1..--nodes");
      }
      if (workload.disconnect_peer.peer == 0U ||
          workload.disconnect_peer.peer > options.nodes) {
        throw std::runtime_error(
            "scenario disconnect_peer workload peer must be in 1..--nodes");
      }
      if (workload.disconnect_peer.node == workload.disconnect_peer.peer) {
        throw std::runtime_error(
            "scenario disconnect_peer workload node and peer must differ");
      }
      if (workload.disconnect_peer.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario disconnect_peer timeout_sec must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kRestartNode) {
      if (workload.restart_node.node == 0U ||
          workload.restart_node.node > options.nodes) {
        throw std::runtime_error(
            "scenario restart_node workload node must be in 1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kFreezeNode) {
      if (workload.freeze_node.node == 0U ||
          workload.freeze_node.node > options.nodes) {
        throw std::runtime_error(
            "scenario freeze_node workload node must be in 1..--nodes");
      }
      if (workload.freeze_node.duration_ms == 0U) {
        throw std::runtime_error(
            "scenario freeze_node duration_ms must be greater than zero");
      }
    } else if (workload.kind == WorkloadKind::kUpdateResourceLimits) {
      if (workload.update_resource_limits.node == 0U ||
          workload.update_resource_limits.node > options.nodes) {
        throw std::runtime_error(
            "scenario update_resource_limits workload node must be in "
            "1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kSetResourceProfile ||
               workload.kind == WorkloadKind::kSetNetworkProfile) {
      if (workload.profile_switch.nodes.empty()) {
        throw std::runtime_error(
            "scenario profile switch workload requires target nodes");
      }
      for (const uint32_t node : workload.profile_switch.nodes) {
        if (node == 0U || node > options.nodes) {
          throw std::runtime_error(
              "scenario profile switch workload node must be in "
              "1..--nodes");
        }
      }
    } else if (workload.kind == WorkloadKind::kResourcePressure) {
      if (workload.resource_pressure.node == 0U ||
          workload.resource_pressure.node > options.nodes) {
        throw std::runtime_error(
            "scenario resource_pressure workload node must be in "
            "1..--nodes");
      }
      if (workload.resource_pressure.duration_ms == 0U) {
        throw std::runtime_error(
            "scenario resource_pressure duration_ms must be greater than "
            "zero");
      }
    } else if (workload.kind == WorkloadKind::kSetNetworkCondition) {
      if (workload.network_condition.node == 0U ||
          workload.network_condition.node > options.nodes) {
        throw std::runtime_error(
            "scenario set_network_condition workload node must be in "
            "1..--nodes");
      }
      ValidateNetworkCondition(workload.network_condition.condition);
    } else if (workload.kind == WorkloadKind::kBlockNetworkFlow ||
               workload.kind == WorkloadKind::kUnblockNetworkFlow) {
      if (workload.network_block.rule.node_index >= options.nodes) {
        throw std::runtime_error(
            "scenario network flow workload node must be in 1..--nodes");
      }
    } else if (workload.kind == WorkloadKind::kPartitionNodes) {
      ValidateNetworkPartitionRule(workload.network_partition.partition,
                                   options.nodes,
                                   "scenario partition_nodes workload");
    } else if (workload.kind == WorkloadKind::kHealPartition) {
      ValidateNetworkPartitionRule(workload.network_partition.partition,
                                   options.nodes,
                                   "scenario heal_partition workload");
    } else if (IsTopologyEdgeAction(workload.kind)) {
      const TopologyEdgeWorkload& edge = workload.topology_edge;
      if (edge.from == 0U || edge.from > options.nodes) {
        throw std::runtime_error(
            "scenario topology edge action from must be in 1..--nodes");
      }
      if (edge.to == 0U || edge.to > options.nodes) {
        throw std::runtime_error(
            "scenario topology edge action to must be in 1..--nodes");
      }
      if (edge.from == edge.to) {
        throw std::runtime_error(
            "scenario topology edge action from and to must differ");
      }
      if (workload.kind == WorkloadKind::kSetEdgeCondition) {
        if (!edge.condition) {
          throw std::runtime_error(
              "scenario set_edge_condition requires condition fields");
        }
        ValidateNetworkCondition(*edge.condition);
      } else if (edge.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario topology edge action timeout_sec must be greater than "
            "zero");
      }
    } else if (workload.kind == WorkloadKind::kSendRawTransaction) {
      const SendRawTransactionWorkload& transaction =
          workload.send_raw_transaction;
      if (transaction.funding_node == 0U ||
          transaction.funding_node > options.nodes) {
        throw std::runtime_error(
            "scenario send_raw_transaction funding_node must be in "
            "1..--nodes");
      }
      if (transaction.submit_node == 0U ||
          transaction.submit_node > options.nodes) {
        throw std::runtime_error(
            "scenario send_raw_transaction submit_node must be in 1..--nodes");
      }
      if (transaction.source_address == transaction.destination_address) {
        throw std::runtime_error(
            "scenario send_raw_transaction source_address and "
            "destination_address must differ");
      }
      if (transaction.funding_blocks < kDefaultCoinbaseSpendableConfirmations) {
        throw std::runtime_error(
            "scenario send_raw_transaction funding_blocks must be at least " +
            std::to_string(kDefaultCoinbaseSpendableConfirmations));
      }
      if (transaction.amount_satoshis == 0U) {
        throw std::runtime_error(
            "scenario send_raw_transaction amount must be greater than zero");
      }
      if (transaction.fee_satoshis == 0U) {
        throw std::runtime_error(
            "scenario send_raw_transaction fee must be greater than zero");
      }
      if (transaction.amount_satoshis >
          std::numeric_limits<uint64_t>::max() - transaction.fee_satoshis) {
        throw std::runtime_error(
            "scenario send_raw_transaction amount plus fee overflows uint64");
      }
      if (transaction.timeout_sec == 0U) {
        throw std::runtime_error(
            "scenario send_raw_transaction timeout_sec must be greater than "
            "zero");
      }
    } else if (workload.kind == WorkloadKind::kWalletTransactions) {
      ValidateWalletTransactionsWorkload(workload.wallet_transactions, options);
    }
  }
  if (ValidateRuntimeTopologyActionSequence(options,
                                            &validated_runtime_topology) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network runtime options require --isolate-network");
  }
  if (options.topology.configured) {
    SimulationRegistry::FromTopology(options.topology,
                                     options.wallet_initialization);
    if (options.block_production.enabled &&
        options.topology.miner_nodes.empty()) {
      throw std::runtime_error(
          "enabled block production requires at least one configured miner");
    }
  }
  RequireSafeRunId(options.run_id);
  const bool needs_chain_daemon =
      !options.probe_network && options.report_run.empty() &&
      options.tui_run.empty() && !options.probe_bandwidth_limit &&
      !options.probe_capabilities && !options.probe_cgroup_freeze &&
      !options.probe_drop_filter &&
      !options.probe_directional_network_condition && !options.probe_netns &&
      !options.probe_veth && !options.probe_address && !options.probe_route &&
      !options.probe_qdisc && !options.probe_qdisc_mutation &&
      !options.probe_network_condition &&
      !options.probe_combined_network_condition &&
      !options.probe_network_condition_update && !options.cleanup_run;
  if (needs_chain_daemon && options.chain_daemon.empty()) {
    throw std::runtime_error("chain runs require --chain-daemon or --" +
                             chain_spec.daemon_option_name);
  }
  if (needs_chain_daemon) {
    RequireExecutable(options.chain_daemon);
  }
  return options;
}

boost::json::array LinksJson(const std::vector<LinkInfo>& links) {
  boost::json::array links_json;
  for (const LinkInfo& link : links) {
    boost::json::object link_json;
    link_json["index"] = link.index;
    link_json["name"] = link.name;
    link_json["up"] = link.up;
    link_json["has_stats"] = link.has_stats;
    link_json["rx_bytes"] = link.rx_bytes;
    link_json["tx_bytes"] = link.tx_bytes;
    link_json["rx_packets"] = link.rx_packets;
    link_json["tx_packets"] = link.tx_packets;
    link_json["rx_dropped"] = link.rx_dropped;
    link_json["tx_dropped"] = link.tx_dropped;
    link_json["rx_errors"] = link.rx_errors;
    link_json["tx_errors"] = link.tx_errors;
    links_json.push_back(std::move(link_json));
  }
  return links_json;
}

boost::json::array AddressesJson(const std::vector<AddressInfo>& addresses) {
  boost::json::array addresses_json;
  for (const AddressInfo& address : addresses) {
    boost::json::object address_json;
    address_json["if_index"] = address.if_index;
    address_json["if_name"] = address.if_name;
    address_json["address"] = address.address;
    address_json["prefix_len"] = address.prefix_len;
    addresses_json.push_back(std::move(address_json));
  }
  return addresses_json;
}

boost::json::array RoutesJson(const std::vector<RouteInfo>& routes) {
  boost::json::array routes_json;
  for (const RouteInfo& route : routes) {
    boost::json::object route_json;
    route_json["destination"] = route.destination;
    route_json["prefix_len"] = route.prefix_len;
    route_json["oif_index"] = route.oif_index;
    route_json["oif_name"] = route.oif_name;
    route_json["gateway"] = route.gateway;
    route_json["table"] = route.table;
    route_json["protocol"] = route.protocol;
    route_json["scope"] = route.scope;
    route_json["type"] = route.type;
    routes_json.push_back(std::move(route_json));
  }
  return routes_json;
}

boost::json::object QdiscJson(const QdiscInfo& qdisc) {
  boost::json::object qdisc_json;
  qdisc_json["if_index"] = qdisc.if_index;
  qdisc_json["if_name"] = qdisc.if_name;
  qdisc_json["kind"] = qdisc.kernel_kind;
  qdisc_json["handle"] = qdisc.handle;
  qdisc_json["parent"] = qdisc.parent;
  qdisc_json["info"] = qdisc.info;
  qdisc_json["has_stats"] = qdisc.has_stats;
  qdisc_json["bytes"] = qdisc.bytes;
  qdisc_json["packets"] = qdisc.packets;
  qdisc_json["drops"] = qdisc.drops;
  qdisc_json["overlimits"] = qdisc.overlimits;
  qdisc_json["qlen"] = qdisc.qlen;
  qdisc_json["backlog"] = qdisc.backlog;
  qdisc_json["requeues"] = qdisc.requeues;
  qdisc_json["has_netem_options"] = qdisc.has_netem_options;
  qdisc_json["netem_latency_us"] = qdisc.netem_latency_us;
  qdisc_json["netem_jitter_us"] = qdisc.netem_jitter_us;
  qdisc_json["netem_loss"] = qdisc.netem_loss;
  qdisc_json["netem_duplicate"] = qdisc.netem_duplicate;
  qdisc_json["netem_corrupt"] = qdisc.netem_corrupt;
  qdisc_json["netem_reorder"] = qdisc.netem_reorder;
  qdisc_json["netem_limit_packets"] = qdisc.netem_limit_packets;
  qdisc_json["has_tbf_options"] = qdisc.has_tbf_options;
  qdisc_json["tbf_rate_bytes_per_sec"] = qdisc.tbf_rate_bytes_per_sec;
  qdisc_json["tbf_limit_bytes"] = qdisc.tbf_limit_bytes;
  qdisc_json["tbf_buffer_ticks"] = qdisc.tbf_buffer_ticks;
  qdisc_json["tbf_mtu_ticks"] = qdisc.tbf_mtu_ticks;
  qdisc_json["has_prio_options"] = qdisc.has_prio_options;
  qdisc_json["prio_bands"] = qdisc.prio_bands;
  return qdisc_json;
}

boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs) {
  boost::json::array qdiscs_json;
  for (const QdiscInfo& qdisc : qdiscs) {
    boost::json::object qdisc_json = QdiscJson(qdisc);
    qdiscs_json.push_back(std::move(qdisc_json));
  }
  return qdiscs_json;
}

boost::json::array TcFiltersJson(const std::vector<TcFilterInfo>& filters) {
  boost::json::array filters_json;
  for (const TcFilterInfo& filter : filters) {
    boost::json::object filter_json;
    filter_json["if_index"] = filter.if_index;
    filter_json["if_name"] = filter.if_name;
    filter_json["kind"] = filter.kernel_kind;
    filter_json["handle"] = filter.handle;
    filter_json["parent"] = filter.parent;
    filter_json["priority"] = filter.priority;
    filter_json["protocol"] = filter.protocol;
    filter_json["egress"] = filter.egress;
    filter_json["ingress"] = filter.ingress;
    filter_json["has_eth_type"] = filter.has_eth_type;
    filter_json["eth_type"] = filter.eth_type;
    filter_json["has_ip_proto"] = filter.has_ip_proto;
    filter_json["ip_proto"] = filter.ip_proto;
    filter_json["has_ipv4_src"] = filter.has_ipv4_src;
    filter_json["ipv4_src"] = filter.ipv4_src;
    filter_json["has_ipv4_src_mask"] = filter.has_ipv4_src_mask;
    filter_json["ipv4_src_mask"] = filter.ipv4_src_mask;
    filter_json["has_ipv4_dst"] = filter.has_ipv4_dst;
    filter_json["ipv4_dst"] = filter.ipv4_dst;
    filter_json["has_ipv4_dst_mask"] = filter.has_ipv4_dst_mask;
    filter_json["ipv4_dst_mask"] = filter.ipv4_dst_mask;
    filter_json["has_tcp_dst"] = filter.has_tcp_dst;
    filter_json["tcp_dst"] = filter.tcp_dst;
    filter_json["has_tcp_dst_mask"] = filter.has_tcp_dst_mask;
    filter_json["tcp_dst_mask"] = filter.tcp_dst_mask;
    filter_json["has_class_id"] = filter.has_class_id;
    filter_json["class_id"] = filter.class_id;
    filter_json["has_drop_action"] = filter.has_drop_action;
    filter_json["has_stats"] = filter.has_stats;
    filter_json["match_bytes"] = filter.match_bytes;
    filter_json["match_packets"] = filter.match_packets;
    filter_json["drop_packets"] = filter.drop_packets;
    filters_json.push_back(std::move(filter_json));
  }
  return filters_json;
}

void AddNetworkConditionJsonFields(const NetworkCondition& condition,
                                   boost::json::object* object) {
  (*object)["bandwidth_mbps"] = condition.bandwidth_mbps;
  (*object)["delay_ms"] = condition.delay_ms;
  (*object)["jitter_ms"] = condition.jitter_ms;
  (*object)["loss_basis_points"] = condition.loss_basis_points;
  (*object)["duplicate_basis_points"] = condition.duplicate_basis_points;
  (*object)["corrupt_basis_points"] = condition.corrupt_basis_points;
  (*object)["reorder_basis_points"] = condition.reorder_basis_points;
  (*object)["limit_packets"] = condition.limit_packets;
}

boost::json::object NetworkConditionJson(const NetworkCondition& condition) {
  boost::json::object object;
  AddNetworkConditionJsonFields(condition, &object);
  return object;
}

boost::json::array DirectionalNetworkPoliciesJson(
    const std::vector<DirectionalNetworkPolicy>& policies) {
  boost::json::array array;
  for (const DirectionalNetworkPolicy& policy : policies) {
    boost::json::object object;
    object["band"] = policy.band;
    object["destination_address"] = policy.destination_address;
    object["condition"] = NetworkConditionJson(policy.condition);
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::object NetworkBlockRuleJson(const NetworkBlockRule& rule) {
  boost::json::object object;
  object["node"] = rule.node_index + 1U;
  if (!rule.src_address.empty()) {
    object["src_address"] = rule.src_address;
  }
  object["dst_address"] = rule.dst_address;
  object["dst_port"] = rule.dst_port;
  object["handle"] = rule.handle;
  return object;
}

boost::json::array NodeGroupJson(const std::vector<uint32_t>& nodes) {
  boost::json::array array;
  for (uint32_t node_index : nodes) {
    array.push_back(node_index + 1U);
  }
  return array;
}

boost::json::object NetworkPartitionRuleJson(const NetworkPartitionRule& rule) {
  boost::json::object object;
  object["group_a"] = NodeGroupJson(rule.group_a);
  object["group_b"] = NodeGroupJson(rule.group_b);
  return object;
}

boost::json::object WalletInitializationJson(
    const WalletInitialization& initialization) {
  boost::json::object object;
  object["strategy"] =
      std::string(WalletInitializationStrategyName(initialization.strategy));
  object["mode"] = std::string(WalletPrivacyModeName(initialization.mode));
  if (!initialization.seed.empty()) {
    object["seed"] = initialization.seed;
  }
  return object;
}

boost::json::object PeerConnectivityPolicyJson(
    const PeerConnectivityPolicy& policy) {
  boost::json::object object;
  object["node"] = policy.node + 1U;
  object["mode"] = std::string(PeerConnectivityModeName(policy.mode));
  if (policy.mode == PeerConnectivityMode::kAllPeers) {
    object["all_peers"] = true;
  } else {
    object["min_peer_count"] = policy.peer_count.minimum();
    object["max_peer_count"] = policy.peer_count.maximum();
  }
  return object;
}

boost::json::array PeerConnectivityPoliciesJson(
    const std::vector<PeerConnectivityPolicy>& policies) {
  boost::json::array array;
  for (const PeerConnectivityPolicy& policy : policies) {
    array.push_back(PeerConnectivityPolicyJson(policy));
  }
  return array;
}

boost::json::array TopologyGroupsJson(
    const std::vector<std::vector<uint32_t>>& groups) {
  boost::json::array array;
  for (const std::vector<uint32_t>& group : groups) {
    array.push_back(NodeGroupJson(group));
  }
  return array;
}

boost::json::array PeerTopologyEdgesJson(
    const std::vector<PeerTopologyEdge>& edges) {
  boost::json::array array;
  for (const PeerTopologyEdge& edge : edges) {
    boost::json::object object;
    object["from"] = edge.from + 1U;
    object["to"] = edge.to + 1U;
    object["bidirectional"] = edge.bidirectional;
    object["active"] = edge.active;
    if (edge.latency_ms) {
      object["latency_ms"] = *edge.latency_ms;
    }
    if (edge.condition) {
      AddNetworkConditionJsonFields(*edge.condition, &object);
    }
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::array PeerTopologyRegionEdgesJson(
    const std::vector<PeerTopologyRegionEdge>& edges) {
  boost::json::array array;
  for (const PeerTopologyRegionEdge& edge : edges) {
    boost::json::object object;
    object["from_region"] = edge.from_region + 1U;
    object["to_region"] = edge.to_region + 1U;
    object["bidirectional"] = edge.bidirectional;
    object["active"] = edge.active;
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::array LatencyMatrixJson(
    const std::vector<std::vector<std::optional<uint32_t>>>& matrix) {
  boost::json::array result;
  for (const auto& input_row : matrix) {
    boost::json::array row;
    for (const std::optional<uint32_t>& latency_ms : input_row) {
      if (latency_ms) {
        row.push_back(*latency_ms);
      } else {
        row.push_back(nullptr);
      }
    }
    result.push_back(std::move(row));
  }
  return result;
}

boost::json::array ResolvedPeerTopologyEdgesJson(
    const PeerTopologyConfig& topology, uint32_t node_count) {
  boost::json::array array;
  for (const ResolvedPeerTopologyEdge& edge :
       ResolvePeerTopologyEdges(topology, node_count)) {
    boost::json::object object;
    object["from"] = edge.from + 1U;
    object["to"] = edge.to + 1U;
    if (edge.latency_ms) {
      object["latency_ms"] = *edge.latency_ms;
    }
    if (edge.condition) {
      AddNetworkConditionJsonFields(*edge.condition, &object);
    }
    array.push_back(std::move(object));
  }
  return array;
}

boost::json::object RuntimePeerTopologyEdgeJson(
    const RuntimePeerTopologyEdge& edge) {
  boost::json::object object;
  object["from"] = edge.from + 1U;
  object["to"] = edge.to + 1U;
  object["band"] = edge.band;
  object["active"] = edge.active;
  if (edge.condition) {
    object["condition"] = NetworkConditionJson(*edge.condition);
  } else {
    object["condition"] = nullptr;
  }
  return object;
}

boost::json::array RuntimePeerTopologyEdgesJson(
    const RuntimePeerTopology& topology) {
  boost::json::array array;
  for (const RuntimePeerTopologyEdge& edge : topology.edges()) {
    array.push_back(RuntimePeerTopologyEdgeJson(edge));
  }
  return array;
}

void AddPeerTopologyJson(const PeerTopologyConfig& topology,
                         uint32_t node_count, boost::json::object* object) {
  (*object)["type"] = std::string(PeerTopologyKindName(topology.kind));
  switch (topology.kind) {
    case PeerTopologyKind::kFullMesh:
    case PeerTopologyKind::kRing:
      break;
    case PeerTopologyKind::kStar:
      (*object)["center_node"] = topology.star_center + 1U;
      break;
    case PeerTopologyKind::kRandomGraph:
      (*object)["seed"] = topology.seed;
      (*object)["average_degree"] = topology.average_degree;
      break;
    case PeerTopologyKind::kScaleFreeGraph:
      (*object)["seed"] = topology.seed;
      (*object)["average_degree"] = topology.average_degree;
      (*object)["attachment_count"] = topology.attachment_count;
      break;
    case PeerTopologyKind::kLatencyMatrix:
      (*object)["latency_matrix_ms"] =
          LatencyMatrixJson(topology.latency_matrix_ms);
      break;
    case PeerTopologyKind::kCustomEdgeList:
      (*object)["edges"] = PeerTopologyEdgesJson(topology.edges);
      break;
    case PeerTopologyKind::kPartitionedGroups:
      (*object)["groups"] = TopologyGroupsJson(topology.groups);
      break;
    case PeerTopologyKind::kInternetLikeRegionGraph:
      (*object)["regions"] = TopologyGroupsJson(topology.regions);
      if (!topology.region_edges.empty()) {
        (*object)["region_edges"] =
            PeerTopologyRegionEdgesJson(topology.region_edges);
      }
      break;
  }
  (*object)["resolved_edges"] =
      ResolvedPeerTopologyEdgesJson(topology, node_count);
}

boost::json::object NodeRoleTopologyJson(
    const NodeRoleTopology& topology,
    const WalletInitialization& wallet_initialization) {
  boost::json::object object;
  object["node_count"] = topology.node_count;
  object["wallet_node_count"] = topology.wallet_node_count;
  object["miner_node_count"] = topology.miner_node_count;
  object["allow_miner_wallet_overlap"] = topology.allow_miner_wallet_overlap;
  object["wallet_nodes"] = NodeGroupJson(topology.wallet_nodes);
  object["miner_nodes"] = NodeGroupJson(topology.miner_nodes);
  object["wallet_initialization"] =
      WalletInitializationJson(wallet_initialization);
  AddPeerTopologyJson(topology.peer_topology, topology.node_count, &object);
  if (!topology.peer_connectivity.empty()) {
    object["peer_connectivity"] =
        PeerConnectivityPoliciesJson(topology.peer_connectivity);
  }
  return object;
}

boost::json::array IoLimitsJson(const std::vector<IoLimit>& io_limits) {
  boost::json::array array;
  for (const IoLimit& limit : io_limits) {
    boost::json::object item;
    item["device"] = BlockDeviceIdText(limit.device);
    const auto add_limit = [&](const char* field,
                               const std::optional<uint64_t>& value) {
      if (value) {
        item[field] = *value;
      } else {
        item[field] = nullptr;
      }
    };
    add_limit("read_bytes_per_sec", limit.read_bytes_per_sec);
    add_limit("write_bytes_per_sec", limit.write_bytes_per_sec);
    add_limit("read_operations_per_sec", limit.read_operations_per_sec);
    add_limit("write_operations_per_sec", limit.write_operations_per_sec);
    array.push_back(std::move(item));
  }
  return array;
}

boost::json::object ResourceLimitsJson(const ResourceLimits& limits) {
  boost::json::object object;
  object["memory_high_bytes"] = limits.memory_high_bytes;
  object["memory_max_bytes"] = limits.memory_max_bytes;
  if (limits.cpu_quota_us) {
    object["cpu_quota_us"] = *limits.cpu_quota_us;
  } else {
    object["cpu_quota_us"] = nullptr;
  }
  object["cpu_period_us"] = limits.cpu_period_us;
  object["cpu_weight"] = limits.cpu_weight;
  object["io_weight"] = limits.io_weight;
  object["io_max"] = IoLimitsJson(limits.io_limits);
  object["pids_max"] = limits.pids_max;
  return object;
}

boost::json::object ResourceLimitPatchJson(const ResourceLimitPatch& patch) {
  boost::json::object object;
  if (patch.memory_high_bytes) {
    object["memory_high_bytes"] = *patch.memory_high_bytes;
  }
  if (patch.memory_max_bytes) {
    object["memory_max_bytes"] = *patch.memory_max_bytes;
  }
  if (patch.cpu_quota_present) {
    if (patch.cpu_quota_us) {
      object["cpu_quota_us"] = *patch.cpu_quota_us;
    } else {
      object["cpu_quota_us"] = nullptr;
    }
  }
  if (patch.cpu_period_us) {
    object["cpu_period_us"] = *patch.cpu_period_us;
  }
  if (patch.cpu_weight) {
    object["cpu_weight"] = *patch.cpu_weight;
  }
  if (patch.io_weight) {
    object["io_weight"] = *patch.io_weight;
  }
  if (patch.io_limits_present) {
    object["io_max"] = IoLimitsJson(patch.io_limits);
  }
  if (patch.pids_max) {
    object["pids_max"] = *patch.pids_max;
  }
  return object;
}

ResourceLimits InitialResourceLimits(const Options& options) {
  return ResourceLimits{
      .memory_high_bytes = options.memory_high_bytes,
      .memory_max_bytes = options.memory_max_bytes,
      .cpu_quota_us = options.cpu_quota_requested
                          ? std::optional<uint64_t>(options.cpu_quota_us)
                          : std::nullopt,
      .cpu_period_us = options.cpu_period_us,
      .cpu_weight = options.cpu_weight,
      .io_weight = options.io_weight,
      .io_limits = options.io_limits,
      .pids_max = options.pids_max,
  };
}

ResourceLimits InitialResourceLimits(const Options& options,
                                     uint32_t node_index) {
  const auto node_limits = options.node_resource_limits.find(node_index);
  return node_limits == options.node_resource_limits.end()
             ? InitialResourceLimits(options)
             : node_limits->second;
}

ResourceLimits ApplyResourceLimitPatch(const ResourceLimits& current,
                                       const ResourceLimitPatch& patch,
                                       const std::string& node_id) {
  ResourceLimits next = current;
  if (patch.memory_high_bytes) {
    next.memory_high_bytes = *patch.memory_high_bytes;
  }
  if (patch.memory_max_bytes) {
    next.memory_max_bytes = *patch.memory_max_bytes;
  }
  if (patch.cpu_quota_present) {
    next.cpu_quota_us = patch.cpu_quota_us;
  }
  if (patch.cpu_period_us) {
    next.cpu_period_us = *patch.cpu_period_us;
  }
  if (patch.cpu_weight) {
    next.cpu_weight = *patch.cpu_weight;
  }
  if (patch.io_weight) {
    next.io_weight = *patch.io_weight;
  }
  if (patch.io_limits_present) {
    next.io_limits = patch.io_limits;
  }
  if (patch.pids_max) {
    next.pids_max = *patch.pids_max;
  }
  if (next.memory_high_bytes > next.memory_max_bytes) {
    throw std::runtime_error("runtime resource update for " + node_id +
                             " would make memory_high_bytes exceed "
                             "memory_max_bytes");
  }
  RequireNonZero(next.memory_max_bytes, "memory_max_bytes");
  RequireNonZero(next.cpu_period_us, "cpu_period_us");
  if (next.cpu_quota_us) {
    RequireNonZero(*next.cpu_quota_us, "cpu_quota_us");
  }
  RequireCgroupWeight(next.cpu_weight, "cpu_weight");
  RequireCgroupWeight(next.io_weight, "io_weight");
  RequireNonZero(next.pids_max, "pids_max");
  return next;
}

void VerifyResourceLimits(const Cgroup& cgroup,
                          const ResourceLimits& expected) {
  const CgroupMetrics actual = cgroup.ReadMetrics();
  const auto finite_io_limits = [](const std::vector<IoLimit>& limits) {
    std::map<BlockDeviceId, IoLimit> result;
    for (const IoLimit& limit : limits) {
      if (limit.read_bytes_per_sec || limit.write_bytes_per_sec ||
          limit.read_operations_per_sec || limit.write_operations_per_sec) {
        result.emplace(limit.device, limit);
      }
    }
    return result;
  };
  if (actual.memory_high_limit_bytes != expected.memory_high_bytes ||
      actual.memory_max_limit_bytes != expected.memory_max_bytes ||
      actual.cpu_quota_us != expected.cpu_quota_us ||
      actual.cpu_period_us != expected.cpu_period_us ||
      actual.cpu_weight != expected.cpu_weight ||
      actual.io_weight != expected.io_weight ||
      finite_io_limits(actual.io_limits) !=
          finite_io_limits(expected.io_limits) ||
      actual.pids_max_limit != expected.pids_max) {
    throw std::runtime_error(
        "resource limit read-back verification failed for " +
        cgroup.path().string());
  }
}

void WriteResourceLimits(const Cgroup& cgroup, const ResourceLimits& previous,
                         const ResourceLimits& next) {
  if (next.memory_max_bytes != previous.memory_max_bytes &&
      next.memory_max_bytes > previous.memory_max_bytes) {
    cgroup.SetMemoryMax(next.memory_max_bytes);
  }
  if (next.memory_high_bytes != previous.memory_high_bytes) {
    cgroup.SetMemoryHigh(next.memory_high_bytes);
  }
  if (next.memory_max_bytes != previous.memory_max_bytes &&
      next.memory_max_bytes <= previous.memory_max_bytes) {
    cgroup.SetMemoryMax(next.memory_max_bytes);
  }
  if (next.cpu_quota_us != previous.cpu_quota_us ||
      next.cpu_period_us != previous.cpu_period_us) {
    cgroup.SetCpuMax(next.cpu_quota_us, next.cpu_period_us);
  }
  if (next.cpu_weight != previous.cpu_weight) {
    cgroup.SetCpuWeight(next.cpu_weight);
  }
  if (next.io_weight != previous.io_weight) {
    cgroup.SetIoWeight(next.io_weight);
  }
  for (const IoLimit& previous_limit : previous.io_limits) {
    const auto next_limit =
        std::find_if(next.io_limits.begin(), next.io_limits.end(),
                     [&](const IoLimit& candidate) {
                       return candidate.device == previous_limit.device;
                     });
    if (next_limit == next.io_limits.end()) {
      cgroup.SetIoMax(IoLimit{
          .device = previous_limit.device,
          .read_bytes_per_sec = std::nullopt,
          .write_bytes_per_sec = std::nullopt,
          .read_operations_per_sec = std::nullopt,
          .write_operations_per_sec = std::nullopt,
      });
    }
  }
  for (const IoLimit& next_limit : next.io_limits) {
    const auto previous_limit =
        std::find_if(previous.io_limits.begin(), previous.io_limits.end(),
                     [&](const IoLimit& candidate) {
                       return candidate.device == next_limit.device;
                     });
    if (previous_limit == previous.io_limits.end() ||
        *previous_limit != next_limit) {
      cgroup.SetIoMax(next_limit);
    }
  }
  if (next.pids_max != previous.pids_max) {
    cgroup.SetPidsMax(next.pids_max);
  }
  VerifyResourceLimits(cgroup, next);
}

std::string NetworkConditionVerificationDetail(
    const NodeVethConfig& config, const QdiscInfo& qdisc,
    std::uint32_t workload_index = 0, std::uint32_t workload_count = 0,
    std::optional<std::uint64_t> operator_sequence = std::nullopt) {
  boost::json::object detail;
  detail["host_if"] = config.host_name;
  detail["condition"] = NetworkConditionJson(config.condition);
  detail["qdisc_kind"] = qdisc.kernel_kind;
  detail["qdisc_handle"] = qdisc.handle;
  detail["qdisc_parent"] = qdisc.parent;
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  return boost::json::serialize(detail);
}

const SimulationNetworkAddressPlan& NetworkAddressPlan(const Options& options) {
  if (!options.network_address_plan) {
    throw std::logic_error(
        "isolated simulation network address plan is not initialized");
  }
  return *options.network_address_plan;
}

uint32_t StableRunHash(std::string_view run_id) {
  uint32_t hash = 2166136261U;
  for (const unsigned char c : run_id) {
    hash ^= c;
    hash *= 16777619U;
  }
  return hash;
}

std::string RunInterfaceToken(std::string_view run_id) {
  constexpr char kHex[] = "0123456789abcdef";
  uint32_t value = StableRunHash(run_id) & 0x00FFFFFFU;
  std::string token(6, '0');
  for (int i = 5; i >= 0; --i) {
    token[static_cast<size_t>(i)] = kHex[value & 0x0FU];
    value >>= 4U;
  }
  return token;
}

std::string NodeInterfaceName(std::string_view run_id, uint32_t node_index,
                              char suffix) {
  return "bbp" + RunInterfaceToken(run_id) + "n" +
         std::to_string(node_index + 1U) + suffix;
}

void RequireRunNetworkInterfacesAvailable(const Options& options) {
  const std::vector<LinkInfo> links = ListNetworkLinks();
  for (std::uint32_t node_index = 0; node_index < options.nodes; ++node_index) {
    for (const char suffix : {'h', 'p'}) {
      const std::string name =
          NodeInterfaceName(options.run_id, node_index, suffix);
      const auto collision =
          std::find_if(links.begin(), links.end(),
                       [&](const LinkInfo& link) { return link.name == name; });
      if (collision != links.end()) {
        throw std::runtime_error(
            "isolated simulation network interface collision: " + name);
      }
    }
  }
}

NodeVethConfig MakeNodeVethConfig(const Options& options, uint32_t node_index) {
  NodeVethConfig config;
  config.host_name = NodeInterfaceName(options.run_id, node_index, 'h');
  config.peer_name = NodeInterfaceName(options.run_id, node_index, 'p');
  config.host_address = NetworkAddressPlan(options).HostAddress(node_index);
  config.node_address = NetworkAddressPlan(options).NodeAddress(node_index);
  config.prefix_len = NetworkAddressPlan(options).NodePrefixLength();
  const auto node_condition = options.node_network_conditions.find(node_index);
  if (node_condition != options.node_network_conditions.end()) {
    config.apply_condition = true;
    config.condition = node_condition->second;
  } else {
    config.apply_condition = options.network_condition_requested;
    config.condition = options.network_condition;
  }
  return config;
}

std::string PeerHost(const Options& options, uint32_t node_index) {
  if (options.isolate_network) {
    return NetworkAddressPlan(options).NodeAddress(node_index);
  }
  return "127.0.0.1";
}

std::string StartupPeerAddress(const Options& options,
                               const ChainDriverSpec& chain_spec,
                               uint32_t node_index) {
  return PeerHost(options, node_index) + ":" +
         std::to_string(static_cast<uint32_t>(chain_spec.p2p_port_base) +
                        node_index);
}

std::vector<uint32_t> ConfiguredStartupPeerIndexes(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    uint32_t node_index) {
  std::vector<uint32_t> eligible =
      runtime_topology.ActivePeerIndexes(node_index);
  const PeerConnectivityPolicy* policy =
      FindPeerConnectivityPolicy(options.topology, node_index);
  if (policy == nullptr) {
    return eligible;
  }
  const uint32_t initial_peer_count = policy->peer_count.minimum();
  if (initial_peer_count > eligible.size()) {
    throw std::runtime_error(
        "initial peer count exceeds eligible logical topology peers");
  }
  eligible.resize(initial_peer_count);
  return eligible;
}

bool TopologyHasDirectionalNetworkConditions(const Options& options) {
  const std::vector<ResolvedPeerTopologyEdge> edges =
      ResolvePeerTopologyEdges(options.topology.peer_topology, options.nodes);
  return std::any_of(edges.begin(), edges.end(), [](const auto& edge) {
    return edge.condition.has_value();
  });
}

std::vector<DirectionalNetworkPolicy> DirectionalNetworkPoliciesForNode(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    uint32_t node_index) {
  return runtime_topology.DirectionalPolicies(NetworkAddressPlan(options),
                                              node_index);
}

std::vector<std::string> StartupPeerAddresses(
    const Options& options, const RuntimePeerTopology& topology,
    const ChainDriverSpec& chain_spec, uint32_t node_index) {
  const std::vector<uint32_t> peer_indexes =
      ConfiguredStartupPeerIndexes(options, topology, node_index);
  std::vector<std::string> peers;
  peers.reserve(peer_indexes.size());
  for (uint32_t peer_index : peer_indexes) {
    peers.push_back(StartupPeerAddress(options, chain_spec, peer_index));
  }
  return peers;
}

bool HostIpv4ForwardingEnabled() {
  const std::string value = ReadText("/proc/sys/net/ipv4/ip_forward");
  return !value.empty() && value.front() == '1';
}

void RequireSafeOutputDirectory(const std::filesystem::path& output_dir) {
  if (output_dir.empty()) {
    throw std::runtime_error("output directory must not be empty");
  }
  const std::filesystem::path absolute = std::filesystem::absolute(output_dir);
  if (absolute == absolute.root_path()) {
    throw std::runtime_error("output directory must not be filesystem root");
  }
}

bool IsOwnedRunDirectory(const std::filesystem::path& run_root) {
  return std::filesystem::exists(run_root / kRunMarkerFile) ||
         std::filesystem::exists(run_root / "resolved-scenario.json");
}

void RequireOwnedRunDirectory(const std::filesystem::path& run_root) {
  if (!std::filesystem::is_directory(run_root)) {
    throw std::runtime_error("run path exists but is not a directory: " +
                             run_root.string());
  }
  if (!IsOwnedRunDirectory(run_root)) {
    throw std::runtime_error(
        "refusing to remove directory without simulator marker: " +
        run_root.string());
  }
}

const LinkInfo* FindLinkByName(const std::vector<LinkInfo>& links,
                               std::string_view name) {
  for (const LinkInfo& link : links) {
    if (link.name == name) {
      return &link;
    }
  }
  return nullptr;
}

const QdiscInfo* FindQdiscByInterfaceName(const std::vector<QdiscInfo>& qdiscs,
                                          std::string_view name) {
  for (const QdiscInfo& qdisc : qdiscs) {
    if (qdisc.if_name == name) {
      return &qdisc;
    }
  }
  return nullptr;
}

QdiscInfo VerifyNodeNetworkCondition(const NodeVethConfig& config) {
  const std::vector<QdiscInfo> qdiscs = ListQdiscs();
  QdiscInfo summary;
  if (!QdiscsMatchNetworkCondition(qdiscs, config.host_name, config.condition,
                                   &summary)) {
    throw std::runtime_error(
        "host-side qdisc does not match requested network condition: " +
        config.host_name);
  }
  return summary;
}

std::string NetworkProbeJson() {
  boost::json::object result;
  result["links"] = LinksJson(ListNetworkLinks());
  result["ipv4_addresses"] = AddressesJson(ListIpv4Addresses());
  result["ipv4_routes"] = RoutesJson(ListIpv4Routes());
  result["qdiscs"] = QdiscsJson(ListQdiscs());
  result["tc_filters"] = TcFiltersJson(ListTcFilters());
  return boost::json::serialize(result);
}

std::string CapabilityProbeJson() {
  boost::json::object result;
  result["cap_sys_admin"] = HasEffectiveCapability(CAP_SYS_ADMIN);
  result["cap_net_admin"] = HasEffectiveCapability(CAP_NET_ADMIN);
  result["cap_sys_resource"] = HasEffectiveCapability(CAP_SYS_RESOURCE);
  return boost::json::serialize(result);
}

std::string CgroupFreezeProbeJson() {
  CgroupFreezeProbe probe = Cgroup::ProbeFreezeThaw();
  boost::json::object result;
  result["run_id"] = probe.run_id;
  result["node_id"] = probe.node_id;
  result["child_pid"] = probe.child_pid;
  result["frozen_after_freeze"] = probe.frozen_after_freeze;
  result["frozen_after_thaw"] = probe.frozen_after_thaw;
  return boost::json::serialize(result);
}

std::string NetworkNamespaceProbeJson() {
  NetworkNamespaceProbe probe = ProbeIsolatedNetworkNamespace();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["parent_links"] = LinksJson(probe.parent_links);
  result["namespace_links"] = LinksJson(probe.namespace_links);
  return boost::json::serialize(result);
}

std::string VethProbeJson() {
  VethProbe probe = ProbeVethPair();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["parent_before"] = LinksJson(probe.parent_before);
  result["parent_after_create"] = LinksJson(probe.parent_after_create);
  result["parent_after_move"] = LinksJson(probe.parent_after_move);
  result["namespace_after_move"] = LinksJson(probe.namespace_after_move);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string NetworkConditionProbeJson() {
  NetworkConditionProbe probe = ProbeNetworkCondition();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string CombinedNetworkConditionProbeJson() {
  NetworkConditionProbe probe = ProbeCombinedNetworkCondition();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string DirectionalNetworkPolicyProbeJson() {
  DirectionalNetworkPolicyProbe probe = ProbeDirectionalNetworkPolicies();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["policies"] = DirectionalNetworkPoliciesJson(probe.policies);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_filters_after_apply"] =
      TcFiltersJson(probe.namespace_filters_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["namespace_filters_after_delete"] =
      TcFiltersJson(probe.namespace_filters_after_delete);
  result["non_owned_root_preserved"] = probe.non_owned_root_preserved;
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string BandwidthLimitProbeJson() {
  BandwidthLimitProbe probe = ProbeBandwidthLimit();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["condition"] = NetworkConditionJson(probe.condition);
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string NetworkConditionUpdateProbeJson() {
  NetworkConditionUpdateProbe probe = ProbeNetworkConditionUpdate();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["initial_condition"] = NetworkConditionJson(probe.initial_condition);
  result["updated_condition"] = NetworkConditionJson(probe.updated_condition);
  result["parent_qdiscs_after_initial"] =
      QdiscsJson(probe.parent_qdiscs_after_initial);
  result["parent_qdiscs_after_update"] =
      QdiscsJson(probe.parent_qdiscs_after_update);
  result["parent_qdiscs_after_delete"] =
      QdiscsJson(probe.parent_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string DropFilterProbeJson() {
  DropFilterProbe probe = ProbeDropFilter();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["dst_address"] = probe.dst_address;
  result["dst_port"] = probe.dst_port;
  result["handle"] = probe.handle;
  result["parent_filters_before"] = TcFiltersJson(probe.parent_filters_before);
  result["parent_filters_after_apply"] =
      TcFiltersJson(probe.parent_filters_after_apply);
  result["parent_filters_after_delete"] =
      TcFiltersJson(probe.parent_filters_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string QdiscProbeJson() {
  QdiscProbe probe = ProbeQdiscDump();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["namespace_links"] = LinksJson(probe.namespace_links);
  result["namespace_qdiscs"] = QdiscsJson(probe.namespace_qdiscs);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string QdiscMutationProbeJson() {
  QdiscMutationProbe probe = ProbeQdiscMutation();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["pfifo_limit_packets"] = probe.pfifo_limit_packets;
  result["namespace_qdiscs_before"] = QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_replace"] =
      QdiscsJson(probe.namespace_qdiscs_after_replace);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string RouteProbeJson() {
  RouteProbe probe = ProbeIpv4RouteAssignment();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["assigned_address"] = probe.assigned_address;
  result["assigned_prefix_len"] = probe.assigned_prefix_len;
  result["route_destination"] = probe.route_destination;
  result["route_prefix_len"] = probe.route_prefix_len;
  result["namespace_links_after_route"] =
      LinksJson(probe.namespace_links_after_route);
  result["namespace_addresses"] = AddressesJson(probe.namespace_addresses);
  result["namespace_routes"] = RoutesJson(probe.namespace_routes);
  result["namespace_addresses_after_delete"] =
      AddressesJson(probe.namespace_addresses_after_delete);
  result["namespace_routes_after_delete"] =
      RoutesJson(probe.namespace_routes_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

std::string AddressProbeJson() {
  AddressProbe probe = ProbeIpv4AddressAssignment();
  boost::json::object result;
  result["helper_pid"] = probe.helper_pid;
  result["host_name"] = probe.host_name;
  result["peer_name"] = probe.peer_name;
  result["assigned_address"] = probe.assigned_address;
  result["assigned_prefix_len"] = probe.assigned_prefix_len;
  result["parent_after_move"] = LinksJson(probe.parent_after_move);
  result["namespace_links_after_address"] =
      LinksJson(probe.namespace_links_after_address);
  result["namespace_addresses"] = AddressesJson(probe.namespace_addresses);
  result["namespace_addresses_after_delete"] =
      AddressesJson(probe.namespace_addresses_after_delete);
  result["parent_after_delete"] = LinksJson(probe.parent_after_delete);
  return boost::json::serialize(result);
}

boost::json::object NetworkPolicyCounterJson(const TcFilterInfo& filter) {
  boost::json::object object;
  object["kind"] = "ipv4_tcp_drop";
  object["handle"] = filter.handle;
  if (filter.has_ipv4_src) {
    object["src_address"] = filter.ipv4_src;
  } else {
    object["src_address"] = nullptr;
  }
  object["dst_address"] = filter.ipv4_dst;
  object["dst_port"] = filter.tcp_dst;
  object["has_stats"] = filter.has_stats;
  object["match_bytes"] = filter.match_bytes;
  object["match_packets"] = filter.match_packets;
  object["drop_packets"] = filter.drop_packets;
  return object;
}

boost::json::object DirectionalNetworkPolicyCounterJson(
    const DirectionalNetworkPolicyCounter& counter) {
  boost::json::object object;
  object["band"] = counter.band;
  object["destination_address"] = counter.destination_address;
  object["filter_handle"] = counter.filter.handle;
  object["filter_has_stats"] = counter.filter.has_stats;
  object["filter_match_bytes"] = counter.filter.match_bytes;
  object["filter_match_packets"] = counter.filter.match_packets;
  object["qdisc_bytes"] = counter.qdisc_bytes;
  object["qdisc_packets"] = counter.qdisc_packets;
  object["qdisc_drops"] = counter.qdisc_drops;
  object["qdisc_overlimits"] = counter.qdisc_overlimits;
  object["qdisc_qlen"] = counter.qdisc_qlen;
  object["qdisc_backlog"] = counter.qdisc_backlog;
  object["qdisc_requeues"] = counter.qdisc_requeues;
  object["qdiscs"] = QdiscsJson(counter.qdiscs);
  return object;
}

void ResetNodePerfCounters(NodeRuntime& node) {
  node.process_perf_counters.reset();
  node.cgroup_perf_counters.reset();
  node.perf_counter_target_pid = -1;
  node.perf_counter_attached_pid = -1;
  node.perf_counter_cgroup_path.clear();
  node.perf_counter_cpus.clear();
  node.perf_counter_error_kind.reset();
  node.perf_counter_error.clear();
}

void SetNodePerfCounterError(NodeRuntime& node, PerfCounterErrorKind kind,
                             std::string_view error) {
  node.process_perf_counters.reset();
  node.cgroup_perf_counters.reset();
  node.perf_counter_attached_pid = -1;
  node.perf_counter_cgroup_path.clear();
  node.perf_counter_cpus.clear();
  node.perf_counter_error_kind = kind;
  node.perf_counter_error = error;
}

void AttachNodePerfCounters(NodeRuntime& node) {
  ResetNodePerfCounters(node);
  node.perf_counter_process_generation = node.RestartCount();
  if (node.process.pid() <= 0 || !node.process.running()) {
    SetNodePerfCounterError(node, PerfCounterErrorKind::kProcessUnavailable,
                            "node process is not running");
    return;
  }

  try {
    if (node.perf_counter_target_kind == PerfCounterTargetKind::kNode ||
        node.perf_counter_target_kind == PerfCounterTargetKind::kWallet) {
      const pid_t target_pid = node.process.pid();
      node.perf_counter_target_pid = target_pid;
      ProcessPerfCounters counters =
          ProcessPerfCounters::Open(target_pid, node.perf_counter_kinds);
      if (node.process.pid() != target_pid || !node.process.running()) {
        SetNodePerfCounterError(
            node, PerfCounterErrorKind::kProcessUnavailable,
            "node process exited or changed while perf counters were opening");
        return;
      }
      node.perf_counter_attached_pid = target_pid;
      node.process_perf_counters.emplace(std::move(counters));
      return;
    }

    if (!node.cgroup) {
      SetNodePerfCounterError(node, PerfCounterErrorKind::kProcessUnavailable,
                              "node cgroup is unavailable");
      return;
    }
    CgroupPerfCounters counters =
        CgroupPerfCounters::Open(node.cgroup->path(), node.perf_counter_kinds);
    if (!node.process.running() || !node.cgroup ||
        counters.cgroup_path() != node.cgroup->path()) {
      SetNodePerfCounterError(
          node, PerfCounterErrorKind::kProcessUnavailable,
          "node process or cgroup changed while perf counters were opening");
      return;
    }
    node.perf_counter_cgroup_path = counters.cgroup_path();
    node.perf_counter_cpus = counters.cpus();
    node.cgroup_perf_counters.emplace(std::move(counters));
  } catch (const PerfCounterError& error) {
    SetNodePerfCounterError(node, error.kind(), error.what());
    BBP_LOG(warning) << "perf counters unavailable for " << node.config.id
                     << " target="
                     << PerfCounterTargetKindName(node.perf_counter_target_kind)
                     << ":" << node.perf_counter_target_id << ": "
                     << error.what();
  }
}

boost::json::array PerfCounterNamesJson(
    const std::vector<PerfCounterKind>& kinds) {
  boost::json::array names;
  names.reserve(kinds.size());
  for (const PerfCounterKind kind : kinds) {
    names.emplace_back(PerfCounterKindName(kind));
  }
  return names;
}

boost::json::array PerfCounterValuesJson(
    const std::vector<PerfCounterValue>& values) {
  boost::json::array counters;
  counters.reserve(values.size());
  for (const PerfCounterValue& value : values) {
    boost::json::object counter;
    counter["name"] = PerfCounterKindName(value.kind);
    counter["raw_value"] = value.raw_value;
    if (value.scaled_value) {
      counter["scaled_value"] = *value.scaled_value;
    } else {
      counter["scaled_value"] = nullptr;
    }
    counter["time_enabled_ns"] = value.time_enabled_ns;
    counter["time_running_ns"] = value.time_running_ns;
    counter["multiplexed"] = value.multiplexed;
    counter["scaled"] = value.scaled;
    counter["scaled_overflow"] = value.scaled_overflow;
    counters.push_back(std::move(counter));
  }
  return counters;
}

struct NodeRuntimeMetrics {
  std::uint32_t node_index = 0;
  std::string chain;
  std::string role;
  std::string lifecycle;
  pid_t pid = -1;
  bool pidfd_available = false;
  bool process_running = false;
  std::optional<int> exit_status;
  std::optional<std::uint64_t> uptime_ms;
  std::string cgroup_path;
  std::string data_dir;
  std::string log_dir;
  std::string rpc_host;
  std::uint16_t rpc_port = 0;
  std::optional<std::uint64_t> network_namespace_inode;
  pid_t network_namespace_helper_pid = -1;
  std::string host_interface;
  std::string child_interface;
  std::string host_address;
  std::string node_address;
  std::uint8_t prefix_length = 0;
  std::vector<RouteInfo> routes;
  std::vector<PerfCounterKind> perf_counter_kinds;
  PerfCounterTargetKind perf_counter_target_kind = PerfCounterTargetKind::kNode;
  std::string perf_counter_target_id;
  pid_t perf_counter_target_pid = -1;
  pid_t perf_counter_attached_pid = -1;
  std::uint64_t perf_counter_process_generation = 0;
  std::string perf_counter_cgroup_path;
  std::vector<int> perf_counter_cpus;
  bool perf_counters_available = false;
  std::optional<PerfCounterErrorKind> perf_counter_error_kind;
  std::string perf_counter_error;
  std::vector<PerfCounterValue> perf_counter_values;
};

std::string MetricsJson(
    const std::string& run_id, const std::string& node_id,
    const NodeRuntimeMetrics& runtime, const ChainMetrics& chain,
    uint64_t generated_block_count, uint64_t mined_transaction_count,
    bool mined_transaction_count_complete, uint64_t restart_count,
    std::string_view resource_profile, std::string_view network_profile,
    const NetworkCondition* network_condition, const CgroupMetrics* cgroup,
    const LinkInfo* link, const QdiscInfo* qdisc,
    const std::vector<QdiscInfo>* qdisc_tree,
    const std::vector<TcFilterInfo>* filters,
    const DirectionalNetworkPolicyStats* directional_stats) {
  boost::json::object object;
  object["timestamp_ms"] = NowUnixMillis();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["node_index"] = runtime.node_index;
  object["chain"] = runtime.chain;
  object["role"] = runtime.role;
  object["lifecycle"] = runtime.lifecycle;
  object["pid"] = runtime.pid;
  object["pidfd_available"] = runtime.pidfd_available;
  object["process_group"] = runtime.pid;
  object["process_running"] = runtime.process_running;
  if (runtime.exit_status) {
    object["exit_status"] = *runtime.exit_status;
  } else {
    object["exit_status"] = nullptr;
  }
  if (runtime.uptime_ms) {
    object["uptime_ms"] = *runtime.uptime_ms;
  } else {
    object["uptime_ms"] = nullptr;
  }
  object["restart_count"] = restart_count;
  object["perf_counter_names"] =
      PerfCounterNamesJson(runtime.perf_counter_kinds);
  object["perf_counter_target_kind"] =
      PerfCounterTargetKindName(runtime.perf_counter_target_kind);
  object["perf_counter_target_id"] = runtime.perf_counter_target_id;
  if (runtime.perf_counter_target_pid > 0) {
    object["perf_counter_target_pid"] = runtime.perf_counter_target_pid;
  } else {
    object["perf_counter_target_pid"] = nullptr;
  }
  if (runtime.perf_counter_attached_pid > 0) {
    object["perf_counter_attached_pid"] = runtime.perf_counter_attached_pid;
  } else {
    object["perf_counter_attached_pid"] = nullptr;
  }
  object["perf_counter_process_generation"] =
      runtime.perf_counter_process_generation;
  if (runtime.perf_counter_cgroup_path.empty()) {
    object["perf_counter_cgroup_path"] = nullptr;
  } else {
    object["perf_counter_cgroup_path"] = runtime.perf_counter_cgroup_path;
  }
  boost::json::array perf_counter_cpus;
  perf_counter_cpus.reserve(runtime.perf_counter_cpus.size());
  for (const int cpu : runtime.perf_counter_cpus) {
    perf_counter_cpus.emplace_back(cpu);
  }
  object["perf_counter_cpus"] = std::move(perf_counter_cpus);
  object["perf_counters_available"] = runtime.perf_counters_available;
  if (runtime.perf_counter_error_kind) {
    object["perf_counter_error_kind"] =
        PerfCounterErrorKindName(*runtime.perf_counter_error_kind);
  } else {
    object["perf_counter_error_kind"] = nullptr;
  }
  if (runtime.perf_counter_error.empty()) {
    object["perf_counter_error"] = nullptr;
  } else {
    object["perf_counter_error"] = runtime.perf_counter_error;
  }
  object["perf_counters"] = PerfCounterValuesJson(runtime.perf_counter_values);
  object["cgroup_path"] = runtime.cgroup_path;
  object["data_dir"] = runtime.data_dir;
  object["log_dir"] = runtime.log_dir;
  object["rpc_host"] = runtime.rpc_host;
  object["rpc_port"] = runtime.rpc_port;
  if (runtime.network_namespace_inode) {
    object["network_namespace_inode"] = *runtime.network_namespace_inode;
  } else {
    object["network_namespace_inode"] = nullptr;
  }
  if (runtime.network_namespace_helper_pid > 0) {
    object["network_namespace_helper_pid"] =
        runtime.network_namespace_helper_pid;
  } else {
    object["network_namespace_helper_pid"] = nullptr;
  }
  if (runtime.host_interface.empty()) {
    object["host_interface"] = nullptr;
    object["child_interface"] = nullptr;
    object["host_address"] = nullptr;
    object["node_address"] = nullptr;
    object["network_prefix_length"] = nullptr;
    object["network_routes"] = boost::json::array{};
  } else {
    object["host_interface"] = runtime.host_interface;
    object["child_interface"] = runtime.child_interface;
    object["host_address"] = runtime.host_address;
    object["node_address"] = runtime.node_address;
    object["network_prefix_length"] = runtime.prefix_length;
    object["network_routes"] = RoutesJson(runtime.routes);
  }
  object["chain_version"] = chain.version;
  object["chain_protocol_version"] = chain.protocol_version;
  object["chain_subversion"] = chain.subversion;
  object["height"] = chain.height;
  object["best_hash"] = chain.best_hash;
  object["peer_count"] = chain.peer_count;
  boost::json::array peer_addresses;
  peer_addresses.reserve(chain.peer_addresses.size());
  for (const std::string& address : chain.peer_addresses) {
    peer_addresses.emplace_back(address);
  }
  object["peer_addresses"] = std::move(peer_addresses);
  object["mempool_tx_count"] = chain.mempool_tx_count;
  object["mempool_bytes"] = chain.mempool_bytes;
  object["generated_block_count"] = generated_block_count;
  object["mined_transaction_count"] = mined_transaction_count;
  object["mined_transaction_count_complete"] = mined_transaction_count_complete;
  if (resource_profile.empty()) {
    object["active_resource_profile"] = nullptr;
  } else {
    object["active_resource_profile"] = resource_profile;
  }
  if (network_profile.empty()) {
    object["active_network_profile"] = nullptr;
  } else {
    object["active_network_profile"] = network_profile;
  }
  if (network_condition == nullptr) {
    object["network_condition"] = nullptr;
  } else {
    object["network_condition"] = NetworkConditionJson(*network_condition);
  }
  if (chain.initial_block_download) {
    object["initial_block_download"] = *chain.initial_block_download;
  } else {
    object["initial_block_download"] = nullptr;
  }
  if (chain.headers) {
    object["headers"] = *chain.headers;
  } else {
    object["headers"] = nullptr;
  }
  object["sync_status"] = ChainSyncStatusName(chain.sync_status);
  if (chain.verification_progress) {
    object["verification_progress"] = *chain.verification_progress;
  } else {
    object["verification_progress"] = nullptr;
  }
  if (chain.difficulty) {
    object["difficulty"] = *chain.difficulty;
  } else {
    object["difficulty"] = nullptr;
  }
  if (chain.hashrate_estimate) {
    object["hashrate_estimate"] = *chain.hashrate_estimate;
  } else {
    object["hashrate_estimate"] = nullptr;
  }
  if (chain.last_block_time) {
    object["last_block_time"] = *chain.last_block_time;
  } else {
    object["last_block_time"] = nullptr;
  }
  if (chain.median_time) {
    object["median_time"] = *chain.median_time;
  } else {
    object["median_time"] = nullptr;
  }
  if (chain.chainwork) {
    object["chainwork"] = *chain.chainwork;
  } else {
    object["chainwork"] = nullptr;
  }
  if (chain.reorg_count) {
    object["reorg_count"] = *chain.reorg_count;
  } else {
    object["reorg_count"] = nullptr;
  }
  object["rpc_latency_ms"] = chain.rpc_latency_ms;
  if (cgroup != nullptr) {
    object["cpu_usage_usec"] = cgroup->cpu_usage_usec;
    object["cpu_throttled_usec"] = cgroup->cpu_throttled_usec;
    object["cpu_pressure_some_total_usec"] =
        cgroup->cpu_pressure_some_total_usec;
    object["cpu_pressure_full_total_usec"] =
        cgroup->cpu_pressure_full_total_usec;
    object["memory_current"] = cgroup->memory_current;
    object["memory_peak"] = cgroup->memory_peak;
    if (cgroup->memory_high_limit_bytes) {
      object["memory_high_limit_bytes"] = *cgroup->memory_high_limit_bytes;
    } else {
      object["memory_high_limit_bytes"] = nullptr;
    }
    if (cgroup->memory_max_limit_bytes) {
      object["memory_max_limit_bytes"] = *cgroup->memory_max_limit_bytes;
    } else {
      object["memory_max_limit_bytes"] = nullptr;
    }
    if (cgroup->cpu_quota_us) {
      object["cpu_quota_us"] = *cgroup->cpu_quota_us;
    } else {
      object["cpu_quota_us"] = nullptr;
    }
    object["cpu_period_us"] = cgroup->cpu_period_us;
    object["cpu_weight"] = cgroup->cpu_weight;
    object["io_weight"] = cgroup->io_weight;
    object["io_max"] = IoLimitsJson(cgroup->io_limits);
    object["io_read_bytes"] = cgroup->io_read_bytes;
    object["io_write_bytes"] = cgroup->io_write_bytes;
    object["io_read_operations"] = cgroup->io_read_operations;
    object["io_write_operations"] = cgroup->io_write_operations;
    object["io_discard_bytes"] = cgroup->io_discard_bytes;
    object["io_discard_operations"] = cgroup->io_discard_operations;
    object["io_pressure_some_total_usec"] = cgroup->io_pressure_some_total_usec;
    object["io_pressure_full_total_usec"] = cgroup->io_pressure_full_total_usec;
    object["pids_current"] = cgroup->pids_current;
    if (cgroup->pids_max_limit) {
      object["pids_max_limit"] = *cgroup->pids_max_limit;
    } else {
      object["pids_max_limit"] = nullptr;
    }
    object["pids_max_events"] = cgroup->pids_max_events;
    object["cgroup_populated"] = cgroup->cgroup_populated;
    object["cgroup_frozen"] = cgroup->cgroup_frozen;
    object["memory_low"] = cgroup->memory_low;
    object["memory_high"] = cgroup->memory_high;
    object["memory_max"] = cgroup->memory_max;
    object["oom"] = cgroup->oom;
    object["oom_kill"] = cgroup->oom_kill;
    object["oom_group_kill"] = cgroup->oom_group_kill;
    boost::json::object memory_stat;
    for (const auto& [name, value] : cgroup->memory_stat) {
      memory_stat[name] = value;
    }
    object["memory_stat"] = std::move(memory_stat);
  }
  if (link != nullptr) {
    object["network_has_stats"] = link->has_stats;
    object["network_rx_bytes"] = link->rx_bytes;
    object["network_tx_bytes"] = link->tx_bytes;
    object["network_rx_packets"] = link->rx_packets;
    object["network_tx_packets"] = link->tx_packets;
    object["network_rx_dropped"] = link->rx_dropped;
    object["network_tx_dropped"] = link->tx_dropped;
    object["network_rx_errors"] = link->rx_errors;
    object["network_tx_errors"] = link->tx_errors;
  }
  if (qdisc != nullptr) {
    object["qdisc_kind"] = qdisc->kernel_kind;
    object["qdisc_handle"] = qdisc->handle;
    object["qdisc_parent"] = qdisc->parent;
    object["qdisc_has_stats"] = qdisc->has_stats;
    object["qdisc_bytes"] = qdisc->bytes;
    object["qdisc_packets"] = qdisc->packets;
    object["qdisc_drops"] = qdisc->drops;
    object["qdisc_overlimits"] = qdisc->overlimits;
    object["qdisc_qlen"] = qdisc->qlen;
    object["qdisc_backlog"] = qdisc->backlog;
    object["qdisc_requeues"] = qdisc->requeues;
    object["qdisc_has_netem_options"] = qdisc->has_netem_options;
    object["qdisc_netem_latency_us"] = qdisc->netem_latency_us;
    object["qdisc_netem_jitter_us"] = qdisc->netem_jitter_us;
    object["qdisc_netem_loss"] = qdisc->netem_loss;
    object["qdisc_netem_duplicate"] = qdisc->netem_duplicate;
    object["qdisc_netem_corrupt"] = qdisc->netem_corrupt;
    object["qdisc_netem_reorder"] = qdisc->netem_reorder;
    object["qdisc_netem_limit_packets"] = qdisc->netem_limit_packets;
    object["qdisc_has_tbf_options"] = qdisc->has_tbf_options;
    object["qdisc_tbf_rate_bytes_per_sec"] = qdisc->tbf_rate_bytes_per_sec;
    object["qdisc_tbf_limit_bytes"] = qdisc->tbf_limit_bytes;
    object["qdisc_tbf_buffer_ticks"] = qdisc->tbf_buffer_ticks;
    object["qdisc_tbf_mtu_ticks"] = qdisc->tbf_mtu_ticks;
  }
  if (qdisc_tree != nullptr) {
    object["network_qdiscs"] = QdiscsJson(*qdisc_tree);
  } else {
    object["network_qdiscs"] = boost::json::array{};
  }
  if (filters != nullptr) {
    const TcFilterStatsSummary summary =
        SummarizeEgressIpv4TcpDropPolicies(*filters, link->name);
    object["network_filter_policy_count"] = summary.policy_count;
    object["network_filter_policies_with_stats"] = summary.policies_with_stats;
    object["network_filter_match_bytes"] = summary.match_bytes;
    object["network_filter_match_packets"] = summary.match_packets;
    object["network_filter_drop_packets"] = summary.drop_packets;
    boost::json::array policies;
    for (const TcFilterInfo& filter : *filters) {
      if (TcFilterIsEgressIpv4TcpDropPolicy(filter, link->name)) {
        policies.push_back(NetworkPolicyCounterJson(filter));
      }
    }
    object["network_policy_counters"] = policies;
    object["network_active_block_rules"] = std::move(policies);
  }
  if (directional_stats != nullptr) {
    object["directional_network_policy_count"] =
        directional_stats->policy_count;
    object["directional_network_policies_with_filter_stats"] =
        directional_stats->policies_with_filter_stats;
    object["directional_network_filter_match_bytes"] =
        directional_stats->filter_match_bytes;
    object["directional_network_filter_match_packets"] =
        directional_stats->filter_match_packets;
    object["directional_network_qdisc_count"] = directional_stats->qdisc_count;
    object["directional_network_qdiscs_with_stats"] =
        directional_stats->qdiscs_with_stats;
    object["directional_network_qdisc_bytes"] = directional_stats->qdisc_bytes;
    object["directional_network_qdisc_packets"] =
        directional_stats->qdisc_packets;
    object["directional_network_qdisc_drops"] = directional_stats->qdisc_drops;
    object["directional_network_qdisc_overlimits"] =
        directional_stats->qdisc_overlimits;
    object["directional_network_qdisc_qlen"] = directional_stats->qdisc_qlen;
    object["directional_network_qdisc_backlog"] =
        directional_stats->qdisc_backlog;
    object["directional_network_qdisc_requeues"] =
        directional_stats->qdisc_requeues;
    boost::json::array policies;
    policies.reserve(directional_stats->policies.size());
    for (const DirectionalNetworkPolicyCounter& counter :
         directional_stats->policies) {
      policies.push_back(DirectionalNetworkPolicyCounterJson(counter));
    }
    object["directional_network_policy_counters"] = std::move(policies);
  }
  return boost::json::serialize(object);
}

void WriteEvent(const std::filesystem::path& events_path,
                const std::string& run_id, const std::string& node_id,
                SimulationEventKind event_kind, std::string_view detail = "") {
  boost::json::object object;
  object["timestamp"] = NowIso8601();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["event"] = SimulationEventKindName(event_kind);
  object["detail"] = detail;
  AppendLine(events_path, boost::json::serialize(object));
}

void WriteNodeState(const std::filesystem::path& events_path,
                    const std::string& run_id, const std::string& node_id,
                    NodeRuntimeLifecycle state) {
  WriteEvent(events_path, run_id, node_id, SimulationEventKind::kState,
             NodeRuntimeLifecycleName(state));
}

std::string GeneratedBlocksDetail(
    uint32_t workload_index, uint32_t workload_count, uint32_t generator_node,
    uint64_t start_height, uint64_t target_height,
    const std::vector<std::string>& hashes, const std::string& reward_address,
    std::optional<std::uint64_t> operator_command_sequence = std::nullopt) {
  boost::json::array hash_array;
  for (const std::string& hash : hashes) {
    hash_array.emplace_back(hash);
  }
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["generator_node"] = generator_node;
  detail["count"] = static_cast<uint64_t>(hashes.size());
  detail["start_height"] = start_height;
  detail["target_height"] = target_height;
  detail["reward_address"] = reward_address;
  detail["hashes"] = std::move(hash_array);
  if (operator_command_sequence) {
    detail["operator_command_sequence"] = *operator_command_sequence;
  }
  return boost::json::serialize(detail);
}

std::string HeightWaitDetail(uint32_t workload_index, uint32_t workload_count,
                             uint32_t node, uint64_t target_height,
                             uint64_t observed_height) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["target_height"] = target_height;
  detail["observed_height"] = observed_height;
  return boost::json::serialize(detail);
}

std::string PeerCountWaitDetail(uint32_t workload_index,
                                uint32_t workload_count, uint32_t node,
                                uint64_t target_peer_count,
                                uint64_t observed_peer_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["target_peer_count"] = target_peer_count;
  detail["observed_peer_count"] = observed_peer_count;
  return boost::json::serialize(detail);
}

std::string PeerAddress(const Options& options,
                        const std::vector<NodeRuntime>& nodes, uint32_t node) {
  const uint32_t node_index = node - 1U;
  return PeerHost(options, node_index) + ":" +
         std::to_string(nodes[node_index].config.p2p_port);
}

std::string PeerChurnDetail(uint32_t workload_index, uint32_t workload_count,
                            uint32_t node, uint32_t peer,
                            const std::string& address,
                            uint64_t before_peer_count,
                            uint64_t after_peer_count, bool connected_before,
                            bool connected_after,
                            std::optional<uint32_t> timeout_sec) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["peer"] = peer;
  detail["address"] = address;
  detail["before_peer_count"] = before_peer_count;
  detail["after_peer_count"] = after_peer_count;
  detail["connected_before"] = connected_before;
  detail["connected_after"] = connected_after;
  if (timeout_sec) {
    detail["timeout_sec"] = *timeout_sec;
  }
  return boost::json::serialize(detail);
}

std::string RawTransactionDetail(uint32_t workload_index,
                                 uint32_t workload_count,
                                 const SendRawTransactionWorkload& workload,
                                 uint64_t start_height, uint64_t target_height,
                                 const std::vector<std::string>& funding_hashes,
                                 const ChainRawTransactionResult& transaction) {
  boost::json::object utxo;
  utxo["txid"] = transaction.utxo.txid;
  utxo["vout"] = transaction.utxo.vout;
  utxo["amount"] = transaction.utxo.amount;
  utxo["block_hash"] = transaction.utxo.block_hash;
  utxo["confirmations"] = transaction.utxo.confirmations;

  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["funding_node"] = workload.funding_node;
  detail["submit_node"] = workload.submit_node;
  detail["source_address"] = workload.source_address;
  detail["destination_address"] = workload.destination_address;
  detail["funding_blocks"] = workload.funding_blocks;
  detail["funding_hash_count"] = static_cast<uint64_t>(funding_hashes.size());
  detail["funding_start_height"] = start_height;
  detail["funding_target_height"] = target_height;
  detail["selected_utxo"] = std::move(utxo);
  detail["amount"] = transaction.destination_amount;
  detail["fee"] = transaction.fee;
  detail["change_amount"] = transaction.change_amount;
  detail["txid"] = transaction.txid;
  detail["mempool_size"] = transaction.mempool_size;
  detail["timeout_sec"] = workload.timeout_sec;
  return boost::json::serialize(detail);
}

boost::json::array TxIdsJson(const std::vector<std::string>& txids) {
  boost::json::array array;
  for (const std::string& txid : txids) {
    array.emplace_back(txid);
  }
  return array;
}

boost::json::array WalletIndexesJson(
    const std::vector<std::uint32_t>& wallets) {
  boost::json::array array;
  array.reserve(wallets.size());
  for (const std::uint32_t wallet : wallets) {
    array.emplace_back(wallet);
  }
  return array;
}

boost::json::object AmountDistributionDetail(
    const AmountDistribution& distribution) {
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min"] = FormatFixed8Amount(distribution.minimum_satoshis);
  object["max"] = FormatFixed8Amount(distribution.maximum_satoshis);
  object["min_satoshis"] = distribution.minimum_satoshis;
  object["max_satoshis"] = distribution.maximum_satoshis;
  return object;
}

boost::json::object IntervalDistributionDetail(
    const IntervalDistribution& distribution) {
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min_ms"] = distribution.minimum.count();
  object["max_ms"] = distribution.maximum.count();
  return object;
}

boost::json::value AmountDistributionConfigurationJson(
    const AmountDistribution& distribution) {
  if (distribution.kind == ValueDistributionKind::kFixed) {
    return boost::json::value(
        FormatFixed8Amount(distribution.minimum_satoshis));
  }
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min"] = FormatFixed8Amount(distribution.minimum_satoshis);
  object["max"] = FormatFixed8Amount(distribution.maximum_satoshis);
  return object;
}

boost::json::value IntervalDistributionConfigurationJson(
    const IntervalDistribution& distribution) {
  const auto duration_text = [](std::chrono::milliseconds duration) {
    return std::to_string(duration.count()) + "ms";
  };
  if (distribution.kind == ValueDistributionKind::kFixed) {
    return boost::json::value(duration_text(distribution.minimum));
  }
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min"] = duration_text(distribution.minimum);
  object["max"] = duration_text(distribution.maximum);
  return object;
}

std::string WalletFundingDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload, const WalletIdentity& wallet,
    uint32_t miner_node, uint64_t start_height, uint64_t target_height,
    uint64_t funding_hash_count, uint64_t ready_height,
    const ChainWalletFundingResult& preparation,
    uint64_t preparation_hash_count, uint64_t ready_balance_satoshis) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["wallet_index"] = wallet.wallet_index;
  detail["node"] = wallet.node;
  detail["address"] = wallet.address;
  detail["funding_address"] = wallet.funding_address;
  detail["miner_node"] = miner_node;
  detail["funding_strategy"] =
      std::string(WalletFundingStrategyName(workload.funding_strategy));
  detail["seed"] = workload.random_seed;
  detail["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  detail["funding_hash_count"] = funding_hash_count;
  detail["funding_start_height"] = start_height;
  detail["funding_target_height"] = target_height;
  detail["funding_ready_height"] = ready_height;
  detail["funding_preparation_txids"] = TxIdsJson(preparation.txids);
  detail["funding_preparation_confirmation_blocks"] =
      preparation.confirmation_blocks_required;
  detail["funding_preparation_minimum_chain_height"] =
      preparation.minimum_chain_height;
  detail["funding_preparation_hash_count"] = preparation_hash_count;
  detail["readiness_confirmations"] = workload.readiness_confirmations;
  detail["funding_threshold"] =
      FormatFixed8Amount(workload.funding_threshold_satoshis);
  detail["funding_threshold_satoshis"] = workload.funding_threshold_satoshis;
  detail["ready_balance"] = FormatFixed8Amount(ready_balance_satoshis);
  detail["ready_balance_satoshis"] = ready_balance_satoshis;
  detail["amount_distribution"] = AmountDistributionDetail(workload.amount);
  detail["interval_distribution"] =
      IntervalDistributionDetail(workload.interval);
  return boost::json::serialize(detail);
}

std::string WalletTransactionDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload, uint32_t transaction_index,
    const WalletIdentity& sender, const WalletIdentity& receiver,
    uint32_t funding_miner_node, uint64_t funding_start_height,
    uint64_t funding_target_height, uint64_t funding_hash_count,
    uint64_t funding_ready_height,
    const ChainWalletFundingResult& funding_preparation,
    uint64_t funding_preparation_hash_count,
    uint64_t funding_ready_balance_satoshis, uint64_t amount_satoshis,
    std::chrono::milliseconds interval_before,
    const ChainWalletTransactionResult& transaction) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["transaction_index"] = transaction_index;
  detail["transaction_count"] = workload.transaction_count;
  detail["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  detail["funding_strategy"] =
      std::string(WalletFundingStrategyName(workload.funding_strategy));
  detail["seed"] = workload.random_seed;
  detail["sender_wallet_index"] = sender.wallet_index;
  detail["receiver_wallet_index"] = receiver.wallet_index;
  detail["sender_node"] = sender.node;
  detail["receiver_node"] = receiver.node;
  detail["sender_address"] = sender.address;
  detail["receiver_address"] = receiver.address;
  detail["funding_miner_node"] = funding_miner_node;
  detail["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  detail["funding_hash_count"] = funding_hash_count;
  detail["funding_start_height"] = funding_start_height;
  detail["funding_target_height"] = funding_target_height;
  detail["funding_ready_height"] = funding_ready_height;
  detail["funding_preparation_txids"] = TxIdsJson(funding_preparation.txids);
  detail["funding_preparation_confirmation_blocks"] =
      funding_preparation.confirmation_blocks_required;
  detail["funding_preparation_minimum_chain_height"] =
      funding_preparation.minimum_chain_height;
  detail["funding_preparation_hash_count"] = funding_preparation_hash_count;
  detail["readiness_confirmations"] = workload.readiness_confirmations;
  detail["funding_threshold"] =
      FormatFixed8Amount(workload.funding_threshold_satoshis);
  detail["funding_threshold_satoshis"] = workload.funding_threshold_satoshis;
  detail["funding_ready_balance"] =
      FormatFixed8Amount(funding_ready_balance_satoshis);
  detail["funding_ready_balance_satoshis"] = funding_ready_balance_satoshis;
  detail["amount_distribution"] = AmountDistributionDetail(workload.amount);
  detail["interval_distribution"] =
      IntervalDistributionDetail(workload.interval);
  detail["interval_before_ms"] = interval_before.count();
  detail["amount"] = FormatFixed8Amount(amount_satoshis);
  detail["amount_satoshis"] = amount_satoshis;
  detail["requested_fee_rate"] = transaction.requested_fee_rate;
  detail["requested_fee_rate_satoshis"] = workload.fee_satoshis;
  detail["txids"] = TxIdsJson(transaction.txids);
  detail["mempool_size"] = transaction.mempool_size;
  detail["timeout_sec"] = workload.timeout_sec;
  return boost::json::serialize(detail);
}

struct TrackedTransaction {
  std::string txid;
  std::string submission_kind;
  std::uint32_t workload_index = 0;
  std::uint32_t workload_count = 0;
  std::uint32_t transaction_index = 0;
  std::uint32_t transaction_count = 0;
  std::uint32_t txid_index = 0;
  std::uint32_t submission_node = 0;
};

std::string TransactionObservationDetail(
    const TrackedTransaction& transaction, std::uint32_t node,
    const std::string& node_id,
    const ChainTransactionObservation& observation) {
  boost::json::object detail;
  detail["txid"] = transaction.txid;
  detail["submission_kind"] = transaction.submission_kind;
  detail["workload_index"] = transaction.workload_index;
  detail["workload_count"] = transaction.workload_count;
  detail["transaction_index"] = transaction.transaction_index;
  detail["transaction_count"] = transaction.transaction_count;
  detail["txid_index"] = transaction.txid_index;
  detail["submission_node"] = transaction.submission_node;
  detail["node"] = node;
  detail["node_id"] = node_id;
  detail["state"] = ChainTransactionStateName(observation.state);
  detail["observed_height"] = observation.observed_height;
  detail["mempool_size"] = observation.mempool_size;
  if (observation.block_hash.empty()) {
    detail["block_hash"] = nullptr;
  } else {
    detail["block_hash"] = observation.block_hash;
  }
  if (observation.confirmation_height) {
    detail["confirmation_height"] = *observation.confirmation_height;
  } else {
    detail["confirmation_height"] = nullptr;
  }
  detail["confirmations"] = observation.confirmations;
  return boost::json::serialize(detail);
}

class TransactionObservationTracker {
 public:
  void TrackAndWaitForVisibility(const Options& options,
                                 const std::filesystem::path& events_path,
                                 const ChainDriver& driver,
                                 const std::vector<NodeRuntime>& nodes,
                                 TrackedTransaction transaction,
                                 std::chrono::seconds timeout,
                                 std::stop_token stop_token) {
    if (transaction.txid.empty()) {
      throw std::runtime_error("cannot track an empty transaction id");
    }
    if (!tracked_txids_.insert(transaction.txid).second) {
      throw std::runtime_error("duplicate submitted transaction id: " +
                               transaction.txid);
    }
    transactions_.push_back(std::move(transaction));
    const TrackedTransaction& tracked = transactions_.back();
    for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
      const NodeRuntime& node = nodes[node_index];
      const ChainTransactionObservation observation = driver.WaitForTransaction(
          node.config, tracked.txid, timeout, stop_token);
      RecordObservation(options, events_path, tracked,
                        static_cast<std::uint32_t>(node_index + 1U),
                        node.config.id, observation);
    }
  }

  void ObserveAll(const Options& options,
                  const std::filesystem::path& events_path,
                  const ChainDriver& driver,
                  const std::vector<NodeRuntime>& nodes,
                  std::stop_token stop_token) {
    for (const TrackedTransaction& transaction : transactions_) {
      for (std::size_t node_index = 0; node_index < nodes.size();
           ++node_index) {
        const NodeRuntime& node = nodes[node_index];
        const ChainTransactionObservation observation =
            driver.ObserveTransaction(node.config, transaction.txid,
                                      stop_token);
        RecordObservation(options, events_path, transaction,
                          static_cast<std::uint32_t>(node_index + 1U),
                          node.config.id, observation);
      }
    }
  }

 private:
  void RecordObservation(const Options& options,
                         const std::filesystem::path& events_path,
                         const TrackedTransaction& transaction,
                         std::uint32_t node, const std::string& node_id,
                         const ChainTransactionObservation& observation) {
    if (observation.state == ChainTransactionState::kUnknown) {
      return;
    }
    const std::pair<std::string, std::string> key{transaction.txid, node_id};
    const std::string detail =
        TransactionObservationDetail(transaction, node, node_id, observation);
    if (visible_.insert(key).second) {
      WriteEvent(events_path, options.run_id, node_id,
                 SimulationEventKind::kTransactionVisible, detail);
    }
    if (observation.state == ChainTransactionState::kConfirmed &&
        confirmed_.insert(key).second) {
      if (!observation.confirmation_height || observation.block_hash.empty() ||
          observation.confirmations == 0U) {
        throw std::runtime_error(
            "confirmed transaction observation is missing block metadata");
      }
      WriteEvent(events_path, options.run_id, node_id,
                 SimulationEventKind::kTransactionConfirmed, detail);
    }
  }

  std::vector<TrackedTransaction> transactions_;
  std::set<std::string> tracked_txids_;
  std::set<std::pair<std::string, std::string>> visible_;
  std::set<std::pair<std::string, std::string>> confirmed_;
};

void RecordGeneratedBlocks(const ChainDriver& driver, NodeRuntime& node,
                           const std::vector<std::string>& block_hashes,
                           std::stop_token stop_token) {
  node.AddGeneratedBlocks(static_cast<std::uint64_t>(block_hashes.size()));
  std::uint64_t mined_transaction_count = 0;
  for (const std::string& block_hash : block_hashes) {
    std::uint64_t block_transaction_count = 0;
    try {
      block_transaction_count = driver.ReadBlockNonRewardTransactionCount(
          node.config, block_hash, stop_token);
    } catch (const SimulationCancelled&) {
      throw;
    } catch (const std::exception& error) {
      node.MarkMinedTransactionCountIncomplete();
      BBP_LOG(warning) << "could not count transactions in generated block "
                       << block_hash << " for " << node.config.id << ": "
                       << error.what();
      continue;
    }
    if (mined_transaction_count >
        std::numeric_limits<std::uint64_t>::max() - block_transaction_count) {
      throw std::runtime_error("mined transaction count overflow");
    }
    mined_transaction_count += block_transaction_count;
  }
  node.AddMinedTransactions(mined_transaction_count);
}

std::string WalletAddressDetail(const WalletIdentity& wallet,
                                const WalletInitialization& initialization) {
  boost::json::object detail;
  detail["wallet_index"] = wallet.wallet_index;
  detail["node"] = wallet.node;
  detail["strategy"] =
      std::string(WalletInitializationStrategyName(initialization.strategy));
  detail["mode"] = std::string(WalletPrivacyModeName(initialization.mode));
  if (!wallet.address.empty()) {
    detail["address"] = wallet.address;
  }
  if (!wallet.funding_address.empty()) {
    detail["funding_address"] = wallet.funding_address;
  }
  return boost::json::serialize(detail);
}

std::string ProcessExitDetail(const ChildProcess& process) {
  const bool running = process.running();
  boost::json::object detail;
  detail["running"] = running;
  detail["pid"] = process.pid();
  const std::optional<int> status = process.exit_status();
  if (!status) {
    return boost::json::serialize(detail);
  }
  detail["raw_status"] = *status;
  if (WIFEXITED(*status)) {
    detail["kind"] = "exit";
    detail["exit_code"] = WEXITSTATUS(*status);
  } else if (WIFSIGNALED(*status)) {
    detail["kind"] = "signal";
    detail["signal"] = WTERMSIG(*status);
  } else {
    detail["kind"] = "other";
  }
  return boost::json::serialize(detail);
}

std::string RestartNodeWorkloadDetail(uint32_t workload_index,
                                      uint32_t workload_count, uint32_t node,
                                      uint64_t restart_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

std::string FreezeNodeWorkloadDetail(uint32_t workload_index,
                                     uint32_t workload_count, uint32_t node,
                                     uint32_t duration_ms) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["duration_ms"] = duration_ms;
  return boost::json::serialize(detail);
}

std::string CheckpointWorkloadDetail(std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     std::string_view name,
                                     std::uint32_t node_metric_samples,
                                     std::uint32_t wallet_metric_samples) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["name"] = name;
  detail["node_metric_samples"] = node_metric_samples;
  detail["wallet_metric_samples"] = wallet_metric_samples;
  detail["total_metric_samples"] =
      static_cast<std::uint64_t>(node_metric_samples) +
      static_cast<std::uint64_t>(wallet_metric_samples);
  return boost::json::serialize(detail);
}

using NodeMetricsFailureHandler =
    std::function<void(const NodeRuntime&, std::string_view)>;
using MetricsStopRequested = std::function<bool()>;

std::uint32_t WriteMetricsSnapshot(
    const std::filesystem::path& metrics_path, const Options& options,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    std::mutex& node_process_mutex,
    const NodeMetricsFailureHandler& node_failure_handler = {},
    const MetricsStopRequested& stop_requested = {},
    std::stop_token stop_token = {}) {
  std::uint32_t sample_count = 0U;
  struct NetworkMetricsState {
    std::optional<NodeVethConfig> network;
    std::string profile;
  };
  const std::vector<LinkInfo> links = ListNetworkLinks();
  std::vector<QdiscInfo> qdiscs;
  std::vector<NetworkMetricsState> network_states(nodes.size());
  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    const bool has_isolated_node = std::any_of(
        nodes.begin(), nodes.end(),
        [](const NodeRuntime& node) { return node.network.has_value(); });
    if (has_isolated_node) {
      qdiscs = ListQdiscs();
    }
    for (std::size_t index = 0; index < nodes.size(); ++index) {
      network_states[index].network = nodes[index].network;
      network_states[index].profile = nodes[index].network_profile;
    }
  }
  for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
    NodeRuntime& node = nodes[node_index];
    if (stop_requested && stop_requested()) {
      return sample_count;
    }
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    ChainMetrics chain;
    try {
      chain = driver.ReadMetrics(node.config, stop_token);
    } catch (const std::exception& error) {
      if (stop_token.stop_requested() || (stop_requested && stop_requested())) {
        return sample_count;
      }
      if (!node.AllowsChainMetrics()) {
        continue;
      }
      if (!node_failure_handler) {
        throw;
      }
      node_failure_handler(node, error.what());
      continue;
    }
    NodeRuntimeMetrics runtime;
    runtime.node_index = static_cast<std::uint32_t>(node_index + 1U);
    runtime.chain = std::string(ChainKindName(options.chain));
    runtime.role =
        NodeRoleName(options, static_cast<std::uint32_t>(node_index));
    runtime.lifecycle = NodeRuntimeLifecycleName(node.Lifecycle());
    runtime.data_dir = node.config.data_dir.string();
    runtime.log_dir = node.config.log_dir.string();
    const RpcEndpoint rpc_endpoint = driver.Endpoint(node.config);
    runtime.rpc_host = rpc_endpoint.host;
    runtime.rpc_port = rpc_endpoint.port;
    {
      std::lock_guard<std::mutex> lock(node_process_mutex);
      runtime.pid = node.process.pid();
      runtime.pidfd_available = node.process.pidfd() >= 0;
      runtime.process_running = node.process.running();
      runtime.exit_status = node.process.exit_status();
      runtime.perf_counter_kinds = node.perf_counter_kinds;
      runtime.perf_counter_target_kind = node.perf_counter_target_kind;
      runtime.perf_counter_target_id = node.perf_counter_target_id;
      runtime.perf_counter_target_pid = node.perf_counter_target_pid;
      runtime.perf_counter_attached_pid = node.perf_counter_attached_pid;
      runtime.perf_counter_process_generation =
          node.perf_counter_process_generation;
      runtime.perf_counter_cgroup_path = node.perf_counter_cgroup_path.string();
      runtime.perf_counter_cpus = node.perf_counter_cpus;
      runtime.perf_counter_error_kind = node.perf_counter_error_kind;
      runtime.perf_counter_error = node.perf_counter_error;
      if (node.process_perf_counters) {
        try {
          runtime.perf_counter_values = node.process_perf_counters->Read();
          runtime.perf_counters_available = true;
          runtime.perf_counter_error_kind.reset();
          runtime.perf_counter_error.clear();
        } catch (const PerfCounterError& error) {
          runtime.perf_counter_error_kind = error.kind();
          runtime.perf_counter_error = error.what();
        }
      } else if (node.cgroup_perf_counters) {
        try {
          runtime.perf_counter_values = node.cgroup_perf_counters->Read();
          runtime.perf_counters_available = true;
          runtime.perf_counter_error_kind.reset();
          runtime.perf_counter_error.clear();
        } catch (const PerfCounterError& error) {
          runtime.perf_counter_error_kind = error.kind();
          runtime.perf_counter_error = error.what();
        }
      }
      if (node.process_started_at) {
        const auto now = std::chrono::steady_clock::now();
        runtime.uptime_ms =
            now <= *node.process_started_at
                ? 0U
                : static_cast<std::uint64_t>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - *node.process_started_at)
                          .count());
      }
    }
    if (node.cgroup) {
      runtime.cgroup_path = node.cgroup->path().string();
    }
    if (node.network_namespace) {
      struct stat namespace_stat {};
      if (fstat(node.network_namespace->fd(), &namespace_stat) != 0) {
        throw std::runtime_error("fstat node network namespace failed");
      }
      runtime.network_namespace_inode = namespace_stat.st_ino;
      runtime.network_namespace_helper_pid =
          node.network_namespace->helper_pid();
    }
    CgroupMetrics cg;
    std::string resource_profile;
    {
      std::lock_guard<std::mutex> lock(node_resource_state_mutex);
      cg = node.cgroup->ReadMetrics();
      resource_profile = node.resource_profile;
    }
    const std::optional<NodeVethConfig>& network =
        network_states[node_index].network;
    if (network) {
      runtime.host_interface = network->host_name;
      runtime.child_interface = network->peer_name;
      runtime.host_address = network->host_address;
      runtime.node_address = network->node_address;
      runtime.prefix_length = network->prefix_len;
      if (node.network_namespace) {
        runtime.routes =
            ListIpv4RoutesInNamespace(node.network_namespace->fd());
      }
    }
    std::optional<DirectionalNetworkPolicyStats> directional_stats;
    if (network && node.network_namespace) {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      directional_stats = ReadDirectionalNetworkPolicyStatsInNamespace(
          node.network_namespace->fd(), network->peer_name,
          node.directional_network_policies);
    }
    const LinkInfo* link =
        network ? FindLinkByName(links, network->host_name) : nullptr;
    std::optional<QdiscInfo> qdisc_summary;
    const QdiscInfo* qdisc = nullptr;
    std::vector<QdiscInfo> qdisc_tree;
    std::vector<TcFilterInfo> filters;
    const std::vector<TcFilterInfo>* filter_metrics = nullptr;
    if (network) {
      for (const QdiscInfo& candidate : qdiscs) {
        if (candidate.if_name == network->host_name) {
          qdisc_tree.push_back(candidate);
        }
      }
      if (network->apply_condition) {
        QdiscInfo candidate;
        if (QdiscsMatchNetworkCondition(qdiscs, network->host_name,
                                        network->condition, &candidate)) {
          qdisc_summary = candidate;
          qdisc = &*qdisc_summary;
        }
      }
      if (qdisc == nullptr) {
        qdisc = FindQdiscByInterfaceName(qdiscs, network->host_name);
      }
      if (link != nullptr) {
        std::lock_guard<std::mutex> lock(node_network_state_mutex);
        filters = ListTcFiltersForInterface(network->host_name);
        filter_metrics = &filters;
      }
    }
    AppendLine(
        metrics_path,
        MetricsJson(
            options.run_id, node.config.id, runtime, chain,
            node.GeneratedBlockCount(), node.MinedTransactionCount(),
            node.MinedTransactionCountComplete(), node.RestartCount(),
            resource_profile, network_states[node_index].profile,
            network && network->apply_condition ? &network->condition : nullptr,
            &cg, link, qdisc, network ? &qdisc_tree : nullptr, filter_metrics,
            directional_stats ? &*directional_stats : nullptr));
    ++sample_count;
  }
  return sample_count;
}

boost::json::object WalletTransactionJson(
    const ChainWalletTransaction& transaction) {
  boost::json::object object;
  object["direction"] =
      ChainWalletTransactionDirectionName(transaction.direction);
  object["amount_satoshis"] = transaction.amount_satoshis;
  object["confirmations"] = transaction.confirmations;
  object["timestamp"] = transaction.timestamp;
  if (!transaction.txid.empty()) {
    object["txid"] = transaction.txid;
  }
  if (!transaction.address.empty()) {
    object["address"] = transaction.address;
  }
  if (transaction.fee_satoshis) {
    object["fee_satoshis"] = *transaction.fee_satoshis;
  }
  if (!transaction.block_hash.empty()) {
    object["block_hash"] = transaction.block_hash;
  }
  if (transaction.abandoned) {
    object["abandoned"] = *transaction.abandoned;
  }
  return object;
}

std::string WalletMetricsJson(const Options& options,
                              std::uint32_t wallet_index,
                              std::uint32_t one_based_node,
                              const ChainWalletSnapshot& snapshot) {
  boost::json::object object;
  object["run_id"] = options.run_id;
  object["timestamp_ms"] = NowUnixMillis();
  object["wallet_index"] = wallet_index;
  object["node"] = one_based_node;
  object["mode"] =
      std::string(WalletPrivacyModeName(options.wallet_initialization.mode));
  object["available_balance_satoshis"] = snapshot.available_balance_satoshis;
  object["unconfirmed_balance_satoshis"] =
      snapshot.unconfirmed_balance_satoshis;
  object["immature_balance_satoshis"] = snapshot.immature_balance_satoshis;
  object["transaction_count"] = snapshot.transaction_count;
  object["transaction_history_truncated"] =
      snapshot.transaction_history_truncated;
  boost::json::array transactions;
  transactions.reserve(snapshot.transactions.size());
  for (const ChainWalletTransaction& transaction : snapshot.transactions) {
    transactions.push_back(WalletTransactionJson(transaction));
  }
  object["transactions"] = std::move(transactions);
  return boost::json::serialize(object);
}

using WalletMetricsFailureHandler =
    std::function<void(std::uint32_t wallet_index, const NodeRuntime& node,
                       std::string_view error)>;

std::uint32_t WriteWalletMetricsSnapshot(
    const std::filesystem::path& metrics_path, const Options& options,
    const ChainDriver& driver, const std::vector<NodeRuntime>& nodes,
    const WalletMetricsFailureHandler& failure_handler = {},
    std::stop_token stop_token = {}) {
  if (!options.wallet_backed_workload_requested) {
    return 0U;
  }
  std::uint32_t sample_count = 0U;
  constexpr std::uint32_t kTransactionLimit = 256U;
  for (std::size_t index = 0; index < options.topology.wallet_nodes.size();
       ++index) {
    const std::uint32_t node_index = options.topology.wallet_nodes[index];
    if (node_index >= nodes.size()) {
      throw std::runtime_error("wallet metrics node is out of range");
    }
    const NodeRuntime& node = nodes[node_index];
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    try {
      const ChainWalletSnapshot snapshot = driver.ReadWalletSnapshot(
          node.config, ToChainWalletMode(options.wallet_initialization),
          kTransactionLimit, stop_token);
      AppendLine(
          metrics_path,
          WalletMetricsJson(options, static_cast<std::uint32_t>(index + 1U),
                            node_index + 1U, snapshot));
      ++sample_count;
    } catch (const std::exception& error) {
      if (stop_token.stop_requested()) {
        return sample_count;
      }
      if (!node.AllowsChainMetrics()) {
        continue;
      }
      if (!failure_handler) {
        throw;
      }
      failure_handler(static_cast<std::uint32_t>(index + 1U), node,
                      error.what());
    }
  }
  return sample_count;
}

std::string LogTailDetail(std::string_view kind, const LogTailChunk& chunk) {
  boost::json::object detail;
  detail["kind"] = kind;
  detail["start_offset"] = chunk.start_offset;
  detail["next_offset"] = chunk.next_offset;
  detail["truncated"] = chunk.truncated;
  detail["offset_reset"] = chunk.offset_reset;
  detail["text"] = chunk.text;
  return boost::json::serialize(detail);
}

SimulationEventKind LogTailEventKind(ChainLogSource source) {
  switch (source) {
    case ChainLogSource::kDaemon:
      return SimulationEventKind::kDaemonLogTail;
    case ChainLogSource::kStdout:
      return SimulationEventKind::kStdoutTail;
    case ChainLogSource::kStderr:
      return SimulationEventKind::kStderrTail;
  }
  throw std::runtime_error("unknown chain log source");
}

void WriteLogTailChunkEvent(const std::filesystem::path& events_path,
                            const Options& options,
                            const ChainNodeConfig& config,
                            ChainLogSource source, const LogTailChunk& chunk) {
  const std::string_view kind = ChainLogSourceName(source);
  WriteEvent(events_path, options.run_id, config.id, LogTailEventKind(source),
             LogTailDetail(kind, chunk));
}

void WriteLogTailEvent(const std::filesystem::path& events_path,
                       const Options& options, const ChainDriver& driver,
                       const NodeRuntime& node, ChainLogSource source,
                       LogTailCursor* cursor) {
  std::optional<LogTailChunk> chunk;
  try {
    chunk = driver.ReadLogTail(node.config, source, *cursor, kMaxLogTailBytes);
  } catch (const std::exception& error) {
    BBP_LOG(warning) << "cannot read " << ChainLogSourceName(source) << " for "
                     << node.config.id << ": " << error.what();
    return;
  }
  if (!chunk) {
    return;
  }
  *cursor = chunk->next_cursor;
  if (chunk->text.empty() && !chunk->truncated && !chunk->offset_reset) {
    return;
  }
  WriteLogTailChunkEvent(events_path, options, node.config, source, *chunk);
}

void WriteNodeLogTail(const std::filesystem::path& events_path,
                      const Options& options, const ChainDriver& driver,
                      NodeRuntime& node) {
  WriteLogTailEvent(events_path, options, driver, node, ChainLogSource::kStdout,
                    &node.stdout_log_cursor);
  WriteLogTailEvent(events_path, options, driver, node, ChainLogSource::kStderr,
                    &node.stderr_log_cursor);
  WriteLogTailEvent(events_path, options, driver, node, ChainLogSource::kDaemon,
                    &node.daemon_log_cursor);
}

void WriteNodeLogTails(const std::filesystem::path& events_path,
                       const Options& options, const ChainDriver& driver,
                       std::vector<NodeRuntime>& nodes) {
  for (NodeRuntime& node : nodes) {
    WriteNodeLogTail(events_path, options, driver, node);
  }
}

void EmitYamlScalar(YamlEmitter* emitter, std::string_view value,
                    yaml_scalar_style_t style) {
  if (value.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    throw std::runtime_error("YAML scalar is too large");
  }
  yaml_event_t event;
  yaml_scalar_event_initialize(
      &event, nullptr, nullptr,
      const_cast<yaml_char_t*>(
          reinterpret_cast<const yaml_char_t*>(value.data())),
      static_cast<int>(value.size()), 1, 1, style);
  emitter->Emit(&event);
}

void EmitYamlJsonValue(YamlEmitter* emitter, const boost::json::value& value);

void EmitYamlJsonObject(YamlEmitter* emitter,
                        const boost::json::object& object) {
  yaml_event_t event;
  yaml_mapping_start_event_initialize(&event, nullptr, nullptr, 1,
                                      YAML_BLOCK_MAPPING_STYLE);
  emitter->Emit(&event);
  for (const auto& item : object) {
    EmitYamlScalar(emitter, item.key(), YAML_PLAIN_SCALAR_STYLE);
    EmitYamlJsonValue(emitter, item.value());
  }
  yaml_mapping_end_event_initialize(&event);
  emitter->Emit(&event);
}

void EmitYamlJsonArray(YamlEmitter* emitter, const boost::json::array& array) {
  yaml_event_t event;
  yaml_sequence_start_event_initialize(&event, nullptr, nullptr, 1,
                                       YAML_BLOCK_SEQUENCE_STYLE);
  emitter->Emit(&event);
  for (const boost::json::value& value : array) {
    EmitYamlJsonValue(emitter, value);
  }
  yaml_sequence_end_event_initialize(&event);
  emitter->Emit(&event);
}

void EmitYamlJsonValue(YamlEmitter* emitter, const boost::json::value& value) {
  if (value.is_object()) {
    EmitYamlJsonObject(emitter, value.as_object());
  } else if (value.is_array()) {
    EmitYamlJsonArray(emitter, value.as_array());
  } else if (value.is_string()) {
    EmitYamlScalar(emitter, std::string_view(value.as_string()),
                   YAML_DOUBLE_QUOTED_SCALAR_STYLE);
  } else if (value.is_bool()) {
    EmitYamlScalar(emitter, value.as_bool() ? "true" : "false",
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_int64()) {
    EmitYamlScalar(emitter, std::to_string(value.as_int64()),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_uint64()) {
    EmitYamlScalar(emitter, std::to_string(value.as_uint64()),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_double()) {
    EmitYamlScalar(emitter, boost::json::serialize(value),
                   YAML_PLAIN_SCALAR_STYLE);
  } else if (value.is_null()) {
    EmitYamlScalar(emitter, "null", YAML_PLAIN_SCALAR_STYLE);
  }
}

std::string YamlFromJson(const boost::json::value& value) {
  YamlEmitter emitter;
  yaml_event_t event;
  yaml_stream_start_event_initialize(&event, YAML_UTF8_ENCODING);
  emitter.Emit(&event);
  yaml_document_start_event_initialize(&event, nullptr, nullptr, nullptr, 0);
  emitter.Emit(&event);
  EmitYamlJsonValue(&emitter, value);
  yaml_document_end_event_initialize(&event, 0);
  emitter.Emit(&event);
  yaml_stream_end_event_initialize(&event);
  emitter.Emit(&event);
  return emitter.Output();
}

std::vector<ScenarioWorkload> EffectiveWorkloads(const Options& options) {
  return options.workloads;
}

std::optional<uint32_t> CommonBlockGenerationNode(
    const std::vector<ScenarioWorkload>& workloads) {
  std::optional<uint32_t> node;
  for (const ScenarioWorkload& workload : workloads) {
    if (workload.kind != WorkloadKind::kBlockGeneration) {
      continue;
    }
    if (!node) {
      node = workload.block_generation.node;
    } else if (*node != workload.block_generation.node) {
      return std::nullopt;
    }
  }
  return node;
}

std::optional<uint32_t> CommonBlockGenerationSyncTimeout(
    const std::vector<ScenarioWorkload>& workloads) {
  std::optional<uint32_t> sync_timeout_sec;
  for (const ScenarioWorkload& workload : workloads) {
    if (workload.kind != WorkloadKind::kBlockGeneration) {
      continue;
    }
    if (!sync_timeout_sec) {
      sync_timeout_sec = workload.block_generation.sync_timeout_sec;
    } else if (*sync_timeout_sec !=
               workload.block_generation.sync_timeout_sec) {
      return std::nullopt;
    }
  }
  return sync_timeout_sec;
}

boost::json::object BlockGenerationWorkloadJson(
    const BlockGenerationWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kBlockGeneration));
  object["node"] = workload.node;
  object["count"] = workload.count;
  object["sync_timeout_sec"] = workload.sync_timeout_sec;
  return object;
}

boost::json::object WaitUntilHeightWorkloadJson(
    const WaitUntilHeightWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kWaitUntilHeight));
  object["node"] = workload.node;
  object["height"] = workload.height;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WaitForPeersWorkloadJson(
    const WaitForPeersWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kWaitForPeers));
  object["node"] = workload.node;
  object["peer_count"] = workload.peer_count;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object ConnectPeerWorkloadJson(
    const ConnectPeerWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kConnectPeer));
  object["node"] = workload.node;
  object["peer"] = workload.peer;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object DisconnectPeerWorkloadJson(
    const DisconnectPeerWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kDisconnectPeer));
  object["node"] = workload.node;
  object["peer"] = workload.peer;
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object RestartNodeWorkloadJson(
    const RestartNodeWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kRestartNode));
  object["node"] = workload.node;
  return object;
}

boost::json::object FreezeNodeWorkloadJson(const FreezeNodeWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kFreezeNode));
  object["node"] = workload.node;
  object["duration_ms"] = workload.duration_ms;
  return object;
}

boost::json::object ResourceLimitUpdateWorkloadJson(
    const ResourceLimitUpdateWorkload& workload) {
  boost::json::object object = ResourceLimitPatchJson(workload.patch);
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kUpdateResourceLimits));
  object["node"] = workload.node;
  return object;
}

boost::json::object ProfileSwitchWorkloadJson(
    const ProfileSwitchWorkload& workload, WorkloadKind kind) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(kind));
  boost::json::array nodes;
  for (const std::string& node_id : workload.node_ids) {
    nodes.emplace_back(node_id);
  }
  object["nodes"] = std::move(nodes);
  object["profile"] = workload.profile;
  return object;
}

boost::json::object ResourcePressureWorkloadJson(
    const ResourcePressureWorkload& workload) {
  boost::json::object object = ResourceLimitPatchJson(workload.patch);
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kResourcePressure));
  object["node"] = workload.node;
  object["duration_ms"] = workload.duration_ms;
  return object;
}

boost::json::object NetworkPartitionWorkloadJson(
    const NetworkPartitionWorkload& workload, WorkloadKind kind) {
  boost::json::object object = NetworkPartitionRuleJson(workload.partition);
  object["type"] = std::string(WorkloadKindName(kind));
  return object;
}

boost::json::object NetworkConditionWorkloadJson(
    const NetworkConditionWorkload& workload) {
  boost::json::object object = NetworkConditionJson(workload.condition);
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kSetNetworkCondition));
  object["node"] = workload.node;
  return object;
}

boost::json::object NetworkBlockWorkloadJson(
    const NetworkBlockWorkload& workload, WorkloadKind kind) {
  boost::json::object object = NetworkBlockRuleJson(workload.rule);
  object["type"] = std::string(WorkloadKindName(kind));
  return object;
}

boost::json::object TopologyEdgeWorkloadJson(
    const TopologyEdgeWorkload& workload, WorkloadKind kind) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(kind));
  object["from"] = workload.from;
  object["to"] = workload.to;
  if (kind == WorkloadKind::kSetEdgeCondition) {
    if (!workload.condition) {
      throw std::runtime_error(
          "set_edge_condition workload is missing its condition");
    }
    AddNetworkConditionJsonFields(*workload.condition, &object);
  } else {
    object["timeout_sec"] = workload.timeout_sec;
  }
  return object;
}

boost::json::object SendRawTransactionWorkloadJson(
    const SendRawTransactionWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kSendRawTransaction));
  object["funding_node"] = workload.funding_node;
  object["submit_node"] = workload.submit_node;
  object["source_address"] = workload.source_address;
  object["source_private_key"] = workload.source_private_key;
  object["destination_address"] = workload.destination_address;
  object["funding_blocks"] = workload.funding_blocks;
  object["amount"] = FormatFixed8Amount(workload.amount_satoshis);
  object["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object WalletTransactionsWorkloadJson(
    const WalletTransactionsWorkload& workload) {
  boost::json::object object;
  object["type"] =
      std::string(WorkloadKindName(WorkloadKind::kWalletTransactions));
  object["funding_strategy"] =
      std::string(WalletFundingStrategyName(workload.funding_strategy));
  object["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  object["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  object["readiness_confirmations"] = workload.readiness_confirmations;
  object["funding_threshold"] =
      FormatFixed8Amount(workload.funding_threshold_satoshis);
  object["transaction_count"] = workload.transaction_count;
  object["amount"] = AmountDistributionConfigurationJson(workload.amount);
  if (workload.interval.kind != ValueDistributionKind::kFixed ||
      workload.interval.minimum != std::chrono::milliseconds(0)) {
    object["interval"] =
        IntervalDistributionConfigurationJson(workload.interval);
  }
  object["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  object["seed"] = workload.random_seed;
  if (!workload.sender_wallets.empty()) {
    object["sender_wallets"] = WalletIndexesJson(workload.sender_wallets);
  }
  if (!workload.receiver_wallets.empty()) {
    object["receiver_wallets"] = WalletIndexesJson(workload.receiver_wallets);
  }
  object["timeout_sec"] = workload.timeout_sec;
  return object;
}

boost::json::object CheckpointWorkloadJson(const CheckpointWorkload& workload) {
  boost::json::object object;
  object["type"] = std::string(WorkloadKindName(WorkloadKind::kCheckpoint));
  if (!workload.name.empty()) {
    object["name"] = workload.name;
  }
  return object;
}

boost::json::object WorkloadJson(const ScenarioWorkload& workload) {
  if (workload.kind == WorkloadKind::kBlockGeneration) {
    return BlockGenerationWorkloadJson(workload.block_generation);
  }
  if (workload.kind == WorkloadKind::kWaitUntilHeight) {
    return WaitUntilHeightWorkloadJson(workload.wait_until_height);
  }
  if (workload.kind == WorkloadKind::kWaitForPeers) {
    return WaitForPeersWorkloadJson(workload.wait_for_peers);
  }
  if (workload.kind == WorkloadKind::kConnectPeer) {
    return ConnectPeerWorkloadJson(workload.connect_peer);
  }
  if (workload.kind == WorkloadKind::kDisconnectPeer) {
    return DisconnectPeerWorkloadJson(workload.disconnect_peer);
  }
  if (workload.kind == WorkloadKind::kRestartNode) {
    return RestartNodeWorkloadJson(workload.restart_node);
  }
  if (workload.kind == WorkloadKind::kFreezeNode) {
    return FreezeNodeWorkloadJson(workload.freeze_node);
  }
  if (workload.kind == WorkloadKind::kUpdateResourceLimits) {
    return ResourceLimitUpdateWorkloadJson(workload.update_resource_limits);
  }
  if (workload.kind == WorkloadKind::kSetResourceProfile ||
      workload.kind == WorkloadKind::kSetNetworkProfile) {
    return ProfileSwitchWorkloadJson(workload.profile_switch, workload.kind);
  }
  if (workload.kind == WorkloadKind::kResourcePressure) {
    return ResourcePressureWorkloadJson(workload.resource_pressure);
  }
  if (workload.kind == WorkloadKind::kSetNetworkCondition) {
    return NetworkConditionWorkloadJson(workload.network_condition);
  }
  if (workload.kind == WorkloadKind::kBlockNetworkFlow ||
      workload.kind == WorkloadKind::kUnblockNetworkFlow) {
    return NetworkBlockWorkloadJson(workload.network_block, workload.kind);
  }
  if (workload.kind == WorkloadKind::kPartitionNodes) {
    return NetworkPartitionWorkloadJson(workload.network_partition,
                                        WorkloadKind::kPartitionNodes);
  }
  if (workload.kind == WorkloadKind::kHealPartition) {
    return NetworkPartitionWorkloadJson(workload.network_partition,
                                        WorkloadKind::kHealPartition);
  }
  if (IsTopologyEdgeAction(workload.kind)) {
    return TopologyEdgeWorkloadJson(workload.topology_edge, workload.kind);
  }
  if (workload.kind == WorkloadKind::kSendRawTransaction) {
    return SendRawTransactionWorkloadJson(workload.send_raw_transaction);
  }
  if (workload.kind == WorkloadKind::kWalletTransactions) {
    return WalletTransactionsWorkloadJson(workload.wallet_transactions);
  }
  if (workload.kind == WorkloadKind::kCheckpoint) {
    return CheckpointWorkloadJson(workload.checkpoint);
  }
  throw std::runtime_error("unknown scenario workload kind");
}

boost::json::object ScheduledScenarioEventJson(
    const ScheduledScenarioEvent& event,
    const SimulationTimeScale& time_scale) {
  boost::json::object object = WorkloadJson(event.action);
  object.erase("type");
  object["action"] = std::string(WorkloadKindName(event.action.kind));
  object["sequence"] = event.sequence;
  object["at"] = std::to_string(event.at.count()) + "ms";
  object["at_ms"] = event.at.count();
  object["wall_at_ms"] = time_scale.WallDuration(event.at).count();
  return object;
}

void WriteScenarioFiles(const Options& options,
                        const std::filesystem::path& run_root,
                        const ChainDriverSpec& chain_spec) {
  const std::vector<ScenarioWorkload> workloads = EffectiveWorkloads(options);
  boost::json::object resolved;
  resolved["run_id"] = options.run_id;
  boost::json::object simulation;
  simulation["name"] = options.simulation_name;
  simulation["seed"] = options.simulation_seed;
  if (options.simulation_duration) {
    simulation["duration"] =
        std::to_string(options.simulation_duration->count()) + "ms";
    simulation["duration_ms"] = options.simulation_duration->count();
    simulation["wall_duration_ms"] =
        options.time_scale.WallDuration(*options.simulation_duration).count();
  } else {
    simulation["duration"] = nullptr;
    simulation["duration_ms"] = nullptr;
    simulation["wall_duration_ms"] = nullptr;
  }
  simulation["time_scale"] = options.time_scale.value();
  simulation["time_scale_millionths"] = options.time_scale.millionths();
  simulation["cleanup_policy"] =
      std::string(CleanupPolicyName(options.cleanup_policy));
  simulation["privilege_mode"] =
      std::string(PrivilegeModeName(options.privilege_mode));
  simulation["log_retention_policy"] =
      std::string(LogRetentionPolicyName(options.log_retention_policy));
  simulation["metrics_interval"] =
      std::to_string(options.metrics_interval.count()) + "ms";
  simulation["metrics_interval_ms"] = options.metrics_interval.count();
  simulation["output_dir"] =
      std::filesystem::absolute(options.output_dir).string();
  simulation["tui_refresh_interval"] =
      std::to_string(options.tui_refresh_ms) + "ms";
  simulation["tui_refresh_interval_ms"] = options.tui_refresh_ms;
  resolved["simulation"] = std::move(simulation);
  resolved["chain"] = chain_spec.name;
  boost::json::object chains;
  for (const auto& [name, chain] : options.chains) {
    boost::json::object definition;
    definition["driver"] = std::string(ChainKindName(chain.driver));
    definition["default_binary"] = chain.default_binary.string();
    chains[name] = std::move(definition);
  }
  resolved["chains"] = std::move(chains);
  resolved["nodes"] = options.nodes;
  if (const std::optional<uint32_t> generate_node =
          CommonBlockGenerationNode(workloads)) {
    resolved["generate_node"] = *generate_node;
  } else {
    resolved["generate_node"] = nullptr;
  }
  resolved["chain_daemon"] = options.chain_daemon.string();
  resolved[chain_spec.daemon_scenario_field] = options.chain_daemon.string();
  if (!options.scenario_json.empty()) {
    resolved["scenario_json"] = options.scenario_json.string();
  }
  if (!options.scenario_yaml.empty()) {
    resolved["scenario_yaml"] = options.scenario_yaml.string();
  }
  resolved["isolated_network"] = options.isolate_network;
  if (options.network_address_plan) {
    resolved["network_address_range"] = options.network_address_plan->Cidr();
  } else {
    resolved["network_address_range"] = nullptr;
  }
  if (const std::optional<uint32_t> sync_timeout_sec =
          CommonBlockGenerationSyncTimeout(workloads)) {
    resolved["sync_timeout_sec"] = *sync_timeout_sec;
  } else {
    resolved["sync_timeout_sec"] = nullptr;
  }
  resolved["metrics_sample_count"] = options.metrics_sample_count;
  resolved["metrics_interval_ms"] = options.metrics_interval.count();
  resolved["keep_artifacts"] = options.keep_artifacts;
  boost::json::object block_production;
  block_production["enabled"] = options.block_production.enabled;
  block_production["native_mining"] =
      options.block_production.mode == MiningMode::kNativeMining;
  block_production["period_ms"] =
      options.block_production.policy.period().count();
  block_production["probability"] =
      options.block_production.policy.probability();
  block_production["seed"] = options.block_production.policy.seed();
  if (options.block_production.difficulty) {
    block_production["difficulty"] =
        options.block_production.difficulty->value();
  } else {
    block_production["difficulty"] = nullptr;
  }
  resolved["block_production"] = std::move(block_production);
  if (options.topology.configured) {
    resolved["topology"] =
        NodeRoleTopologyJson(options.topology, options.wallet_initialization);
  }
  resolved["topology_initial_edges"] = RuntimePeerTopologyEdgesJson(
      RuntimePeerTopology(options.topology.peer_topology, options.nodes));
  if (!options.resource_profiles.empty()) {
    boost::json::object profiles;
    for (const auto& [name, limits] : options.resource_profiles) {
      profiles[name] = ResourceLimitsJson(limits);
    }
    resolved["resource_profiles"] = std::move(profiles);
  }
  if (!options.network_profiles.empty()) {
    boost::json::object profiles;
    for (const auto& [name, condition] : options.network_profiles) {
      profiles[name] = NetworkConditionJson(condition);
    }
    resolved["network_profiles"] = std::move(profiles);
  }
  boost::json::array node_configs;
  for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
    boost::json::object node;
    node["index"] = node_index + 1U;
    node["id"] = options.node_ids.empty() ? chain_spec.node_id_prefix + "-" +
                                                std::to_string(node_index + 1U)
                                          : options.node_ids.at(node_index);
    node["chain"] = chain_spec.name;
    if (!options.node_roles.empty()) {
      node["role"] = options.node_roles.at(node_index);
    } else {
      const bool wallet =
          NodeListContains(options.topology.wallet_nodes, node_index);
      const bool miner =
          NodeListContains(options.topology.miner_nodes, node_index);
      node["role"] = wallet && miner ? "wallet_miner"
                     : wallet        ? "wallet"
                     : miner         ? "miner"
                                     : "base";
    }
    boost::json::object resources_config;
    const auto resource_profile =
        options.node_resource_profiles.find(node_index);
    if (resource_profile != options.node_resource_profiles.end()) {
      resources_config["profile"] = resource_profile->second;
    } else {
      resources_config["profile"] = nullptr;
    }
    resources_config["resolved"] =
        ResourceLimitsJson(InitialResourceLimits(options, node_index));
    node["resources"] = std::move(resources_config);

    boost::json::object network_config;
    const auto network_profile = options.node_network_profiles.find(node_index);
    if (network_profile != options.node_network_profiles.end()) {
      network_config["profile"] = network_profile->second;
    } else {
      network_config["profile"] = nullptr;
    }
    const auto node_condition =
        options.node_network_conditions.find(node_index);
    if (node_condition != options.node_network_conditions.end()) {
      network_config["resolved"] = NetworkConditionJson(node_condition->second);
    } else if (options.network_condition_requested) {
      network_config["resolved"] =
          NetworkConditionJson(options.network_condition);
    } else {
      network_config["resolved"] = nullptr;
    }
    node["network"] = std::move(network_config);
    node_configs.push_back(std::move(node));
  }
  resolved["node_configs"] = std::move(node_configs);
  boost::json::array workload_array;
  for (const ScenarioWorkload& workload : workloads) {
    workload_array.push_back(WorkloadJson(workload));
  }
  resolved["workloads"] = std::move(workload_array);
  boost::json::array scheduled_event_array;
  for (const ScheduledScenarioEvent& event : options.scheduled_events) {
    scheduled_event_array.push_back(
        ScheduledScenarioEventJson(event, options.time_scale));
  }
  resolved["events"] = std::move(scheduled_event_array);
  boost::json::object resources;
  resources["memory_high_bytes"] = options.memory_high_bytes;
  resources["memory_max_bytes"] = options.memory_max_bytes;
  if (options.cpu_quota_requested) {
    resources["cpu_quota_us"] = options.cpu_quota_us;
  } else {
    resources["cpu_quota_us"] = nullptr;
  }
  resources["cpu_period_us"] = options.cpu_period_us;
  resources["cpu_weight"] = options.cpu_weight;
  resources["io_weight"] = options.io_weight;
  resources["io_max"] = IoLimitsJson(options.io_limits);
  resources["pids_max"] = options.pids_max;
  resolved["resources"] = std::move(resources);
  if (options.network_condition_requested) {
    resolved["default_network_condition"] =
        NetworkConditionJson(options.network_condition);
  }
  if (!options.node_network_conditions.empty()) {
    boost::json::array node_conditions;
    for (const auto& [node_index, condition] :
         options.node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      node_conditions.push_back(std::move(node_condition));
    }
    resolved["node_network_conditions"] = std::move(node_conditions);
  }
  if (!options.runtime_node_network_conditions.empty()) {
    boost::json::array runtime_node_conditions;
    for (const auto& [node_index, condition] :
         options.runtime_node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      runtime_node_conditions.push_back(std::move(node_condition));
    }
    resolved["runtime_node_network_conditions"] =
        std::move(runtime_node_conditions);
  }
  if (!options.runtime_node_blocks.empty()) {
    boost::json::array runtime_node_blocks;
    for (const NetworkBlockRule& rule : options.runtime_node_blocks) {
      runtime_node_blocks.push_back(NetworkBlockRuleJson(rule));
    }
    resolved["runtime_node_blocks"] = std::move(runtime_node_blocks);
  }
  if (!options.runtime_node_unblocks.empty()) {
    boost::json::array runtime_node_unblocks;
    for (const NetworkBlockRule& rule : options.runtime_node_unblocks) {
      runtime_node_unblocks.push_back(NetworkBlockRuleJson(rule));
    }
    resolved["runtime_node_unblocks"] = std::move(runtime_node_unblocks);
  }
  if (!options.runtime_partitions.empty()) {
    boost::json::array runtime_partitions;
    for (const NetworkPartitionRule& rule : options.runtime_partitions) {
      runtime_partitions.push_back(NetworkPartitionRuleJson(rule));
    }
    resolved["runtime_partitions"] = std::move(runtime_partitions);
  }
  if (!options.runtime_partition_heals.empty()) {
    boost::json::array runtime_partition_heals;
    for (const NetworkPartitionRule& rule : options.runtime_partition_heals) {
      runtime_partition_heals.push_back(NetworkPartitionRuleJson(rule));
    }
    resolved["runtime_partition_heals"] = std::move(runtime_partition_heals);
  }
  if (!options.runtime_node_resource_updates.empty()) {
    boost::json::array runtime_node_limits;
    for (const auto& [node_index, patch] :
         options.runtime_node_resource_updates) {
      boost::json::object node_limits;
      node_limits["node"] = node_index + 1U;
      node_limits["limits"] = ResourceLimitPatchJson(patch);
      runtime_node_limits.push_back(std::move(node_limits));
    }
    resolved["runtime_node_resource_limits"] = std::move(runtime_node_limits);
  }
  if (!options.runtime_node_restarts.empty() ||
      !options.runtime_node_freezes.empty()) {
    resolved["process"] = ProcessControlConfigJson(ProcessControlConfig{
        .restart_node_indexes = options.runtime_node_restarts,
        .freezes = options.runtime_node_freezes,
    });
  }
  WriteText(run_root / "resolved-scenario.json",
            boost::json::serialize(resolved) + "\n");
  WriteText(run_root / "scenario.yaml", YamlFromJson(resolved));
}

void LoadCleanupMetadata(const std::filesystem::path& run_root,
                         Options* options) {
  const std::filesystem::path resolved_path =
      run_root / "resolved-scenario.json";
  if (!std::filesystem::exists(resolved_path)) {
    return;
  }

  const boost::json::value value = boost::json::parse(ReadText(resolved_path));
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             resolved_path.string());
  }
  const boost::json::object& object = value.as_object();
  options->chain = ParseChainKind(JsonOptionalStringField(
      object, "chain", std::string(ChainKindName(options->chain))));
  options->nodes = JsonOptionalUint32Field(object, "nodes", options->nodes);
  const boost::json::value* isolated = object.if_contains("isolated_network");
  if (isolated != nullptr) {
    if (!isolated->is_bool()) {
      throw std::runtime_error(
          "resolved scenario isolated_network is not a boolean");
    }
    options->isolate_network = isolated->as_bool();
  }
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options->chain);
  if (options->nodes < 1 || options->nodes > chain_spec.max_nodes) {
    throw std::runtime_error(
        "cleanup currently supports resolved node counts in 1.." +
        std::to_string(chain_spec.max_nodes));
  }
}

void CleanupRun(Options options) {
  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  LoadCleanupMetadata(run_root, &options);
  RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");

  std::unique_ptr<NetworkAllocationLock> network_allocation_lock;
  if (options.isolate_network) {
    network_allocation_lock = std::make_unique<NetworkAllocationLock>();
  }

  for (uint32_t i = 0; i < options.nodes; ++i) {
    NodeVethConfig config;
    config.host_name = NodeInterfaceName(options.run_id, i, 'h');
    config.peer_name = NodeInterfaceName(options.run_id, i, 'p');
    DeleteNodeVethNetwork(config);
  }
  Cgroup::RemoveRun(options.run_id);

  BBP_LOG(info) << "cleanup_run=" << options.run_id << "\n"
                << "nodes=" << options.nodes << "\n"
                << "run_dir=" << run_root;
}

void StartNodes(const Options& options, const std::filesystem::path& run_root,
                const std::filesystem::path& events_path,
                const ChainDriverSpec& chain_spec, const ChainDriver& driver,
                const RuntimePeerTopology& runtime_topology,
                std::vector<NodeRuntime>& nodes, std::stop_token stop_token) {
  if (options.isolate_network) {
    RequireNetworkSetupCapabilities();
  }
  if (options.isolate_network && options.nodes > 1 &&
      !HostIpv4ForwardingEnabled()) {
    throw std::runtime_error(
        "isolated multi-node chain runs require IPv4 forwarding in the parent "
        "network namespace");
  }
  nodes.reserve(options.nodes);
  for (uint32_t i = 0; i < options.nodes; ++i) {
    ThrowIfStopRequested(stop_token);
    ChainNodeConfigRequest config_request;
    config_request.run_id = options.run_id;
    config_request.run_root = run_root;
    config_request.daemon_binary = options.chain_daemon;
    config_request.node_index = i;
    if (!options.node_ids.empty()) {
      config_request.node_id = options.node_ids.at(i);
    }
    config_request.wallet_enabled =
        options.wallet_backed_workload_requested &&
        NodeListContains(options.topology.wallet_nodes, i);
    config_request.connect_peers =
        StartupPeerAddresses(options, runtime_topology, chain_spec, i);

    ChainNodeConfig config = MakeChainNodeConfig(chain_spec, config_request);
    const std::string node_id = config.id;

    NodeRuntime runtime;
    try {
      runtime.config = config;
      runtime.perf_counter_target_kind = PerfCounterTargetKind::kNode;
      runtime.perf_counter_target_id = node_id;
      const auto resource_profile = options.node_resource_profiles.find(i);
      if (resource_profile != options.node_resource_profiles.end()) {
        runtime.resource_profile = resource_profile->second;
      }
      const auto network_profile = options.node_network_profiles.find(i);
      if (network_profile != options.node_network_profiles.end()) {
        runtime.network_profile = network_profile->second;
      }
      WriteNodeState(events_path, options.run_id, node_id,
                     NodeRuntimeLifecycle::kPreparing);
      if (options.isolate_network) {
        runtime.network_namespace = NetworkNamespace::Create();
        runtime.network = MakeNodeVethConfig(options, i);
        SetupNodeVethNetwork(runtime.network_namespace->fd(), *runtime.network);
        if (runtime.network->apply_condition) {
          const QdiscInfo qdisc = VerifyNodeNetworkCondition(*runtime.network);
          WriteEvent(
              events_path, options.run_id, node_id,
              SimulationEventKind::kNetworkConditionVerified,
              NetworkConditionVerificationDetail(*runtime.network, qdisc));
        }
        runtime.directional_network_policies =
            DirectionalNetworkPoliciesForNode(options, runtime_topology, i);
        if (!runtime.directional_network_policies.empty()) {
          UpdateDirectionalNetworkPoliciesInNamespace(
              runtime.network_namespace->fd(), runtime.network->peer_name, {},
              runtime.directional_network_policies);
          boost::json::object detail;
          detail["source_node"] = i + 1U;
          detail["peer_if"] = runtime.network->peer_name;
          detail["verified"] = true;
          detail["policies"] = DirectionalNetworkPoliciesJson(
              runtime.directional_network_policies);
          WriteEvent(events_path, options.run_id, node_id,
                     SimulationEventKind::kDirectionalNetworkPoliciesVerified,
                     boost::json::serialize(detail));
        }
        runtime.config.rpc_host = runtime.network->node_address;
        runtime.config.rpc_bind = runtime.network->node_address;
        runtime.config.rpc_allow_ips = {runtime.network->host_address};
        runtime.config.p2p_host = runtime.network->node_address;
        runtime.config.p2p_bind = runtime.network->node_address;
        WriteEvent(events_path, options.run_id, node_id,
                   SimulationEventKind::kNetworkReady,
                   "node_ip=" + runtime.network->node_address +
                       " host_if=" + runtime.network->host_name +
                       " peer_if=" + runtime.network->peer_name);
        WriteNodeState(events_path, options.run_id, node_id,
                       NodeRuntimeLifecycle::kNetworkNamespaceReady);
      }

      runtime.cgroup = Cgroup::Create(options.run_id, node_id);
      runtime.resources = InitialResourceLimits(options, i);
      runtime.cgroup->SetMemoryHigh(runtime.resources.memory_high_bytes);
      runtime.cgroup->SetMemoryMax(runtime.resources.memory_max_bytes);
      runtime.cgroup->SetCpuMax(runtime.resources.cpu_quota_us,
                                runtime.resources.cpu_period_us);
      runtime.cgroup->SetCpuWeight(runtime.resources.cpu_weight);
      runtime.cgroup->SetIoWeight(runtime.resources.io_weight);
      for (const IoLimit& io_limit : runtime.resources.io_limits) {
        runtime.cgroup->SetIoMax(io_limit);
      }
      runtime.cgroup->SetPidsMax(runtime.resources.pids_max);
      WriteNodeState(events_path, options.run_id, node_id,
                     NodeRuntimeLifecycle::kCgroupReady);

      ProcessSpec process = driver.RenderProcess(runtime.config);
      if (runtime.network_namespace) {
        process.network_namespace_fd = runtime.network_namespace->fd();
      }
      WriteNodeState(events_path, options.run_id, node_id,
                     NodeRuntimeLifecycle::kStarting);
      runtime.process = ChildProcess::Spawn(process, runtime.cgroup->path());
      runtime.process_started_at = std::chrono::steady_clock::now();
      AttachNodePerfCounters(runtime);
      BBP_LOG(info) << "started " << node_id
                    << " pid=" << runtime.process.pid();
      WriteEvent(events_path, options.run_id, node_id,
                 SimulationEventKind::kProcessStarted,
                 "pid=" + std::to_string(runtime.process.pid()));
      nodes.push_back(std::move(runtime));
    } catch (...) {
      WriteNodeState(events_path, options.run_id, node_id,
                     NodeRuntimeLifecycle::kFailed);
      runtime.process.Kill();
      if (runtime.cgroup) {
        try {
          runtime.cgroup->KillAll();
          runtime.cgroup->Remove();
        } catch (const std::exception&) {
        }
      }
      if (runtime.network) {
        DeleteNodeVethNetwork(*runtime.network);
      }
      throw;
    }
  }

  for (auto& node : nodes) {
    try {
      driver.WaitReady(node.config,
                       std::chrono::seconds(options.ready_timeout_sec),
                       stop_token);
    } catch (...) {
      if (!node.process.running()) {
        WriteEvent(events_path, options.run_id, node.config.id,
                   SimulationEventKind::kProcessExitedBeforeRpcReady,
                   ProcessExitDetail(node.process));
      }
      throw;
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kRpcReady);
    node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
    WriteNodeState(events_path, options.run_id, node.config.id,
                   NodeRuntimeLifecycle::kRunning);
  }

  for (auto& node : nodes) {
    for (const std::string& peer : node.config.connect_peers) {
      ThrowIfStopRequested(stop_token);
      driver.ConnectPeer(node.config, peer, stop_token);
      driver.WaitForPeerAddress(node.config, peer,
                                std::chrono::seconds(options.ready_timeout_sec),
                                stop_token);
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kStartupPeerConnected, "address=" + peer);
    }
  }
}

void InitializeWalletNodes(const Options& options,
                           const std::filesystem::path& events_path,
                           const ChainDriver& driver,
                           std::vector<NodeRuntime>& nodes,
                           SimulationRegistry& registry,
                           std::stop_token stop_token) {
  if (!options.wallet_backed_workload_requested) {
    return;
  }
  if (registry.wallet_initialization().strategy !=
      WalletInitializationStrategy::kDriverRpc) {
    throw std::runtime_error(
        "wallet-backed workload requires driver_rpc wallet initialization");
  }

  for (size_t wallet_index = 0; wallet_index < registry.wallets().size();
       ++wallet_index) {
    ThrowIfStopRequested(stop_token);
    WalletIdentity& wallet = registry.MutableWalletByIndex(wallet_index);
    if (wallet.node == 0U || wallet.node > nodes.size()) {
      throw std::runtime_error("wallet node is out of range");
    }
    NodeRuntime& node = nodes[wallet.node - 1U];
    if (!node.config.wallet_enabled) {
      throw std::runtime_error(
          "wallet node was not started with wallet support enabled");
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kWalletAddressRequested,
               WalletAddressDetail(wallet, registry.wallet_initialization()));
    wallet.address = driver.CreateWalletAddress(
        node.config, ToChainWalletMode(registry.wallet_initialization()),
        stop_token);
    if (wallet.address.empty()) {
      throw std::runtime_error("chain wallet RPC returned an empty address");
    }
    wallet.funding_address = driver.CreateWalletFundingAddress(
        node.config, ToChainWalletMode(registry.wallet_initialization()),
        wallet.address, stop_token);
    if (wallet.funding_address.empty()) {
      throw std::runtime_error(
          "chain wallet RPC returned an empty funding address");
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kWalletAddressCreated,
               WalletAddressDetail(wallet, registry.wallet_initialization()));
  }
}

std::string ResourceLimitUpdateDetail(
    const ResourceLimitPatch& patch, const ResourceLimits& previous,
    const ResourceLimits& current,
    std::optional<uint32_t> workload_index = std::nullopt,
    std::optional<uint32_t> workload_count = std::nullopt,
    std::optional<uint32_t> node = std::nullopt,
    std::optional<std::uint64_t> operator_sequence = std::nullopt) {
  boost::json::object detail;
  if (workload_index) {
    detail["workload_index"] = *workload_index;
  }
  if (workload_count) {
    detail["workload_count"] = *workload_count;
  }
  if (node) {
    detail["node"] = *node;
  }
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  detail["requested"] = ResourceLimitPatchJson(patch);
  detail["previous"] = ResourceLimitsJson(previous);
  detail["current"] = ResourceLimitsJson(current);
  return boost::json::serialize(detail);
}

void ApplyResourceLimitUpdate(
    const Options& options, const std::filesystem::path& events_path,
    NodeRuntime& node, const ResourceLimitPatch& patch,
    std::optional<uint32_t> workload_index = std::nullopt,
    std::optional<uint32_t> workload_count = std::nullopt,
    std::optional<uint32_t> workload_node = std::nullopt,
    std::optional<std::uint64_t> operator_sequence = std::nullopt,
    bool resolve_operator_io_limit = false) {
  std::lock_guard<std::mutex> lock(node_resource_state_mutex);
  if (!node.cgroup) {
    throw std::runtime_error("resource update requires a node cgroup");
  }
  const ResourceLimits previous = node.resources;
  const ResourceLimitPatch effective_patch =
      resolve_operator_io_limit
          ? ResolveOperatorResourceLimitPatch(previous, patch)
          : patch;
  const ResourceLimits next =
      ApplyResourceLimitPatch(previous, effective_patch, node.config.id);
  try {
    WriteResourceLimits(*node.cgroup, previous, next);
  } catch (...) {
    const std::exception_ptr apply_error = std::current_exception();
    try {
      WriteResourceLimits(*node.cgroup, next, previous);
    } catch (const std::exception& restore_error) {
      BBP_LOG(error) << "failed to restore partially applied resource update "
                        "for "
                     << node.config.id << ": " << restore_error.what();
    } catch (...) {
      BBP_LOG(error) << "failed to restore partially applied resource update "
                        "for "
                     << node.config.id << ": unknown exception";
    }
    std::rethrow_exception(apply_error);
  }
  node.resources = next;
  node.resource_profile.clear();
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kResourceLimitsUpdated,
             ResourceLimitUpdateDetail(patch, previous, next, workload_index,
                                       workload_count, workload_node,
                                       operator_sequence));
}

std::string ExceptionMessage(const std::exception_ptr& error);

std::string ProfileRollbackFailureDetail(
    WorkloadKind kind, std::string_view profile,
    std::string_view original_error,
    const std::vector<std::string>& rollback_errors) {
  boost::json::object detail;
  detail["action"] = WorkloadKindName(kind);
  detail["profile"] = profile;
  detail["original_error"] = original_error;
  boost::json::array errors;
  for (const std::string& error : rollback_errors) {
    errors.emplace_back(error);
  }
  detail["rollback_errors"] = std::move(errors);
  return boost::json::serialize(detail);
}

void WriteProfileRollbackFailureEventSafely(
    const Options& options, const std::filesystem::path& events_path,
    WorkloadKind kind, std::string_view profile,
    const std::exception_ptr& original_error,
    const std::vector<std::string>& rollback_errors) noexcept {
  if (rollback_errors.empty()) {
    return;
  }
  try {
    WriteEvent(
        events_path, options.run_id, "sim",
        SimulationEventKind::kProfileUpdateRollbackFailed,
        ProfileRollbackFailureDetail(
            kind, profile, ExceptionMessage(original_error), rollback_errors));
  } catch (const std::exception& event_error) {
    BBP_LOG(error) << "failed to record profile rollback failure: "
                   << event_error.what();
  } catch (...) {
    BBP_LOG(error) << "failed to record profile rollback failure: unknown "
                      "exception";
  }
}

std::string ResourceProfileUpdateDetail(const ProfileSwitchWorkload& workload,
                                        uint32_t node,
                                        std::string_view previous_profile,
                                        const ResourceLimits& previous,
                                        const ResourceLimits& current,
                                        uint32_t workload_index,
                                        uint32_t workload_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["profile"] = workload.profile;
  if (previous_profile.empty()) {
    detail["previous_profile"] = nullptr;
  } else {
    detail["previous_profile"] = previous_profile;
  }
  detail["previous"] = ResourceLimitsJson(previous);
  detail["current"] = ResourceLimitsJson(current);
  detail["kernel_verified"] = true;
  return boost::json::serialize(detail);
}

void ApplyResourceProfileSwitch(const Options& options,
                                const std::filesystem::path& events_path,
                                std::vector<NodeRuntime>& nodes,
                                const ProfileSwitchWorkload& workload,
                                uint32_t workload_index,
                                uint32_t workload_count,
                                std::stop_token stop_token) {
  const ResourceLimits& desired =
      options.resource_profiles.at(workload.profile);
  struct PreviousState {
    uint32_t node = 0U;
    ResourceLimits limits;
    std::string profile;
  };
  std::vector<PreviousState> previous_states;
  previous_states.reserve(workload.nodes.size());
  std::vector<std::size_t> attempted;

  {
    std::lock_guard<std::mutex> lock(node_resource_state_mutex);
    for (const uint32_t one_based_node : workload.nodes) {
      if (one_based_node == 0U || one_based_node > nodes.size()) {
        throw std::runtime_error(
            "resource profile target node is out of range");
      }
      const NodeRuntime& runtime = nodes[one_based_node - 1U];
      if (!runtime.cgroup) {
        throw std::runtime_error("resource profile update requires a cgroup");
      }
      previous_states.push_back(PreviousState{
          .node = one_based_node,
          .limits = runtime.resources,
          .profile = runtime.resource_profile,
      });
    }

    try {
      for (std::size_t index = 0; index < previous_states.size(); ++index) {
        ThrowIfStopRequested(stop_token);
        attempted.push_back(index);
        NodeRuntime& runtime = nodes[previous_states[index].node - 1U];
        WriteResourceLimits(*runtime.cgroup, previous_states[index].limits,
                            desired);
      }
    } catch (...) {
      const std::exception_ptr original_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      for (auto iter = attempted.rbegin(); iter != attempted.rend(); ++iter) {
        const PreviousState& previous = previous_states[*iter];
        NodeRuntime& runtime = nodes[previous.node - 1U];
        try {
          WriteResourceLimits(*runtime.cgroup, desired, previous.limits);
        } catch (const std::exception& error) {
          rollback_errors.push_back(runtime.config.id + ": " + error.what());
        } catch (...) {
          rollback_errors.push_back(runtime.config.id +
                                    ": unknown rollback error");
        }
      }
      WriteProfileRollbackFailureEventSafely(
          options, events_path, WorkloadKind::kSetResourceProfile,
          workload.profile, original_error, rollback_errors);
      std::rethrow_exception(original_error);
    }

    for (const PreviousState& previous : previous_states) {
      NodeRuntime& runtime = nodes[previous.node - 1U];
      runtime.resources = desired;
      runtime.resource_profile = workload.profile;
    }
  }

  for (const PreviousState& previous : previous_states) {
    const NodeRuntime& runtime = nodes[previous.node - 1U];
    WriteEvent(events_path, options.run_id, runtime.config.id,
               SimulationEventKind::kResourceProfileUpdated,
               ResourceProfileUpdateDetail(
                   workload, previous.node, previous.profile, previous.limits,
                   desired, workload_index, workload_count));
  }
}

void ApplyRuntimeResourceLimitUpdates(const Options& options,
                                      const std::filesystem::path& events_path,
                                      std::vector<NodeRuntime>& nodes,
                                      std::stop_token stop_token) {
  for (const auto& [node_index, patch] :
       options.runtime_node_resource_updates) {
    ThrowIfStopRequested(stop_token);
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime resource update node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    ApplyResourceLimitUpdate(options, events_path, node, patch);
  }
}

std::string ResourcePressureDetail(const ResourcePressureWorkload& workload,
                                   const ResourceLimits& previous_limits,
                                   const ResourceLimits& pressure_limits,
                                   const ResourceLimits& current_limits,
                                   uint32_t workload_index,
                                   uint32_t workload_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = workload.node;
  detail["duration_ms"] = workload.duration_ms;
  detail["requested"] = ResourceLimitPatchJson(workload.patch);
  detail["previous"] = ResourceLimitsJson(previous_limits);
  detail["pressure"] = ResourceLimitsJson(pressure_limits);
  detail["current"] = ResourceLimitsJson(current_limits);
  return boost::json::serialize(detail);
}

void ApplyResourcePressureWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const std::filesystem::path& metrics_path, const ChainDriver& driver,
    std::vector<NodeRuntime>& nodes, std::mutex& node_process_mutex,
    const ResourcePressureWorkload& workload, uint32_t workload_index,
    uint32_t workload_count, std::stop_token stop_token) {
  NodeRuntime& node = nodes[workload.node - 1U];
  if (!node.cgroup) {
    throw std::runtime_error("resource pressure requires a node cgroup");
  }

  ResourceLimits previous_limits;
  ResourceLimits pressure_limits;
  std::string previous_profile;
  {
    std::lock_guard<std::mutex> lock(node_resource_state_mutex);
    previous_limits = node.resources;
    previous_profile = node.resource_profile;
    pressure_limits = ApplyResourceLimitPatch(previous_limits, workload.patch,
                                              node.config.id);
    try {
      WriteResourceLimits(*node.cgroup, previous_limits, pressure_limits);
    } catch (...) {
      const std::exception_ptr apply_error = std::current_exception();
      try {
        WriteResourceLimits(*node.cgroup, pressure_limits, previous_limits);
      } catch (const std::exception& restore_error) {
        BBP_LOG(error) << "failed to restore partially applied resource "
                          "pressure limits for "
                       << node.config.id << ": " << restore_error.what();
      } catch (...) {
        BBP_LOG(error) << "failed to restore partially applied resource "
                          "pressure limits for "
                       << node.config.id << ": unknown exception";
      }
      std::rethrow_exception(apply_error);
    }
    node.resources = pressure_limits;
    node.resource_profile.clear();
  }

  try {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kResourcePressureStarted,
               ResourcePressureDetail(workload, previous_limits,
                                      pressure_limits, pressure_limits,
                                      workload_index, workload_count));
    WaitForDuration(std::chrono::milliseconds(workload.duration_ms),
                    stop_token);
    WriteMetricsSnapshot(metrics_path, options, driver, nodes,
                         node_process_mutex, {}, {}, stop_token);
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    try {
      {
        std::lock_guard<std::mutex> lock(node_resource_state_mutex);
        WriteResourceLimits(*node.cgroup, node.resources, previous_limits);
        node.resources = previous_limits;
        node.resource_profile = previous_profile;
      }
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kResourcePressureRestoredAfterError,
                 ResourcePressureDetail(workload, previous_limits,
                                        pressure_limits, previous_limits,
                                        workload_index, workload_count));
    } catch (const std::exception& restore_error) {
      BBP_LOG(error) << "failed to restore resource pressure limits for "
                     << node.config.id << ": " << restore_error.what();
    } catch (...) {
      BBP_LOG(error) << "failed to restore resource pressure limits for "
                     << node.config.id << ": unknown exception";
    }
    std::rethrow_exception(original_error);
  }

  {
    std::lock_guard<std::mutex> lock(node_resource_state_mutex);
    WriteResourceLimits(*node.cgroup, node.resources, previous_limits);
    node.resources = previous_limits;
    node.resource_profile = previous_profile;
  }
  WriteEvent(
      events_path, options.run_id, node.config.id,
      SimulationEventKind::kResourcePressureFinished,
      ResourcePressureDetail(workload, previous_limits, pressure_limits,
                             previous_limits, workload_index, workload_count));
}

void ApplyConnectPeerWorkload(const Options& options,
                              const std::filesystem::path& events_path,
                              const ChainDriver& driver,
                              PeerConnectivityController& controller,
                              std::vector<NodeRuntime>& nodes,
                              const ConnectPeerWorkload& workload,
                              uint32_t workload_index, uint32_t workload_count,
                              std::stop_token stop_token) {
  NodeRuntime& node = nodes[workload.node - 1U];
  const std::string address = PeerAddress(options, nodes, workload.peer);
  const std::vector<std::string> before_addresses =
      driver.PeerAddresses(node.config, stop_token);
  const bool connected_before =
      !driver.ConnectedPeerAddresses(node.config, {address}, stop_token)
           .empty();
  controller.ConnectPeer(node.config.id, nodes[workload.peer - 1U].config.id,
                         std::chrono::seconds(workload.timeout_sec),
                         stop_token);
  const std::vector<std::string> after_addresses =
      driver.PeerAddresses(node.config, stop_token);
  const bool connected_after =
      !driver.ConnectedPeerAddresses(node.config, {address}, stop_token)
           .empty();
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kPeerConnected,
             PeerChurnDetail(
                 workload_index, workload_count, workload.node, workload.peer,
                 address, static_cast<uint64_t>(before_addresses.size()),
                 static_cast<uint64_t>(after_addresses.size()),
                 connected_before, connected_after, workload.timeout_sec));
}

void ApplyDisconnectPeerWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, PeerConnectivityController& controller,
    std::vector<NodeRuntime>& nodes, const DisconnectPeerWorkload& workload,
    uint32_t workload_index, uint32_t workload_count,
    std::stop_token stop_token) {
  NodeRuntime& node = nodes[workload.node - 1U];
  const std::string address = PeerAddress(options, nodes, workload.peer);
  const std::vector<std::string> before_addresses =
      driver.PeerAddresses(node.config, stop_token);
  const bool connected_before =
      !driver.ConnectedPeerAddresses(node.config, {address}, stop_token)
           .empty();
  controller.DisconnectPeer(node.config.id, nodes[workload.peer - 1U].config.id,
                            std::chrono::seconds(workload.timeout_sec),
                            stop_token);
  const std::vector<std::string> after_addresses =
      driver.PeerAddresses(node.config, stop_token);
  const bool connected_after =
      !driver.ConnectedPeerAddresses(node.config, {address}, stop_token)
           .empty();
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kPeerDisconnected,
             PeerChurnDetail(
                 workload_index, workload_count, workload.node, workload.peer,
                 address, static_cast<uint64_t>(before_addresses.size()),
                 static_cast<uint64_t>(after_addresses.size()),
                 connected_before, connected_after, workload.timeout_sec));
}

std::vector<std::string> RuntimeTopologyAllowedPeers(
    const RuntimePeerTopology& topology, const std::vector<NodeRuntime>& nodes,
    std::uint32_t node_index) {
  std::vector<std::string> allowed;
  for (const std::uint32_t peer_index :
       topology.ActivePeerIndexes(node_index)) {
    if (peer_index >= nodes.size()) {
      throw std::runtime_error(
          "runtime topology peer references an unknown node");
    }
    allowed.push_back(nodes[peer_index].config.id);
  }
  return allowed;
}

std::string TopologyEdgeUpdateDetail(WorkloadKind action,
                                     std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     const RuntimePeerTopologyEdge& previous,
                                     const RuntimePeerTopologyEdge& current,
                                     bool kernel_verified, bool peer_verified,
                                     std::uint32_t timeout_sec) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["action"] = WorkloadKindName(action);
  detail["from"] = current.from + 1U;
  detail["to"] = current.to + 1U;
  detail["previous"] = RuntimePeerTopologyEdgeJson(previous);
  detail["current"] = RuntimePeerTopologyEdgeJson(current);
  detail["kernel_verified"] = kernel_verified;
  detail["peer_verified"] = peer_verified;
  if (action != WorkloadKind::kSetEdgeCondition) {
    detail["timeout_sec"] = timeout_sec;
  }
  return boost::json::serialize(detail);
}

std::string TopologyEdgeRollbackFailureDetail(
    WorkloadKind action, const RuntimePeerTopologyEdge& previous,
    const RuntimePeerTopologyEdge& attempted, std::string_view original_error,
    const std::vector<std::string>& rollback_errors) {
  boost::json::object detail;
  detail["action"] = WorkloadKindName(action);
  detail["from"] = previous.from + 1U;
  detail["to"] = previous.to + 1U;
  detail["previous"] = RuntimePeerTopologyEdgeJson(previous);
  detail["attempted"] = RuntimePeerTopologyEdgeJson(attempted);
  detail["original_error"] = original_error;
  boost::json::array errors;
  for (const std::string& error : rollback_errors) {
    errors.emplace_back(error);
  }
  detail["rollback_errors"] = std::move(errors);
  return boost::json::serialize(detail);
}

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

void ApplyTopologyEdgeWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriverSpec& chain_spec, const ChainDriver& driver,
    PeerConnectivityController& controller,
    RuntimePeerTopology& runtime_topology, std::vector<NodeRuntime>& nodes,
    const TopologyEdgeWorkload& workload, WorkloadKind action,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  const std::uint32_t from = workload.from - 1U;
  const std::uint32_t to = workload.to - 1U;
  if (from >= nodes.size() || to >= nodes.size() || from == to) {
    throw std::runtime_error("runtime topology edge action node is invalid");
  }

  NodeRuntime& source = nodes[from];
  NodeRuntime& target = nodes[to];
  const RuntimePeerTopologyEdge previous = runtime_topology.Edge(from, to);
  RuntimePeerTopologyEdge attempted = previous;
  const std::vector<DirectionalNetworkPolicy> expected_previous_policies =
      runtime_topology.DirectionalPolicies(NetworkAddressPlan(options), from);
  std::vector<DirectionalNetworkPolicy> previous_policies;
  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    previous_policies = source.directional_network_policies;
  }
  if (previous_policies != expected_previous_policies) {
    throw std::runtime_error(
        "runtime directional policy state does not match topology model");
  }
  const std::vector<std::string> expected_previous_allowed =
      RuntimeTopologyAllowedPeers(runtime_topology, nodes, from);
  const std::vector<std::string> previous_allowed =
      controller.AllowedPeersFor(source.config.id);
  if (previous_allowed != expected_previous_allowed) {
    throw std::runtime_error(
        "runtime allowed-peer state does not match topology model");
  }
  const std::vector<std::string> expected_previous_restart_peers =
      StartupPeerAddresses(options, runtime_topology, chain_spec, from);
  const std::vector<std::string> previous_restart_peers =
      source.config.connect_peers;
  if (previous_restart_peers != expected_previous_restart_peers) {
    throw std::runtime_error(
        "runtime restart-peer state does not match topology model");
  }

  const std::string peer_address = PeerAddress(options, nodes, workload.to);
  std::vector<std::string> before_peer_addresses;
  bool connected_before = false;

  bool model_mutated = false;
  bool active_transition = false;
  bool kernel_updated = false;
  bool allowed_updated = false;
  bool peer_transition_attempted = false;
  bool config_updated = false;
  std::vector<DirectionalNetworkPolicy> desired_policies;
  std::vector<DirectionalNetworkPolicy> rollback_policy_state;
  std::vector<std::string> desired_allowed;
  std::vector<std::string> desired_restart_peers;
  try {
    if (action == WorkloadKind::kSetEdgeCondition) {
      if (!workload.condition) {
        throw std::runtime_error(
            "set_edge_condition action is missing its condition");
      }
      static_cast<void>(
          runtime_topology.SetCondition(from, to, *workload.condition));
    } else if (action == WorkloadKind::kActivateEdge) {
      static_cast<void>(runtime_topology.SetActive(from, to, true));
    } else if (action == WorkloadKind::kDeactivateEdge) {
      static_cast<void>(runtime_topology.SetActive(from, to, false));
    } else if (action == WorkloadKind::kRestoreEdge) {
      static_cast<void>(runtime_topology.RestoreBaseline(from, to));
    } else {
      throw std::runtime_error("unknown runtime topology edge action");
    }
    model_mutated = true;
    attempted = runtime_topology.Edge(from, to);
    active_transition = previous.active != attempted.active;
    if (active_transition) {
      before_peer_addresses = driver.PeerAddresses(source.config, stop_token);
      connected_before = !driver
                              .ConnectedPeerAddresses(
                                  source.config, {peer_address}, stop_token)
                              .empty();
    }

    desired_policies =
        runtime_topology.DirectionalPolicies(NetworkAddressPlan(options), from);
    desired_allowed =
        RuntimeTopologyAllowedPeers(runtime_topology, nodes, from);
    desired_restart_peers =
        StartupPeerAddresses(options, runtime_topology, chain_spec, from);
    controller.ValidateAllowedPeerUpdate(source.config.id, desired_allowed);
    rollback_policy_state = desired_policies;

    ThrowIfStopRequested(stop_token);
    if (source.network) {
      if (!source.network_namespace) {
        throw std::runtime_error(
            "runtime topology source has no network namespace");
      }
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      UpdateDirectionalNetworkPoliciesInNamespace(
          source.network_namespace->fd(), source.network->peer_name,
          previous_policies, desired_policies);
      source.directional_network_policies.swap(rollback_policy_state);
      kernel_updated = true;
    } else if (previous_policies != desired_policies) {
      throw std::runtime_error(
          "runtime topology conditions require isolated networking");
    }

    if (previous_allowed != desired_allowed) {
      controller.SetAllowedPeers(source.config.id, desired_allowed);
      allowed_updated = true;
    }

    std::vector<std::string> after_peer_addresses;
    bool connected_after = connected_before;
    if (active_transition) {
      peer_transition_attempted = true;
      if (attempted.active) {
        controller.ConnectPeer(source.config.id, target.config.id,
                               std::chrono::seconds(workload.timeout_sec),
                               stop_token);
      } else {
        controller.DisconnectPeer(source.config.id, target.config.id,
                                  std::chrono::seconds(workload.timeout_sec),
                                  stop_token);
      }
      after_peer_addresses = driver.PeerAddresses(source.config, stop_token);
      connected_after = attempted.active;
    }

    source.config.connect_peers = desired_restart_peers;
    config_updated = true;

    if (active_transition) {
      WriteEvent(events_path, options.run_id, source.config.id,
                 attempted.active ? SimulationEventKind::kPeerConnected
                                  : SimulationEventKind::kPeerDisconnected,
                 PeerChurnDetail(
                     workload_index, workload_count, workload.from, workload.to,
                     peer_address,
                     static_cast<std::uint64_t>(before_peer_addresses.size()),
                     static_cast<std::uint64_t>(after_peer_addresses.size()),
                     connected_before, connected_after, workload.timeout_sec));
    }
    WriteEvent(events_path, options.run_id, source.config.id,
               SimulationEventKind::kTopologyEdgeUpdated,
               TopologyEdgeUpdateDetail(action, workload_index, workload_count,
                                        previous, attempted, true, true,
                                        workload.timeout_sec));
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::vector<std::string> rollback_errors;
    const auto rollback = [&](std::string_view step, const auto& operation) {
      try {
        operation();
      } catch (const std::exception& error) {
        rollback_errors.push_back(std::string(step) + ": " + error.what());
      } catch (...) {
        rollback_errors.push_back(std::string(step) + ": unknown exception");
      }
    };

    if (kernel_updated) {
      rollback("kernel policy", [&] {
        std::lock_guard<std::mutex> lock(node_network_state_mutex);
        UpdateDirectionalNetworkPoliciesInNamespace(
            source.network_namespace->fd(), source.network->peer_name,
            desired_policies, previous_policies);
        source.directional_network_policies.swap(rollback_policy_state);
      });
    }
    // A deactivation removes the target from the allow-list before
    // disconnecting it.  Restore that list before attempting a compensating
    // reconnect.  For an activation, keep the target eligible until any
    // compensating disconnect has completed, then restore the prior (inactive)
    // allow-list.
    const bool restore_allowed_before_peer = allowed_updated && previous.active;
    if (restore_allowed_before_peer) {
      rollback("allowed peers", [&] {
        controller.SetAllowedPeers(source.config.id, previous_allowed);
      });
      allowed_updated = false;
    }
    if (peer_transition_attempted) {
      rollback("peer state", [&] {
        const bool connected_now =
            !driver.ConnectedPeerAddresses(source.config, {peer_address})
                 .empty();
        if (connected_now == connected_before) {
          return;
        }
        if (connected_before) {
          if (!previous.active) {
            throw std::runtime_error(
                "cannot restore a pre-existing physical session for an "
                "inactive logical edge");
          }
          controller.ConnectPeer(source.config.id, target.config.id,
                                 std::chrono::seconds(workload.timeout_sec));
        } else {
          controller.DisconnectPeer(source.config.id, target.config.id,
                                    std::chrono::seconds(workload.timeout_sec));
        }
      });
    }
    if (allowed_updated) {
      rollback("allowed peers", [&] {
        controller.SetAllowedPeers(source.config.id, previous_allowed);
      });
    }
    if (config_updated || model_mutated) {
      source.config.connect_peers = previous_restart_peers;
    }
    if (model_mutated) {
      rollback("topology model",
               [&] { runtime_topology.RestoreState(previous); });
    }

    if (!rollback_errors.empty()) {
      const std::string original_message = ExceptionMessage(original_error);
      for (const std::string& rollback_error : rollback_errors) {
        BBP_LOG(error) << "topology edge rollback failed after "
                       << original_message << ": " << rollback_error;
      }
      try {
        WriteEvent(events_path, options.run_id, source.config.id,
                   SimulationEventKind::kTopologyEdgeUpdateRollbackFailed,
                   TopologyEdgeRollbackFailureDetail(
                       action, previous, attempted, original_message,
                       rollback_errors));
      } catch (const std::exception& event_error) {
        BBP_LOG(error) << "failed to record topology rollback failure: "
                       << event_error.what();
      }
    }
    std::rethrow_exception(original_error);
  }
}

void ApplySendRawTransactionWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    TransactionObservationTracker& transaction_tracker,
    const SendRawTransactionWorkload& workload, uint32_t workload_index,
    uint32_t workload_count, std::stop_token stop_token) {
  NodeRuntime& funder = nodes[workload.funding_node - 1U];
  NodeRuntime& submitter = nodes[workload.submit_node - 1U];
  const uint64_t start_height =
      driver.ReadMetrics(funder.config, stop_token).height;
  std::vector<std::string> funding_hashes =
      driver.GenerateBlocks(funder.config, workload.funding_blocks,
                            workload.source_address, stop_token);
  RecordGeneratedBlocks(driver, funder, funding_hashes, stop_token);
  const uint64_t target_height =
      start_height + static_cast<uint64_t>(funding_hashes.size());
  for (auto& node : nodes) {
    driver.WaitForHeight(node.config, target_height,
                         std::chrono::seconds(workload.timeout_sec),
                         stop_token);
  }

  const uint64_t minimum_amount =
      workload.amount_satoshis + workload.fee_satoshis;
  const ChainUtxo utxo = driver.FindSpendableOutput(
      funder.config, funding_hashes, workload.source_address, minimum_amount,
      ChainDriverSpecFor(options.chain).coinbase_spendable_confirmations,
      stop_token);
  const ChainRawTransactionResult transaction = driver.SendRawTransaction(
      submitter.config, utxo, workload.source_address,
      workload.source_private_key, workload.destination_address,
      workload.amount_satoshis, workload.fee_satoshis,
      std::chrono::seconds(workload.timeout_sec), stop_token);
  WriteEvent(events_path, options.run_id, submitter.config.id,
             SimulationEventKind::kRawTransactionSubmitted,
             RawTransactionDetail(workload_index, workload_count, workload,
                                  start_height, target_height, funding_hashes,
                                  transaction));
  transaction_tracker.TrackAndWaitForVisibility(
      options, events_path, driver, nodes,
      TrackedTransaction{
          .txid = transaction.txid,
          .submission_kind = "raw_transaction_submitted",
          .workload_index = workload_index,
          .workload_count = workload_count,
          .transaction_index = 1U,
          .transaction_count = 1U,
          .txid_index = 1U,
          .submission_node = workload.submit_node,
      },
      std::chrono::seconds(workload.timeout_sec), stop_token);
}

void ApplyWalletTransactionsWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    const SimulationRegistry& registry,
    TransactionObservationTracker& transaction_tracker,
    const WalletTransactionsWorkload& workload, uint32_t workload_index,
    uint32_t workload_count, std::stop_token stop_token) {
  struct FundingState {
    uint32_t miner_node = 1;
    uint64_t start_height = 0;
    uint64_t target_height = 0;
    uint64_t ready_height = 0;
    uint64_t ready_balance_satoshis = 0;
    std::vector<std::string> hashes;
    ChainWalletFundingResult preparation;
    std::vector<std::string> preparation_hashes;
  };

  const std::vector<WalletIdentity>& wallets = registry.wallets();
  const std::vector<uint32_t> funding_miner_indexes =
      WalletFundingMinerNodes(registry.topology().miner_nodes, wallets.size(),
                              workload.funding_strategy, workload.random_seed);
  std::vector<FundingState> funding;
  funding.reserve(wallets.size());
  for (size_t wallet_index = 0; wallet_index < wallets.size(); ++wallet_index) {
    ThrowIfStopRequested(stop_token);
    const WalletIdentity& wallet = wallets[wallet_index];
    if (wallet.address.empty() || wallet.funding_address.empty()) {
      throw std::runtime_error(
          "wallet-backed workload requires initialized WalletNode addresses "
          "and funding addresses");
    }
    const uint32_t miner_node = funding_miner_indexes[wallet_index] + 1U;
    NodeRuntime& miner = nodes[miner_node - 1U];
    FundingState state;
    state.miner_node = miner_node;
    state.start_height = driver.ReadMetrics(miner.config, stop_token).height;
    state.hashes =
        driver.GenerateBlocks(miner.config, workload.funding_blocks_per_wallet,
                              wallet.funding_address, stop_token);
    RecordGeneratedBlocks(driver, miner, state.hashes, stop_token);
    if (state.start_height >
        std::numeric_limits<std::uint64_t>::max() - state.hashes.size()) {
      throw std::runtime_error("wallet funding target height overflow");
    }
    state.target_height =
        state.start_height + static_cast<uint64_t>(state.hashes.size());
    for (NodeRuntime& node : nodes) {
      driver.WaitForHeight(node.config, state.target_height,
                           std::chrono::seconds(workload.timeout_sec),
                           stop_token);
    }
    NodeRuntime& wallet_node = nodes[wallet.node - 1U];
    state.preparation = driver.PrepareWalletFunding(
        wallet_node.config, ToChainWalletMode(registry.wallet_initialization()),
        wallet.address, workload.funding_threshold_satoshis,
        workload.readiness_confirmations,
        std::chrono::seconds(workload.timeout_sec), stop_token);
    for (const std::string& txid : state.preparation.txids) {
      for (NodeRuntime& node : nodes) {
        driver.WaitForMempoolTransaction(
            node.config, txid, std::chrono::seconds(workload.timeout_sec),
            stop_token);
      }
    }
    state.ready_height = state.target_height;
    if (state.preparation.confirmation_blocks_required != 0U &&
        state.preparation.txids.empty()) {
      throw std::runtime_error(
          "wallet funding preparation requested confirmation blocks without "
          "transaction ids");
    }
    std::uint64_t preparation_block_count =
        state.preparation.confirmation_blocks_required;
    if (state.preparation.minimum_chain_height > state.ready_height) {
      preparation_block_count =
          std::max(preparation_block_count,
                   state.preparation.minimum_chain_height - state.ready_height);
    }
    if (preparation_block_count > std::numeric_limits<std::uint32_t>::max()) {
      throw std::runtime_error(
          "wallet funding preparation block count exceeds uint32");
    }
    if (preparation_block_count != 0U) {
      state.preparation_hashes = driver.GenerateBlocks(
          miner.config, static_cast<std::uint32_t>(preparation_block_count),
          wallet.funding_address, stop_token);
      if (state.preparation_hashes.size() != preparation_block_count) {
        throw std::runtime_error(
            "wallet funding preparation returned an unexpected block count");
      }
      RecordGeneratedBlocks(driver, miner, state.preparation_hashes,
                            stop_token);
      if (state.ready_height > std::numeric_limits<std::uint64_t>::max() -
                                   state.preparation_hashes.size()) {
        throw std::runtime_error("wallet funding ready height overflow");
      }
      state.ready_height +=
          static_cast<uint64_t>(state.preparation_hashes.size());
      for (NodeRuntime& node : nodes) {
        driver.WaitForHeight(node.config, state.ready_height,
                             std::chrono::seconds(workload.timeout_sec),
                             stop_token);
      }
    }
    if (state.ready_height < state.preparation.minimum_chain_height) {
      throw std::runtime_error(
          "wallet funding preparation did not reach its minimum chain height");
    }
    state.ready_balance_satoshis = driver.WaitForWalletBalance(
        wallet_node.config, ToChainWalletMode(registry.wallet_initialization()),
        workload.funding_threshold_satoshis, workload.readiness_confirmations,
        std::chrono::seconds(workload.timeout_sec), stop_token);
    WriteEvent(events_path, options.run_id, wallet_node.config.id,
               SimulationEventKind::kWalletFunded,
               WalletFundingDetail(
                   workload_index, workload_count, workload, wallet, miner_node,
                   state.start_height, state.target_height,
                   static_cast<uint64_t>(state.hashes.size()),
                   state.ready_height, state.preparation,
                   static_cast<uint64_t>(state.preparation_hashes.size()),
                   state.ready_balance_satoshis));
    funding.push_back(std::move(state));
  }

  const std::vector<WalletTransactionPlanEntry> transfer_plan =
      BuildWalletTransactionPlan(wallets.size(), workload);
  for (size_t transaction_index = 0; transaction_index < transfer_plan.size();
       ++transaction_index) {
    ThrowIfStopRequested(stop_token);
    const WalletTransactionPlanEntry& plan_entry =
        transfer_plan[transaction_index];
    if (plan_entry.interval_before != std::chrono::milliseconds(0)) {
      WaitForDuration(plan_entry.interval_before, stop_token);
    }
    const size_t sender_index = plan_entry.sender_index;
    const size_t receiver_index = plan_entry.receiver_index;
    const WalletIdentity& sender = wallets[sender_index];
    const WalletIdentity& receiver = wallets[receiver_index];
    const FundingState& sender_funding = funding[sender_index];
    NodeRuntime& sender_node = nodes[sender.node - 1U];

    const ChainWalletTransactionResult transaction =
        driver.SendWalletTransaction(
            sender_node.config,
            ToChainWalletMode(registry.wallet_initialization()),
            receiver.address, plan_entry.amount_satoshis, workload.fee_satoshis,
            std::chrono::seconds(workload.timeout_sec), stop_token);
    WriteEvent(
        events_path, options.run_id, sender_node.config.id,
        SimulationEventKind::kWalletTransactionSubmitted,
        WalletTransactionDetail(
            workload_index, workload_count, workload,
            static_cast<uint32_t>(transaction_index + 1U), sender, receiver,
            sender_funding.miner_node, sender_funding.start_height,
            sender_funding.target_height,
            static_cast<uint64_t>(sender_funding.hashes.size()),
            sender_funding.ready_height, sender_funding.preparation,
            static_cast<uint64_t>(sender_funding.preparation_hashes.size()),
            sender_funding.ready_balance_satoshis, plan_entry.amount_satoshis,
            plan_entry.interval_before, transaction));
    if (transaction.txids.size() > std::numeric_limits<std::uint32_t>::max()) {
      throw std::runtime_error("wallet transaction txid count exceeds uint32");
    }
    for (std::size_t txid_index = 0; txid_index < transaction.txids.size();
         ++txid_index) {
      transaction_tracker.TrackAndWaitForVisibility(
          options, events_path, driver, nodes,
          TrackedTransaction{
              .txid = transaction.txids[txid_index],
              .submission_kind = "wallet_transaction_submitted",
              .workload_index = workload_index,
              .workload_count = workload_count,
              .transaction_index =
                  static_cast<std::uint32_t>(transaction_index + 1U),
              .transaction_count = workload.transaction_count,
              .txid_index = static_cast<std::uint32_t>(txid_index + 1U),
              .submission_node = sender.node,
          },
          std::chrono::seconds(workload.timeout_sec), stop_token);
    }
  }
}

void RestoreNodeNetworkCondition(const NodeVethConfig& previous) {
  if (previous.apply_condition) {
    ReplaceNetworkConditionQdisc(previous.host_name, previous.condition);
    static_cast<void>(VerifyNodeNetworkCondition(previous));
    return;
  }
  std::exception_ptr delete_error;
  try {
    DeleteRootQdisc(previous.host_name);
  } catch (...) {
    delete_error = std::current_exception();
  }
  const QdiscInfo* qdisc =
      FindQdiscByInterfaceName(ListQdiscs(), previous.host_name);
  if (qdisc != nullptr &&
      (qdisc->kind == QdiscKind::kNetem || qdisc->kind == QdiscKind::kTbf ||
       qdisc->kind == QdiscKind::kTbfNetem)) {
    if (delete_error) {
      std::rethrow_exception(delete_error);
    }
    throw std::runtime_error(
        "network condition qdisc remained after rollback removal");
  }
}

QdiscInfo ReplaceNodeNetworkConditionTransactional(
    NodeRuntime* node, const NetworkCondition& condition,
    std::stop_token stop_token) {
  if (!node->network) {
    throw std::runtime_error(
        "runtime network condition requires isolated networking");
  }
  const NodeVethConfig previous = *node->network;
  NodeVethConfig updated = previous;
  updated.apply_condition = true;
  updated.condition = condition;
  try {
    ThrowIfStopRequested(stop_token);
    ReplaceNetworkConditionQdisc(updated.host_name, updated.condition);
    ThrowIfStopRequested(stop_token);
    const QdiscInfo qdisc = VerifyNodeNetworkCondition(updated);
    node->network = updated;
    node->network_profile.clear();
    return qdisc;
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::string rollback_error;
    try {
      RestoreNodeNetworkCondition(previous);
    } catch (const std::exception& restore_error) {
      rollback_error = restore_error.what();
      BBP_LOG(error) << "failed to restore network condition for "
                     << node->config.id << ": " << restore_error.what();
    } catch (...) {
      rollback_error = "unknown exception";
      BBP_LOG(error) << "failed to restore network condition for "
                     << node->config.id << ": unknown exception";
    }
    if (!rollback_error.empty()) {
      throw std::runtime_error("network condition update failed: " +
                               ExceptionMessage(original_error) +
                               "; rollback failed: " + rollback_error);
    }
    std::rethrow_exception(original_error);
  }
}

void ApplyRuntimeNetworkConditionUpdates(
    const Options& options, const std::filesystem::path& events_path,
    std::vector<NodeRuntime>& nodes, std::stop_token stop_token) {
  for (const auto& [node_index, condition] :
       options.runtime_node_network_conditions) {
    ThrowIfStopRequested(stop_token);
    if (node_index >= nodes.size()) {
      throw std::runtime_error(
          "runtime network condition node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    QdiscInfo qdisc;
    NodeVethConfig updated_network;
    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      qdisc = ReplaceNodeNetworkConditionTransactional(&node, condition,
                                                       stop_token);
      updated_network = *node.network;
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kNetworkConditionUpdated,
               NetworkConditionVerificationDetail(updated_network, qdisc));
  }
}

std::string NetworkProfileUpdateDetail(
    const ProfileSwitchWorkload& workload, uint32_t node,
    std::string_view previous_profile, const NodeVethConfig& previous,
    const NodeVethConfig& current, const QdiscInfo& qdisc,
    uint32_t workload_index, uint32_t workload_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["profile"] = workload.profile;
  if (previous_profile.empty()) {
    detail["previous_profile"] = nullptr;
  } else {
    detail["previous_profile"] = previous_profile;
  }
  detail["previous"] =
      previous.apply_condition
          ? boost::json::value(NetworkConditionJson(previous.condition))
          : boost::json::value(nullptr);
  detail["current"] = NetworkConditionJson(current.condition);
  detail["qdisc"] = QdiscJson(qdisc);
  detail["kernel_verified"] = true;
  return boost::json::serialize(detail);
}

void ApplyNetworkProfileSwitch(const Options& options,
                               const std::filesystem::path& events_path,
                               std::vector<NodeRuntime>& nodes,
                               const ProfileSwitchWorkload& workload,
                               uint32_t workload_index, uint32_t workload_count,
                               std::stop_token stop_token) {
  const NetworkCondition& desired =
      options.network_profiles.at(workload.profile);
  struct PreviousState {
    uint32_t node = 0U;
    NodeVethConfig network;
    std::string profile;
    NodeVethConfig current_network;
    QdiscInfo applied_qdisc{};
  };
  std::vector<PreviousState> previous_states;
  previous_states.reserve(workload.nodes.size());
  std::vector<std::size_t> attempted;

  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    for (const uint32_t one_based_node : workload.nodes) {
      if (one_based_node == 0U || one_based_node > nodes.size()) {
        throw std::runtime_error("network profile target node is out of range");
      }
      const NodeRuntime& runtime = nodes[one_based_node - 1U];
      if (!runtime.network) {
        throw std::runtime_error(
            "network profile update requires isolated networking");
      }
      previous_states.push_back(PreviousState{
          .node = one_based_node,
          .network = *runtime.network,
          .profile = runtime.network_profile,
          .current_network = {},
          .applied_qdisc = {},
      });
    }

    try {
      for (std::size_t index = 0; index < previous_states.size(); ++index) {
        ThrowIfStopRequested(stop_token);
        attempted.push_back(index);
        PreviousState& previous = previous_states[index];
        NodeVethConfig desired_network = previous.network;
        desired_network.apply_condition = true;
        desired_network.condition = desired;
        ReplaceNetworkConditionQdisc(desired_network.host_name, desired);
        ThrowIfStopRequested(stop_token);
        previous.applied_qdisc = VerifyNodeNetworkCondition(desired_network);
        previous.current_network = std::move(desired_network);
      }
    } catch (...) {
      const std::exception_ptr original_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      for (auto iter = attempted.rbegin(); iter != attempted.rend(); ++iter) {
        const PreviousState& previous = previous_states[*iter];
        try {
          RestoreNodeNetworkCondition(previous.network);
        } catch (const std::exception& error) {
          rollback_errors.push_back(nodes[previous.node - 1U].config.id + ": " +
                                    error.what());
        } catch (...) {
          rollback_errors.push_back(nodes[previous.node - 1U].config.id +
                                    ": unknown rollback error");
        }
      }
      WriteProfileRollbackFailureEventSafely(
          options, events_path, WorkloadKind::kSetNetworkProfile,
          workload.profile, original_error, rollback_errors);
      std::rethrow_exception(original_error);
    }

    for (PreviousState& previous : previous_states) {
      NodeRuntime& runtime = nodes[previous.node - 1U];
      runtime.network = previous.current_network;
      runtime.network_profile = workload.profile;
    }
  }

  for (const PreviousState& previous : previous_states) {
    const NodeRuntime& runtime = nodes[previous.node - 1U];
    WriteEvent(events_path, options.run_id, runtime.config.id,
               SimulationEventKind::kNetworkProfileUpdated,
               NetworkProfileUpdateDetail(
                   workload, previous.node, previous.profile, previous.network,
                   previous.current_network, previous.applied_qdisc,
                   workload_index, workload_count));
  }
}

bool NetworkBlockRulePresent(const NodeRuntime& node,
                             const NetworkBlockRule& rule) {
  if (!node.network) {
    return false;
  }
  const std::vector<TcFilterInfo> filters =
      ListTcFiltersForInterface(node.network->host_name);
  for (const TcFilterInfo& filter : filters) {
    if (TcFilterMatchesEgressIpv4TcpDrop(filter, node.network->host_name,
                                         rule.src_address, rule.dst_address,
                                         rule.dst_port, rule.handle)) {
      return true;
    }
  }
  return false;
}

std::optional<NetworkBlockRule> NetworkBlockRuleForHandle(
    const NodeRuntime& node, std::uint32_t handle) {
  if (!node.network) {
    return std::nullopt;
  }
  const std::vector<TcFilterInfo> filters =
      ListTcFiltersForInterface(node.network->host_name);
  for (const TcFilterInfo& filter : filters) {
    if (filter.handle != handle ||
        !TcFilterIsEgressIpv4TcpDropPolicy(filter, node.network->host_name)) {
      continue;
    }
    NetworkBlockRule rule;
    rule.src_address = filter.has_ipv4_src ? filter.ipv4_src : "";
    rule.dst_address = filter.ipv4_dst;
    rule.dst_port = filter.tcp_dst;
    rule.handle = filter.handle;
    return rule;
  }
  return std::nullopt;
}

void RequireNetworkBlockHandleAvailable(const NodeRuntime& node,
                                        const NetworkBlockRule& rule) {
  if (!node.network) {
    return;
  }
  const std::vector<TcFilterInfo> filters =
      ListTcFiltersForInterface(node.network->host_name);
  for (const TcFilterInfo& filter : filters) {
    if (filter.handle != rule.handle) {
      continue;
    }
    if (!TcFilterMatchesEgressIpv4TcpDrop(filter, node.network->host_name,
                                          rule.src_address, rule.dst_address,
                                          rule.dst_port, rule.handle)) {
      throw std::runtime_error(
          "network block rule handle is already used by different filter: " +
          std::to_string(rule.handle));
    }
  }
}

std::string NetworkBlockRuleDetail(
    const NodeRuntime& node, const NetworkBlockRule& rule, bool existed_before,
    bool present_after, std::uint32_t workload_index = 0,
    std::uint32_t workload_count = 0,
    std::optional<std::uint64_t> operator_sequence = std::nullopt) {
  boost::json::object detail = NetworkBlockRuleJson(rule);
  if (node.network) {
    detail["host_if"] = node.network->host_name;
  } else {
    detail["host_if"] = nullptr;
  }
  detail["existed_before"] = existed_before;
  detail["present_after"] = present_after;
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  return boost::json::serialize(detail);
}

void RequireNetworkBlockNode(const NodeRuntime& node) {
  if (!node.network) {
    throw std::runtime_error(
        "runtime network block rule requires isolated networking");
  }
}

struct NetworkBlockMutationResult {
  bool existed_before = false;
  bool present_after = false;
};

void RestoreNetworkBlockRule(const NodeRuntime& node,
                             const NetworkBlockRule& rule,
                             bool should_be_present) {
  RequireNetworkBlockHandleAvailable(node, rule);
  const bool present = NetworkBlockRulePresent(node, rule);
  if (should_be_present && !present) {
    ReplaceEgressIpv4TcpDropFilter(node.network->host_name, rule.src_address,
                                   rule.dst_address, rule.dst_port,
                                   rule.handle);
  } else if (!should_be_present && present) {
    DeleteEgressIpv4TcpDropFilter(node.network->host_name, rule.handle);
  }
  if (NetworkBlockRulePresent(node, rule) != should_be_present) {
    throw std::runtime_error(
        "network block rule rollback did not restore prior state");
  }
}

NetworkBlockMutationResult MutateNetworkBlockRuleTransactional(
    const NodeRuntime& node, const NetworkBlockRule& rule, bool remove,
    std::stop_token stop_token) {
  RequireNetworkBlockNode(node);
  ThrowIfStopRequested(stop_token);
  RequireNetworkBlockHandleAvailable(node, rule);
  const bool existed_before = NetworkBlockRulePresent(node, rule);
  try {
    if (remove) {
      if (existed_before) {
        DeleteEgressIpv4TcpDropFilter(node.network->host_name, rule.handle);
      }
    } else {
      ReplaceEgressIpv4TcpDropFilter(node.network->host_name, rule.src_address,
                                     rule.dst_address, rule.dst_port,
                                     rule.handle);
    }
    ThrowIfStopRequested(stop_token);
    const bool present_after = NetworkBlockRulePresent(node, rule);
    if (!remove && !present_after) {
      throw std::runtime_error(
          "runtime network block rule was not visible after apply");
    }
    if (remove && present_after) {
      throw std::runtime_error(
          "runtime network block rule remained after unblock");
    }
    return NetworkBlockMutationResult{
        .existed_before = existed_before,
        .present_after = present_after,
    };
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::string rollback_error;
    try {
      RestoreNetworkBlockRule(node, rule, existed_before);
    } catch (const std::exception& error) {
      rollback_error = error.what();
      BBP_LOG(error) << "failed to restore network block rule " << rule.handle
                     << " on " << node.config.id << ": " << error.what();
    } catch (...) {
      rollback_error = "unknown exception";
      BBP_LOG(error) << "failed to restore network block rule " << rule.handle
                     << " on " << node.config.id << ": unknown exception";
    }
    if (!rollback_error.empty()) {
      throw std::runtime_error(
          "network block mutation failed: " + ExceptionMessage(original_error) +
          "; rollback failed: " + rollback_error);
    }
    std::rethrow_exception(original_error);
  }
}

void ApplyRuntimeNetworkBlockRules(const Options& options,
                                   const std::filesystem::path& events_path,
                                   std::vector<NodeRuntime>& nodes,
                                   std::stop_token stop_token) {
  for (const NetworkBlockRule& rule : options.runtime_node_blocks) {
    ThrowIfStopRequested(stop_token);
    if (rule.node_index >= nodes.size()) {
      throw std::runtime_error("runtime network block node is out of range");
    }
    NodeRuntime& node = nodes[rule.node_index];
    NetworkBlockMutationResult result;
    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      result =
          MutateNetworkBlockRuleTransactional(node, rule, false, stop_token);
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kNetworkBlockApplied,
               NetworkBlockRuleDetail(node, rule, result.existed_before,
                                      result.present_after));
  }
}

void ApplyRuntimeNetworkUnblockRules(const Options& options,
                                     const std::filesystem::path& events_path,
                                     std::vector<NodeRuntime>& nodes,
                                     std::stop_token stop_token) {
  for (const NetworkBlockRule& rule : options.runtime_node_unblocks) {
    ThrowIfStopRequested(stop_token);
    if (rule.node_index >= nodes.size()) {
      throw std::runtime_error("runtime network unblock node is out of range");
    }
    NodeRuntime& node = nodes[rule.node_index];
    NetworkBlockMutationResult result;
    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      result =
          MutateNetworkBlockRuleTransactional(node, rule, true, stop_token);
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kNetworkBlockRemoved,
               NetworkBlockRuleDetail(node, rule, result.existed_before,
                                      result.present_after));
  }
}

NetworkBlockRule MakeP2pBlockRule(uint32_t src_node_index,
                                  uint32_t dst_node_index,
                                  const std::vector<NodeRuntime>& nodes) {
  if (src_node_index >= nodes.size() || dst_node_index >= nodes.size()) {
    throw std::runtime_error("partition node is out of range");
  }
  NetworkBlockRule rule;
  rule.node_index = dst_node_index;
  if (!nodes[src_node_index].network || !nodes[dst_node_index].network) {
    throw std::runtime_error(
        "partition nodes do not have isolated network addresses");
  }
  rule.src_address = nodes[src_node_index].network->node_address;
  rule.dst_address = nodes[dst_node_index].network->node_address;
  rule.dst_port = nodes[dst_node_index].config.p2p_port;
  rule.handle = StableRuleHandle(rule);
  return rule;
}

std::vector<NetworkBlockRule> PartitionBlockRules(
    const NetworkPartitionRule& partition,
    const std::vector<NodeRuntime>& nodes) {
  std::vector<NetworkBlockRule> rules;
  rules.reserve((partition.group_a.size() * partition.group_b.size()) * 2U);
  for (uint32_t node_a : partition.group_a) {
    for (uint32_t node_b : partition.group_b) {
      rules.push_back(MakeP2pBlockRule(node_a, node_b, nodes));
      rules.push_back(MakeP2pBlockRule(node_b, node_a, nodes));
    }
  }
  return rules;
}

boost::json::object PartitionRuleResultJson(const NodeRuntime& node,
                                            const NetworkBlockRule& rule,
                                            bool existed_before,
                                            bool present_after) {
  boost::json::object object = NetworkBlockRuleJson(rule);
  object["node_id"] = node.config.id;
  if (node.network) {
    object["host_if"] = node.network->host_name;
  } else {
    object["host_if"] = nullptr;
  }
  object["existed_before"] = existed_before;
  object["present_after"] = present_after;
  return object;
}

std::string NetworkPartitionDetail(
    const NetworkPartitionRule& partition,
    const boost::json::array& rule_results, uint32_t workload_index = 0,
    uint32_t workload_count = 0,
    std::optional<std::uint64_t> operator_sequence = std::nullopt) {
  boost::json::object detail = NetworkPartitionRuleJson(partition);
  if (workload_index != 0U) {
    detail["workload_index"] = workload_index;
  }
  if (workload_count != 0U) {
    detail["workload_count"] = workload_count;
  }
  detail["rules"] = rule_results;
  detail["scope"] = "source_aware_group";
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  return boost::json::serialize(detail);
}

void ApplyRuntimeNetworkPartition(
    const Options& options, const std::filesystem::path& events_path,
    std::vector<NodeRuntime>& nodes, const NetworkPartitionRule& partition,
    bool heal, uint32_t workload_index = 0, uint32_t workload_count = 0,
    std::stop_token stop_token = {},
    std::optional<std::uint64_t> operator_sequence = std::nullopt) {
  struct PartitionRuleState {
    NetworkBlockRule rule;
    bool existed_before = false;
    bool present_after = false;
  };
  const std::vector<NetworkBlockRule> rules =
      PartitionBlockRules(partition, nodes);
  std::vector<PartitionRuleState> states;
  states.reserve(rules.size());
  boost::json::array rule_results;
  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    std::set<std::pair<std::uint32_t, std::uint32_t>> planned_handles;
    for (const NetworkBlockRule& rule : rules) {
      ThrowIfStopRequested(stop_token);
      NodeRuntime& node = nodes[rule.node_index];
      RequireNetworkBlockNode(node);
      if (!planned_handles.emplace(rule.node_index, rule.handle).second) {
        throw std::runtime_error(
            "network partition produced a duplicate rule handle: " +
            std::to_string(rule.handle));
      }
      RequireNetworkBlockHandleAvailable(node, rule);
      states.push_back(PartitionRuleState{
          .rule = rule,
          .existed_before = NetworkBlockRulePresent(node, rule),
          .present_after = false,
      });
    }

    std::vector<std::size_t> attempted;
    try {
      for (std::size_t index = 0; index < states.size(); ++index) {
        ThrowIfStopRequested(stop_token);
        attempted.push_back(index);
        PartitionRuleState& state = states[index];
        NodeRuntime& node = nodes[state.rule.node_index];
        if (heal) {
          if (state.existed_before) {
            DeleteEgressIpv4TcpDropFilter(node.network->host_name,
                                          state.rule.handle);
          }
        } else {
          ReplaceEgressIpv4TcpDropFilter(
              node.network->host_name, state.rule.src_address,
              state.rule.dst_address, state.rule.dst_port, state.rule.handle);
        }
        ThrowIfStopRequested(stop_token);
        state.present_after = NetworkBlockRulePresent(node, state.rule);
        if (!heal && !state.present_after) {
          throw std::runtime_error(
              "runtime network partition rule was not visible after apply");
        }
        if (heal && state.present_after) {
          throw std::runtime_error(
              "runtime network partition rule remained after heal");
        }
      }
    } catch (...) {
      const std::exception_ptr original_error = std::current_exception();
      std::vector<std::string> rollback_errors;
      for (auto iter = attempted.rbegin(); iter != attempted.rend(); ++iter) {
        const PartitionRuleState& state = states[*iter];
        try {
          RestoreNetworkBlockRule(nodes[state.rule.node_index], state.rule,
                                  state.existed_before);
        } catch (const std::exception& error) {
          rollback_errors.push_back(std::to_string(state.rule.handle) + " on " +
                                    nodes[state.rule.node_index].config.id +
                                    ": " + error.what());
          BBP_LOG(error) << "failed to roll back partition rule "
                         << state.rule.handle << " on "
                         << nodes[state.rule.node_index].config.id << ": "
                         << error.what();
        } catch (...) {
          rollback_errors.push_back(std::to_string(state.rule.handle) + " on " +
                                    nodes[state.rule.node_index].config.id +
                                    ": unknown exception");
          BBP_LOG(error) << "failed to roll back partition rule "
                         << state.rule.handle << " on "
                         << nodes[state.rule.node_index].config.id
                         << ": unknown exception";
        }
      }
      if (!rollback_errors.empty()) {
        std::string message = "network partition mutation failed: " +
                              ExceptionMessage(original_error) +
                              "; rollback failed:";
        for (const std::string& rollback_error : rollback_errors) {
          message += " [" + rollback_error + "]";
        }
        throw std::runtime_error(message);
      }
      std::rethrow_exception(original_error);
    }
  }

  for (const PartitionRuleState& state : states) {
    const NodeRuntime& node = nodes[state.rule.node_index];
    rule_results.push_back(PartitionRuleResultJson(
        node, state.rule, state.existed_before, state.present_after));
  }

  const SimulationEventKind event_kind =
      heal ? SimulationEventKind::kNetworkPartitionHealed
           : SimulationEventKind::kNetworkPartitionApplied;
  WriteEvent(events_path, options.run_id, "sim", event_kind,
             NetworkPartitionDetail(partition, rule_results, workload_index,
                                    workload_count, operator_sequence));
}

void ApplyRuntimeNetworkPartitions(const Options& options,
                                   const std::filesystem::path& events_path,
                                   std::vector<NodeRuntime>& nodes,
                                   std::stop_token stop_token) {
  for (const NetworkPartitionRule& partition : options.runtime_partitions) {
    ThrowIfStopRequested(stop_token);
    ApplyRuntimeNetworkPartition(options, events_path, nodes, partition, false,
                                 0U, 0U, stop_token);
  }
}

void ApplyRuntimeNetworkPartitionHeals(const Options& options,
                                       const std::filesystem::path& events_path,
                                       std::vector<NodeRuntime>& nodes,
                                       std::stop_token stop_token) {
  for (const NetworkPartitionRule& partition :
       options.runtime_partition_heals) {
    ThrowIfStopRequested(stop_token);
    ApplyRuntimeNetworkPartition(options, events_path, nodes, partition, true,
                                 0U, 0U, stop_token);
  }
}

std::string RestartDetail(pid_t pid, uint64_t restart_count) {
  boost::json::object detail;
  detail["pid"] = pid;
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

std::string RestartPeerConnectionDetail(std::string_view peer_address,
                                        std::uint64_t restart_count) {
  boost::json::object detail;
  detail["reason"] = "restart_topology_restore";
  detail["peer_address"] = std::string(peer_address);
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

bool WaitForNodeFrozenState(const Cgroup& cgroup, bool expected,
                            std::stop_token stop_token);
void SetNodeFrozen(const Options& options,
                   const std::filesystem::path& events_path, NodeRuntime& node,
                   bool frozen, std::stop_token stop_token);

void RestartNode(const Options& options,
                 const std::filesystem::path& events_path,
                 const ChainDriver& driver, NodeRuntime& node,
                 std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node restart requires a node cgroup");
  }

  WriteNodeState(events_path, options.run_id, node.config.id,
                 NodeRuntimeLifecycle::kRestarting);
  node.SetLifecycle(NodeRuntimeLifecycle::kRestarting);
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kRestartRequested,
             "restart_count=" + std::to_string(node.RestartCount() + 1U));
  ResetNodePerfCounters(node);
  if (node.cgroup->Frozen()) {
    SetNodeFrozen(options, events_path, node, false, stop_token);
  }
  if (node.process.running()) {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kRpcStop);
    try {
      driver.Stop(node.config, stop_token);
    } catch (...) {
      if (node.process.running()) {
        AttachNodePerfCounters(node);
        node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
        WriteNodeState(events_path, options.run_id, node.config.id,
                       NodeRuntimeLifecycle::kRunning);
      }
      throw;
    }
  } else {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kRpcStopSkipped, "process is not running");
  }
  const auto exit_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (node.process.running() &&
         std::chrono::steady_clock::now() < exit_deadline) {
    WaitForDuration(std::chrono::milliseconds(50), stop_token);
  }
  if (node.process.running()) {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kSigterm);
    node.process.Terminate(std::chrono::seconds(5));
  }
  ThrowIfStopRequested(stop_token);
  ProcessSpec process = driver.RenderProcess(node.config);
  if (node.network_namespace) {
    process.network_namespace_fd = node.network_namespace->fd();
  }
  WriteNodeState(events_path, options.run_id, node.config.id,
                 NodeRuntimeLifecycle::kStarting);
  node.process = ChildProcess::Spawn(process, node.cgroup->path());
  node.process_started_at = std::chrono::steady_clock::now();
  const std::uint64_t restart_count = node.IncrementRestartCount();
  AttachNodePerfCounters(node);
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kProcessRestarted,
             RestartDetail(node.process.pid(), restart_count));
  driver.WaitReady(node.config, std::chrono::seconds(options.ready_timeout_sec),
                   stop_token);
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kRpcReady);
  for (const std::string& peer : node.config.connect_peers) {
    ThrowIfStopRequested(stop_token);
    driver.ConnectPeer(node.config, peer, stop_token);
    driver.WaitForPeerAddress(node.config, peer,
                              std::chrono::seconds(options.ready_timeout_sec),
                              stop_token);
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kPeerConnected,
               RestartPeerConnectionDetail(peer, restart_count));
  }
  node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
  WriteNodeState(events_path, options.run_id, node.config.id,
                 NodeRuntimeLifecycle::kRunning);
}

void ApplyRuntimeNodeRestarts(const Options& options,
                              const std::filesystem::path& events_path,
                              const ChainDriver& driver,
                              std::vector<NodeRuntime>& nodes,
                              std::stop_token stop_token) {
  for (uint32_t node_index : options.runtime_node_restarts) {
    ThrowIfStopRequested(stop_token);
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime restart node is out of range");
    }
    RestartNode(options, events_path, driver, nodes[node_index], stop_token);
  }
}

bool WaitForNodeFrozenState(const Cgroup& cgroup, bool expected,
                            std::stop_token stop_token) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    ThrowIfStopRequested(stop_token);
    if (cgroup.Frozen() == expected) {
      return true;
    }
    WaitForDuration(std::chrono::milliseconds(20), stop_token);
  }
  return false;
}

std::string PersistentFreezeDetail(bool frozen) {
  boost::json::object detail;
  detail["frozen"] = frozen;
  detail["persistent"] = true;
  return boost::json::serialize(detail);
}

void SetNodeFrozen(const Options& options,
                   const std::filesystem::path& events_path, NodeRuntime& node,
                   bool frozen, std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node freeze control requires a node cgroup");
  }
  if (frozen) {
    node.cgroup->Freeze();
  } else {
    node.cgroup->Thaw();
  }
  if (!WaitForNodeFrozenState(*node.cgroup, frozen, stop_token)) {
    throw std::runtime_error("node cgroup did not report " +
                             std::string(frozen ? "frozen: " : "thawed: ") +
                             node.config.id);
  }
  WriteEvent(events_path, options.run_id, node.config.id,
             frozen ? SimulationEventKind::kCgroupFrozen
                    : SimulationEventKind::kCgroupThawed,
             PersistentFreezeDetail(frozen));
}

void StopNodeProcess(const Options& options,
                     const std::filesystem::path& events_path,
                     const ChainDriver& driver, NodeRuntime& node,
                     std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.process.running()) {
    throw std::runtime_error("node process is not running: " + node.config.id);
  }
  if (node.cgroup && node.cgroup->Frozen()) {
    SetNodeFrozen(options, events_path, node, false, stop_token);
  }
  ResetNodePerfCounters(node);
  node.SetLifecycle(NodeRuntimeLifecycle::kStopping);
  WriteNodeState(events_path, options.run_id, node.config.id,
                 NodeRuntimeLifecycle::kStopping);
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kRpcStop);
  try {
    driver.Stop(node.config, stop_token);
    const auto exit_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (node.process.running() &&
           std::chrono::steady_clock::now() < exit_deadline) {
      WaitForDuration(std::chrono::milliseconds(50), stop_token);
    }
    if (node.process.running()) {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kSigterm);
      node.process.Terminate(std::chrono::seconds(5));
    }
    if (node.process.running()) {
      throw std::runtime_error("node process survived graceful stop: " +
                               node.config.id);
    }
  } catch (...) {
    if (node.process.running()) {
      AttachNodePerfCounters(node);
      node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
      WriteNodeState(events_path, options.run_id, node.config.id,
                     NodeRuntimeLifecycle::kRunning);
    }
    throw;
  }
  node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
  WriteNodeState(events_path, options.run_id, node.config.id,
                 NodeRuntimeLifecycle::kStopped);
}

std::string FreezeDetail(uint32_t duration_ms, bool frozen) {
  boost::json::object detail;
  detail["duration_ms"] = duration_ms;
  detail["frozen"] = frozen;
  return boost::json::serialize(detail);
}

void FreezeNodeForDuration(const Options& options,
                           const std::filesystem::path& events_path,
                           NodeRuntime& node, uint32_t duration_ms,
                           std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node freeze requires a node cgroup");
  }

  node.cgroup->Freeze();
  try {
    if (!WaitForNodeFrozenState(*node.cgroup, true, stop_token)) {
      throw std::runtime_error("node cgroup did not report frozen: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kCgroupFrozen,
               FreezeDetail(duration_ms, true));
    WaitForDuration(std::chrono::milliseconds(duration_ms), stop_token);
    node.cgroup->Thaw();
    if (!WaitForNodeFrozenState(*node.cgroup, false, stop_token)) {
      throw std::runtime_error("node cgroup did not report thawed: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kCgroupThawed,
               FreezeDetail(duration_ms, false));
  } catch (...) {
    try {
      node.cgroup->Thaw();
    } catch (const std::exception&) {
    }
    throw;
  }
}

void ApplyRuntimeNodeFreezes(const Options& options,
                             const std::filesystem::path& events_path,
                             std::vector<NodeRuntime>& nodes,
                             std::stop_token stop_token) {
  for (const FreezeRequest& freeze : options.runtime_node_freezes) {
    ThrowIfStopRequested(stop_token);
    if (freeze.node_index >= nodes.size()) {
      throw std::runtime_error("runtime freeze node is out of range");
    }
    FreezeNodeForDuration(options, events_path, nodes[freeze.node_index],
                          freeze.duration_ms, stop_token);
  }
}

template <typename Action>
void RunNodeCleanupStep(bool best_effort, std::string_view description,
                        Action&& action) {
  try {
    action();
  } catch (const std::exception& error) {
    if (!best_effort) {
      throw;
    }
    BBP_LOG(error) << description
                   << " failed during node cleanup: " << error.what();
  } catch (...) {
    if (!best_effort) {
      throw;
    }
    BBP_LOG(error) << description << " failed during node cleanup";
  }
}

void StopNodes(const Options& options, const std::filesystem::path& events_path,
               const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
               bool best_effort = false) {
  const auto shutdown_timeout =
      best_effort ? std::chrono::seconds(2) : std::chrono::seconds(15);
  const auto shutdown_deadline =
      std::chrono::steady_clock::now() + shutdown_timeout;
  std::exception_ptr first_failure;
  const auto record_failure = [&](std::string_view description,
                                  const std::exception_ptr& failure) {
    if (!failure) {
      return;
    }
    if (!first_failure) {
      first_failure = failure;
    }
    if (best_effort) {
      RunNodeCleanupStep(true, description,
                         [&] { std::rethrow_exception(failure); });
    }
  };

  for (auto& node : nodes) {
    ResetNodePerfCounters(node);
    RunNodeCleanupStep(best_effort, "stopping state event", [&] {
      WriteNodeState(events_path, options.run_id, node.config.id,
                     NodeRuntimeLifecycle::kStopping);
    });
  }

  std::stop_source rpc_stop_source;
  std::vector<std::exception_ptr> rpc_failures(nodes.size());
  std::vector<std::size_t> running_processes;
  running_processes.reserve(nodes.size());
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    if (nodes[index].process.running()) {
      running_processes.push_back(index);
      RunNodeCleanupStep(best_effort, "RPC stop event", [&] {
        WriteEvent(events_path, options.run_id, nodes[index].config.id,
                   SimulationEventKind::kRpcStop);
      });
    } else {
      RunNodeCleanupStep(best_effort, "RPC stop skipped event", [&] {
        WriteEvent(events_path, options.run_id, nodes[index].config.id,
                   SimulationEventKind::kRpcStopSkipped,
                   "process is not running");
      });
    }
  }
  std::atomic<std::size_t> completed_rpc_stops = 0U;
  std::vector<std::jthread> rpc_workers;
  rpc_workers.reserve(running_processes.size());
  for (const std::size_t index : running_processes) {
    rpc_workers.emplace_back([&, index] {
      try {
        driver.Stop(nodes[index].config, rpc_stop_source.get_token());
      } catch (...) {
        rpc_failures[index] = std::current_exception();
      }
      completed_rpc_stops.fetch_add(1U, std::memory_order_release);
    });
  }
  while (completed_rpc_stops.load(std::memory_order_acquire) <
             running_processes.size() &&
         std::chrono::steady_clock::now() < shutdown_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  rpc_stop_source.request_stop();
  rpc_workers.clear();
  for (const std::exception_ptr& failure : rpc_failures) {
    record_failure("node RPC stop", failure);
  }

  bool process_running = true;
  while (process_running &&
         std::chrono::steady_clock::now() < shutdown_deadline) {
    process_running = false;
    for (auto& node : nodes) {
      process_running = node.process.running() || process_running;
    }
    if (process_running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  running_processes.clear();
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    if (!nodes[index].process.running()) {
      continue;
    }
    running_processes.push_back(index);
    RunNodeCleanupStep(best_effort, "SIGTERM event", [&] {
      WriteEvent(events_path, options.run_id, nodes[index].config.id,
                 SimulationEventKind::kSigterm);
    });
  }
  std::vector<std::exception_ptr> termination_failures(nodes.size());
  std::vector<std::jthread> termination_workers;
  termination_workers.reserve(running_processes.size());
  for (const std::size_t index : running_processes) {
    termination_workers.emplace_back([&, index] {
      try {
        nodes[index].process.Terminate(std::chrono::seconds(1));
      } catch (...) {
        termination_failures[index] = std::current_exception();
      }
    });
  }
  termination_workers.clear();
  for (const std::exception_ptr& failure : termination_failures) {
    record_failure("node process termination", failure);
  }

  for (auto& node : nodes) {
    RunNodeCleanupStep(best_effort, "stopped state event", [&] {
      WriteNodeState(events_path, options.run_id, node.config.id,
                     NodeRuntimeLifecycle::kStopped);
    });
    RunNodeCleanupStep(best_effort, "cleaning state event", [&] {
      WriteNodeState(events_path, options.run_id, node.config.id,
                     NodeRuntimeLifecycle::kCleaning);
    });
    if (node.cgroup) {
      RunNodeCleanupStep(best_effort, "cgroup process kill",
                         [&] { node.cgroup->KillAll(); });
      if (options.cleanup_policy != CleanupPolicy::kRetainCgroups) {
        if (best_effort) {
          RunNodeCleanupStep(true, "cgroup removal",
                             [&] { node.cgroup->Remove(); });
        } else {
          try {
            node.cgroup->Remove();
          } catch (const std::exception& error) {
            WriteEvent(events_path, options.run_id, node.config.id,
                       SimulationEventKind::kCgroupRemoveFailed, error.what());
          }
        }
      }
    }
    if (node.network) {
      RunNodeCleanupStep(best_effort, "node network removal",
                         [&] { DeleteNodeVethNetwork(*node.network); });
      RunNodeCleanupStep(best_effort, "network removal event", [&] {
        WriteEvent(events_path, options.run_id, node.config.id,
                   SimulationEventKind::kNetworkRemoved);
      });
    }
    if (node.network_namespace) {
      RunNodeCleanupStep(best_effort, "network namespace stop",
                         [&] { node.network_namespace->Stop(); });
    }
    RunNodeCleanupStep(best_effort, "cleaned state event", [&] {
      WriteNodeState(events_path, options.run_id, node.config.id,
                     NodeRuntimeLifecycle::kCleaned);
    });
  }
  if (options.cleanup_policy != CleanupPolicy::kRetainCgroups) {
    if (best_effort) {
      RunNodeCleanupStep(true, "run cgroup removal",
                         [&] { Cgroup::RemoveRun(options.run_id); });
    } else {
      try {
        Cgroup::RemoveRun(options.run_id);
      } catch (const std::exception& error) {
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kRunCgroupRemoveFailed, error.what());
      }
    }
  }
  if (!best_effort && first_failure) {
    std::rethrow_exception(first_failure);
  }
}

std::filesystem::path BenchmarkRunRoot(const Options& options) {
  return std::filesystem::absolute(options.output_dir) / options.run_id;
}

std::vector<std::uint32_t> ConfiguredMinerIndexes(const Options& options) {
  if (options.topology.configured) {
    return options.topology.miner_nodes;
  }
  return {options.generate_node - 1U};
}

NodeRuntime& FindNodeRuntimeById(std::vector<NodeRuntime>& nodes,
                                 const std::string& node_id) {
  const auto node = std::find_if(nodes.begin(), nodes.end(),
                                 [&node_id](const NodeRuntime& candidate) {
                                   return candidate.config.id == node_id;
                                 });
  if (node == nodes.end()) {
    throw std::runtime_error("unknown block producer node: " + node_id);
  }
  return *node;
}

bool IsCanonicalWalletPerfTargetId(std::string_view id) {
  constexpr std::string_view kPrefix = "wallet-";
  if (!id.starts_with(kPrefix)) {
    return false;
  }
  id.remove_prefix(kPrefix.size());
  if (id.empty() || (id.size() > 1U && id.front() == '0')) {
    return false;
  }
  std::uint64_t value = 0U;
  const auto [end, error] =
      std::from_chars(id.data(), id.data() + id.size(), value);
  return error == std::errc() && end == id.data() + id.size() && value > 0U;
}

struct NodePerfCounterSnapshot {
  explicit NodePerfCounterSnapshot(NodeRuntime& runtime,
                                   const PerfCounterTarget& target,
                                   const std::vector<PerfCounterKind>& kinds)
      : node(&runtime),
        configuration_kinds(kinds),
        configuration_id(target.id) {}

  NodeRuntime* node;
  std::vector<PerfCounterKind> configuration_kinds;
  PerfCounterTargetKind configuration_kind = PerfCounterTargetKind::kNode;
  std::string configuration_id;
  std::optional<ProcessPerfCounters> process_counters;
  std::optional<CgroupPerfCounters> cgroup_counters;
  pid_t target_pid = -1;
  pid_t attached_pid = -1;
  std::uint64_t process_generation = 0U;
  std::filesystem::path cgroup_path;
  std::vector<int> cpus;
  std::optional<PerfCounterErrorKind> error_kind;
  std::string error;
  bool replacement_started = false;
};

void BeginNodePerfCounterReplacement(NodePerfCounterSnapshot& snapshot,
                                     PerfCounterTargetKind target_kind) {
  NodeRuntime& node = *snapshot.node;
  snapshot.configuration_kind = node.perf_counter_target_kind;
  node.perf_counter_kinds.swap(snapshot.configuration_kinds);
  node.perf_counter_target_id.swap(snapshot.configuration_id);
  node.perf_counter_target_kind = target_kind;
  if (node.process_perf_counters) {
    snapshot.process_counters.emplace(std::move(*node.process_perf_counters));
    node.process_perf_counters.reset();
  }
  if (node.cgroup_perf_counters) {
    snapshot.cgroup_counters.emplace(std::move(*node.cgroup_perf_counters));
    node.cgroup_perf_counters.reset();
  }
  snapshot.target_pid = node.perf_counter_target_pid;
  snapshot.attached_pid = node.perf_counter_attached_pid;
  snapshot.process_generation = node.perf_counter_process_generation;
  snapshot.cgroup_path = std::move(node.perf_counter_cgroup_path);
  snapshot.cpus = std::move(node.perf_counter_cpus);
  snapshot.error_kind = node.perf_counter_error_kind;
  snapshot.error = std::move(node.perf_counter_error);
  snapshot.replacement_started = true;
}

void RestoreNodePerfCounterSnapshot(NodePerfCounterSnapshot& snapshot) {
  if (!snapshot.replacement_started) {
    return;
  }
  NodeRuntime& node = *snapshot.node;
  node.process_perf_counters.reset();
  node.cgroup_perf_counters.reset();
  node.perf_counter_kinds.swap(snapshot.configuration_kinds);
  node.perf_counter_target_kind = snapshot.configuration_kind;
  node.perf_counter_target_id.swap(snapshot.configuration_id);
  node.process_perf_counters = std::move(snapshot.process_counters);
  node.cgroup_perf_counters = std::move(snapshot.cgroup_counters);
  node.perf_counter_target_pid = snapshot.target_pid;
  node.perf_counter_attached_pid = snapshot.attached_pid;
  node.perf_counter_process_generation = snapshot.process_generation;
  node.perf_counter_cgroup_path = std::move(snapshot.cgroup_path);
  node.perf_counter_cpus = std::move(snapshot.cpus);
  node.perf_counter_error_kind = snapshot.error_kind;
  node.perf_counter_error = std::move(snapshot.error);
}

void RequireNodePerfCounterAttachment(const NodeRuntime& node) {
  if (node.perf_counter_error_kind || !node.perf_counter_error.empty()) {
    throw std::runtime_error(
        "perf counter attachment failed for " + node.config.id + ": " +
        (node.perf_counter_error.empty() ? std::string(PerfCounterErrorKindName(
                                               *node.perf_counter_error_kind))
                                         : node.perf_counter_error));
  }
  const bool process_target =
      node.perf_counter_target_kind == PerfCounterTargetKind::kNode ||
      node.perf_counter_target_kind == PerfCounterTargetKind::kWallet;
  if (process_target) {
    if (!node.process_perf_counters || node.cgroup_perf_counters ||
        node.perf_counter_target_pid <= 0 ||
        node.perf_counter_attached_pid != node.perf_counter_target_pid) {
      throw std::runtime_error(
          "process perf counter attachment is incomplete "
          "for " +
          node.config.id);
    }
    return;
  }
  if (!node.cgroup_perf_counters || node.process_perf_counters ||
      node.perf_counter_cgroup_path.empty() || node.perf_counter_cpus.empty()) {
    throw std::runtime_error(
        "cgroup perf counter attachment is incomplete for " + node.config.id);
  }
}

void ApplyPerfCounterCommand(const SimulationCommand& command,
                             std::vector<NodeRuntime>& nodes) {
  if (!command.perf_counter_target) {
    throw std::runtime_error("perf counter command requires a typed target");
  }
  const PerfCounterTarget& target = *command.perf_counter_target;
  if (target.id.empty()) {
    throw std::runtime_error("perf counter target id must not be empty");
  }
  if (target.node_ids.empty()) {
    throw std::runtime_error("perf counter target must resolve to a node");
  }
  if (command.perf_counter_kinds.empty()) {
    throw std::runtime_error("perf counter selection must not be empty");
  }
  const std::set<PerfCounterKind> unique_kinds(
      command.perf_counter_kinds.begin(), command.perf_counter_kinds.end());
  if (unique_kinds.size() != command.perf_counter_kinds.size()) {
    throw std::runtime_error("perf counter selection contains duplicates");
  }
  if (target.kind == PerfCounterTargetKind::kGroup) {
    if (command.node_id != "sim") {
      throw std::runtime_error("group perf counter command must target sim");
    }
  } else {
    if (target.node_ids.size() != 1U ||
        command.node_id != target.node_ids.front()) {
      throw std::runtime_error(
          "non-group perf counter command must target its resolved node");
    }
    if ((target.kind == PerfCounterTargetKind::kNode ||
         target.kind == PerfCounterTargetKind::kCgroup) &&
        target.id != target.node_ids.front()) {
      throw std::runtime_error(
          "node and cgroup perf target ids must equal their resolved node");
    }
    if (target.kind == PerfCounterTargetKind::kWallet &&
        !IsCanonicalWalletPerfTargetId(target.id)) {
      throw std::runtime_error(
          "wallet perf target id must be wallet-<positive-index>");
    }
  }

  std::vector<NodeRuntime*> target_nodes;
  target_nodes.reserve(target.node_ids.size());
  std::set<std::string> unique_nodes;
  for (const std::string& node_id : target.node_ids) {
    if (node_id.empty() || !unique_nodes.insert(node_id).second) {
      throw std::runtime_error(
          "perf counter target contains an empty or duplicate node id");
    }
    const auto node = std::find_if(nodes.begin(), nodes.end(),
                                   [&](const NodeRuntime& candidate) {
                                     return candidate.config.id == node_id;
                                   });
    if (node == nodes.end()) {
      throw std::runtime_error("perf counter target references unknown node: " +
                               node_id);
    }
    if (!node->process.running()) {
      throw std::runtime_error("perf counter target node is not running: " +
                               node_id);
    }
    if ((target.kind == PerfCounterTargetKind::kGroup ||
         target.kind == PerfCounterTargetKind::kCgroup) &&
        !node->cgroup) {
      throw std::runtime_error(
          "perf counter target node has no owned cgroup: " + node_id);
    }
    target_nodes.push_back(&*node);
  }

  std::vector<NodePerfCounterSnapshot> snapshots;
  snapshots.reserve(target_nodes.size());
  for (NodeRuntime* node : target_nodes) {
    snapshots.emplace_back(*node, target, command.perf_counter_kinds);
  }

  try {
    for (NodePerfCounterSnapshot& snapshot : snapshots) {
      BeginNodePerfCounterReplacement(snapshot, target.kind);
      AttachNodePerfCounters(*snapshot.node);
      RequireNodePerfCounterAttachment(*snapshot.node);
    }
  } catch (...) {
    for (NodePerfCounterSnapshot& snapshot : snapshots) {
      RestoreNodePerfCounterSnapshot(snapshot);
    }
    throw;
  }
}

std::map<std::string, PeerCountPolicy> InitialPeerCountPolicies(
    const Options& options, const std::vector<NodeRuntime>& nodes) {
  std::map<std::string, PeerCountPolicy> policies;
  for (const PeerConnectivityPolicy& policy :
       options.topology.peer_connectivity) {
    if (policy.node >= nodes.size()) {
      throw std::runtime_error(
          "peer connectivity policy references an unknown node");
    }
    policies.emplace(nodes[policy.node].config.id, policy.peer_count);
  }
  return policies;
}

PeerConnectivityController::AllowedPeerMap InitialAllowedPeers(
    const RuntimePeerTopology& topology,
    const std::vector<NodeRuntime>& nodes) {
  PeerConnectivityController::AllowedPeerMap allowed;
  for (std::uint32_t node_index = 0; node_index < nodes.size(); ++node_index) {
    std::vector<std::string> peer_ids;
    for (const std::uint32_t peer_index :
         topology.ActivePeerIndexes(node_index)) {
      if (peer_index >= nodes.size()) {
        throw std::runtime_error(
            "logical topology peer references an unknown running node");
      }
      peer_ids.push_back(nodes[peer_index].config.id);
    }
    allowed.emplace(nodes[node_index].config.id, std::move(peer_ids));
  }
  return allowed;
}

std::string ScheduledBlockDetail(const std::vector<std::string>& hashes) {
  boost::json::object detail;
  boost::json::array block_hashes;
  block_hashes.reserve(hashes.size());
  for (const std::string& hash : hashes) {
    block_hashes.emplace_back(hash);
  }
  detail["hashes"] = std::move(block_hashes);
  return boost::json::serialize(detail);
}

std::filesystem::path NodeReportRelativePath(const SimulationCommand& command) {
  return std::filesystem::path("node-reports") /
         (command.node_id + "-" + std::to_string(command.sequence) + ".json");
}

void ExportNodeReport(const std::filesystem::path& run_root,
                      const SimulationCommand& command) {
  const std::filesystem::path relative = NodeReportRelativePath(command);
  const std::filesystem::path output = run_root / relative;
  EnsureDirectory(output.parent_path());
  std::filesystem::path temporary = output;
  temporary += ".tmp";
  std::error_code ec;
  std::filesystem::remove(temporary, ec);
  ec.clear();
  try {
    WriteText(temporary,
              BuildNodeReportJson(run_root, command.node_id, command.sequence) +
                  "\n");
    std::filesystem::rename(temporary, output, ec);
    if (ec) {
      throw std::runtime_error("rename node report failed: " + ec.message());
    }
  } catch (...) {
    std::filesystem::remove(temporary, ec);
    throw;
  }
}

std::string SimulationCommandDetail(const SimulationCommand& command,
                                    std::string_view error = {}) {
  boost::json::object detail;
  detail["sequence"] = command.sequence;
  detail["kind"] = SimulationCommandKindName(command.kind);
  if (command.block_production_policy) {
    detail["period_ms"] = command.block_production_policy->period().count();
    detail["probability"] = command.block_production_policy->probability();
    detail["seed"] = command.block_production_policy->seed();
  }
  if (command.mining_difficulty) {
    detail["difficulty"] = command.mining_difficulty->value();
  }
  if (command.peer_node_id) {
    detail["peer_node_id"] = *command.peer_node_id;
  }
  if (command.peer_count_policy) {
    detail["minimum_peer_count"] = command.peer_count_policy->minimum();
    detail["maximum_peer_count"] = command.peer_count_policy->maximum();
  }
  if (command.block_count) {
    detail["block_count"] = *command.block_count;
  }
  if (command.profile) {
    detail["profile"] = *command.profile;
  }
  if (command.resource_limit_patch) {
    detail["resource_limits"] =
        ResourceLimitPatchJson(*command.resource_limit_patch);
  }
  if (command.network_condition) {
    detail["network_condition"] =
        NetworkConditionJson(*command.network_condition);
  }
  if (command.network_flow) {
    boost::json::object flow;
    if (!command.network_flow->src_address.empty()) {
      flow["src_address"] = command.network_flow->src_address;
    }
    if (!command.network_flow->dst_address.empty()) {
      flow["dst_address"] = command.network_flow->dst_address;
    }
    if (command.network_flow->dst_port != 0U) {
      flow["dst_port"] = command.network_flow->dst_port;
    }
    if (command.network_flow->handle != 0U) {
      flow["handle"] = command.network_flow->handle;
    }
    detail["network_flow"] = std::move(flow);
  }
  if (command.perf_counter_target) {
    boost::json::object target;
    target["kind"] =
        PerfCounterTargetKindName(command.perf_counter_target->kind);
    target["id"] = command.perf_counter_target->id;
    boost::json::array node_ids;
    node_ids.reserve(command.perf_counter_target->node_ids.size());
    for (const std::string& node_id : command.perf_counter_target->node_ids) {
      node_ids.emplace_back(node_id);
    }
    target["node_ids"] = std::move(node_ids);
    detail["perf_target"] = std::move(target);
  }
  if (!command.perf_counter_kinds.empty()) {
    detail["perf_counters"] = PerfCounterNamesJson(command.perf_counter_kinds);
  }
  if (command.kind == SimulationCommandKind::kExportNodeReport) {
    detail["output_path"] = NodeReportRelativePath(command).generic_string();
  }
  detail["confirmed"] = command.confirmed;
  if (!error.empty()) {
    detail["error"] = error;
  }
  return boost::json::serialize(detail);
}

int RunBenchmarkHeadless(Options options, SimulationCommandQueue* command_queue,
                         std::stop_token external_stop_token = {}) {
  std::stop_source simulation_stop_source;
  std::stop_callback stop_simulation_on_external_request(
      external_stop_token,
      [&simulation_stop_source] { simulation_stop_source.request_stop(); });
  const std::stop_token stop_token = simulation_stop_source.get_token();
  const auto run_root = BenchmarkRunRoot(options);
  if (std::filesystem::exists(run_root)) {
    if (!options.replace_run) {
      throw std::runtime_error(
          "run directory already exists: " + run_root.string() +
          " (use --replace-run to remove it)");
    }
    RequireOwnedRunDirectory(run_root);
    std::error_code ec;
    std::filesystem::remove_all(run_root, ec);
    if (ec) {
      throw std::runtime_error("remove existing run directory failed: " +
                               ec.message());
    }
    Cgroup::RemoveRun(options.run_id);
  }
  EnsureDirectory(run_root);
  AttachRunLogFile(run_root);
  BBP_LOG(info) << "starting run " << options.run_id;
  WriteText(run_root / kRunMarkerFile, "bbp run\n");
  EnsureDirectory(run_root / "nodes");
  std::unique_ptr<NetworkAllocationLock> network_allocation_lock;
  if (options.isolate_network) {
    network_allocation_lock = std::make_unique<NetworkAllocationLock>();
    RequireRunNetworkInterfacesAvailable(options);
    options.network_address_plan = SimulationNetworkAddressPlan::Allocate(
        options.run_id, options.nodes, ListIpv4Routes());
  }
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  RuntimePeerTopology runtime_topology(options.topology.peer_topology,
                                       options.nodes);
  SimulationRegistry simulation_registry = SimulationRegistry::FromTopology(
      options.topology, options.wallet_initialization);
  WriteScenarioFiles(options, run_root, chain_spec);

  const auto events_path = run_root / "events.jsonl";
  const auto metrics_path = run_root / "metrics.jsonl";
  const auto wallet_metrics_path = run_root / "wallet-metrics.jsonl";
  WriteEvent(events_path, options.run_id, "sim",
             SimulationEventKind::kRunStarted);

  std::unique_ptr<ChainDriver> driver_owner = CreateChainDriver(options.chain);
  ChainDriver& driver = *driver_owner;
  std::vector<NodeRuntime> nodes;
  std::unique_ptr<NodeLogCollector> log_collector;
  std::unique_ptr<PeriodicMetricsCollector> metrics_collector;
  std::unique_ptr<ProbabilisticBlockScheduler> block_scheduler;
  std::unique_ptr<PeerConnectivityController> peer_connectivity_controller;
  std::unique_ptr<ChainCommandExecutor> chain_command_executor;
  std::unique_ptr<SimulationCommandProcessor> command_processor;
  std::optional<std::jthread> duration_timer;
  std::optional<std::chrono::steady_clock::time_point> simulation_epoch;
  std::atomic<bool> simulation_duration_reached = false;
  std::atomic<std::uint64_t> duration_stop_requested_at_ms = 0U;
  std::vector<std::string> active_native_miner_ids;
  std::set<std::string> paused_scheduled_miners;
  std::mutex node_process_mutex;
  std::stop_source command_rpc_stop_source;
  std::stop_source block_production_rpc_stop_source;
  std::stop_source metrics_rpc_stop_source;
  std::atomic<bool> wallets_initialized = false;
  std::stop_callback cancel_command_rpc(stop_token, [&command_rpc_stop_source] {
    command_rpc_stop_source.request_stop();
  });
  std::stop_callback cancel_block_production_rpc(
      stop_token, [&block_production_rpc_stop_source] {
        block_production_rpc_stop_source.request_stop();
      });
  std::stop_callback cancel_metrics_rpc(stop_token, [&metrics_rpc_stop_source] {
    metrics_rpc_stop_source.request_stop();
  });
  const auto stop_duration_timer = [&]() {
    if (duration_timer) {
      duration_timer->request_stop();
      if (duration_timer->joinable()) {
        duration_timer->join();
      }
      duration_timer.reset();
    }
  };
  const auto stop_command_processor = [&]() {
    command_rpc_stop_source.request_stop();
    if (command_processor) {
      command_processor->Stop();
    } else if (command_queue != nullptr) {
      command_queue->Cancel();
    }
  };
  const auto stop_peer_connectivity = [&]() {
    if (peer_connectivity_controller) {
      peer_connectivity_controller->Stop();
    }
  };
  const auto stop_block_production = [&]() {
    block_production_rpc_stop_source.request_stop();
    if (block_scheduler) {
      block_scheduler->Stop();
    }
    std::exception_ptr first_failure;
    for (const std::string& node_id : active_native_miner_ids) {
      try {
        driver.StopMining(FindNodeRuntimeById(nodes, node_id).config);
      } catch (...) {
        if (!first_failure) {
          first_failure = std::current_exception();
        }
      }
    }
    active_native_miner_ids.clear();
    if (first_failure) {
      std::rethrow_exception(first_failure);
    }
  };
  const auto cleanup_step = [](std::string_view component, auto&& action) {
    try {
      action();
    } catch (const std::exception& error) {
      BBP_LOG(error) << component
                     << " failed during run cleanup: " << error.what();
    } catch (...) {
      BBP_LOG(error) << component << " failed during run cleanup";
    }
  };
  const auto handle_run_failure = [&](std::string_view detail) {
    stop_duration_timer();
    cleanup_step("command processor shutdown", stop_command_processor);
    cleanup_step("peer connectivity shutdown", stop_peer_connectivity);
    cleanup_step("block production shutdown", stop_block_production);
    cleanup_step("metrics collector shutdown", [&] {
      if (metrics_collector) {
        metrics_rpc_stop_source.request_stop();
        metrics_collector->Stop();
      }
    });
    for (auto& node : nodes) {
      cleanup_step("failed node state event", [&] {
        WriteNodeState(events_path, options.run_id, node.config.id,
                       NodeRuntimeLifecycle::kFailed);
      });
    }
    cleanup_step("run failure event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunFailed, detail);
    });
    if (!log_collector) {
      cleanup_step("node log tail collection", [&] {
        WriteNodeLogTails(events_path, options, driver, nodes);
      });
    }
    cleanup_step("node shutdown",
                 [&] { StopNodes(options, events_path, driver, nodes, true); });
    for (auto& node : nodes) {
      cleanup_step("node process fallback shutdown", [&] {
        if (node.process.running()) {
          node.process.Terminate(std::chrono::seconds(1));
        }
      });
    }
    if (log_collector) {
      cleanup_step("node log collector shutdown",
                   [&] { log_collector->Stop(); });
    } else {
      cleanup_step("final node log tail collection", [&] {
        WriteNodeLogTails(events_path, options, driver, nodes);
      });
    }
  };
  const auto handle_run_cancellation = [&]() {
    stop_duration_timer();
    cleanup_step("command processor shutdown", stop_command_processor);
    cleanup_step("peer connectivity shutdown", stop_peer_connectivity);
    cleanup_step("block production shutdown", stop_block_production);
    cleanup_step("metrics collector shutdown", [&] {
      if (metrics_collector) {
        metrics_rpc_stop_source.request_stop();
        metrics_collector->Stop();
      }
    });
    cleanup_step("run cancellation event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunCancelled);
    });
    cleanup_step("node shutdown",
                 [&] { StopNodes(options, events_path, driver, nodes, true); });
    cleanup_step("node log collector shutdown", [&] {
      if (log_collector) {
        log_collector->Stop();
      } else {
        WriteNodeLogTails(events_path, options, driver, nodes);
      }
    });
    cleanup_step("run finished event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunFinished);
    });
    BBP_LOG(info) << "cancelled run " << options.run_id;
  };
  const auto handle_simulation_duration = [&]() {
    stop_duration_timer();
    cleanup_step("command processor shutdown", stop_command_processor);
    cleanup_step("peer connectivity shutdown", stop_peer_connectivity);
    cleanup_step("block production shutdown", stop_block_production);
    cleanup_step("metrics collector shutdown", [&] {
      if (metrics_collector) {
        metrics_rpc_stop_source.request_stop();
        metrics_collector->Stop();
      }
    });
    cleanup_step("simulation duration event", [&] {
      boost::json::object detail;
      detail["duration_ms"] = options.simulation_duration->count();
      detail["wall_duration_ms"] =
          options.time_scale.WallDuration(*options.simulation_duration).count();
      detail["time_scale"] = options.time_scale.value();
      detail["stop_requested_at_ms"] =
          duration_stop_requested_at_ms.load(std::memory_order_acquire);
      detail["elapsed_wall_ms"] =
          simulation_epoch
              ? ElapsedMilliseconds(*simulation_epoch,
                                    std::chrono::steady_clock::now())
              : 0U;
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kSimulationDurationReached,
                 boost::json::serialize(detail));
    });
    cleanup_step("node shutdown",
                 [&] { StopNodes(options, events_path, driver, nodes, true); });
    cleanup_step("node log collector shutdown", [&] {
      if (log_collector) {
        log_collector->Stop();
      } else {
        WriteNodeLogTails(events_path, options, driver, nodes);
      }
    });
    cleanup_step("run finished event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunFinished);
    });
    BBP_LOG(info) << "simulation duration reached for run " << options.run_id;
  };
  try {
    StartNodes(options, run_root, events_path, chain_spec, driver,
               runtime_topology, nodes, stop_token);
    network_allocation_lock.reset();
    const std::vector<std::uint32_t> miner_indexes =
        ConfiguredMinerIndexes(options);
    if (options.block_production.enabled && miner_indexes.empty()) {
      throw std::runtime_error(
          "enabled block production requires at least one configured miner");
    }
    std::vector<std::string> miner_node_ids;
    miner_node_ids.reserve(miner_indexes.size());
    for (const std::uint32_t miner_index : miner_indexes) {
      if (miner_index >= nodes.size()) {
        throw std::runtime_error(
            "configured miner index exceeds the running node count");
      }
      miner_node_ids.push_back(nodes[miner_index].config.id);
      if (options.block_production.enabled &&
          options.block_production.difficulty) {
        driver.SetMiningDifficulty(nodes[miner_index].config,
                                   *options.block_production.difficulty,
                                   stop_token);
      }
    }
    if (options.block_production.enabled &&
        options.block_production.mode ==
            MiningMode::kScheduledBlockProduction &&
        !miner_node_ids.empty()) {
      block_scheduler = std::make_unique<ProbabilisticBlockScheduler>(
          miner_node_ids, options.block_production.policy,
          [&](const std::string& node_id) {
            std::lock_guard<std::mutex> process_lock(node_process_mutex);
            NodeRuntime& miner = FindNodeRuntimeById(nodes, node_id);
            const std::vector<std::string> hashes = driver.GenerateBlocks(
                miner.config, 1U, chain_spec.default_reward_address,
                block_production_rpc_stop_source.get_token());
            RecordGeneratedBlocks(driver, miner, hashes,
                                  block_production_rpc_stop_source.get_token());
            WriteEvent(events_path, options.run_id, node_id,
                       SimulationEventKind::kScheduledBlockProduced,
                       ScheduledBlockDetail(hashes));
          },
          [&](const std::string& node_id, std::string_view error) {
            if (block_production_rpc_stop_source.stop_requested()) {
              return;
            }
            WriteEvent(events_path, options.run_id, node_id,
                       SimulationEventKind::kScheduledBlockFailed, error);
            BBP_LOG(warning) << "scheduled block production failed for "
                             << node_id << ": " << error;
          });
    }
    std::vector<ChainNodeConfig> log_nodes;
    log_nodes.reserve(nodes.size());
    for (const NodeRuntime& node : nodes) {
      log_nodes.push_back(node.config);
    }
    peer_connectivity_controller = std::make_unique<PeerConnectivityController>(
        driver, log_nodes, InitialPeerCountPolicies(options, nodes),
        InitialAllowedPeers(runtime_topology, nodes), options.metrics_interval,
        [&](std::string_view node_id) {
          return FindNodeRuntimeById(nodes, std::string(node_id))
              .AllowsChainMetrics();
        },
        [&](std::string_view node_id, std::string_view peer_node_id,
            PeerConnectivityAction action, const PeerCountPolicy& policy) {
          boost::json::object detail;
          detail["peer_node_id"] = peer_node_id;
          detail["minimum_peer_count"] = policy.minimum();
          detail["maximum_peer_count"] = policy.maximum();
          const SimulationEventKind event_kind =
              action == PeerConnectivityAction::kConnected
                  ? SimulationEventKind::kPeerPolicyConnected
                  : SimulationEventKind::kPeerPolicyDisconnected;
          WriteEvent(events_path, options.run_id, std::string(node_id),
                     event_kind, boost::json::serialize(detail));
          BBP_LOG(info) << SimulationEventKindName(event_kind) << " " << node_id
                        << " -> " << peer_node_id;
        },
        [&](std::string_view node_id, std::string_view error) {
          WriteEvent(events_path, options.run_id, std::string(node_id),
                     SimulationEventKind::kPeerPolicyEnforcementFailed, error);
          BBP_LOG(warning) << "peer policy enforcement failed for " << node_id
                           << ": " << error;
        });
    if (command_queue != nullptr) {
      chain_command_executor = std::make_unique<ChainCommandExecutor>(
          driver, log_nodes,
          [&](const ChainNodeConfig& config,
              std::stop_token command_stop_token) {
            if (std::find(miner_node_ids.begin(), miner_node_ids.end(),
                          config.id) == miner_node_ids.end()) {
              throw std::runtime_error("node is not a configured miner: " +
                                       config.id);
            }
            if (options.block_production.mode ==
                MiningMode::kScheduledBlockProduction) {
              if (!block_scheduler) {
                throw std::runtime_error(
                    "scheduled block production is not active");
              }
              block_scheduler->StopMiner(config.id);
            } else {
              driver.StopMining(config, command_stop_token);
              active_native_miner_ids.erase(
                  std::remove(active_native_miner_ids.begin(),
                              active_native_miner_ids.end(), config.id),
                  active_native_miner_ids.end());
            }
          },
          [&](BlockProductionPolicy policy) {
            if (!block_scheduler) {
              throw UnsupportedChainOperation(
                  "active mining mode",
                  "probabilistic block production policy adjustment");
            }
            block_scheduler->UpdatePolicy(policy);
          },
          [&](const ChainNodeConfig& config, MiningDifficulty difficulty,
              std::stop_token command_stop_token) {
            if (std::find(miner_node_ids.begin(), miner_node_ids.end(),
                          config.id) == miner_node_ids.end()) {
              throw std::runtime_error("node is not a configured miner: " +
                                       config.id);
            }
            driver.SetMiningDifficulty(config, difficulty, command_stop_token);
          },
          [&](const ChainNodeConfig& config, const ChainNodeConfig& peer,
              std::stop_token command_stop_token) {
            peer_connectivity_controller->ConnectPeer(config.id, peer.id,
                                                      std::chrono::seconds(10),
                                                      command_stop_token);
          },
          [&](const ChainNodeConfig& config, const ChainNodeConfig& peer,
              std::stop_token command_stop_token) {
            peer_connectivity_controller->DisconnectPeer(
                config.id, peer.id, std::chrono::seconds(10),
                command_stop_token);
          },
          [&](const ChainNodeConfig& config, PeerCountPolicy policy) {
            peer_connectivity_controller->SetPolicy(config.id, policy);
          });
      command_processor = std::make_unique<SimulationCommandProcessor>(
          *command_queue,
          [&](const SimulationCommand& command) {
            WriteEvent(events_path, options.run_id, command.node_id,
                       SimulationEventKind::kOperatorCommandStarted,
                       SimulationCommandDetail(command));
            const bool scheduled_miner =
                block_scheduler &&
                std::find(miner_node_ids.begin(), miner_node_ids.end(),
                          command.node_id) != miner_node_ids.end();
            const auto stop_scheduled_miner = [&] {
              return scheduled_miner
                         ? block_scheduler->StopMiner(command.node_id)
                         : false;
            };
            NodeRuntime& node =
                command.node_id == "sim"
                    ? nodes.front()
                    : FindNodeRuntimeById(nodes, command.node_id);
            if (command.kind == SimulationCommandKind::kExportNodeReport) {
              ExportNodeReport(run_root, command);
            } else if (command.kind ==
                       SimulationCommandKind::kSetPerfCounters) {
              std::lock_guard<std::mutex> lock(node_process_mutex);
              ApplyPerfCounterCommand(command, nodes);
            } else if (command.kind ==
                       SimulationCommandKind::kSetResourceLimits) {
              if (!command.resource_limit_patch) {
                throw std::runtime_error("resource limit patch is missing");
              }
              ApplyResourceLimitUpdate(options, events_path, node,
                                       *command.resource_limit_patch,
                                       std::nullopt, std::nullopt, std::nullopt,
                                       command.sequence, true);
            } else if (command.kind == SimulationCommandKind::kKillNode) {
              const bool resume_on_failure =
                  stop_scheduled_miner() ||
                  paused_scheduled_miners.contains(command.node_id);
              try {
                std::lock_guard<std::mutex> lock(node_process_mutex);
                if (!node.process.running()) {
                  throw std::runtime_error("node process is not running: " +
                                           command.node_id);
                }
                if (node.cgroup && node.cgroup->Frozen()) {
                  SetNodeFrozen(options, events_path, node, false,
                                command_rpc_stop_source.get_token());
                }
                const pid_t pid = node.process.pid();
                node.SetLifecycle(NodeRuntimeLifecycle::kKilling);
                WriteNodeState(events_path, options.run_id, command.node_id,
                               NodeRuntimeLifecycle::kKilling);
                WriteEvent(events_path, options.run_id, command.node_id,
                           SimulationEventKind::kProcessKillRequested,
                           "pid=" + std::to_string(pid));
                try {
                  node.process.Kill();
                  if (node.process.running()) {
                    throw std::runtime_error("node process survived SIGKILL: " +
                                             command.node_id);
                  }
                } catch (...) {
                  if (node.process.running()) {
                    node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
                    WriteNodeState(events_path, options.run_id, command.node_id,
                                   NodeRuntimeLifecycle::kRunning);
                  }
                  throw;
                }
                node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
                WriteEvent(events_path, options.run_id, command.node_id,
                           SimulationEventKind::kProcessKilled,
                           "pid=" + std::to_string(pid));
                WriteNodeState(events_path, options.run_id, command.node_id,
                               NodeRuntimeLifecycle::kKilled);
              } catch (...) {
                if (resume_on_failure && node.process.running()) {
                  block_scheduler->StartMiner(command.node_id);
                  paused_scheduled_miners.erase(command.node_id);
                }
                throw;
              }
              paused_scheduled_miners.erase(command.node_id);
              active_native_miner_ids.erase(
                  std::remove(active_native_miner_ids.begin(),
                              active_native_miner_ids.end(), command.node_id),
                  active_native_miner_ids.end());
            } else if (command.kind == SimulationCommandKind::kStopNode) {
              const bool resume_on_failure =
                  stop_scheduled_miner() ||
                  paused_scheduled_miners.contains(command.node_id);
              try {
                std::lock_guard<std::mutex> lock(node_process_mutex);
                StopNodeProcess(options, events_path, driver, node,
                                command_rpc_stop_source.get_token());
              } catch (...) {
                if (resume_on_failure && node.process.running()) {
                  block_scheduler->StartMiner(command.node_id);
                  paused_scheduled_miners.erase(command.node_id);
                }
                throw;
              }
              paused_scheduled_miners.erase(command.node_id);
              active_native_miner_ids.erase(
                  std::remove(active_native_miner_ids.begin(),
                              active_native_miner_ids.end(), command.node_id),
                  active_native_miner_ids.end());
            } else if (command.kind == SimulationCommandKind::kRestartNode) {
              const bool resume_scheduled_miner =
                  stop_scheduled_miner() ||
                  paused_scheduled_miners.contains(command.node_id);
              if (resume_scheduled_miner) {
                paused_scheduled_miners.insert(command.node_id);
              }
              const bool resume_native_miner =
                  std::find(active_native_miner_ids.begin(),
                            active_native_miner_ids.end(),
                            command.node_id) != active_native_miner_ids.end();
              try {
                std::lock_guard<std::mutex> lock(node_process_mutex);
                RestartNode(options, events_path, driver, node,
                            command_rpc_stop_source.get_token());
                if (resume_native_miner) {
                  driver.StartMining(node.config,
                                     chain_spec.default_reward_address,
                                     command_rpc_stop_source.get_token());
                }
              } catch (...) {
                if (resume_scheduled_miner && node.process.running()) {
                  block_scheduler->StartMiner(command.node_id);
                  paused_scheduled_miners.erase(command.node_id);
                }
                throw;
              }
              if (resume_scheduled_miner) {
                block_scheduler->StartMiner(command.node_id);
                paused_scheduled_miners.erase(command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kFreezeNode) {
              const bool resume_on_thaw = stop_scheduled_miner();
              try {
                std::lock_guard<std::mutex> lock(node_process_mutex);
                SetNodeFrozen(options, events_path, node, true,
                              command_rpc_stop_source.get_token());
              } catch (...) {
                if (resume_on_thaw) {
                  block_scheduler->StartMiner(command.node_id);
                }
                throw;
              }
              if (resume_on_thaw) {
                paused_scheduled_miners.insert(command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kThawNode) {
              {
                std::lock_guard<std::mutex> lock(node_process_mutex);
                SetNodeFrozen(options, events_path, node, false,
                              command_rpc_stop_source.get_token());
              }
              if (paused_scheduled_miners.erase(command.node_id) != 0U) {
                block_scheduler->StartMiner(command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kGenerateBlocks) {
              if (!command.block_count || *command.block_count == 0U) {
                throw std::runtime_error(
                    "generate-blocks command requires a positive count");
              }
              std::lock_guard<std::mutex> lock(node_process_mutex);
              const std::uint64_t start_height =
                  driver
                      .ReadMetrics(node.config,
                                   command_rpc_stop_source.get_token())
                      .height;
              const std::vector<std::string> hashes =
                  driver.GenerateBlocks(node.config, *command.block_count,
                                        chain_spec.default_reward_address,
                                        command_rpc_stop_source.get_token());
              RecordGeneratedBlocks(driver, node, hashes,
                                    command_rpc_stop_source.get_token());
              if (start_height >
                  std::numeric_limits<std::uint64_t>::max() - hashes.size()) {
                throw std::runtime_error(
                    "generated block target height overflows uint64");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              const std::uint32_t one_based_node =
                  static_cast<std::uint32_t>(
                      std::distance(nodes.begin(), node_iter)) +
                  1U;
              WriteEvent(
                  events_path, options.run_id, command.node_id,
                  SimulationEventKind::kGeneratedBlocks,
                  GeneratedBlocksDetail(
                      0U, 0U, one_based_node, start_height,
                      start_height + static_cast<std::uint64_t>(hashes.size()),
                      hashes, chain_spec.default_reward_address,
                      command.sequence));
            } else if (command.kind ==
                       SimulationCommandKind::kSetNetworkCondition) {
              if (!command.network_condition) {
                throw std::runtime_error(
                    "set-network-condition command requires a condition");
              }
              QdiscInfo qdisc;
              NodeVethConfig updated_network;
              {
                std::lock_guard<std::mutex> lock(node_network_state_mutex);
                qdisc = ReplaceNodeNetworkConditionTransactional(
                    &node, *command.network_condition,
                    command_rpc_stop_source.get_token());
                updated_network = *node.network;
              }
              WriteEvent(events_path, options.run_id, command.node_id,
                         SimulationEventKind::kNetworkConditionUpdated,
                         NetworkConditionVerificationDetail(
                             updated_network, qdisc, 0U, 0U, command.sequence));
            } else if (command.kind ==
                           SimulationCommandKind::kBlockNetworkFlow ||
                       command.kind ==
                           SimulationCommandKind::kUnblockNetworkFlow) {
              if (!command.network_flow) {
                throw std::runtime_error(
                    "network flow command requires a typed flow");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              if (node_iter == nodes.end()) {
                throw std::runtime_error("unknown network flow node: " +
                                         command.node_id);
              }
              const std::size_t zero_based_node = static_cast<std::size_t>(
                  std::distance(nodes.begin(), node_iter));
              if (zero_based_node > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error(
                    "network flow node index exceeds uint32");
              }
              NetworkBlockRule rule;
              NetworkBlockMutationResult result;
              {
                std::lock_guard<std::mutex> lock(node_network_state_mutex);
                if (command.network_flow->dst_address.empty()) {
                  if (command.kind !=
                          SimulationCommandKind::kUnblockNetworkFlow ||
                      command.network_flow->handle == 0U) {
                    throw std::runtime_error(
                        "handle-only network flow command must be unblock");
                  }
                  const std::optional<NetworkBlockRule> existing =
                      NetworkBlockRuleForHandle(node,
                                                command.network_flow->handle);
                  if (!existing) {
                    throw std::runtime_error(
                        "active network block rule handle was not found: " +
                        std::to_string(command.network_flow->handle));
                  }
                  rule = *existing;
                } else {
                  rule.src_address = command.network_flow->src_address;
                  rule.dst_address = command.network_flow->dst_address;
                  rule.dst_port = command.network_flow->dst_port;
                  rule.handle = command.network_flow->handle;
                }
                rule.node_index = static_cast<std::uint32_t>(zero_based_node);
                if (rule.handle == 0U) {
                  rule.handle = StableRuleHandle(rule);
                }
                result = MutateNetworkBlockRuleTransactional(
                    node, rule,
                    command.kind == SimulationCommandKind::kUnblockNetworkFlow,
                    command_rpc_stop_source.get_token());
              }
              WriteEvent(
                  events_path, options.run_id, command.node_id,
                  command.kind == SimulationCommandKind::kUnblockNetworkFlow
                      ? SimulationEventKind::kNetworkBlockRemoved
                      : SimulationEventKind::kNetworkBlockApplied,
                  NetworkBlockRuleDetail(node, rule, result.existed_before,
                                         result.present_after, 0U, 0U,
                                         command.sequence));
            } else if (command.kind == SimulationCommandKind::kPartitionNodes ||
                       command.kind == SimulationCommandKind::kHealPartition) {
              if (!command.peer_node_id) {
                throw std::runtime_error(
                    "partition command requires a peer node id");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              const auto peer_iter = std::find_if(
                  nodes.begin(), nodes.end(),
                  [&](const NodeRuntime& candidate) {
                    return candidate.config.id == *command.peer_node_id;
                  });
              if (node_iter == nodes.end() || peer_iter == nodes.end()) {
                throw std::runtime_error(
                    "partition command references an unknown node");
              }
              const std::size_t node_index = static_cast<std::size_t>(
                  std::distance(nodes.begin(), node_iter));
              const std::size_t peer_index = static_cast<std::size_t>(
                  std::distance(nodes.begin(), peer_iter));
              if (node_index > std::numeric_limits<std::uint32_t>::max() ||
                  peer_index > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error(
                    "partition command node index exceeds uint32");
              }
              const NetworkPartitionRule partition{
                  .group_a = {static_cast<std::uint32_t>(node_index)},
                  .group_b = {static_cast<std::uint32_t>(peer_index)},
              };
              ApplyRuntimeNetworkPartition(
                  options, events_path, nodes, partition,
                  command.kind == SimulationCommandKind::kHealPartition, 0U, 0U,
                  command_rpc_stop_source.get_token(), command.sequence);
            } else if (command.kind ==
                           SimulationCommandKind::kSetResourceProfile ||
                       command.kind ==
                           SimulationCommandKind::kSetNetworkProfile) {
              if (!command.profile || command.profile->empty()) {
                throw std::runtime_error(
                    "profile command requires a profile name");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              const std::uint32_t one_based_node =
                  static_cast<std::uint32_t>(
                      std::distance(nodes.begin(), node_iter)) +
                  1U;
              const ProfileSwitchWorkload workload{
                  .nodes = {one_based_node},
                  .node_ids = {command.node_id},
                  .profile = *command.profile,
              };
              if (command.kind == SimulationCommandKind::kSetResourceProfile) {
                if (!options.resource_profiles.contains(*command.profile)) {
                  throw std::runtime_error("unknown resource profile: " +
                                           *command.profile);
                }
                ApplyResourceProfileSwitch(options, events_path, nodes,
                                           workload, 0U, 0U,
                                           command_rpc_stop_source.get_token());
              } else {
                if (!options.network_profiles.contains(*command.profile)) {
                  throw std::runtime_error("unknown network profile: " +
                                           *command.profile);
                }
                ApplyNetworkProfileSwitch(options, events_path, nodes, workload,
                                          0U, 0U,
                                          command_rpc_stop_source.get_token());
              }
            } else {
              chain_command_executor->Execute(
                  command, command_rpc_stop_source.get_token());
            }
            WriteEvent(events_path, options.run_id, command.node_id,
                       SimulationEventKind::kOperatorCommandCompleted,
                       SimulationCommandDetail(command));
            BBP_LOG(info) << "command #" << command.sequence << " "
                          << SimulationCommandKindName(command.kind) << " for "
                          << command.node_id << " completed";
          },
          [&](const SimulationCommand& command, std::string_view error) {
            if (command_rpc_stop_source.stop_requested()) {
              return;
            }
            WriteEvent(events_path, options.run_id, command.node_id,
                       SimulationEventKind::kOperatorCommandFailed,
                       SimulationCommandDetail(command, error));
            BBP_LOG(warning)
                << "command #" << command.sequence << " "
                << SimulationCommandKindName(command.kind) << " for "
                << command.node_id << " failed: " << error;
          });
    }
    log_collector = std::make_unique<NodeLogCollector>(
        driver, std::move(log_nodes), options.metrics_interval,
        kMaxLogTailBytes,
        [&](const ChainNodeConfig& config, ChainLogSource source,
            const LogTailChunk& chunk) {
          WriteLogTailChunkEvent(events_path, options, config, source, chunk);
        });
    log_collector->Start();
    metrics_collector = std::make_unique<PeriodicMetricsCollector>(
        options.metrics_sample_count, options.metrics_interval,
        [&](std::uint32_t sample) {
          WriteMetricsSnapshot(
              metrics_path, options, driver, nodes, node_process_mutex,
              [&](const NodeRuntime& node, std::string_view error) {
                boost::json::object detail;
                detail["sample"] = sample;
                detail["error"] = error;
                WriteEvent(events_path, options.run_id, node.config.id,
                           SimulationEventKind::kMetricsNodeUnavailable,
                           boost::json::serialize(detail));
                BBP_LOG(warning) << "metrics sample " << sample << " skipped "
                                 << node.config.id << ": " << error;
              },
              [&] { return metrics_collector->StopRequested(); },
              metrics_rpc_stop_source.get_token());
          if (metrics_collector->StopRequested()) {
            return;
          }
          if (wallets_initialized.load(std::memory_order_acquire)) {
            WriteWalletMetricsSnapshot(
                wallet_metrics_path, options, driver, nodes,
                [&](std::uint32_t wallet_index, const NodeRuntime& node,
                    std::string_view error) {
                  boost::json::object detail;
                  detail["sample"] = sample;
                  detail["wallet_index"] = wallet_index;
                  detail["error"] = error;
                  WriteEvent(events_path, options.run_id, node.config.id,
                             SimulationEventKind::kWalletMetricsUnavailable,
                             boost::json::serialize(detail));
                  BBP_LOG(warning) << "wallet metrics sample " << sample
                                   << " skipped #" << wallet_index << " on "
                                   << node.config.id << ": " << error;
                },
                metrics_rpc_stop_source.get_token());
          }
          if (metrics_collector->StopRequested()) {
            return;
          }
          boost::json::object detail;
          detail["sample"] = sample;
          detail["sample_count"] = options.metrics_sample_count;
          detail["interval_ms"] = options.metrics_interval.count();
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kMetricsSample,
                     boost::json::serialize(detail));
        },
        [stop_token] { return stop_token.stop_requested(); });
    metrics_collector->Start();
    InitializeWalletNodes(options, events_path, driver, nodes,
                          simulation_registry, stop_token);
    wallets_initialized.store(true, std::memory_order_release);

    ThrowIfStopRequested(stop_token);
    peer_connectivity_controller->Start();
    if (options.block_production.enabled) {
      if (options.block_production.mode == MiningMode::kNativeMining) {
        for (const std::uint32_t miner_index : miner_indexes) {
          driver.StartMining(nodes[miner_index].config,
                             chain_spec.default_reward_address, stop_token);
          active_native_miner_ids.push_back(nodes[miner_index].config.id);
        }
      } else if (block_scheduler) {
        block_scheduler->Start();
      }
    }
    if (command_processor) {
      command_processor->Start();
    }

    ThrowIfStopRequested(stop_token);
    WriteMetricsSnapshot(metrics_path, options, driver, nodes,
                         node_process_mutex, {}, {}, stop_token);
    WriteWalletMetricsSnapshot(wallet_metrics_path, options, driver, nodes, {},
                               stop_token);

    ApplyRuntimeResourceLimitUpdates(options, events_path, nodes, stop_token);
    ApplyRuntimeNetworkConditionUpdates(options, events_path, nodes,
                                        stop_token);
    ApplyRuntimeNetworkBlockRules(options, events_path, nodes, stop_token);
    ApplyRuntimeNetworkPartitions(options, events_path, nodes, stop_token);
    ApplyRuntimeNetworkPartitionHeals(options, events_path, nodes, stop_token);
    ApplyRuntimeNetworkUnblockRules(options, events_path, nodes, stop_token);
    {
      std::lock_guard<std::mutex> lock(node_process_mutex);
      ApplyRuntimeNodeRestarts(options, events_path, driver, nodes, stop_token);
    }
    ApplyRuntimeNodeFreezes(options, events_path, nodes, stop_token);
    std::vector<ScheduledScenarioEvent> runtime_actions;
    runtime_actions.reserve(options.workloads.size() +
                            options.scheduled_events.size());
    for (const ScenarioWorkload& workload : EffectiveWorkloads(options)) {
      runtime_actions.push_back(
          ScheduledScenarioEvent{.at = std::chrono::milliseconds(0),
                                 .sequence = 0U,
                                 .action = workload});
    }
    std::vector<ScheduledScenarioEvent> scheduled_events =
        OrderScheduledScenarioEvents(options.scheduled_events);
    runtime_actions.insert(runtime_actions.end(), scheduled_events.begin(),
                           scheduled_events.end());
    const auto event_engine_epoch = std::chrono::steady_clock::now();
    simulation_epoch = event_engine_epoch;
    if (options.simulation_duration) {
      const std::chrono::milliseconds wall_duration =
          options.time_scale.WallDuration(*options.simulation_duration);
      const auto duration_deadline =
          SteadyDeadline(event_engine_epoch, wall_duration);
      duration_timer.emplace(
          [duration_deadline, &simulation_duration_reached,
           &duration_stop_requested_at_ms, &simulation_stop_source,
           event_engine_epoch](std::stop_token timer_stop_token) {
            try {
              WaitUntil(duration_deadline, timer_stop_token);
            } catch (const SimulationCancelled&) {
              return;
            }
            duration_stop_requested_at_ms.store(
                ElapsedMilliseconds(event_engine_epoch,
                                    std::chrono::steady_clock::now()),
                std::memory_order_release);
            simulation_duration_reached.store(true, std::memory_order_release);
            simulation_stop_source.request_stop();
          });
    }
    TransactionObservationTracker transaction_tracker;
    for (size_t workload_index = 0; workload_index < runtime_actions.size();
         ++workload_index) {
      ThrowIfStopRequested(stop_token);
      const ScheduledScenarioEvent& runtime_action =
          runtime_actions[workload_index];
      const bool is_scheduled = runtime_action.sequence != 0U;
      const std::uint32_t action_index =
          is_scheduled ? runtime_action.sequence
                       : static_cast<std::uint32_t>(workload_index + 1U);
      const std::uint32_t action_count = static_cast<std::uint32_t>(
          is_scheduled ? scheduled_events.size() : options.workloads.size());
      const std::chrono::milliseconds scheduled_wall_at =
          options.time_scale.WallDuration(runtime_action.at);
      if (is_scheduled) {
        WaitUntil(SteadyDeadline(event_engine_epoch, scheduled_wall_at),
                  stop_token);
      }
      const auto action_started = std::chrono::steady_clock::now();
      if (is_scheduled) {
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kScheduledEventStarted,
                   boost::json::serialize(ScheduledEventLifecycleDetail(
                       runtime_action, scheduled_wall_at, event_engine_epoch,
                       action_started, std::nullopt)));
      }
      const ScenarioWorkload& scenario_workload = runtime_action.action;
      try {
        if (scenario_workload.kind == WorkloadKind::kBlockGeneration) {
          const BlockGenerationWorkload& workload =
              scenario_workload.block_generation;
          if (workload.count == 0U) {
            if (is_scheduled) {
              const auto action_finished = std::chrono::steady_clock::now();
              WriteEvent(
                  events_path, options.run_id, "sim",
                  SimulationEventKind::kScheduledEventCompleted,
                  boost::json::serialize(ScheduledEventLifecycleDetail(
                      runtime_action, scheduled_wall_at, event_engine_epoch,
                      action_started, action_finished)));
            }
            continue;
          }
          NodeRuntime& generator = nodes[workload.node - 1U];
          uint64_t start_height = 0U;
          std::vector<std::string> hashes;
          {
            std::lock_guard<std::mutex> lock(node_process_mutex);
            start_height =
                driver.ReadMetrics(generator.config, stop_token).height;
            hashes = driver.GenerateBlocks(generator.config, workload.count,
                                           chain_spec.default_reward_address,
                                           stop_token);
            RecordGeneratedBlocks(driver, generator, hashes, stop_token);
          }
          const uint64_t target_height =
              start_height + static_cast<uint64_t>(hashes.size());
          WriteEvent(
              events_path, options.run_id, generator.config.id,
              SimulationEventKind::kGeneratedBlocks,
              GeneratedBlocksDetail(action_index, action_count, workload.node,
                                    start_height, target_height, hashes,
                                    chain_spec.default_reward_address));
          for (auto& node : nodes) {
            driver.WaitForHeight(
                node.config, target_height,
                std::chrono::seconds(workload.sync_timeout_sec), stop_token);
            WriteEvent(events_path, options.run_id, node.config.id,
                       SimulationEventKind::kHeightReached,
                       std::to_string(target_height));
          }
          transaction_tracker.ObserveAll(options, events_path, driver, nodes,
                                         stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kWaitUntilHeight) {
          const WaitUntilHeightWorkload& workload =
              scenario_workload.wait_until_height;
          NodeRuntime& node = nodes[workload.node - 1U];
          driver.WaitForHeight(node.config, workload.height,
                               std::chrono::seconds(workload.timeout_sec),
                               stop_token);
          const uint64_t observed_height =
              driver.ReadMetrics(node.config, stop_token).height;
          WriteEvent(events_path, options.run_id, node.config.id,
                     SimulationEventKind::kHeightWaitReached,
                     HeightWaitDetail(action_index, action_count, workload.node,
                                      workload.height, observed_height));
        } else if (scenario_workload.kind == WorkloadKind::kWaitForPeers) {
          const WaitForPeersWorkload& workload =
              scenario_workload.wait_for_peers;
          NodeRuntime& node = nodes[workload.node - 1U];
          driver.WaitForPeerCount(node.config, workload.peer_count,
                                  std::chrono::seconds(workload.timeout_sec),
                                  stop_token);
          const uint64_t observed_peer_count =
              driver.ReadMetrics(node.config, stop_token).peer_count;
          WriteEvent(
              events_path, options.run_id, node.config.id,
              SimulationEventKind::kPeerCountReached,
              PeerCountWaitDetail(action_index, action_count, workload.node,
                                  workload.peer_count, observed_peer_count));
        } else if (scenario_workload.kind == WorkloadKind::kConnectPeer) {
          ApplyConnectPeerWorkload(options, events_path, driver,
                                   *peer_connectivity_controller, nodes,
                                   scenario_workload.connect_peer, action_index,
                                   action_count, stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kDisconnectPeer) {
          ApplyDisconnectPeerWorkload(options, events_path, driver,
                                      *peer_connectivity_controller, nodes,
                                      scenario_workload.disconnect_peer,
                                      action_index, action_count, stop_token);
        } else if (scenario_workload.kind ==
                   WorkloadKind::kSendRawTransaction) {
          ApplySendRawTransactionWorkload(
              options, events_path, driver, nodes, transaction_tracker,
              scenario_workload.send_raw_transaction, action_index,
              action_count, stop_token);
        } else if (scenario_workload.kind ==
                   WorkloadKind::kWalletTransactions) {
          ApplyWalletTransactionsWorkload(
              options, events_path, driver, nodes, simulation_registry,
              transaction_tracker, scenario_workload.wallet_transactions,
              action_index, action_count, stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kRestartNode) {
          const RestartNodeWorkload& workload = scenario_workload.restart_node;
          NodeRuntime& node = nodes[workload.node - 1U];
          {
            std::lock_guard<std::mutex> lock(node_process_mutex);
            RestartNode(options, events_path, driver, node, stop_token);
          }
          WriteEvent(
              events_path, options.run_id, node.config.id,
              SimulationEventKind::kNodeRestarted,
              RestartNodeWorkloadDetail(action_index, action_count,
                                        workload.node, node.RestartCount()));
        } else if (scenario_workload.kind == WorkloadKind::kFreezeNode) {
          const FreezeNodeWorkload& workload = scenario_workload.freeze_node;
          NodeRuntime& node = nodes[workload.node - 1U];
          FreezeNodeForDuration(options, events_path, node,
                                workload.duration_ms, stop_token);
          WriteEvent(
              events_path, options.run_id, node.config.id,
              SimulationEventKind::kNodeFreezeCompleted,
              FreezeNodeWorkloadDetail(action_index, action_count,
                                       workload.node, workload.duration_ms));
        } else if (scenario_workload.kind ==
                   WorkloadKind::kUpdateResourceLimits) {
          const ResourceLimitUpdateWorkload& workload =
              scenario_workload.update_resource_limits;
          NodeRuntime& node = nodes[workload.node - 1U];
          ApplyResourceLimitUpdate(options, events_path, node, workload.patch,
                                   action_index, action_count, workload.node);
        } else if (scenario_workload.kind ==
                   WorkloadKind::kSetResourceProfile) {
          ApplyResourceProfileSwitch(options, events_path, nodes,
                                     scenario_workload.profile_switch,
                                     action_index, action_count, stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kSetNetworkProfile) {
          ApplyNetworkProfileSwitch(options, events_path, nodes,
                                    scenario_workload.profile_switch,
                                    action_index, action_count, stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kResourcePressure) {
          ApplyResourcePressureWorkload(options, events_path, metrics_path,
                                        driver, nodes, node_process_mutex,
                                        scenario_workload.resource_pressure,
                                        action_index, action_count, stop_token);
        } else if (scenario_workload.kind ==
                   WorkloadKind::kSetNetworkCondition) {
          const NetworkConditionWorkload& workload =
              scenario_workload.network_condition;
          NodeRuntime& node = nodes[workload.node - 1U];
          QdiscInfo qdisc;
          NodeVethConfig updated_network;
          {
            std::lock_guard<std::mutex> lock(node_network_state_mutex);
            qdisc = ReplaceNodeNetworkConditionTransactional(
                &node, workload.condition, stop_token);
            updated_network = *node.network;
          }
          WriteEvent(events_path, options.run_id, node.config.id,
                     SimulationEventKind::kNetworkConditionUpdated,
                     NetworkConditionVerificationDetail(
                         updated_network, qdisc, action_index, action_count));
        } else if (scenario_workload.kind == WorkloadKind::kBlockNetworkFlow ||
                   scenario_workload.kind ==
                       WorkloadKind::kUnblockNetworkFlow) {
          const NetworkBlockRule& rule = scenario_workload.network_block.rule;
          NodeRuntime& node = nodes[rule.node_index];
          NetworkBlockMutationResult result;
          {
            std::lock_guard<std::mutex> lock(node_network_state_mutex);
            result = MutateNetworkBlockRuleTransactional(
                node, rule,
                scenario_workload.kind == WorkloadKind::kUnblockNetworkFlow,
                stop_token);
          }
          WriteEvent(events_path, options.run_id, node.config.id,
                     scenario_workload.kind == WorkloadKind::kUnblockNetworkFlow
                         ? SimulationEventKind::kNetworkBlockRemoved
                         : SimulationEventKind::kNetworkBlockApplied,
                     NetworkBlockRuleDetail(node, rule, result.existed_before,
                                            result.present_after, action_index,
                                            action_count));
        } else if (scenario_workload.kind == WorkloadKind::kPartitionNodes) {
          ApplyRuntimeNetworkPartition(
              options, events_path, nodes,
              scenario_workload.network_partition.partition, false,
              action_index, action_count, stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kHealPartition) {
          ApplyRuntimeNetworkPartition(
              options, events_path, nodes,
              scenario_workload.network_partition.partition, true, action_index,
              action_count, stop_token);
        } else if (scenario_workload.kind == WorkloadKind::kCheckpoint) {
          const CheckpointWorkload& workload = scenario_workload.checkpoint;
          const std::string name =
              workload.name.empty()
                  ? "checkpoint-" + std::to_string(action_index)
                  : workload.name;
          transaction_tracker.ObserveAll(options, events_path, driver, nodes,
                                         stop_token);
          const std::uint32_t node_metric_samples =
              WriteMetricsSnapshot(metrics_path, options, driver, nodes,
                                   node_process_mutex, {}, {}, stop_token);
          const std::uint32_t wallet_metric_samples =
              WriteWalletMetricsSnapshot(wallet_metrics_path, options, driver,
                                         nodes, {}, stop_token);
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kCheckpointRecorded,
                     CheckpointWorkloadDetail(action_index, action_count, name,
                                              node_metric_samples,
                                              wallet_metric_samples));
        } else if (IsTopologyEdgeAction(scenario_workload.kind)) {
          std::lock_guard<std::mutex> lock(node_process_mutex);
          ApplyTopologyEdgeWorkload(
              options, events_path, chain_spec, driver,
              *peer_connectivity_controller, runtime_topology, nodes,
              scenario_workload.topology_edge, scenario_workload.kind,
              action_index, action_count, stop_token);
        }
      } catch (const std::exception& e) {
        if (is_scheduled) {
          const auto action_finished = std::chrono::steady_clock::now();
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kScheduledEventFailed,
                     boost::json::serialize(ScheduledEventLifecycleDetail(
                         runtime_action, scheduled_wall_at, event_engine_epoch,
                         action_started, action_finished, e.what())));
        }
        throw;
      } catch (...) {
        if (is_scheduled) {
          const auto action_finished = std::chrono::steady_clock::now();
          WriteEvent(
              events_path, options.run_id, "sim",
              SimulationEventKind::kScheduledEventFailed,
              boost::json::serialize(ScheduledEventLifecycleDetail(
                  runtime_action, scheduled_wall_at, event_engine_epoch,
                  action_started, action_finished, "unknown exception")));
        }
        throw;
      }
      if (is_scheduled) {
        const auto action_finished = std::chrono::steady_clock::now();
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kScheduledEventCompleted,
                   boost::json::serialize(ScheduledEventLifecycleDetail(
                       runtime_action, scheduled_wall_at, event_engine_epoch,
                       action_started, action_finished)));
      }
    }

    metrics_collector->Wait();
    ThrowIfStopRequested(stop_token);
    stop_duration_timer();
    stop_command_processor();
    stop_peer_connectivity();
    stop_block_production();
    transaction_tracker.ObserveAll(options, events_path, driver, nodes,
                                   stop_token);
    WriteMetricsSnapshot(metrics_path, options, driver, nodes,
                         node_process_mutex, {}, {}, stop_token);
    WriteWalletMetricsSnapshot(wallet_metrics_path, options, driver, nodes, {},
                               stop_token);

    StopNodes(options, events_path, driver, nodes);
    log_collector->Stop();
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRunFinished);
    BBP_LOG(info) << "finished run " << options.run_id;
  } catch (const SimulationCancelled&) {
    if (simulation_duration_reached.load(std::memory_order_acquire) &&
        !external_stop_token.stop_requested()) {
      handle_simulation_duration();
    } else {
      handle_run_cancellation();
    }
  } catch (const std::exception& e) {
    handle_run_failure(e.what());
    throw;
  } catch (...) {
    handle_run_failure("unknown exception");
    throw;
  }

  BBP_LOG(info) << "run_id=" << options.run_id << "\n"
                << "output_dir=" << run_root << "\n"
                << "metrics=" << metrics_path << "\n"
                << "wallet_metrics=" << wallet_metrics_path << "\n"
                << "events=" << events_path;
  return 0;
}

int RunBenchmarkWithTui(Options options, std::stop_token signal_stop_token) {
  SetConsoleLoggingEnabled(false);
  try {
    const auto run_root = BenchmarkRunRoot(options);
    std::exception_ptr simulation_failure;
    std::exception_ptr tui_failure;
    int simulation_result = 1;
    SimulationCommandQueue command_queue;
    std::stop_source stop_source;
    std::stop_callback cancel_on_signal(signal_stop_token, [&] {
      stop_source.request_stop();
      command_queue.Close();
    });

    std::thread simulation_thread([&options, &simulation_failure,
                                   &simulation_result, &command_queue,
                                   &stop_source]() {
      try {
        simulation_result = RunBenchmarkHeadless(options, &command_queue,
                                                 stop_source.get_token());
      } catch (...) {
        simulation_failure = std::current_exception();
      }
    });

    int tui_result = 1;
    try {
      tui_result = RunTuiReport(run_root, false, options.tui_refresh_ms,
                                &command_queue, stop_source.get_token());
    } catch (...) {
      tui_failure = std::current_exception();
    }
    stop_source.request_stop();
    command_queue.Close();
    simulation_thread.join();

    if (simulation_failure) {
      std::rethrow_exception(simulation_failure);
    }
    if (tui_failure) {
      std::rethrow_exception(tui_failure);
    }
    SetConsoleLoggingEnabled(true);
    return simulation_result == 0 ? tui_result : simulation_result;
  } catch (...) {
    SetConsoleLoggingEnabled(true);
    throw;
  }
}

std::filesystem::path ResolveRunReference(
    const std::filesystem::path& benchmark_root,
    const std::filesystem::path& reference) {
  if (reference.is_absolute() || reference.has_parent_path() ||
      std::filesystem::exists(reference)) {
    return reference;
  }
  return benchmark_root / reference;
}

}  // namespace

int SimulatorApp::Run(int argc, char** argv) {
  Options options = ParseOptions(argc, argv);
  SetMinimumLogLevel(options.log_level);
  RequireSafeOutputDirectory(options.output_dir);
  if (options.probe_network) {
    BBP_LOG(info) << NetworkProbeJson();
    return 0;
  }
  if (!options.report_run.empty()) {
    BBP_LOG(info) << BuildRunReportJson(
        ResolveRunReference(options.output_dir, options.report_run));
    return 0;
  }
  if (!options.tui_run.empty()) {
    return RunTuiReport(
        ResolveRunReference(options.output_dir, options.tui_run),
        options.tui_once, options.tui_refresh_ms);
  }
  if (options.probe_capabilities) {
    BBP_LOG(info) << CapabilityProbeJson();
    return 0;
  }
  if (options.probe_cgroup_freeze) {
    BBP_LOG(info) << CgroupFreezeProbeJson();
    return 0;
  }
  if (options.probe_drop_filter) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << DropFilterProbeJson();
    return 0;
  }
  if (options.probe_directional_network_condition) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << DirectionalNetworkPolicyProbeJson();
    return 0;
  }
  if (options.probe_netns) {
    RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
    BBP_LOG(info) << NetworkNamespaceProbeJson();
    return 0;
  }
  if (options.probe_veth) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << VethProbeJson();
    return 0;
  }
  if (options.probe_bandwidth_limit) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << BandwidthLimitProbeJson();
    return 0;
  }
  if (options.probe_network_condition) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << NetworkConditionProbeJson();
    return 0;
  }
  if (options.probe_combined_network_condition) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << CombinedNetworkConditionProbeJson();
    return 0;
  }
  if (options.probe_network_condition_update) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << NetworkConditionUpdateProbeJson();
    return 0;
  }
  if (options.probe_address) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << AddressProbeJson();
    return 0;
  }
  if (options.probe_route) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << RouteProbeJson();
    return 0;
  }
  if (options.probe_qdisc) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << QdiscProbeJson();
    return 0;
  }
  if (options.probe_qdisc_mutation) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << QdiscMutationProbeJson();
    return 0;
  }
  if (options.cleanup_run) {
    CleanupRun(options);
    return 0;
  }
  if (options.no_tui) {
    SignalStopMonitor signal_monitor;
    const int result =
        RunBenchmarkHeadless(options, nullptr, signal_monitor.GetToken());
    if (signal_monitor.ReceivedSignal() != 0) {
      BBP_LOG(info) << "graceful shutdown completed after signal "
                    << signal_monitor.ReceivedSignal();
    }
    return result;
  }
  SignalStopMonitor signal_monitor;
  const int result = RunBenchmarkWithTui(options, signal_monitor.GetToken());
  if (signal_monitor.ReceivedSignal() != 0) {
    BBP_LOG(info) << "graceful shutdown completed after signal "
                  << signal_monitor.ReceivedSignal();
  }
  return result;
}

}  // namespace bbp
