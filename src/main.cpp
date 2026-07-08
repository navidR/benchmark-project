#include "benchmark_sim/capability.h"
#include "benchmark_sim/cgroup.h"
#include "benchmark_sim/firo_driver.h"
#include "benchmark_sim/log_tail.h"
#include "benchmark_sim/logging.h"
#include "benchmark_sim/network.h"
#include "benchmark_sim/process.h"
#include "benchmark_sim/run_report.h"
#include "benchmark_sim/util.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <linux/capability.h>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/program_options.hpp>

namespace bsim {
namespace {

constexpr const char* kDefaultRewardAddress =
    "TTJW6FsYqLbSiF3ZUwMXRghgQuXK7XTodR";
constexpr const char* kRunMarkerFile = ".benchmark-sim-run";
constexpr uint64_t kMaxLogTailBytes = 4096;

struct ResourceLimits {
  uint64_t memory_high_bytes = 0;
  uint64_t memory_max_bytes = 0;
  std::optional<uint64_t> cpu_quota_us;
  uint64_t cpu_period_us = 0;
  uint64_t pids_max = 0;
};

struct ResourceLimitPatch {
  std::optional<uint64_t> memory_high_bytes;
  std::optional<uint64_t> memory_max_bytes;
  bool cpu_quota_present = false;
  std::optional<uint64_t> cpu_quota_us;
  std::optional<uint64_t> cpu_period_us;
  std::optional<uint64_t> pids_max;
};

struct FreezeRequest {
  uint32_t node_index = 0;
  uint32_t duration_ms = 0;
};

struct Options {
  std::filesystem::path scenario_json;
  std::filesystem::path firod;
  std::filesystem::path output_dir = "runs";
  std::filesystem::path report_run;
  std::string run_id = MakeRunId();
  uint32_t nodes = 1;
  uint32_t generate_blocks = 1;
  uint32_t ready_timeout_sec = 30;
  uint32_t sync_timeout_sec = 30;
  uint32_t metrics_sample_count = 0;
  uint32_t metrics_interval_ms = 1000;
  uint64_t memory_high_bytes = 1536ULL * 1024ULL * 1024ULL;
  uint64_t memory_max_bytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
  uint64_t cpu_period_us = 100000;
  uint64_t cpu_quota_us = 0;
  bool cpu_quota_requested = false;
  uint64_t pids_max = 256;
  bool keep_cgroups = false;
  bool cleanup_run = false;
  bool isolate_network = false;
  bool network_condition_requested = false;
  NetworkCondition network_condition;
  std::vector<std::string> node_network_condition_json;
  std::vector<std::string> runtime_node_network_condition_json;
  std::vector<std::string> runtime_node_resource_json;
  std::vector<std::string> runtime_node_restart_json;
  std::vector<std::string> runtime_node_freeze_json;
  std::map<uint32_t, NetworkCondition> node_network_conditions;
  std::map<uint32_t, NetworkCondition> runtime_node_network_conditions;
  std::map<uint32_t, ResourceLimitPatch> runtime_node_resource_updates;
  std::vector<uint32_t> runtime_node_restarts;
  std::vector<FreezeRequest> runtime_node_freezes;
  bool replace_run = false;
  bool probe_address = false;
  bool probe_bandwidth_limit = false;
  bool probe_capabilities = false;
  bool probe_cgroup_freeze = false;
  bool probe_netns = false;
  bool probe_combined_network_condition = false;
  bool probe_network_condition = false;
  bool probe_network_condition_update = false;
  bool probe_qdisc = false;
  bool probe_qdisc_mutation = false;
  bool probe_route = false;
  bool probe_veth = false;
  bool probe_network = false;
};

struct NodeRuntime {
  FiroNodeConfig config;
  std::optional<Cgroup> cgroup;
  std::optional<NetworkNamespace> network_namespace;
  std::optional<NodeVethConfig> network;
  ChildProcess process;
  ResourceLimits resources;
  uint64_t generated_block_count = 0;
  uint64_t restart_count = 0;
  uint64_t stdout_offset = 0;
  uint64_t stderr_offset = 0;
  uint64_t daemon_log_offset = 0;
};

uint32_t JsonUint32Field(const boost::json::object& object,
                         const char* field) {
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

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

NetworkCondition ParseNetworkConditionObject(
    const boost::json::object& object) {
  NetworkCondition condition;
  condition.bandwidth_mbps = JsonOptionalUint32Field(
      object, "bandwidth_mbps", condition.bandwidth_mbps);
  condition.delay_ms =
      JsonOptionalUint32Field(object, "delay_ms", condition.delay_ms);
  condition.jitter_ms =
      JsonOptionalUint32Field(object, "jitter_ms", condition.jitter_ms);
  condition.loss_basis_points = JsonOptionalUint32Field(
      object, "loss_basis_points", condition.loss_basis_points);
  condition.duplicate_basis_points = JsonOptionalUint32Field(
      object, "duplicate_basis_points", condition.duplicate_basis_points);
  condition.corrupt_basis_points = JsonOptionalUint32Field(
      object, "corrupt_basis_points", condition.corrupt_basis_points);
  condition.reorder_basis_points = JsonOptionalUint32Field(
      object, "reorder_basis_points", condition.reorder_basis_points);
  condition.limit_packets = JsonOptionalUint32Field(
      object, "limit_packets", condition.limit_packets);
  return condition;
}

bool ResourceLimitPatchEmpty(const ResourceLimitPatch& patch) {
  return !patch.memory_high_bytes && !patch.memory_max_bytes &&
         !patch.cpu_quota_present && !patch.cpu_period_us && !patch.pids_max;
}

void RequireNonZero(uint64_t value, std::string_view field) {
  if (value == 0U) {
    throw std::runtime_error(std::string(field) +
                             " must be greater than zero");
  }
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
  patch.cpu_period_us =
      JsonOptionalUint64FieldValue(object, "cpu_period_us");
  patch.pids_max = JsonOptionalUint64FieldValue(object, "pids_max");

  if (ResourceLimitPatchEmpty(patch)) {
    throw std::runtime_error("runtime resource update has no limit fields");
  }
  if (patch.memory_max_bytes) {
    RequireNonZero(*patch.memory_max_bytes, "memory_max_bytes");
  }
  if (patch.cpu_quota_us) {
    RequireNonZero(*patch.cpu_quota_us, "cpu_quota_us");
  }
  if (patch.cpu_period_us) {
    RequireNonZero(*patch.cpu_period_us, "cpu_period_us");
  }
  if (patch.pids_max) {
    RequireNonZero(*patch.pids_max, "pids_max");
  }
  if (patch.memory_high_bytes && patch.memory_max_bytes &&
      *patch.memory_high_bytes > *patch.memory_max_bytes) {
    throw std::runtime_error(
        "memory_high_bytes must be less than or equal to memory_max_bytes");
  }
  return patch;
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

void ApplyResourceLimitPatches(
    const boost::json::array& updates, uint32_t nodes, std::string_view source,
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

void ApplyScenarioJson(const boost::json::object& scenario,
                       const boost::program_options::variables_map& vm,
                       Options& options) {
  if (!OptionProvided(vm, "firod")) {
    options.firod = JsonOptionalPathField(scenario, "firod", options.firod);
  }
  if (!OptionProvided(vm, "output-dir")) {
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
  if (!OptionProvided(vm, "nodes")) {
    options.nodes = JsonOptionalUint32Field(scenario, "nodes", options.nodes);
  }
  if (!OptionProvided(vm, "generate-blocks")) {
    options.generate_blocks = JsonOptionalUint32Field(
        scenario, "generate_blocks", options.generate_blocks);
  }
  if (!OptionProvided(vm, "ready-timeout-sec")) {
    options.ready_timeout_sec = JsonOptionalUint32Field(
        scenario, "ready_timeout_sec", options.ready_timeout_sec);
  }
  if (!OptionProvided(vm, "sync-timeout-sec")) {
    options.sync_timeout_sec = JsonOptionalUint32Field(
        scenario, "sync_timeout_sec", options.sync_timeout_sec);
  }
  if (!OptionProvided(vm, "metrics-sample-count")) {
    options.metrics_sample_count = JsonOptionalUint32Field(
        scenario, "metrics_sample_count", options.metrics_sample_count);
  }
  if (!OptionProvided(vm, "metrics-interval-ms")) {
    options.metrics_interval_ms = JsonOptionalUint32Field(
        scenario, "metrics_interval_ms", options.metrics_interval_ms);
  }
  if (!OptionProvided(vm, "isolate-network")) {
    options.isolate_network = JsonOptionalBoolField(
        scenario, "isolated_network", options.isolate_network);
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
      options.cpu_period_us = JsonOptionalUint64Field(
          object, "cpu_period_us", options.cpu_period_us);
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
        options.cpu_quota_us =
            quota->is_uint64() ? quota->as_uint64()
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
      ApplyResourceLimitPatches(runtime_node_limits->as_array(),
                                options.nodes,
                                "scenario resources.runtime_node_limits",
                                options.runtime_node_resource_updates);
    }
  }

  const boost::json::value* process = scenario.if_contains("process");
  if (process != nullptr) {
    if (!process->is_object()) {
      throw std::runtime_error("scenario process must be a JSON object");
    }
    const boost::json::object& object = process->as_object();
    const boost::json::value* runtime_node_restarts =
        object.if_contains("runtime_node_restarts");
    if (runtime_node_restarts != nullptr) {
      if (!runtime_node_restarts->is_array()) {
        throw std::runtime_error(
            "scenario process.runtime_node_restarts must be a JSON array");
      }
      for (const boost::json::value& value :
           runtime_node_restarts->as_array()) {
        if (!value.is_object()) {
          throw std::runtime_error(
              "scenario process.runtime_node_restarts entries must be JSON "
              "objects");
        }
        const uint32_t node = JsonUint32Field(value.as_object(), "node");
        if (node == 0 || node > options.nodes) {
          throw std::runtime_error(
              "scenario process.runtime_node_restarts node must be in 1.." +
              std::to_string(options.nodes));
        }
        options.runtime_node_restarts.push_back(node - 1U);
      }
    }
    const boost::json::value* runtime_node_freezes =
        object.if_contains("runtime_node_freezes");
    if (runtime_node_freezes != nullptr) {
      if (!runtime_node_freezes->is_array()) {
        throw std::runtime_error(
            "scenario process.runtime_node_freezes must be a JSON array");
      }
      for (const boost::json::value& value :
           runtime_node_freezes->as_array()) {
        if (!value.is_object()) {
          throw std::runtime_error(
              "scenario process.runtime_node_freezes entries must be JSON "
              "objects");
        }
        const boost::json::object& freeze = value.as_object();
        const uint32_t node = JsonUint32Field(freeze, "node");
        if (node == 0 || node > options.nodes) {
          throw std::runtime_error(
              "scenario process.runtime_node_freezes node must be in 1.." +
              std::to_string(options.nodes));
        }
        const uint32_t duration_ms = JsonUint32Field(freeze, "duration_ms");
        if (duration_ms == 0U) {
          throw std::runtime_error(
              "scenario process.runtime_node_freezes duration_ms must be "
              "greater than zero");
        }
        options.runtime_node_freezes.push_back(
            FreezeRequest{.node_index = node - 1U,
                          .duration_ms = duration_ms});
      }
    }
  }

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
  }
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

void ParseNodeNetworkConditions(Options& options) {
  ParseNodeNetworkConditionTexts(options.node_network_condition_json,
                                 options.nodes,
                                 "--node-network-condition-json",
                                 options.node_network_conditions);
  ParseNodeNetworkConditionTexts(options.runtime_node_network_condition_json,
                                 options.nodes,
                                 "--runtime-node-network-condition-json",
                                 options.runtime_node_network_conditions);
  ParseRuntimeNodeResourceTexts(options.runtime_node_resource_json,
                                options.nodes,
                                options.runtime_node_resource_updates);
  ParseRuntimeNodeRestartTexts(options.runtime_node_restart_json, options.nodes,
                               options.runtime_node_restarts);
  ParseRuntimeNodeFreezeTexts(options.runtime_node_freeze_json, options.nodes,
                              options.runtime_node_freezes);
}

Options ParseOptions(int argc, char** argv) {
  namespace po = boost::program_options;
  Options options;

  po::options_description desc("Allowed options");
  desc.add_options()("help", "show this help")(
      "scenario-json", po::value<std::filesystem::path>(&options.scenario_json),
      "Boost.JSON scenario file for the Firo MVP")(
      "firod", po::value<std::filesystem::path>(&options.firod),
      "explicit firod binary")(
      "output-dir", po::value<std::filesystem::path>(&options.output_dir),
      "run output root")("run-id", po::value<std::string>(&options.run_id),
                         "safe run id")(
      "report-run", po::value<std::filesystem::path>(&options.report_run),
      "summarize an existing run directory as JSON and exit")(
      "nodes", po::value<uint32_t>(&options.nodes), "Firo regtest nodes, 1..2")(
      "generate-blocks", po::value<uint32_t>(&options.generate_blocks),
      "blocks generated on node 0")(
      "ready-timeout-sec", po::value<uint32_t>(&options.ready_timeout_sec),
      "RPC startup timeout")(
      "sync-timeout-sec", po::value<uint32_t>(&options.sync_timeout_sec),
      "block propagation timeout")(
      "metrics-sample-count",
      po::value<uint32_t>(&options.metrics_sample_count),
      "extra metric samples to collect after runtime events and before "
      "workload generation")(
      "metrics-interval-ms",
      po::value<uint32_t>(&options.metrics_interval_ms),
      "milliseconds between extra metric samples")(
      "memory-high-bytes", po::value<uint64_t>(&options.memory_high_bytes),
      "cgroup memory.high soft pressure threshold in bytes")(
      "memory-max-bytes", po::value<uint64_t>(&options.memory_max_bytes),
      "cgroup memory.max hard limit in bytes")(
      "cpu-quota-us", po::value<uint64_t>(&options.cpu_quota_us),
      "optional cgroup cpu.max quota in microseconds per period")(
      "cpu-period-us", po::value<uint64_t>(&options.cpu_period_us),
      "cgroup cpu.max period in microseconds")(
      "pids-max", po::value<uint64_t>(&options.pids_max),
      "cgroup pids.max process limit")(
      "keep-cgroups", po::bool_switch(&options.keep_cgroups),
      "leave cgroups after exit for inspection")(
      "cleanup-run", po::bool_switch(&options.cleanup_run),
      "remove stale simulator-owned veth and cgroup objects for --run-id and "
      "exit")(
      "isolate-network", po::bool_switch(&options.isolate_network),
      "run each Firo node in its own network namespace and veth link")(
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
          &options.node_network_condition_json)
          ->composing(),
      "repeatable JSON object with node plus network condition fields for one "
      "isolated "
      "node")(
      "runtime-node-network-condition-json",
      po::value<std::vector<std::string>>(
          &options.runtime_node_network_condition_json)
          ->composing(),
      "repeatable JSON object with node plus live network condition fields to "
      "apply after isolated nodes are running")(
      "runtime-node-resource-json",
      po::value<std::vector<std::string>>(
          &options.runtime_node_resource_json)
          ->composing(),
      "repeatable JSON object with node plus live cgroup limit fields to apply "
      "after nodes are running")(
      "runtime-node-restart-json",
      po::value<std::vector<std::string>>(
          &options.runtime_node_restart_json)
          ->composing(),
      "repeatable JSON object with node field for one live node restart after "
      "nodes are running")(
      "runtime-node-freeze-json",
      po::value<std::vector<std::string>>(
          &options.runtime_node_freeze_json)
          ->composing(),
      "repeatable JSON object with node and duration_ms for one live cgroup "
      "freeze/thaw after nodes are running")(
      "replace-run", po::bool_switch(&options.replace_run),
      "remove an existing run directory first")(
      "probe-address", po::bool_switch(&options.probe_address),
      "assign and inspect an IPv4 address inside a temporary netns through "
      "libmnl")(
      "probe-bandwidth-limit",
      po::bool_switch(&options.probe_bandwidth_limit),
      "apply and remove a TBF bandwidth limit on a temporary veth peer through "
      "libmnl")(
      "probe-capabilities", po::bool_switch(&options.probe_capabilities),
      "report effective Linux capabilities needed by privileged simulator "
      "paths")(
      "probe-cgroup-freeze", po::bool_switch(&options.probe_cgroup_freeze),
      "attach a child process to a cgroup and verify cgroup.freeze/thaw "
      "paths")(
      "probe-netns", po::bool_switch(&options.probe_netns),
      "create a temporary network namespace and inspect it through setns/libmnl")(
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
      "probe-qdisc-mutation",
      po::bool_switch(&options.probe_qdisc_mutation),
      "replace and delete a root pfifo qdisc on a temporary veth peer through "
      "libmnl")(
      "probe-route", po::bool_switch(&options.probe_route),
      "assign and inspect an IPv4 route inside a temporary netns through libmnl")(
      "probe-veth", po::bool_switch(&options.probe_veth),
      "create, move, inspect, and delete a temporary veth pair through libmnl")(
      "probe-network", po::bool_switch(&options.probe_network),
      "list links through rtnetlink/libmnl and exit");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") != 0U) {
    std::cout << "Usage: " << argv[0] << " [options]\n" << desc << "\n";
    std::exit(0);
  }
  if (vm.count("scenario-json") != 0U) {
    const boost::json::value scenario =
        boost::json::parse(ReadText(options.scenario_json));
    if (!scenario.is_object()) {
      throw std::runtime_error("--scenario-json root must be a JSON object");
    }
    ApplyScenarioJson(scenario.as_object(), vm, options);
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
  if (options.pids_max == 0U) {
    throw std::runtime_error("--pids-max must be greater than zero");
  }
  if (options.metrics_sample_count > 0U && options.metrics_interval_ms == 0U) {
    throw std::runtime_error(
        "--metrics-interval-ms must be greater than zero when sampling");
  }
  if ((options.network_condition_requested ||
       !options.node_network_condition_json.empty() ||
       !options.node_network_conditions.empty() ||
       !options.runtime_node_network_condition_json.empty() ||
       !options.runtime_node_network_conditions.empty()) &&
      !options.isolate_network) {
    throw std::runtime_error(
        "network condition options require --isolate-network");
  }
  if (options.nodes < 1 || options.nodes > 2) {
    throw std::runtime_error("--nodes currently supports 1..2 for MVP smoke");
  }
  ParseNodeNetworkConditions(options);
  RequireSafeRunId(options.run_id);
  const bool needs_firod = !options.probe_network &&
                           options.report_run.empty() &&
                           !options.probe_bandwidth_limit &&
                           !options.probe_capabilities &&
                           !options.probe_cgroup_freeze &&
                           !options.probe_netns && !options.probe_veth &&
                           !options.probe_address && !options.probe_route &&
                           !options.probe_qdisc &&
                           !options.probe_qdisc_mutation &&
                           !options.probe_network_condition &&
                           !options.probe_combined_network_condition &&
                           !options.probe_network_condition_update &&
                           !options.cleanup_run;
  if (needs_firod && options.firod.empty()) {
    throw std::runtime_error(
        "Firo runs require an explicit --firod path or scenario firod field");
  }
  if (needs_firod) {
    RequireExecutable(options.firod);
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

boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs) {
  boost::json::array qdiscs_json;
  for (const QdiscInfo& qdisc : qdiscs) {
    boost::json::object qdisc_json;
    qdisc_json["if_index"] = qdisc.if_index;
    qdisc_json["if_name"] = qdisc.if_name;
    qdisc_json["kind"] = qdisc.kind;
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
    qdiscs_json.push_back(std::move(qdisc_json));
  }
  return qdiscs_json;
}

boost::json::object NetworkConditionJson(const NetworkCondition& condition) {
  boost::json::object object;
  object["bandwidth_mbps"] = condition.bandwidth_mbps;
  object["delay_ms"] = condition.delay_ms;
  object["jitter_ms"] = condition.jitter_ms;
  object["loss_basis_points"] = condition.loss_basis_points;
  object["duplicate_basis_points"] = condition.duplicate_basis_points;
  object["corrupt_basis_points"] = condition.corrupt_basis_points;
  object["reorder_basis_points"] = condition.reorder_basis_points;
  object["limit_packets"] = condition.limit_packets;
  return object;
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
      .pids_max = options.pids_max,
  };
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
  RequireNonZero(next.pids_max, "pids_max");
  return next;
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
  if (next.pids_max != previous.pids_max) {
    cgroup.SetPidsMax(next.pids_max);
  }
}

std::string NetworkConditionVerificationDetail(const NodeVethConfig& config,
                                               const QdiscInfo& qdisc) {
  boost::json::object detail;
  detail["host_if"] = config.host_name;
  detail["condition"] = NetworkConditionJson(config.condition);
  detail["qdisc_kind"] = qdisc.kind;
  detail["qdisc_handle"] = qdisc.handle;
  detail["qdisc_parent"] = qdisc.parent;
  return boost::json::serialize(detail);
}

std::string NodeHostAddress(uint32_t node_index) {
  return "10.210." + std::to_string(node_index + 1U) + ".1";
}

std::string NodeAddress(uint32_t node_index) {
  return "10.210." + std::to_string(node_index + 1U) + ".2";
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
  return "bs" + RunInterfaceToken(run_id) + "n" +
         std::to_string(node_index + 1U) + suffix;
}

NodeVethConfig MakeNodeVethConfig(const Options& options,
                                  uint32_t node_index) {
  NodeVethConfig config;
  config.host_name = NodeInterfaceName(options.run_id, node_index, 'h');
  config.peer_name = NodeInterfaceName(options.run_id, node_index, 'p');
  config.host_address = NodeHostAddress(node_index);
  config.node_address = NodeAddress(node_index);
  config.prefix_len = 30;
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

std::string FiroPeerHost(const Options& options, uint32_t node_index) {
  if (options.isolate_network) {
    return NodeAddress(node_index);
  }
  return "127.0.0.1";
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
  return boost::json::serialize(result);
}

std::string CapabilityProbeJson() {
  const uint64_t effective = ReadEffectiveCapabilities();
  boost::json::object result;
  result["cap_sys_admin"] = HasCapability(effective, CAP_SYS_ADMIN);
  result["cap_net_admin"] = HasCapability(effective, CAP_NET_ADMIN);
  result["cap_sys_resource"] = HasCapability(effective, CAP_SYS_RESOURCE);
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
  result["namespace_qdiscs_before"] =
      QdiscsJson(probe.namespace_qdiscs_before);
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
  result["namespace_qdiscs_before"] =
      QdiscsJson(probe.namespace_qdiscs_before);
  result["namespace_qdiscs_after_apply"] =
      QdiscsJson(probe.namespace_qdiscs_after_apply);
  result["namespace_qdiscs_after_delete"] =
      QdiscsJson(probe.namespace_qdiscs_after_delete);
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
  result["namespace_qdiscs_before"] =
      QdiscsJson(probe.namespace_qdiscs_before);
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
  result["initial_condition"] =
      NetworkConditionJson(probe.initial_condition);
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
  result["namespace_qdiscs_before"] =
      QdiscsJson(probe.namespace_qdiscs_before);
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

std::string MetricsJson(const std::string& run_id, const std::string& node_id,
                        const FiroMetrics& chain,
                        uint64_t generated_block_count,
                        uint64_t restart_count, const CgroupMetrics* cgroup,
                        const LinkInfo* link, const QdiscInfo* qdisc) {
  boost::json::object object;
  object["timestamp_ms"] = NowUnixMillis();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["chain_version"] = chain.version;
  object["chain_protocol_version"] = chain.protocol_version;
  object["chain_subversion"] = chain.subversion;
  object["height"] = chain.height;
  object["best_hash"] = chain.best_hash;
  object["peer_count"] = chain.peer_count;
  object["mempool_tx_count"] = chain.mempool_tx_count;
  object["mempool_bytes"] = chain.mempool_bytes;
  object["generated_block_count"] = generated_block_count;
  object["restart_count"] = restart_count;
  if (chain.initial_block_download) {
    object["initial_block_download"] = *chain.initial_block_download;
  } else {
    object["initial_block_download"] = nullptr;
  }
  if (chain.difficulty) {
    object["difficulty"] = *chain.difficulty;
  } else {
    object["difficulty"] = nullptr;
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
    object["io_read_bytes"] = cgroup->io_read_bytes;
    object["io_write_bytes"] = cgroup->io_write_bytes;
    object["io_pressure_some_total_usec"] =
        cgroup->io_pressure_some_total_usec;
    object["io_pressure_full_total_usec"] =
        cgroup->io_pressure_full_total_usec;
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
    object["qdisc_kind"] = qdisc->kind;
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
  return boost::json::serialize(object);
}

void WriteEvent(const std::filesystem::path& events_path,
                const std::string& run_id, const std::string& node_id,
                std::string_view event, std::string_view detail = "") {
  boost::json::object object;
  object["timestamp"] = NowIso8601();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["event"] = event;
  object["detail"] = detail;
  AppendLine(events_path, boost::json::serialize(object));
}

void WriteNodeState(const std::filesystem::path& events_path,
                    const std::string& run_id, const std::string& node_id,
                    std::string_view state) {
  WriteEvent(events_path, run_id, node_id, "state", state);
}

void WriteMetricsSnapshot(const std::filesystem::path& metrics_path,
                          const Options& options, const FiroDriver& driver,
                          std::vector<NodeRuntime>& nodes) {
  const std::vector<LinkInfo> links = ListNetworkLinks();
  const std::vector<QdiscInfo> qdiscs = ListQdiscs();
  for (auto& node : nodes) {
    FiroMetrics chain = driver.ReadMetrics(node.config);
    CgroupMetrics cg = node.cgroup->ReadMetrics();
    const LinkInfo* link =
        node.network ? FindLinkByName(links, node.network->host_name) : nullptr;
    std::optional<QdiscInfo> qdisc_summary;
    const QdiscInfo* qdisc = nullptr;
    if (node.network) {
      if (node.network->apply_condition) {
        QdiscInfo candidate;
        if (QdiscsMatchNetworkCondition(qdiscs, node.network->host_name,
                                        node.network->condition, &candidate)) {
          qdisc_summary = candidate;
          qdisc = &*qdisc_summary;
        }
      }
      if (qdisc == nullptr) {
        qdisc = FindQdiscByInterfaceName(qdiscs, node.network->host_name);
      }
    }
    AppendLine(metrics_path,
               MetricsJson(options.run_id, node.config.id, chain,
                           node.generated_block_count, node.restart_count, &cg,
                           link, qdisc));
  }
}

void WritePeriodicMetrics(const std::filesystem::path& events_path,
                          const std::filesystem::path& metrics_path,
                          const Options& options, const FiroDriver& driver,
                          std::vector<NodeRuntime>& nodes) {
  for (uint32_t sample = 0; sample < options.metrics_sample_count; ++sample) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(options.metrics_interval_ms));
    WriteMetricsSnapshot(metrics_path, options, driver, nodes);
    boost::json::object detail;
    detail["sample"] = sample + 1U;
    detail["sample_count"] = options.metrics_sample_count;
    detail["interval_ms"] = options.metrics_interval_ms;
    WriteEvent(events_path, options.run_id, "sim", "metrics_sample",
               boost::json::serialize(detail));
  }
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

void WriteLogTailEvent(const std::filesystem::path& events_path,
                       const Options& options, const NodeRuntime& node,
                       std::string_view kind,
                       const std::filesystem::path& path, uint64_t* offset) {
  if (!std::filesystem::exists(path)) {
    return;
  }
  const LogTailChunk chunk = TailLogFile(path, *offset, kMaxLogTailBytes);
  *offset = chunk.next_offset;
  if (chunk.text.empty() && !chunk.truncated && !chunk.offset_reset) {
    return;
  }
  WriteEvent(events_path, options.run_id, node.config.id,
             std::string(kind) + "_tail", LogTailDetail(kind, chunk));
}

void WriteNodeLogTail(const std::filesystem::path& events_path,
                      const Options& options, const FiroDriver& driver,
                      NodeRuntime& node) {
  WriteLogTailEvent(events_path, options, node, "stdout",
                    node.config.log_dir / "stdout.log",
                    &node.stdout_offset);
  WriteLogTailEvent(events_path, options, node, "stderr",
                    node.config.log_dir / "stderr.log",
                    &node.stderr_offset);
  WriteLogTailEvent(events_path, options, node, "daemon_log",
                    driver.LogPath(node.config), &node.daemon_log_offset);
}

void WriteNodeLogTails(const std::filesystem::path& events_path,
                       const Options& options, const FiroDriver& driver,
                       std::vector<NodeRuntime>& nodes) {
  for (NodeRuntime& node : nodes) {
    WriteNodeLogTail(events_path, options, driver, node);
  }
}

void WriteScenarioFiles(const Options& options,
                        const std::filesystem::path& run_root) {
  std::string network_yaml =
      "network:\n"
      "  isolated: " +
      std::string(options.isolate_network ? "true" : "false") + "\n";
  if (options.network_condition_requested) {
    network_yaml +=
        "  default_condition:\n"
        "    bandwidth_mbps: " +
        std::to_string(options.network_condition.bandwidth_mbps) +
        "\n"
        "    delay_ms: " +
        std::to_string(options.network_condition.delay_ms) +
        "\n"
        "    jitter_ms: " +
        std::to_string(options.network_condition.jitter_ms) +
        "\n"
        "    loss_basis_points: " +
        std::to_string(options.network_condition.loss_basis_points) +
        "\n"
        "    duplicate_basis_points: " +
        std::to_string(options.network_condition.duplicate_basis_points) +
        "\n"
        "    corrupt_basis_points: " +
        std::to_string(options.network_condition.corrupt_basis_points) +
        "\n"
        "    reorder_basis_points: " +
        std::to_string(options.network_condition.reorder_basis_points) +
        "\n"
        "    limit_packets: " +
        std::to_string(options.network_condition.limit_packets) + "\n";
  }
  if (!options.node_network_conditions.empty()) {
    network_yaml += "  node_conditions:\n";
    for (const auto& [node_index, condition] :
         options.node_network_conditions) {
      network_yaml +=
          "    firo-" + std::to_string(node_index + 1U) +
          ":\n"
          "      bandwidth_mbps: " +
          std::to_string(condition.bandwidth_mbps) +
          "\n"
          "      delay_ms: " +
          std::to_string(condition.delay_ms) +
          "\n"
          "      jitter_ms: " +
          std::to_string(condition.jitter_ms) +
          "\n"
          "      loss_basis_points: " +
          std::to_string(condition.loss_basis_points) +
          "\n"
          "      duplicate_basis_points: " +
          std::to_string(condition.duplicate_basis_points) +
          "\n"
          "      corrupt_basis_points: " +
          std::to_string(condition.corrupt_basis_points) +
          "\n"
          "      reorder_basis_points: " +
          std::to_string(condition.reorder_basis_points) +
          "\n"
          "      limit_packets: " +
          std::to_string(condition.limit_packets) + "\n";
    }
  }
  if (!options.runtime_node_network_conditions.empty()) {
    network_yaml += "  runtime_node_conditions:\n";
    for (const auto& [node_index, condition] :
         options.runtime_node_network_conditions) {
      network_yaml +=
          "    firo-" + std::to_string(node_index + 1U) +
          ":\n"
          "      bandwidth_mbps: " +
          std::to_string(condition.bandwidth_mbps) +
          "\n"
          "      delay_ms: " +
          std::to_string(condition.delay_ms) +
          "\n"
          "      jitter_ms: " +
          std::to_string(condition.jitter_ms) +
          "\n"
          "      loss_basis_points: " +
          std::to_string(condition.loss_basis_points) +
          "\n"
          "      duplicate_basis_points: " +
          std::to_string(condition.duplicate_basis_points) +
          "\n"
          "      corrupt_basis_points: " +
          std::to_string(condition.corrupt_basis_points) +
          "\n"
          "      reorder_basis_points: " +
          std::to_string(condition.reorder_basis_points) +
          "\n"
          "      limit_packets: " +
          std::to_string(condition.limit_packets) + "\n";
    }
  }

  std::string runtime_resource_yaml;
  if (!options.runtime_node_resource_updates.empty()) {
    runtime_resource_yaml += "  runtime_node_limits:\n";
    for (const auto& [node_index, patch] :
         options.runtime_node_resource_updates) {
      runtime_resource_yaml +=
          "    firo-" + std::to_string(node_index + 1U) + ":\n";
      if (patch.memory_high_bytes) {
        runtime_resource_yaml +=
            "      memory_high_bytes: " +
            std::to_string(*patch.memory_high_bytes) + "\n";
      }
      if (patch.memory_max_bytes) {
        runtime_resource_yaml +=
            "      memory_max_bytes: " +
            std::to_string(*patch.memory_max_bytes) + "\n";
      }
      if (patch.cpu_quota_present) {
        runtime_resource_yaml +=
            "      cpu_quota_us: " +
            (patch.cpu_quota_us ? std::to_string(*patch.cpu_quota_us)
                                : std::string("max")) +
            "\n";
      }
      if (patch.cpu_period_us) {
        runtime_resource_yaml +=
            "      cpu_period_us: " + std::to_string(*patch.cpu_period_us) +
            "\n";
      }
      if (patch.pids_max) {
        runtime_resource_yaml +=
            "      pids_max: " + std::to_string(*patch.pids_max) + "\n";
      }
    }
  }

  std::string process_yaml;
  if (!options.runtime_node_restarts.empty() ||
      !options.runtime_node_freezes.empty()) {
    process_yaml += "process:\n";
    if (!options.runtime_node_restarts.empty()) {
      process_yaml += "  runtime_node_restarts:\n";
      for (uint32_t node_index : options.runtime_node_restarts) {
        process_yaml +=
            "    - node: " + std::to_string(node_index + 1U) + "\n";
      }
    }
    if (!options.runtime_node_freezes.empty()) {
      process_yaml += "  runtime_node_freezes:\n";
      for (const FreezeRequest& freeze : options.runtime_node_freezes) {
        process_yaml +=
            "    - node: " + std::to_string(freeze.node_index + 1U) +
            "\n"
            "      duration_ms: " +
            std::to_string(freeze.duration_ms) + "\n";
      }
    }
  }

  WriteText(run_root / "scenario.yaml",
            "simulation:\n"
            "  name: " +
                options.run_id +
                "\n"
                "  output_dir: " +
                run_root.string() +
                "\n"
                "chains:\n"
                "  firo:\n"
                "    driver: firo\n"
                "    default_binary: " +
                options.firod.string() +
                "\n"
                "nodes: " +
                std::to_string(options.nodes) +
                "\n"
                "metrics:\n"
                "  extra_sample_count: " +
                std::to_string(options.metrics_sample_count) +
                "\n"
                "  interval_ms: " +
                std::to_string(options.metrics_interval_ms) +
                "\n"
                "resources:\n"
                "  default:\n"
                "    memory_high_bytes: " +
                std::to_string(options.memory_high_bytes) +
                "\n"
                "    memory_max_bytes: " +
                std::to_string(options.memory_max_bytes) +
                "\n"
                "    cpu_quota_us: " +
                (options.cpu_quota_requested
                     ? std::to_string(options.cpu_quota_us)
                     : std::string("max")) +
                "\n"
                "    cpu_period_us: " +
                std::to_string(options.cpu_period_us) +
                "\n"
                "    pids_max: " +
                std::to_string(options.pids_max) +
                "\n" +
                runtime_resource_yaml + network_yaml +
                process_yaml +
                "workloads:\n"
                "  - type: block_generation\n"
                "    count: " +
                std::to_string(options.generate_blocks) +
                "\n"
                "    sync_timeout_sec: " +
                std::to_string(options.sync_timeout_sec) + "\n");

  boost::json::object resolved;
  resolved["run_id"] = options.run_id;
  resolved["chain"] = "firo";
  resolved["nodes"] = options.nodes;
  resolved["firod"] = options.firod.string();
  if (!options.scenario_json.empty()) {
    resolved["scenario_json"] = options.scenario_json.string();
  }
  resolved["isolated_network"] = options.isolate_network;
  resolved["sync_timeout_sec"] = options.sync_timeout_sec;
  resolved["metrics_sample_count"] = options.metrics_sample_count;
  resolved["metrics_interval_ms"] = options.metrics_interval_ms;
  boost::json::object resources;
  resources["memory_high_bytes"] = options.memory_high_bytes;
  resources["memory_max_bytes"] = options.memory_max_bytes;
  if (options.cpu_quota_requested) {
    resources["cpu_quota_us"] = options.cpu_quota_us;
  } else {
    resources["cpu_quota_us"] = nullptr;
  }
  resources["cpu_period_us"] = options.cpu_period_us;
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
  if (!options.runtime_node_restarts.empty()) {
    boost::json::array runtime_node_restarts;
    for (uint32_t node_index : options.runtime_node_restarts) {
      boost::json::object restart;
      restart["node"] = node_index + 1U;
      runtime_node_restarts.push_back(std::move(restart));
    }
    resolved["runtime_node_restarts"] = std::move(runtime_node_restarts);
  }
  if (!options.runtime_node_freezes.empty()) {
    boost::json::array runtime_node_freezes;
    for (const FreezeRequest& freeze : options.runtime_node_freezes) {
      boost::json::object object;
      object["node"] = freeze.node_index + 1U;
      object["duration_ms"] = freeze.duration_ms;
      runtime_node_freezes.push_back(std::move(object));
    }
    resolved["runtime_node_freezes"] = std::move(runtime_node_freezes);
  }
  WriteText(run_root / "resolved-scenario.json",
            boost::json::serialize(resolved) + "\n");
}

void LoadCleanupMetadata(const std::filesystem::path& run_root,
                         Options* options) {
  const std::filesystem::path resolved_path = run_root / "resolved-scenario.json";
  if (!std::filesystem::exists(resolved_path)) {
    return;
  }

  const boost::json::value value = boost::json::parse(ReadText(resolved_path));
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             resolved_path.string());
  }
  const boost::json::object& object = value.as_object();
  options->nodes = JsonOptionalUint32Field(object, "nodes", options->nodes);
  const boost::json::value* isolated = object.if_contains("isolated_network");
  if (isolated != nullptr) {
    if (!isolated->is_bool()) {
      throw std::runtime_error(
          "resolved scenario isolated_network is not a boolean");
    }
    options->isolate_network = isolated->as_bool();
  }
  if (options->nodes < 1 || options->nodes > 2) {
    throw std::runtime_error(
        "cleanup currently supports resolved node counts in 1..2");
  }
}

void CleanupRun(Options options) {
  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  LoadCleanupMetadata(run_root, &options);
  RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");

  for (uint32_t i = 0; i < options.nodes; ++i) {
    DeleteNodeVethNetwork(MakeNodeVethConfig(options, i));
  }
  Cgroup::RemoveRun(options.run_id);

  std::cout << "cleanup_run=" << options.run_id << "\n"
            << "nodes=" << options.nodes << "\n"
            << "run_dir=" << run_root << "\n";
}

void StartNodes(const Options& options, const std::filesystem::path& run_root,
                const std::filesystem::path& events_path,
                const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  if (options.isolate_network) {
    RequireNetworkSetupCapabilities();
  }
  if (options.isolate_network && options.nodes > 1 &&
      !HostIpv4ForwardingEnabled()) {
    throw std::runtime_error(
        "isolated multi-node Firo runs require IPv4 forwarding in the parent "
        "network namespace");
  }
  nodes.reserve(options.nodes);
  for (uint32_t i = 0; i < options.nodes; ++i) {
    const std::string node_id = "firo-" + std::to_string(i + 1);
    const auto node_root = run_root / "nodes" / node_id;

    FiroNodeConfig config;
    config.id = node_id;
    config.binary = options.firod;
    config.data_dir = node_root / "data";
    config.log_dir = node_root;
    config.p2p_port = static_cast<uint16_t>(18168 + i);
    config.rpc_port = static_cast<uint16_t>(18888 + i);
    config.rpc_user = "sim-" + options.run_id;
    config.rpc_password = "pass-" + options.run_id + "-" + std::to_string(i);
    config.listen = true;
    if (i > 0) {
      config.connect_peers.push_back(FiroPeerHost(options, 0) + ":18168");
    }

    NodeRuntime runtime;
    try {
      runtime.config = config;
      WriteNodeState(events_path, options.run_id, node_id, "Preparing");
      if (options.isolate_network) {
        runtime.network_namespace = NetworkNamespace::Create();
        runtime.network = MakeNodeVethConfig(options, i);
        SetupNodeVethNetwork(runtime.network_namespace->fd(),
                             *runtime.network);
        if (runtime.network->apply_condition) {
          const QdiscInfo qdisc =
              VerifyNodeNetworkCondition(*runtime.network);
          WriteEvent(events_path, options.run_id, node_id,
                     "network_condition_verified",
                     NetworkConditionVerificationDetail(*runtime.network,
                                                        qdisc));
        }
        runtime.config.rpc_host = runtime.network->node_address;
        runtime.config.rpc_bind = runtime.network->node_address;
        runtime.config.rpc_allow_ips = {runtime.network->host_address};
        runtime.config.p2p_bind = runtime.network->node_address;
        WriteEvent(events_path, options.run_id, node_id, "network_ready",
                   "node_ip=" + runtime.network->node_address +
                       " host_if=" + runtime.network->host_name +
                       " peer_if=" + runtime.network->peer_name);
        WriteNodeState(events_path, options.run_id, node_id, "NetnsReady");
      }

      runtime.cgroup = Cgroup::Create(options.run_id, node_id);
      runtime.resources = InitialResourceLimits(options);
      runtime.cgroup->SetMemoryHigh(runtime.resources.memory_high_bytes);
      runtime.cgroup->SetMemoryMax(runtime.resources.memory_max_bytes);
      runtime.cgroup->SetCpuMax(runtime.resources.cpu_quota_us,
                                runtime.resources.cpu_period_us);
      runtime.cgroup->SetPidsMax(runtime.resources.pids_max);
      WriteNodeState(events_path, options.run_id, node_id, "CgroupReady");

      ProcessSpec process = driver.RenderProcess(runtime.config);
      if (runtime.network_namespace) {
        process.network_namespace_fd = runtime.network_namespace->fd();
      }
      WriteNodeState(events_path, options.run_id, node_id, "Starting");
      runtime.process = ChildProcess::Spawn(process, runtime.cgroup->path());
      BSIM_LOG(info) << "started " << node_id
                     << " pid=" << runtime.process.pid();
      WriteEvent(events_path, options.run_id, node_id, "process_started",
                 "pid=" + std::to_string(runtime.process.pid()));
      nodes.push_back(std::move(runtime));
    } catch (...) {
      WriteNodeState(events_path, options.run_id, node_id, "Failed");
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
    driver.WaitReady(node.config,
                     std::chrono::seconds(options.ready_timeout_sec));
    WriteEvent(events_path, options.run_id, node.config.id, "rpc_ready");
    WriteNodeState(events_path, options.run_id, node.config.id, "Running");
  }
}

std::string ResourceLimitUpdateDetail(const ResourceLimitPatch& patch,
                                      const ResourceLimits& previous,
                                      const ResourceLimits& current) {
  boost::json::object detail;
  detail["requested"] = ResourceLimitPatchJson(patch);
  detail["previous"] = ResourceLimitsJson(previous);
  detail["current"] = ResourceLimitsJson(current);
  return boost::json::serialize(detail);
}

void ApplyRuntimeResourceLimitUpdates(
    const Options& options, const std::filesystem::path& events_path,
    std::vector<NodeRuntime>& nodes) {
  for (const auto& [node_index, patch] : options.runtime_node_resource_updates) {
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime resource update node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    if (!node.cgroup) {
      throw std::runtime_error(
          "runtime resource update requires a node cgroup");
    }
    const ResourceLimits previous = node.resources;
    const ResourceLimits next =
        ApplyResourceLimitPatch(previous, patch, node.config.id);
    WriteResourceLimits(*node.cgroup, previous, next);
    node.resources = next;
    WriteEvent(events_path, options.run_id, node.config.id,
               "resource_limits_updated",
               ResourceLimitUpdateDetail(patch, previous, next));
  }
}

void ApplyRuntimeNetworkConditionUpdates(
    const Options& options, const std::filesystem::path& events_path,
    std::vector<NodeRuntime>& nodes) {
  for (const auto& [node_index, condition] :
       options.runtime_node_network_conditions) {
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime network condition node is out of range");
    }
    NodeRuntime& node = nodes[node_index];
    if (!node.network) {
      throw std::runtime_error(
          "runtime network condition requires isolated networking");
    }
    node.network->apply_condition = true;
    node.network->condition = condition;
    ReplaceNetworkConditionQdisc(node.network->host_name,
                                 node.network->condition);
    const QdiscInfo qdisc = VerifyNodeNetworkCondition(*node.network);
    WriteEvent(events_path, options.run_id, node.config.id,
               "network_condition_updated",
               NetworkConditionVerificationDetail(*node.network, qdisc));
  }
}

std::string RestartDetail(pid_t pid, uint64_t restart_count) {
  boost::json::object detail;
  detail["pid"] = pid;
  detail["restart_count"] = restart_count;
  return boost::json::serialize(detail);
}

void RestartNode(const Options& options, const std::filesystem::path& events_path,
                 const FiroDriver& driver, NodeRuntime& node) {
  if (!node.cgroup) {
    throw std::runtime_error("node restart requires a node cgroup");
  }

  WriteNodeState(events_path, options.run_id, node.config.id, "Restarting");
  WriteEvent(events_path, options.run_id, node.config.id, "restart_requested",
             "restart_count=" + std::to_string(node.restart_count + 1U));
  driver.Stop(node.config);
  if (!node.process.WaitForExit(std::chrono::seconds(15))) {
    WriteEvent(events_path, options.run_id, node.config.id, "sigterm");
    node.process.Terminate(std::chrono::seconds(5));
  }
  WriteNodeLogTail(events_path, options, driver, node);

  ProcessSpec process = driver.RenderProcess(node.config);
  if (node.network_namespace) {
    process.network_namespace_fd = node.network_namespace->fd();
  }
  WriteNodeState(events_path, options.run_id, node.config.id, "Starting");
  node.process = ChildProcess::Spawn(process, node.cgroup->path());
  ++node.restart_count;
  WriteEvent(events_path, options.run_id, node.config.id, "process_restarted",
             RestartDetail(node.process.pid(), node.restart_count));
  driver.WaitReady(node.config, std::chrono::seconds(options.ready_timeout_sec));
  WriteEvent(events_path, options.run_id, node.config.id, "rpc_ready");
  WriteNodeState(events_path, options.run_id, node.config.id, "Running");
  WriteNodeLogTail(events_path, options, driver, node);
}

void ApplyRuntimeNodeRestarts(const Options& options,
                              const std::filesystem::path& events_path,
                              const FiroDriver& driver,
                              std::vector<NodeRuntime>& nodes) {
  for (uint32_t node_index : options.runtime_node_restarts) {
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime restart node is out of range");
    }
    RestartNode(options, events_path, driver, nodes[node_index]);
  }
}

bool WaitForNodeFrozenState(const Cgroup& cgroup, bool expected) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    if (cgroup.Frozen() == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

std::string FreezeDetail(uint32_t duration_ms, bool frozen) {
  boost::json::object detail;
  detail["duration_ms"] = duration_ms;
  detail["frozen"] = frozen;
  return boost::json::serialize(detail);
}

void FreezeNodeForDuration(const Options& options,
                           const std::filesystem::path& events_path,
                           NodeRuntime& node, uint32_t duration_ms) {
  if (!node.cgroup) {
    throw std::runtime_error("node freeze requires a node cgroup");
  }

  node.cgroup->Freeze();
  try {
    if (!WaitForNodeFrozenState(*node.cgroup, true)) {
      throw std::runtime_error("node cgroup did not report frozen: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id, "cgroup_frozen",
               FreezeDetail(duration_ms, true));
    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
    node.cgroup->Thaw();
    if (!WaitForNodeFrozenState(*node.cgroup, false)) {
      throw std::runtime_error("node cgroup did not report thawed: " +
                               node.config.id);
    }
    WriteEvent(events_path, options.run_id, node.config.id, "cgroup_thawed",
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
                             std::vector<NodeRuntime>& nodes) {
  for (const FreezeRequest& freeze : options.runtime_node_freezes) {
    if (freeze.node_index >= nodes.size()) {
      throw std::runtime_error("runtime freeze node is out of range");
    }
    FreezeNodeForDuration(options, events_path, nodes[freeze.node_index],
                          freeze.duration_ms);
  }
}

void StopNodes(const Options& options, const std::filesystem::path& events_path,
               const FiroDriver& driver, std::vector<NodeRuntime>& nodes) {
  for (auto& node : nodes) {
    WriteNodeState(events_path, options.run_id, node.config.id, "Stopping");
    WriteEvent(events_path, options.run_id, node.config.id, "rpc_stop");
    driver.Stop(node.config);
  }
  for (auto& node : nodes) {
    if (!node.process.WaitForExit(std::chrono::seconds(15))) {
      WriteEvent(events_path, options.run_id, node.config.id, "sigterm");
      node.process.Terminate(std::chrono::seconds(5));
    }
    WriteNodeState(events_path, options.run_id, node.config.id, "Stopped");
    WriteNodeState(events_path, options.run_id, node.config.id, "Cleaning");
    if (node.cgroup && !options.keep_cgroups) {
      node.cgroup->KillAll();
      try {
        node.cgroup->Remove();
      } catch (const std::exception& e) {
        WriteEvent(events_path, options.run_id, node.config.id,
                   "cgroup_remove_failed", e.what());
      }
    }
    if (node.network) {
      DeleteNodeVethNetwork(*node.network);
      WriteEvent(events_path, options.run_id, node.config.id,
                 "network_removed");
    }
    if (node.network_namespace) {
      node.network_namespace->Stop();
    }
    WriteNodeState(events_path, options.run_id, node.config.id, "Cleaned");
  }
  if (!options.keep_cgroups) {
    try {
      Cgroup::RemoveRun(options.run_id);
    } catch (const std::exception& e) {
      WriteEvent(events_path, options.run_id, "sim", "run_cgroup_remove_failed",
                 e.what());
    }
  }
}

int Run(int argc, char** argv) {
  Options options = ParseOptions(argc, argv);
  RequireSafeOutputDirectory(options.output_dir);
  if (options.probe_network) {
    std::cout << NetworkProbeJson() << "\n";
    return 0;
  }
  if (!options.report_run.empty()) {
    std::cout << BuildRunReportJson(options.report_run) << "\n";
    return 0;
  }
  if (options.probe_capabilities) {
    std::cout << CapabilityProbeJson() << "\n";
    return 0;
  }
  if (options.probe_cgroup_freeze) {
    std::cout << CgroupFreezeProbeJson() << "\n";
    return 0;
  }
  if (options.probe_netns) {
    RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
    std::cout << NetworkNamespaceProbeJson() << "\n";
    return 0;
  }
  if (options.probe_veth) {
    RequireNetworkSetupCapabilities();
    std::cout << VethProbeJson() << "\n";
    return 0;
  }
  if (options.probe_bandwidth_limit) {
    RequireNetworkSetupCapabilities();
    std::cout << BandwidthLimitProbeJson() << "\n";
    return 0;
  }
  if (options.probe_network_condition) {
    RequireNetworkSetupCapabilities();
    std::cout << NetworkConditionProbeJson() << "\n";
    return 0;
  }
  if (options.probe_combined_network_condition) {
    RequireNetworkSetupCapabilities();
    std::cout << CombinedNetworkConditionProbeJson() << "\n";
    return 0;
  }
  if (options.probe_network_condition_update) {
    RequireNetworkSetupCapabilities();
    std::cout << NetworkConditionUpdateProbeJson() << "\n";
    return 0;
  }
  if (options.probe_address) {
    RequireNetworkSetupCapabilities();
    std::cout << AddressProbeJson() << "\n";
    return 0;
  }
  if (options.probe_route) {
    RequireNetworkSetupCapabilities();
    std::cout << RouteProbeJson() << "\n";
    return 0;
  }
  if (options.probe_qdisc) {
    RequireNetworkSetupCapabilities();
    std::cout << QdiscProbeJson() << "\n";
    return 0;
  }
  if (options.probe_qdisc_mutation) {
    RequireNetworkSetupCapabilities();
    std::cout << QdiscMutationProbeJson() << "\n";
    return 0;
  }
  if (options.cleanup_run) {
    CleanupRun(options);
    return 0;
  }

  BSIM_LOG(info) << "starting run " << options.run_id;
  const auto run_root =
      std::filesystem::absolute(options.output_dir) / options.run_id;
  if (std::filesystem::exists(run_root)) {
    if (!options.replace_run) {
      throw std::runtime_error("run directory already exists: " +
                               run_root.string() +
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
  WriteText(run_root / kRunMarkerFile, "benchmark-sim run\n");
  EnsureDirectory(run_root / "nodes");
  WriteScenarioFiles(options, run_root);

  const auto events_path = run_root / "events.jsonl";
  const auto metrics_path = run_root / "metrics.jsonl";
  WriteEvent(events_path, options.run_id, "sim", "run_started");

  FiroDriver driver(std::chrono::seconds(5));
  std::vector<NodeRuntime> nodes;
  try {
    StartNodes(options, run_root, events_path, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);

    WriteMetricsSnapshot(metrics_path, options, driver, nodes);

    ApplyRuntimeResourceLimitUpdates(options, events_path, nodes);
    ApplyRuntimeNetworkConditionUpdates(options, events_path, nodes);
    ApplyRuntimeNodeRestarts(options, events_path, driver, nodes);
    ApplyRuntimeNodeFreezes(options, events_path, nodes);
    WritePeriodicMetrics(events_path, metrics_path, options, driver, nodes);

    if (options.generate_blocks > 0) {
      const uint64_t start_height =
          driver.ReadMetrics(nodes.front().config).height;
      std::vector<std::string> hashes = driver.GenerateBlocks(
          nodes.front().config, options.generate_blocks, kDefaultRewardAddress);
      nodes.front().generated_block_count += hashes.size();
      WriteEvent(events_path, options.run_id, nodes.front().config.id,
                 "generated_blocks", std::to_string(hashes.size()));
      const uint64_t target_height =
          start_height + static_cast<uint64_t>(hashes.size());
      for (auto& node : nodes) {
        driver.WaitForHeight(node.config, target_height,
                             std::chrono::seconds(options.sync_timeout_sec));
        WriteEvent(events_path, options.run_id, node.config.id,
                   "height_reached", std::to_string(target_height));
      }
    }

    WriteMetricsSnapshot(metrics_path, options, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);

    StopNodes(options, events_path, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);
    WriteEvent(events_path, options.run_id, "sim", "run_finished");
    BSIM_LOG(info) << "finished run " << options.run_id;
  } catch (...) {
    for (auto& node : nodes) {
      WriteNodeState(events_path, options.run_id, node.config.id, "Failed");
    }
    WriteNodeLogTails(events_path, options, driver, nodes);
    StopNodes(options, events_path, driver, nodes);
    WriteNodeLogTails(events_path, options, driver, nodes);
    WriteEvent(events_path, options.run_id, "sim", "run_failed");
    throw;
  }

  std::cout << "run_id=" << options.run_id << "\n"
            << "output_dir=" << run_root << "\n"
            << "metrics=" << metrics_path << "\n"
            << "events=" << events_path << "\n";
  return 0;
}

}  // namespace
}  // namespace bsim

int main(int argc, char** argv) {
  try {
    bsim::InitLogging();
    return bsim::Run(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "benchmark-sim: " << e.what() << "\n";
    return 1;
  }
}
