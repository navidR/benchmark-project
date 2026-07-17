#include "bbp/node_file_pane.h"

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cctype>
#include <cstdint>
#include <string>
#include <utility>

#include "bbp/node_artifact_inventory.h"

namespace bbp {
namespace {

constexpr std::size_t kMaximumConfigurationLines = 256U;
constexpr std::size_t kMaximumConfigurationLineBytes = 1024U;
constexpr std::size_t kMaximumConfigurationDepth = 16U;

const boost::json::object* NodeAt(const boost::json::object& report,
                                  std::size_t selected_node) {
  const boost::json::value* nodes_value = report.if_contains("nodes_summary");
  if (nodes_value == nullptr || !nodes_value->is_array()) {
    return nullptr;
  }
  const boost::json::array& nodes = nodes_value->as_array();
  if (selected_node >= nodes.size() || !nodes[selected_node].is_object()) {
    return nullptr;
  }
  return &nodes[selected_node].as_object();
}

std::string JsonString(const boost::json::object& object,
                       std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_string()
             ? std::string(value->as_string())
             : std::string{};
}

const boost::json::object* FindNodeConfig(const boost::json::object& report,
                                          std::string_view node_id) {
  const boost::json::value* configs_value = report.if_contains("node_configs");
  if (configs_value == nullptr || !configs_value->is_array()) {
    return nullptr;
  }
  for (const boost::json::value& config_value : configs_value->as_array()) {
    if (config_value.is_object() &&
        JsonString(config_value.as_object(), "id") == node_id) {
      return &config_value.as_object();
    }
  }
  return nullptr;
}

const boost::json::object* LastMetrics(const boost::json::object& node) {
  const boost::json::value* value = node.if_contains("last_metrics");
  return value != nullptr && value->is_object() ? &value->as_object() : nullptr;
}

std::string BoundedLine(std::string line) {
  if (line.size() <= kMaximumConfigurationLineBytes) {
    return line;
  }
  line.resize(kMaximumConfigurationLineBytes - 3U);
  line += "...";
  return line;
}

bool IsSensitiveConfigurationPath(std::string_view path) {
  std::string lower;
  lower.reserve(path.size());
  std::transform(path.begin(), path.end(), std::back_inserter(lower),
                 [](unsigned char character) {
                   return static_cast<char>(std::tolower(character));
                 });
  constexpr std::array<std::string_view, 4> kSensitiveNames = {
      "password", "secret", "token", "cookie"};
  return std::any_of(kSensitiveNames.begin(), kSensitiveNames.end(),
                     [&](std::string_view name) {
                       return lower.find(name) != std::string::npos;
                     });
}

std::string EscapeConfigurationPathComponent(std::string_view component) {
  constexpr std::string_view kHex = "0123456789ABCDEF";
  std::string escaped;
  escaped.reserve(component.size());
  for (const unsigned char character : component) {
    if (character >= 0x20U && character <= 0x7eU && character != '\\') {
      escaped.push_back(static_cast<char>(character));
    } else if (character == '\\') {
      escaped += "\\\\";
    } else {
      escaped += "\\x";
      escaped.push_back(kHex[character >> 4U]);
      escaped.push_back(kHex[character & 0x0fU]);
    }
  }
  return escaped;
}

struct ConfigurationAppendState {
  std::vector<std::string>* lines = nullptr;
  bool line_limit_truncated = false;
};

bool AppendConfigurationLine(std::string line,
                             ConfigurationAppendState* state) {
  if (state->lines->size() >= kMaximumConfigurationLines) {
    state->line_limit_truncated = true;
    return false;
  }
  state->lines->push_back(BoundedLine(std::move(line)));
  return true;
}

void AppendConfigurationValue(const boost::json::value& value,
                              const std::string& path, std::size_t depth,
                              ConfigurationAppendState* state) {
  if (state->line_limit_truncated) {
    return;
  }
  if (IsSensitiveConfigurationPath(path)) {
    static_cast<void>(
        AppendConfigurationLine(path + " = \"<redacted>\"", state));
    return;
  }
  const bool nonempty_container =
      (value.is_object() && !value.as_object().empty()) ||
      (value.is_array() && !value.as_array().empty());
  if (nonempty_container && depth >= kMaximumConfigurationDepth) {
    static_cast<void>(AppendConfigurationLine(
        path + " = \"<nested value truncated at depth " +
            std::to_string(kMaximumConfigurationDepth) + ">\"",
        state));
    return;
  }
  if (value.is_object()) {
    if (value.as_object().empty()) {
      static_cast<void>(AppendConfigurationLine(path + " = {}", state));
      return;
    }
    for (const auto& member : value.as_object()) {
      const std::string key = EscapeConfigurationPathComponent(member.key());
      AppendConfigurationValue(member.value(),
                               path.empty() ? key : path + "." + key,
                               depth + 1U, state);
      if (state->line_limit_truncated) {
        return;
      }
    }
    return;
  }
  if (value.is_array()) {
    if (value.as_array().empty()) {
      static_cast<void>(AppendConfigurationLine(path + " = []", state));
      return;
    }
    for (std::size_t index = 0U; index < value.as_array().size(); ++index) {
      AppendConfigurationValue(value.as_array()[index],
                               path + "[" + std::to_string(index) + "]",
                               depth + 1U, state);
      if (state->line_limit_truncated) {
        return;
      }
    }
    return;
  }
  static_cast<void>(AppendConfigurationLine(
      path + " = " + boost::json::serialize(value), state));
}

std::string BytesText(std::uint64_t bytes) {
  constexpr std::uint64_t kKiB = 1024U;
  constexpr std::uint64_t kMiB = kKiB * 1024U;
  constexpr std::uint64_t kGiB = kMiB * 1024U;
  if (bytes >= kGiB) {
    return std::to_string(bytes / kGiB) + "GiB";
  }
  if (bytes >= kMiB) {
    return std::to_string(bytes / kMiB) + "MiB";
  }
  if (bytes >= kKiB) {
    return std::to_string(bytes / kKiB) + "KiB";
  }
  return std::to_string(bytes) + "B";
}

std::string ArtifactLine(const NodeArtifactEntry& entry) {
  char marker = '?';
  switch (entry.type) {
    case NodeArtifactType::kDirectory:
      marker = 'D';
      break;
    case NodeArtifactType::kRegularFile:
      marker = 'F';
      break;
    case NodeArtifactType::kSymbolicLink:
      marker = 'L';
      break;
    case NodeArtifactType::kOther:
      marker = '?';
      break;
  }
  std::string line(1U, marker);
  line += "  ";
  if (entry.type == NodeArtifactType::kRegularFile) {
    line += BytesText(entry.size_bytes);
  } else {
    line += "-";
  }
  line += "  " + entry.relative_path;
  if (entry.type == NodeArtifactType::kDirectory) {
    line += '/';
  } else if (entry.type == NodeArtifactType::kSymbolicLink) {
    line += " [not followed]";
  }
  return line;
}

void AppendRuntimeConfiguration(const boost::json::object& node,
                                ConfigurationAppendState* state) {
  const boost::json::object* metrics = LastMetrics(node);
  if (metrics == nullptr) {
    return;
  }
  constexpr std::array<std::string_view, 10> kFields = {
      "data_dir",       "log_dir",        "rpc_host",
      "rpc_port",       "host_interface", "child_interface",
      "host_address",   "node_address",   "network_prefix_length",
      "network_routes",
  };
  if (!AppendConfigurationLine("runtime (latest normalized metrics):", state)) {
    return;
  }
  for (const std::string_view field : kFields) {
    const boost::json::value* value = metrics->if_contains(field);
    if (value != nullptr) {
      AppendConfigurationValue(*value, "runtime." + std::string(field), 0U,
                               state);
      if (state->line_limit_truncated) {
        return;
      }
    }
  }
}

}  // namespace

std::string_view NodeFileSectionName(NodeFileSection section) {
  switch (section) {
    case NodeFileSection::kDataDirectory:
      return "data";
    case NodeFileSection::kConfiguration:
      return "configuration";
    case NodeFileSection::kLogFiles:
      return "logs";
  }
  return "data";
}

void NodeFilePane::Toggle(const std::filesystem::path& run_root,
                          const boost::json::object& report,
                          std::size_t selected_node) {
  open_ = !open_;
  if (open_) {
    Load(run_root, report, selected_node, true);
  }
}

void NodeFilePane::Close() { open_ = false; }

void NodeFilePane::Refresh(const std::filesystem::path& run_root,
                           const boost::json::object& report,
                           std::size_t selected_node) {
  if (!open_) {
    return;
  }
  const boost::json::object* node = NodeAt(report, selected_node);
  const std::string next_node_id =
      node == nullptr ? std::string{} : JsonString(*node, "node_id");
  if (next_node_id != node_id_) {
    Load(run_root, report, selected_node, true);
  }
}

void NodeFilePane::Reload(const std::filesystem::path& run_root,
                          const boost::json::object& report,
                          std::size_t selected_node) {
  if (open_) {
    Load(run_root, report, selected_node, false);
  }
}

void NodeFilePane::PreviousSection() {
  switch (section_) {
    case NodeFileSection::kDataDirectory:
      section_ = NodeFileSection::kLogFiles;
      break;
    case NodeFileSection::kConfiguration:
      section_ = NodeFileSection::kDataDirectory;
      break;
    case NodeFileSection::kLogFiles:
      section_ = NodeFileSection::kConfiguration;
      break;
  }
  first_visible_line_ = 0U;
}

void NodeFilePane::NextSection() {
  switch (section_) {
    case NodeFileSection::kDataDirectory:
      section_ = NodeFileSection::kConfiguration;
      break;
    case NodeFileSection::kConfiguration:
      section_ = NodeFileSection::kLogFiles;
      break;
    case NodeFileSection::kLogFiles:
      section_ = NodeFileSection::kDataDirectory;
      break;
  }
  first_visible_line_ = 0U;
}

void NodeFilePane::ScrollUp(std::size_t visible_rows, std::size_t line_count) {
  const std::size_t delta = std::min(first_visible_line_, line_count);
  first_visible_line_ -= delta;
  first_visible_line_ =
      std::min(first_visible_line_, MaximumScroll(visible_rows));
}

void NodeFilePane::ScrollDown(std::size_t visible_rows,
                              std::size_t line_count) {
  const std::size_t maximum = MaximumScroll(visible_rows);
  const std::size_t remaining =
      first_visible_line_ < maximum ? maximum - first_visible_line_ : 0U;
  first_visible_line_ += std::min(remaining, line_count);
}

void NodeFilePane::ScrollHome() { first_visible_line_ = 0U; }

void NodeFilePane::ScrollEnd(std::size_t visible_rows) {
  first_visible_line_ = MaximumScroll(visible_rows);
}

bool NodeFilePane::IsOpen() const { return open_; }

std::string_view NodeFilePane::NodeId() const { return node_id_; }

NodeFileSection NodeFilePane::Section() const { return section_; }

const std::vector<std::string>& NodeFilePane::Lines() const {
  switch (section_) {
    case NodeFileSection::kDataDirectory:
      return data_lines_;
    case NodeFileSection::kConfiguration:
      return configuration_lines_;
    case NodeFileSection::kLogFiles:
      return log_lines_;
  }
  return data_lines_;
}

std::size_t NodeFilePane::FirstVisibleLine(std::size_t visible_rows) const {
  return std::min(first_visible_line_, MaximumScroll(visible_rows));
}

std::size_t NodeFilePane::LastVisibleLine(std::size_t visible_rows) const {
  return std::min(Lines().size(),
                  FirstVisibleLine(visible_rows) + visible_rows);
}

void NodeFilePane::Load(const std::filesystem::path& run_root,
                        const boost::json::object& report,
                        std::size_t selected_node, bool reset_section) {
  const boost::json::object* node = NodeAt(report, selected_node);
  node_id_ = node == nullptr ? std::string{} : JsonString(*node, "node_id");
  data_lines_.clear();
  configuration_lines_.clear();
  log_lines_.clear();
  first_visible_line_ = 0U;
  if (reset_section) {
    section_ = NodeFileSection::kDataDirectory;
  }
  if (node == nullptr || node_id_.empty()) {
    data_lines_.push_back("No node artifact inventory is available.");
    configuration_lines_.push_back("No node configuration is available.");
    log_lines_.push_back("No node log-file inventory is available.");
    return;
  }

  const boost::json::object* config = FindNodeConfig(report, node_id_);
  const std::filesystem::path data_directory =
      config == nullptr
          ? std::filesystem::path{}
          : std::filesystem::path(JsonString(*config, "data_dir"));
  const NodeArtifactInventory inventory =
      InspectNodeArtifacts(run_root, node_id_, data_directory);
  data_lines_.push_back("directory: " + inventory.data_directory);
  if (!inventory.error.empty()) {
    data_lines_.push_back("error: " + inventory.error);
  } else if (inventory.data_entries.empty()) {
    data_lines_.push_back("No data-directory entries.");
  } else {
    for (const NodeArtifactEntry& entry : inventory.data_entries) {
      data_lines_.push_back(ArtifactLine(entry));
    }
  }
  if (inventory.data_entries_truncated) {
    data_lines_.push_back("[data inventory truncated at a safe bound]");
  }
  if (!inventory.warning.empty()) {
    data_lines_.push_back("warning: " + inventory.warning);
  }

  ConfigurationAppendState configuration_state{.lines = &configuration_lines_};
  static_cast<void>(AppendConfigurationLine(
      "source: resolved-scenario.json / node_configs", &configuration_state));
  if (config == nullptr) {
    static_cast<void>(AppendConfigurationLine(
        "No canonical node configuration is present in the report.",
        &configuration_state));
  } else {
    AppendConfigurationValue(*config, "node", 0U, &configuration_state);
  }
  AppendRuntimeConfiguration(*node, &configuration_state);
  if (configuration_state.line_limit_truncated) {
    if (configuration_lines_.size() < kMaximumConfigurationLines) {
      configuration_lines_.push_back(
          "[configuration view truncated at a safe line bound]");
    } else {
      configuration_lines_.back() =
          "[configuration view truncated at a safe line bound]";
    }
  }

  log_lines_.push_back(
      "contents: press l for bounded normalized daemon/stdout/stderr tails");
  if (!inventory.error.empty()) {
    log_lines_.push_back("error: " + inventory.error);
  } else if (inventory.log_files.empty()) {
    log_lines_.push_back("No regular .log files.");
  } else {
    for (const NodeArtifactEntry& entry : inventory.log_files) {
      log_lines_.push_back(ArtifactLine(entry));
    }
  }
  if (inventory.log_files_truncated) {
    log_lines_.push_back("[log-file inventory truncated at a safe bound]");
  }
  if (!inventory.warning.empty()) {
    log_lines_.push_back("warning: " + inventory.warning);
  }
}

std::size_t NodeFilePane::MaximumScroll(std::size_t visible_rows) const {
  return Lines().size() > visible_rows ? Lines().size() - visible_rows : 0U;
}

}  // namespace bbp
