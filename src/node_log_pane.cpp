#include "bbp/node_log_pane.h"

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <sstream>
#include <utility>

namespace bbp {
namespace {

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
  if (value == nullptr || !value->is_string()) {
    return {};
  }
  return std::string(value->as_string());
}

bool JsonBool(const boost::json::object& object, std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_bool() && value->as_bool();
}

const boost::json::object* LogDetail(const boost::json::object& node,
                                     ChainLogSource source) {
  const boost::json::value* tails_value = node.if_contains("log_tails");
  if (tails_value == nullptr || !tails_value->is_object()) {
    return nullptr;
  }
  const boost::json::object& tails = tails_value->as_object();
  const boost::json::value* detail =
      tails.if_contains(ChainLogSourceName(source));
  if (detail == nullptr || !detail->is_object()) {
    return nullptr;
  }
  return &detail->as_object();
}

std::vector<std::string> SplitLogLines(std::string_view text) {
  std::istringstream input{std::string(text)};
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(input, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    std::replace(line.begin(), line.end(), '\t', ' ');
    lines.push_back(std::move(line));
  }
  return lines;
}

}  // namespace

void NodeLogPane::Toggle(const boost::json::object& report,
                         std::size_t selected_node) {
  open_ = !open_;
  if (open_) {
    Load(report, selected_node);
  }
}

void NodeLogPane::Close() { open_ = false; }

void NodeLogPane::Refresh(const boost::json::object& report,
                          std::size_t selected_node) {
  if (open_) {
    Load(report, selected_node);
  }
}

void NodeLogPane::ScrollUp(std::size_t visible_rows, std::size_t line_count) {
  lines_from_bottom_ =
      std::min(MaximumScroll(visible_rows), lines_from_bottom_ + line_count);
}

void NodeLogPane::ScrollDown(std::size_t visible_rows, std::size_t line_count) {
  lines_from_bottom_ =
      std::min(lines_from_bottom_, MaximumScroll(visible_rows));
  lines_from_bottom_ =
      line_count >= lines_from_bottom_ ? 0U : lines_from_bottom_ - line_count;
}

void NodeLogPane::ScrollHome(std::size_t visible_rows) {
  lines_from_bottom_ = MaximumScroll(visible_rows);
}

void NodeLogPane::ScrollEnd() { lines_from_bottom_ = 0; }

bool NodeLogPane::IsOpen() const { return open_; }

std::string_view NodeLogPane::NodeId() const { return node_id_; }

std::string_view NodeLogPane::SourceName() const {
  return ChainLogSourceName(source_);
}

const std::vector<std::string>& NodeLogPane::Lines() const { return lines_; }

std::size_t NodeLogPane::FirstVisibleLine(std::size_t visible_rows) const {
  if (visible_rows == 0U || lines_.empty()) {
    return 0;
  }
  const std::size_t end = LastVisibleLine(visible_rows);
  return end > visible_rows ? end - visible_rows : 0U;
}

std::size_t NodeLogPane::LastVisibleLine(std::size_t visible_rows) const {
  return lines_.size() -
         std::min(lines_from_bottom_, MaximumScroll(visible_rows));
}

void NodeLogPane::Load(const boost::json::object& report,
                       std::size_t selected_node) {
  const boost::json::object* node = NodeAt(report, selected_node);
  if (node == nullptr) {
    node_id_.clear();
    lines_.clear();
    lines_from_bottom_ = 0;
    return;
  }

  constexpr std::array<ChainLogSource, 3> kSourcePreference = {
      ChainLogSource::kDaemon,
      ChainLogSource::kStderr,
      ChainLogSource::kStdout,
  };
  ChainLogSource selected_source = ChainLogSource::kDaemon;
  const boost::json::object* selected_detail = nullptr;
  for (ChainLogSource source : kSourcePreference) {
    const boost::json::object* detail = LogDetail(*node, source);
    if (detail == nullptr) {
      continue;
    }
    if (selected_detail == nullptr) {
      selected_source = source;
      selected_detail = detail;
    }
    if (!JsonString(*detail, "text").empty()) {
      selected_source = source;
      selected_detail = detail;
      break;
    }
  }

  const std::string next_node_id = JsonString(*node, "node_id");
  const bool source_changed =
      next_node_id != node_id_ || selected_source != source_;
  const std::size_t previous_line_count = lines_.size();
  const bool offset_reset =
      selected_detail != nullptr && JsonBool(*selected_detail, "offset_reset");
  node_id_ = next_node_id;
  source_ = selected_source;
  lines_ = selected_detail == nullptr
               ? std::vector<std::string>{}
               : SplitLogLines(JsonString(*selected_detail, "text"));
  if (source_changed || offset_reset) {
    lines_from_bottom_ = 0;
  } else {
    if (lines_from_bottom_ != 0U && lines_.size() > previous_line_count) {
      lines_from_bottom_ += lines_.size() - previous_line_count;
    }
    lines_from_bottom_ = std::min(lines_from_bottom_, lines_.size());
  }
}

std::size_t NodeLogPane::MaximumScroll(std::size_t visible_rows) const {
  return lines_.size() > visible_rows ? lines_.size() - visible_rows : 0U;
}

}  // namespace bbp
