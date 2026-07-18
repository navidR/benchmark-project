#include "bbp/network_rule_pane.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <limits>

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

const boost::json::object* LastMetrics(const boost::json::object& node) {
  const boost::json::value* value = node.if_contains("last_metrics");
  return value != nullptr && value->is_object() ? &value->as_object() : nullptr;
}

std::string JsonString(const boost::json::object& object,
                       std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_string()
             ? std::string(value->as_string())
             : std::string{};
}

std::uint64_t JsonUnsigned(const boost::json::object& object,
                           std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return 0U;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  return 0U;
}

}  // namespace

void NetworkRulePane::Toggle(const boost::json::object& report,
                             std::size_t selected_node) {
  open_ = !open_;
  if (open_) {
    Load(report, selected_node);
  }
}

void NetworkRulePane::Close() { open_ = false; }

void NetworkRulePane::Refresh(const boost::json::object& report,
                              std::size_t selected_node) {
  if (open_) {
    Load(report, selected_node);
  }
}

void NetworkRulePane::ScrollUp(std::size_t visible_rows,
                               std::size_t line_count) {
  const std::size_t delta = std::min(first_visible_rule_, line_count);
  first_visible_rule_ -= delta;
  first_visible_rule_ =
      std::min(first_visible_rule_, MaximumScroll(visible_rows));
}

void NetworkRulePane::ScrollDown(std::size_t visible_rows,
                                 std::size_t line_count) {
  const std::size_t maximum = MaximumScroll(visible_rows);
  const std::size_t remaining =
      first_visible_rule_ < maximum ? maximum - first_visible_rule_ : 0U;
  first_visible_rule_ += std::min(remaining, line_count);
}

void NetworkRulePane::ScrollHome() { first_visible_rule_ = 0U; }

void NetworkRulePane::ScrollEnd(std::size_t visible_rows) {
  first_visible_rule_ = MaximumScroll(visible_rows);
}

bool NetworkRulePane::IsOpen() const { return open_; }

std::string_view NetworkRulePane::NodeId() const { return node_id_; }

const std::vector<NetworkRuleSummary>& NetworkRulePane::Rules() const {
  return rules_;
}

std::size_t NetworkRulePane::FirstVisibleRule(std::size_t visible_rows) const {
  return std::min(first_visible_rule_, MaximumScroll(visible_rows));
}

std::size_t NetworkRulePane::LastVisibleRule(std::size_t visible_rows) const {
  return std::min(rules_.size(), FirstVisibleRule(visible_rows) + visible_rows);
}

void NetworkRulePane::Load(const boost::json::object& report,
                           std::size_t selected_node) {
  const boost::json::object* node = NodeAt(report, selected_node);
  if (node == nullptr) {
    node_id_.clear();
    rules_.clear();
    first_visible_rule_ = 0U;
    return;
  }
  const std::string next_node_id = JsonString(*node, "node_id");
  if (next_node_id != node_id_) {
    first_visible_rule_ = 0U;
  }
  node_id_ = next_node_id;
  rules_.clear();
  const boost::json::object* metrics = LastMetrics(*node);
  const boost::json::value* rules_value =
      metrics == nullptr ? nullptr
                         : metrics->if_contains("network_active_block_rules");
  if (rules_value == nullptr || !rules_value->is_array()) {
    first_visible_rule_ = 0U;
    return;
  }
  rules_.reserve(rules_value->as_array().size());
  for (const boost::json::value& value : rules_value->as_array()) {
    if (!value.is_object()) {
      continue;
    }
    const boost::json::object& object = value.as_object();
    const std::uint64_t handle = JsonUnsigned(object, "handle");
    const std::uint64_t source_port = JsonUnsigned(object, "src_port");
    const std::uint64_t port = JsonUnsigned(object, "dst_port");
    if (handle == 0U || handle > std::numeric_limits<std::uint32_t>::max() ||
        source_port > std::numeric_limits<std::uint16_t>::max() || port == 0U ||
        port > std::numeric_limits<std::uint16_t>::max() ||
        JsonString(object, "dst_address").empty()) {
      continue;
    }
    rules_.push_back(NetworkRuleSummary{
        .handle = static_cast<std::uint32_t>(handle),
        .source_address = JsonString(object, "src_address"),
        .source_port = static_cast<std::uint16_t>(source_port),
        .destination_address = JsonString(object, "dst_address"),
        .destination_port = static_cast<std::uint16_t>(port),
        .match_packets = JsonUnsigned(object, "match_packets"),
        .drop_packets = JsonUnsigned(object, "drop_packets"),
    });
  }
  first_visible_rule_ = std::min(first_visible_rule_, rules_.size());
}

std::size_t NetworkRulePane::MaximumScroll(std::size_t visible_rows) const {
  return rules_.size() > visible_rows ? rules_.size() - visible_rows : 0U;
}

}  // namespace bbp
