#include "bbp/peer_list_pane.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
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

const boost::json::object* LastMetrics(const boost::json::object& node) {
  const boost::json::value* value = node.if_contains("last_metrics");
  return value != nullptr && value->is_object() ? &value->as_object() : nullptr;
}

std::uint64_t JsonUnsigned(const boost::json::object& object,
                           std::string_view field, std::uint64_t fallback) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return fallback;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  return fallback;
}

std::vector<std::string> PeerAddresses(const boost::json::object& metrics) {
  const boost::json::value* value = metrics.if_contains("peer_addresses");
  if (value == nullptr || !value->is_array()) {
    return {};
  }
  std::vector<std::string> peers;
  peers.reserve(value->as_array().size());
  for (const boost::json::value& peer : value->as_array()) {
    if (peer.is_string()) {
      peers.emplace_back(peer.as_string());
    }
  }
  return peers;
}

}  // namespace

void PeerListPane::Toggle(const boost::json::object& report,
                          std::size_t selected_node) {
  open_ = !open_;
  if (open_) {
    Load(report, selected_node);
  }
}

void PeerListPane::Close() { open_ = false; }

void PeerListPane::Refresh(const boost::json::object& report,
                           std::size_t selected_node) {
  if (open_) {
    Load(report, selected_node);
  }
}

void PeerListPane::ScrollUp(std::size_t visible_rows, std::size_t line_count) {
  const std::size_t delta = std::min(first_visible_peer_, line_count);
  first_visible_peer_ -= delta;
  first_visible_peer_ =
      std::min(first_visible_peer_, MaximumScroll(visible_rows));
}

void PeerListPane::ScrollDown(std::size_t visible_rows,
                              std::size_t line_count) {
  first_visible_peer_ =
      std::min(MaximumScroll(visible_rows), first_visible_peer_ + line_count);
}

void PeerListPane::ScrollHome() { first_visible_peer_ = 0; }

void PeerListPane::ScrollEnd(std::size_t visible_rows) {
  first_visible_peer_ = MaximumScroll(visible_rows);
}

bool PeerListPane::IsOpen() const { return open_; }

std::string_view PeerListPane::NodeId() const { return node_id_; }

std::uint64_t PeerListPane::PeerCount() const { return peer_count_; }

const std::vector<std::string>& PeerListPane::Peers() const { return peers_; }

std::size_t PeerListPane::FirstVisiblePeer(std::size_t visible_rows) const {
  return std::min(first_visible_peer_, MaximumScroll(visible_rows));
}

std::size_t PeerListPane::LastVisiblePeer(std::size_t visible_rows) const {
  return std::min(peers_.size(), FirstVisiblePeer(visible_rows) + visible_rows);
}

void PeerListPane::Load(const boost::json::object& report,
                        std::size_t selected_node) {
  const boost::json::object* node = NodeAt(report, selected_node);
  if (node == nullptr) {
    node_id_.clear();
    peer_count_ = 0;
    peers_.clear();
    first_visible_peer_ = 0;
    return;
  }

  const std::string next_node_id = JsonString(*node, "node_id");
  if (next_node_id != node_id_) {
    first_visible_peer_ = 0;
  }
  node_id_ = next_node_id;
  const boost::json::object* metrics = LastMetrics(*node);
  if (metrics == nullptr) {
    peer_count_ = 0;
    peers_.clear();
    first_visible_peer_ = 0;
    return;
  }
  peers_ = PeerAddresses(*metrics);
  peer_count_ = JsonUnsigned(*metrics, "peer_count", peers_.size());
  first_visible_peer_ = std::min(first_visible_peer_, peers_.size());
}

std::size_t PeerListPane::MaximumScroll(std::size_t visible_rows) const {
  return peers_.size() > visible_rows ? peers_.size() - visible_rows : 0U;
}

}  // namespace bbp
