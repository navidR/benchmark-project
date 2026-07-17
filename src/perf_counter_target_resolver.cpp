#include "bbp/perf_counter_target_resolver.h"

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <charconv>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string_view>

namespace bbp {
namespace {

const boost::json::array& RequireArray(const boost::json::object& report,
                                       std::string_view field) {
  const boost::json::value* value = report.if_contains(field);
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("live report has no valid " + std::string(field));
  }
  return value->as_array();
}

std::string RequireString(const boost::json::object& object,
                          std::string_view field, std::string_view context) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string() || value->as_string().empty()) {
    throw std::runtime_error(std::string(context) + " has no valid " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

std::uint64_t RequireUnsigned(const boost::json::object& object,
                              std::string_view field,
                              std::string_view context) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error(std::string(context) + " has no valid " +
                             std::string(field));
  }
  if (value->is_uint64() && value->as_uint64() > 0U) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() > 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  throw std::runtime_error(std::string(context) + " has no valid " +
                           std::string(field));
}

const boost::json::object& RequireObjectAt(const boost::json::array& values,
                                           std::size_t index,
                                           std::string_view context) {
  if (index >= values.size() || !values[index].is_object()) {
    throw std::runtime_error("no valid selected " + std::string(context));
  }
  return values[index].as_object();
}

const boost::json::object& FindObjectByString(const boost::json::array& values,
                                              std::string_view field,
                                              std::string_view expected,
                                              std::string_view context) {
  const boost::json::object* match = nullptr;
  for (const boost::json::value& value : values) {
    if (!value.is_object()) {
      continue;
    }
    const boost::json::value* candidate = value.as_object().if_contains(field);
    if (candidate != nullptr && candidate->is_string() &&
        candidate->as_string() == expected) {
      if (match != nullptr) {
        throw std::runtime_error("duplicate perf counter " +
                                 std::string(context) + ": " +
                                 std::string(expected));
      }
      match = &value.as_object();
    }
  }
  if (match != nullptr) {
    return *match;
  }
  throw std::runtime_error("unknown perf counter " + std::string(context) +
                           ": " + std::string(expected));
}

std::uint64_t ParseWalletIndex(std::string_view id) {
  if (id.starts_with('#')) {
    id.remove_prefix(1U);
  }
  if (id.empty() || (id.size() > 1U && id.front() == '0')) {
    throw std::runtime_error(
        "perf counter wallet id must be a canonical "
        "positive wallet index");
  }
  std::uint64_t index = 0U;
  const auto [end, error] =
      std::from_chars(id.data(), id.data() + id.size(), index);
  if (error != std::errc() || end != id.data() + id.size() || index == 0U) {
    throw std::runtime_error(
        "perf counter wallet id must be a canonical "
        "positive wallet index");
  }
  return index;
}

const boost::json::object& FindWallet(const boost::json::array& wallets,
                                      std::uint64_t wallet_index) {
  const boost::json::object* match = nullptr;
  for (const boost::json::value& value : wallets) {
    if (!value.is_object()) {
      continue;
    }
    const boost::json::value* index =
        value.as_object().if_contains("wallet_index");
    if (index != nullptr &&
        ((index->is_uint64() && index->as_uint64() == wallet_index) ||
         (index->is_int64() && index->as_int64() > 0 &&
          static_cast<std::uint64_t>(index->as_int64()) == wallet_index))) {
      if (match != nullptr) {
        throw std::runtime_error("duplicate perf counter wallet: " +
                                 std::to_string(wallet_index));
      }
      match = &value.as_object();
    }
  }
  if (match != nullptr) {
    return *match;
  }
  throw std::runtime_error("unknown perf counter wallet: " +
                           std::to_string(wallet_index));
}

const boost::json::object& BackingNode(const boost::json::object& report,
                                       const boost::json::object& wallet) {
  const std::uint64_t one_based = RequireUnsigned(wallet, "node", "wallet");
  const boost::json::array& nodes = RequireArray(report, "nodes_summary");
  const boost::json::object* match = nullptr;
  for (const boost::json::value& value : nodes) {
    if (!value.is_object()) {
      continue;
    }
    const boost::json::value* index =
        value.as_object().if_contains("node_index");
    const bool matches =
        index != nullptr &&
        ((index->is_uint64() && index->as_uint64() == one_based) ||
         (index->is_int64() && index->as_int64() > 0 &&
          static_cast<std::uint64_t>(index->as_int64()) == one_based));
    if (!matches) {
      continue;
    }
    if (match != nullptr) {
      throw std::runtime_error("duplicate wallet backing node index: " +
                               std::to_string(one_based));
    }
    match = &value.as_object();
  }
  if (match == nullptr) {
    throw std::runtime_error("unknown wallet backing node index: " +
                             std::to_string(one_based));
  }
  return *match;
}

PerfCounterTarget NodeTarget(const boost::json::object& node,
                             PerfCounterTargetKind kind) {
  const std::string node_id = RequireString(node, "node_id", "node summary");
  return {.kind = kind, .id = node_id, .node_ids = {node_id}};
}

PerfCounterTarget WalletTarget(const boost::json::object& report,
                               const boost::json::object& wallet) {
  const std::uint64_t wallet_index =
      RequireUnsigned(wallet, "wallet_index", "wallet summary");
  const boost::json::object& node = BackingNode(report, wallet);
  const std::string node_id = RequireString(node, "node_id", "node summary");
  return {.kind = PerfCounterTargetKind::kWallet,
          .id = "wallet-" + std::to_string(wallet_index),
          .node_ids = {node_id}};
}

PerfCounterTarget GroupTarget(const boost::json::object& report,
                              const boost::json::object& group) {
  const std::string group_id = RequireString(group, "group", "topology group");
  const boost::json::value* members = group.if_contains("node_ids");
  if (members == nullptr || !members->is_array() ||
      members->as_array().empty()) {
    throw std::runtime_error("topology group has no valid node_ids: " +
                             group_id);
  }
  std::vector<std::string> node_ids;
  std::set<std::string> unique;
  node_ids.reserve(members->as_array().size());
  for (const boost::json::value& member : members->as_array()) {
    if (!member.is_string() || member.as_string().empty()) {
      throw std::runtime_error("topology group contains an invalid node id: " +
                               group_id);
    }
    std::string node_id(member.as_string());
    if (!unique.insert(node_id).second) {
      throw std::runtime_error("topology group contains a duplicate node id: " +
                               group_id);
    }
    static_cast<void>(FindObjectByString(RequireArray(report, "nodes_summary"),
                                         "node_id", node_id, "node"));
    node_ids.push_back(std::move(node_id));
  }
  return {.kind = PerfCounterTargetKind::kGroup,
          .id = group_id,
          .node_ids = std::move(node_ids)};
}

PerfCounterTarget ResolveExplicit(const boost::json::object& report,
                                  PerfCounterTargetKind kind,
                                  std::string_view id) {
  if (id.empty()) {
    throw std::runtime_error("perf counter target id must not be empty");
  }
  switch (kind) {
    case PerfCounterTargetKind::kNode:
    case PerfCounterTargetKind::kCgroup:
      return NodeTarget(
          FindObjectByString(RequireArray(report, "nodes_summary"), "node_id",
                             id, "node"),
          kind);
    case PerfCounterTargetKind::kWallet:
      return WalletTarget(report,
                          FindWallet(RequireArray(report, "wallets_summary"),
                                     ParseWalletIndex(id)));
    case PerfCounterTargetKind::kGroup:
      return GroupTarget(
          report,
          FindObjectByString(RequireArray(report, "topology_groups_summary"),
                             "group", id, "group"));
  }
  throw std::runtime_error("unknown perf counter target kind");
}

}  // namespace

PerfCounterTarget ResolvePerfCounterTarget(
    const boost::json::object& report,
    const PerfCounterSelectionContext& selection,
    std::optional<PerfCounterTargetKind> requested_kind,
    const std::optional<std::string>& requested_id) {
  if (requested_id) {
    if (!requested_kind) {
      throw std::runtime_error("perf counter target id requires a target kind");
    }
    return ResolveExplicit(report, *requested_kind, *requested_id);
  }

  switch (selection.view) {
    case TuiView::kNodes: {
      const PerfCounterTargetKind kind =
          requested_kind.value_or(PerfCounterTargetKind::kNode);
      if (kind != PerfCounterTargetKind::kNode &&
          kind != PerfCounterTargetKind::kCgroup) {
        throw std::runtime_error(
            "selected node supports only node or cgroup perf targets");
      }
      return NodeTarget(RequireObjectAt(RequireArray(report, "nodes_summary"),
                                        selection.selected_node, "node"),
                        kind);
    }
    case TuiView::kWallets: {
      const PerfCounterTargetKind kind =
          requested_kind.value_or(PerfCounterTargetKind::kWallet);
      const boost::json::object& wallet =
          RequireObjectAt(RequireArray(report, "wallets_summary"),
                          selection.selected_wallet, "wallet");
      if (kind == PerfCounterTargetKind::kWallet) {
        return WalletTarget(report, wallet);
      }
      if (kind == PerfCounterTargetKind::kNode ||
          kind == PerfCounterTargetKind::kCgroup) {
        return NodeTarget(BackingNode(report, wallet), kind);
      }
      throw std::runtime_error(
          "selected wallet supports only wallet, node, or cgroup perf targets");
    }
    case TuiView::kTopology: {
      const PerfCounterTargetKind kind =
          requested_kind.value_or(PerfCounterTargetKind::kGroup);
      if (kind != PerfCounterTargetKind::kGroup) {
        throw std::runtime_error(
            "selected topology supports only group perf targets");
      }
      return GroupTarget(
          report,
          RequireObjectAt(RequireArray(report, "topology_groups_summary"),
                          selection.selected_topology_group, "topology group"));
    }
  }
  throw std::runtime_error("unknown TUI view for perf counter target");
}

}  // namespace bbp
