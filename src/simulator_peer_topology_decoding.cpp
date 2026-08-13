#include "simulator_peer_topology_decoding.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/scenario_fields.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

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

void ValidateRoleNodeList(const std::vector<uint32_t>& role_nodes,
                          uint32_t node_count, std::string_view source) {
  for (uint32_t node_index : role_nodes) {
    if (node_index >= node_count) {
      throw std::runtime_error(std::string(source) + " must be in 1.." +
                               std::to_string(node_count));
    }
  }
}

PeerConnectivityPolicy ParsePeerConnectivityPolicyObject(
    const boost::json::object& object, uint32_t node_count) {
  RejectUnsupportedFields(
      object, ScenarioObjectFields(ScenarioObjectKind::kPeerConnectivity),
      "scenario topology.peer_connectivity entry");
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
  RejectUnsupportedFields(
      object, ScenarioObjectFields(ScenarioObjectKind::kTopologyEdge),
      "scenario topology.edges entry");
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
  bool condition_present = edge.latency_ms.has_value();
  for (const std::string_view field :
       ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition)) {
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

bool HasTopologyEdgeConditionField(const boost::json::object& object) {
  return std::any_of(object.begin(), object.end(), [](const auto& member) {
    return IsTopologyEdgeConditionField(member.key());
  });
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
    RejectUnsupportedFields(
        edge_object,
        ScenarioObjectFields(ScenarioObjectKind::kTopologyRegionEdge),
        "scenario topology.region_edges entry");
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
    const boost::json::value* latency = edge_object.if_contains("latency_ms");
    if (latency != nullptr) {
      edge.latency_ms = JsonUint32Value(*latency, "latency_ms");
    }
    bool condition_present = edge.latency_ms.has_value();
    for (const std::string_view field :
         ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition)) {
      condition_present =
          condition_present || edge_object.if_contains(field) != nullptr;
    }
    if (condition_present) {
      NetworkCondition condition = ParseNetworkConditionObject(edge_object);
      if (edge.latency_ms) {
        const boost::json::value* delay = edge_object.if_contains("delay_ms");
        if (delay != nullptr && condition.delay_ms != *edge.latency_ms) {
          throw std::runtime_error(
              "scenario topology region edge latency_ms and delay_ms must "
              "match");
        }
        condition.delay_ms = *edge.latency_ms;
      }
      ValidateNetworkCondition(condition);
      edge.condition = condition;
    }
    edges.push_back(edge);
  }
  return edges;
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

}  // namespace

std::vector<uint32_t> ConsecutiveNodeIndexes(uint32_t first_index,
                                             uint32_t count) {
  std::vector<uint32_t> nodes;
  nodes.reserve(count);
  for (uint32_t offset = 0; offset < count; ++offset) {
    nodes.push_back(first_index + offset);
  }
  return nodes;
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

bool IsTopologyEdgeConditionField(std::string_view field) {
  return field == "latency_ms" ||
         ScenarioObjectFieldAllowed(ScenarioObjectKind::kNetworkCondition,
                                    field);
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

PeerTopologyConfig ParsePeerTopologyConfig(const boost::json::object& object,
                                           uint32_t node_count,
                                           std::uint64_t default_seed) {
  PeerTopologyConfig topology;
  topology.kind = ParsePeerTopologyKind(
      JsonOptionalStringField(object, "type", "full_mesh"));
  for (const auto& member : object) {
    if (!ScenarioTopologyFieldAllowed(topology.kind, member.key())) {
      throw std::runtime_error(
          "scenario topology " +
          std::string(PeerTopologyKindName(topology.kind)) +
          " has unsupported field: " + std::string(member.key()));
    }
  }
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
    case PeerTopologyKind::kCount:
      throw std::logic_error("unknown scenario topology kind");
  }
  ResolvePeerTopologyEdges(topology, node_count);
  return topology;
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

}  // namespace bbp::simulator_app_internal
