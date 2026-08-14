#pragma once

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

struct NetworkBlockRule;
struct NetworkPartitionRule;
struct NodeRuntime;
struct NodeVethConfig;
struct ProfileSwitchWorkload;
struct QdiscInfo;
struct RuntimePeerTopologyEdge;
enum class WorkloadKind;

namespace simulator_app_internal {

boost::json::object QdiscJson(const QdiscInfo& qdisc);
std::string NetworkConditionVerificationDetail(
    const NodeVethConfig& config, const QdiscInfo& qdisc,
    std::uint32_t workload_index = 0, std::uint32_t workload_count = 0,
    std::optional<std::uint64_t> operator_sequence = std::nullopt);
std::string TopologyEdgeUpdateDetail(WorkloadKind action,
                                     std::uint32_t workload_index,
                                     std::uint32_t workload_count,
                                     const RuntimePeerTopologyEdge& previous,
                                     const RuntimePeerTopologyEdge& current,
                                     bool kernel_verified, bool peer_verified,
                                     std::uint32_t timeout_sec);
std::string TopologyEdgeRollbackFailureDetail(
    WorkloadKind action, const RuntimePeerTopologyEdge& previous,
    const RuntimePeerTopologyEdge& attempted, std::string_view original_error,
    const std::vector<std::string>& rollback_errors);
std::string NetworkProfileUpdateDetail(
    const ProfileSwitchWorkload& workload, std::uint32_t node,
    std::string_view previous_profile, const NodeVethConfig& previous,
    const NodeVethConfig& current, const QdiscInfo& qdisc,
    std::uint32_t workload_index, std::uint32_t workload_count);
std::string NetworkBlockRuleDetail(
    const NodeRuntime& node, const NetworkBlockRule& rule, bool existed_before,
    bool present_after, std::uint32_t workload_index = 0,
    std::uint32_t workload_count = 0,
    std::optional<std::uint64_t> operator_sequence = std::nullopt);
boost::json::object PartitionRuleResultJson(const NodeRuntime& node,
                                            const NetworkBlockRule& rule,
                                            bool existed_before,
                                            bool present_after);
std::string NetworkPartitionDetail(
    const NetworkPartitionRule& partition,
    const boost::json::array& rule_results, std::uint32_t workload_index = 0,
    std::uint32_t workload_count = 0,
    std::optional<std::uint64_t> operator_sequence = std::nullopt);

}  // namespace simulator_app_internal
}  // namespace bbp
