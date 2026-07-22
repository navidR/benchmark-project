#pragma once

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <cstddef>
#include <span>
#include <string_view>

namespace bbp {

inline constexpr std::string_view kMcpProtocolVersion = "2025-11-25";
inline constexpr std::size_t kMcpListPageSize = 64U;
inline constexpr std::size_t kMcpMaximumSessions = 16U;
inline constexpr std::size_t kMcpMaximumTasksPerSession = 64U;
inline constexpr std::size_t kMcpMaximumSubscriptionsPerSession = 64U;
inline constexpr std::size_t kMcpMaximumNotificationsPerSession = 256U;
inline constexpr std::size_t kMcpMaximumRetainedOperations = 256U;

enum class McpOperationKind {
  kValidateScenario,
  kResolveScenario,
  kLaunchRun,
  kStopRun,
  kCleanRun,
  kReplayRun,
  kReportRun,
  kInvokeRuntimeCommand,
  kCreateFiroQtLauncher,
  kAddNode,
  kRemoveNode,
  kStopNode,
  kKillNode,
  kRestartNode,
  kReplaceNode,
  kAddWallet,
  kRemoveWallet,
  kAssignRole,
  kRemoveRole,
  kAddMiner,
  kRemoveMiner,
  kAddMasternode,
  kRemoveMasternode,
  kRestartMasternode,
  kStartWorkload,
  kReconfigureWorkload,
  kPauseWorkload,
  kResumeWorkload,
  kStopWorkload,
  kStartInstrumentation,
  kReconfigureInstrumentation,
  kStopInstrumentation,
  kQueryEvidence,
  kQueryLogs,
  kFollowLogs,
  kReadArtifact,
  kGetOperation,
  kCancelOperation,
  kCreateSubscription,
  kPollSubscription,
  kCancelSubscription,
  kCount,
};

enum class McpInformationFamily {
  kCapabilities,
  kSchemas,
  kSourceScenario,
  kResolvedScenario,
  kRunRegistry,
  kLifecycle,
  kNodes,
  kRoles,
  kPeers,
  kTopology,
  kWallets,
  kBalances,
  kMining,
  kTransactionLoad,
  kWorkloads,
  kWorkloadHistory,
  kProcesses,
  kNamespaces,
  kInterfaces,
  kQdiscs,
  kCgroups,
  kResourceState,
  kCleanupState,
  kMetrics,
  kWalletMetrics,
  kInstrumentation,
  kMeasurements,
  kMeasurementHistory,
  kComparisons,
  kEvents,
  kLogs,
  kLogHistory,
  kRpcFailures,
  kErrors,
  kCommandHistory,
  kReports,
  kGeneratedCommands,
  kArtifacts,
  kArtifactContents,
  kProgress,
  kOperations,
  kNotifications,
  kCount,
};

enum class McpResultFamily {
  kValidation,
  kScenario,
  kRunLifecycle,
  kRuntimeCommand,
  kMutation,
  kRoleMutation,
  kWorkload,
  kInstrumentation,
  kEvidencePage,
  kArtifactContent,
  kOperation,
  kSubscription,
  kCleanup,
  kError,
  kCount,
};

struct McpNamedCapability {
  std::string_view name;
  std::string_view description;
};

std::string_view McpOperationKindName(McpOperationKind kind);
std::string_view McpInformationFamilyName(McpInformationFamily family);
std::string_view McpResultFamilyName(McpResultFamily family);

std::span<const McpNamedCapability> McpOperationRegistry();
std::span<const McpNamedCapability> McpInformationFamilyRegistry();
std::span<const McpNamedCapability> McpResultFamilyRegistry();
std::span<const std::string_view> McpScenarioMemberRegistry();

boost::json::object BuildMcpCapabilityDocument();
boost::json::object BuildMcpScenarioSchema();
boost::json::array BuildMcpToolRegistry();
boost::json::array BuildMcpResourceRegistry();

}  // namespace bbp
