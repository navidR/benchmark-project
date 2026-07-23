#pragma once

#include <array>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <cstddef>
#include <span>
#include <string_view>

#include "bbp/scenario_fields.h"

namespace bbp {

inline constexpr std::string_view kMcpProtocolVersion = "2025-11-25";
inline constexpr std::array<std::string_view, 2U>
    kMcpSupportedProtocolVersions = {kMcpProtocolVersion, "2025-06-18"};
inline constexpr std::size_t kMcpListPageSize = 64U;
inline constexpr std::size_t kMcpMaximumSessions = 16U;
inline constexpr std::size_t kMcpMaximumTasksPerSession = 64U;
inline constexpr std::size_t kMcpMaximumSubscriptionsPerSession = 64U;
inline constexpr std::size_t kMcpMaximumNotificationsPerSession = 256U;
inline constexpr std::size_t kMcpMaximumRetainedOperations = 256U;
inline constexpr std::size_t kMcpMaximumRetainedResultBytes =
    4U * 1024U * 1024U;
inline constexpr std::size_t kMcpMaximumEvidenceTextBytes = 1024U * 1024U;
inline constexpr std::size_t kMcpMaximumSelectionItems = 10000U;

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
McpResultFamily McpOperationResultFamily(McpOperationKind operation);

std::span<const McpNamedCapability> McpOperationRegistry();
std::span<const McpNamedCapability> McpInformationFamilyRegistry();
std::span<const McpNamedCapability> McpResultFamilyRegistry();
std::span<const std::string_view> McpScenarioMemberRegistry();

boost::json::object BuildMcpCapabilityDocument();
boost::json::object BuildMcpCapabilityDocument(
    std::span<const McpOperationKind> operations);
boost::json::object BuildMcpCapabilityDocument(
    std::span<const McpOperationKind> operations,
    std::span<const McpInformationFamily> information_families);
boost::json::object BuildMcpScenarioObjectSchema(ScenarioObjectKind kind);
boost::json::object BuildMcpScenarioSchema();
boost::json::object BuildMcpWorkloadSchema();
boost::json::object BuildMcpSimulationCommandSchema();
boost::json::object BuildMcpOperationInputSchema(McpOperationKind operation);
boost::json::object BuildMcpOperationInputSchema(
    McpOperationKind operation,
    std::span<const McpInformationFamily> information_families);
boost::json::object BuildMcpResultSchema(McpResultFamily family);
boost::json::object BuildMcpResultSchema(
    McpResultFamily family,
    std::span<const McpOperationKind> operations);
boost::json::object BuildMcpOperationOutputSchema(McpOperationKind operation);
boost::json::object BuildMcpOperationOutputSchema(
    McpOperationKind operation,
    std::span<const McpOperationKind> operations);
boost::json::array BuildMcpToolRegistry();
boost::json::array BuildMcpToolRegistry(
    std::span<const McpOperationKind> operations);
boost::json::array BuildMcpToolRegistry(
    std::span<const McpOperationKind> operations,
    std::span<const McpInformationFamily> information_families);
boost::json::array BuildMcpResourceRegistry();
boost::json::array BuildMcpResourceRegistry(
    std::span<const McpInformationFamily> information_families);

}  // namespace bbp
