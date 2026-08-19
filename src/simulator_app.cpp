#include "bbp/simulator_app.h"

#include <fcntl.h>
#include <linux/capability.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "bbp/application_instance_lock.h"
#include "bbp/capability.h"
#include "bbp/cgroup.h"
#include "bbp/default_peer_topology.h"
#include "bbp/drivers/chain_command_executor.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/json_secret_redaction.h"
#include "bbp/log_tail.h"
#include "bbp/logging.h"
#include "bbp/mcp_endpoint.h"
#include "bbp/mcp_host_application.h"
#include "bbp/mcp_live_application.h"
#include "bbp/network.h"
#include "bbp/network_allocation_lock.h"
#include "bbp/node_log_collector.h"
#include "bbp/operator_connection.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/perf_counter.h"
#include "bbp/periodic_metrics_collector.h"
#include "bbp/positive_duration.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/process.h"
#include "bbp/run_ownership.h"
#include "bbp/run_process_state.h"
#include "bbp/run_report.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/scenario_fields.h"
#include "bbp/scenario_service.h"
#include "bbp/signal_stop_monitor.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_processor.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/block_generation_workload.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/legacy_cli_inputs.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/process_control_config.h"
#include "bbp/simulator/scenario_workload_admission.h"
#include "bbp/simulator/transaction_load.h"
#include "bbp/simulator/transaction_observation_store.h"
#include "bbp/simulator/wallet_transaction_plan.h"
#include "bbp/tui.h"
#include "bbp/util.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_combined_stop_token.h"
#include "simulator_event_writing.h"
#include "simulator_height_wait_readback.h"
#include "simulator_host_probes.h"
#include "simulator_initial_peer_connectivity.h"
#include "simulator_json_field_decoding.h"
#include "simulator_live_block_generation_control.h"
#include "simulator_live_height_wait_control.h"
#include "simulator_live_instrumentation_controller.h"
#include "simulator_live_wait_for_peers_control.h"
#include "simulator_live_wallet_workload_control.h"
#include "simulator_live_wallet_workload_launcher.h"
#include "simulator_live_workload_reading.h"
#include "simulator_live_workload_shutdown_request.h"
#include "simulator_live_workload_state.h"
#include "simulator_managed_run_root.h"
#include "simulator_masternode_funding_boundary.h"
#include "simulator_metrics_sampling.h"
#include "simulator_native_mining_rpc.h"
#include "simulator_network_block_application.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_network_launch_planning.h"
#include "simulator_network_partition_application.h"
#include "simulator_network_partition_planning.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_node_lifecycle_event_details.h"
#include "simulator_node_log_tail.h"
#include "simulator_node_process_state.h"
#include "simulator_node_report_export.h"
#include "simulator_offline_run_cleanup.h"
#include "simulator_operator_connection_publication.h"
#include "simulator_option_parsing.h"
#include "simulator_peer_churn_workloads.h"
#include "simulator_peer_topology_decoding.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_perf_counter_transactions.h"
#include "simulator_process_spawn_readiness.h"
#include "simulator_profile_assignment.h"
#include "simulator_profile_switching.h"
#include "simulator_raw_transaction_workload.h"
#include "simulator_resolved_scenario_persistence.h"
#include "simulator_resource_event_details.h"
#include "simulator_resource_limit_application.h"
#include "simulator_resource_limit_decoding.h"
#include "simulator_resource_limit_orchestration.h"
#include "simulator_resource_pressure_workload.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_retained_run_registry.h"
#include "simulator_retained_tui_application.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_runtime_network_block_rules.h"
#include "simulator_runtime_network_condition_updates.h"
#include "simulator_runtime_network_partition_rules.h"
#include "simulator_runtime_node_addition.h"
#include "simulator_runtime_node_cleanup.h"
#include "simulator_runtime_node_freeze.h"
#include "simulator_runtime_node_preparation.h"
#include "simulator_runtime_node_removal.h"
#include "simulator_runtime_node_replacement.h"
#include "simulator_runtime_node_restart.h"
#include "simulator_runtime_node_startup.h"
#include "simulator_runtime_node_stop.h"
#include "simulator_runtime_published_node_config.h"
#include "simulator_runtime_topology_publication_planning.h"
#include "simulator_runtime_workload_validation.h"
#include "simulator_scenario_chain_decoding.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_mutation_option_decoding.h"
#include "simulator_scenario_node_decoding.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_scenario_option_application.h"
#include "simulator_scenario_serialization.h"
#include "simulator_scenario_workload_decoding.h"
#include "simulator_scheduled_command_decoding.h"
#include "simulator_scheduled_command_event_details.h"
#include "simulator_scheduled_event_decoding.h"
#include "simulator_source_scenario_persistence.h"
#include "simulator_stop_coordination.h"
#include "simulator_tcp_endpoint_reservation.h"
#include "simulator_topology_edge_workload.h"
#include "simulator_transaction_load_completion_publication.h"
#include "simulator_transaction_observation_tracking.h"
#include "simulator_wallet_configuration_decoding.h"
#include "simulator_wallet_node_initialization.h"
#include "simulator_wallet_transaction_distribution_decoding.h"
#include "simulator_wallet_transaction_validation.h"
#include "simulator_wallet_transaction_workload_execution.h"
#include "simulator_workload_event_details.h"
#include "simulator_workload_mutation_error.h"
#include "simulator_workload_service_shutdown_diagnostic.h"
#include "simulator_yaml_decoding.h"

namespace bbp {
namespace {

using simulator_app_internal::AcquireScenarioHeightWaitAdmission;
using simulator_app_internal::AddRuntimeNodesTransactional;
using simulator_app_internal::ApplyConnectPeerWorkload;
using simulator_app_internal::ApplyDeclarativeStopDuringStart;
using simulator_app_internal::ApplyDisconnectPeerWorkload;
using simulator_app_internal::ApplyNetworkBlockRules;
using simulator_app_internal::ApplyNetworkPartitionRules;
using simulator_app_internal::ApplyNetworkProfileSwitch;
using simulator_app_internal::ApplyNodeConditions;
using simulator_app_internal::ApplyPerfCounterCommand;
using simulator_app_internal::ApplyResourceLimitPatches;
using simulator_app_internal::ApplyResourceLimitUpdate;
using simulator_app_internal::ApplyResourcePressureWorkload;
using simulator_app_internal::ApplyResourceProfileSwitch;
using simulator_app_internal::ApplyRuntimeNetworkBlockRules;
using simulator_app_internal::ApplyRuntimeNetworkConditionUpdates;
using simulator_app_internal::ApplyRuntimeNetworkPartition;
using simulator_app_internal::ApplyRuntimeNetworkPartitionHeals;
using simulator_app_internal::ApplyRuntimeNetworkPartitions;
using simulator_app_internal::ApplyRuntimeNetworkUnblockRules;
using simulator_app_internal::ApplyRuntimeNodeFreezes;
using simulator_app_internal::ApplyRuntimeResourceLimitUpdates;
using simulator_app_internal::ApplyScenarioJson;
using simulator_app_internal::ApplyScheduledScenarioEvents;
using simulator_app_internal::ApplySendRawTransactionWorkload;
using simulator_app_internal::ApplyTopologyEdgeWorkload;
using simulator_app_internal::AttachNodePerfCounters;
using simulator_app_internal::BenchmarkRunRoot;
using simulator_app_internal::BlockGenerationOutcomeUnconfirmed;
using simulator_app_internal::CheckpointWorkloadDetail;
using simulator_app_internal::CombinedStopToken;
using simulator_app_internal::ConfirmMasternodeTransactions;
using simulator_app_internal::ConsecutiveNodeIndexes;
using simulator_app_internal::DirectionalNetworkPoliciesForNode;
using simulator_app_internal::DiscoverRetainedRuns;
using simulator_app_internal::DynamicDirectionalNetworkPolicies;
using simulator_app_internal::DynamicPhysicalTopologyPeerEndpoints;
using simulator_app_internal::DynamicRestartPeerEndpoints;
using simulator_app_internal::DynamicTopologyPeerIds;
using simulator_app_internal::EffectiveNodeBinary;
using simulator_app_internal::EffectiveNodeChainNetwork;
using simulator_app_internal::EffectiveNodeExtraArgs;
using simulator_app_internal::EffectiveNodeLifecyclePolicy;
using simulator_app_internal::EffectiveNodeWalletConfig;
using simulator_app_internal::ElapsedMilliseconds;
using simulator_app_internal::ExpectedTransactionLoadObservations;
using simulator_app_internal::ExportNodeReport;
using simulator_app_internal::FindPeerConnectivityPolicy;
using simulator_app_internal::FreezeNodeForDuration;
using simulator_app_internal::FreezeNodeWorkloadDetail;
using simulator_app_internal::GenerateBlocksSerialized;
using simulator_app_internal::GenerateBlockWorkloadBoundary;
using simulator_app_internal::GeneratedBlocksDetail;
using simulator_app_internal::GeneratedBlockWorkloadBoundary;
using simulator_app_internal::HasTimedNodeLifecycle;
using simulator_app_internal::HeightWaitDetail;
using simulator_app_internal::HostIpv4ForwardingEnabled;
using simulator_app_internal::InitialAllowedPeers;
using simulator_app_internal::InitialAllPeerPolicyNodeIds;
using simulator_app_internal::InitializeWalletNodes;
using simulator_app_internal::InitialPeerCountPolicies;
using simulator_app_internal::InitialResourceLimits;
using simulator_app_internal::IsCurrentRunningNodeProcess;
using simulator_app_internal::IsTerminalLiveWalletWorkloadState;
using simulator_app_internal::IsTerminalLiveWorkloadState;
using simulator_app_internal::IsTopologyEdgeConditionField;
using simulator_app_internal::JoinLiveWalletWorkloadWorker;
using simulator_app_internal::JsonAmountField;
using simulator_app_internal::JsonOptionalAmountField;
using simulator_app_internal::JsonOptionalBoolField;
using simulator_app_internal::JsonOptionalDoubleField;
using simulator_app_internal::JsonOptionalNullableDoubleField;
using simulator_app_internal::JsonOptionalNullableUint32Field;
using simulator_app_internal::JsonOptionalPathField;
using simulator_app_internal::JsonOptionalStringField;
using simulator_app_internal::JsonOptionalUint32Field;
using simulator_app_internal::JsonOptionalUint64Field;
using simulator_app_internal::JsonOptionalUint64FieldValue;
using simulator_app_internal::JsonPercentBasisPoints;
using simulator_app_internal::JsonStringField;
using simulator_app_internal::JsonUint32Field;
using simulator_app_internal::JsonUint32Value;
using simulator_app_internal::JsonUint64Field;
using simulator_app_internal::JsonUint64Value;
using simulator_app_internal::kRunStopNotObserved;
using simulator_app_internal::kWorkloadServiceShutdownBound;
using simulator_app_internal::LiveBlockGenerationBoundaryResult;
using simulator_app_internal::LiveBlockGenerationWorkloadJson;
using simulator_app_internal::LiveBlockGenerationWorkloadRecord;
using simulator_app_internal::LiveBlockGenerationWorkloadRegistry;
using simulator_app_internal::LiveInstrumentationControllerPtr;
using simulator_app_internal::LiveInstrumentationMeasurementCollector;
using simulator_app_internal::LiveWaitForPeersResult;
using simulator_app_internal::LiveWaitForPeersWorkloadJson;
using simulator_app_internal::LiveWaitForPeersWorkloadRecord;
using simulator_app_internal::LiveWaitForPeersWorkloadRegistry;
using simulator_app_internal::LiveWaitUntilHeightResult;
using simulator_app_internal::LiveWaitUntilHeightWorkloadJson;
using simulator_app_internal::LiveWaitUntilHeightWorkloadRecord;
using simulator_app_internal::LiveWaitUntilHeightWorkloadRegistry;
using simulator_app_internal::LiveWalletWorkloadRecord;
using simulator_app_internal::LiveWalletWorkloadRegistry;
using simulator_app_internal::LiveWalletWorkloadRequest;
using simulator_app_internal::LiveWalletWorkloadState;
using simulator_app_internal::LiveWalletWorkloadStateName;
using simulator_app_internal::LiveWorkloadRequest;
using simulator_app_internal::LiveWorkloadState;
using simulator_app_internal::LoadRetainedSourceScenario;
using simulator_app_internal::LockNodeProcessState;
using simulator_app_internal::MakeLiveBlockGenerationOperation;
using simulator_app_internal::MakeLiveInstrumentationController;
using simulator_app_internal::MakeLiveInstrumentationRegistry;
using simulator_app_internal::MakeLiveInstrumentationService;
using simulator_app_internal::MakeLiveWaitForPeersOperation;
using simulator_app_internal::MakeLiveWaitUntilHeightOperation;
using simulator_app_internal::MakeLiveWalletWorkloadLauncher;
using simulator_app_internal::MakeLiveWalletWorkloadOperation;
using simulator_app_internal::MakeNodeVethConfig;
using simulator_app_internal::MasternodeTransactionConfirmation;
using simulator_app_internal::MutateNetworkBlockRuleTransactional;
using simulator_app_internal::NetworkAddressPlan;
using simulator_app_internal::NetworkBlockMutationResult;
using simulator_app_internal::NetworkBlockRuleForHandle;
using simulator_app_internal::NodeDataDirectoryRelative;
using simulator_app_internal::NodeExitedBeforeRpcReady;
using simulator_app_internal::NodeLifecycleDeadlineDetail;
using simulator_app_internal::NodeListContains;
using simulator_app_internal::NodeListsOverlap;
using simulator_app_internal::NodePerfCounterTransactionBackend;
using simulator_app_internal::NodeProcessGeneration;
using simulator_app_internal::NodeProcessRunning;
using simulator_app_internal::NodeRestartAdmission;
using simulator_app_internal::NodeRoleName;
using simulator_app_internal::ObservedRunStop;
using simulator_app_internal::OneShotRawTransactionRejected;
using simulator_app_internal::OperatorWalletTransactionDetail;
using simulator_app_internal::ParseAmountDistribution;
using simulator_app_internal::ParseAndValidateLiveBlockGenerationWorkload;
using simulator_app_internal::ParseAndValidateLiveWaitForPeersWorkload;
using simulator_app_internal::ParseAndValidateLiveWaitUntilHeightWorkload;
using simulator_app_internal::ParseAndValidateOneShotWorkload;
using simulator_app_internal::ParseIntervalDistribution;
using simulator_app_internal::ParseIoLimits;
using simulator_app_internal::ParseNetworkBlockRuleObject;
using simulator_app_internal::ParseNetworkConditionObject;
using simulator_app_internal::ParseNetworkPartitionRuleObject;
using simulator_app_internal::ParseNetworkProfiles;
using simulator_app_internal::ParseNodeRoleTopologyObject;
using simulator_app_internal::ParseOptions;
using simulator_app_internal::ParsePeerTopologyConfig;
using simulator_app_internal::ParseProfileSwitchWorkload;
using simulator_app_internal::ParseResourceLimitPatchObject;
using simulator_app_internal::ParseResourceProfiles;
using simulator_app_internal::ParseScenarioChains;
using simulator_app_internal::ParseScenarioNodes;
using simulator_app_internal::ParseScheduledSimulationCommand;
using simulator_app_internal::ParseTopologyEdgeWorkloadCondition;
using simulator_app_internal::ParseWalletFundingStrategy;
using simulator_app_internal::ParseWalletIndexList;
using simulator_app_internal::ParseWalletInitializationObject;
using simulator_app_internal::ParseWalletTransactionFeePolicy;
using simulator_app_internal::ParseWalletTransactionMode;
using simulator_app_internal::ParseWalletTransferStrategy;
using simulator_app_internal::PeerCountWaitDetail;
using simulator_app_internal::PendingTransactionLoadCompletion;
using simulator_app_internal::PrepareManagedRunRoot;
using simulator_app_internal::PrepareMasternodeFunding;
using simulator_app_internal::PrepareNodeRuntime;
using simulator_app_internal::ProcessExitDetail;
using simulator_app_internal::PublishOperatorConnectionCommand;
using simulator_app_internal::RawTransactionDetail;
using simulator_app_internal::ReadLiveWorkloads;
using simulator_app_internal::RecordAndPublishGeneratedBlockWorkloadBoundary;
using simulator_app_internal::RecordGeneratedBlocks;
using simulator_app_internal::RecordRunStop;
using simulator_app_internal::RegisteredMasternodeIdentity;
using simulator_app_internal::RejectTopologyEdgeConditionFields;
using simulator_app_internal::RejectUnsupportedFields;
using simulator_app_internal::RejectUnsupportedScenarioActionFields;
using simulator_app_internal::RemovePreparedRunRoot;
using simulator_app_internal::RemoveRuntimeNodesTransactional;
using simulator_app_internal::ReplaceNodeNetworkConditionTransactional;
using simulator_app_internal::ReplaceRuntimeNodeTransactional;
using simulator_app_internal::RequestLiveWorkloadShutdown;
using simulator_app_internal::RequestNodeKill;
using simulator_app_internal::RequestNodeTerminate;
using simulator_app_internal::RequireCgroupWeight;
using simulator_app_internal::RequireNoActiveBlockGenerationWorkloads;
using simulator_app_internal::RequireNoActiveWaitForPeersWorkloads;
using simulator_app_internal::RequireNoActiveWaitUntilHeightWorkloads;
using simulator_app_internal::RequireNodeRunning;
using simulator_app_internal::RequireNoLiveInstrumentationCommandConflict;
using simulator_app_internal::RequireNonZero;
using simulator_app_internal::RequireRunNetworkInterfacesAvailable;
using simulator_app_internal::RequireRunOwnership;
using simulator_app_internal::RequireRuntimeNodeNumber;
using simulator_app_internal::RequireSafeScenarioIdentifier;
using simulator_app_internal::RequireSingleWalletTransactionId;
using simulator_app_internal::ReservedManagedRunRoot;
using simulator_app_internal::ReserveManagedReplayRunRoot;
using simulator_app_internal::ReserveTcpEndpoint;
using simulator_app_internal::ResetNodePerfCounters;
using simulator_app_internal::ResolveNodeProfileAssignments;
using simulator_app_internal::ResourceLimitUpdateDetail;
using simulator_app_internal::RestartNodeWorkloadDetail;
using simulator_app_internal::RestartPolicyAppliedDetail;
using simulator_app_internal::RestartRequestedDetail;
using simulator_app_internal::RunningNodeProcessGeneration;
using simulator_app_internal::RunRetainedTuiWithMcp;
using simulator_app_internal::RunStopTick;
using simulator_app_internal::RuntimeMasternodeAddContext;
using simulator_app_internal::RuntimeMasternodeIdentityJson;
using simulator_app_internal::RuntimeNodeAdditionDependencies;
using simulator_app_internal::RuntimeNodeAdditionRole;
using simulator_app_internal::RuntimeNodeAddResult;
using simulator_app_internal::RuntimeNodePointers;
using simulator_app_internal::RuntimeNodeRemovalDependencies;
using simulator_app_internal::RuntimeNodeRemoveResult;
using simulator_app_internal::RuntimeNodeReplacementDependencies;
using simulator_app_internal::RuntimeNodeReplaceResult;
using simulator_app_internal::RuntimeNodeResourceEntryFor;
using simulator_app_internal::RuntimeNodeResourceManifestFor;
using simulator_app_internal::RuntimeNodeSupportDestructionAllowed;
using simulator_app_internal::RuntimeOneShotWorkloadValidationOptions;
using simulator_app_internal::RuntimePartitionRule;
using simulator_app_internal::RuntimePublishedNodeConfig;
using simulator_app_internal::RuntimeRoleGenerationDetail;
using simulator_app_internal::RuntimeWalletGenerationDetail;
using simulator_app_internal::RuntimeWalletIdentityJson;
using simulator_app_internal::ScenarioHeightWaitAdmissionLease;
using simulator_app_internal::ScenarioNodeConfigAt;
using simulator_app_internal::ScenarioNodeId;
using simulator_app_internal::ScenarioNodeRoles;
using simulator_app_internal::ScheduledBlockDetail;
using simulator_app_internal::ScheduledEventLifecycleDetail;
using simulator_app_internal::SetNodeFrozen;
using simulator_app_internal::SimulationCommandDetail;
using simulator_app_internal::StableRuleHandle;
using simulator_app_internal::StartNativeMiningForCurrentProcess;
using simulator_app_internal::StartNodeProcessAttempt;
using simulator_app_internal::StartupPeerAddresses;
using simulator_app_internal::SteadyDeadline;
using simulator_app_internal::StopNativeMining;
using simulator_app_internal::StopNativeMiningBeforeDeadline;
using simulator_app_internal::StopNodeProcess;
using simulator_app_internal::StopRuntimeNodes;
using simulator_app_internal::SynchronizeBlockWorkloadBoundary;
using simulator_app_internal::ThrowIfStopRequested;
using simulator_app_internal::ThrowWorkloadMutationOutcomeUnconfirmed;
using simulator_app_internal::ToChainWalletMode;
using simulator_app_internal::TransactionLoadAttemptDetail;
using simulator_app_internal::TransactionLoadProgressDetail;
using simulator_app_internal::TransactionObservationDetail;
using simulator_app_internal::TransactionObservationTracker;
using simulator_app_internal::TransactionSetObservation;
using simulator_app_internal::TransitionNodeState;
using simulator_app_internal::ValidateLiveInstrumentationDuration;
using simulator_app_internal::ValidateNetworkPartitionRule;
using simulator_app_internal::ValidateProfileSwitchReferences;
using simulator_app_internal::ValidateWalletTransactionsWorkload;
using simulator_app_internal::WaitForDuration;
using simulator_app_internal::WaitForHeightReadback;
using simulator_app_internal::WaitForNodeFrozenState;
using simulator_app_internal::WaitForNodeProcessExitUntil;
using simulator_app_internal::WaitUntil;
using simulator_app_internal::WalletAddressDetail;
using simulator_app_internal::WalletFundingDetail;
using simulator_app_internal::WalletTransactionDetail;
using simulator_app_internal::WorkloadMutationCancelledAfterRollback;
using simulator_app_internal::WorkloadMutationFailedAfterRollback;
using simulator_app_internal::WorkloadMutationOutcomeUnconfirmed;
using simulator_app_internal::WorkloadServiceShutdownTimeout;
using simulator_app_internal::WorkloadShutdownRecords;
using simulator_app_internal::WriteEvent;
using simulator_app_internal::WriteLiveBlockGenerationWorkloadState;
using simulator_app_internal::WriteLiveWaitForPeersWorkloadState;
using simulator_app_internal::WriteLiveWaitUntilHeightWorkloadState;
using simulator_app_internal::WriteLogTailChunkEvent;
using simulator_app_internal::WriteMetricsSnapshot;
using simulator_app_internal::WriteNodeLogTails;
using simulator_app_internal::WriteNodeStateEvent;
using simulator_app_internal::WriteRetainedRunRegistrySummary;
using simulator_app_internal::WriteSourceScenarioFile;
using simulator_app_internal::WriteTransactionLoadProgress;
using simulator_app_internal::WriteWalletMetricsSnapshot;

std::mutex node_network_state_mutex;
std::timed_mutex runtime_publication_mutex;
#ifdef BBP_ENABLE_TEST_HOOKS
std::function<void()> run_cleanup_root_removed_test_hook;
#endif

std::shared_ptr<std::timed_mutex> RuntimePublicationMutex() {
  static const std::shared_ptr<std::timed_mutex> mutex(
      &runtime_publication_mutex, [](std::timed_mutex*) {});
  return mutex;
}
std::mutex node_resource_state_mutex;

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

}  // namespace

namespace {

using simulator_app_internal::AddPeerTopologyJson;
using simulator_app_internal::BlockGenerationWorkloadJson;
using simulator_app_internal::BuildResolvedScenarioDocument;
using simulator_app_internal::CleanupRun;
using simulator_app_internal::DirectionalNetworkPoliciesJson;
using simulator_app_internal::EffectiveWorkloads;
using simulator_app_internal::IoLimitsJson;
using simulator_app_internal::NetworkBlockRuleDetail;
using simulator_app_internal::NetworkBlockRuleJson;
using simulator_app_internal::NetworkConditionJson;
using simulator_app_internal::NetworkConditionVerificationDetail;
using simulator_app_internal::NetworkPartitionRuleJson;
using simulator_app_internal::NodeRoleTopologyJson;
using simulator_app_internal::PerfCounterNamesJson;
using simulator_app_internal::QdiscJson;
using simulator_app_internal::ResourceLimitPatchJson;
using simulator_app_internal::ResourceLimitsJson;
using simulator_app_internal::RuntimePeerTopologyEdgeJson;
using simulator_app_internal::RuntimePeerTopologyEdgesJson;
using simulator_app_internal::ScenarioNodeWalletConfigJson;
using simulator_app_internal::ScheduledScenarioEventJson;
using simulator_app_internal::ShutdownLiveInstrumentation;
using simulator_app_internal::VerifyResourceLimits;
using simulator_app_internal::WaitForPeersWorkloadJson;
using simulator_app_internal::WaitUntilHeightWorkloadJson;
using simulator_app_internal::WalletTransactionsWorkloadJson;
using simulator_app_internal::WorkloadJson;
using simulator_app_internal::WriteScenarioFiles;
using simulator_app_internal::WriteTransactionLoadCompletions;
using simulator_app_internal::YamlFromJson;

}  // namespace

namespace {

void RequireSafeOutputDirectory(const std::filesystem::path& output_dir) {
  if (output_dir.empty()) {
    throw std::runtime_error("output directory must not be empty");
  }
  const std::filesystem::path absolute = std::filesystem::absolute(output_dir);
  if (absolute == absolute.root_path()) {
    throw std::runtime_error("output directory must not be filesystem root");
  }
}

bool StartNodeProcessWithPolicy(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node, std::string_view reason,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    bool first_attempt_is_restart, bool transition_to_running,
    std::stop_token stop_token,
    const ChainNodeConfig* process_config_override = nullptr) {
  return simulator_app_internal::StartNodeProcessWithPolicy(
      options, events_path, driver, node, node_network_state_mutex, reason,
      lifecycle_epoch, first_attempt_is_restart, transition_to_running,
      stop_token, process_config_override);
}

bool StartPreparedNode(const Options& options,
                       const std::filesystem::path& events_path,
                       const ChainDriver& driver, NodeRuntime& node,
                       std::string_view reason,
                       std::chrono::steady_clock::time_point lifecycle_epoch,
                       std::stop_token stop_token) {
  return simulator_app_internal::StartPreparedNode(
      options, events_path, driver, node, node_network_state_mutex, reason,
      lifecycle_epoch, stop_token);
}

void ConnectAvailableStartupPeers(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, auto& nodes,
    std::optional<std::size_t> changed_node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token) {
  simulator_app_internal::ConnectAvailableStartupPeers(
      options, events_path, driver, nodes, node_network_state_mutex,
      changed_node, lifecycle_epoch, stop_token);
}

bool RestartNode(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver,
    PeerConnectivityController& peer_connectivity_controller, NodeRuntime& node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token, std::string_view reason = "requested",
    SimulationCommandControl* operation_control = nullptr,
    NodeRestartAdmission* admitted_state = nullptr,
    bool request_topology_restore = true,
    const ChainNodeConfig* process_config_override = nullptr,
    bool publish_running = true,
    SimulationCommandControl* cancellation_commit_control = nullptr,
    std::stop_token committed_stop_token = {}) {
  return simulator_app_internal::RestartNode(
      options, events_path, driver, peer_connectivity_controller, node,
      lifecycle_epoch, StartNodeProcessWithPolicy, stop_token, reason,
      operation_control, admitted_state, request_topology_restore,
      process_config_override, publish_running, cancellation_commit_control,
      committed_stop_token);
}

void ApplyRuntimeNodeRestarts(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver,
    PeerConnectivityController& peer_connectivity_controller, auto& nodes,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token) {
  simulator_app_internal::ApplyRuntimeNodeRestarts(
      options, events_path, driver, peer_connectivity_controller, nodes,
      lifecycle_epoch, StartNodeProcessWithPolicy, stop_token);
}

std::vector<bool> StopNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, auto& nodes, bool best_effort = false,
    bool remove_run_cgroup = true,
    const std::vector<std::uint32_t>* explicit_resource_slots = nullptr,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token cleanup_stop_token = {},
    bool allow_partial_preparation = false) {
  return StopRuntimeNodes(
      options, events_path, driver, nodes, RuntimeNodeResourceEntryFor,
      best_effort, remove_run_cgroup, explicit_resource_slots,
      absolute_deadline, cleanup_stop_token, allow_partial_preparation);
}

std::unique_lock<std::timed_mutex> AcquireNodeMutationLock(
    std::timed_mutex& mutex, std::stop_token stop_token) {
  std::unique_lock<std::timed_mutex> lock(mutex, std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(20))) {
    ThrowIfStopRequested(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  return lock;
}

std::unique_lock<std::timed_mutex> AcquireRuntimePublicationLock(
    std::stop_token stop_token) {
  std::unique_lock<std::timed_mutex> lock(runtime_publication_mutex,
                                          std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(20))) {
    ThrowIfStopRequested(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  return lock;
}

bool SimulationCommandRequiresNodeMutationLock(SimulationCommandKind kind) {
  switch (kind) {
    case SimulationCommandKind::kSetBlockProductionPolicy:
    case SimulationCommandKind::kGenerateBlocks:
    case SimulationCommandKind::kExportNodeReport:
    case SimulationCommandKind::kAssignRole:
    case SimulationCommandKind::kRemoveRole:
      return false;
    case SimulationCommandKind::kCount:
      return false;
    default:
      return true;
  }
}

RuntimeNodeAdditionDependencies MakeRuntimeNodeAdditionDependencies() {
  return RuntimeNodeAdditionDependencies{
      .network_state_mutex = node_network_state_mutex,
      .resource_entry =
          [](const Options& options, const ChainNodeConfig& config,
             std::uint32_t resource_slot, RuntimeNodeResourceState state) {
            return RuntimeNodeResourceEntryFor(options, config, resource_slot,
                                               state);
          },
      .resource_manifest =
          [](const Options& options, const RuntimeNodeSnapshot& nodes) {
            return RuntimeNodeResourceManifestFor(options, nodes);
          },
      .directional_network_policies =
          [](const RuntimePeerTopology& topology,
             const SimulationNetworkAddressPlan& address_plan,
             const std::vector<std::uint32_t>& resource_slots,
             std::uint32_t node_index) {
            return DynamicDirectionalNetworkPolicies(
                topology, address_plan, resource_slots, node_index);
          },
      .topology_peer_ids =
          [](const RuntimePeerTopology& topology,
             const std::vector<ChainNodeConfig>& configs,
             std::uint32_t node_index) {
            return DynamicTopologyPeerIds(topology, configs, node_index);
          },
      .restart_peer_endpoints =
          [](const NodeRoleTopology& role_topology,
             const RuntimePeerTopology& topology,
             const std::vector<ChainNodeConfig>& configs,
             std::uint32_t node_index) {
            return DynamicRestartPeerEndpoints(role_topology, topology, configs,
                                               node_index);
          },
      .physical_peer_required =
          [](const RuntimePeerTopology& topology, std::uint32_t first,
             std::uint32_t second) {
            return topology.PhysicalPeerRequired(first, second);
          },
      .physical_peer_endpoints =
          [](const RuntimePeerTopology& topology,
             const std::vector<ChainNodeConfig>& configs,
             std::uint32_t node_index) {
            return DynamicPhysicalTopologyPeerEndpoints(topology, configs,
                                                        node_index);
          },
      .prepare_node =
          [](const Options& options, const std::filesystem::path& events_path,
             NodeRuntime& runtime, ChainNodeConfig config,
             std::uint32_t resource_slot, ResourceLimits resources,
             std::string resource_profile, std::string network_profile,
             std::vector<DirectionalNetworkPolicy> directional_network_policies,
             std::optional<NetworkCondition> network_condition,
             RunProcessState& run_process_state, std::stop_token stop_token,
             bool* runtime_root_acquired) {
            PrepareNodeRuntime(options, events_path, runtime, std::move(config),
                               resource_slot, std::move(resources),
                               std::move(resource_profile),
                               std::move(network_profile),
                               std::move(directional_network_policies),
                               std::move(network_condition), run_process_state,
                               stop_token, runtime_root_acquired);
          },
      .start_node =
          [](const Options& options, const std::filesystem::path& events_path,
             const ChainDriver& driver, NodeRuntime& node,
             std::string_view reason,
             std::chrono::steady_clock::time_point lifecycle_epoch,
             std::stop_token stop_token) {
            return StartPreparedNode(options, events_path, driver, node, reason,
                                     lifecycle_epoch, stop_token);
          },
      .restart_masternode =
          [](const Options& options, const std::filesystem::path& events_path,
             const ChainDriver& driver,
             PeerConnectivityController& peer_controller, NodeRuntime& node,
             std::chrono::steady_clock::time_point lifecycle_epoch,
             std::stop_token stop_token, std::string_view reason) {
            return RestartNode(options, events_path, driver, peer_controller,
                               node, lifecycle_epoch, stop_token, reason,
                               nullptr, nullptr, false);
          },
      .stop_candidates =
          [](const Options& options, const std::filesystem::path& events_path,
             const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
             const std::vector<std::uint32_t>& resource_slots,
             std::chrono::steady_clock::time_point deadline,
             std::stop_token stop_token) {
            return StopNodes(options, events_path, driver, nodes, true, false,
                             &resource_slots, deadline, stop_token, true);
          },
      .acquire_publication_lock =
          [](std::stop_token stop_token) {
            return AcquireRuntimePublicationLock(stop_token);
          },
  };
}

RuntimeNodeReplacementDependencies MakeRuntimeNodeReplacementDependencies() {
  return RuntimeNodeReplacementDependencies{
      .network_state_mutex = node_network_state_mutex,
      .resource_state_mutex = node_resource_state_mutex,
      .resource_manifest =
          [](const Options& options, const RuntimeNodeSnapshot& nodes) {
            return RuntimeNodeResourceManifestFor(options, nodes);
          },
      .wait_for_frozen_state =
          [](const Cgroup& cgroup, bool expected, std::stop_token stop_token) {
            return WaitForNodeFrozenState(cgroup, expected, stop_token);
          },
      .stop_node =
          [](const Options& options, const std::filesystem::path& events_path,
             const ChainDriver& driver, NodeRuntime& node,
             std::stop_token stop_token, bool allow_rpc_unavailable) {
            StopNodeProcess(options, events_path, driver, node, stop_token,
                            allow_rpc_unavailable);
          },
      .start_node =
          [](const Options& options, const std::filesystem::path& events_path,
             const ChainDriver& driver, NodeRuntime& node,
             std::string_view reason,
             std::chrono::steady_clock::time_point lifecycle_epoch,
             bool first_attempt_is_restart, bool transition_to_running,
             std::stop_token stop_token) {
            return StartNodeProcessWithPolicy(
                options, events_path, driver, node, reason, lifecycle_epoch,
                first_attempt_is_restart, transition_to_running, stop_token);
          },
      .acquire_publication_lock =
          [](std::stop_token stop_token) {
            return AcquireRuntimePublicationLock(stop_token);
          },
  };
}

RuntimeNodeRemovalDependencies MakeRuntimeNodeRemovalDependencies() {
  return RuntimeNodeRemovalDependencies{
      .network_state_mutex = node_network_state_mutex,
      .resource_manifest =
          [](const Options& options, const RuntimeNodeSnapshot& nodes) {
            return RuntimeNodeResourceManifestFor(options, nodes);
          },
      .directional_network_policies =
          [](const RuntimePeerTopology& topology,
             const SimulationNetworkAddressPlan& address_plan,
             const std::vector<std::uint32_t>& resource_slots,
             std::uint32_t node_index) {
            return DynamicDirectionalNetworkPolicies(
                topology, address_plan, resource_slots, node_index);
          },
      .topology_peer_ids =
          [](const RuntimePeerTopology& topology,
             const std::vector<ChainNodeConfig>& configs,
             std::uint32_t node_index) {
            return DynamicTopologyPeerIds(topology, configs, node_index);
          },
      .restart_peer_endpoints =
          [](const NodeRoleTopology& role_topology,
             const RuntimePeerTopology& topology,
             const std::vector<ChainNodeConfig>& configs,
             std::uint32_t node_index) {
            return DynamicRestartPeerEndpoints(role_topology, topology, configs,
                                               node_index);
          },
      .physical_peer_endpoints =
          [](const RuntimePeerTopology& topology,
             const std::vector<ChainNodeConfig>& configs,
             std::uint32_t node_index) {
            return DynamicPhysicalTopologyPeerEndpoints(topology, configs,
                                                        node_index);
          },
      .stop_retired_nodes =
          [](const Options& options, const std::filesystem::path& events_path,
             const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
             const std::vector<std::uint32_t>& resource_slots,
             std::chrono::steady_clock::time_point deadline) {
            return StopNodes(options, events_path, driver, nodes, false, false,
                             &resource_slots, deadline);
          },
      .acquire_publication_lock =
          [](std::stop_token stop_token) {
            return AcquireRuntimePublicationLock(stop_token);
          },
  };
}

std::vector<std::uint32_t> ConfiguredMinerIndexes(const Options& options) {
  if (options.empty_control_plane) {
    return {};
  }
  if (options.topology.configured) {
    return options.topology.miner_nodes;
  }
  return {options.generate_node - 1U};
}

NodeRuntime& FindNodeRuntimeById(auto& nodes, const std::string& node_id) {
  const auto node = std::find_if(nodes.begin(), nodes.end(),
                                 [&node_id](const NodeRuntime& candidate) {
                                   return candidate.config.id == node_id;
                                 });
  if (node == nodes.end()) {
    throw std::runtime_error("unknown block producer node: " + node_id);
  }
  return *node;
}

enum class BenchmarkTerminalOutcome {
  kFinished,
  kCancelled,
};

struct BenchmarkHeadlessResult {
  int result = 0;
  BenchmarkTerminalOutcome terminal_outcome =
      BenchmarkTerminalOutcome::kFinished;
};

BenchmarkHeadlessResult RunBenchmarkHeadless(
    Options options, SimulationCommandQueue& command_queue,
    McpLiveApplication& mcp_application, RuntimeNodeInventory& node_inventory,
    std::stop_source& simulation_stop_source,
    std::atomic<RunStopTick>& run_stop_tick,
    std::stop_token external_stop_token = {}) {
  const auto record_run_stop =
      [&](std::chrono::steady_clock::time_point observed_at) {
        RecordRunStop(run_stop_tick, observed_at);
      };
  const auto request_simulation_stop = [&] {
    record_run_stop(std::chrono::steady_clock::now());
    return simulation_stop_source.request_stop();
  };
  std::stop_callback stop_simulation_on_external_request(
      external_stop_token, [&] {
        mcp_application.MarkRunStopping();
        request_simulation_stop();
      });
  const std::stop_token stop_token = simulation_stop_source.get_token();
  std::stop_callback observe_run_stop(
      stop_token, [&] { record_run_stop(std::chrono::steady_clock::now()); });
  const auto observed_run_stop = [&] { return ObservedRunStop(run_stop_tick); };
  SimulationCommandQueue* active_command_queue = &command_queue;
  std::mutex scheduled_command_outcome_mutex;
  std::condition_variable_any scheduled_command_outcome_ready;
  std::map<std::uint32_t, std::optional<std::string>>
      scheduled_command_outcomes;
  const auto record_scheduled_command_outcome =
      [&](const SimulationCommand& command,
          std::optional<std::string_view> error) {
        if (!command.scheduled_event_sequence) {
          return;
        }
        std::lock_guard<std::mutex> lock(scheduled_command_outcome_mutex);
        auto [outcome, inserted] = scheduled_command_outcomes.emplace(
            *command.scheduled_event_sequence,
            error ? std::optional<std::string>(*error) : std::nullopt);
        if (!inserted) {
          outcome->second = "scheduled command produced more than one outcome";
        }
        scheduled_command_outcome_ready.notify_all();
      };
  const auto wait_for_scheduled_command =
      [&](std::uint32_t scheduled_event_sequence) {
        std::unique_lock<std::mutex> lock(scheduled_command_outcome_mutex);
        const bool ready =
            scheduled_command_outcome_ready.wait(lock, stop_token, [&] {
              return scheduled_command_outcomes.contains(
                  scheduled_event_sequence);
            });
        if (!ready) {
          throw SimulationCancelled();
        }
        std::optional<std::string> outcome =
            std::move(scheduled_command_outcomes.at(scheduled_event_sequence));
        scheduled_command_outcomes.erase(scheduled_event_sequence);
        return outcome;
      };
  const auto run_root = BenchmarkRunRoot(options);
  static_cast<void>(RequireRunOwnership(options));
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  auto runtime_topology = std::make_unique<RuntimePeerTopology>(
      options.topology.peer_topology, options.nodes,
      options.empty_control_plane);
  PeerTopologyConfig live_topology_config = options.topology.peer_topology;
  SimulationRegistry simulation_registry = SimulationRegistry::FromTopology(
      options.topology, options.wallet_initialization);
  RuntimeWalletRegistry runtime_wallet_registry;

  const auto events_path = run_root / "events.jsonl";
  const auto metrics_path = run_root / "metrics.jsonl";
  const auto wallet_metrics_path = run_root / "wallet-metrics.jsonl";
  WriteEvent(events_path, options.run_id, "sim",
             SimulationEventKind::kRunStarted);

  std::unique_ptr<ChainDriver> driver_owner = CreateChainDriver(options.chain);
  ChainDriver& driver = *driver_owner;
  std::vector<NodeRuntime> startup_nodes;
  RuntimeNodeSnapshot nodes;
  std::unique_ptr<NodeLogCollector> log_collector;
  std::unique_ptr<PeriodicMetricsCollector> metrics_collector;
  std::unique_ptr<ProbabilisticBlockScheduler> block_scheduler;
  std::unique_ptr<PeerConnectivityController> peer_connectivity_controller;
  std::unique_ptr<ChainCommandExecutor> chain_command_executor;
  std::unique_ptr<SimulationCommandProcessor> command_processor;
  std::optional<std::jthread> duration_timer;
  std::optional<std::jthread> lifecycle_supervisor;
  std::optional<std::jthread> transaction_observer;
  std::optional<std::chrono::steady_clock::time_point> simulation_epoch;
  std::atomic<bool> simulation_duration_reached = false;
  std::atomic<std::uint64_t> duration_stop_requested_at_ms = 0U;
  RunProcessState run_process_state;
  std::stop_source command_rpc_stop_source;
  std::stop_source block_production_rpc_stop_source;
  std::stop_source metrics_rpc_stop_source;
  std::atomic<bool> wallets_initialized = false;
  TransactionObservationTracker transaction_tracker;
  std::vector<PendingTransactionLoadCompletion>
      pending_transaction_load_completions;
  auto wallet_workloads = std::make_shared<LiveWalletWorkloadRegistry>();
  auto block_generation_workloads =
      std::make_shared<LiveBlockGenerationWorkloadRegistry>();
  auto wait_until_height_workloads =
      std::make_shared<LiveWaitUntilHeightWorkloadRegistry>();
  auto wait_for_peers_workloads =
      std::make_shared<LiveWaitForPeersWorkloadRegistry>();
  auto live_instrumentation = MakeLiveInstrumentationRegistry();
  const RuntimeNodeAdditionDependencies runtime_node_addition_dependencies =
      MakeRuntimeNodeAdditionDependencies();
  const RuntimeNodeRemovalDependencies runtime_node_removal_dependencies =
      MakeRuntimeNodeRemovalDependencies();
  const RuntimeNodeReplacementDependencies
      runtime_node_replacement_dependencies =
          MakeRuntimeNodeReplacementDependencies();
  std::shared_ptr<McpLiveWorkloadService> installed_workload_service;
  std::shared_ptr<McpLiveInstrumentationService>
      installed_instrumentation_service;
  std::shared_ptr<McpLiveRoleService> installed_role_service;
  bool workload_shutdown_complete = false;
  bool workload_shutdown_safe_to_destroy = false;
  std::exception_ptr workload_shutdown_failure;
  std::atomic<std::shared_ptr<McpLiveRoleService>> command_role_service;
  std::mutex lifecycle_failure_mutex;
  std::timed_mutex node_mutation_mutex;
  std::timed_mutex one_shot_workload_mutex;
  std::uint64_t next_one_shot_invocation = 1U;
  std::mutex configured_miner_node_ids_mutex;
  std::timed_mutex block_generation_mutex;
  std::mutex runtime_topology_mutex;
  std::exception_ptr lifecycle_failure;
  std::stop_callback cancel_command_rpc(stop_token, [&command_rpc_stop_source] {
    command_rpc_stop_source.request_stop();
  });
  std::stop_callback cancel_block_production_rpc(
      stop_token, [&block_production_rpc_stop_source] {
        block_production_rpc_stop_source.request_stop();
      });
  std::stop_callback cancel_metrics_rpc(stop_token, [&metrics_rpc_stop_source] {
    metrics_rpc_stop_source.request_stop();
  });
  auto instrumentation_controller = MakeLiveInstrumentationController(
      options, metrics_path, events_path, driver, node_inventory,
      runtime_wallet_registry, run_process_state, node_mutation_mutex,
      {node_network_state_mutex, node_resource_state_mutex},
      live_instrumentation);
  const auto stop_instrumentation = [&](bool run_failed) {
    mcp_application.SetInstrumentationService(nullptr);
    std::exception_ptr shutdown_failure;
    try {
      ShutdownLiveInstrumentation(*instrumentation_controller, run_failed);
    } catch (...) {
      shutdown_failure = std::current_exception();
    }
    while (installed_instrumentation_service &&
           installed_instrumentation_service.use_count() > 1U) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    installed_instrumentation_service.reset();
    if (shutdown_failure) {
      std::rethrow_exception(shutdown_failure);
    }
  };
  const auto stop_role_mutations = [&] {
    mcp_application.SetRoleService(nullptr);
    command_rpc_stop_source.request_stop();
    command_role_service.store(nullptr, std::memory_order_release);
    while (installed_role_service && installed_role_service.use_count() > 1U) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    installed_role_service.reset();
  };
  const auto stop_duration_timer = [&]() {
    if (duration_timer) {
      duration_timer->request_stop();
      if (duration_timer->joinable()) {
        duration_timer->join();
      }
      duration_timer.reset();
    }
  };
  const auto stop_lifecycle_supervisor = [&]() {
    if (lifecycle_supervisor) {
      lifecycle_supervisor->request_stop();
      if (lifecycle_supervisor->joinable()) {
        lifecycle_supervisor->join();
      }
      lifecycle_supervisor.reset();
    }
  };
  const auto request_workload_shutdown =
      [&](bool run_failed) -> WorkloadShutdownRecords {
    const std::chrono::steady_clock::time_point shutdown_requested_at =
        observed_run_stop().value_or(std::chrono::steady_clock::now());
    return RequestLiveWorkloadShutdown(
        wallet_workloads, block_generation_workloads,
        wait_until_height_workloads, wait_for_peers_workloads, run_failed,
        shutdown_requested_at);
  };
  const auto stop_wallet_workloads = [&](bool run_failed) {
    if (workload_shutdown_complete) {
      if (workload_shutdown_failure) {
        std::rethrow_exception(workload_shutdown_failure);
      }
      return;
    }
    const std::chrono::steady_clock::time_point shutdown_deadline =
        std::chrono::steady_clock::now() + kWorkloadServiceShutdownBound;
    std::exception_ptr shutdown_failure;
    const auto remember_shutdown_failure =
        [&](const std::exception_ptr& failure) noexcept {
          if (!shutdown_failure) {
            shutdown_failure = failure;
          }
        };
    try {
      mcp_application.CloseWorkloadService(shutdown_deadline);
    } catch (const std::exception& error) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload service admission closure failed; retrying: "
                     << error.what();
      try {
        mcp_application.CloseWorkloadService(shutdown_deadline);
      } catch (...) {
        BBP_LOG(error)
            << "workload service admission closure failed repeatedly";
        std::terminate();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload service admission closure failed; retrying";
      try {
        mcp_application.CloseWorkloadService(shutdown_deadline);
      } catch (...) {
        BBP_LOG(error)
            << "workload service admission closure failed repeatedly";
        std::terminate();
      }
    }
    WorkloadShutdownRecords retained_records;
    try {
      retained_records = request_workload_shutdown(run_failed);
    } catch (const std::exception& error) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload lifecycle cancellation failed; retrying: "
                     << error.what();
      try {
        retained_records = request_workload_shutdown(run_failed);
      } catch (...) {
        BBP_LOG(error) << "workload lifecycle cancellation failed repeatedly";
        std::terminate();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload lifecycle cancellation failed; retrying";
      try {
        retained_records = request_workload_shutdown(run_failed);
      } catch (...) {
        BBP_LOG(error) << "workload lifecycle cancellation failed repeatedly";
        std::terminate();
      }
    }
    try {
      mcp_application.RequestWorkloadServiceCancellation();
    } catch (const std::exception& error) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload service cancellation failed; retrying: "
                     << error.what();
      try {
        mcp_application.RequestWorkloadServiceCancellation();
      } catch (...) {
        BBP_LOG(error) << "workload service cancellation failed repeatedly";
        std::terminate();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload service cancellation failed; retrying";
      try {
        mcp_application.RequestWorkloadServiceCancellation();
      } catch (...) {
        BBP_LOG(error) << "workload service cancellation failed repeatedly";
        std::terminate();
      }
    }
    std::optional<McpLiveWorkloadDrainResult> shutdown_deadline_snapshot;
    bool safe_to_destroy = false;
    try {
      const McpLiveWorkloadDrainResult drain_result =
          mcp_application.WaitForWorkloadServiceDrain();
      safe_to_destroy = drain_result.safe_to_destroy();
      if (!safe_to_destroy) {
        shutdown_deadline_snapshot = drain_result;
        mcp_application.PublishWorkloadServiceShutdownTimeout(
            drain_result, std::chrono::duration_cast<std::chrono::milliseconds>(
                              kWorkloadServiceShutdownBound));
        BBP_LOG(error)
            << "workload service did not drain within the 15000 ms shutdown "
               "bound; active_callbacks="
            << drain_result.active_callback_count
            << "; active_workers=" << drain_result.active_worker_count;
        try {
          const WorkloadServiceShutdownTimeout shutdown_timeout(drain_result);
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kRunFailed,
                     boost::json::serialize(shutdown_timeout.Diagnostic()));
        } catch (const std::exception& error) {
          BBP_LOG(error) << "workload service shutdown timeout evidence "
                            "publication failed: "
                         << error.what();
        } catch (...) {
          BBP_LOG(error) << "workload service shutdown timeout evidence "
                            "publication failed";
        }
      }
    } catch (const std::exception& error) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "bounded workload service drain failed: "
                     << error.what();
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "bounded workload service drain failed";
    }
    if (!safe_to_destroy) {
      try {
        const McpLiveWorkloadDrainResult quarantine_result =
            mcp_application.WaitForWorkloadServiceQuarantine();
        safe_to_destroy = quarantine_result.safe_to_destroy();
      } catch (const std::exception& error) {
        remember_shutdown_failure(std::current_exception());
        BBP_LOG(error) << "workload service quarantine wait failed: "
                       << error.what();
      } catch (...) {
        remember_shutdown_failure(std::current_exception());
        BBP_LOG(error) << "workload service quarantine wait failed";
      }
    }
    if (!safe_to_destroy && installed_workload_service) {
      try {
        const McpLiveWorkloadDrainResult quarantine_result =
            installed_workload_service->WaitUntilDrained();
        safe_to_destroy = quarantine_result.safe_to_destroy();
      } catch (const std::exception& error) {
        remember_shutdown_failure(std::current_exception());
        BBP_LOG(error) << "direct workload service quarantine wait failed: "
                       << error.what();
      } catch (...) {
        remember_shutdown_failure(std::current_exception());
        BBP_LOG(error) << "direct workload service quarantine wait failed";
      }
    }
    if (!safe_to_destroy) {
      BBP_LOG(error)
          << "workload service quarantine did not prove referenced simulator "
             "state safe to destroy";
      std::terminate();
    }
    workload_shutdown_safe_to_destroy = true;
    const auto join_worker = [](std::thread& worker) {
      if (!worker.joinable()) {
        return;
      }
      try {
        worker.join();
      } catch (...) {
        BBP_LOG(error) << "drained workload worker could not be joined";
        std::terminate();
      }
    };
    for (std::size_t index = 0U; index < retained_records.wallet_count;
         ++index) {
      JoinLiveWalletWorkloadWorker(*retained_records.wallets[index]);
    }
    for (std::size_t index = 0U; index < retained_records.height_wait_count;
         ++index) {
      join_worker(retained_records.height_waits[index]->worker);
    }
    for (std::size_t index = 0U; index < retained_records.peer_wait_count;
         ++index) {
      join_worker(retained_records.peer_waits[index]->worker);
    }
    for (std::size_t index = 0U; index < retained_records.block_generator_count;
         ++index) {
      join_worker(retained_records.block_generators[index]->worker);
    }
    for (std::size_t index = 0U; index < retained_records.wallet_count;
         ++index) {
      const std::shared_ptr<LiveWalletWorkloadRecord>& record =
          retained_records.wallets[index];
      try {
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (!IsTerminalLiveWalletWorkloadState(record->state)) {
          record->state = run_failed ? LiveWalletWorkloadState::kFailed
                                     : LiveWalletWorkloadState::kCancelled;
          record->terminal_outcome = run_failed ? "failed" : "cancelled";
          if (run_failed && !record->failure) {
            record->failure = "run failed while wallet workload was active";
          }
          record->changed.notify_all();
        }
      } catch (...) {
        remember_shutdown_failure(std::current_exception());
      }
    }
    for (std::size_t index = 0U; index < retained_records.height_wait_count;
         ++index) {
      const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>& record =
          retained_records.height_waits[index];
      try {
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (!IsTerminalLiveWorkloadState(record->state)) {
          record->state = run_failed ? LiveWorkloadState::kFailed
                                     : LiveWorkloadState::kCancelled;
          record->terminal_outcome = run_failed ? "failed" : "cancelled";
          if (run_failed && !record->failure) {
            record->failure =
                "run failed while wait-until-height workload was active";
          }
          record->changed.notify_all();
        }
      } catch (...) {
        remember_shutdown_failure(std::current_exception());
      }
    }
    for (std::size_t index = 0U; index < retained_records.peer_wait_count;
         ++index) {
      const std::shared_ptr<LiveWaitForPeersWorkloadRecord>& record =
          retained_records.peer_waits[index];
      try {
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (!IsTerminalLiveWorkloadState(record->state)) {
          record->state = run_failed ? LiveWorkloadState::kFailed
                                     : LiveWorkloadState::kCancelled;
          record->terminal_outcome = run_failed ? "failed" : "cancelled";
          if (run_failed && !record->failure) {
            record->failure =
                "run failed while wait-for-peers workload was active";
          }
          record->changed.notify_all();
        }
      } catch (...) {
        remember_shutdown_failure(std::current_exception());
      }
    }
    for (std::size_t index = 0U; index < retained_records.block_generator_count;
         ++index) {
      const std::shared_ptr<LiveBlockGenerationWorkloadRecord>& record =
          retained_records.block_generators[index];
      try {
        std::lock_guard<std::mutex> record_lock(record->mutex);
        if (!IsTerminalLiveWorkloadState(record->state)) {
          record->state = run_failed ? LiveWorkloadState::kFailed
                                     : LiveWorkloadState::kCancelled;
          record->terminal_outcome = run_failed ? "failed" : "cancelled";
          if (run_failed && !record->failure) {
            record->failure =
                "run failed while block generation workload was active";
          }
          record->changed.notify_all();
        }
      } catch (...) {
        remember_shutdown_failure(std::current_exception());
      }
    }
    installed_workload_service.reset();
    workload_shutdown_complete = true;
    if (shutdown_deadline_snapshot) {
      try {
        workload_shutdown_failure = std::make_exception_ptr(
            WorkloadServiceShutdownTimeout(*shutdown_deadline_snapshot));
      } catch (...) {
        workload_shutdown_failure = std::current_exception();
      }
    } else {
      workload_shutdown_failure = shutdown_failure;
    }
    if (workload_shutdown_failure) {
      std::rethrow_exception(workload_shutdown_failure);
    }
  };
  const auto stop_transaction_observer = [&] {
    if (transaction_observer) {
      transaction_observer->request_stop();
      if (transaction_observer->joinable()) {
        transaction_observer->join();
      }
      transaction_observer.reset();
    }
  };
  const auto stop_command_processor = [&]() {
    command_rpc_stop_source.request_stop();
    if (command_processor) {
      command_processor->Stop();
    } else if (active_command_queue != nullptr) {
      for (const SimulationCommand& command : active_command_queue->Cancel()) {
        constexpr std::string_view kCancellation =
            "simulation stopped before command processor startup";
        if ((command.kind == SimulationCommandKind::kAddNodes ||
             command.kind == SimulationCommandKind::kReplaceNode ||
             command.kind == SimulationCommandKind::kRemoveNodes ||
             command.kind == SimulationCommandKind::kAssignRole ||
             command.kind == SimulationCommandKind::kRemoveRole) &&
            command.operation_control) {
          static_cast<void>(command.operation_control->RequestCancellation(
              SimulationCommandCancellationCause::kApplicationShutdown));
        }
        record_scheduled_command_outcome(
            command, std::optional<std::string_view>(kCancellation));
        mcp_application.RecordCommandOutcome(
            command,
            SimulationCommandOutcome{
                .state = SimulationCommandOutcomeState::kCancelled,
                .cancellation_cause =
                    SimulationCommandCancellationCause::kApplicationShutdown,
                .error = std::string(kCancellation),
                .node_lifecycle = std::nullopt,
                .added_node_ids = {},
                .removed_node_ids = {},
                .inventory_generation = std::nullopt,
                .final_node_count = std::nullopt,
            });
      }
    }
  };
  const auto stop_peer_connectivity = [&]() {
    if (peer_connectivity_controller) {
      peer_connectivity_controller->Stop();
    }
  };
  const auto stop_block_production = [&]() {
    block_production_rpc_stop_source.request_stop();
    if (block_scheduler) {
      block_scheduler->Stop();
    }
    const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
    std::exception_ptr first_failure;
    std::vector<std::string> active_native_miners;
    {
      auto process_guard = run_process_state.Lock();
      active_native_miners =
          run_process_state.ActiveNativeMiners(process_guard);
    }
    for (const std::string& node_id : active_native_miners) {
      try {
        StopNativeMining(driver, FindNodeRuntimeById(current_nodes, node_id),
                         run_process_state);
      } catch (...) {
        if (!first_failure) {
          first_failure = std::current_exception();
        }
      }
    }
    if (first_failure) {
      std::rethrow_exception(first_failure);
    }
  };
  const auto cleanup_step = [](std::string_view component, auto&& action) {
    try {
      action();
    } catch (const std::exception& error) {
      BBP_LOG(error) << component
                     << " failed during run cleanup: " << error.what();
    } catch (...) {
      BBP_LOG(error) << component << " failed during run cleanup";
    }
  };
  const auto cleanup_workload_step =
      [&](bool run_failed) -> std::exception_ptr {
    try {
      stop_wallet_workloads(run_failed);
      return {};
    } catch (const WorkloadServiceShutdownTimeout& error) {
      if (!workload_shutdown_safe_to_destroy) {
        BBP_LOG(error)
            << "workload shutdown timeout escaped before a safe drain";
        std::terminate();
      }
      BBP_LOG(error) << "wallet workload shutdown failed during run cleanup: "
                     << error.what();
      return std::current_exception();
    } catch (const std::exception& error) {
      if (!workload_shutdown_safe_to_destroy) {
        BBP_LOG(error)
            << "workload shutdown failure escaped before a safe drain";
        std::terminate();
      }
      BBP_LOG(error) << "wallet workload shutdown failed during run cleanup: "
                     << error.what();
      return std::current_exception();
    } catch (...) {
      if (!workload_shutdown_safe_to_destroy) {
        BBP_LOG(error)
            << "unknown workload shutdown failure escaped before a safe drain";
        std::terminate();
      }
      BBP_LOG(error) << "wallet workload shutdown failed during run cleanup";
      return std::current_exception();
    }
  };
  const auto handle_run_failure = [&](std::string_view detail) {
    mcp_application.MarkRunStopping();
    stop_duration_timer();
    stop_lifecycle_supervisor();
    // The pre-existing run failure remains primary; workload shutdown retains
    // its own failure only after reaching a safe drain.
    static_cast<void>(cleanup_workload_step(true));
    cleanup_step("instrumentation shutdown",
                 [&] { stop_instrumentation(true); });
    cleanup_step("role mutation shutdown", stop_role_mutations);
    cleanup_step("transaction observer shutdown", stop_transaction_observer);
    cleanup_step("command processor shutdown", stop_command_processor);
    cleanup_step("peer connectivity shutdown", stop_peer_connectivity);
    cleanup_step("block production shutdown", stop_block_production);
    cleanup_step("metrics collector shutdown", [&] {
      if (metrics_collector) {
        metrics_rpc_stop_source.request_stop();
        metrics_collector->Stop();
      }
    });
    const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
    for (auto& node : current_nodes) {
      cleanup_step("failed node state event", [&] {
        TransitionNodeState(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kFailed);
      });
    }
    cleanup_step("run failure event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunFailed, detail);
    });
    if (!log_collector) {
      cleanup_step("node log tail collection", [&] {
        WriteNodeLogTails(events_path, options, driver, current_nodes);
      });
    }
    cleanup_step("node shutdown", [&] {
      StopNodes(options, events_path, driver, current_nodes, true);
    });
    for (auto& node : current_nodes) {
      cleanup_step("node process fallback signal",
                   [&] { static_cast<void>(RequestNodeKill(node)); });
    }
    const auto fallback_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (auto& node : current_nodes) {
      cleanup_step("node process fallback shutdown", [&] {
        if (!WaitForNodeProcessExitUntil(node, fallback_deadline)) {
          throw std::runtime_error("node process survived fallback SIGKILL: " +
                                   node.config.id);
        }
      });
    }
    if (log_collector) {
      cleanup_step("node log collector shutdown",
                   [&] { log_collector->Stop(); });
    } else {
      cleanup_step("final node log tail collection", [&] {
        WriteNodeLogTails(events_path, options, driver, current_nodes);
      });
    }
    cleanup_step("MCP terminal launcher cleanup",
                 [&] { mcp_application.MarkRunStopped(); });
  };
  const auto handle_run_cancellation = [&]() {
    mcp_application.MarkRunStopping();
    stop_duration_timer();
    stop_lifecycle_supervisor();
    const std::exception_ptr workload_shutdown_error =
        cleanup_workload_step(false);
    cleanup_step("instrumentation shutdown",
                 [&] { stop_instrumentation(false); });
    cleanup_step("role mutation shutdown", stop_role_mutations);
    cleanup_step("transaction observer shutdown", stop_transaction_observer);
    cleanup_step("command processor shutdown", stop_command_processor);
    cleanup_step("peer connectivity shutdown", stop_peer_connectivity);
    cleanup_step("block production shutdown", stop_block_production);
    cleanup_step("metrics collector shutdown", [&] {
      if (metrics_collector) {
        metrics_rpc_stop_source.request_stop();
        metrics_collector->Stop();
      }
    });
    if (!workload_shutdown_error) {
      cleanup_step("run cancellation event", [&] {
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kRunCancelled);
      });
    }
    const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
    cleanup_step("node shutdown", [&] {
      StopNodes(options, events_path, driver, current_nodes, true);
    });
    cleanup_step("node log collector shutdown", [&] {
      if (log_collector) {
        log_collector->Stop();
      } else {
        WriteNodeLogTails(events_path, options, driver, current_nodes);
      }
    });
    if (workload_shutdown_error) {
      std::rethrow_exception(workload_shutdown_error);
    }
    cleanup_step("run finished event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunFinished);
    });
    mcp_application.MarkRunStopped();
    BBP_LOG(info) << "cancelled run " << options.run_id;
  };
  const auto handle_simulation_duration = [&]() {
    mcp_application.MarkRunStopping();
    stop_duration_timer();
    stop_lifecycle_supervisor();
    const std::exception_ptr workload_shutdown_error =
        cleanup_workload_step(false);
    cleanup_step("instrumentation shutdown",
                 [&] { stop_instrumentation(false); });
    cleanup_step("role mutation shutdown", stop_role_mutations);
    cleanup_step("transaction observer shutdown", stop_transaction_observer);
    cleanup_step("command processor shutdown", stop_command_processor);
    cleanup_step("peer connectivity shutdown", stop_peer_connectivity);
    cleanup_step("block production shutdown", stop_block_production);
    cleanup_step("metrics collector shutdown", [&] {
      if (metrics_collector) {
        metrics_rpc_stop_source.request_stop();
        metrics_collector->Stop();
      }
    });
    if (!workload_shutdown_error) {
      cleanup_step("simulation duration event", [&] {
        boost::json::object detail;
        detail["duration_ms"] = options.simulation_duration->count();
        detail["wall_duration_ms"] =
            options.time_scale.WallDuration(*options.simulation_duration)
                .count();
        detail["time_scale"] = options.time_scale.value();
        detail["stop_requested_at_ms"] =
            duration_stop_requested_at_ms.load(std::memory_order_acquire);
        detail["elapsed_wall_ms"] =
            simulation_epoch
                ? ElapsedMilliseconds(*simulation_epoch,
                                      std::chrono::steady_clock::now())
                : 0U;
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kSimulationDurationReached,
                   boost::json::serialize(detail));
      });
    }
    const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
    cleanup_step("node shutdown", [&] {
      StopNodes(options, events_path, driver, current_nodes, true);
    });
    cleanup_step("node log collector shutdown", [&] {
      if (log_collector) {
        log_collector->Stop();
      } else {
        WriteNodeLogTails(events_path, options, driver, current_nodes);
      }
    });
    if (workload_shutdown_error) {
      std::rethrow_exception(workload_shutdown_error);
    }
    cleanup_step("run finished event", [&] {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRunFinished);
    });
    mcp_application.MarkRunStopped();
    BBP_LOG(info) << "simulation duration reached for run " << options.run_id;
  };
  const bool timed_node_lifecycle = HasTimedNodeLifecycle(options);
  std::chrono::steady_clock::time_point lifecycle_epoch;
  bool operator_connection_resolved = false;
  std::chrono::steady_clock::time_point event_engine_epoch;
  const auto start_duration_timer =
      [&](std::chrono::steady_clock::time_point epoch) {
        simulation_epoch = epoch;
        if (!options.simulation_duration) {
          return;
        }
        const std::chrono::milliseconds wall_duration =
            options.time_scale.WallDuration(*options.simulation_duration);
        const auto duration_deadline = SteadyDeadline(epoch, wall_duration);
        duration_timer.emplace([duration_deadline, &simulation_duration_reached,
                                &duration_stop_requested_at_ms,
                                &mcp_application, &request_simulation_stop,
                                epoch](std::stop_token timer_stop_token) {
          try {
            WaitUntil(duration_deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          duration_stop_requested_at_ms.store(
              ElapsedMilliseconds(epoch, std::chrono::steady_clock::now()),
              std::memory_order_release);
          simulation_duration_reached.store(true, std::memory_order_release);
          mcp_application.MarkRunStopping();
          request_simulation_stop();
        });
      };
  BenchmarkTerminalOutcome terminal_outcome =
      BenchmarkTerminalOutcome::kFinished;
  try {
    Cgroup::PrepareRun(RequireRunOwnership(options));
  } catch (const std::exception& error) {
    mcp_application.MarkRunStopping();
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRunFailed, error.what());
    mcp_application.MarkRunStopped();
    throw;
  }
  lifecycle_epoch = std::chrono::steady_clock::now();
  event_engine_epoch = lifecycle_epoch;
  if (timed_node_lifecycle) {
    start_duration_timer(event_engine_epoch);
  }
  const auto initialize_node_inventory = [&] {
    try {
      node_inventory.Initialize(startup_nodes);
      nodes = node_inventory.Snapshot();
    } catch (...) {
      const std::exception_ptr initialization_failure =
          std::current_exception();
      cleanup_step("startup node shutdown after inventory failure", [&] {
        StopNodes(options, events_path, driver, startup_nodes, true);
      });
      std::rethrow_exception(initialization_failure);
    }
  };
  try {
    try {
      simulator_app_internal::StartInitialNodes(
          options, run_root, events_path, chain_spec, driver, *runtime_topology,
          startup_nodes, run_process_state, node_network_state_mutex,
          lifecycle_epoch, stop_token);
    } catch (...) {
      initialize_node_inventory();
      throw;
    }
    initialize_node_inventory();
    PublishOperatorConnectionCommand(options, run_root, events_path, driver,
                                     nodes, &operator_connection_resolved);
    const std::vector<std::uint32_t> miner_indexes =
        ConfiguredMinerIndexes(options);
    if (options.block_production.enabled && miner_indexes.empty()) {
      throw std::runtime_error(
          "enabled block production requires at least one configured miner");
    }
    std::vector<std::string> miner_node_ids;
    miner_node_ids.reserve(miner_indexes.size());
    for (const std::uint32_t miner_index : miner_indexes) {
      if (miner_index >= nodes.size()) {
        throw std::runtime_error(
            "configured miner index exceeds the running node count");
      }
      miner_node_ids.push_back(nodes[miner_index].config.id);
      if (options.block_production.enabled &&
          options.block_production.difficulty &&
          nodes[miner_index].AllowsChainMetrics()) {
        driver.SetMiningDifficulty(nodes[miner_index].config,
                                   *options.block_production.difficulty,
                                   stop_token);
      }
    }
    const auto is_configured_miner = [&](std::string_view node_id) {
      std::lock_guard<std::mutex> lock(configured_miner_node_ids_mutex);
      return std::find(miner_node_ids.begin(), miner_node_ids.end(), node_id) !=
             miner_node_ids.end();
    };
    if (options.block_production.enabled &&
        options.block_production.mode ==
            MiningMode::kScheduledBlockProduction &&
        !miner_node_ids.empty()) {
      block_scheduler = std::make_unique<ProbabilisticBlockScheduler>(
          miner_node_ids, options.block_production.policy,
          [&](const std::string& node_id) {
            const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
            NodeRuntime& miner = FindNodeRuntimeById(current_nodes, node_id);
            {
              auto process_guard = run_process_state.Lock();
              RequireNodeRunning(miner, process_guard,
                                 "scheduled block production");
            }
            const std::vector<std::string> hashes = GenerateBlocksSerialized(
                block_generation_mutex, driver, miner.config, 1U,
                chain_spec.default_reward_address,
                block_production_rpc_stop_source.get_token());
            RecordGeneratedBlocks(driver, miner, hashes,
                                  block_production_rpc_stop_source.get_token());
            WriteEvent(events_path, options.run_id, node_id,
                       SimulationEventKind::kScheduledBlockProduced,
                       ScheduledBlockDetail(hashes));
          },
          [&](const std::string& node_id, std::string_view error) {
            if (block_production_rpc_stop_source.stop_requested()) {
              return;
            }
            WriteEvent(events_path, options.run_id, node_id,
                       SimulationEventKind::kScheduledBlockFailed, error);
            BBP_LOG(warning) << "scheduled block production failed for "
                             << node_id << ": " << error;
          });
    }
    {
      const NodeConfigSnapshot initial_node_configs =
          node_inventory.ConfigSnapshot();
      peer_connectivity_controller =
          std::make_unique<PeerConnectivityController>(
              driver, initial_node_configs.nodes(),
              InitialPeerCountPolicies(options, nodes),
              InitialAllowedPeers(*runtime_topology, nodes),
              options.metrics_interval,
              [&](std::string_view node_id) {
                const RuntimeNodeSnapshot current_nodes =
                    node_inventory.Snapshot();
                return FindNodeRuntimeById(current_nodes, std::string(node_id))
                    .AllowsChainMetrics();
              },
              [&](std::string_view node_id, std::string_view peer_node_id,
                  PeerConnectivityAction action,
                  const PeerCountPolicy& policy) {
                boost::json::object detail;
                detail["peer_node_id"] = peer_node_id;
                SimulationEventKind event_kind;
                if (action == PeerConnectivityAction::kTopologyRestored) {
                  detail["reason"] = "restart_topology_restore";
                  event_kind = SimulationEventKind::kPeerConnected;
                } else {
                  detail["minimum_peer_count"] = policy.minimum();
                  detail["maximum_peer_count"] = policy.maximum();
                  event_kind =
                      action == PeerConnectivityAction::kConnected
                          ? SimulationEventKind::kPeerPolicyConnected
                          : SimulationEventKind::kPeerPolicyDisconnected;
                }
                WriteEvent(events_path, options.run_id, std::string(node_id),
                           event_kind, boost::json::serialize(detail));
                BBP_LOG(info) << SimulationEventKindName(event_kind) << " "
                              << node_id << " -> " << peer_node_id;
              },
              [&](std::string_view node_id, std::string_view error) {
                WriteEvent(events_path, options.run_id, std::string(node_id),
                           SimulationEventKind::kPeerPolicyEnforcementFailed,
                           error);
                BBP_LOG(warning) << "peer policy enforcement failed for "
                                 << node_id << ": " << error;
              },
              InitialAllPeerPolicyNodeIds(options, nodes));
    }
    if (active_command_queue != nullptr) {
      chain_command_executor = std::make_unique<ChainCommandExecutor>(
          driver, [&] { return node_inventory.ConfigSnapshot(); },
          [&](const ChainNodeConfig& config,
              std::stop_token command_stop_token) {
            if (!is_configured_miner(config.id)) {
              throw std::runtime_error("node is not a configured miner: " +
                                       config.id);
            }
            if (options.block_production.mode ==
                MiningMode::kScheduledBlockProduction) {
              if (!block_scheduler) {
                throw std::runtime_error(
                    "scheduled block production is not active");
              }
              block_scheduler->StopMiner(config.id);
            } else {
              const RuntimeNodeSnapshot current_nodes =
                  node_inventory.Snapshot();
              StopNativeMining(driver,
                               FindNodeRuntimeById(current_nodes, config.id),
                               run_process_state, command_stop_token);
            }
          },
          [&](BlockProductionPolicy policy) {
            if (!block_scheduler) {
              throw UnsupportedChainOperation(
                  "active mining mode",
                  "probabilistic block production policy adjustment");
            }
            block_scheduler->UpdatePolicy(policy);
          },
          [&](const ChainNodeConfig& config, MiningDifficulty difficulty,
              std::stop_token command_stop_token) {
            if (!is_configured_miner(config.id)) {
              throw std::runtime_error("node is not a configured miner: " +
                                       config.id);
            }
            driver.SetMiningDifficulty(config, difficulty, command_stop_token);
          },
          [&](const ChainNodeConfig& config, const ChainNodeConfig& peer,
              std::stop_token command_stop_token) {
            peer_connectivity_controller->ConnectPeer(config.id, peer.id,
                                                      std::chrono::seconds(10),
                                                      command_stop_token);
          },
          [&](const ChainNodeConfig& config, const ChainNodeConfig& peer,
              std::stop_token command_stop_token) {
            peer_connectivity_controller->DisconnectPeer(
                config.id, peer.id, std::chrono::seconds(10),
                command_stop_token);
          },
          [&](const ChainNodeConfig& config, PeerCountPolicy policy) {
            peer_connectivity_controller->SetPolicy(config.id, policy);
          });
      command_processor = std::make_unique<SimulationCommandProcessor>(
          *active_command_queue,
          [&](const SimulationCommand& command) {
            SimulationCommandOutcome command_outcome;
            std::stop_callback application_shutdown_callback(
                command_rpc_stop_source.get_token(), [&] {
                  if ((command.kind == SimulationCommandKind::kAddNodes ||
                       command.kind == SimulationCommandKind::kReplaceNode ||
                       command.kind == SimulationCommandKind::kRemoveNodes ||
                       command.kind == SimulationCommandKind::kAssignRole ||
                       command.kind == SimulationCommandKind::kRemoveRole) &&
                      command.operation_control) {
                    static_cast<void>(
                        command.operation_control->RequestCancellation(
                            SimulationCommandCancellationCause::
                                kApplicationShutdown));
                  }
                });
            const std::stop_token operation_stop_token =
                command.operation_control
                    ? command.operation_control->stop_source.get_token()
                    : std::stop_token{};
            CombinedStopToken combined_stop_token(
                command_rpc_stop_source.get_token(), operation_stop_token);
            const std::stop_token command_stop_token =
                combined_stop_token.get_token();
            ThrowIfStopRequested(command_stop_token);
            std::optional<std::unique_lock<std::timed_mutex>> mutation_lock;
            if (SimulationCommandRequiresNodeMutationLock(command.kind)) {
              mutation_lock.emplace(AcquireNodeMutationLock(
                  node_mutation_mutex, command_stop_token));
            }
            RequireNoLiveInstrumentationCommandConflict(*live_instrumentation,
                                                        command);
            WriteEvent(events_path, options.run_id, command.node_id,
                       SimulationEventKind::kOperatorCommandStarted,
                       SimulationCommandDetail(command));
            const bool scheduled_miner =
                block_scheduler && is_configured_miner(command.node_id);
            const auto stop_scheduled_miner = [&] {
              return scheduled_miner
                         ? block_scheduler->StopMiner(command.node_id)
                         : false;
            };
            const bool needs_runtime_snapshot =
                command.kind != SimulationCommandKind::kExportNodeReport &&
                command.kind !=
                    SimulationCommandKind::kSetBlockProductionPolicy &&
                command.kind != SimulationCommandKind::kAddNodes &&
                command.kind != SimulationCommandKind::kReplaceNode &&
                command.kind != SimulationCommandKind::kRemoveNodes &&
                command.kind != SimulationCommandKind::kAssignRole &&
                command.kind != SimulationCommandKind::kRemoveRole;
            const bool needs_direct_node =
                needs_runtime_snapshot &&
                command.kind != SimulationCommandKind::kSetPerfCounters &&
                command.kind != SimulationCommandKind::kSendWalletTransaction &&
                command.kind != SimulationCommandKind::kPartitionNodes &&
                command.kind != SimulationCommandKind::kHealPartition;
            RuntimeNodeSnapshot nodes;
            std::optional<RuntimeWalletSnapshot> command_wallet_snapshot;
            if (needs_runtime_snapshot) {
              if (command.kind ==
                  SimulationCommandKind::kSendWalletTransaction) {
                std::unique_lock<std::timed_mutex> publication_lock =
                    AcquireRuntimePublicationLock(command_stop_token);
                nodes = node_inventory.Snapshot();
                if (wallets_initialized.load(std::memory_order_acquire)) {
                  command_wallet_snapshot.emplace(
                      runtime_wallet_registry.Snapshot());
                }
              } else {
                nodes = node_inventory.Snapshot();
              }
            }
            NodeRuntime unused_node;
            NodeRuntime& node =
                needs_direct_node ? FindNodeRuntimeById(nodes, command.node_id)
                                  : unused_node;
            if (command.kind == SimulationCommandKind::kAddNodes) {
              if (!command.node_add) {
                throw std::runtime_error("node-add payload is missing");
              }
              std::lock_guard<std::mutex> topology_lock(runtime_topology_mutex);
              try {
                RuntimeNodeAddResult added = AddRuntimeNodesTransactional(
                    options, run_root, events_path, chain_spec, driver,
                    node_inventory, runtime_wallet_registry,
                    RuntimeNodeAdditionRole::kBase, block_scheduler.get(),
                    &miner_node_ids, &configured_miner_node_ids_mutex, nullptr,
                    *peer_connectivity_controller, &runtime_topology,
                    &live_topology_config, run_process_state, lifecycle_epoch,
                    *command.node_add, command.operation_control.get(),
                    command_stop_token, runtime_node_addition_dependencies);
                command_outcome.added_node_ids =
                    std::move(added.added_node_ids);
                command_outcome.inventory_generation =
                    added.inventory_generation;
                command_outcome.final_node_count = added.final_node_count;
              } catch (const SimulationNodeResourceUnavailable& error) {
                command.operation_control->RecordNodeResourceFailure(
                    error.failure());
                throw;
              }
            } else if (command.kind == SimulationCommandKind::kReplaceNode) {
              if (!command.node_replace) {
                throw std::runtime_error("node-replace payload is missing");
              }
              const auto mining_intent_deadline =
                  command.operation_control->absolute_deadline.value_or(
                      std::chrono::steady_clock::now() +
                      SimulationNodeReplaceDefaultExecutionTimeout(
                          *command.node_replace));
              std::optional<RunProcessState::NativeMiningRestartIntent>
                  mining_intent = run_process_state.TryBeginNativeMiningRestart(
                      command.node_id, mining_intent_deadline,
                      command_stop_token);
              if (!mining_intent) {
                ThrowIfStopRequested(command_stop_token);
                throw std::runtime_error(
                    "native mining RPC lock deadline expired before node "
                    "replacement: " +
                    command.node_id);
              }
              const bool resume_native_miner =
                  mining_intent->native_miner_active;
              const bool resume_scheduled_miner =
                  stop_scheduled_miner() ||
                  mining_intent->scheduled_miner_paused;
              if (resume_scheduled_miner) {
                auto process_guard = run_process_state.Lock();
                run_process_state.PauseScheduledMiner(process_guard,
                                                      command.node_id);
              }
              const auto restore_scheduled_miner = [&] {
                if (!resume_scheduled_miner) {
                  return;
                }
                if (!block_scheduler) {
                  throw std::logic_error(
                      "node-replace lost its scheduled block producer");
                }
                const RuntimeNodeSnapshot current_nodes =
                    node_inventory.Snapshot();
                NodeRuntime& current_node =
                    FindNodeRuntimeById(current_nodes, command.node_id);
                if (!NodeProcessRunning(current_node)) {
                  throw std::runtime_error(
                      "node-replace cannot restore scheduled mining on a "
                      "stopped node");
                }
                block_scheduler->StartMiner(command.node_id);
                auto process_guard = run_process_state.Lock();
                static_cast<void>(run_process_state.ResumeScheduledMiner(
                    process_guard, command.node_id));
              };
              try {
                std::lock_guard<std::mutex> topology_lock(
                    runtime_topology_mutex);
                RuntimeNodeReplaceResult replaced =
                    ReplaceRuntimeNodeTransactional(
                        options, run_root, events_path, driver, node_inventory,
                        runtime_wallet_registry, *peer_connectivity_controller,
                        *runtime_topology, live_topology_config,
                        wallet_workloads, block_generation_workloads,
                        wait_until_height_workloads, wait_for_peers_workloads,
                        transaction_tracker, run_process_state, lifecycle_epoch,
                        command.node_id, *command.node_replace,
                        resume_native_miner, chain_spec.default_reward_address,
                        command.operation_control.get(), command_stop_token,
                        runtime_node_replacement_dependencies);
                command_outcome.inventory_generation =
                    replaced.inventory_generation;
                command_outcome.final_node_count = replaced.final_node_count;
              } catch (const SimulationCommandOutcomeUnconfirmed&) {
                mining_intent.reset();
                command.operation_control->outcome_unconfirmed.store(
                    true, std::memory_order_release);
                request_simulation_stop();
                throw;
              } catch (...) {
                const std::exception_ptr failure = std::current_exception();
                mining_intent.reset();
                try {
                  restore_scheduled_miner();
                } catch (...) {
                  command.operation_control->outcome_unconfirmed.store(
                      true, std::memory_order_release);
                  request_simulation_stop();
                  throw SimulationCommandOutcomeUnconfirmed(
                      "node-replace failed: " + ExceptionMessage(failure) +
                      "; scheduled mining restoration failed: " +
                      ExceptionMessage(std::current_exception()));
                }
                std::rethrow_exception(failure);
              }
              mining_intent.reset();
              try {
                restore_scheduled_miner();
              } catch (...) {
                command.operation_control->outcome_unconfirmed.store(
                    true, std::memory_order_release);
                request_simulation_stop();
                throw SimulationCommandOutcomeUnconfirmed(
                    "node-replace committed but scheduled mining restoration "
                    "failed: " +
                    ExceptionMessage(std::current_exception()));
              }
            } else if (command.kind == SimulationCommandKind::kRemoveNodes) {
              if (!command.node_remove) {
                throw std::runtime_error("node-remove payload is missing");
              }
              std::lock_guard<std::mutex> topology_lock(runtime_topology_mutex);
              RuntimeNodeRemoveResult removed = RemoveRuntimeNodesTransactional(
                  options, events_path, driver, node_inventory,
                  runtime_wallet_registry, *peer_connectivity_controller,
                  &runtime_topology, &live_topology_config, wallet_workloads,
                  block_generation_workloads, wait_until_height_workloads,
                  wait_for_peers_workloads, transaction_tracker,
                  *command.node_remove, command.operation_control.get(),
                  command_stop_token, runtime_node_removal_dependencies);
              command_outcome.removed_node_ids =
                  std::move(removed.removed_node_ids);
              command_outcome.inventory_generation =
                  removed.inventory_generation;
              command_outcome.final_node_count = removed.final_node_count;
            } else if (command.kind == SimulationCommandKind::kAssignRole ||
                       command.kind == SimulationCommandKind::kRemoveRole) {
              if (!command.role_mutation) {
                throw std::runtime_error("role mutation payload is missing");
              }
              if (!command.operation_control) {
                throw std::runtime_error(
                    "role mutation operation control is missing");
              }
              if (!command.operation_control->absolute_deadline) {
                command.operation_control->absolute_deadline =
                    std::chrono::steady_clock::now() +
                    SimulationRoleMutationExecutionTimeout(
                        command.kind, *command.role_mutation);
              }
              const std::shared_ptr<McpLiveRoleService> role_service =
                  command_role_service.load(std::memory_order_acquire);
              if (!role_service) {
                throw std::runtime_error(
                    "authoritative role mutation service is unavailable");
              }
              try {
                command_outcome.role_mutation =
                    ExecuteAndNormalizeSimulationRoleMutation(
                        *role_service, options.run_id, command.kind,
                        *command.role_mutation, command_stop_token);
              } catch (const SimulationCancelled&) {
                if (std::chrono::steady_clock::now() >=
                    *command.operation_control->absolute_deadline) {
                  static_cast<void>(
                      command.operation_control->RequestCancellation(
                          SimulationCommandCancellationCause::kDeadline));
                }
                throw;
              }
            } else if (command.kind ==
                       SimulationCommandKind::kExportNodeReport) {
              ExportNodeReport(run_root, command);
            } else if (command.kind ==
                       SimulationCommandKind::kSetPerfCounters) {
              auto process_guard = run_process_state.Lock();
              ApplyPerfCounterCommand(command, nodes, process_guard);
            } else if (command.kind ==
                       SimulationCommandKind::kSendWalletTransaction) {
              if (!command.wallet_send) {
                throw std::runtime_error("wallet send payload is missing");
              }
              if (!wallets_initialized.load(std::memory_order_acquire)) {
                throw std::runtime_error(
                    "wallet registry is not initialized for live sends");
              }
              const SimulationWalletSend& send = *command.wallet_send;
              if (send.sender_wallet_index == 0U ||
                  send.receiver_wallet_index == 0U ||
                  send.sender_wallet_index == send.receiver_wallet_index ||
                  send.amount_satoshis == 0U || send.timeout_sec == 0U ||
                  send.amount_satoshis >
                      std::numeric_limits<std::uint64_t>::max() -
                          send.fee_satoshis) {
                throw std::runtime_error("wallet send payload is invalid");
              }
              if (!command_wallet_snapshot) {
                throw std::runtime_error(
                    "wallet registry snapshot is unavailable for live sends");
              }
              const RuntimeWalletSnapshot& wallet_snapshot =
                  *command_wallet_snapshot;
              const SimulationRegistry& registry = wallet_snapshot.registry();
              const WalletIdentity& sender = registry.WalletByIndex(
                  static_cast<std::size_t>(send.sender_wallet_index - 1U));
              const WalletIdentity& receiver = registry.WalletByIndex(
                  static_cast<std::size_t>(send.receiver_wallet_index - 1U));
              if (sender.wallet_index != send.sender_wallet_index ||
                  receiver.wallet_index != send.receiver_wallet_index) {
                throw std::runtime_error(
                    "wallet registry index does not match live send payload");
              }
              if (sender.node == 0U || sender.node > nodes.size() ||
                  receiver.node == 0U || receiver.node > nodes.size()) {
                throw std::runtime_error(
                    "wallet send references an invalid backing node");
              }
              if (sender.address.empty() || receiver.address.empty()) {
                throw std::runtime_error(
                    "wallet send requires initialized wallet addresses");
              }
              NodeRuntime& sender_node = nodes[sender.node - 1U];
              if (sender_node.config.id != command.node_id) {
                throw std::runtime_error(
                    "wallet send backing node does not match sender wallet");
              }
              ChainWalletTransactionResult transaction;
              {
                auto process_guard = run_process_state.Lock();
                RequireNodeRunning(sender_node, process_guard,
                                   "operator wallet send");
                if (!sender_node.config.wallet_enabled) {
                  throw std::runtime_error(
                      "operator wallet send requires wallet support on " +
                      sender_node.config.id);
                }
              }
              TransactionObservationTracker::Reservation
                  observation_reservation = transaction_tracker.Reserve(nodes);
              transaction = driver.SendWalletTransaction(
                  sender_node.config,
                  ToChainWalletMode(registry.wallet_initialization()),
                  receiver.address, send.amount_satoshis, send.fee_satoshis,
                  std::chrono::seconds(send.timeout_sec), command_stop_token);
              const std::string& txid = RequireSingleWalletTransactionId(
                  transaction, "operator wallet send");
              WriteEvent(
                  events_path, options.run_id, sender_node.config.id,
                  SimulationEventKind::kWalletTransactionSubmitted,
                  OperatorWalletTransactionDetail(
                      send, sender, receiver, registry.wallet_initialization(),
                      transaction, command.sequence));
              transaction_tracker.TrackAndWaitForVisibility(
                  std::move(observation_reservation), options, events_path,
                  driver, nodes,
                  TrackedTransaction{
                      .txid = txid,
                      .submission_kind = "operator_wallet_send",
                      .workload_id = {},
                      .workload_index = 0U,
                      .workload_count = 0U,
                      .transaction_index = command.sequence,
                      .transaction_count = std::nullopt,
                      .transaction_rate = std::nullopt,
                      .txid_index = 1U,
                      .submission_node = sender.node,
                      .load_confirmation = nullptr,
                  },
                  std::chrono::seconds(send.timeout_sec), command_stop_token);
            } else if (command.kind ==
                       SimulationCommandKind::kSetResourceLimits) {
              if (!command.resource_limit_patch) {
                throw std::runtime_error("resource limit patch is missing");
              }
              ApplyResourceLimitUpdate(
                  options, events_path, node, *command.resource_limit_patch,
                  node_resource_state_mutex, {}, {}, std::nullopt, std::nullopt,
                  std::nullopt, command.sequence, true);
            } else if (command.kind == SimulationCommandKind::kKillNode) {
              bool was_paused = false;
              {
                auto process_guard = run_process_state.Lock();
                was_paused = run_process_state.IsPausedScheduledMiner(
                    process_guard, command.node_id);
              }
              const bool resume_on_failure =
                  stop_scheduled_miner() || was_paused;
              pid_t pid = -1;
              try {
                {
                  auto process_guard = run_process_state.Lock();
                  if (node.Lifecycle() != NodeRuntimeLifecycle::kRunning) {
                    throw std::runtime_error(
                        "node kill conflicts with an active lifecycle "
                        "operation: " +
                        command.node_id + " (state=" +
                        std::string(
                            NodeRuntimeLifecycleName(node.Lifecycle())) +
                        ")");
                  }
                  if (!node.process.running()) {
                    throw std::runtime_error("node process is not running: " +
                                             command.node_id);
                  }
                  pid = node.process.pid();
                  ResetNodePerfCounters(node, process_guard);
                  node.SetLifecycle(NodeRuntimeLifecycle::kKilling);
                }
                WriteNodeStateEvent(events_path, options.run_id, node,
                                    NodeRuntimeLifecycle::kKilling);
                if (node.cgroup && node.cgroup->Frozen()) {
                  SetNodeFrozen(options, events_path, node, false,
                                command_stop_token);
                }
                WriteEvent(events_path, options.run_id, command.node_id,
                           SimulationEventKind::kProcessKillRequested,
                           "pid=" + std::to_string(pid));
                try {
                  static_cast<void>(RequestNodeKill(node));
                  const auto kill_deadline = std::chrono::steady_clock::now() +
                                             std::chrono::seconds(5);
                  if (!WaitForNodeProcessExitUntil(node, kill_deadline,
                                                   command_stop_token)) {
                    throw std::runtime_error("node process survived SIGKILL: " +
                                             command.node_id);
                  }
                } catch (const SimulationCancelled&) {
                  auto reconciliation_deadline =
                      std::chrono::steady_clock::now() +
                      kSimulationCommandCancellationReconciliation;
                  if (command.operation_control &&
                      command.operation_control->absolute_deadline) {
                    reconciliation_deadline =
                        std::min(reconciliation_deadline,
                                 *command.operation_control->absolute_deadline);
                  }
                  if (!WaitForNodeProcessExitUntil(node,
                                                   reconciliation_deadline)) {
                    if (command.operation_control) {
                      command.operation_control->outcome_unconfirmed.store(
                          true, std::memory_order_release);
                    }
                    throw;
                  }
                  throw;
                } catch (...) {
                  bool restored_running = false;
                  bool reconciled_killed = false;
                  {
                    auto process_guard = run_process_state.Lock();
                    if (node.process.running()) {
                      AttachNodePerfCounters(node, process_guard);
                      node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
                      restored_running = true;
                    } else if (node.Lifecycle() ==
                               NodeRuntimeLifecycle::kKilling) {
                      ResetNodePerfCounters(node, process_guard);
                      node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
                      reconciled_killed = true;
                    }
                  }
                  if (restored_running) {
                    WriteNodeStateEvent(events_path, options.run_id, node,
                                        NodeRuntimeLifecycle::kRunning);
                  } else if (reconciled_killed) {
                    WriteEvent(events_path, options.run_id, command.node_id,
                               SimulationEventKind::kProcessKilled,
                               "pid=" + std::to_string(pid));
                    WriteNodeStateEvent(events_path, options.run_id, node,
                                        NodeRuntimeLifecycle::kKilled);
                  }
                  throw;
                }
                WriteEvent(events_path, options.run_id, command.node_id,
                           SimulationEventKind::kProcessKilled,
                           "pid=" + std::to_string(pid));
                {
                  auto process_guard = run_process_state.Lock();
                  node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
                }
                WriteNodeStateEvent(events_path, options.run_id, node,
                                    NodeRuntimeLifecycle::kKilled);
              } catch (...) {
                bool restored_running = false;
                bool reconciled_killed = false;
                const bool outcome_unconfirmed =
                    command.operation_control &&
                    command.operation_control->outcome_unconfirmed.load(
                        std::memory_order_acquire);
                {
                  auto process_guard = run_process_state.Lock();
                  if (node.Lifecycle() == NodeRuntimeLifecycle::kKilling &&
                      node.process.running() && !outcome_unconfirmed) {
                    AttachNodePerfCounters(node, process_guard);
                    node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
                    restored_running = true;
                  } else if (node.Lifecycle() ==
                                 NodeRuntimeLifecycle::kKilling &&
                             !node.process.running()) {
                    if (command.operation_control) {
                      command.operation_control->outcome_unconfirmed.store(
                          false, std::memory_order_release);
                    }
                    ResetNodePerfCounters(node, process_guard);
                    node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
                    reconciled_killed = true;
                  }
                }
                if (restored_running) {
                  WriteNodeStateEvent(events_path, options.run_id, node,
                                      NodeRuntimeLifecycle::kRunning);
                } else if (reconciled_killed) {
                  WriteEvent(events_path, options.run_id, command.node_id,
                             SimulationEventKind::kProcessKilled,
                             "pid=" + std::to_string(pid));
                  WriteNodeStateEvent(events_path, options.run_id, node,
                                      NodeRuntimeLifecycle::kKilled);
                }
                const bool node_running = NodeProcessRunning(node);
                if (resume_on_failure && node_running && !outcome_unconfirmed) {
                  block_scheduler->StartMiner(command.node_id);
                  auto process_guard = run_process_state.Lock();
                  static_cast<void>(run_process_state.ResumeScheduledMiner(
                      process_guard, command.node_id));
                } else if (!node_running) {
                  auto process_guard = run_process_state.Lock();
                  static_cast<void>(run_process_state.ResumeScheduledMiner(
                      process_guard, command.node_id));
                  run_process_state.RemoveActiveNativeMiner(process_guard,
                                                            command.node_id);
                }
                throw;
              }
              {
                auto process_guard = run_process_state.Lock();
                static_cast<void>(run_process_state.ResumeScheduledMiner(
                    process_guard, command.node_id));
                run_process_state.RemoveActiveNativeMiner(process_guard,
                                                          command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kStopNode) {
              bool was_paused = false;
              {
                auto process_guard = run_process_state.Lock();
                was_paused = run_process_state.IsPausedScheduledMiner(
                    process_guard, command.node_id);
              }
              const bool resume_on_failure =
                  stop_scheduled_miner() || was_paused;
              try {
                StopNodeProcess(options, events_path, driver, node,
                                command_stop_token, false,
                                command.operation_control.get());
              } catch (...) {
                const bool node_running = NodeProcessRunning(node);
                const bool outcome_unconfirmed =
                    command.operation_control &&
                    command.operation_control->outcome_unconfirmed.load(
                        std::memory_order_acquire);
                if (resume_on_failure && node_running && !outcome_unconfirmed) {
                  block_scheduler->StartMiner(command.node_id);
                  auto process_guard = run_process_state.Lock();
                  static_cast<void>(run_process_state.ResumeScheduledMiner(
                      process_guard, command.node_id));
                } else if (!node_running) {
                  auto process_guard = run_process_state.Lock();
                  static_cast<void>(run_process_state.ResumeScheduledMiner(
                      process_guard, command.node_id));
                  run_process_state.RemoveActiveNativeMiner(process_guard,
                                                            command.node_id);
                }
                throw;
              }
              {
                auto process_guard = run_process_state.Lock();
                static_cast<void>(run_process_state.ResumeScheduledMiner(
                    process_guard, command.node_id));
                run_process_state.RemoveActiveNativeMiner(process_guard,
                                                          command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kRestartNode) {
              bool was_paused = false;
              bool resume_native_miner = false;
              bool native_mining_rpc_attempted = false;
              NodeRestartAdmission restart_admission;
              SimulationCommandControl local_restart_control;
              SimulationCommandControl* restart_control =
                  command.operation_control ? command.operation_control.get()
                                            : &local_restart_control;
              const auto resume_intent_deadline =
                  restart_control->absolute_deadline.value_or(
                      std::chrono::steady_clock::now() +
                      std::chrono::seconds(options.ready_timeout_sec));
              std::optional<RunProcessState::NativeMiningRestartIntent>
                  restart_intent =
                      run_process_state.TryBeginNativeMiningRestart(
                          command.node_id, resume_intent_deadline,
                          command_stop_token);
              if (!restart_intent) {
                ThrowIfStopRequested(command_stop_token);
                throw std::runtime_error(
                    "native mining RPC lock deadline expired before node "
                    "restart: " +
                    command.node_id);
              }
              was_paused = restart_intent->scheduled_miner_paused;
              resume_native_miner = restart_intent->native_miner_active;
              const bool resume_scheduled_miner =
                  stop_scheduled_miner() || was_paused;
              if (resume_scheduled_miner) {
                auto process_guard = run_process_state.Lock();
                run_process_state.PauseScheduledMiner(process_guard,
                                                      command.node_id);
              }
              try {
                if (!RestartNode(options, events_path, driver,
                                 *peer_connectivity_controller, node,
                                 lifecycle_epoch, command_stop_token,
                                 "requested", restart_control,
                                 &restart_admission)) {
                  throw std::runtime_error(
                      "operator restart reached node stop_time before "
                      "completion: " +
                      node.config.id);
                }
                restart_intent.reset();
                if (resume_native_miner) {
                  if (!StartNativeMiningForCurrentProcess(
                          driver, node, run_process_state,
                          chain_spec.default_reward_address, command_stop_token,
                          "operator native mining restart",
                          restart_control->absolute_deadline,
                          &native_mining_rpc_attempted)) {
                    throw std::runtime_error(
                        "node process changed during native mining "
                        "restart: " +
                        node.config.id);
                  }
                }
              } catch (...) {
                restart_intent.reset();
                bool resume_miner_after_failure = true;
                auto reconciliation_deadline =
                    std::chrono::steady_clock::now() +
                    kSimulationCommandCancellationReconciliation;
                if (restart_control->absolute_deadline) {
                  reconciliation_deadline =
                      std::min(reconciliation_deadline,
                               *restart_control->absolute_deadline);
                }
                if (command_stop_token.stop_requested() &&
                    restart_admission.admitted) {
                  const SimulationNodeRestartReconciliation reconciliation =
                      ReconcileCancelledSimulationNodeRestart(
                          restart_admission.process,
                          restart_control->restart_phase.load(
                              std::memory_order_acquire),
                          reconciliation_deadline,
                          [&] {
                            auto process_guard = run_process_state.Lock();
                            return SimulationNodeProcessObservation{
                                .running = node.process.running(),
                                .pid = node.process.pid(),
                                .restart_count = node.RestartCount(),
                            };
                          },
                          [&](const SimulationNodeProcessObservation&
                                  expected) {
                            bool requested = false;
                            try {
                              {
                                auto process_guard = run_process_state.Lock();
                                if (node.process.running() &&
                                    node.process.pid() == expected.pid &&
                                    node.RestartCount() ==
                                        expected.restart_count) {
                                  requested = node.process.RequestKill();
                                }
                              }
                            } catch (const std::exception& error) {
                              BBP_LOG(error)
                                  << "failed to stop unready replacement "
                                  << expected.pid << " for " << command.node_id
                                  << ": " << error.what();
                              return false;
                            }
                            if (requested) {
                              try {
                                WriteEvent(
                                    events_path, options.run_id,
                                    command.node_id,
                                    SimulationEventKind::kProcessKillRequested,
                                    "restart cancellation "
                                    "reconciliation pid=" +
                                        std::to_string(expected.pid));
                              } catch (const std::exception& error) {
                                BBP_LOG(error)
                                    << "failed to record unready replacement "
                                       "stop for "
                                    << command.node_id << ": " << error.what();
                              }
                            }
                            return requested;
                          });
                  const bool unchanged =
                      reconciliation ==
                      SimulationNodeRestartReconciliation::kUnchanged;
                  const bool stopped =
                      reconciliation ==
                      SimulationNodeRestartReconciliation::kStopped;
                  const bool replacement_ready =
                      reconciliation ==
                      SimulationNodeRestartReconciliation::kReplacementReady;
                  NodeRuntimeLifecycle observed_lifecycle;
                  {
                    auto process_guard = run_process_state.Lock();
                    observed_lifecycle = node.Lifecycle();
                  }
                  const NodeRuntimeLifecycle reconciled_state =
                      ReconciledSimulationNodeRestartLifecycle(
                          restart_admission.lifecycle, observed_lifecycle,
                          reconciliation);
                  bool state_changed = false;
                  {
                    auto process_guard = run_process_state.Lock();
                    if (reconciled_state == NodeRuntimeLifecycle::kRunning) {
                      AttachNodePerfCounters(node, process_guard);
                    } else {
                      ResetNodePerfCounters(node, process_guard);
                    }
                    if (node.Lifecycle() != reconciled_state) {
                      node.SetLifecycle(reconciled_state);
                      state_changed = true;
                    }
                  }
                  if (!unchanged && !stopped && !replacement_ready) {
                    resume_miner_after_failure = false;
                    restart_control->outcome_unconfirmed.store(
                        true, std::memory_order_release);
                  }
                  if (state_changed) {
                    WriteNodeStateEvent(events_path, options.run_id, node,
                                        reconciled_state);
                  }
                }
                if (resume_native_miner && restart_admission.admitted) {
                  auto mining_rpc_guard =
                      run_process_state.TryLockNativeMiningRpcUntil(
                          reconciliation_deadline);
                  bool original_running_generation_unchanged = false;
                  bool replacement_running = false;
                  if (mining_rpc_guard) {
                    auto process_guard = run_process_state.Lock();
                    original_running_generation_unchanged =
                        restart_admission.process.running &&
                        node.process.running() &&
                        node.process.pid() == restart_admission.process.pid &&
                        node.RestartCount() ==
                            restart_admission.process.restart_count;
                    replacement_running =
                        !original_running_generation_unchanged &&
                        node.process.running();
                  }

                  bool replacement_mining_confirmed_inactive =
                      mining_rpc_guard &&
                      (!replacement_running || !native_mining_rpc_attempted);
                  std::string compensation_error;
                  if (mining_rpc_guard && replacement_running &&
                      native_mining_rpc_attempted) {
                    replacement_mining_confirmed_inactive =
                        StopNativeMiningBeforeDeadline(driver, node,
                                                       reconciliation_deadline,
                                                       &compensation_error);
                  } else if (!mining_rpc_guard) {
                    compensation_error =
                        "native mining RPC lock deadline expired";
                  }

                  if (mining_rpc_guard) {
                    auto process_guard = run_process_state.Lock();
                    run_process_state
                        .ReconcileActiveNativeMinerAfterRestartFailure(
                            *mining_rpc_guard, process_guard, command.node_id,
                            original_running_generation_unchanged,
                            replacement_mining_confirmed_inactive);
                  }
                  if (!original_running_generation_unchanged &&
                      !replacement_mining_confirmed_inactive) {
                    resume_miner_after_failure = false;
                    restart_control->outcome_unconfirmed.store(
                        true, std::memory_order_release);
                    BBP_LOG(warning)
                        << "native mining state remains active after failed "
                           "restart reconciliation for "
                        << command.node_id << ": " << compensation_error;
                  }
                }
                if (!command.operation_control &&
                    restart_control->outcome_unconfirmed.load(
                        std::memory_order_acquire)) {
                  request_simulation_stop();
                  throw SimulationCommandOutcomeUnconfirmed(
                      ExceptionMessage(std::current_exception()));
                }
                if (resume_scheduled_miner && resume_miner_after_failure &&
                    NodeProcessRunning(node)) {
                  block_scheduler->StartMiner(command.node_id);
                  auto process_guard = run_process_state.Lock();
                  static_cast<void>(run_process_state.ResumeScheduledMiner(
                      process_guard, command.node_id));
                }
                throw;
              }
              if (resume_scheduled_miner) {
                block_scheduler->StartMiner(command.node_id);
                auto process_guard = run_process_state.Lock();
                static_cast<void>(run_process_state.ResumeScheduledMiner(
                    process_guard, command.node_id));
              }
            } else if (command.kind == SimulationCommandKind::kFreezeNode) {
              const bool resume_on_thaw = stop_scheduled_miner();
              try {
                RequireNodeRunning(node, "operator freeze");
                SetNodeFrozen(options, events_path, node, true,
                              command_stop_token);
              } catch (...) {
                if (resume_on_thaw) {
                  block_scheduler->StartMiner(command.node_id);
                }
                throw;
              }
              if (resume_on_thaw) {
                auto process_guard = run_process_state.Lock();
                run_process_state.PauseScheduledMiner(process_guard,
                                                      command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kThawNode) {
              SetNodeFrozen(options, events_path, node, false,
                            command_stop_token);
              bool resume_scheduled_miner = false;
              {
                auto process_guard = run_process_state.Lock();
                resume_scheduled_miner = run_process_state.ResumeScheduledMiner(
                    process_guard, command.node_id);
              }
              if (resume_scheduled_miner) {
                block_scheduler->StartMiner(command.node_id);
              }
            } else if (command.kind == SimulationCommandKind::kGenerateBlocks) {
              if (!command.block_count || *command.block_count == 0U) {
                throw std::runtime_error(
                    "generate-blocks command requires a positive count");
              }
              RequireNodeRunning(node, "operator block generation");
              const std::uint64_t start_height =
                  driver.ReadMetrics(node.config, command_stop_token).height;
              const std::vector<std::string> hashes = GenerateBlocksSerialized(
                  block_generation_mutex, driver, node.config,
                  *command.block_count, chain_spec.default_reward_address,
                  command_stop_token);
              RecordGeneratedBlocks(driver, node, hashes, command_stop_token);
              if (start_height >
                  std::numeric_limits<std::uint64_t>::max() - hashes.size()) {
                throw std::runtime_error(
                    "generated block target height overflows uint64");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              const std::uint32_t one_based_node =
                  static_cast<std::uint32_t>(
                      std::distance(nodes.begin(), node_iter)) +
                  1U;
              WriteEvent(
                  events_path, options.run_id, command.node_id,
                  SimulationEventKind::kGeneratedBlocks,
                  GeneratedBlocksDetail(
                      0U, 0U, one_based_node, start_height,
                      start_height + static_cast<std::uint64_t>(hashes.size()),
                      hashes, chain_spec.default_reward_address,
                      command.sequence));
            } else if (command.kind ==
                       SimulationCommandKind::kSetNetworkCondition) {
              if (!command.network_condition) {
                throw std::runtime_error(
                    "set-network-condition command requires a condition");
              }
              QdiscInfo qdisc;
              NodeVethConfig updated_network;
              {
                std::lock_guard<std::mutex> lock(node_network_state_mutex);
                qdisc = ReplaceNodeNetworkConditionTransactional(
                    &node, *command.network_condition, command_stop_token);
                updated_network = *node.network;
              }
              WriteEvent(events_path, options.run_id, command.node_id,
                         SimulationEventKind::kNetworkConditionUpdated,
                         NetworkConditionVerificationDetail(
                             updated_network, qdisc, 0U, 0U, command.sequence));
            } else if (command.kind ==
                           SimulationCommandKind::kBlockNetworkFlow ||
                       command.kind ==
                           SimulationCommandKind::kUnblockNetworkFlow) {
              if (!command.network_flow) {
                throw std::runtime_error(
                    "network flow command requires a typed flow");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              if (node_iter == nodes.end()) {
                throw std::runtime_error("unknown network flow node: " +
                                         command.node_id);
              }
              const std::size_t zero_based_node = static_cast<std::size_t>(
                  std::distance(nodes.begin(), node_iter));
              if (zero_based_node > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error(
                    "network flow node index exceeds uint32");
              }
              NetworkBlockRule rule;
              NetworkBlockMutationResult result;
              {
                std::lock_guard<std::mutex> lock(node_network_state_mutex);
                if (command.network_flow->dst_address.empty()) {
                  if (command.kind !=
                          SimulationCommandKind::kUnblockNetworkFlow ||
                      command.network_flow->handle == 0U) {
                    throw std::runtime_error(
                        "handle-only network flow command must be unblock");
                  }
                  const std::optional<NetworkBlockRule> existing =
                      NetworkBlockRuleForHandle(node,
                                                command.network_flow->handle);
                  if (!existing) {
                    throw std::runtime_error(
                        "active network block rule handle was not found: " +
                        std::to_string(command.network_flow->handle));
                  }
                  rule = *existing;
                } else {
                  rule.src_address = command.network_flow->src_address;
                  rule.src_port = command.network_flow->src_port;
                  rule.dst_address = command.network_flow->dst_address;
                  rule.dst_port = command.network_flow->dst_port;
                  rule.handle = command.network_flow->handle;
                }
                rule.node_index = static_cast<std::uint32_t>(zero_based_node);
                if (rule.handle == 0U) {
                  rule.handle = StableRuleHandle(rule);
                }
                result = MutateNetworkBlockRuleTransactional(
                    node, rule,
                    command.kind == SimulationCommandKind::kUnblockNetworkFlow,
                    command_stop_token);
              }
              WriteEvent(
                  events_path, options.run_id, command.node_id,
                  command.kind == SimulationCommandKind::kUnblockNetworkFlow
                      ? SimulationEventKind::kNetworkBlockRemoved
                      : SimulationEventKind::kNetworkBlockApplied,
                  NetworkBlockRuleDetail(node, rule, result.existed_before,
                                         result.present_after, 0U, 0U,
                                         command.sequence));
            } else if (command.kind == SimulationCommandKind::kPartitionNodes ||
                       command.kind == SimulationCommandKind::kHealPartition) {
              if (!command.partition) {
                throw std::runtime_error(
                    "partition command requires a typed partition target");
              }
              const NetworkPartitionRule partition =
                  RuntimePartitionRule(*command.partition, nodes);
              ApplyRuntimeNetworkPartition(
                  options, events_path, nodes, node_network_state_mutex,
                  partition,
                  command.kind == SimulationCommandKind::kHealPartition, 0U, 0U,
                  command_stop_token, command.sequence);
            } else if (command.kind ==
                           SimulationCommandKind::kSetResourceProfile ||
                       command.kind ==
                           SimulationCommandKind::kSetNetworkProfile) {
              if (!command.profile || command.profile->empty()) {
                throw std::runtime_error(
                    "profile command requires a profile name");
              }
              const auto node_iter =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == command.node_id;
                               });
              const std::uint32_t one_based_node =
                  static_cast<std::uint32_t>(
                      std::distance(nodes.begin(), node_iter)) +
                  1U;
              const ProfileSwitchWorkload workload{
                  .nodes = {one_based_node},
                  .node_ids = {command.node_id},
                  .profile = *command.profile,
              };
              if (command.kind == SimulationCommandKind::kSetResourceProfile) {
                if (!options.resource_profiles.contains(*command.profile)) {
                  throw std::runtime_error("unknown resource profile: " +
                                           *command.profile);
                }
                ApplyResourceProfileSwitch(options, events_path, nodes,
                                           node_resource_state_mutex, workload,
                                           0U, 0U, command_stop_token);
              } else {
                if (!options.network_profiles.contains(*command.profile)) {
                  throw std::runtime_error("unknown network profile: " +
                                           *command.profile);
                }
                ApplyNetworkProfileSwitch(options, events_path, nodes,
                                          node_network_state_mutex, workload,
                                          0U, 0U, command_stop_token);
              }
            } else {
              try {
                chain_command_executor->Execute(command, command_stop_token);
              } catch (const PeerMutationOutcomeUnconfirmed&) {
                if (command.operation_control) {
                  command.operation_control->outcome_unconfirmed.store(
                      true, std::memory_order_release);
                } else {
                  request_simulation_stop();
                }
                throw;
              }
            }
            try {
              WriteEvent(
                  events_path, options.run_id, command.node_id,
                  SimulationEventKind::kOperatorCommandCompleted,
                  SimulationCommandDetail(command, {}, &command_outcome));
            } catch (const std::exception& error) {
              if ((command.kind == SimulationCommandKind::kAddNodes &&
                   !command_outcome.added_node_ids.empty()) ||
                  (command.kind == SimulationCommandKind::kReplaceNode &&
                   command_outcome.inventory_generation.has_value()) ||
                  (command.kind == SimulationCommandKind::kRemoveNodes &&
                   !command_outcome.removed_node_ids.empty()) ||
                  ((command.kind == SimulationCommandKind::kAssignRole ||
                    command.kind == SimulationCommandKind::kRemoveRole) &&
                   command_outcome.role_mutation.has_value())) {
                throw SimulationCommandOutcomeUnconfirmed(
                    "runtime mutation published but completion evidence "
                    "failed: " +
                    std::string(error.what()));
              }
              throw;
            }
            BBP_LOG(info) << "command #" << command.sequence << " "
                          << SimulationCommandKindName(command.kind) << " for "
                          << command.node_id << " completed";
            return command_outcome;
          },
          [&](const SimulationCommand& command, std::string_view error) {
            WriteEvent(events_path, options.run_id, command.node_id,
                       SimulationEventKind::kOperatorCommandFailed,
                       SimulationCommandDetail(command, error));
            BBP_LOG(warning)
                << "command #" << command.sequence << " "
                << SimulationCommandKindName(command.kind) << " for "
                << command.node_id << " failed: " << error;
          },
          [&](const SimulationCommand& command,
              const SimulationCommandOutcome& outcome) {
            SimulationCommandOutcome authoritative_outcome = outcome;
            if (outcome.state ==
                SimulationCommandOutcomeState::kOutcomeUnconfirmed) {
              BBP_LOG(error)
                  << "command #" << command.sequence << " "
                  << SimulationCommandKindName(command.kind) << " for "
                  << command.node_id << " has an unconfirmed outcome: "
                  << outcome.error.value_or("no diagnostic");
              if (command.operation_control) {
                command.operation_control->outcome_unconfirmed.store(
                    true, std::memory_order_release);
              }
              request_simulation_stop();
            }
            const RuntimeNodeSnapshot nodes = node_inventory.Snapshot();
            const auto node = std::find_if(
                nodes.begin(), nodes.end(), [&](const NodeRuntime& candidate) {
                  return candidate.config.id == command.node_id;
                });
            if (node != nodes.end()) {
              auto process_guard = run_process_state.Lock();
              std::string lifecycle(
                  NodeRuntimeLifecycleName(node->Lifecycle()));
              std::transform(
                  lifecycle.begin(), lifecycle.end(), lifecycle.begin(),
                  [](char character) {
                    return character >= 'A' && character <= 'Z'
                               ? static_cast<char>(character - 'A' + 'a')
                               : character;
                  });
              authoritative_outcome.node_lifecycle = std::move(lifecycle);
            }
            std::optional<std::string> scheduled_error;
            if (authoritative_outcome.state !=
                SimulationCommandOutcomeState::kSucceeded) {
              scheduled_error = authoritative_outcome.error.value_or(
                  "scheduled command did not succeed");
            }
            record_scheduled_command_outcome(
                command, scheduled_error
                             ? std::optional<std::string_view>(*scheduled_error)
                             : std::nullopt);
            mcp_application.RecordCommandOutcome(command,
                                                 authoritative_outcome);
          });
    }
    if (block_scheduler) {
      for (const NodeRuntime& node : nodes) {
        if (is_configured_miner(node.config.id) && !node.AllowsChainMetrics()) {
          block_scheduler->StopMiner(node.config.id);
        }
      }
    }
    lifecycle_supervisor.emplace([&](std::stop_token supervisor_stop_token) {
      std::stop_source operation_stop_source;
      std::stop_callback stop_on_supervisor(
          supervisor_stop_token,
          [&operation_stop_source] { operation_stop_source.request_stop(); });
      std::stop_callback stop_on_simulation(
          stop_token,
          [&operation_stop_source] { operation_stop_source.request_stop(); });
      const std::stop_token operation_stop_token =
          operation_stop_source.get_token();
      try {
        std::condition_variable_any wakeup;
        std::mutex wakeup_mutex;
        while (!operation_stop_token.stop_requested()) {
          {
            auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                         operation_stop_token);
            const RuntimeNodeSnapshot nodes = node_inventory.Snapshot();
            for (std::size_t index = 0; index < nodes.size(); ++index) {
              if (operation_stop_token.stop_requested()) {
                return;
              }
              NodeRuntime& node = nodes[index];
              const auto now = std::chrono::steady_clock::now();
              const bool stop_due =
                  node.lifecycle_policy.stop_time &&
                  now >= SteadyDeadline(lifecycle_epoch,
                                        options.time_scale.WallDuration(
                                            *node.lifecycle_policy.stop_time));
              if (stop_due && !node.DeclarativeStopApplied()) {
                if (block_scheduler && is_configured_miner(node.config.id)) {
                  block_scheduler->StopMiner(node.config.id);
                }
                if (node.DeclarativeStopApplied()) {
                  continue;
                }
                node.MarkDeclarativeStopApplied();
                WriteEvent(
                    events_path, options.run_id, node.config.id,
                    SimulationEventKind::kNodeStopDeadlineReached,
                    NodeLifecycleDeadlineDetail(
                        node, options.time_scale, lifecycle_epoch,
                        *node.lifecycle_policy.stop_time, "declarative_stop"));
                if (NodeProcessRunning(node)) {
                  StopNodeProcess(options, events_path, driver, node,
                                  operation_stop_token);
                } else {
                  {
                    auto process_guard = run_process_state.Lock();
                    ResetNodePerfCounters(node, process_guard);
                    node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
                  }
                  WriteNodeStateEvent(events_path, options.run_id, node,
                                      NodeRuntimeLifecycle::kStopped);
                }
                {
                  auto process_guard = run_process_state.Lock();
                  run_process_state.RemoveActiveNativeMiner(process_guard,
                                                            node.config.id);
                }
                continue;
              }

              bool node_started = false;
              if (node.lifecycle_policy.start_time &&
                  node.Lifecycle() == NodeRuntimeLifecycle::kCgroupReady &&
                  now >=
                      SteadyDeadline(lifecycle_epoch,
                                     options.time_scale.WallDuration(
                                         *node.lifecycle_policy.start_time))) {
                WriteEvent(events_path, options.run_id, node.config.id,
                           SimulationEventKind::kNodeStartDeadlineReached,
                           NodeLifecycleDeadlineDetail(
                               node, options.time_scale, lifecycle_epoch,
                               *node.lifecycle_policy.start_time,
                               "declarative_start"));
                node_started = StartPreparedNode(
                    options, events_path, driver, node, "declarative_start",
                    lifecycle_epoch, operation_stop_token);
                if (node_started) {
                  ConnectAvailableStartupPeers(options, events_path, driver,
                                               nodes, index, lifecycle_epoch,
                                               operation_stop_token);
                  if (is_configured_miner(node.config.id) &&
                      options.block_production.enabled &&
                      options.block_production.difficulty) {
                    RequireNodeRunning(node, "declarative mining difficulty");
                    driver.SetMiningDifficulty(
                        node.config, *options.block_production.difficulty,
                        operation_stop_token);
                  }
                  PublishOperatorConnectionCommand(
                      options, run_root, events_path, driver, nodes,
                      &operator_connection_resolved);
                }
              }
              if (node_started && block_scheduler &&
                  is_configured_miner(node.config.id)) {
                block_scheduler->StartMiner(node.config.id);
              } else if (node_started && options.block_production.enabled &&
                         options.block_production.mode ==
                             MiningMode::kNativeMining &&
                         is_configured_miner(node.config.id)) {
                static_cast<void>(StartNativeMiningForCurrentProcess(
                    driver, node, run_process_state,
                    chain_spec.default_reward_address, operation_stop_token,
                    "declarative native mining start"));
              }

              bool node_restarted = false;
              std::optional<int> exited_wait_status;
              std::string process_exit_detail;
              {
                auto process_guard = run_process_state.Lock();
                if (node.Lifecycle() != NodeRuntimeLifecycle::kRunning ||
                    node.process.running()) {
                  continue;
                }
                exited_wait_status = node.process.exit_status();
                if (!exited_wait_status) {
                  throw std::runtime_error(
                      "node exited without a wait status: " + node.config.id);
                }
                ResetNodePerfCounters(node, process_guard);
                process_exit_detail =
                    ProcessExitDetail(node.process, process_guard);
                node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
              }
              WriteEvent(events_path, options.run_id, node.config.id,
                         SimulationEventKind::kProcessExited,
                         process_exit_detail);
              WriteEvent(
                  events_path, options.run_id, node.config.id,
                  SimulationEventKind::kState,
                  NodeRuntimeLifecycleName(NodeRuntimeLifecycle::kFailed));
              const bool restart = NodeRestartPolicyAllowsRestart(
                  node.lifecycle_policy.restart_policy, *exited_wait_status);
              WriteEvent(events_path, options.run_id, node.config.id,
                         SimulationEventKind::kRestartPolicyApplied,
                         RestartPolicyAppliedDetail(node, *exited_wait_status,
                                                    restart));
              if (!restart) {
                throw std::runtime_error(
                    "node process exited and restart policy did not restart "
                    "it: " +
                    node.config.id);
              }
              node_restarted = RestartNode(
                  options, events_path, driver, *peer_connectivity_controller,
                  node, lifecycle_epoch, operation_stop_token,
                  "restart_policy");
              if (node_restarted && is_configured_miner(node.config.id) &&
                  options.block_production.enabled &&
                  options.block_production.difficulty) {
                RequireNodeRunning(node, "restart mining difficulty restore");
                driver.SetMiningDifficulty(node.config,
                                           *options.block_production.difficulty,
                                           operation_stop_token);
              }
              if (node_restarted && block_scheduler &&
                  is_configured_miner(node.config.id)) {
                block_scheduler->StartMiner(node.config.id);
              } else if (node_restarted && options.block_production.enabled &&
                         options.block_production.mode ==
                             MiningMode::kNativeMining &&
                         is_configured_miner(node.config.id)) {
                static_cast<void>(StartNativeMiningForCurrentProcess(
                    driver, node, run_process_state,
                    chain_spec.default_reward_address, operation_stop_token,
                    "restart-policy native mining restore"));
              }
            }
          }
          std::unique_lock<std::mutex> wait_lock(wakeup_mutex);
          wakeup.wait_for(wait_lock, operation_stop_token,
                          std::chrono::milliseconds(20), [] { return false; });
        }
      } catch (const SimulationCancelled&) {
        if (operation_stop_token.stop_requested()) {
          return;
        }
        throw;
      } catch (...) {
        {
          std::lock_guard<std::mutex> lock(lifecycle_failure_mutex);
          if (!lifecycle_failure) {
            lifecycle_failure = std::current_exception();
          }
        }
        mcp_application.MarkRunStopping();
        request_simulation_stop();
      }
    });
    log_collector = std::make_unique<NodeLogCollector>(
        driver,
        [&] {
          std::unique_lock<std::timed_mutex> publication_lock =
              AcquireRuntimePublicationLock({});
          return node_inventory.ConfigSnapshot();
        },
        options.metrics_interval, kMaxLogTailBytes,
        [&](const ChainNodeConfig& config, ChainLogSource source,
            const LogTailChunk& chunk) {
          std::unique_lock<std::timed_mutex> publication_lock =
              AcquireRuntimePublicationLock({});
          WriteLogTailChunkEvent(events_path, options, config, source, chunk);
        });
    log_collector->Start();
    metrics_collector = std::make_unique<PeriodicMetricsCollector>(
        options.metrics_sample_count, options.metrics_interval,
        [&](std::uint32_t sample) {
          RuntimeNodeSnapshot nodes;
          std::optional<RuntimeWalletSnapshot> wallet_snapshot;
          {
            std::unique_lock<std::timed_mutex> publication_lock =
                AcquireRuntimePublicationLock(
                    metrics_rpc_stop_source.get_token());
            nodes = node_inventory.Snapshot();
            if (wallets_initialized.load(std::memory_order_acquire)) {
              wallet_snapshot.emplace(runtime_wallet_registry.Snapshot());
            }
          }
          WriteMetricsSnapshot(
              metrics_path, options, driver, nodes, run_process_state,
              {node_network_state_mutex, node_resource_state_mutex},
              [&](const NodeRuntime& node, std::string_view error) {
                boost::json::object detail;
                detail["sample"] = sample;
                detail["error"] = error;
                WriteEvent(events_path, options.run_id, node.config.id,
                           SimulationEventKind::kMetricsNodeUnavailable,
                           boost::json::serialize(detail));
                BBP_LOG(warning) << "metrics sample " << sample << " skipped "
                                 << node.config.id << ": " << error;
              },
              [&] { return metrics_collector->StopRequested(); },
              metrics_rpc_stop_source.get_token(),
              wallet_snapshot ? &wallet_snapshot->registry().topology()
                              : nullptr);
          if (metrics_collector->StopRequested()) {
            return;
          }
          if (wallet_snapshot) {
            WriteWalletMetricsSnapshot(
                wallet_metrics_path, options, driver, nodes,
                wallet_snapshot->registry(),
                [&](std::uint32_t wallet_index, const NodeRuntime& node,
                    std::string_view error) {
                  boost::json::object detail;
                  detail["sample"] = sample;
                  detail["wallet_index"] = wallet_index;
                  detail["error"] = error;
                  WriteEvent(events_path, options.run_id, node.config.id,
                             SimulationEventKind::kWalletMetricsUnavailable,
                             boost::json::serialize(detail));
                  BBP_LOG(warning) << "wallet metrics sample " << sample
                                   << " skipped #" << wallet_index << " on "
                                   << node.config.id << ": " << error;
                },
                metrics_rpc_stop_source.get_token());
          }
          if (metrics_collector->StopRequested()) {
            return;
          }
          boost::json::object detail;
          detail["sample"] = sample;
          detail["sample_count"] = options.metrics_sample_count;
          detail["interval_ms"] = options.metrics_interval.count();
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kMetricsSample,
                     boost::json::serialize(detail));
        },
        [stop_token] { return stop_token.stop_requested(); });
    metrics_collector->Start();
    InitializeWalletNodes(options, events_path, driver, nodes,
                          simulation_registry, stop_token);
    runtime_wallet_registry.Initialize(std::move(simulation_registry));
    wallets_initialized.store(true, std::memory_order_release);
    auto instrumentation_service =
        MakeLiveInstrumentationService(*instrumentation_controller);
    mcp_application.SetInstrumentationService(instrumentation_service);
    installed_instrumentation_service = std::move(instrumentation_service);
    auto workload_service = std::make_shared<McpLiveWorkloadService>();
    const auto runtime_wallet_validation_options = [&] {
      Options validation = options;
      const RuntimeWalletSnapshot wallet_snapshot =
          runtime_wallet_registry.Snapshot();
      validation.topology = wallet_snapshot.registry().topology();
      validation.wallet_backed_workload_requested =
          !wallet_snapshot.wallets().empty();
      return validation;
    };

    const auto launch_wallet_workload = MakeLiveWalletWorkloadLauncher(
        options, events_path, driver, node_inventory, runtime_wallet_registry,
        transaction_tracker, block_generation_mutex, *workload_service,
        wallet_workloads, block_generation_workloads,
        wait_until_height_workloads, wait_for_peers_workloads);

    const auto find_block_generation_workload_record =
        [block_generation_workloads](std::string_view workload_id) {
          std::lock_guard<std::mutex> lock(block_generation_workloads->mutex);
          const auto found = block_generation_workloads->records.find(
              std::string(workload_id));
          return found == block_generation_workloads->records.end()
                     ? std::shared_ptr<LiveBlockGenerationWorkloadRecord>{}
                     : found->second;
        };
    const auto launch_block_generation_workload =
        [&](BlockGenerationWorkload workload,
            std::optional<std::string> requested_id)
        -> std::shared_ptr<LiveBlockGenerationWorkloadRecord> {
      auto record = std::make_shared<LiveBlockGenerationWorkloadRecord>();
      record->workload = workload;
      {
        std::scoped_lock registry_lock(wallet_workloads->mutex,
                                       block_generation_workloads->mutex,
                                       wait_until_height_workloads->mutex,
                                       wait_for_peers_workloads->mutex);
        if (wallet_workloads->shutting_down ||
            block_generation_workloads->shutting_down ||
            wait_until_height_workloads->shutting_down ||
            wait_for_peers_workloads->shutting_down) {
          throw McpOperationFailure(
              "run_not_active",
              "the run is stopping and cannot start another workload", false);
        }
        if (wallet_workloads->records.size() +
                block_generation_workloads->records.size() +
                wait_until_height_workloads->records.size() +
                wait_for_peers_workloads->records.size() >=
            kMaximumScenarioActionCount) {
          throw McpOperationFailure(
              "workload_capacity_exceeded",
              "workload retained-instance capacity is exhausted", false);
        }
        for (const auto& [id, existing] : block_generation_workloads->records) {
          static_cast<void>(id);
          std::lock_guard<std::mutex> existing_lock(existing->mutex);
          if (!IsTerminalLiveWorkloadState(existing->state)) {
            throw McpOperationFailure(
                "workload_capacity_exceeded",
                "another block generation workload is already active", true);
          }
        }
        if (requested_id) {
          ValidateMcpIdentifier(*requested_id, "workload_id");
          if (wallet_workloads->records.contains(*requested_id) ||
              block_generation_workloads->records.contains(*requested_id) ||
              wait_until_height_workloads->records.contains(*requested_id) ||
              wait_for_peers_workloads->records.contains(*requested_id)) {
            throw McpOperationFailure(
                "workload_id_conflict",
                "workload_id is already retained: " + *requested_id, false);
          }
          record->id = *requested_id;
        } else {
          do {
            if (block_generation_workloads->next_id ==
                std::numeric_limits<std::uint64_t>::max()) {
              throw McpOperationFailure(
                  "workload_id_exhausted",
                  "block generation workload identity sequence is exhausted",
                  false);
            }
            record->id = "block-generation-workload-" +
                         std::to_string(block_generation_workloads->next_id++);
          } while (wallet_workloads->records.contains(record->id) ||
                   block_generation_workloads->records.contains(record->id) ||
                   wait_until_height_workloads->records.contains(record->id) ||
                   wait_for_peers_workloads->records.contains(record->id));
        }
        record->ordinal = static_cast<std::uint32_t>(
            wallet_workloads->records.size() +
            block_generation_workloads->records.size() +
            wait_until_height_workloads->records.size() +
            wait_for_peers_workloads->records.size() + 1U);
        block_generation_workloads->records.emplace(record->id, record);

        try {
          auto worker_lease = installed_workload_service->AcquireWorkerLease();
          record->worker = std::thread([&, record,
                                        worker_lease =
                                            std::move(worker_lease)] {
            const std::stop_token service_stop_token =
                worker_lease.stop_token();
            const auto set_terminal =
                [&](LiveWorkloadState state, std::string outcome,
                    std::optional<std::string> failure = std::nullopt) {
                  {
                    std::lock_guard<std::mutex> lock(record->mutex);
                    record->state = state;
                    record->terminal_outcome = std::move(outcome);
                    record->failure = std::move(failure);
                    record->changed.notify_all();
                  }
                  try {
                    WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                          *record);
                  } catch (const std::exception& error) {
                    BBP_LOG(error)
                        << "failed to publish terminal block generation "
                           "workload "
                        << record->id << ": " << error.what();
                  }
                };
            const auto finalize_outstanding_attempt = [&](bool cancelled) {
              std::lock_guard<std::mutex> lock(record->mutex);
              const std::uint64_t finalized = record->completed_boundaries +
                                              record->failed +
                                              record->cancelled;
              if (record->attempted > finalized) {
                if (cancelled) {
                  ++record->cancelled;
                } else {
                  ++record->failed;
                }
              }
            };
            try {
              while (true) {
                BlockGenerationWorkload boundary_workload;
                std::stop_token boundary_stop_token;
                bool publish_running = false;
                {
                  std::unique_lock<std::mutex> lock(record->mutex);
                  record->boundary_mutation_admitted = false;
                  while (record->request == LiveWorkloadRequest::kPause) {
                    record->state = LiveWorkloadState::kPaused;
                    record->changed.notify_all();
                    lock.unlock();
                    WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                          *record);
                    lock.lock();
                    if (!record->changed.wait(lock, service_stop_token, [&] {
                          return record->request != LiveWorkloadRequest::kPause;
                        })) {
                      record->request = LiveWorkloadRequest::kShutdown;
                      record->boundary_stop_source.request_stop();
                      break;
                    }
                  }
                  if (record->request == LiveWorkloadRequest::kStopCancel ||
                      record->request == LiveWorkloadRequest::kStopSettle) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kStopped, "stopped");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kShutdown) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kRunFailure) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kFailed, "failed",
                                 "run failed while block generation workload "
                                 "was active");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kReconfigure) {
                    if (!record->pending_workload) {
                      throw std::logic_error(
                          "block generation workload reconfigure has no "
                          "configuration");
                    }
                    if (record->configuration_revision ==
                        std::numeric_limits<std::uint64_t>::max()) {
                      throw std::runtime_error(
                          "block generation workload configuration revision "
                          "exceeds uint64");
                    }
                    record->workload = *record->pending_workload;
                    record->pending_workload.reset();
                    ++record->configuration_revision;
                    record->request = LiveWorkloadRequest::kNone;
                    record->state = LiveWorkloadState::kStarting;
                    record->changed.notify_all();
                    lock.unlock();
                    WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                          *record);
                    lock.lock();
                  }
                  if (record->completed_boundaries >= record->workload.count) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCompleted,
                                 "count_reached");
                    return;
                  }
                  if (service_stop_token.stop_requested()) {
                    record->request = LiveWorkloadRequest::kShutdown;
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                    return;
                  }
                  record->boundary_stop_source = std::stop_source();
                  boundary_stop_token =
                      record->boundary_stop_source.get_token();
                  boundary_workload = record->workload;
                  boundary_workload.count = 1U;
                  if (record->state != LiveWorkloadState::kRunning) {
                    record->state = LiveWorkloadState::kRunning;
                    publish_running = true;
                  }
                  if (record->attempted ==
                      std::numeric_limits<std::uint64_t>::max()) {
                    throw std::runtime_error(
                        "block generation workload attempt count exceeds "
                        "uint64");
                  }
                  ++record->attempted;
                  record->changed.notify_all();
                }
                if (publish_running) {
                  WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                        *record);
                }

                CombinedStopToken execution_stop(service_stop_token,
                                                 boundary_stop_token);
                const std::stop_token execution_stop_token =
                    execution_stop.get_token();
                auto mutation_lock = AcquireNodeMutationLock(
                    node_mutation_mutex, execution_stop_token);
                RuntimeNodeSnapshot execution_nodes = node_inventory.Snapshot();
                const auto authorize_mutation = [&, record] {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  if (execution_stop_token.stop_requested() ||
                      record->request == LiveWorkloadRequest::kStopCancel ||
                      record->request == LiveWorkloadRequest::kShutdown ||
                      record->request == LiveWorkloadRequest::kRunFailure) {
                    throw SimulationCancelled();
                  }
                  if (record->boundary_mutation_admitted) {
                    throw std::logic_error(
                        "block generation workload admitted one mutation "
                        "twice");
                  }
                  record->boundary_mutation_admitted = true;
                  record->changed.notify_all();
                };
                const GeneratedBlockWorkloadBoundary boundary =
                    GenerateBlockWorkloadBoundary(
                        driver, block_generation_mutex, execution_nodes,
                        boundary_workload, chain_spec.default_reward_address,
                        std::stop_token{}, execution_stop_token,
                        authorize_mutation);
                if (boundary.hashes.size() != 1U) {
                  throw std::logic_error(
                      "single block generation boundary returned an invalid "
                      "hash count");
                }
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  ++record->generated;
                  record->last_result = LiveBlockGenerationBoundaryResult{
                      .generator_node = boundary.generator_node,
                      .generator_node_id = boundary.generator_node_id,
                      .start_height = boundary.start_height,
                      .target_height = boundary.target_height,
                      .block_hash = boundary.hashes.front(),
                      .reward_address = boundary.reward_address,
                      .synchronized = false,
                  };
                  record->changed.notify_all();
                }
                RecordAndPublishGeneratedBlockWorkloadBoundary(
                    options, events_path, driver, execution_nodes, boundary,
                    record->ordinal, 0U, std::stop_token{}, record->id);
                SynchronizeBlockWorkloadBoundary(
                    options, events_path, driver, execution_nodes, boundary,
                    boundary_workload.sync_timeout_sec, execution_stop_token);
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  ++record->completed_boundaries;
                  if (!record->last_result || record->last_result->block_hash !=
                                                  boundary.hashes.front()) {
                    throw std::logic_error(
                        "block generation workload lost its boundary result");
                  }
                  record->last_result->synchronized = true;
                  record->changed.notify_all();
                }
              }
            } catch (const SimulationCancelled&) {
              LiveWorkloadRequest request;
              {
                std::lock_guard<std::mutex> lock(record->mutex);
                request = record->request;
              }
              if (request == LiveWorkloadRequest::kRunFailure) {
                finalize_outstanding_attempt(false);
                set_terminal(
                    LiveWorkloadState::kFailed, "failed",
                    "run failed while block generation workload was active");
              } else if (request == LiveWorkloadRequest::kStopCancel ||
                         request == LiveWorkloadRequest::kStopSettle) {
                finalize_outstanding_attempt(true);
                set_terminal(LiveWorkloadState::kStopped, "stopped");
              } else if (request == LiveWorkloadRequest::kShutdown ||
                         service_stop_token.stop_requested()) {
                finalize_outstanding_attempt(true);
                set_terminal(LiveWorkloadState::kCancelled, "cancelled");
              } else {
                finalize_outstanding_attempt(false);
                set_terminal(
                    LiveWorkloadState::kFailed, "failed",
                    "block generation workload execution was cancelled "
                    "unexpectedly");
              }
            } catch (const BlockGenerationOutcomeUnconfirmed& error) {
              {
                std::lock_guard<std::mutex> lock(record->mutex);
                record->generation_outcome_unconfirmed = true;
              }
              finalize_outstanding_attempt(false);
              set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
            } catch (const std::exception& error) {
              finalize_outstanding_attempt(false);
              set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
            } catch (...) {
              finalize_outstanding_attempt(false);
              set_terminal(LiveWorkloadState::kFailed, "failed",
                           "unknown block generation workload failure");
            }
          });
        } catch (...) {
          const auto found =
              block_generation_workloads->records.find(record->id);
          if (found != block_generation_workloads->records.end() &&
              found->second == record) {
            block_generation_workloads->records.erase(found);
          }
          throw;
        }
      }
      return record;
    };

    const auto block_generation_operation = MakeLiveBlockGenerationOperation(
        find_block_generation_workload_record,
        [&, launch_block_generation_workload](
            const boost::json::object& workload_value,
            std::optional<std::string> requested_id,
            std::stop_token operation_stop_token) {
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
          const BlockGenerationWorkload workload =
              ParseAndValidateLiveBlockGenerationWorkload(
                  workload_value, options, current_nodes);
          return launch_block_generation_workload(workload,
                                                  std::move(requested_id));
        },
        [&](const boost::json::object& workload_value) {
          const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
          return ParseAndValidateLiveBlockGenerationWorkload(
              workload_value, options, current_nodes);
        },
        [&](const LiveBlockGenerationWorkloadRecord& record) {
          WriteLiveBlockGenerationWorkloadState(events_path, options, record);
        });

    const auto find_wait_until_height_workload_record =
        [wait_until_height_workloads](std::string_view workload_id) {
          std::lock_guard<std::mutex> lock(wait_until_height_workloads->mutex);
          const auto found = wait_until_height_workloads->records.find(
              std::string(workload_id));
          return found == wait_until_height_workloads->records.end()
                     ? std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>{}
                     : found->second;
        };
    const auto launch_wait_until_height_workload =
        [&](WaitUntilHeightWorkload workload,
            std::optional<std::string> requested_id)
        -> std::shared_ptr<LiveWaitUntilHeightWorkloadRecord> {
      auto record = std::make_shared<LiveWaitUntilHeightWorkloadRecord>();
      record->workload = workload;
      {
        std::scoped_lock registry_lock(wallet_workloads->mutex,
                                       block_generation_workloads->mutex,
                                       wait_until_height_workloads->mutex,
                                       wait_for_peers_workloads->mutex);
        if (wallet_workloads->shutting_down ||
            block_generation_workloads->shutting_down ||
            wait_until_height_workloads->shutting_down ||
            wait_for_peers_workloads->shutting_down) {
          throw McpOperationFailure(
              "run_not_active",
              "the run is stopping and cannot start another workload", false);
        }
        if (wallet_workloads->records.size() +
                block_generation_workloads->records.size() +
                wait_until_height_workloads->records.size() +
                wait_for_peers_workloads->records.size() >=
            kMaximumScenarioActionCount) {
          throw McpOperationFailure(
              "workload_capacity_exceeded",
              "workload retained-instance capacity is exhausted", false);
        }
        if (requested_id) {
          ValidateMcpIdentifier(*requested_id, "workload_id");
          if (wallet_workloads->records.contains(*requested_id) ||
              block_generation_workloads->records.contains(*requested_id) ||
              wait_until_height_workloads->records.contains(*requested_id) ||
              wait_for_peers_workloads->records.contains(*requested_id)) {
            throw McpOperationFailure(
                "workload_id_conflict",
                "workload_id is already retained: " + *requested_id, false);
          }
          record->id = *requested_id;
        } else {
          do {
            if (wait_until_height_workloads->next_id ==
                std::numeric_limits<std::uint64_t>::max()) {
              throw McpOperationFailure(
                  "workload_id_exhausted",
                  "wait-until-height workload identity sequence is exhausted",
                  false);
            }
            record->id = "wait-until-height-workload-" +
                         std::to_string(wait_until_height_workloads->next_id++);
          } while (wallet_workloads->records.contains(record->id) ||
                   block_generation_workloads->records.contains(record->id) ||
                   wait_until_height_workloads->records.contains(record->id) ||
                   wait_for_peers_workloads->records.contains(record->id));
        }
        record->ordinal = static_cast<std::uint32_t>(
            wallet_workloads->records.size() +
            block_generation_workloads->records.size() +
            wait_until_height_workloads->records.size() +
            wait_for_peers_workloads->records.size() + 1U);
        wait_until_height_workloads->records.emplace(record->id, record);

        try {
          auto worker_lease = installed_workload_service->AcquireWorkerLease();
          record->worker = std::thread([&, record,
                                        worker_lease =
                                            std::move(worker_lease)] {
            const std::stop_token service_stop_token =
                worker_lease.stop_token();
            const auto set_terminal =
                [&](LiveWorkloadState state, std::string outcome,
                    std::optional<std::string> failure = std::nullopt) {
                  {
                    std::lock_guard<std::mutex> lock(record->mutex);
                    record->state = state;
                    record->terminal_outcome = std::move(outcome);
                    record->failure = std::move(failure);
                    record->completion_pending = false;
                    record->changed.notify_all();
                  }
                  try {
                    WriteLiveWaitUntilHeightWorkloadState(events_path, options,
                                                          *record);
                  } catch (const std::exception& error) {
                    BBP_LOG(error)
                        << "failed to publish terminal wait-until-height "
                           "workload "
                        << record->id << ": " << error.what();
                  }
                };
            try {
              while (true) {
                WaitUntilHeightWorkload epoch_workload;
                std::stop_source epoch_stop_source;
                std::chrono::steady_clock::time_point deadline;
                bool publish_running = false;
                {
                  std::unique_lock<std::mutex> lock(record->mutex);
                  while (record->request == LiveWorkloadRequest::kPause) {
                    record->state = LiveWorkloadState::kPaused;
                    record->epoch_deadline =
                        std::chrono::steady_clock::time_point{};
                    record->epoch_timed_out = false;
                    record->epoch_run_stop_requested_at.reset();
                    record->changed.notify_all();
                    lock.unlock();
                    WriteLiveWaitUntilHeightWorkloadState(events_path, options,
                                                          *record);
                    lock.lock();
                    if (!record->changed.wait(lock, service_stop_token, [&] {
                          return record->request != LiveWorkloadRequest::kPause;
                        })) {
                      record->request = LiveWorkloadRequest::kShutdown;
                      record->epoch_stop_source.request_stop();
                      break;
                    }
                  }
                  if (record->request == LiveWorkloadRequest::kStopCancel ||
                      record->request == LiveWorkloadRequest::kStopSettle) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kStopped, "stopped");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kShutdown) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kRunFailure) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kFailed, "failed",
                                 "run failed while wait-until-height workload "
                                 "was active");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kReconfigure) {
                    if (!record->pending_workload) {
                      throw std::logic_error(
                          "wait-until-height workload reconfigure has no "
                          "configuration");
                    }
                    if (record->configuration_revision ==
                        std::numeric_limits<std::uint64_t>::max()) {
                      throw std::runtime_error(
                          "wait-until-height workload configuration revision "
                          "exceeds uint64");
                    }
                    record->workload = *record->pending_workload;
                    record->pending_workload.reset();
                    ++record->configuration_revision;
                    record->request = LiveWorkloadRequest::kNone;
                    record->state = LiveWorkloadState::kStarting;
                    record->epoch_deadline =
                        std::chrono::steady_clock::time_point{};
                    record->changed.notify_all();
                    lock.unlock();
                    WriteLiveWaitUntilHeightWorkloadState(events_path, options,
                                                          *record);
                    lock.lock();
                    if (record->request != LiveWorkloadRequest::kNone) {
                      continue;
                    }
                  }
                  if (service_stop_token.stop_requested()) {
                    record->request = LiveWorkloadRequest::kShutdown;
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                    return;
                  }
                  record->epoch_stop_source = std::stop_source();
                  epoch_stop_source = record->epoch_stop_source;
                  epoch_workload = record->workload;
                  deadline = std::chrono::steady_clock::now() +
                             std::chrono::seconds(epoch_workload.timeout_sec);
                  record->epoch_deadline = deadline;
                  record->epoch_timed_out = false;
                  record->epoch_run_stop_requested_at.reset();
                  if (record->state != LiveWorkloadState::kRunning) {
                    record->state = LiveWorkloadState::kRunning;
                    publish_running = true;
                  }
                  record->changed.notify_all();
                }
                if (publish_running) {
                  WriteLiveWaitUntilHeightWorkloadState(events_path, options,
                                                        *record);
                }

                CombinedStopToken execution_stop(service_stop_token,
                                                 epoch_stop_source.get_token());
                const std::stop_token execution_stop_token =
                    execution_stop.get_token();
                std::stop_callback stop_epoch_on_service_stop(
                    service_stop_token, [&, epoch_stop_source] {
                      const auto observed_at = std::chrono::steady_clock::now();
                      record_run_stop(observed_at);
                      const auto requested_at =
                          observed_run_stop().value_or(observed_at);
                      std::lock_guard<std::mutex> lock(record->mutex);
                      if (record->completion_pending) {
                        return;
                      }
                      record->epoch_run_stop_requested_at = requested_at;
                      epoch_stop_source.request_stop();
                      record->changed.notify_all();
                    });
                std::mutex deadline_mutex;
                std::condition_variable_any deadline_changed;
                std::jthread deadline_timer(
                    [&, epoch_stop_source](std::stop_token timer_stop_token) {
                      {
                        std::unique_lock<std::mutex> lock(deadline_mutex);
                        static_cast<void>(deadline_changed.wait_until(
                            lock, timer_stop_token, deadline,
                            [] { return false; }));
                      }
                      if (timer_stop_token.stop_requested()) {
                        return;
                      }
                      std::lock_guard<std::mutex> lock(record->mutex);
                      if (!timer_stop_token.stop_requested() &&
                          (!record->epoch_run_stop_requested_at ||
                           *record->epoch_run_stop_requested_at >= deadline) &&
                          (record->request == LiveWorkloadRequest::kNone ||
                           record->request ==
                               LiveWorkloadRequest::kStopSettle)) {
                        record->epoch_timed_out = true;
                        epoch_stop_source.request_stop();
                      }
                    });
                try {
                  ChainNodeConfig target_config;
                  {
                    auto mutation_lock = AcquireNodeMutationLock(
                        node_mutation_mutex, execution_stop_token);
                    const RuntimeNodeSnapshot execution_nodes =
                        node_inventory.Snapshot();
                    NodeRuntime& node = RequireRuntimeNodeNumber(
                        execution_nodes, epoch_workload.node,
                        "wait_until_height workload");
                    RequireNodeRunning(node, "wait_until_height workload");
                    target_config = node.config;
                  }
                  while (true) {
                    const std::optional<std::uint64_t> observed_height =
                        WaitForHeightReadback(
                            driver, target_config, epoch_workload.height,
                            std::chrono::seconds(epoch_workload.timeout_sec),
                            execution_stop_token);
                    std::optional<LiveWaitUntilHeightResult> completed_result;
                    if (observed_height) {
                      completed_result.emplace(LiveWaitUntilHeightResult{
                          .node = epoch_workload.node,
                          .node_id = target_config.id,
                          .target_height = epoch_workload.height,
                          .observed_height = *observed_height,
                      });
                    }
                    std::unique_lock<std::mutex> lock(record->mutex);
                    if ((record->request != LiveWorkloadRequest::kNone &&
                         record->request != LiveWorkloadRequest::kStopSettle)) {
                      throw SimulationCancelled();
                    }
                    const auto require_open_epoch = [&] {
                      if (record->epoch_run_stop_requested_at &&
                          *record->epoch_run_stop_requested_at < deadline) {
                        throw SimulationCancelled();
                      }
                      if (record->epoch_timed_out ||
                          std::chrono::steady_clock::now() >= deadline) {
                        record->epoch_timed_out = true;
                        epoch_stop_source.request_stop();
                        throw SimulationCancelled();
                      }
                      if (execution_stop_token.stop_requested()) {
                        throw SimulationCancelled();
                      }
                    };
                    require_open_epoch();
                    if (!observed_height) {
                      lock.unlock();
                      continue;
                    }
                    deadline_timer.request_stop();
                    require_open_epoch();
                    const bool settle =
                        record->request == LiveWorkloadRequest::kStopSettle;
                    record->completion_pending = true;
                    record->state = LiveWorkloadState::kStopping;
                    record->changed.notify_all();
                    lock.unlock();
                    WriteEvent(events_path, options.run_id, target_config.id,
                               SimulationEventKind::kHeightWaitReached,
                               HeightWaitDetail(record->ordinal, 0U,
                                                epoch_workload.node,
                                                epoch_workload.height,
                                                *observed_height, record->id));
                    lock.lock();
                    record->result = std::move(*completed_result);
                    record->completion_pending = false;
                    if (settle) {
                      record->state = LiveWorkloadState::kStopped;
                      record->terminal_outcome = "stopped";
                    } else {
                      record->state = LiveWorkloadState::kCompleted;
                      record->terminal_outcome = "height_reached";
                    }
                    record->changed.notify_all();
                    lock.unlock();
                    try {
                      WriteLiveWaitUntilHeightWorkloadState(events_path,
                                                            options, *record);
                    } catch (const std::exception& error) {
                      BBP_LOG(error)
                          << "failed to publish completed wait-until-height "
                             "workload "
                          << record->id << ": " << error.what();
                    }
                    return;
                  }
                } catch (const SimulationCancelled&) {
                  deadline_timer.request_stop();
                  LiveWorkloadRequest request;
                  bool timed_out = false;
                  bool run_stop_admitted = false;
                  {
                    std::lock_guard<std::mutex> lock(record->mutex);
                    request = record->request;
                    run_stop_admitted =
                        record->epoch_run_stop_requested_at &&
                        *record->epoch_run_stop_requested_at < deadline;
                    if (!run_stop_admitted &&
                        (request == LiveWorkloadRequest::kNone ||
                         request == LiveWorkloadRequest::kStopSettle) &&
                        std::chrono::steady_clock::now() >= deadline) {
                      record->epoch_timed_out = true;
                      epoch_stop_source.request_stop();
                    }
                    timed_out = record->epoch_timed_out && !run_stop_admitted;
                  }
                  if (timed_out) {
                    throw std::runtime_error(
                        "wait_until_height workload timed out after " +
                        std::to_string(epoch_workload.timeout_sec) +
                        " seconds waiting for height " +
                        std::to_string(epoch_workload.height));
                  }
                  if (!run_stop_admitted &&
                      (request == LiveWorkloadRequest::kPause ||
                       request == LiveWorkloadRequest::kReconfigure)) {
                    continue;
                  }
                  throw;
                }
              }
            } catch (const SimulationCancelled&) {
              LiveWorkloadRequest request;
              bool run_stop_admitted = false;
              {
                std::lock_guard<std::mutex> lock(record->mutex);
                request = record->request;
                run_stop_admitted = record->epoch_run_stop_requested_at &&
                                    *record->epoch_run_stop_requested_at <
                                        record->epoch_deadline;
              }
              if (request == LiveWorkloadRequest::kRunFailure) {
                set_terminal(
                    LiveWorkloadState::kFailed, "failed",
                    "run failed while wait-until-height workload was active");
              } else if (run_stop_admitted ||
                         request == LiveWorkloadRequest::kShutdown ||
                         service_stop_token.stop_requested()) {
                set_terminal(LiveWorkloadState::kCancelled, "cancelled");
              } else if (request == LiveWorkloadRequest::kStopCancel ||
                         request == LiveWorkloadRequest::kStopSettle) {
                set_terminal(LiveWorkloadState::kStopped, "stopped");
              } else {
                set_terminal(
                    LiveWorkloadState::kFailed, "failed",
                    "wait-until-height workload execution was cancelled "
                    "unexpectedly");
              }
            } catch (const std::exception& error) {
              set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
            } catch (...) {
              set_terminal(LiveWorkloadState::kFailed, "failed",
                           "unknown wait-until-height workload failure");
            }
          });
        } catch (...) {
          const auto found =
              wait_until_height_workloads->records.find(record->id);
          if (found != wait_until_height_workloads->records.end() &&
              found->second == record) {
            wait_until_height_workloads->records.erase(found);
          }
          throw;
        }
      }
      return record;
    };

    const auto wait_until_height_operation = MakeLiveWaitUntilHeightOperation(
        find_wait_until_height_workload_record,
        [&, launch_wait_until_height_workload](
            const boost::json::object& workload_value,
            std::optional<std::string> requested_id,
            std::stop_token operation_stop_token) {
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
          const WaitUntilHeightWorkload workload =
              ParseAndValidateLiveWaitUntilHeightWorkload(
                  workload_value, options, current_nodes);
          return launch_wait_until_height_workload(workload,
                                                   std::move(requested_id));
        },
        [&](const boost::json::object& workload_value,
            std::stop_token operation_stop_token) {
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          return ParseAndValidateLiveWaitUntilHeightWorkload(
              workload_value, options, node_inventory.Snapshot());
        },
        [&](const LiveWaitUntilHeightWorkloadRecord& record) {
          WriteLiveWaitUntilHeightWorkloadState(events_path, options, record);
        });

    const auto find_wait_for_peers_workload_record =
        [wait_for_peers_workloads](std::string_view workload_id) {
          std::lock_guard<std::mutex> lock(wait_for_peers_workloads->mutex);
          const auto found =
              wait_for_peers_workloads->records.find(std::string(workload_id));
          return found == wait_for_peers_workloads->records.end()
                     ? std::shared_ptr<LiveWaitForPeersWorkloadRecord>{}
                     : found->second;
        };
    const auto launch_wait_for_peers_workload =
        [&](WaitForPeersWorkload workload,
            std::optional<std::string> requested_id)
        -> std::shared_ptr<LiveWaitForPeersWorkloadRecord> {
      auto record = std::make_shared<LiveWaitForPeersWorkloadRecord>();
      record->workload = workload;
      {
        std::scoped_lock registry_lock(wallet_workloads->mutex,
                                       block_generation_workloads->mutex,
                                       wait_for_peers_workloads->mutex,
                                       wait_until_height_workloads->mutex);
        if (wallet_workloads->shutting_down ||
            block_generation_workloads->shutting_down ||
            wait_for_peers_workloads->shutting_down ||
            wait_until_height_workloads->shutting_down) {
          throw McpOperationFailure(
              "run_not_active",
              "the run is stopping and cannot start another workload", false);
        }
        if (wallet_workloads->records.size() +
                block_generation_workloads->records.size() +
                wait_for_peers_workloads->records.size() +
                wait_until_height_workloads->records.size() >=
            kMaximumScenarioActionCount) {
          throw McpOperationFailure(
              "workload_capacity_exceeded",
              "workload retained-instance capacity is exhausted", false);
        }
        if (requested_id) {
          ValidateMcpIdentifier(*requested_id, "workload_id");
          if (wallet_workloads->records.contains(*requested_id) ||
              block_generation_workloads->records.contains(*requested_id) ||
              wait_for_peers_workloads->records.contains(*requested_id) ||
              wait_until_height_workloads->records.contains(*requested_id)) {
            throw McpOperationFailure(
                "workload_id_conflict",
                "workload_id is already retained: " + *requested_id, false);
          }
          record->id = *requested_id;
        } else {
          do {
            if (wait_for_peers_workloads->next_id ==
                std::numeric_limits<std::uint64_t>::max()) {
              throw McpOperationFailure(
                  "workload_id_exhausted",
                  "wait-for-peers workload identity sequence is exhausted",
                  false);
            }
            record->id = "wait-for-peers-workload-" +
                         std::to_string(wait_for_peers_workloads->next_id++);
          } while (wallet_workloads->records.contains(record->id) ||
                   block_generation_workloads->records.contains(record->id) ||
                   wait_for_peers_workloads->records.contains(record->id) ||
                   wait_until_height_workloads->records.contains(record->id));
        }
        record->ordinal = static_cast<std::uint32_t>(
            wallet_workloads->records.size() +
            block_generation_workloads->records.size() +
            wait_for_peers_workloads->records.size() +
            wait_until_height_workloads->records.size() + 1U);
        wait_for_peers_workloads->records.emplace(record->id, record);

        try {
          auto worker_lease = installed_workload_service->AcquireWorkerLease();
          record->worker = std::thread([&, record,
                                        worker_lease =
                                            std::move(worker_lease)] {
            const std::stop_token service_stop_token =
                worker_lease.stop_token();
            const auto set_terminal =
                [&](LiveWorkloadState state, std::string outcome,
                    std::optional<std::string> failure = std::nullopt) {
                  {
                    std::lock_guard<std::mutex> lock(record->mutex);
                    record->state = state;
                    record->terminal_outcome = std::move(outcome);
                    record->failure = std::move(failure);
                    record->completion_pending = false;
                    record->changed.notify_all();
                  }
                  try {
                    WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                       *record);
                  } catch (const std::exception& error) {
                    BBP_LOG(error)
                        << "failed to publish terminal wait-for-peers "
                           "workload "
                        << record->id << ": " << error.what();
                  }
                };
            try {
              while (true) {
                WaitForPeersWorkload epoch_workload;
                std::stop_source epoch_stop_source;
                std::chrono::steady_clock::time_point deadline;
                bool publish_running = false;
                {
                  std::unique_lock<std::mutex> lock(record->mutex);
                  while (record->request == LiveWorkloadRequest::kPause) {
                    record->state = LiveWorkloadState::kPaused;
                    record->epoch_deadline =
                        std::chrono::steady_clock::time_point{};
                    record->epoch_timed_out = false;
                    record->epoch_run_stop_requested_at.reset();
                    record->changed.notify_all();
                    lock.unlock();
                    WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                       *record);
                    lock.lock();
                    if (!record->changed.wait(lock, service_stop_token, [&] {
                          return record->request != LiveWorkloadRequest::kPause;
                        })) {
                      record->request = LiveWorkloadRequest::kShutdown;
                      record->epoch_stop_source.request_stop();
                      break;
                    }
                  }
                  if (record->request == LiveWorkloadRequest::kStopCancel ||
                      record->request == LiveWorkloadRequest::kStopSettle) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kStopped, "stopped");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kShutdown) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kRunFailure) {
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kFailed, "failed",
                                 "run failed while wait-for-peers workload "
                                 "was active");
                    return;
                  }
                  if (record->request == LiveWorkloadRequest::kReconfigure) {
                    if (!record->pending_workload) {
                      throw std::logic_error(
                          "wait-for-peers workload reconfigure has no "
                          "configuration");
                    }
                    if (record->configuration_revision ==
                        std::numeric_limits<std::uint64_t>::max()) {
                      throw std::runtime_error(
                          "wait-for-peers workload configuration revision "
                          "exceeds uint64");
                    }
                    record->workload = *record->pending_workload;
                    record->pending_workload.reset();
                    ++record->configuration_revision;
                    record->request = LiveWorkloadRequest::kNone;
                    record->state = LiveWorkloadState::kStarting;
                    record->epoch_deadline =
                        std::chrono::steady_clock::time_point{};
                    record->changed.notify_all();
                    lock.unlock();
                    WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                       *record);
                    lock.lock();
                    if (record->request != LiveWorkloadRequest::kNone) {
                      continue;
                    }
                  }
                  if (service_stop_token.stop_requested()) {
                    record->request = LiveWorkloadRequest::kShutdown;
                    lock.unlock();
                    set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                    return;
                  }
                  record->epoch_stop_source = std::stop_source();
                  epoch_stop_source = record->epoch_stop_source;
                  epoch_workload = record->workload;
                  deadline = std::chrono::steady_clock::now() +
                             std::chrono::seconds(epoch_workload.timeout_sec);
                  record->epoch_deadline = deadline;
                  record->epoch_timed_out = false;
                  record->epoch_run_stop_requested_at.reset();
                  if (record->state != LiveWorkloadState::kRunning) {
                    record->state = LiveWorkloadState::kRunning;
                    publish_running = true;
                  }
                  record->changed.notify_all();
                }
                if (publish_running) {
                  WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                     *record);
                }

                CombinedStopToken execution_stop(service_stop_token,
                                                 epoch_stop_source.get_token());
                const std::stop_token execution_stop_token =
                    execution_stop.get_token();
                std::stop_callback stop_epoch_on_service_stop(
                    service_stop_token, [&, epoch_stop_source] {
                      const auto observed_at = std::chrono::steady_clock::now();
                      record_run_stop(observed_at);
                      const auto requested_at =
                          observed_run_stop().value_or(observed_at);
                      std::lock_guard<std::mutex> lock(record->mutex);
                      if (record->completion_pending) {
                        return;
                      }
                      record->epoch_run_stop_requested_at = requested_at;
                      epoch_stop_source.request_stop();
                      record->changed.notify_all();
                    });
                std::mutex deadline_mutex;
                std::condition_variable_any deadline_changed;
                std::jthread deadline_timer(
                    [&, epoch_stop_source](std::stop_token timer_stop_token) {
                      {
                        std::unique_lock<std::mutex> lock(deadline_mutex);
                        static_cast<void>(deadline_changed.wait_until(
                            lock, timer_stop_token, deadline,
                            [] { return false; }));
                      }
                      if (timer_stop_token.stop_requested()) {
                        return;
                      }
                      std::lock_guard<std::mutex> lock(record->mutex);
                      if (!timer_stop_token.stop_requested() &&
                          (!record->epoch_run_stop_requested_at ||
                           *record->epoch_run_stop_requested_at >= deadline) &&
                          (record->request == LiveWorkloadRequest::kNone ||
                           record->request ==
                               LiveWorkloadRequest::kStopSettle)) {
                        record->epoch_timed_out = true;
                        epoch_stop_source.request_stop();
                      }
                    });
                try {
                  ChainNodeConfig target_config;
                  {
                    auto mutation_lock = AcquireNodeMutationLock(
                        node_mutation_mutex, execution_stop_token);
                    const RuntimeNodeSnapshot execution_nodes =
                        node_inventory.Snapshot();
                    NodeRuntime& node = RequireRuntimeNodeNumber(
                        execution_nodes, epoch_workload.node,
                        "wait_for_peers workload");
                    RequireNodeRunning(node, "wait_for_peers workload");
                    target_config = node.config;
                  }
                  while (true) {
                    const std::uint64_t observed_peer_count =
                        driver.WaitForPeerCount(
                            target_config, epoch_workload.peer_count,
                            std::chrono::seconds(epoch_workload.timeout_sec),
                            execution_stop_token);
                    std::optional<LiveWaitForPeersResult> completed_result;
                    if (observed_peer_count >= epoch_workload.peer_count) {
                      completed_result.emplace(LiveWaitForPeersResult{
                          .node = epoch_workload.node,
                          .node_id = target_config.id,
                          .target_peer_count = epoch_workload.peer_count,
                          .observed_peer_count = observed_peer_count,
                      });
                    }
                    std::unique_lock<std::mutex> lock(record->mutex);
                    if ((record->request != LiveWorkloadRequest::kNone &&
                         record->request != LiveWorkloadRequest::kStopSettle)) {
                      throw SimulationCancelled();
                    }
                    const auto require_open_epoch = [&] {
                      if (record->epoch_run_stop_requested_at &&
                          *record->epoch_run_stop_requested_at < deadline) {
                        throw SimulationCancelled();
                      }
                      if (record->epoch_timed_out ||
                          std::chrono::steady_clock::now() >= deadline) {
                        record->epoch_timed_out = true;
                        epoch_stop_source.request_stop();
                        throw SimulationCancelled();
                      }
                      if (execution_stop_token.stop_requested()) {
                        throw SimulationCancelled();
                      }
                    };
                    require_open_epoch();
                    if (observed_peer_count < epoch_workload.peer_count) {
                      lock.unlock();
                      continue;
                    }
                    deadline_timer.request_stop();
                    require_open_epoch();
                    const bool settle =
                        record->request == LiveWorkloadRequest::kStopSettle;
                    record->completion_pending = true;
                    record->state = LiveWorkloadState::kStopping;
                    record->changed.notify_all();
                    lock.unlock();
                    WriteEvent(events_path, options.run_id, target_config.id,
                               SimulationEventKind::kPeerCountReached,
                               PeerCountWaitDetail(
                                   record->ordinal, 0U, epoch_workload.node,
                                   epoch_workload.peer_count,
                                   observed_peer_count, record->id));
                    lock.lock();
                    record->result = std::move(*completed_result);
                    record->completion_pending = false;
                    if (settle) {
                      record->state = LiveWorkloadState::kStopped;
                      record->terminal_outcome = "stopped";
                    } else {
                      record->state = LiveWorkloadState::kCompleted;
                      record->terminal_outcome = "peer_count_reached";
                    }
                    record->changed.notify_all();
                    lock.unlock();
                    try {
                      WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                         *record);
                    } catch (const std::exception& error) {
                      BBP_LOG(error)
                          << "failed to publish completed wait-for-peers "
                             "workload "
                          << record->id << ": " << error.what();
                    }
                    return;
                  }
                } catch (const SimulationCancelled&) {
                  deadline_timer.request_stop();
                  LiveWorkloadRequest request;
                  bool timed_out = false;
                  bool run_stop_admitted = false;
                  {
                    std::lock_guard<std::mutex> lock(record->mutex);
                    request = record->request;
                    run_stop_admitted =
                        record->epoch_run_stop_requested_at &&
                        *record->epoch_run_stop_requested_at < deadline;
                    if (!run_stop_admitted &&
                        (request == LiveWorkloadRequest::kNone ||
                         request == LiveWorkloadRequest::kStopSettle) &&
                        std::chrono::steady_clock::now() >= deadline) {
                      record->epoch_timed_out = true;
                      epoch_stop_source.request_stop();
                    }
                    timed_out = record->epoch_timed_out && !run_stop_admitted;
                  }
                  if (timed_out) {
                    throw std::runtime_error(
                        "wait_for_peers workload timed out after " +
                        std::to_string(epoch_workload.timeout_sec) +
                        " seconds waiting for peer count " +
                        std::to_string(epoch_workload.peer_count));
                  }
                  if (!run_stop_admitted &&
                      (request == LiveWorkloadRequest::kPause ||
                       request == LiveWorkloadRequest::kReconfigure)) {
                    continue;
                  }
                  throw;
                }
              }
            } catch (const SimulationCancelled&) {
              LiveWorkloadRequest request;
              bool run_stop_admitted = false;
              {
                std::lock_guard<std::mutex> lock(record->mutex);
                request = record->request;
                run_stop_admitted = record->epoch_run_stop_requested_at &&
                                    *record->epoch_run_stop_requested_at <
                                        record->epoch_deadline;
              }
              if (request == LiveWorkloadRequest::kRunFailure) {
                set_terminal(
                    LiveWorkloadState::kFailed, "failed",
                    "run failed while wait-for-peers workload was active");
              } else if (run_stop_admitted ||
                         request == LiveWorkloadRequest::kShutdown ||
                         service_stop_token.stop_requested()) {
                set_terminal(LiveWorkloadState::kCancelled, "cancelled");
              } else if (request == LiveWorkloadRequest::kStopCancel ||
                         request == LiveWorkloadRequest::kStopSettle) {
                set_terminal(LiveWorkloadState::kStopped, "stopped");
              } else {
                set_terminal(LiveWorkloadState::kFailed, "failed",
                             "wait-for-peers workload execution was cancelled "
                             "unexpectedly");
              }
            } catch (const std::exception& error) {
              set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
            } catch (...) {
              set_terminal(LiveWorkloadState::kFailed, "failed",
                           "unknown wait-for-peers workload failure");
            }
          });
        } catch (...) {
          const auto found = wait_for_peers_workloads->records.find(record->id);
          if (found != wait_for_peers_workloads->records.end() &&
              found->second == record) {
            wait_for_peers_workloads->records.erase(found);
          }
          throw;
        }
      }
      return record;
    };

    const auto wait_for_peers_operation = MakeLiveWaitForPeersOperation(
        find_wait_for_peers_workload_record,
        [&, launch_wait_for_peers_workload](
            const boost::json::object& workload_value,
            std::optional<std::string> requested_id,
            std::stop_token operation_stop_token) {
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
          const WaitForPeersWorkload workload =
              ParseAndValidateLiveWaitForPeersWorkload(workload_value, options,
                                                       current_nodes);
          return launch_wait_for_peers_workload(workload,
                                                std::move(requested_id));
        },
        [&](const boost::json::object& workload_value,
            std::stop_token operation_stop_token) {
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          return ParseAndValidateLiveWaitForPeersWorkload(
              workload_value, options, node_inventory.Snapshot());
        },
        [&](const LiveWaitForPeersWorkloadRecord& record) {
          WriteLiveWaitForPeersWorkloadState(events_path, options, record);
        });

    const auto dispatch_one_shot_workload =
        [&](const ScenarioWorkload& scenario_workload,
            const RuntimeNodeSnapshot& nodes, std::uint32_t action_index,
            std::uint32_t action_count, std::stop_token operation_stop_token,
            SimulationCommandControl* cancellation_commit_control = nullptr) {
          if (!IsOneShotWorkloadKind(scenario_workload.kind)) {
            throw std::logic_error(
                "one-shot dispatcher received a lifecycle workload");
          }
          ThrowIfStopRequested(operation_stop_token);
          std::function<void()> authorize_mutation;
          if (cancellation_commit_control != nullptr) {
            authorize_mutation = [&] {
              if (cancellation_commit_control->TryBeginCommit()) {
                return;
              }
              if (cancellation_commit_control->CommitPhase() ==
                  SimulationCommandCommitPhase::kCancelled) {
                throw SimulationCancelled();
              }
              ThrowIfStopRequested(operation_stop_token);
              throw std::logic_error(
                  "one-shot workload mutation admission reached an unexpected "
                  "commit phase");
            };
          }
          switch (scenario_workload.kind) {
            case WorkloadKind::kConnectPeer:
              ApplyConnectPeerWorkload(
                  options, events_path, driver, *peer_connectivity_controller,
                  nodes, scenario_workload.connect_peer, action_index,
                  action_count, operation_stop_token,
                  cancellation_commit_control);
              break;
            case WorkloadKind::kDisconnectPeer:
              ApplyDisconnectPeerWorkload(
                  options, events_path, driver, *peer_connectivity_controller,
                  nodes, scenario_workload.disconnect_peer, action_index,
                  action_count, operation_stop_token,
                  cancellation_commit_control);
              break;
            case WorkloadKind::kRestartNode: {
              const RestartNodeWorkload& workload =
                  scenario_workload.restart_node;
              const auto selected =
                  std::find_if(nodes.begin(), nodes.end(),
                               [&](const NodeRuntime& candidate) {
                                 return candidate.config.id == workload.node_id;
                               });
              if (selected == nodes.end()) {
                throw std::runtime_error(
                    "restart_node workload references an inactive node id: " +
                    workload.node_id);
              }
              NodeRuntime& node = *selected;
              if (!RestartNode(options, events_path, driver,
                               *peer_connectivity_controller, node,
                               lifecycle_epoch, operation_stop_token,
                               "requested", cancellation_commit_control,
                               nullptr, true, nullptr, true,
                               cancellation_commit_control, stop_token)) {
                throw std::runtime_error(
                    "restart_node workload reached node stop_time before "
                    "completion: " +
                    node.config.id);
              }
              WriteEvent(events_path, options.run_id, node.config.id,
                         SimulationEventKind::kNodeRestarted,
                         RestartNodeWorkloadDetail(action_index, action_count,
                                                   workload.node,
                                                   node.RestartCount()));
              if (cancellation_commit_control != nullptr) {
                cancellation_commit_control->MarkCommitted();
              }
              break;
            }
            case WorkloadKind::kFreezeNode: {
              const FreezeNodeWorkload& workload =
                  scenario_workload.freeze_node;
              NodeRuntime& node = RequireRuntimeNodeNumber(
                  nodes, workload.node, "freeze_node workload");
              RequireNodeRunning(node, "freeze_node workload");
              FreezeNodeForDuration(options, events_path, node,
                                    workload.duration_ms, operation_stop_token);
              try {
                WriteEvent(events_path, options.run_id, node.config.id,
                           SimulationEventKind::kNodeFreezeCompleted,
                           FreezeNodeWorkloadDetail(action_index, action_count,
                                                    workload.node,
                                                    workload.duration_ms));
              } catch (...) {
                ThrowWorkloadMutationOutcomeUnconfirmed(
                    "node freeze completed without a publishable workload "
                    "outcome",
                    std::current_exception());
              }
              break;
            }
            case WorkloadKind::kUpdateResourceLimits: {
              const ResourceLimitUpdateWorkload& workload =
                  scenario_workload.update_resource_limits;
              NodeRuntime& node = nodes[workload.node - 1U];
              ApplyResourceLimitUpdate(
                  options, events_path, node, workload.patch,
                  node_resource_state_mutex, operation_stop_token,
                  authorize_mutation, action_index, action_count,
                  workload.node);
              break;
            }
            case WorkloadKind::kSetResourceProfile:
              ApplyResourceProfileSwitch(
                  options, events_path, nodes, node_resource_state_mutex,
                  scenario_workload.profile_switch, action_index, action_count,
                  operation_stop_token, authorize_mutation);
              break;
            case WorkloadKind::kSetNetworkProfile:
              ApplyNetworkProfileSwitch(
                  options, events_path, nodes, node_network_state_mutex,
                  scenario_workload.profile_switch, action_index, action_count,
                  operation_stop_token);
              break;
            case WorkloadKind::kResourcePressure:
              ApplyResourcePressureWorkload(
                  options, events_path, metrics_path, driver, nodes,
                  node_network_state_mutex, node_resource_state_mutex,
                  run_process_state,
                  runtime_wallet_registry.Snapshot().registry().topology(),
                  scenario_workload.resource_pressure, action_index,
                  action_count, operation_stop_token);
              break;
            case WorkloadKind::kSetNetworkCondition: {
              const NetworkConditionWorkload& workload =
                  scenario_workload.network_condition;
              NodeRuntime& node = nodes[workload.node - 1U];
              QdiscInfo qdisc;
              NodeVethConfig updated_network;
              {
                std::lock_guard<std::mutex> lock(node_network_state_mutex);
                qdisc = ReplaceNodeNetworkConditionTransactional(
                    &node, workload.condition, operation_stop_token);
                try {
                  updated_network = *node.network;
                } catch (...) {
                  ThrowWorkloadMutationOutcomeUnconfirmed(
                      "network condition update completed without coherent "
                      "runtime evidence",
                      std::current_exception());
                }
              }
              try {
                WriteEvent(
                    events_path, options.run_id, node.config.id,
                    SimulationEventKind::kNetworkConditionUpdated,
                    NetworkConditionVerificationDetail(
                        updated_network, qdisc, action_index, action_count));
              } catch (...) {
                ThrowWorkloadMutationOutcomeUnconfirmed(
                    "network condition update completed without a publishable "
                    "outcome",
                    std::current_exception());
              }
              break;
            }
            case WorkloadKind::kBlockNetworkFlow:
            case WorkloadKind::kUnblockNetworkFlow: {
              const NetworkBlockRule& rule =
                  scenario_workload.network_block.rule;
              NodeRuntime& node = nodes[rule.node_index];
              NetworkBlockMutationResult result;
              {
                std::lock_guard<std::mutex> lock(node_network_state_mutex);
                result = MutateNetworkBlockRuleTransactional(
                    node, rule,
                    scenario_workload.kind == WorkloadKind::kUnblockNetworkFlow,
                    operation_stop_token);
              }
              try {
                WriteEvent(
                    events_path, options.run_id, node.config.id,
                    scenario_workload.kind == WorkloadKind::kUnblockNetworkFlow
                        ? SimulationEventKind::kNetworkBlockRemoved
                        : SimulationEventKind::kNetworkBlockApplied,
                    NetworkBlockRuleDetail(node, rule, result.existed_before,
                                           result.present_after, action_index,
                                           action_count));
              } catch (...) {
                ThrowWorkloadMutationOutcomeUnconfirmed(
                    "network flow mutation completed without a publishable "
                    "outcome",
                    std::current_exception());
              }
              break;
            }
            case WorkloadKind::kPartitionNodes:
              ApplyRuntimeNetworkPartition(
                  options, events_path, nodes, node_network_state_mutex,
                  scenario_workload.network_partition.partition, false,
                  action_index, action_count, operation_stop_token);
              break;
            case WorkloadKind::kHealPartition:
              ApplyRuntimeNetworkPartition(
                  options, events_path, nodes, node_network_state_mutex,
                  scenario_workload.network_partition.partition, true,
                  action_index, action_count, operation_stop_token);
              break;
            case WorkloadKind::kSetEdgeCondition:
            case WorkloadKind::kActivateEdge:
            case WorkloadKind::kDeactivateEdge:
            case WorkloadKind::kRestoreEdge: {
              std::lock_guard<std::mutex> topology_lock(runtime_topology_mutex);
              ApplyTopologyEdgeWorkload(
                  options, events_path, chain_spec, driver,
                  *peer_connectivity_controller, *runtime_topology, nodes,
                  node_network_state_mutex, scenario_workload.topology_edge,
                  scenario_workload.kind, action_index, action_count,
                  operation_stop_token);
              break;
            }
            case WorkloadKind::kSendRawTransaction:
              ApplySendRawTransactionWorkload(
                  options, events_path, driver, block_generation_mutex, nodes,
                  transaction_tracker, scenario_workload.send_raw_transaction,
                  action_index, action_count, operation_stop_token,
                  cancellation_commit_control);
              break;
            case WorkloadKind::kCheckpoint: {
              const CheckpointWorkload& workload = scenario_workload.checkpoint;
              const std::string name =
                  workload.name.empty()
                      ? "checkpoint-" + std::to_string(action_index)
                      : workload.name;
              transaction_tracker.ObserveAll(options, events_path, driver,
                                             nodes, operation_stop_token);
              const RuntimeWalletSnapshot checkpoint_registry =
                  runtime_wallet_registry.Snapshot();
              ThrowIfStopRequested(operation_stop_token);
              try {
                const std::uint32_t node_metric_samples = WriteMetricsSnapshot(
                    metrics_path, options, driver, nodes, run_process_state,
                    {node_network_state_mutex, node_resource_state_mutex}, {},
                    {}, operation_stop_token,
                    &checkpoint_registry.registry().topology());
                const std::uint32_t wallet_metric_samples =
                    WriteWalletMetricsSnapshot(wallet_metrics_path, options,
                                               driver, nodes,
                                               checkpoint_registry.registry(),
                                               {}, operation_stop_token);
                WriteEvent(events_path, options.run_id, "sim",
                           SimulationEventKind::kCheckpointRecorded,
                           CheckpointWorkloadDetail(action_index, action_count,
                                                    name, node_metric_samples,
                                                    wallet_metric_samples));
              } catch (const SimulationCancelled&) {
                throw;
              } catch (...) {
                ThrowWorkloadMutationOutcomeUnconfirmed(
                    "checkpoint did not reach a publishable completion "
                    "boundary",
                    std::current_exception());
              }
              break;
            }
            case WorkloadKind::kBlockGeneration:
            case WorkloadKind::kWaitUntilHeight:
            case WorkloadKind::kWaitForPeers:
            case WorkloadKind::kWalletTransactions:
            case WorkloadKind::kCount:
              throw std::logic_error(
                  "one-shot workload kind has no production dispatcher");
          }
        };

    const auto execute_one_shot_workload =
        [&](const ScenarioWorkload& scenario_workload,
            std::uint32_t action_index, std::uint32_t action_count,
            std::stop_token operation_stop_token) {
          auto one_shot_lock = AcquireNodeMutationLock(one_shot_workload_mutex,
                                                       operation_stop_token);
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
          dispatch_one_shot_workload(scenario_workload, current_nodes,
                                     action_index, action_count,
                                     operation_stop_token);
        };

    const auto invoke_one_shot_workload =
        [&](const boost::json::object& workload,
            std::stop_token operation_stop_token) {
          SimulationCommandControl cancellation_commit_control;
          std::atomic_bool interrupt_after_mutation_admission = false;
          std::stop_callback cancel_before_irreversible_commit(
              operation_stop_token, [&] {
                const bool cancellation_won =
                    cancellation_commit_control.RequestCancellation(
                        SimulationCommandCancellationCause::kClientCancel);
                if (!cancellation_won &&
                    interrupt_after_mutation_admission.load(
                        std::memory_order_acquire)) {
                  cancellation_commit_control.stop_source.request_stop();
                }
              });
          const std::stop_token invocation_stop_token =
              cancellation_commit_control.stop_source.get_token();
          auto one_shot_lock = AcquireNodeMutationLock(one_shot_workload_mutex,
                                                       invocation_stop_token);
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       invocation_stop_token);
          const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
          const RuntimeWalletSnapshot current_roles =
              runtime_wallet_registry.Snapshot();
          ScenarioWorkload parsed = ParseAndValidateOneShotWorkload(
              workload,
              RuntimeOneShotWorkloadValidationOptions(
                  options, current_nodes, current_roles, live_topology_config));
          interrupt_after_mutation_admission.store(
              parsed.kind == WorkloadKind::kConnectPeer ||
                  parsed.kind == WorkloadKind::kDisconnectPeer ||
                  parsed.kind == WorkloadKind::kSendRawTransaction,
              std::memory_order_release);
          if (next_one_shot_invocation ==
              std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error(
                "one-shot workload invocation identity exhausted");
          }
          const std::string invocation_id =
              "workload-invocation-" +
              std::to_string(next_one_shot_invocation++);
          if (parsed.kind == WorkloadKind::kCheckpoint &&
              parsed.checkpoint.name.empty()) {
            parsed.checkpoint.name = invocation_id;
          }
          const std::string action(WorkloadKindName(parsed.kind));
          boost::json::object completed_result{
              {"result_family", "workload_invocation"},
              {"run_id", options.run_id},
              {"invocation_id", invocation_id},
              {"action", action},
              {"state", "completed"},
          };
          const auto irreversible_commit_started = [&] {
            const SimulationCommandCommitPhase commit_phase =
                cancellation_commit_control.CommitPhase();
            return commit_phase ==
                       SimulationCommandCommitPhase::kCommitStarted ||
                   commit_phase == SimulationCommandCommitPhase::kCommitted;
          };
          const auto throw_outcome_unconfirmed =
              [&](std::string_view evidence = {}) -> void {
            mcp_application.MarkRunStopping();
            request_simulation_stop();
            std::string message =
                action +
                " did not reach its authoritative completion boundary "
                "after workload side effects may have begun; run stop was "
                "requested and blind retry is unsafe";
            if (!evidence.empty()) {
              message += ": " + std::string(evidence);
            }
            boost::json::object diagnostic{
                {"code", "workload_invocation_outcome_unconfirmed"},
                {"message", message},
                {"path", invocation_id},
                {"action", action},
                {"state", "indeterminate"},
                {"recoverable", false}};
            if (parsed.kind == WorkloadKind::kRestartNode) {
              diagnostic["node_id"] = parsed.restart_node.node_id;
              diagnostic["phase"] = SimulationNodeRestartPhaseName(
                  cancellation_commit_control.restart_phase.load(
                      std::memory_order_acquire));
            }
            throw McpOperationFailure(
                "workload_invocation_outcome_unconfirmed", message, false,
                boost::json::array{std::move(diagnostic)});
          };
          try {
            dispatch_one_shot_workload(parsed, current_nodes, 1U, 1U,
                                       invocation_stop_token,
                                       &cancellation_commit_control);
            SimulationCommandCommitPhase phase =
                cancellation_commit_control.CommitPhase();
            if (phase == SimulationCommandCommitPhase::kOpen) {
              if (cancellation_commit_control.TryBeginCommit()) {
                phase = SimulationCommandCommitPhase::kCommitStarted;
              } else {
                phase = cancellation_commit_control.CommitPhase();
              }
            }
            if (phase == SimulationCommandCommitPhase::kCommitStarted) {
              cancellation_commit_control.MarkCommitted();
            } else if (phase == SimulationCommandCommitPhase::kCancelled) {
              throw_outcome_unconfirmed(
                  "cancellation was accepted before completion could be "
                  "committed");
            } else if (phase != SimulationCommandCommitPhase::kCommitted) {
              throw std::logic_error(
                  "one-shot workload reached an unknown commit phase");
            }
          } catch (const WorkloadMutationCancelledAfterRollback&) {
            throw SimulationCancelled();
          } catch (const WorkloadMutationFailedAfterRollback& error) {
            throw std::runtime_error(error.what());
          } catch (const WorkloadMutationOutcomeUnconfirmed& error) {
            throw_outcome_unconfirmed(error.what());
          } catch (const PeerMutationOutcomeUnconfirmed& error) {
            throw_outcome_unconfirmed(error.what());
          } catch (const OneShotRawTransactionRejected&) {
            throw;
          } catch (const SimulationCancelled&) {
            if (irreversible_commit_started()) {
              if (parsed.kind == WorkloadKind::kRestartNode &&
                  stop_token.stop_requested()) {
                throw;
              }
              throw_outcome_unconfirmed();
            }
            throw;
          } catch (...) {
            if (irreversible_commit_started()) {
              throw_outcome_unconfirmed();
            }
            throw;
          }
          return completed_result;
        };

    const auto wallet_workload_operation = MakeLiveWalletWorkloadOperation(
        wallet_workloads,
        [&, launch_wallet_workload, runtime_wallet_validation_options](
            const boost::json::object& workload_value,
            std::optional<std::string> requested_id,
            std::stop_token operation_stop_token) {
          auto mutation_lock = AcquireNodeMutationLock(node_mutation_mutex,
                                                       operation_stop_token);
          const Options validation_options =
              runtime_wallet_validation_options();
          const WalletTransactionsWorkload workload =
              ParseAndValidateWalletTransactionsWorkload(workload_value,
                                                         validation_options);
          return launch_wallet_workload(workload, std::move(requested_id));
        },
        [runtime_wallet_validation_options](
            const boost::json::object& workload_value) {
          const Options validation_options =
              runtime_wallet_validation_options();
          return ParseAndValidateWalletTransactionsWorkload(workload_value,
                                                            validation_options);
        },
        [&](const WalletTransactionsWorkload& workload) {
          RuntimeWalletSnapshot wallet_snapshot =
              runtime_wallet_registry.Snapshot();
          Options validation = options;
          validation.topology = wallet_snapshot.registry().topology();
          validation.wallet_backed_workload_requested =
              !wallet_snapshot.wallets().empty();
          ValidateWalletTransactionsWorkload(workload, validation);
          return wallet_snapshot;
        });

    workload_service->operation = [&, wallet_workload_operation,
                                   invoke_one_shot_workload,
                                   find_block_generation_workload_record,
                                   block_generation_operation,
                                   find_wait_until_height_workload_record,
                                   wait_until_height_operation,
                                   find_wait_for_peers_workload_record,
                                   wait_for_peers_operation](
                                      McpOperationKind kind,
                                      const boost::json::object& arguments,
                                      std::stop_token operation_stop_token) {
      if (kind == McpOperationKind::kInvokeWorkload) {
        const boost::json::value* workload = arguments.if_contains("workload");
        if (workload == nullptr || !workload->is_object()) {
          throw std::invalid_argument(
              "workload.invoke requires a workload object");
        }
        return invoke_one_shot_workload(workload->as_object(),
                                        operation_stop_token);
      }
      if (kind == McpOperationKind::kStartWorkload) {
        const boost::json::value* workload_value =
            arguments.if_contains("workload");
        if (workload_value != nullptr && workload_value->is_object()) {
          const boost::json::value* type =
              workload_value->as_object().if_contains("type");
          if (type != nullptr && type->is_string()) {
            if (type->as_string() == "block_generation") {
              return block_generation_operation(kind, arguments,
                                                operation_stop_token);
            }
            if (type->as_string() == "wait_until_height") {
              return wait_until_height_operation(kind, arguments,
                                                 operation_stop_token);
            }
            if (type->as_string() == "wait_for_peers") {
              return wait_for_peers_operation(kind, arguments,
                                              operation_stop_token);
            }
          }
        }
      } else {
        const boost::json::value* workload_id =
            arguments.if_contains("workload_id");
        if (workload_id != nullptr && workload_id->is_string()) {
          if (find_block_generation_workload_record(workload_id->as_string())) {
            return block_generation_operation(kind, arguments,
                                              operation_stop_token);
          }
          if (find_wait_until_height_workload_record(
                  workload_id->as_string())) {
            return wait_until_height_operation(kind, arguments,
                                               operation_stop_token);
          }
          if (find_wait_for_peers_workload_record(workload_id->as_string())) {
            return wait_for_peers_operation(kind, arguments,
                                            operation_stop_token);
          }
        }
      }
      return wallet_workload_operation(kind, arguments, operation_stop_token);
    };
    workload_service->read =
        [wallet_workloads, block_generation_workloads,
         wait_until_height_workloads, wait_for_peers_workloads](
            bool history, std::stop_token read_stop_token) {
          return ReadLiveWorkloads(wallet_workloads, block_generation_workloads,
                                   wait_until_height_workloads,
                                   wait_for_peers_workloads, history,
                                   read_stop_token);
        };
    installed_workload_service = workload_service;
    mcp_application.SetWorkloadService(workload_service);
    workload_service.reset();

    auto role_service = std::make_shared<McpLiveRoleService>();
    role_service->operation = [&](McpOperationKind kind,
                                  const boost::json::object& arguments,
                                  std::stop_token operation_stop_token) {
      if (kind == McpOperationKind::kAddMasternode ||
          kind == McpOperationKind::kRemoveMasternode ||
          kind == McpOperationKind::kRestartMasternode) {
        const bool adding = kind == McpOperationKind::kAddMasternode;
        const bool removing = kind == McpOperationKind::kRemoveMasternode;
        const std::string action = std::string(McpOperationKindName(kind));
        if (adding) {
          constexpr std::array<std::string_view, 6U> kAllowedFields = {
              "run_id",       "node_ids",          "count",
              "create_nodes", "funding_wallet_id", "timeout_sec"};
          RejectUnsupportedFields(arguments, kAllowedFields, action);
        } else {
          constexpr std::array<std::string_view, 3U> kAllowedFields = {
              "run_id", "node_ids", "timeout_sec"};
          RejectUnsupportedFields(arguments, kAllowedFields, action);
        }
        if (!driver.SupportsMasternodes()) {
          const UnsupportedChainOperation error(ChainKindName(options.chain),
                                                action);
          throw McpOperationFailure("unsupported_chain_operation", error.what(),
                                    false);
        }
        if (options.block_production.enabled &&
            options.block_production.mode == MiningMode::kNativeMining) {
          const UnsupportedChainOperation error(
              ChainKindName(options.chain),
              "masternode mutation while native mining is active");
          throw McpOperationFailure("unsupported_chain_operation", error.what(),
                                    false);
        }

        const std::uint32_t timeout_sec =
            JsonOptionalUint32Field(arguments, "timeout_sec", 60U);
        if (timeout_sec == 0U || timeout_sec > 3600U) {
          throw std::invalid_argument(action +
                                      " timeout_sec must be in 1..3600");
        }
        const std::uint32_t count =
            adding ? JsonOptionalUint32Field(arguments, "count", 0U) : 0U;
        if (adding && (count == 0U || count > kSimulationNodeAddMaximumCount)) {
          throw std::invalid_argument("masternode.add count must be in 1..16");
        }
        std::vector<std::string> requested_node_ids;
        if (const boost::json::value* node_ids =
                arguments.if_contains("node_ids")) {
          if (!node_ids->is_array()) {
            throw std::invalid_argument(action + " node_ids must be an array");
          }
          std::set<std::string> unique_node_ids;
          requested_node_ids.reserve(node_ids->as_array().size());
          for (const boost::json::value& node_id : node_ids->as_array()) {
            if (!node_id.is_string()) {
              throw std::invalid_argument(action +
                                          " node_ids must contain strings");
            }
            std::string id(node_id.as_string());
            RequireSafeScenarioIdentifier(id, action + " node_ids");
            if (!unique_node_ids.insert(id).second) {
              throw std::invalid_argument(action + " node_ids must be unique");
            }
            requested_node_ids.push_back(std::move(id));
          }
        }
        if (adding && !requested_node_ids.empty() &&
            requested_node_ids.size() != count) {
          throw std::invalid_argument(
              "masternode.add count must match node_ids size");
        }
        if (!adding && requested_node_ids.empty()) {
          throw std::invalid_argument(action + " node_ids must not be empty");
        }
        const boost::json::value* create_nodes =
            adding ? arguments.if_contains("create_nodes") : nullptr;
        if (create_nodes != nullptr && !create_nodes->is_object()) {
          throw std::invalid_argument(
              "masternode.add create_nodes must be an object");
        }
        if (create_nodes != nullptr && !requested_node_ids.empty()) {
          throw std::invalid_argument(
              "masternode.add node_ids and create_nodes are mutually "
              "exclusive");
        }
        std::string funding_wallet_id;
        if (adding) {
          funding_wallet_id = JsonOptionalStringField(
              arguments, "funding_wallet_id", std::string_view());
          RequireSafeScenarioIdentifier(funding_wallet_id,
                                        "masternode.add funding_wallet_id");
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(timeout_sec);
        std::stop_source bounded_stop_source;
        std::atomic_bool deadline_expired = false;
        std::stop_callback stop_on_operation(
            operation_stop_token,
            [&bounded_stop_source] { bounded_stop_source.request_stop(); });
        std::jthread deadline_timer(
            [deadline, &bounded_stop_source,
             &deadline_expired](std::stop_token timer_stop_token) {
              try {
                WaitUntil(deadline, timer_stop_token);
              } catch (const SimulationCancelled&) {
                return;
              }
              deadline_expired.store(true, std::memory_order_release);
              bounded_stop_source.request_stop();
            });
        const std::stop_token bounded_stop_token =
            bounded_stop_source.get_token();
        ThrowIfStopRequested(bounded_stop_token);
        auto mutation_lock =
            AcquireNodeMutationLock(node_mutation_mutex, bounded_stop_token);
        RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
        const RuntimeWalletSnapshot before_roles =
            runtime_wallet_registry.Snapshot();
        const NodeRoleTopology& before_topology =
            before_roles.registry().topology();

        if (adding && create_nodes != nullptr) {
          if (before_roles.registry().wallet_initialization().mode !=
              WalletPrivacyMode::kPublic) {
            throw McpOperationFailure(
                "wallet_mode_conflict",
                "masternode.add requires the active run public wallet mode",
                false);
          }
          Options validation_options = options;
          validation_options.nodes =
              static_cast<std::uint32_t>(current_nodes.size());
          SimulationNodeAddRequest node_request;
          try {
            node_request = ParseAndValidateSimulationNodeAddRequest(
                create_nodes->as_object(), validation_options);
          } catch (const std::runtime_error& error) {
            if (std::string_view(error.what()) !=
                "node.add request exceeds the configured node capacity") {
              throw;
            }
            throw McpOperationFailure(
                "node_capacity_exceeded", error.what(), false,
                boost::json::array{boost::json::object{
                    {"code", "node_capacity_exceeded"},
                    {"message",
                     "the requested masternode batch exceeds available "
                     "capacity"},
                    {"path", "create_nodes.count"},
                    {"requested_count", count},
                    {"current_node_count", current_nodes.size()},
                    {"node_capacity", node_inventory.capacity()},
                    {"available_node_capacity",
                     current_nodes.size() <= node_inventory.capacity()
                         ? node_inventory.capacity() - current_nodes.size()
                         : 0U},
                    {"recoverable", false}}});
          }
          if (node_request.count != count) {
            throw std::invalid_argument(
                "masternode.add count must match create_nodes.count");
          }
          SimulationCommandControl role_control;
          role_control.absolute_deadline = deadline;
          std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
            static_cast<void>(role_control.RequestCancellation(
                deadline_expired.load(std::memory_order_acquire)
                    ? SimulationCommandCancellationCause::kDeadline
                    : SimulationCommandCancellationCause::kClientCancel));
          });
          RuntimeMasternodeAddContext masternode_context{
              .funding_wallet_node_id = funding_wallet_id,
              .block_generation_mutex = &block_generation_mutex,
          };
          RuntimeNodeAddResult added;
          try {
            std::lock_guard<std::mutex> topology_lock(runtime_topology_mutex);
            added = AddRuntimeNodesTransactional(
                options, run_root, events_path, chain_spec, driver,
                node_inventory, runtime_wallet_registry,
                RuntimeNodeAdditionRole::kMasternode, block_scheduler.get(),
                &miner_node_ids, &configured_miner_node_ids_mutex,
                &masternode_context, *peer_connectivity_controller,
                &runtime_topology, &live_topology_config, run_process_state,
                lifecycle_epoch, node_request, &role_control,
                bounded_stop_token, runtime_node_addition_dependencies);
          } catch (const SimulationNodeResourceUnavailable& error) {
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            const SimulationNodeResourceFailure& failure = error.failure();
            throw McpOperationFailure(
                "node_resource_unavailable", error.what(), true,
                boost::json::array{boost::json::object{
                    {"code", "node_resource_unavailable"},
                    {"message", error.what()},
                    {"path", "create_nodes"},
                    {"resource_kind", failure.resource_kind},
                    {"node_id", failure.node_id},
                    {"address", failure.address},
                    {"port", failure.port},
                    {"purpose", failure.purpose},
                    {"mutation_started", failure.mutation_started},
                    {"action", action},
                    {"recoverable", true}}});
          } catch (const SimulationCommandOutcomeUnconfirmed& error) {
            mcp_application.MarkRunStopping();
            request_simulation_stop();
            throw McpOperationFailure(
                "masternode_add_outcome_unconfirmed",
                "masternode.add create_nodes outcome is unconfirmed: " +
                    std::string(error.what()),
                false);
          } catch (...) {
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            throw;
          }
          try {
            if (!added.role_generation || !added.final_masternode_count ||
                added.added_node_ids.size() != count ||
                added.added_masternodes.size() != count) {
              throw std::logic_error(
                  "masternode.add create_nodes omitted its joint "
                  "publication evidence");
            }
            boost::json::array node_ids;
            boost::json::array created_node_ids;
            boost::json::array masternodes;
            for (const std::string& node_id : added.added_node_ids) {
              node_ids.emplace_back(node_id);
              created_node_ids.emplace_back(node_id);
            }
            for (const MasternodeIdentity& masternode :
                 added.added_masternodes) {
              masternodes.push_back(RuntimeMasternodeIdentityJson(masternode));
            }
            return boost::json::object{
                {"node_ids", std::move(node_ids)},
                {"assigned_roles", boost::json::array{"masternode"}},
                {"removed_roles", boost::json::array{}},
                {"action", action},
                {"state", "ready"},
                {"created_node_ids", std::move(created_node_ids)},
                {"role_generation", *added.role_generation},
                {"final_masternode_count", *added.final_masternode_count},
                {"masternodes", std::move(masternodes)},
                {"inventory_generation", added.inventory_generation},
                {"final_node_count", added.final_node_count},
            };
          } catch (...) {
            mcp_application.MarkRunStopping();
            request_simulation_stop();
            throw McpOperationFailure(
                "masternode_add_outcome_unconfirmed",
                "masternode.add create_nodes published but completion "
                "evidence failed: " +
                    ExceptionMessage(std::current_exception()),
                false);
          }
        }

        RuntimeNodePointers active_nodes;
        active_nodes.reserve(current_nodes.size());
        for (NodeRuntime& node : current_nodes) {
          active_nodes.push_back(&node);
        }
        const auto find_node_index = [&](std::string_view node_id) {
          const auto found =
              std::find_if(current_nodes.begin(), current_nodes.end(),
                           [&](const NodeRuntime& node) {
                             return node.config.id == node_id;
                           });
          if (found == current_nodes.end()) {
            throw McpOperationFailure(
                "node_not_found",
                action + " node is not active: " + std::string(node_id), false);
          }
          return static_cast<std::size_t>(
              std::distance(current_nodes.begin(), found));
        };
        const auto find_wallet =
            [&](std::string_view node_id) -> const WalletIdentity& {
          const auto found = std::find_if(before_roles.wallets().begin(),
                                          before_roles.wallets().end(),
                                          [&](const WalletIdentity& wallet) {
                                            return wallet.node_id == node_id;
                                          });
          if (found == before_roles.wallets().end()) {
            throw McpOperationFailure(
                "funding_wallet_not_found",
                action + " funding wallet is not registered: " +
                    std::string(node_id),
                false);
          }
          return *found;
        };
        const auto running_miner_index = [&]() -> std::size_t {
          for (const std::uint32_t miner : before_topology.miner_nodes) {
            if (miner < current_nodes.size() &&
                current_nodes[miner].AllowsChainMetrics() &&
                NodeProcessRunning(current_nodes[miner])) {
              return miner;
            }
          }
          throw McpOperationFailure(
              "miner_unavailable",
              action + " requires a running configured miner", false);
        };
        const auto public_identities_json =
            [](const std::vector<MasternodeIdentity>& identities) {
              boost::json::array result;
              result.reserve(identities.size());
              for (const MasternodeIdentity& identity : identities) {
                result.push_back(RuntimeMasternodeIdentityJson(identity));
              }
              return result;
            };
        const auto node_ids_json =
            [](const std::vector<std::string>& node_ids) {
              boost::json::array result;
              result.reserve(node_ids.size());
              for (const std::string& node_id : node_ids) {
                result.emplace_back(node_id);
              }
              return result;
            };

        if (adding) {
          SimulationCommandControl role_control;
          role_control.absolute_deadline = deadline;
          std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
            static_cast<void>(role_control.RequestCancellation(
                deadline_expired.load(std::memory_order_acquire)
                    ? SimulationCommandCancellationCause::kDeadline
                    : SimulationCommandCancellationCause::kClientCancel));
          });
          if (before_roles.registry().wallet_initialization().mode !=
              WalletPrivacyMode::kPublic) {
            throw McpOperationFailure(
                "wallet_mode_conflict",
                "masternode.add requires the active run public wallet mode",
                false);
          }
          const WalletIdentity& funding_wallet = find_wallet(funding_wallet_id);
          if (funding_wallet.node == 0U ||
              funding_wallet.node > current_nodes.size() ||
              funding_wallet.funding_address.empty()) {
            throw std::logic_error(
                "masternode funding wallet identity is incomplete");
          }
          NodeRuntime& funding_node = current_nodes[funding_wallet.node - 1U];
          RequireNodeRunning(funding_node, "masternode.add funding wallet");
          if (!funding_node.config.wallet_enabled) {
            throw McpOperationFailure(
                "wallet_support_unavailable",
                "masternode.add funding node was started without wallet "
                "support: " +
                    funding_node.config.id,
                false);
          }
          NodeRuntime& miner = current_nodes[running_miner_index()];
          std::vector<std::size_t> selected_indexes;
          selected_indexes.reserve(count);
          if (!requested_node_ids.empty()) {
            for (const std::string& node_id : requested_node_ids) {
              selected_indexes.push_back(find_node_index(node_id));
            }
          } else {
            for (std::size_t index = 0U; index < current_nodes.size() &&
                                         selected_indexes.size() < count;
                 ++index) {
              if (!NodeListContains(before_topology.masternode_nodes,
                                    static_cast<std::uint32_t>(index)) &&
                  !current_nodes[index].config.masternode &&
                  current_nodes[index].AllowsChainMetrics() &&
                  NodeProcessRunning(current_nodes[index])) {
                selected_indexes.push_back(index);
              }
            }
            if (selected_indexes.size() != count) {
              throw McpOperationFailure(
                  "masternode_backing_node_unavailable",
                  "masternode.add found fewer compatible running nodes than "
                  "requested",
                  false);
            }
          }
          std::vector<std::string> selected_node_ids;
          selected_node_ids.reserve(selected_indexes.size());
          {
            auto process_guard = run_process_state.Lock();
            for (const std::size_t index : selected_indexes) {
              if (NodeListContains(before_topology.masternode_nodes,
                                   static_cast<std::uint32_t>(index)) ||
                  current_nodes[index].config.masternode) {
                throw McpOperationFailure(
                    "masternode_already_configured",
                    "masternode.add node is already a masternode: " +
                        current_nodes[index].config.id,
                    false);
              }
              RequireNodeRunning(current_nodes[index], process_guard,
                                 "masternode.add");
              selected_node_ids.push_back(current_nodes[index].config.id);
            }
          }
          const ChainMasternodeFundingRequirements requirements =
              driver.MasternodeFundingRequirements(count);
          const auto operation_timeout = std::chrono::seconds(timeout_sec);
          static_cast<void>(PrepareMasternodeFunding(
              driver, block_generation_mutex, miner, funding_node,
              funding_wallet.funding_address, active_nodes, requirements,
              operation_timeout, bounded_stop_token));

          const auto completion_deadline =
              std::chrono::steady_clock::now() +
              std::chrono::seconds(std::max<std::uint32_t>(60U, timeout_sec));
          std::stop_source completion_stop_source;
          std::jthread completion_timer(
              [completion_deadline,
               &completion_stop_source](std::stop_token timer_stop_token) {
                try {
                  WaitUntil(completion_deadline, timer_stop_token);
                } catch (const SimulationCancelled&) {
                  return;
                }
                completion_stop_source.request_stop();
              });
          const std::stop_token completion_stop_token =
              completion_stop_source.get_token();
          std::vector<ChainMasternodeRegistration> registrations;
          std::vector<MasternodeIdentity> added_masternodes;
          std::vector<bool> scheduled_miner_was_active(selected_indexes.size(),
                                                       false);
          registrations.reserve(selected_indexes.size());
          added_masternodes.reserve(selected_indexes.size());
          const auto rollback_registration = [&] {
            const auto rollback_deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(60);
            std::stop_source rollback_stop_source;
            std::jthread rollback_timer(
                [rollback_deadline,
                 &rollback_stop_source](std::stop_token timer_stop_token) {
                  try {
                    WaitUntil(rollback_deadline, timer_stop_token);
                  } catch (const SimulationCancelled&) {
                    return;
                  }
                  rollback_stop_source.request_stop();
                });
            const std::stop_token rollback_stop_token =
                rollback_stop_source.get_token();
            const auto resume_scheduled_miners = [&] {
              if (block_scheduler == nullptr) {
                return;
              }
              for (std::size_t offset = 0U; offset < selected_indexes.size();
                   ++offset) {
                if (!scheduled_miner_was_active[offset]) {
                  continue;
                }
                NodeRuntime& node = current_nodes[selected_indexes[offset]];
                if (!NodeProcessRunning(node)) {
                  continue;
                }
                block_scheduler->StartMiner(node.config.id);
                scheduled_miner_was_active[offset] = false;
              }
            };
            try {
              for (std::size_t offset = 0U; offset < selected_indexes.size();
                   ++offset) {
                const std::size_t index = selected_indexes[offset];
                NodeRuntime& node = current_nodes[index];
                if (!node.config.masternode) {
                  continue;
                }
                const bool stopped_miner =
                    block_scheduler != nullptr &&
                    NodeListContains(before_topology.miner_nodes,
                                     static_cast<std::uint32_t>(index)) &&
                    block_scheduler->StopMiner(node.config.id);
                scheduled_miner_was_active[offset] =
                    scheduled_miner_was_active[offset] || stopped_miner;
                node.config.masternode.reset();
                if (!RestartNode(options, events_path, driver,
                                 *peer_connectivity_controller, node,
                                 lifecycle_epoch, rollback_stop_token,
                                 "masternode_add_rollback")) {
                  throw std::runtime_error(
                      "masternode rollback reached node stop_time: " +
                      node.config.id);
                }
              }

              NodeRuntime& rollback_miner =
                  current_nodes[running_miner_index()];
              std::vector<MasternodeTransactionConfirmation>
                  registration_transactions;
              registration_transactions.reserve(registrations.size());
              for (const ChainMasternodeRegistration& registration :
                   registrations) {
                registration_transactions.push_back(
                    MasternodeTransactionConfirmation{
                        .funding_wallet = &funding_node,
                        .transaction_id = registration.pro_tx_hash,
                    });
              }
              ConfirmMasternodeTransactions(
                  driver, block_generation_mutex, rollback_miner,
                  funding_wallet.funding_address, active_nodes,
                  registration_transactions,
                  requirements.registration_confirmation_blocks,
                  std::chrono::seconds(60), rollback_stop_token);

              std::vector<MasternodeTransactionConfirmation> revocations;
              revocations.reserve(registrations.size());
              for (const ChainMasternodeRegistration& registration :
                   registrations) {
                revocations.push_back(MasternodeTransactionConfirmation{
                    .funding_wallet = &funding_node,
                    .transaction_id = driver.RevokeMasternode(
                        funding_node.config, registration.pro_tx_hash,
                        registration.operator_secret_key,
                        funding_wallet.funding_address, rollback_stop_token),
                });
              }
              ConfirmMasternodeTransactions(
                  driver, block_generation_mutex, rollback_miner,
                  funding_wallet.funding_address, active_nodes, revocations,
                  requirements.revocation_confirmation_blocks,
                  std::chrono::seconds(60), rollback_stop_token);
            } catch (...) {
              const std::exception_ptr rollback_failure =
                  std::current_exception();
              try {
                resume_scheduled_miners();
              } catch (...) {
                throw std::runtime_error(
                    "masternode rollback failed: " +
                    ExceptionMessage(rollback_failure) +
                    "; scheduled miner restoration failed: " +
                    ExceptionMessage(std::current_exception()));
              }
              std::rethrow_exception(rollback_failure);
            }
            resume_scheduled_miners();
            const RuntimeWalletSnapshot restored =
                runtime_wallet_registry.Snapshot();
            if (restored.generation() != before_roles.generation() ||
                restored.masternodes().size() !=
                    before_roles.masternodes().size()) {
              throw std::runtime_error(
                  "masternode.add rollback registry read-back changed");
            }
          };
          try {
            for (const std::size_t index : selected_indexes) {
              ThrowIfStopRequested(bounded_stop_token);
              NodeRuntime& node = current_nodes[index];
              const std::string service = node.config.p2p_host + ":" +
                                          std::to_string(node.config.p2p_port);
              registrations.push_back(driver.RegisterMasternode(
                  funding_node.config, service, funding_wallet.funding_address,
                  completion_stop_token));
              ThrowIfStopRequested(bounded_stop_token);
            }
            std::vector<MasternodeTransactionConfirmation>
                registration_transactions;
            registration_transactions.reserve(registrations.size());
            for (const ChainMasternodeRegistration& registration :
                 registrations) {
              registration_transactions.push_back(
                  MasternodeTransactionConfirmation{
                      .funding_wallet = &funding_node,
                      .transaction_id = registration.pro_tx_hash,
                  });
            }
            ConfirmMasternodeTransactions(
                driver, block_generation_mutex, miner,
                funding_wallet.funding_address, active_nodes,
                registration_transactions,
                requirements.registration_confirmation_blocks,
                operation_timeout, completion_stop_token);
            ThrowIfStopRequested(bounded_stop_token);
            for (std::size_t offset = 0U; offset < selected_indexes.size();
                 ++offset) {
              const std::size_t index = selected_indexes[offset];
              NodeRuntime& node = current_nodes[index];
              const ChainMasternodeRegistration& registration =
                  registrations[offset];
              const bool resume_miner =
                  block_scheduler != nullptr &&
                  NodeListContains(before_topology.miner_nodes,
                                   static_cast<std::uint32_t>(index)) &&
                  block_scheduler->StopMiner(node.config.id);
              scheduled_miner_was_active[offset] = resume_miner;
              node.config.masternode = ChainNodeConfig::MasternodeProcessConfig{
                  .operator_secret_key = registration.operator_secret_key,
                  .service = registration.service,
              };
              try {
                if (!RestartNode(options, events_path, driver,
                                 *peer_connectivity_controller, node,
                                 lifecycle_epoch, completion_stop_token,
                                 "masternode_add")) {
                  throw std::runtime_error(
                      "masternode.add target reached stop_time during "
                      "restart: " +
                      node.config.id);
                }
                const ChainMasternodeStatus status =
                    driver.WaitForMasternodeReady(
                        node.config, registration.pro_tx_hash,
                        operation_timeout, completion_stop_token);
                added_masternodes.push_back(RegisteredMasternodeIdentity(
                    static_cast<std::uint32_t>(index + 1U), node.config.id,
                    funding_wallet_id, registration, status));
              } catch (...) {
                if (resume_miner && NodeProcessRunning(node)) {
                  block_scheduler->StartMiner(node.config.id);
                }
                throw;
              }
              if (resume_miner) {
                block_scheduler->StartMiner(node.config.id);
              }
              ThrowIfStopRequested(bounded_stop_token);
            }
          } catch (...) {
            const std::exception_ptr failure = std::current_exception();
            bool registration_outcome_unknown = false;
            try {
              std::rethrow_exception(failure);
            } catch (const ChainMasternodeOutcomeUnknown&) {
              registration_outcome_unknown = true;
            } catch (...) {
            }
            if (!registrations.empty()) {
              try {
                rollback_registration();
              } catch (...) {
                mcp_application.MarkRunStopping();
                request_simulation_stop();
                throw McpOperationFailure(
                    "masternode_add_outcome_unconfirmed",
                    "masternode.add failed after registration: " +
                        ExceptionMessage(failure) +
                        "; rollback could not be verified: " +
                        ExceptionMessage(std::current_exception()),
                    false);
              }
            }
            if (registration_outcome_unknown) {
              mcp_application.MarkRunStopping();
              request_simulation_stop();
              throw McpOperationFailure("masternode_add_outcome_unconfirmed",
                                        ExceptionMessage(failure), false);
            }
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            std::rethrow_exception(failure);
          }

          RuntimeWalletSnapshot published_roles;
          try {
            std::unique_lock<std::timed_mutex> publication_lock =
                AcquireRuntimePublicationLock(bounded_stop_token);
            ThrowIfStopRequested(bounded_stop_token);
            RuntimeWalletRegistry::PreparedAppend prepared_roles =
                runtime_wallet_registry.PrepareUpdate(
                    before_roles.generation(), {}, {}, added_masternodes,
                    static_cast<std::uint32_t>(current_nodes.size()));
            if (!role_control.TryBeginCommit()) {
              throw SimulationCancelled();
            }
            published_roles = prepared_roles.Commit();
            role_control.MarkCommitted();
          } catch (...) {
            const std::exception_ptr failure = std::current_exception();
            try {
              rollback_registration();
            } catch (...) {
              mcp_application.MarkRunStopping();
              request_simulation_stop();
              throw McpOperationFailure(
                  "masternode_add_outcome_unconfirmed",
                  "masternode.add could not publish after registration: " +
                      ExceptionMessage(failure) +
                      "; rollback could not be verified: " +
                      ExceptionMessage(std::current_exception()),
                  false);
            }
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            std::rethrow_exception(failure);
          }
          try {
            WriteEvent(events_path, options.run_id, "sim",
                       SimulationEventKind::kRuntimeRoleGenerationPublished,
                       boost::json::serialize(RuntimeRoleGenerationDetail(
                           published_roles, current_nodes)));
            return boost::json::object{
                {"node_ids", node_ids_json(selected_node_ids)},
                {"assigned_roles", boost::json::array{"masternode"}},
                {"removed_roles", boost::json::array{}},
                {"action", action},
                {"state", "ready"},
                {"created_node_ids", boost::json::array{}},
                {"role_generation", published_roles.generation()},
                {"final_masternode_count",
                 published_roles.masternodes().size()},
                {"masternodes", public_identities_json(added_masternodes)},
                {"inventory_generation", current_nodes.generation()},
                {"final_node_count", current_nodes.size()},
            };
          } catch (...) {
            mcp_application.MarkRunStopping();
            request_simulation_stop();
            throw McpOperationFailure(
                "masternode_add_outcome_unconfirmed",
                "masternode.add published but completion evidence failed: " +
                    ExceptionMessage(std::current_exception()),
                false);
          }
        }

        std::vector<MasternodeIdentity> selected_masternodes;
        std::vector<std::size_t> selected_indexes;
        selected_masternodes.reserve(requested_node_ids.size());
        selected_indexes.reserve(requested_node_ids.size());
        for (const std::string& node_id : requested_node_ids) {
          const auto identity =
              std::find_if(before_roles.masternodes().begin(),
                           before_roles.masternodes().end(),
                           [&](const MasternodeIdentity& masternode) {
                             return masternode.node_id == node_id;
                           });
          if (identity == before_roles.masternodes().end()) {
            throw McpOperationFailure(
                "masternode_not_found",
                action + " node has no registered masternode role: " + node_id,
                false);
          }
          const std::size_t index = find_node_index(node_id);
          if (!current_nodes[index].config.masternode ||
              current_nodes[index].config.masternode->operator_secret_key !=
                  identity->operator_secret_key ||
              current_nodes[index].config.masternode->service !=
                  identity->service) {
            throw McpOperationFailure(
                "masternode_configuration_mismatch",
                action +
                    " process configuration does not match the registered "
                    "masternode identity: " +
                    node_id,
                false);
          }
          selected_masternodes.push_back(*identity);
          selected_indexes.push_back(index);
        }

        if (removing) {
          const std::size_t miner_index = running_miner_index();
          NodeRuntime& miner = current_nodes[miner_index];
          const ChainMasternodeFundingRequirements requirements =
              driver.MasternodeFundingRequirements(
                  static_cast<std::uint32_t>(selected_masternodes.size()));
          const auto completion_deadline =
              std::chrono::steady_clock::now() +
              std::chrono::seconds(std::max<std::uint32_t>(60U, timeout_sec));
          std::stop_source completion_stop_source;
          std::jthread completion_timer(
              [completion_deadline,
               &completion_stop_source](std::stop_token timer_stop_token) {
                try {
                  WaitUntil(completion_deadline, timer_stop_token);
                } catch (const SimulationCancelled&) {
                  return;
                }
                completion_stop_source.request_stop();
              });
          const std::stop_token completion_stop_token =
              completion_stop_source.get_token();
          std::vector<MasternodeTransactionConfirmation> revocations;
          revocations.reserve(selected_masternodes.size());
          bool revocation_attempted = false;
          try {
            ThrowIfStopRequested(bounded_stop_token);
            for (const MasternodeIdentity& masternode : selected_masternodes) {
              const WalletIdentity& wallet =
                  find_wallet(masternode.funding_wallet_node_id);
              NodeRuntime& funding_node = current_nodes.at(wallet.node - 1U);
              RequireNodeRunning(funding_node, "masternode.remove funding");
              revocation_attempted = true;
              revocations.push_back(MasternodeTransactionConfirmation{
                  .funding_wallet = &funding_node,
                  .transaction_id = driver.RevokeMasternode(
                      funding_node.config, masternode.pro_tx_hash,
                      masternode.operator_secret_key, wallet.funding_address,
                      completion_stop_token),
              });
            }
            const WalletIdentity& confirmation_wallet = find_wallet(
                selected_masternodes.front().funding_wallet_node_id);
            ConfirmMasternodeTransactions(
                driver, block_generation_mutex, miner,
                confirmation_wallet.funding_address, active_nodes, revocations,
                requirements.revocation_confirmation_blocks,
                std::chrono::seconds(timeout_sec), completion_stop_token);
            for (const std::size_t index : selected_indexes) {
              NodeRuntime& node = current_nodes[index];
              const bool resume_miner =
                  block_scheduler != nullptr &&
                  NodeListContains(before_topology.miner_nodes,
                                   static_cast<std::uint32_t>(index)) &&
                  block_scheduler->StopMiner(node.config.id);
              node.config.masternode.reset();
              try {
                if (!RestartNode(options, events_path, driver,
                                 *peer_connectivity_controller, node,
                                 lifecycle_epoch, completion_stop_token,
                                 "masternode_remove")) {
                  throw std::runtime_error(
                      "masternode.remove target reached stop_time during "
                      "restart: " +
                      node.config.id);
                }
              } catch (...) {
                if (resume_miner && NodeProcessRunning(node)) {
                  block_scheduler->StartMiner(node.config.id);
                }
                throw;
              }
              if (resume_miner) {
                block_scheduler->StartMiner(node.config.id);
              }
            }
            SimulationRegistry next_registry = before_roles.registry();
            std::vector<std::uint32_t> removed_indexes;
            removed_indexes.reserve(selected_indexes.size());
            for (const std::size_t index : selected_indexes) {
              removed_indexes.push_back(static_cast<std::uint32_t>(index));
            }
            next_registry.RemoveMasternodeNodes(removed_indexes);
            std::unique_lock<std::timed_mutex> publication_lock =
                AcquireRuntimePublicationLock(completion_stop_token);
            RuntimeWalletRegistry::PreparedAppend prepared_roles =
                runtime_wallet_registry.PrepareReplace(
                    before_roles.generation(), std::move(next_registry));
            const RuntimeWalletSnapshot published_roles =
                prepared_roles.Commit();
            for (MasternodeIdentity& masternode : selected_masternodes) {
              masternode.state = "REVOKED";
              masternode.status = "revoked";
            }
            WriteEvent(events_path, options.run_id, "sim",
                       SimulationEventKind::kRuntimeRoleGenerationPublished,
                       boost::json::serialize(RuntimeRoleGenerationDetail(
                           published_roles, current_nodes)));
            return boost::json::object{
                {"node_ids", node_ids_json(requested_node_ids)},
                {"assigned_roles", boost::json::array{}},
                {"removed_roles", boost::json::array{"masternode"}},
                {"action", action},
                {"state", "removed"},
                {"created_node_ids", boost::json::array{}},
                {"role_generation", published_roles.generation()},
                {"final_masternode_count",
                 published_roles.masternodes().size()},
                {"masternodes", public_identities_json(selected_masternodes)},
                {"inventory_generation", current_nodes.generation()},
                {"final_node_count", current_nodes.size()},
            };
          } catch (...) {
            const std::exception_ptr failure = std::current_exception();
            if (revocation_attempted) {
              mcp_application.MarkRunStopping();
              request_simulation_stop();
              throw McpOperationFailure(
                  "masternode_remove_outcome_unconfirmed",
                  "masternode.remove failed after revocation began: " +
                      ExceptionMessage(failure),
                  false);
            }
            std::rethrow_exception(failure);
          }
        }

        const auto completion_deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(std::max<std::uint32_t>(60U, timeout_sec));
        std::stop_source completion_stop_source;
        std::jthread completion_timer(
            [completion_deadline,
             &completion_stop_source](std::stop_token timer_stop_token) {
              try {
                WaitUntil(completion_deadline, timer_stop_token);
              } catch (const SimulationCancelled&) {
                return;
              }
              completion_stop_source.request_stop();
            });
        const std::stop_token completion_stop_token =
            completion_stop_source.get_token();
        std::vector<MasternodeIdentity> restarted_masternodes;
        restarted_masternodes.reserve(selected_indexes.size());
        bool restart_mutation_started = false;
        try {
          ThrowIfStopRequested(bounded_stop_token);
          for (std::size_t offset = 0U; offset < selected_indexes.size();
               ++offset) {
            const std::size_t index = selected_indexes[offset];
            NodeRuntime& node = current_nodes[index];
            const bool resume_miner =
                block_scheduler != nullptr &&
                NodeListContains(before_topology.miner_nodes,
                                 static_cast<std::uint32_t>(index)) &&
                block_scheduler->StopMiner(node.config.id);
            NodeRestartAdmission restart_admission;
            try {
              if (!RestartNode(options, events_path, driver,
                               *peer_connectivity_controller, node,
                               lifecycle_epoch, completion_stop_token,
                               "masternode_restart", nullptr,
                               &restart_admission)) {
                throw std::runtime_error(
                    "masternode.restart target reached stop_time during "
                    "restart: " +
                    node.config.id);
              }
              restart_mutation_started =
                  restart_mutation_started || restart_admission.admitted;
              const ChainMasternodeStatus status =
                  driver.WaitForMasternodeReady(
                      node.config, selected_masternodes[offset].pro_tx_hash,
                      std::chrono::seconds(timeout_sec), completion_stop_token);
              if (status.collateral_hash !=
                      selected_masternodes[offset].collateral_hash ||
                  status.collateral_index !=
                      selected_masternodes[offset].collateral_index) {
                throw std::runtime_error(
                    "masternode.restart readiness returned a different "
                    "collateral identity");
              }
              MasternodeIdentity current = selected_masternodes[offset];
              current.state = status.state;
              current.status = status.status;
              restarted_masternodes.push_back(std::move(current));
            } catch (...) {
              restart_mutation_started =
                  restart_mutation_started || restart_admission.admitted;
              if (resume_miner && NodeProcessRunning(node)) {
                block_scheduler->StartMiner(node.config.id);
              }
              throw;
            }
            if (resume_miner) {
              block_scheduler->StartMiner(node.config.id);
            }
          }
          return boost::json::object{
              {"node_ids", node_ids_json(requested_node_ids)},
              {"assigned_roles", boost::json::array{}},
              {"removed_roles", boost::json::array{}},
              {"action", action},
              {"state", "ready"},
              {"created_node_ids", boost::json::array{}},
              {"role_generation", before_roles.generation()},
              {"final_masternode_count", before_roles.masternodes().size()},
              {"masternodes", public_identities_json(restarted_masternodes)},
              {"inventory_generation", current_nodes.generation()},
              {"final_node_count", current_nodes.size()},
          };
        } catch (...) {
          if (!restart_mutation_started) {
            throw;
          }
          const std::exception_ptr failure = std::current_exception();
          mcp_application.MarkRunStopping();
          request_simulation_stop();
          throw McpOperationFailure(
              "masternode_restart_outcome_unconfirmed",
              "masternode.restart failed after process mutation began: " +
                  ExceptionMessage(failure),
              false);
        }
      }
      if (kind == McpOperationKind::kRemoveWallet) {
        constexpr std::array<std::string_view, 4U> kAllowedFields = {
            "run_id", "node_id", "node_ids", "timeout_sec"};
        RejectUnsupportedFields(arguments, kAllowedFields, "wallet.remove");
        const bool has_node_id = arguments.if_contains("node_id") != nullptr;
        const bool has_node_ids = arguments.if_contains("node_ids") != nullptr;
        if (has_node_id == has_node_ids) {
          throw std::invalid_argument(
              "wallet.remove requires exactly one of node_id or node_ids");
        }
        std::vector<std::string> requested_node_ids;
        if (has_node_id) {
          requested_node_ids.push_back(JsonOptionalStringField(
              arguments, "node_id", std::string_view()));
          RequireSafeScenarioIdentifier(requested_node_ids.front(),
                                        "wallet.remove node_id");
        } else {
          const boost::json::value& node_ids = arguments.at("node_ids");
          if (!node_ids.is_array() || node_ids.as_array().empty() ||
              node_ids.as_array().size() > kSimulationNodeRemoveMaximumCount) {
            throw std::invalid_argument(
                "wallet.remove node_ids must contain 1..16 ids");
          }
          std::set<std::string> unique_node_ids;
          requested_node_ids.reserve(node_ids.as_array().size());
          for (const boost::json::value& node_id : node_ids.as_array()) {
            if (!node_id.is_string()) {
              throw std::invalid_argument(
                  "wallet.remove node_ids must contain strings");
            }
            std::string id(node_id.as_string());
            RequireSafeScenarioIdentifier(id, "wallet.remove node_ids");
            if (!unique_node_ids.insert(id).second) {
              throw std::invalid_argument(
                  "wallet.remove node_ids must be unique");
            }
            requested_node_ids.push_back(std::move(id));
          }
        }
        const std::uint32_t timeout_sec =
            JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
        if (timeout_sec == 0U || timeout_sec > 3600U) {
          throw std::invalid_argument(
              "wallet.remove timeout_sec must be in 1..3600");
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(timeout_sec);
        std::stop_source bounded_stop_source;
        std::atomic_bool deadline_expired = false;
        std::stop_callback stop_on_operation(
            operation_stop_token,
            [&bounded_stop_source] { bounded_stop_source.request_stop(); });
        std::jthread deadline_timer(
            [deadline, &bounded_stop_source,
             &deadline_expired](std::stop_token timer_stop_token) {
              try {
                WaitUntil(deadline, timer_stop_token);
              } catch (const SimulationCancelled&) {
                return;
              }
              deadline_expired.store(true, std::memory_order_release);
              bounded_stop_source.request_stop();
            });
        const std::stop_token bounded_stop_token =
            bounded_stop_source.get_token();
        SimulationCommandControl role_control;
        role_control.absolute_deadline = deadline;
        std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
          static_cast<void>(role_control.RequestCancellation(
              deadline_expired.load(std::memory_order_acquire)
                  ? SimulationCommandCancellationCause::kDeadline
                  : SimulationCommandCancellationCause::kClientCancel));
        });
        ThrowIfStopRequested(bounded_stop_token);
        auto mutation_lock =
            AcquireNodeMutationLock(node_mutation_mutex, bounded_stop_token);
        const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
        const RuntimeWalletSnapshot before_wallets =
            runtime_wallet_registry.Snapshot();
        std::vector<std::uint32_t> selected_role_indexes;
        selected_role_indexes.reserve(requested_node_ids.size());
        for (const std::string& requested_node_id : requested_node_ids) {
          const auto selected =
              std::find_if(current_nodes.begin(), current_nodes.end(),
                           [&](const NodeRuntime& node) {
                             return node.config.id == requested_node_id;
                           });
          if (selected == current_nodes.end()) {
            throw McpOperationFailure(
                "node_not_found",
                "wallet.remove node is not active: " + requested_node_id,
                false);
          }
          const std::uint32_t node_index = static_cast<std::uint32_t>(
              std::distance(current_nodes.begin(), selected));
          if (!NodeListContains(
                  before_wallets.registry().topology().wallet_nodes,
                  node_index)) {
            throw McpOperationFailure(
                "wallet_not_found",
                "wallet.remove node has no registered wallet role: " +
                    requested_node_id,
                false);
          }
          selected_role_indexes.push_back(node_index);
        }

        std::vector<WalletIdentity> removed_wallets;
        for (const std::string& requested_node_id : requested_node_ids) {
          const std::size_t previous_size = removed_wallets.size();
          for (const WalletIdentity& wallet : before_wallets.wallets()) {
            if (wallet.node_id == requested_node_id) {
              removed_wallets.push_back(wallet);
            }
          }
          if (removed_wallets.size() == previous_size) {
            throw std::logic_error(
                "wallet.remove role has no registered wallet identities: " +
                requested_node_id);
          }
        }
        for (const MasternodeIdentity& masternode :
             before_wallets.masternodes()) {
          if (std::find(requested_node_ids.begin(), requested_node_ids.end(),
                        masternode.funding_wallet_node_id) !=
              requested_node_ids.end()) {
            throw McpOperationFailure(
                "wallet_in_use",
                "wallet.remove requires removing funded masternode " +
                    masternode.node_id + " first",
                false);
          }
        }

        const auto acquire_workload_lock =
            [&](std::mutex& mutex) -> std::unique_lock<std::mutex> {
          std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
          while (!lock.try_lock()) {
            WaitForDuration(std::chrono::milliseconds(20), bounded_stop_token);
          }
          ThrowIfStopRequested(bounded_stop_token);
          return lock;
        };
        std::unique_lock<std::mutex> workloads_lock =
            acquire_workload_lock(wallet_workloads->mutex);
        for (const auto& [workload_id, record] : wallet_workloads->records) {
          std::unique_lock<std::mutex> record_lock =
              acquire_workload_lock(record->mutex);
          if (!IsTerminalLiveWalletWorkloadState(record->state)) {
            throw McpOperationFailure(
                "wallet_in_use",
                "wallet.remove requires every wallet workload to be terminal; "
                "active workload: " +
                    workload_id,
                true);
          }
        }

        SimulationRegistry next_registry = before_wallets.registry();
        for (const std::uint32_t node_index : selected_role_indexes) {
          next_registry.RemoveWalletNode(node_index);
        }
        std::unique_lock<std::timed_mutex> publication_lock =
            AcquireRuntimePublicationLock(bounded_stop_token);
        ThrowIfStopRequested(bounded_stop_token);
        RuntimeWalletRegistry::PreparedAppend prepared =
            runtime_wallet_registry.PrepareReplace(before_wallets.generation(),
                                                   std::move(next_registry));
        if (std::chrono::steady_clock::now() >= deadline) {
          deadline_expired.store(true, std::memory_order_release);
          bounded_stop_source.request_stop();
        }
        if (!role_control.TryBeginCommit()) {
          throw SimulationCancelled();
        }
        const RuntimeWalletSnapshot published = prepared.Commit();
        role_control.MarkCommitted();
        workloads_lock.unlock();

        try {
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kRuntimeWalletGenerationPublished,
                     boost::json::serialize(RuntimeWalletGenerationDetail(
                         published, std::span<const WalletIdentity>{},
                         removed_wallets)));
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kRuntimeRoleGenerationPublished,
                     boost::json::serialize(RuntimeRoleGenerationDetail(
                         published, current_nodes)));
          boost::json::array wallets;
          wallets.reserve(removed_wallets.size());
          for (const WalletIdentity& wallet : removed_wallets) {
            wallets.push_back(RuntimeWalletIdentityJson(
                wallet, published.registry().wallet_initialization()));
          }
          boost::json::array affected_node_ids;
          affected_node_ids.reserve(requested_node_ids.size());
          for (const std::string& node_id : requested_node_ids) {
            affected_node_ids.emplace_back(node_id);
          }
          return boost::json::object{
              {"added_node_ids", boost::json::array{}},
              {"removed_node_ids", boost::json::array{}},
              {"affected_node_ids", std::move(affected_node_ids)},
              {"action", "wallet.remove"},
              {"state", "removed"},
              {"unchanged", false},
              {"wallets", std::move(wallets)},
              {"inventory_generation", current_nodes.generation()},
              {"final_node_count", current_nodes.size()},
              {"wallet_generation", published.generation()},
              {"final_wallet_count", published.wallets().size()},
              {"final_wallet_node_count",
               published.registry().topology().wallet_nodes.size()},
          };
        } catch (...) {
          mcp_application.MarkRunStopping();
          request_simulation_stop();
          throw McpOperationFailure(
              "wallet_remove_outcome_unconfirmed",
              "wallet.remove published but completion evidence failed: " +
                  ExceptionMessage(std::current_exception()),
              false);
        }
      }
      if (kind == McpOperationKind::kRemoveMiner) {
        constexpr std::array<std::string_view, 3U> kAllowedFields = {
            "run_id", "node_ids", "timeout_sec"};
        RejectUnsupportedFields(arguments, kAllowedFields, "miner.remove");
        const boost::json::value* node_ids = arguments.if_contains("node_ids");
        if (node_ids == nullptr || !node_ids->is_array() ||
            node_ids->as_array().empty() ||
            node_ids->as_array().size() > kMcpMaximumSelectionItems) {
          throw std::invalid_argument(
              "miner.remove node_ids must be a nonempty bounded array");
        }
        std::set<std::string> unique_node_ids;
        std::vector<std::string> requested_node_ids;
        requested_node_ids.reserve(node_ids->as_array().size());
        for (const boost::json::value& node_id : node_ids->as_array()) {
          if (!node_id.is_string()) {
            throw std::invalid_argument(
                "miner.remove node_ids must contain strings");
          }
          std::string id(node_id.as_string());
          RequireSafeScenarioIdentifier(id, "miner.remove node_ids");
          if (!unique_node_ids.insert(id).second) {
            throw std::invalid_argument("miner.remove node_ids must be unique");
          }
          requested_node_ids.push_back(std::move(id));
        }
        const std::uint32_t timeout_sec =
            JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
        if (timeout_sec == 0U || timeout_sec > 3600U) {
          throw std::invalid_argument(
              "miner.remove timeout_sec must be in 1..3600");
        }
        if (options.block_production.enabled &&
            options.block_production.mode == MiningMode::kNativeMining) {
          const UnsupportedChainOperation error(
              ChainKindName(options.chain),
              "transactional runtime native-miner removal");
          throw McpOperationFailure("unsupported_chain_operation", error.what(),
                                    false);
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(timeout_sec);
        std::stop_source bounded_stop_source;
        std::atomic_bool deadline_expired = false;
        std::stop_callback stop_on_operation(
            operation_stop_token,
            [&bounded_stop_source] { bounded_stop_source.request_stop(); });
        std::jthread deadline_timer(
            [deadline, &bounded_stop_source,
             &deadline_expired](std::stop_token timer_stop_token) {
              try {
                WaitUntil(deadline, timer_stop_token);
              } catch (const SimulationCancelled&) {
                return;
              }
              deadline_expired.store(true, std::memory_order_release);
              bounded_stop_source.request_stop();
            });
        const std::stop_token bounded_stop_token =
            bounded_stop_source.get_token();
        SimulationCommandControl role_control;
        role_control.absolute_deadline = deadline;
        std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
          static_cast<void>(role_control.RequestCancellation(
              deadline_expired.load(std::memory_order_acquire)
                  ? SimulationCommandCancellationCause::kDeadline
                  : SimulationCommandCancellationCause::kClientCancel));
        });
        ThrowIfStopRequested(bounded_stop_token);
        auto mutation_lock =
            AcquireNodeMutationLock(node_mutation_mutex, bounded_stop_token);
        const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
        const RuntimeWalletSnapshot before_roles =
            runtime_wallet_registry.Snapshot();
        const NodeRoleTopology& before_topology =
            before_roles.registry().topology();

        std::vector<std::uint32_t> selected_role_indexes;
        selected_role_indexes.reserve(requested_node_ids.size());
        for (const std::string& requested_node_id : requested_node_ids) {
          const auto selected =
              std::find_if(current_nodes.begin(), current_nodes.end(),
                           [&](const NodeRuntime& node) {
                             return node.config.id == requested_node_id;
                           });
          if (selected == current_nodes.end()) {
            throw McpOperationFailure(
                "node_not_found",
                "miner.remove node is not active: " + requested_node_id, false);
          }
          const std::uint32_t index = static_cast<std::uint32_t>(
              std::distance(current_nodes.begin(), selected));
          if (!NodeListContains(before_topology.miner_nodes, index)) {
            throw McpOperationFailure(
                "miner_not_found",
                "miner.remove node has no configured miner role: " +
                    requested_node_id,
                false);
          }
          selected_role_indexes.push_back(index);
        }
        if (options.block_production.enabled &&
            selected_role_indexes.size() ==
                before_topology.miner_nodes.size()) {
          throw McpOperationFailure(
              "miner_required",
              "miner.remove cannot remove every miner while block production "
              "is enabled",
              false);
        }
        if (options.block_production.enabled &&
            options.block_production.mode ==
                MiningMode::kScheduledBlockProduction &&
            block_scheduler == nullptr) {
          throw McpOperationFailure(
              "mining_scheduler_unavailable",
              "miner.remove requires the active scheduled block producer",
              true);
        }

        SimulationRegistry next_registry = before_roles.registry();
        next_registry.RemoveMinerNodes(selected_role_indexes);
        std::unique_lock<std::timed_mutex> publication_lock =
            AcquireRuntimePublicationLock(bounded_stop_token);
        ThrowIfStopRequested(bounded_stop_token);
        RuntimeWalletRegistry::PreparedAppend prepared_roles =
            runtime_wallet_registry.PrepareReplace(before_roles.generation(),
                                                   std::move(next_registry));
        std::unique_lock<std::mutex> configured_miners_lock(
            configured_miner_node_ids_mutex);
        std::vector<std::string> next_miner_node_ids = miner_node_ids;
        for (const std::string& node_id : requested_node_ids) {
          const auto miner = std::find(next_miner_node_ids.begin(),
                                       next_miner_node_ids.end(), node_id);
          if (miner == next_miner_node_ids.end()) {
            throw std::logic_error(
                "miner.remove registry and configured miners disagree: " +
                node_id);
          }
          next_miner_node_ids.erase(miner);
        }
        std::optional<ProbabilisticBlockScheduler::PreparedRemove>
            prepared_scheduler;
        if (block_scheduler != nullptr) {
          prepared_scheduler.emplace(block_scheduler->PrepareRemoveMiners(
              requested_node_ids, bounded_stop_token));
        }
        if (!role_control.TryBeginCommit()) {
          throw SimulationCancelled();
        }
        const RuntimeWalletSnapshot published_roles = prepared_roles.Commit();
        if (prepared_scheduler) {
          prepared_scheduler->Commit();
        }
        miner_node_ids.swap(next_miner_node_ids);
        role_control.MarkCommitted();

        try {
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kRuntimeRoleGenerationPublished,
                     boost::json::serialize(RuntimeRoleGenerationDetail(
                         published_roles, current_nodes)));
          boost::json::array removed_node_ids;
          removed_node_ids.reserve(requested_node_ids.size());
          for (const std::string& node_id : requested_node_ids) {
            removed_node_ids.emplace_back(node_id);
          }
          return boost::json::object{
              {"node_ids", std::move(removed_node_ids)},
              {"assigned_roles", boost::json::array{}},
              {"removed_roles", boost::json::array{"miner"}},
              {"action", "miner.remove"},
              {"state", "removed"},
              {"created_node_ids", boost::json::array{}},
              {"role_generation", published_roles.generation()},
              {"final_miner_count",
               published_roles.registry().topology().miner_nodes.size()},
              {"inventory_generation", current_nodes.generation()},
              {"final_node_count", current_nodes.size()},
          };
        } catch (...) {
          mcp_application.MarkRunStopping();
          request_simulation_stop();
          throw McpOperationFailure(
              "miner_remove_outcome_unconfirmed",
              "miner.remove published but completion evidence failed: " +
                  ExceptionMessage(std::current_exception()),
              false);
        }
      }
      if (kind == McpOperationKind::kAddMiner) {
        constexpr std::array<std::string_view, 6U> kAllowedFields = {
            "run_id",       "node_ids",       "count",
            "create_nodes", "wallet_node_id", "timeout_sec"};
        RejectUnsupportedFields(arguments, kAllowedFields, "miner.add");
        const std::uint32_t count =
            JsonOptionalUint32Field(arguments, "count", 0U);
        if (count == 0U || count > kSimulationNodeAddMaximumCount) {
          throw std::invalid_argument("miner.add count must be in 1..16");
        }
        const std::uint32_t timeout_sec =
            JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
        if (timeout_sec == 0U || timeout_sec > 3600U) {
          throw std::invalid_argument(
              "miner.add timeout_sec must be in 1..3600");
        }

        std::vector<std::string> requested_node_ids;
        if (const boost::json::value* node_ids =
                arguments.if_contains("node_ids")) {
          if (!node_ids->is_array()) {
            throw std::invalid_argument("miner.add node_ids must be an array");
          }
          std::set<std::string> unique_node_ids;
          requested_node_ids.reserve(node_ids->as_array().size());
          for (const boost::json::value& node_id : node_ids->as_array()) {
            if (!node_id.is_string()) {
              throw std::invalid_argument(
                  "miner.add node_ids must contain strings");
            }
            std::string id(node_id.as_string());
            RequireSafeScenarioIdentifier(id, "miner.add node_ids");
            if (!unique_node_ids.insert(id).second) {
              throw std::invalid_argument("miner.add node_ids must be unique");
            }
            requested_node_ids.push_back(std::move(id));
          }
          if (requested_node_ids.size() != count) {
            throw std::invalid_argument(
                "miner.add count must match node_ids size");
          }
        }

        std::optional<std::string> wallet_node_id;
        if (arguments.if_contains("wallet_node_id") != nullptr) {
          wallet_node_id = JsonOptionalStringField(arguments, "wallet_node_id",
                                                   std::string_view());
          RequireSafeScenarioIdentifier(*wallet_node_id,
                                        "miner.add wallet_node_id");
          if (count != 1U) {
            throw std::invalid_argument(
                "miner.add wallet_node_id requires count=1");
          }
        }
        const boost::json::value* create_nodes =
            arguments.if_contains("create_nodes");
        if (create_nodes != nullptr && !create_nodes->is_object()) {
          throw std::invalid_argument(
              "miner.add create_nodes must be an object");
        }
        const std::uint32_t selector_count =
            (!requested_node_ids.empty() ? 1U : 0U) +
            (wallet_node_id ? 1U : 0U) + (create_nodes != nullptr ? 1U : 0U);
        if (selector_count > 1U) {
          throw std::invalid_argument(
              "miner.add node_ids, wallet_node_id, and create_nodes are "
              "mutually exclusive");
        }

        if (options.block_production.enabled &&
            options.block_production.mode == MiningMode::kNativeMining) {
          const UnsupportedChainOperation error(
              ChainKindName(options.chain),
              "transactional runtime native-miner activation");
          throw McpOperationFailure("unsupported_chain_operation", error.what(),
                                    false);
        }
        if (options.block_production.enabled &&
            options.block_production.difficulty) {
          const UnsupportedChainOperation error(
              ChainKindName(options.chain),
              "transactional runtime mining-difficulty activation");
          throw McpOperationFailure("unsupported_chain_operation", error.what(),
                                    false);
        }
        if (options.block_production.enabled &&
            options.block_production.mode ==
                MiningMode::kScheduledBlockProduction &&
            block_scheduler == nullptr) {
          throw McpOperationFailure(
              "mining_scheduler_unavailable",
              "miner.add requires the active scheduled block producer", true);
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(timeout_sec);
        std::stop_source bounded_stop_source;
        std::atomic_bool deadline_expired = false;
        std::stop_callback stop_on_operation(
            operation_stop_token,
            [&bounded_stop_source] { bounded_stop_source.request_stop(); });
        std::jthread deadline_timer(
            [deadline, &bounded_stop_source,
             &deadline_expired](std::stop_token timer_stop_token) {
              try {
                WaitUntil(deadline, timer_stop_token);
              } catch (const SimulationCancelled&) {
                return;
              }
              deadline_expired.store(true, std::memory_order_release);
              bounded_stop_source.request_stop();
            });
        const std::stop_token bounded_stop_token =
            bounded_stop_source.get_token();
        SimulationCommandControl role_control;
        role_control.absolute_deadline = deadline;
        std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
          static_cast<void>(role_control.RequestCancellation(
              deadline_expired.load(std::memory_order_acquire)
                  ? SimulationCommandCancellationCause::kDeadline
                  : SimulationCommandCancellationCause::kClientCancel));
        });
        ThrowIfStopRequested(bounded_stop_token);
        auto mutation_lock =
            AcquireNodeMutationLock(node_mutation_mutex, bounded_stop_token);
        const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
        const RuntimeWalletSnapshot before_roles =
            runtime_wallet_registry.Snapshot();
        const NodeRoleTopology& before_topology =
            before_roles.registry().topology();

        if (create_nodes != nullptr) {
          Options validation_options = options;
          validation_options.nodes =
              static_cast<std::uint32_t>(current_nodes.size());
          SimulationNodeAddRequest node_request;
          try {
            node_request = ParseAndValidateSimulationNodeAddRequest(
                create_nodes->as_object(), validation_options);
          } catch (const std::runtime_error& error) {
            if (std::string_view(error.what()) !=
                "node.add request exceeds the configured node capacity") {
              throw;
            }
            throw McpOperationFailure(
                "node_capacity_exceeded", error.what(), false,
                boost::json::array{boost::json::object{
                    {"code", "node_capacity_exceeded"},
                    {"message",
                     "the requested miner-node batch exceeds available "
                     "capacity"},
                    {"path", "create_nodes.count"},
                    {"requested_count", count},
                    {"current_node_count", current_nodes.size()},
                    {"node_capacity", node_inventory.capacity()},
                    {"available_node_capacity",
                     current_nodes.size() <= node_inventory.capacity()
                         ? node_inventory.capacity() - current_nodes.size()
                         : 0U},
                    {"recoverable", false}}});
          }
          if (node_request.count != count) {
            throw std::invalid_argument(
                "miner.add count must match create_nodes.count");
          }

          RuntimeNodeAddResult added;
          try {
            std::lock_guard<std::mutex> topology_lock(runtime_topology_mutex);
            added = AddRuntimeNodesTransactional(
                options, run_root, events_path, chain_spec, driver,
                node_inventory, runtime_wallet_registry,
                RuntimeNodeAdditionRole::kMiner, block_scheduler.get(),
                &miner_node_ids, &configured_miner_node_ids_mutex, nullptr,
                *peer_connectivity_controller, &runtime_topology,
                &live_topology_config, run_process_state, lifecycle_epoch,
                node_request, &role_control, bounded_stop_token,
                runtime_node_addition_dependencies);
          } catch (const SimulationNodeResourceUnavailable& error) {
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            const SimulationNodeResourceFailure& failure = error.failure();
            throw McpOperationFailure(
                "node_resource_unavailable", error.what(), true,
                boost::json::array{boost::json::object{
                    {"code", "node_resource_unavailable"},
                    {"message", error.what()},
                    {"path", "create_nodes"},
                    {"resource_kind", failure.resource_kind},
                    {"node_id", failure.node_id},
                    {"address", failure.address},
                    {"port", failure.port},
                    {"purpose", failure.purpose},
                    {"mutation_started", failure.mutation_started},
                    {"action", "miner.add"},
                    {"recoverable", true}}});
          } catch (const SimulationCommandOutcomeUnconfirmed& error) {
            mcp_application.MarkRunStopping();
            request_simulation_stop();
            throw McpOperationFailure(
                "miner_add_outcome_unconfirmed",
                "miner.add create_nodes outcome is unconfirmed: " +
                    std::string(error.what()),
                false);
          } catch (const std::exception&) {
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            throw;
          } catch (...) {
            if (role_control.CommitPhase() ==
                SimulationCommandCommitPhase::kCancelled) {
              throw SimulationCancelled();
            }
            throw;
          }

          try {
            if (!added.role_generation || !added.final_miner_count ||
                added.added_node_ids.size() != count) {
              throw std::logic_error(
                  "miner.add create_nodes omitted its joint publication "
                  "evidence");
            }
            boost::json::array node_ids;
            boost::json::array created_node_ids;
            node_ids.reserve(added.added_node_ids.size());
            created_node_ids.reserve(added.added_node_ids.size());
            for (const std::string& node_id : added.added_node_ids) {
              node_ids.emplace_back(node_id);
              created_node_ids.emplace_back(node_id);
            }
            return boost::json::object{
                {"node_ids", std::move(node_ids)},
                {"assigned_roles", boost::json::array{"miner"}},
                {"removed_roles", boost::json::array{}},
                {"action", "miner.add"},
                {"state", "ready"},
                {"created_node_ids", std::move(created_node_ids)},
                {"role_generation", *added.role_generation},
                {"final_miner_count", *added.final_miner_count},
                {"inventory_generation", added.inventory_generation},
                {"final_node_count", added.final_node_count},
            };
          } catch (...) {
            mcp_application.MarkRunStopping();
            request_simulation_stop();
            throw McpOperationFailure(
                "miner_add_outcome_unconfirmed",
                "miner.add create_nodes published but completion evidence "
                "failed: " +
                    ExceptionMessage(std::current_exception()),
                false);
          }
        }

        const auto is_wallet_node = [&](std::size_t index) {
          return NodeListContains(before_topology.wallet_nodes,
                                  static_cast<std::uint32_t>(index));
        };
        const auto is_miner_node = [&](std::size_t index) {
          return NodeListContains(before_topology.miner_nodes,
                                  static_cast<std::uint32_t>(index));
        };
        const auto require_compatible_index = [&](std::size_t index) {
          if (is_miner_node(index)) {
            throw McpOperationFailure("miner_already_configured",
                                      "miner.add node is already a miner: " +
                                          current_nodes[index].config.id,
                                      false);
          }
          if (is_wallet_node(index) &&
              !before_topology.allow_miner_wallet_overlap) {
            throw McpOperationFailure(
                "role_conflict",
                "miner.add cannot overlap the selected wallet role: " +
                    current_nodes[index].config.id,
                false);
          }
        };

        std::vector<std::size_t> selected_indexes;
        selected_indexes.reserve(count);
        if (!requested_node_ids.empty()) {
          for (const std::string& requested_node_id : requested_node_ids) {
            const auto selected =
                std::find_if(current_nodes.begin(), current_nodes.end(),
                             [&](const NodeRuntime& node) {
                               return node.config.id == requested_node_id;
                             });
            if (selected == current_nodes.end()) {
              throw McpOperationFailure(
                  "node_not_found",
                  "miner.add node is not active: " + requested_node_id, false);
            }
            const std::size_t index = static_cast<std::size_t>(
                std::distance(current_nodes.begin(), selected));
            require_compatible_index(index);
            selected_indexes.push_back(index);
          }
        } else if (wallet_node_id) {
          if (!before_topology.allow_miner_wallet_overlap) {
            throw McpOperationFailure(
                "role_conflict",
                "miner.add wallet_node_id requires miner-wallet overlap "
                "permission",
                false);
          }
          const auto selected =
              std::find_if(current_nodes.begin(), current_nodes.end(),
                           [&](const NodeRuntime& node) {
                             return node.config.id == *wallet_node_id;
                           });
          if (selected == current_nodes.end()) {
            throw McpOperationFailure(
                "node_not_found",
                "miner.add wallet node is not active: " + *wallet_node_id,
                false);
          }
          const std::size_t index = static_cast<std::size_t>(
              std::distance(current_nodes.begin(), selected));
          if (!is_wallet_node(index)) {
            throw McpOperationFailure(
                "wallet_role_required",
                "miner.add wallet_node_id is not a registered wallet node: " +
                    *wallet_node_id,
                false);
          }
          require_compatible_index(index);
          selected_indexes.push_back(index);
        } else {
          auto process_guard = run_process_state.Lock();
          const auto select_compatible = [&](bool wallet_nodes) {
            for (std::size_t index = 0U; index < current_nodes.size() &&
                                         selected_indexes.size() < count;
                 ++index) {
              NodeRuntime& node = current_nodes[index];
              if (is_miner_node(index) ||
                  is_wallet_node(index) != wallet_nodes ||
                  (wallet_nodes &&
                   !before_topology.allow_miner_wallet_overlap) ||
                  !node.AllowsChainMetrics() || !node.process.running()) {
                continue;
              }
              selected_indexes.push_back(index);
            }
          };
          select_compatible(false);
          if (selected_indexes.size() < count &&
              before_topology.allow_miner_wallet_overlap) {
            select_compatible(true);
          }
          if (selected_indexes.size() != count) {
            throw McpOperationFailure(
                "miner_backing_node_unavailable",
                "miner.add found fewer compatible running non-miner nodes "
                "than requested",
                false);
          }
        }

        std::vector<std::uint32_t> selected_role_indexes;
        std::vector<std::string> selected_node_ids;
        selected_role_indexes.reserve(selected_indexes.size());
        selected_node_ids.reserve(selected_indexes.size());
        {
          auto process_guard = run_process_state.Lock();
          for (const std::size_t index : selected_indexes) {
            NodeRuntime& node = current_nodes[index];
            RequireNodeRunning(node, process_guard, "miner.add");
            selected_role_indexes.push_back(static_cast<std::uint32_t>(index));
            selected_node_ids.push_back(node.config.id);
          }
        }

        std::unique_lock<std::timed_mutex> publication_lock =
            AcquireRuntimePublicationLock(bounded_stop_token);
        ThrowIfStopRequested(bounded_stop_token);
        RuntimeWalletRegistry::PreparedAppend prepared_roles =
            runtime_wallet_registry.PrepareUpdate(
                before_roles.generation(), {}, selected_role_indexes, {},
                static_cast<std::uint32_t>(current_nodes.size()));
        std::unique_lock<std::mutex> configured_miners_lock(
            configured_miner_node_ids_mutex);
        std::vector<std::string> next_miner_node_ids = miner_node_ids;
        next_miner_node_ids.reserve(next_miner_node_ids.size() +
                                    selected_node_ids.size());
        for (const std::string& node_id : selected_node_ids) {
          if (std::find(next_miner_node_ids.begin(), next_miner_node_ids.end(),
                        node_id) != next_miner_node_ids.end()) {
            throw std::logic_error(
                "miner.add selected an already configured miner: " + node_id);
          }
          next_miner_node_ids.push_back(node_id);
        }
        std::optional<ProbabilisticBlockScheduler::PreparedAdd>
            prepared_scheduler;
        if (block_scheduler != nullptr) {
          prepared_scheduler.emplace(
              block_scheduler->PrepareAddMinersInactive(selected_node_ids));
        }
        if (!role_control.TryBeginCommit()) {
          throw SimulationCancelled();
        }
        const RuntimeWalletSnapshot published_roles = prepared_roles.Commit();
        if (prepared_scheduler) {
          prepared_scheduler->Commit();
        }
        miner_node_ids.swap(next_miner_node_ids);
        role_control.MarkCommitted();

        try {
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kRuntimeRoleGenerationPublished,
                     boost::json::serialize(RuntimeRoleGenerationDetail(
                         published_roles, current_nodes)));
          if (block_scheduler != nullptr) {
            for (const std::string& node_id : selected_node_ids) {
              block_scheduler->StartMiner(node_id);
            }
          }
          configured_miners_lock.unlock();
          boost::json::array node_ids;
          node_ids.reserve(selected_node_ids.size());
          for (const std::string& node_id : selected_node_ids) {
            node_ids.emplace_back(node_id);
          }
          return boost::json::object{
              {"node_ids", std::move(node_ids)},
              {"assigned_roles", boost::json::array{"miner"}},
              {"removed_roles", boost::json::array{}},
              {"action", "miner.add"},
              {"state", "ready"},
              {"created_node_ids", boost::json::array{}},
              {"role_generation", published_roles.generation()},
              {"final_miner_count",
               published_roles.registry().topology().miner_nodes.size()},
              {"inventory_generation", current_nodes.generation()},
              {"final_node_count", current_nodes.size()},
          };
        } catch (...) {
          mcp_application.MarkRunStopping();
          request_simulation_stop();
          throw McpOperationFailure(
              "miner_add_outcome_unconfirmed",
              "miner.add published but completion evidence failed: " +
                  ExceptionMessage(std::current_exception()),
              false);
        }
      }
      if (kind != McpOperationKind::kAddWallet) {
        throw std::logic_error("unknown live role mutation operation");
      }
      constexpr std::array<std::string_view, 8U> kAllowedFields = {
          "run_id",
          "node_id",
          "node_ids",
          "count",
          "mode",
          "create_node",
          "readiness_confirmations",
          "timeout_sec"};
      RejectUnsupportedFields(arguments, kAllowedFields, "wallet.add");
      const boost::json::value* create_node =
          arguments.if_contains("create_node");
      if (create_node != nullptr && !create_node->is_object()) {
        throw std::invalid_argument("wallet.add create_node must be an object");
      }
      const std::uint32_t count =
          JsonOptionalUint32Field(arguments, "count", 0U);
      if (count == 0U || count > kSimulationNodeAddMaximumCount) {
        throw std::invalid_argument("wallet.add count must be in 1..16");
      }
      const std::string mode_name =
          JsonOptionalStringField(arguments, "mode", std::string_view());
      const std::optional<WalletPrivacyMode> requested_mode =
          WalletPrivacyModeFromName(mode_name);
      if (!requested_mode) {
        throw std::invalid_argument(
            "wallet.add mode must be public or private");
      }
      const std::uint64_t readiness_confirmations =
          JsonOptionalUint64Field(arguments, "readiness_confirmations", 0U);
      if (readiness_confirmations != 0U) {
        throw McpOperationFailure(
            "wallet_funding_policy_required",
            "wallet.add readiness_confirmations requires a funding policy",
            false);
      }
      const std::uint32_t timeout_sec =
          JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
      if (timeout_sec == 0U || timeout_sec > 3600U) {
        throw std::invalid_argument(
            "wallet.add timeout_sec must be in 1..3600");
      }
      std::optional<std::string> requested_node_id;
      if (arguments.if_contains("node_id") != nullptr) {
        requested_node_id =
            JsonOptionalStringField(arguments, "node_id", std::string_view());
        RequireSafeScenarioIdentifier(*requested_node_id, "wallet.add node_id");
        if (count != 1U) {
          throw std::invalid_argument(
              "wallet.add with node_id requires count=1");
        }
      }
      std::vector<std::string> requested_node_ids;
      if (const boost::json::value* node_ids =
              arguments.if_contains("node_ids")) {
        if (!node_ids->is_array() || node_ids->as_array().empty() ||
            node_ids->as_array().size() > kSimulationNodeAddMaximumCount) {
          throw std::invalid_argument(
              "wallet.add node_ids must contain 1..16 ids");
        }
        std::set<std::string> unique_node_ids;
        requested_node_ids.reserve(node_ids->as_array().size());
        for (const boost::json::value& node_id : node_ids->as_array()) {
          if (!node_id.is_string()) {
            throw std::invalid_argument(
                "wallet.add node_ids must contain strings");
          }
          std::string id(node_id.as_string());
          RequireSafeScenarioIdentifier(id, "wallet.add node_ids");
          if (!unique_node_ids.insert(id).second) {
            throw std::invalid_argument("wallet.add node_ids must be unique");
          }
          requested_node_ids.push_back(std::move(id));
        }
        if (requested_node_ids.size() != count) {
          throw std::invalid_argument(
              "wallet.add count must match node_ids size");
        }
      }
      if (requested_node_id && !requested_node_ids.empty()) {
        throw std::invalid_argument(
            "wallet.add node_id and node_ids are mutually exclusive");
      }
      if ((requested_node_id || !requested_node_ids.empty()) &&
          create_node != nullptr) {
        throw std::invalid_argument(
            "wallet.add explicit node ids and create_node are mutually "
            "exclusive");
      }

      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
      std::stop_source bounded_stop_source;
      std::atomic_bool deadline_expired = false;
      std::stop_callback stop_on_operation(
          operation_stop_token,
          [&bounded_stop_source] { bounded_stop_source.request_stop(); });
      std::jthread deadline_timer(
          [deadline, &bounded_stop_source,
           &deadline_expired](std::stop_token timer_stop_token) {
            try {
              WaitUntil(deadline, timer_stop_token);
            } catch (const SimulationCancelled&) {
              return;
            }
            deadline_expired.store(true, std::memory_order_release);
            bounded_stop_source.request_stop();
          });
      const std::stop_token bounded_stop_token =
          bounded_stop_source.get_token();
      ThrowIfStopRequested(bounded_stop_token);
      auto mutation_lock =
          AcquireNodeMutationLock(node_mutation_mutex, bounded_stop_token);

      RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
      const RuntimeWalletSnapshot before_wallets =
          runtime_wallet_registry.Snapshot();
      const WalletInitialization initialization =
          before_wallets.registry().wallet_initialization();
      if (*requested_mode != initialization.mode) {
        throw McpOperationFailure(
            "wallet_mode_conflict",
            "wallet.add mode must match the active run wallet mode", false);
      }

      if (create_node != nullptr) {
        Options validation_options = options;
        validation_options.nodes =
            static_cast<std::uint32_t>(current_nodes.size());
        SimulationNodeAddRequest node_request;
        try {
          node_request = ParseAndValidateSimulationNodeAddRequest(
              create_node->as_object(), validation_options);
        } catch (const std::runtime_error& error) {
          if (std::string_view(error.what()) !=
              "node.add request exceeds the configured node capacity") {
            throw;
          }
          throw McpOperationFailure(
              "node_capacity_exceeded", error.what(), false,
              boost::json::array{boost::json::object{
                  {"code", "node_capacity_exceeded"},
                  {"message",
                   "the requested wallet-node batch exceeds available "
                   "capacity"},
                  {"path", "create_node.count"},
                  {"requested_count", count},
                  {"current_node_count", current_nodes.size()},
                  {"node_capacity", node_inventory.capacity()},
                  {"available_node_capacity",
                   current_nodes.size() <= node_inventory.capacity()
                       ? node_inventory.capacity() - current_nodes.size()
                       : 0U},
                  {"recoverable", false}}});
        }
        if (node_request.count != count) {
          throw std::invalid_argument(
              "wallet.add count must match create_node.count");
        }

        SimulationCommandControl node_add_control;
        node_add_control.absolute_deadline = deadline;
        std::stop_callback cancel_node_add(bounded_stop_token, [&] {
          static_cast<void>(node_add_control.RequestCancellation(
              deadline_expired.load(std::memory_order_acquire)
                  ? SimulationCommandCancellationCause::kDeadline
                  : SimulationCommandCancellationCause::kClientCancel));
        });
        const auto cancellation_won = [&] {
          return node_add_control.CommitPhase() ==
                 SimulationCommandCommitPhase::kCancelled;
        };
        RuntimeNodeAddResult added;
        try {
          std::lock_guard<std::mutex> topology_lock(runtime_topology_mutex);
          added = AddRuntimeNodesTransactional(
              options, run_root, events_path, chain_spec, driver,
              node_inventory, runtime_wallet_registry,
              RuntimeNodeAdditionRole::kWallet, block_scheduler.get(),
              &miner_node_ids, &configured_miner_node_ids_mutex, nullptr,
              *peer_connectivity_controller, &runtime_topology,
              &live_topology_config, run_process_state, lifecycle_epoch,
              node_request, &node_add_control, bounded_stop_token,
              runtime_node_addition_dependencies);
        } catch (const SimulationNodeResourceUnavailable& error) {
          if (cancellation_won()) {
            throw SimulationCancelled();
          }
          const SimulationNodeResourceFailure& failure = error.failure();
          throw McpOperationFailure(
              "node_resource_unavailable", error.what(), true,
              boost::json::array{boost::json::object{
                  {"code", "node_resource_unavailable"},
                  {"message", error.what()},
                  {"path", "create_node"},
                  {"resource_kind", failure.resource_kind},
                  {"node_id", failure.node_id},
                  {"address", failure.address},
                  {"port", failure.port},
                  {"purpose", failure.purpose},
                  {"mutation_started", failure.mutation_started},
                  {"action", "wallet.add"},
                  {"recoverable", true}}});
        } catch (const SimulationCommandOutcomeUnconfirmed& error) {
          mcp_application.MarkRunStopping();
          request_simulation_stop();
          throw McpOperationFailure(
              "wallet_add_outcome_unconfirmed",
              "wallet.add create_node outcome is unconfirmed: " +
                  std::string(error.what()),
              false);
        } catch (const std::exception&) {
          if (cancellation_won()) {
            throw SimulationCancelled();
          }
          throw;
        } catch (...) {
          if (cancellation_won()) {
            throw SimulationCancelled();
          }
          throw;
        }

        try {
          if (!added.wallet_generation || !added.final_wallet_count ||
              !added.final_wallet_node_count ||
              added.added_wallets.size() != count ||
              added.added_node_ids.size() != count) {
            throw std::logic_error(
                "wallet.add create_node omitted its joint publication "
                "evidence");
          }
          boost::json::array added_node_ids;
          boost::json::array affected_node_ids;
          boost::json::array wallets_json;
          added_node_ids.reserve(added.added_node_ids.size());
          affected_node_ids.reserve(added.added_node_ids.size());
          wallets_json.reserve(added.added_wallets.size());
          for (const std::string& node_id : added.added_node_ids) {
            added_node_ids.emplace_back(node_id);
            affected_node_ids.emplace_back(node_id);
          }
          for (const WalletIdentity& wallet : added.added_wallets) {
            wallets_json.push_back(
                RuntimeWalletIdentityJson(wallet, initialization));
          }
          return boost::json::object{
              {"added_node_ids", std::move(added_node_ids)},
              {"removed_node_ids", boost::json::array{}},
              {"affected_node_ids", std::move(affected_node_ids)},
              {"action", "wallet.add"},
              {"state", "ready"},
              {"unchanged", false},
              {"wallets", std::move(wallets_json)},
              {"inventory_generation", added.inventory_generation},
              {"final_node_count", added.final_node_count},
              {"wallet_generation", *added.wallet_generation},
              {"final_wallet_count", *added.final_wallet_count},
              {"final_wallet_node_count", *added.final_wallet_node_count},
          };
        } catch (...) {
          mcp_application.MarkRunStopping();
          request_simulation_stop();
          throw McpOperationFailure(
              "wallet_add_outcome_unconfirmed",
              "wallet.add create_node published but completion evidence "
              "failed: " +
                  ExceptionMessage(std::current_exception()),
              false);
        }
      }

      SimulationCommandControl role_control;
      role_control.absolute_deadline = deadline;
      std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
        static_cast<void>(role_control.RequestCancellation(
            deadline_expired.load(std::memory_order_acquire)
                ? SimulationCommandCancellationCause::kDeadline
                : SimulationCommandCancellationCause::kClientCancel));
      });
      const auto is_registered_wallet = [&](std::string_view node_id) {
        return std::any_of(before_wallets.wallets().begin(),
                           before_wallets.wallets().end(),
                           [&](const WalletIdentity& wallet) {
                             return wallet.node_id == node_id;
                           });
      };
      const auto has_forbidden_miner_overlap = [&](std::size_t index) {
        const NodeRoleTopology& topology = before_wallets.registry().topology();
        return !topology.allow_miner_wallet_overlap &&
               NodeListContains(topology.miner_nodes,
                                static_cast<std::uint32_t>(index));
      };
      std::vector<std::size_t> selected_indexes;
      selected_indexes.reserve(count);
      if (!requested_node_ids.empty()) {
        for (const std::string& requested : requested_node_ids) {
          const auto selected =
              std::find_if(current_nodes.begin(), current_nodes.end(),
                           [&](const NodeRuntime& node) {
                             return node.config.id == requested;
                           });
          if (selected == current_nodes.end()) {
            throw McpOperationFailure(
                "node_not_found",
                "wallet.add backing node is not active: " + requested, false);
          }
          selected_indexes.push_back(static_cast<std::size_t>(
              std::distance(current_nodes.begin(), selected)));
        }
      } else if (requested_node_id) {
        const auto selected =
            std::find_if(current_nodes.begin(), current_nodes.end(),
                         [&](const NodeRuntime& node) {
                           return node.config.id == *requested_node_id;
                         });
        if (selected == current_nodes.end()) {
          throw McpOperationFailure(
              "node_not_found",
              "wallet.add backing node is not active: " + *requested_node_id,
              false);
        }
        selected_indexes.push_back(static_cast<std::size_t>(
            std::distance(current_nodes.begin(), selected)));
      } else {
        for (std::size_t index = 0U;
             index < current_nodes.size() && selected_indexes.size() < count;
             ++index) {
          if (current_nodes[index].config.wallet_enabled &&
              !has_forbidden_miner_overlap(index)) {
            selected_indexes.push_back(index);
          }
        }
        if (selected_indexes.size() != count) {
          throw McpOperationFailure(
              "wallet_backing_node_unavailable",
              "wallet.add found fewer wallet-capable nodes than requested; "
              "use create_node after wallet-enabled node creation is "
              "available",
              false);
        }
      }

      {
        auto process_guard = run_process_state.Lock();
        for (const std::size_t index : selected_indexes) {
          NodeRuntime& node = current_nodes[index];
          if (!requested_node_ids.empty() &&
              is_registered_wallet(node.config.id)) {
            throw McpOperationFailure(
                "wallet_already_configured",
                "wallet.add node is already a wallet: " + node.config.id,
                false);
          }
          if (has_forbidden_miner_overlap(index)) {
            throw McpOperationFailure(
                "role_conflict",
                "wallet.add cannot overlap the selected miner role: " +
                    node.config.id,
                false);
          }
          if (!node.config.wallet_enabled) {
            throw McpOperationFailure(
                "wallet_support_unavailable",
                "wallet.add backing node was started without wallet "
                "support: " +
                    node.config.id,
                false);
          }
          RequireNodeRunning(node, process_guard, "wallet.add");
        }
      }

      if (before_wallets.wallets().size() >
          std::numeric_limits<std::uint32_t>::max() - count) {
        throw std::overflow_error("wallet.add wallet index exceeds uint32");
      }
      std::vector<WalletIdentity> added_wallets;
      added_wallets.reserve(count);
      for (std::size_t offset = 0U; offset < selected_indexes.size();
           ++offset) {
        ThrowIfStopRequested(bounded_stop_token);
        NodeRuntime& node = current_nodes[selected_indexes[offset]];
        WalletIdentity wallet{
            .wallet_index = static_cast<std::uint32_t>(
                before_wallets.wallets().size() + offset + 1U),
            .node = static_cast<std::uint32_t>(selected_indexes[offset] + 1U),
            .node_id = node.config.id,
            .address = {},
            .funding_address = {},
        };
        WriteEvent(events_path, options.run_id, node.config.id,
                   SimulationEventKind::kWalletAddressRequested,
                   WalletAddressDetail(wallet, initialization));
        wallet.address = driver.CreateWalletAddress(
            node.config, ToChainWalletMode(initialization), bounded_stop_token);
        if (wallet.address.empty()) {
          throw std::runtime_error(
              "wallet.add chain RPC returned an empty address");
        }
        wallet.funding_address = driver.CreateWalletFundingAddress(
            node.config, ToChainWalletMode(initialization), wallet.address,
            bounded_stop_token);
        if (wallet.funding_address.empty()) {
          throw std::runtime_error(
              "wallet.add chain RPC returned an empty funding address");
        }
        added_wallets.push_back(std::move(wallet));
      }

      std::unique_lock<std::timed_mutex> publication_lock =
          AcquireRuntimePublicationLock(bounded_stop_token);
      ThrowIfStopRequested(bounded_stop_token);
      RuntimeWalletRegistry::PreparedAppend prepared =
          runtime_wallet_registry.PrepareAppend(
              before_wallets.generation(), added_wallets,
              static_cast<std::uint32_t>(current_nodes.size()));
      if (!role_control.TryBeginCommit()) {
        throw SimulationCancelled();
      }
      const RuntimeWalletSnapshot published = prepared.Commit();
      role_control.MarkCommitted();
      try {
        for (const WalletIdentity& wallet : added_wallets) {
          WriteEvent(events_path, options.run_id, wallet.node_id,
                     SimulationEventKind::kWalletAddressCreated,
                     WalletAddressDetail(wallet, initialization));
        }
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kRuntimeWalletGenerationPublished,
                   boost::json::serialize(RuntimeWalletGenerationDetail(
                       published, added_wallets)));
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kRuntimeRoleGenerationPublished,
                   boost::json::serialize(
                       RuntimeRoleGenerationDetail(published, current_nodes)));

        boost::json::array affected_node_ids;
        boost::json::array wallets_json;
        affected_node_ids.reserve(added_wallets.size());
        wallets_json.reserve(added_wallets.size());
        for (const WalletIdentity& wallet : added_wallets) {
          affected_node_ids.emplace_back(wallet.node_id);
          wallets_json.push_back(
              RuntimeWalletIdentityJson(wallet, initialization));
        }
        return boost::json::object{
            {"added_node_ids", boost::json::array{}},
            {"removed_node_ids", boost::json::array{}},
            {"affected_node_ids", std::move(affected_node_ids)},
            {"action", "wallet.add"},
            {"state", "ready"},
            {"unchanged", false},
            {"wallets", std::move(wallets_json)},
            {"inventory_generation", current_nodes.generation()},
            {"final_node_count", current_nodes.size()},
            {"wallet_generation", published.generation()},
            {"final_wallet_count", published.wallets().size()},
            {"final_wallet_node_count",
             published.registry().topology().wallet_nodes.size()},
        };
      } catch (...) {
        mcp_application.MarkRunStopping();
        request_simulation_stop();
        throw McpOperationFailure(
            "wallet_add_outcome_unconfirmed",
            "wallet.add published but completion evidence failed: " +
                ExceptionMessage(std::current_exception()),
            false);
      }
    };
    mcp_application.SetRoleService(role_service);
    command_role_service.store(role_service, std::memory_order_release);
    installed_role_service = std::move(role_service);

    transaction_observer.emplace([&](std::stop_token observer_stop_token) {
      CombinedStopToken observer_stop(stop_token, observer_stop_token);
      const std::stop_token operation_stop_token = observer_stop.get_token();
      std::condition_variable_any wakeup;
      std::mutex wakeup_mutex;
      while (!operation_stop_token.stop_requested()) {
        try {
          const RuntimeNodeSnapshot observer_nodes = node_inventory.Snapshot();
          transaction_tracker.ObserveAll(options, events_path, driver,
                                         observer_nodes, operation_stop_token);
        } catch (const SimulationCancelled&) {
          return;
        } catch (const std::exception& error) {
          BBP_LOG(warning) << "transaction confirmation observation failed: "
                           << error.what();
        } catch (...) {
          BBP_LOG(warning) << "transaction confirmation observation failed";
        }
        std::unique_lock<std::mutex> lock(wakeup_mutex);
        wakeup.wait_for(lock, operation_stop_token,
                        std::chrono::milliseconds(50), [] { return false; });
      }
    });

    ThrowIfStopRequested(stop_token);
    if (peer_connectivity_controller) {
      peer_connectivity_controller->Start();
    }
    if (options.block_production.enabled) {
      if (options.block_production.mode == MiningMode::kNativeMining) {
        for (const std::uint32_t miner_index : miner_indexes) {
          if (!nodes[miner_index].AllowsChainMetrics()) {
            continue;
          }
          static_cast<void>(StartNativeMiningForCurrentProcess(
              driver, nodes[miner_index], run_process_state,
              chain_spec.default_reward_address, stop_token,
              "initial native mining start"));
        }
      } else if (block_scheduler) {
        block_scheduler->Start();
      }
    }
    nodes = RuntimeNodeSnapshot{};
    if (command_processor) {
      command_processor->Start();
    }

    ThrowIfStopRequested(stop_token);
    mcp_application.MarkRunStarted();
    {
      RuntimeNodeSnapshot nodes;
      RuntimeWalletSnapshot wallet_snapshot;
      {
        std::unique_lock<std::timed_mutex> publication_lock =
            AcquireRuntimePublicationLock(stop_token);
        nodes = node_inventory.Snapshot();
        wallet_snapshot = runtime_wallet_registry.Snapshot();
      }
      WriteMetricsSnapshot(
          metrics_path, options, driver, nodes, run_process_state,
          {node_network_state_mutex, node_resource_state_mutex},
          [&](const NodeRuntime& node, std::string_view error) {
            boost::json::object detail;
            detail["sample"] = 0;
            detail["initial"] = true;
            detail["error"] = error;
            WriteEvent(events_path, options.run_id, node.config.id,
                       SimulationEventKind::kMetricsNodeUnavailable,
                       boost::json::serialize(detail));
            BBP_LOG(warning) << "initial metrics snapshot skipped "
                             << node.config.id << ": " << error;
          },
          {}, stop_token, &wallet_snapshot.registry().topology());
      WriteWalletMetricsSnapshot(
          wallet_metrics_path, options, driver, nodes,
          wallet_snapshot.registry(),
          [&](std::uint32_t wallet_index, const NodeRuntime& node,
              std::string_view error) {
            boost::json::object detail;
            detail["sample"] = 0;
            detail["initial"] = true;
            detail["wallet_index"] = wallet_index;
            detail["error"] = error;
            WriteEvent(events_path, options.run_id, node.config.id,
                       SimulationEventKind::kWalletMetricsUnavailable,
                       boost::json::serialize(detail));
            BBP_LOG(warning)
                << "initial wallet metrics snapshot skipped #" << wallet_index
                << " on " << node.config.id << ": " << error;
          },
          stop_token);

      ApplyRuntimeResourceLimitUpdates(options, events_path, nodes,
                                       node_resource_state_mutex, stop_token);
      ApplyRuntimeNetworkConditionUpdates(options, events_path, nodes,
                                          node_network_state_mutex, stop_token);
      ApplyRuntimeNetworkBlockRules(options, events_path, nodes,
                                    node_network_state_mutex, stop_token);
      ApplyRuntimeNetworkPartitions(options, events_path, nodes,
                                    node_network_state_mutex, stop_token);
      ApplyRuntimeNetworkPartitionHeals(options, events_path, nodes,
                                        node_network_state_mutex, stop_token);
      ApplyRuntimeNetworkUnblockRules(options, events_path, nodes,
                                      node_network_state_mutex, stop_token);
      ApplyRuntimeNodeRestarts(options, events_path, driver,
                               *peer_connectivity_controller, nodes,
                               lifecycle_epoch, stop_token);
      ApplyRuntimeNodeFreezes(options, events_path, nodes, stop_token);
    }
    if (!timed_node_lifecycle) {
      event_engine_epoch = std::chrono::steady_clock::now();
      start_duration_timer(event_engine_epoch);
    }
    std::vector<ScheduledScenarioEvent> runtime_actions;
    runtime_actions.reserve(options.workloads.size() +
                            options.scheduled_events.size());
    for (const ScenarioWorkload& workload : EffectiveWorkloads(options)) {
      runtime_actions.emplace_back(std::chrono::milliseconds(0), 0U, workload);
    }
    std::vector<ScheduledScenarioEvent> scheduled_events =
        OrderScheduledScenarioEvents(options.scheduled_events);
    runtime_actions.insert(runtime_actions.end(), scheduled_events.begin(),
                           scheduled_events.end());
    for (size_t workload_index = 0; workload_index < runtime_actions.size();
         ++workload_index) {
      ThrowIfStopRequested(stop_token);
      const ScheduledScenarioEvent& runtime_action =
          runtime_actions[workload_index];
      const bool is_scheduled = runtime_action.sequence != 0U;
      const std::uint32_t action_index =
          is_scheduled ? runtime_action.sequence
                       : static_cast<std::uint32_t>(workload_index + 1U);
      const std::uint32_t action_count = static_cast<std::uint32_t>(
          is_scheduled ? scheduled_events.size() : options.workloads.size());
      const std::chrono::milliseconds scheduled_wall_at =
          options.time_scale.WallDuration(runtime_action.at);
      if (is_scheduled) {
        WaitUntil(SteadyDeadline(event_engine_epoch, scheduled_wall_at),
                  stop_token);
      }
      const auto action_started = std::chrono::steady_clock::now();
      if (is_scheduled) {
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kScheduledEventStarted,
                   boost::json::serialize(ScheduledEventLifecycleDetail(
                       runtime_action, scheduled_wall_at, event_engine_epoch,
                       action_started, std::nullopt)));
      }
      try {
        if (const auto* scheduled_command =
                std::get_if<SimulationCommand>(&runtime_action.action)) {
          if (!is_scheduled || active_command_queue == nullptr) {
            throw std::runtime_error(
                "scheduled command processor is not available");
          }
          const std::uint64_t operator_sequence =
              active_command_queue->PushScenarioCommand(*scheduled_command);
          const std::optional<std::string> outcome =
              wait_for_scheduled_command(runtime_action.sequence);
          if (outcome) {
            throw std::runtime_error("scheduled command operator sequence " +
                                     std::to_string(operator_sequence) +
                                     " failed: " + *outcome);
          }
        } else {
          const ScenarioWorkload& scenario_workload =
              std::get<ScenarioWorkload>(runtime_action.action);
          if (IsOneShotWorkloadKind(scenario_workload.kind)) {
            execute_one_shot_workload(scenario_workload, action_index,
                                      action_count, stop_token);
          } else {
            const RuntimeNodeSnapshot nodes = SnapshotScenarioDispatchNodes(
                node_inventory, scenario_workload.kind);
            if (scenario_workload.kind == WorkloadKind::kBlockGeneration) {
              const BlockGenerationWorkload& workload =
                  scenario_workload.block_generation;
              if (workload.count == 0U) {
                if (is_scheduled) {
                  const auto action_finished = std::chrono::steady_clock::now();
                  WriteEvent(
                      events_path, options.run_id, "sim",
                      SimulationEventKind::kScheduledEventCompleted,
                      boost::json::serialize(ScheduledEventLifecycleDetail(
                          runtime_action, scheduled_wall_at, event_engine_epoch,
                          action_started, action_finished)));
                }
                continue;
              }
              auto mutation_lock =
                  AcquireNodeMutationLock(node_mutation_mutex, stop_token);
              const RuntimeNodeSnapshot generation_nodes =
                  node_inventory.Snapshot();
              const GeneratedBlockWorkloadBoundary boundary =
                  GenerateBlockWorkloadBoundary(
                      driver, block_generation_mutex, generation_nodes,
                      workload, chain_spec.default_reward_address, stop_token,
                      stop_token);
              RecordAndPublishGeneratedBlockWorkloadBoundary(
                  options, events_path, driver, generation_nodes, boundary,
                  action_index, action_count, stop_token);
              SynchronizeBlockWorkloadBoundary(
                  options, events_path, driver, generation_nodes, boundary,
                  workload.sync_timeout_sec, stop_token);
              transaction_tracker.ObserveAll(options, events_path, driver,
                                             generation_nodes, stop_token);
            } else if (scenario_workload.kind ==
                       WorkloadKind::kWaitUntilHeight) {
              const WaitUntilHeightWorkload& workload =
                  scenario_workload.wait_until_height;
              const auto timeout = std::chrono::seconds(workload.timeout_sec);
              const auto deadline = std::chrono::steady_clock::now() + timeout;
              std::stop_source deadline_stop_source;
              std::jthread deadline_timer(
                  [deadline,
                   &deadline_stop_source](std::stop_token timer_stop_token) {
                    try {
                      WaitUntil(deadline, timer_stop_token);
                    } catch (const SimulationCancelled&) {
                      return;
                    }
                    if (!timer_stop_token.stop_requested()) {
                      deadline_stop_source.request_stop();
                    }
                  });
              CombinedStopToken execution_stop_tokens(
                  stop_token, deadline_stop_source.get_token());
              const std::stop_token execution_stop_token =
                  execution_stop_tokens.get_token();
              const auto timeout_failure = [&] {
                return std::runtime_error(
                    "wait_until_height workload timed out after " +
                    std::to_string(workload.timeout_sec) +
                    " seconds waiting for height " +
                    std::to_string(workload.height));
              };
              const auto require_open_wait = [&] {
                const std::optional<std::chrono::steady_clock::time_point>
                    run_stop_requested_at = observed_run_stop();
                if (run_stop_requested_at &&
                    *run_stop_requested_at < deadline) {
                  throw SimulationCancelled();
                }
                if (std::chrono::steady_clock::now() >= deadline) {
                  deadline_stop_source.request_stop();
                  throw timeout_failure();
                }
                ThrowIfStopRequested(stop_token);
                ThrowIfStopRequested(execution_stop_token);
              };
              try {
                ChainNodeConfig target_config;
                std::optional<ScenarioHeightWaitAdmissionLease> admission;
                {
                  auto mutation_lock = AcquireNodeMutationLock(
                      node_mutation_mutex, execution_stop_token);
                  const RuntimeNodeSnapshot height_nodes =
                      node_inventory.Snapshot();
                  const auto selected = std::find_if(
                      height_nodes.begin(), height_nodes.end(),
                      [&](const NodeRuntime& candidate) {
                        return candidate.config.id == workload.node_id;
                      });
                  if (selected == height_nodes.end()) {
                    throw std::runtime_error(
                        "wait_until_height workload references an inactive "
                        "node "
                        "id: " +
                        workload.node_id);
                  }
                  RequireNodeRunning(*selected, "wait_until_height workload");
                  target_config = selected->config;
                  admission.emplace(AcquireScenarioHeightWaitAdmission(
                      wait_until_height_workloads, target_config.id));
                }
                while (true) {
                  const std::optional<std::uint64_t> observed_height =
                      WaitForHeightReadback(driver, target_config,
                                            workload.height, timeout,
                                            execution_stop_token);
                  require_open_wait();
                  if (!observed_height) {
                    continue;
                  }
                  deadline_timer.request_stop();
                  require_open_wait();
                  WriteEvent(events_path, options.run_id, target_config.id,
                             SimulationEventKind::kHeightWaitReached,
                             HeightWaitDetail(action_index, action_count,
                                              workload.node, workload.height,
                                              *observed_height));
                  break;
                }
              } catch (const SimulationCancelled&) {
                deadline_timer.request_stop();
                const std::optional<std::chrono::steady_clock::time_point>
                    run_stop_requested_at = observed_run_stop();
                if (run_stop_requested_at &&
                    *run_stop_requested_at < deadline) {
                  throw;
                }
                if (deadline_stop_source.stop_requested() ||
                    std::chrono::steady_clock::now() >= deadline) {
                  throw timeout_failure();
                }
                throw;
              }
            } else if (scenario_workload.kind == WorkloadKind::kWaitForPeers) {
              const WaitForPeersWorkload& workload =
                  scenario_workload.wait_for_peers;
              NodeRuntime& node = nodes[workload.node - 1U];
              RequireNodeRunning(node, "wait_for_peers workload");
              const uint64_t observed_peer_count = driver.WaitForPeerCount(
                  node.config, workload.peer_count,
                  std::chrono::seconds(workload.timeout_sec), stop_token);
              WriteEvent(events_path, options.run_id, node.config.id,
                         SimulationEventKind::kPeerCountReached,
                         PeerCountWaitDetail(action_index, action_count,
                                             workload.node, workload.peer_count,
                                             observed_peer_count));
            } else if (scenario_workload.kind ==
                       WorkloadKind::kWalletTransactions) {
              std::shared_ptr<LiveWalletWorkloadRecord> workload_record;
              {
                auto mutation_lock =
                    AcquireNodeMutationLock(node_mutation_mutex, stop_token);
                workload_record = launch_wallet_workload(
                    scenario_workload.wallet_transactions, std::nullopt);
              }
              if (ExplicitWalletTransactionAttemptLimit(
                      scenario_workload.wallet_transactions)) {
                std::unique_lock<std::mutex> workload_lock(
                    workload_record->mutex);
                if (!workload_record->changed.wait(
                        workload_lock, stop_token, [&] {
                          return IsTerminalLiveWalletWorkloadState(
                              workload_record->state);
                        })) {
                  throw SimulationCancelled();
                }
                if (workload_record->state ==
                    LiveWalletWorkloadState::kFailed) {
                  throw std::runtime_error(workload_record->failure.value_or(
                      "wallet workload failed without a diagnostic"));
                }
              }
            } else {
              throw std::logic_error(
                  "lifecycle workload kind has no scenario dispatcher");
            }
          }
        }
      } catch (const std::exception& e) {
        if (is_scheduled) {
          const auto action_finished = std::chrono::steady_clock::now();
          WriteEvent(events_path, options.run_id, "sim",
                     SimulationEventKind::kScheduledEventFailed,
                     boost::json::serialize(ScheduledEventLifecycleDetail(
                         runtime_action, scheduled_wall_at, event_engine_epoch,
                         action_started, action_finished, e.what())));
        }
        throw;
      } catch (...) {
        if (is_scheduled) {
          const auto action_finished = std::chrono::steady_clock::now();
          WriteEvent(
              events_path, options.run_id, "sim",
              SimulationEventKind::kScheduledEventFailed,
              boost::json::serialize(ScheduledEventLifecycleDetail(
                  runtime_action, scheduled_wall_at, event_engine_epoch,
                  action_started, action_finished, "unknown exception")));
        }
        throw;
      }
      if (is_scheduled) {
        const auto action_finished = std::chrono::steady_clock::now();
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kScheduledEventCompleted,
                   boost::json::serialize(ScheduledEventLifecycleDetail(
                       runtime_action, scheduled_wall_at, event_engine_epoch,
                       action_started, action_finished)));
      }
    }

    metrics_collector->Wait();
    ThrowIfStopRequested(stop_token);
    mcp_application.MarkRunStopping();
    stop_duration_timer();
    stop_lifecycle_supervisor();
    stop_wallet_workloads(false);
    stop_instrumentation(false);
    stop_role_mutations();
    stop_transaction_observer();
    stop_command_processor();
    stop_peer_connectivity();
    stop_block_production();
    const RuntimeNodeSnapshot final_nodes = node_inventory.Snapshot();
    transaction_tracker.ObserveAll(options, events_path, driver, final_nodes,
                                   stop_token);
    WriteTransactionLoadCompletions(options, events_path,
                                    pending_transaction_load_completions);
    const RuntimeWalletSnapshot final_registry =
        runtime_wallet_registry.Snapshot();
    WriteMetricsSnapshot(
        metrics_path, options, driver, final_nodes, run_process_state,
        {node_network_state_mutex, node_resource_state_mutex}, {}, {},
        stop_token, &final_registry.registry().topology());
    WriteWalletMetricsSnapshot(wallet_metrics_path, options, driver,
                               final_nodes, final_registry.registry(), {},
                               stop_token);

    StopNodes(options, events_path, driver, final_nodes);
    log_collector->Stop();
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRunFinished);
    mcp_application.MarkRunStopped();
    BBP_LOG(info) << "finished run " << options.run_id;
  } catch (const SimulationCancelled&) {
    stop_lifecycle_supervisor();
    std::exception_ptr policy_failure;
    {
      std::lock_guard<std::mutex> lock(lifecycle_failure_mutex);
      policy_failure = lifecycle_failure;
    }
    if (policy_failure) {
      const std::string detail = ExceptionMessage(policy_failure);
      handle_run_failure(detail);
      std::rethrow_exception(policy_failure);
    }
    if (simulation_duration_reached.load(std::memory_order_acquire) &&
        !external_stop_token.stop_requested()) {
      handle_simulation_duration();
    } else {
      handle_run_cancellation();
      terminal_outcome = BenchmarkTerminalOutcome::kCancelled;
    }
  } catch (const std::exception& e) {
    handle_run_failure(e.what());
    throw;
  } catch (...) {
    handle_run_failure("unknown exception");
    throw;
  }

  BBP_LOG(info) << "run_id=" << options.run_id << "\n"
                << "output_dir=" << run_root << "\n"
                << "metrics=" << metrics_path << "\n"
                << "wallet_metrics=" << wallet_metrics_path << "\n"
                << "events=" << events_path;
  return BenchmarkHeadlessResult{
      .result = 0,
      .terminal_outcome = terminal_outcome,
  };
}

enum class EditorRunState {
  kStarting,
  kActive,
  kStopping,
  kStopped,
  kFailed,
};

bool IsTerminalEditorRunState(EditorRunState state) {
  return state == EditorRunState::kStopped || state == EditorRunState::kFailed;
}

struct EditorTuiReadLeaseState {
  std::mutex mutex;
  std::condition_variable_any drained;
  std::size_t readers = 0U;
};

class EditorTuiReadLease {
 public:
  explicit EditorTuiReadLease(std::shared_ptr<EditorTuiReadLeaseState> state)
      : state_(std::move(state)) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    ++state_->readers;
  }

  ~EditorTuiReadLease() {
    std::lock_guard<std::mutex> lock(state_->mutex);
    --state_->readers;
    state_->drained.notify_all();
  }

  EditorTuiReadLease(const EditorTuiReadLease&) = delete;
  EditorTuiReadLease& operator=(const EditorTuiReadLease&) = delete;

 private:
  std::shared_ptr<EditorTuiReadLeaseState> state_;
};

[[maybe_unused]] std::string_view EditorRunStateName(EditorRunState state) {
  switch (state) {
    case EditorRunState::kStarting:
      return "starting";
    case EditorRunState::kActive:
      return "active";
    case EditorRunState::kStopping:
      return "stopping";
    case EditorRunState::kStopped:
      return "stopped";
    case EditorRunState::kFailed:
      return "failed";
  }
  throw std::logic_error("unknown editor run state");
}

struct EditorRunContext {
  std::uint64_t generation = 0U;
  std::shared_ptr<Options> options;
  std::optional<boost::json::object> source_scenario;
  std::shared_ptr<SimulationCommandQueue> command_queue;
#ifdef BBP_FIRO_GUI_LAUNCHER
  std::shared_ptr<OperatorConnectionLauncher> operator_connection_launcher;
#endif
  std::shared_ptr<RuntimeNodeInventory> node_inventory;
  std::shared_ptr<EditorTuiReadLeaseState> tui_read_lease_state =
      std::make_shared<EditorTuiReadLeaseState>();
  std::atomic<RunStopTick> run_stop_tick{kRunStopNotObserved};
  std::stop_source simulation_stop_source;
  std::shared_ptr<McpLiveApplication> mcp_application;
  std::shared_ptr<ReservedManagedRunRoot> reserved_run_root;
  std::function<void(std::stop_token)> run_root_prepared;
  std::jthread worker;
  mutable std::mutex mutex;
  std::condition_variable_any state_changed;
  EditorRunState state = EditorRunState::kStarting;
  bool reached_active = false;
  bool host_stop_requested = false;
  bool retained_run_root_available = false;
  std::exception_ptr failure;
  int result = 1;
};

struct EditorRunSnapshot {
  std::uint64_t generation = 0U;
  std::string run_id;
  std::filesystem::path run_root;
  std::string chain;
  std::uint32_t node_count = 0U;
  std::uint32_t node_capacity = 0U;
  std::uint32_t chain_node_maximum = 0U;
  std::uint32_t available_node_capacity = 0U;
  EditorRunState state = EditorRunState::kStarting;
  std::shared_ptr<SimulationCommandQueue> command_queue;
#ifdef BBP_FIRO_GUI_LAUNCHER
  std::shared_ptr<OperatorConnectionLauncher> operator_connection_launcher;
#endif
  std::shared_ptr<McpLiveApplication> mcp_application;
  std::shared_ptr<void> tui_read_lease;
};

BenchmarkHeadlessResult RunPreparedBenchmark(
    const std::shared_ptr<EditorRunContext>& context) {
  Options& options = *context->options;
  const std::filesystem::path run_root = BenchmarkRunRoot(options);
  const std::stop_token setup_stop_token =
      context->simulation_stop_source.get_token();
  std::unique_ptr<NetworkAllocationLock> network_allocation_lock;
  bool run_prepared = false;
  try {
    ThrowIfStopRequested(setup_stop_token);
    if (context->reserved_run_root) {
      const RunOwnership& ownership = context->reserved_run_root->ownership();
      if (ownership.run_id != options.run_id ||
          ownership.run_root != run_root.lexically_normal()) {
        throw std::runtime_error(
            "reserved replay destination does not match the launched run");
      }
      options.run_ownership = ownership;
      context->reserved_run_root->Adopt();
      run_prepared = true;
      PrepareManagedRunRoot(&options, MakeNodeVethConfig,
                            context->reserved_run_root);
    } else {
      PrepareManagedRunRoot(&options, MakeNodeVethConfig);
      run_prepared = true;
    }
    context->retained_run_root_available = true;
    if (context->run_root_prepared) {
      context->run_root_prepared(setup_stop_token);
    }
    if (options.isolate_network) {
      network_allocation_lock =
          std::make_unique<NetworkAllocationLock>(setup_stop_token);
      RequireRunNetworkInterfacesAvailable(options, setup_stop_token);
      options.network_address_plan = SimulationNetworkAddressPlan::Allocate(
          options.run_id, options.node_capacity,
          ListIpv4Routes(setup_stop_token));
    }
    ThrowIfStopRequested(setup_stop_token);
    WriteScenarioFiles(options, run_root, ChainDriverSpecFor(options.chain),
                       context->reserved_run_root
                           ? context->reserved_run_root->descriptor()
                           : -1);
    if (context->source_scenario) {
      WriteSourceScenarioFile(
          *context->source_scenario, options.run_id, run_root,
          context->reserved_run_root
              ? std::optional<int>(context->reserved_run_root->descriptor())
              : std::nullopt);
    }
    if (context->reserved_run_root &&
        LoadRunOwnershipAt(options.run_id, run_root.lexically_normal(),
                           context->reserved_run_root->descriptor()) !=
            RequireRunOwnership(options)) {
      throw std::runtime_error(
          "reserved replay destination identity changed during publication");
    }
    ThrowIfStopRequested(setup_stop_token);
  } catch (...) {
    const std::exception_ptr setup_failure = std::current_exception();
    if (run_prepared) {
      try {
        RemovePreparedRunRoot(options, context->reserved_run_root);
        context->retained_run_root_available = false;
      } catch (...) {
        throw std::runtime_error(
            "run setup failed: " + ExceptionMessage(setup_failure) +
            "; setup cleanup also failed: " +
            ExceptionMessage(std::current_exception()));
      }
    }
    if (setup_stop_token.stop_requested()) {
      throw SimulationCancelled();
    }
    std::rethrow_exception(setup_failure);
  }

  return RunBenchmarkHeadless(
      options, *context->command_queue, *context->mcp_application,
      *context->node_inventory, context->simulation_stop_source,
      context->run_stop_tick);
}

class EditorRunController {
 public:
  EditorRunController() = default;

  ~EditorRunController() {
    try {
      Shutdown();
    } catch (...) {
    }
  }

  EditorRunController(const EditorRunController&) = delete;
  EditorRunController& operator=(const EditorRunController&) = delete;

  void SetEvidenceCallbacks(
      std::function<void(McpEvidenceRecord)> publish_evidence,
      std::function<void(std::string_view)> close_run_subscriptions) {
    std::lock_guard<std::timed_mutex> transition_lock(transition_mutex_);
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_) {
      throw std::logic_error(
          "MCP evidence callbacks must be configured before launching a run");
    }
    publish_evidence_ = std::move(publish_evidence);
    close_run_subscriptions_ = std::move(close_run_subscriptions);
  }

  std::shared_ptr<EditorRunContext> LaunchScenario(
      const boost::json::object& scenario,
      const std::filesystem::path& benchmark_root, std::stop_token stop_token) {
    boost::json::object hosted_scenario = scenario;
    Options options = PrepareHostedScenario(&hosted_scenario, benchmark_root);
    return Launch(std::move(options), std::move(hosted_scenario), stop_token);
  }

  std::shared_ptr<EditorRunContext> ReplayScenario(
      std::string_view source_run_id,
      std::optional<std::string> destination_run_id,
      const std::filesystem::path& benchmark_root, std::stop_token stop_token) {
    RequireSafeRunId(source_run_id);
    if (destination_run_id) {
      RequireSafeRunId(*destination_run_id);
      if (*destination_run_id == source_run_id) {
        throw std::invalid_argument(
            "replay destination run id must differ from its source");
      }
    }
    std::unique_lock<std::timed_mutex> transition_lock =
        AcquireTransitionLock(stop_token);
    if (CurrentRun(false)) {
      throw McpOperationFailure(
          "run_already_active",
          "a managed run is already starting, active, or stopping", true);
    }

    const std::filesystem::path source_root =
        CleanupRunRoot(benchmark_root, source_run_id);
    const boost::json::object source_scenario =
        LoadRetainedSourceScenario(source_root, source_run_id, stop_token);

    const auto launch_destination = [&](std::string_view destination) {
      boost::json::object replay_scenario = source_scenario;
      replay_scenario["run_id"] = destination;
      Options options = PrepareHostedScenario(&replay_scenario, benchmark_root);
      return LaunchLocked(std::move(options), std::move(replay_scenario),
                          stop_token, true);
    };
    if (destination_run_id) {
      return launch_destination(*destination_run_id);
    }

    constexpr std::size_t kMaximumGeneratedRunIdAttempts = 32U;
    for (std::size_t attempt = 0U; attempt < kMaximumGeneratedRunIdAttempts;
         ++attempt) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      const std::string candidate = MakeRunId();
      if (candidate == source_run_id) {
        continue;
      }
      try {
        return launch_destination(candidate);
      } catch (const McpOperationFailure& failure) {
        if (failure.code() != "run_replay_destination_exists") {
          throw;
        }
      }
    }
    throw McpOperationFailure("run_replay_id_unavailable",
                              "BBP could not allocate a fresh replay run id",
                              true);
  }

  std::shared_ptr<EditorRunContext> LaunchOptions(Options options) {
    return Launch(std::move(options), std::nullopt, {});
  }

  EditorRunSnapshot WaitUntilActive(
      const std::shared_ptr<EditorRunContext>& context,
      std::stop_token stop_token) const {
    std::unique_lock<std::mutex> lock(context->mutex);
    const bool ready = context->state_changed.wait(lock, stop_token, [&] {
      return context->reached_active ||
             IsTerminalEditorRunState(context->state);
    });
    if (!ready) {
      throw McpOperationCancelled();
    }
    if (context->reached_active) {
      return SnapshotLocked(*context);
    }
    if (context->failure) {
      throw McpOperationFailure(
          "run_launch_failed",
          "managed run startup failed: " + ExceptionMessage(context->failure),
          false);
    }
    throw McpOperationFailure("run_launch_cancelled",
                              "managed run stopped before startup completed",
                              false);
  }

  EditorRunSnapshot StopRun(std::string_view run_id,
                            std::chrono::seconds timeout,
                            std::stop_token stop_token) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::shared_ptr<EditorRunContext> context;
    {
      std::unique_lock<std::timed_mutex> transition_lock(transition_mutex_,
                                                         std::defer_lock);
      constexpr auto kTransitionPollInterval = std::chrono::milliseconds(25);
      while (!transition_lock.try_lock_until(std::min(
          deadline,
          std::chrono::steady_clock::now() + kTransitionPollInterval))) {
        if (stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          throw McpOperationFailure(
              "run_stop_timeout",
              "managed run stop could not start before the timeout", true);
        }
      }
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_stop_timeout",
            "managed run stop could not start before the timeout", true);
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        context = active_;
      }
      if (!context) {
        throw McpOperationFailure("run_not_active",
                                  "there is no active managed run", false);
      }
      {
        std::lock_guard<std::mutex> lock(context->mutex);
        if (context->options->run_id != run_id) {
          throw McpOperationFailure(
              "run_id_mismatch",
              "requested run does not match the active managed run", false);
        }
        if (context->state == EditorRunState::kStopping ||
            IsTerminalEditorRunState(context->state)) {
          throw McpOperationFailure(
              "run_not_active", "the managed run is no longer active", false);
        }
        context->host_stop_requested = true;
      }
      context->mcp_application->MarkRunStopping();
      RequestStop(context);
    }

    std::unique_lock<std::mutex> lock(context->mutex);
    const bool stopped = context->state_changed.wait_until(
        lock, stop_token, deadline,
        [&] { return IsTerminalEditorRunState(context->state); });
    if (!stopped) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      throw McpOperationFailure(
          "run_stop_timeout",
          "managed run cleanup did not finish before the timeout", true);
    }
    if (context->state == EditorRunState::kFailed) {
      if (context->failure) {
        try {
          std::rethrow_exception(context->failure);
        } catch (const WorkloadServiceShutdownTimeout& error) {
          throw McpOperationFailure("workload_service_shutdown_timeout",
                                    error.what(), false,
                                    boost::json::array{error.Diagnostic()});
        } catch (...) {
        }
      }
      const std::string detail = context->failure
                                     ? ExceptionMessage(context->failure)
                                     : "unknown managed-run failure";
      throw McpOperationFailure(
          "run_stop_failed",
          "managed run failed during bounded shutdown: " + detail, false);
    }
    return SnapshotLocked(*context);
  }

  McpRunCleanupResult CleanRun(const std::filesystem::path& benchmark_root,
                               std::string_view run_id,
                               std::chrono::seconds timeout,
                               bool remove_retained_artifacts,
                               std::stop_token stop_token) {
    RequireSafeOutputDirectory(benchmark_root);
    RequireSafeRunId(run_id);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::stop_source deadline_stop_source;
    std::jthread deadline_timer(
        [deadline, &deadline_stop_source](std::stop_token timer_stop_token) {
          try {
            WaitUntil(deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          if (!timer_stop_token.stop_requested()) {
            deadline_stop_source.request_stop();
          }
        });
    CombinedStopToken operation_stop_tokens(stop_token,
                                            deadline_stop_source.get_token());
    const std::stop_token operation_stop_token =
        operation_stop_tokens.get_token();
    std::unique_lock<std::timed_mutex> transition_lock(transition_mutex_,
                                                       std::defer_lock);
    constexpr auto kTransitionPollInterval = std::chrono::milliseconds(25);
    while (!transition_lock.try_lock_until(
        std::min(deadline,
                 std::chrono::steady_clock::now() + kTransitionPollInterval))) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup could not start before the timeout", true);
      }
    }

    try {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup could not start before the timeout", true);
      }

      std::shared_ptr<EditorRunContext> context;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
          throw McpOperationFailure(
              "application_stopping",
              "BBP is shutting down and cannot clean a retained run", false);
        }
        context = active_;
      }
      if (context) {
        std::lock_guard<std::mutex> lock(context->mutex);
        if (!IsTerminalEditorRunState(context->state)) {
          throw McpOperationFailure(
              "run_cleanup_requires_stop",
              "run.clean requires the managed run to be stopped first", true);
        }
      }
      if (context) {
        ReapTerminalRun(deadline, operation_stop_token);
      }
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup could not start before the timeout", true);
      }

      const std::filesystem::path run_root =
          CleanupRunRoot(benchmark_root, run_id);
      const std::optional<OwnedRunRootCleanupReceipt> durable_receipt =
          TryLoadOwnedRunRootCleanupReceipt(run_id, run_root, deadline,
                                            operation_stop_token);
      auto receipt = cleanup_receipts_.find(run_id);
      if (receipt != cleanup_receipts_.end() &&
          receipt->second.ownership.run_root != run_root) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "run cleanup receipt does not match the configured run root",
            false);
      }
      if (durable_receipt) {
        if (receipt != cleanup_receipts_.end() &&
            (receipt->second.ownership != durable_receipt->ownership ||
             receipt->second.root_identity != durable_receipt->root_identity)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "durable and in-memory cleanup receipts disagree", false);
        }
        if (receipt == cleanup_receipts_.end()) {
          const auto inserted = cleanup_receipts_.emplace(
              std::string(run_id),
              RunCleanupReceipt{
                  .ownership = durable_receipt->ownership,
                  .root_identity = durable_receipt->root_identity,
                  .result = SuccessfulCleanupResult(run_id),
                  .external_cleanup_complete = true,
                  .complete = false,
              });
          if (!inserted.second) {
            throw std::logic_error(
                "durable cleanup receipt insertion lost serialization");
          }
          receipt = inserted.first;
        }
      }
      bool durable_receipt_available = durable_receipt.has_value();
      std::optional<OwnedRunRootIdentity> initial_identity;
      try {
        initial_identity =
            InspectRunRootIdentity(run_root, deadline, operation_stop_token);
      } catch (const std::exception& error) {
        if (stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          throw McpOperationFailure(
              "run_clean_timeout",
              "run cleanup root verification exceeded the timeout", true);
        }
        throw McpOperationFailure(
            "run_cleanup_unverified",
            "run cleanup could not verify the retained root identity: " +
                std::string(error.what()),
            false);
      }
      if (durable_receipt_available && !initial_identity) {
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        if (!InspectRunRootIdentity(quarantine, deadline,
                                    operation_stop_token)) {
          receipt->second.complete = true;
          return receipt->second.result;
        }
      }
      if (receipt != cleanup_receipts_.end() && !receipt->second.complete &&
          !remove_retained_artifacts) {
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "an incomplete artifact-removal cleanup must be resumed with "
            "remove_retained_artifacts enabled",
            false);
      }
      const auto require_removed_artifacts_absent =
          [&](const std::filesystem::path& quarantine,
              std::string_view failure_detail) {
            if (InspectRunRootIdentity(run_root) ||
                InspectRunRootIdentity(quarantine)) {
              throw McpOperationFailure("run_cleanup_identity_reused",
                                        std::string(failure_detail), false);
            }
          };
      const auto complete_prepared_artifact_removal = [&] {
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        const std::optional<OwnedRunRootIdentity> quarantined_identity =
            InspectRunRootIdentity(quarantine, deadline, operation_stop_token);
        if ((initial_identity &&
             *initial_identity != receipt->second.root_identity) ||
            (quarantined_identity &&
             *quarantined_identity != receipt->second.root_identity)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "an incomplete cleanup found a foreign public or quarantined "
              "run root",
              false);
        }
        if (initial_identity && quarantined_identity) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "an incomplete cleanup found both public and quarantined run "
              "roots",
              false);
        }
        if (!initial_identity && !quarantined_identity) {
          if (!durable_receipt_available) {
            throw McpOperationFailure(
                "run_cleanup_state_uncertain",
                "a prepared run cleanup lost its ownership root before "
                "completion was verified",
                false);
          }
          receipt->second.complete = true;
          return receipt->second.result;
        }
        DetachRunLogFile(run_root, deadline, operation_stop_token);
        if (!durable_receipt_available) {
          WriteOwnedRunRootCleanupReceipt(receipt->second.ownership,
                                          receipt->second.root_identity,
                                          deadline, operation_stop_token);
          durable_receipt_available = true;
        }
        RemoveOwnedRunRoot(receipt->second.ownership, deadline,
                           operation_stop_token, receipt->second.root_identity);
#ifdef BBP_ENABLE_TEST_HOOKS
        if (run_cleanup_root_removed_test_hook) {
          run_cleanup_root_removed_test_hook();
        }
#endif
        require_removed_artifacts_absent(
            quarantine,
            "a public or quarantined root appeared while prepared cleanup "
            "completed");
        receipt->second.complete = true;
        return receipt->second.result;
      };
      if (receipt != cleanup_receipts_.end() && !receipt->second.complete &&
          receipt->second.external_cleanup_complete) {
        return complete_prepared_artifact_removal();
      }
      if (receipt != cleanup_receipts_.end() && receipt->second.complete) {
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        if (initial_identity || InspectRunRootIdentity(quarantine, deadline,
                                                       operation_stop_token)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "a run root reappeared after its verified cleanup completed",
              false);
        }
        return receipt->second.result;
      }
      if (!initial_identity) {
        if (receipt == cleanup_receipts_.end()) {
          throw McpOperationFailure(
              "run_cleanup_unverified",
              "run cleanup cannot verify ownership of an absent run root",
              false);
        }
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "a prepared run cleanup lost its ownership root before "
            "completion was verified",
            false);
      }

      RunOwnership ownership;
      try {
        ownership = LoadRunOwnership(std::string(run_id), run_root,
                                     operation_stop_token);
      } catch (const std::exception& error) {
        if (stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          throw McpOperationFailure(
              "run_clean_timeout",
              "run cleanup ownership verification exceeded the timeout", true);
        }
        throw McpOperationFailure("run_cleanup_unverified",
                                  "run cleanup could not verify ownership: " +
                                      std::string(error.what()),
                                  false);
      }
      const std::optional<OwnedRunRootIdentity> confirmed_identity =
          InspectRunRootIdentity(run_root, deadline, operation_stop_token);
      if (!confirmed_identity || *confirmed_identity != *initial_identity ||
          ownership.run_root != run_root) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "run identity changed while cleanup ownership was verified", false);
      }
      if (receipt != cleanup_receipts_.end() &&
          (receipt->second.ownership != ownership ||
           receipt->second.root_identity != *initial_identity)) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "run cleanup refuses a replaced or foreign run identity", false);
      }
      if (receipt == cleanup_receipts_.end() && remove_retained_artifacts) {
        RunCleanupReceipt prepared{
            .ownership = ownership,
            .root_identity = *initial_identity,
            .result = SuccessfulCleanupResult(run_id),
            .external_cleanup_complete = false,
            .complete = false,
        };
        const auto inserted =
            cleanup_receipts_.emplace(std::string(run_id), std::move(prepared));
        if (!inserted.second) {
          throw std::logic_error(
              "run cleanup receipt insertion lost controller serialization");
        }
        receipt = inserted.first;
      }

      Options cleanup_options;
      cleanup_options.output_dir = run_root.parent_path();
      cleanup_options.run_id = std::string(run_id);
      const RunOwnership* expected_ownership =
          receipt == cleanup_receipts_.end() ? &ownership
                                             : &receipt->second.ownership;
      McpRunCleanupResult result = CleanupRun(
          std::move(cleanup_options), deadline, operation_stop_token,
          remove_retained_artifacts, expected_ownership, *initial_identity,
          receipt == cleanup_receipts_.end()
              ? nullptr
              : &receipt->second.external_cleanup_complete);
      if (remove_retained_artifacts) {
        if (receipt == cleanup_receipts_.end()) {
          throw std::logic_error(
              "artifact-removing cleanup completed without a receipt");
        }
#ifdef BBP_ENABLE_TEST_HOOKS
        if (run_cleanup_root_removed_test_hook) {
          run_cleanup_root_removed_test_hook();
        }
#endif
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        require_removed_artifacts_absent(
            quarantine,
            "a public or quarantined root appeared during verified cleanup");
        receipt->second.complete = true;
      } else {
        const std::optional<OwnedRunRootIdentity> retained_identity =
            InspectRunRootIdentity(run_root, deadline, operation_stop_token);
        if (!retained_identity || *retained_identity != *initial_identity) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "retained run identity changed before ownership was rechecked",
              false);
        }
        RunOwnership retained_ownership;
        try {
          retained_ownership = LoadRunOwnership(std::string(run_id), run_root,
                                                operation_stop_token);
        } catch (...) {
          const std::string detail = ExceptionMessage(std::current_exception());
          if (stop_token.stop_requested()) {
            throw McpOperationCancelled();
          }
          if (std::chrono::steady_clock::now() >= deadline) {
            throw McpOperationFailure(
                "run_clean_timeout",
                "retained run ownership recheck exceeded the timeout: " +
                    detail,
                true);
          }
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "retained run ownership changed before cleanup completion: " +
                  detail,
              false);
        }
        const std::optional<OwnedRunRootIdentity> final_identity =
            InspectRunRootIdentity(run_root, deadline, operation_stop_token);
        if (!final_identity || *final_identity != *initial_identity ||
            retained_ownership != ownership ||
            InspectRunRootIdentity(OwnedRunRootCleanupQuarantinePath(ownership),
                                   deadline, operation_stop_token)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "retained run identity changed before cleanup completion", false);
        }
      }
      return result;
    } catch (const McpOperationCancelled&) {
      if (stop_token.stop_requested()) {
        throw;
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup did not finish before the timeout", true);
      }
      throw;
    } catch (const McpOperationFailure&) {
      throw;
    } catch (const OwnedRunRootIdentityMismatch& error) {
      throw McpOperationFailure(
          "run_cleanup_identity_reused",
          "run cleanup refused a replaced root: " + std::string(error.what()),
          false);
    } catch (const CgroupOwnershipMismatch& error) {
      throw McpOperationFailure(
          "run_cleanup_unverified",
          "run cleanup refused unverified cgroup ownership: " +
              std::string(error.what()),
          false);
    } catch (...) {
      const std::string detail = ExceptionMessage(std::current_exception());
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup did not finish before the timeout: " + detail, true);
      }
      throw McpOperationFailure("run_clean_failed",
                                "run cleanup failed: " + detail, true);
    }
  }

  std::optional<EditorRunSnapshot> CurrentRun(
      bool include_terminal, bool acquire_tui_read_lease = false) const {
    std::shared_ptr<EditorRunContext> context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      context = active_;
    }
    if (!context) {
      return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(context->mutex);
    if (!include_terminal && IsTerminalEditorRunState(context->state)) {
      return std::nullopt;
    }
    return SnapshotLocked(*context, acquire_tui_read_lease);
  }

  std::uint64_t RunMembershipRevision() const noexcept {
    return run_membership_revision_.load(std::memory_order_acquire);
  }

  void RequestActiveRunStop() {
    std::shared_ptr<EditorRunContext> context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      context = active_;
    }
    if (!context) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(context->mutex);
      context->host_stop_requested = true;
    }
    context->mcp_application->MarkRunStopping();
    RequestStop(context);
  }

  int WaitForInitialRun(const std::shared_ptr<EditorRunContext>& context,
                        std::stop_token application_stop_token) const {
    std::unique_lock<std::mutex> lock(context->mutex);
    const bool completed = context->state_changed.wait(
        lock, application_stop_token,
        [&] { return IsTerminalEditorRunState(context->state); });
    if (!completed) {
      return 0;
    }
    if (context->host_stop_requested) {
      lock.unlock();
      WaitForApplicationStop(application_stop_token);
      return 0;
    }
    if (context->failure) {
      std::rethrow_exception(context->failure);
    }
    return context->result;
  }

  void Shutdown() {
    std::shared_ptr<EditorRunContext> context;
    std::lock_guard<std::timed_mutex> transition_lock(transition_mutex_);
    JoinRetiredWorkers();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (shutdown_ && !active_) {
        return;
      }
      shutdown_ = true;
      context = active_;
    }
    if (context) {
      RequestStop(context);
    }
    bool application_shutdown_succeeded = false;
    try {
      JoinAndShutdown(context, true, &application_shutdown_succeeded);
    } catch (...) {
      if (application_shutdown_succeeded) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (active_ == context) {
          active_.reset();
        }
      }
      throw;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_ == context) {
      active_.reset();
    }
  }

 private:
  struct RunCleanupReceipt {
    RunOwnership ownership;
    OwnedRunRootIdentity root_identity;
    McpRunCleanupResult result;
    bool external_cleanup_complete = false;
    bool complete = false;
  };

  static Options PrepareHostedScenario(
      boost::json::object* hosted_scenario,
      const std::filesystem::path& benchmark_root) {
    const boost::json::value* simulation =
        hosted_scenario->if_contains("simulation");
    const bool has_nested_output =
        simulation != nullptr && simulation->is_object() &&
        simulation->as_object().if_contains("output_dir") != nullptr;
    if (hosted_scenario->if_contains("output_dir") == nullptr &&
        !has_nested_output) {
      (*hosted_scenario)["output_dir"] = benchmark_root.string();
    }

    Options options = ParseAndValidateScenario(*hosted_scenario);
    const std::filesystem::path requested_root =
        std::filesystem::weakly_canonical(
            std::filesystem::absolute(options.output_dir))
            .lexically_normal();
    const std::filesystem::path configured_root =
        benchmark_root.lexically_normal();
    if (requested_root != configured_root) {
      throw McpOperationFailure(
          "run_output_root_mismatch",
          "managed runs must use the editor host benchmark root", false);
    }
    options.output_dir = configured_root;
    return options;
  }

  static std::filesystem::path CleanupRunRoot(
      const std::filesystem::path& benchmark_root, std::string_view run_id) {
    std::error_code error;
    const std::filesystem::path absolute_root =
        std::filesystem::absolute(benchmark_root, error);
    if (error) {
      throw std::runtime_error("resolve editor host benchmark root failed: " +
                               error.message());
    }
    const std::filesystem::path canonical_root =
        std::filesystem::weakly_canonical(absolute_root, error);
    if (error) {
      throw std::runtime_error(
          "canonicalize editor host benchmark root failed: " + error.message());
    }
    return (canonical_root / run_id).lexically_normal();
  }

  static std::optional<OwnedRunRootIdentity> InspectRunRootIdentity(
      const std::filesystem::path& run_root,
      std::optional<std::chrono::steady_clock::time_point> deadline =
          std::nullopt,
      std::stop_token stop_token = {}) {
    const auto require_active = [&] {
      if (stop_token.stop_requested()) {
        throw std::runtime_error("retained run root inspection was cancelled");
      }
      if (deadline && std::chrono::steady_clock::now() >= *deadline) {
        throw std::runtime_error(
            "retained run root inspection deadline expired");
      }
    };
    require_active();
    const int descriptor =
        open(run_root.c_str(),
             O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
      const int open_error = errno;
      require_active();
      if (open_error == ENOENT) {
        return std::nullopt;
      }
      throw std::runtime_error(
          "open retained run root without following links failed: " +
          std::error_code(open_error, std::generic_category()).message());
    }

    struct stat opened{};
    const int inspect_result = fstat(descriptor, &opened);
    const int inspect_error = inspect_result == 0 ? 0 : errno;
    struct stat linked{};
    const int link_result =
        inspect_result == 0
            ? fstatat(AT_FDCWD, run_root.c_str(), &linked, AT_SYMLINK_NOFOLLOW)
            : -1;
    const int link_error = link_result == 0 ? 0 : errno;
    const int close_result = close(descriptor);
    const int close_error = close_result == 0 ? 0 : errno;
    require_active();
    if (inspect_result != 0) {
      throw std::runtime_error(
          "inspect retained run root failed: " +
          std::error_code(inspect_error, std::generic_category()).message());
    }
    if (link_result != 0) {
      throw std::runtime_error(
          "reinspect retained run root link failed: " +
          std::error_code(link_error, std::generic_category()).message());
    }
    if (close_result != 0) {
      throw std::runtime_error(
          "close retained run root failed: " +
          std::error_code(close_error, std::generic_category()).message());
    }
    if (!S_ISDIR(linked.st_mode) || opened.st_dev != linked.st_dev ||
        opened.st_ino != linked.st_ino) {
      throw std::runtime_error(
          "retained run root identity changed during no-follow inspection");
    }
    require_active();
    return OwnedRunRootIdentity{
        .device = static_cast<std::uintmax_t>(opened.st_dev),
        .inode = static_cast<std::uintmax_t>(opened.st_ino),
    };
  }

  static McpRunCleanupResult SuccessfulCleanupResult(std::string_view run_id) {
    return McpRunCleanupResult{
        .run_id = std::string(run_id),
        .verified_owned = true,
        .processes_remaining = 0U,
        .network_resources_remaining = 0U,
        .cgroups_remaining = 0U,
        .credentials_remaining = 0U,
        .complete = true,
    };
  }

  static EditorRunSnapshot SnapshotLocked(const EditorRunContext& context,
                                          bool acquire_tui_read_lease = false) {
    const std::uint32_t node_count =
        context.mcp_application->current_node_count();
    const std::uint32_t node_capacity = context.options->node_capacity;
    return EditorRunSnapshot{
        .generation = context.generation,
        .run_id = context.options->run_id,
        .run_root = BenchmarkRunRoot(*context.options),
        .chain = std::string(ChainKindName(context.options->chain)),
        .node_count = node_count,
        .node_capacity = node_capacity,
        .chain_node_maximum =
            ChainDriverSpecFor(context.options->chain).max_nodes,
        .available_node_capacity =
            node_count <= node_capacity ? node_capacity - node_count : 0U,
        .state = context.state,
        .command_queue = context.command_queue,
#ifdef BBP_FIRO_GUI_LAUNCHER
        .operator_connection_launcher = context.operator_connection_launcher,
#endif
        .mcp_application = context.mcp_application,
        .tui_read_lease = acquire_tui_read_lease
                              ? std::make_shared<EditorTuiReadLease>(
                                    context.tui_read_lease_state)
                              : std::shared_ptr<void>{}};
  }

  static void RequestStop(const std::shared_ptr<EditorRunContext>& context) {
    RecordRunStop(context->run_stop_tick, std::chrono::steady_clock::now());
    context->simulation_stop_source.request_stop();
    context->command_queue->Close();
  }

  static void RequestHostedStop(
      const std::shared_ptr<EditorRunContext>& context) {
    {
      std::lock_guard<std::mutex> lock(context->mutex);
      context->host_stop_requested = true;
      if (!IsTerminalEditorRunState(context->state)) {
        context->state = EditorRunState::kStopping;
        context->state_changed.notify_all();
      }
    }
    RequestStop(context);
  }

  static void WaitForApplicationStop(std::stop_token stop_token) {
    std::condition_variable_any stopped;
    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    static_cast<void>(stopped.wait(lock, stop_token, [] { return false; }));
  }

  static void WaitForTuiReadLeaseDrain(
      const std::shared_ptr<EditorRunContext>& context,
      std::optional<std::chrono::steady_clock::time_point> deadline,
      std::stop_token stop_token) {
    const std::shared_ptr<EditorTuiReadLeaseState> state =
        context->tui_read_lease_state;
    std::unique_lock<std::mutex> lock(state->mutex);
    const auto drained = [&] { return state->readers == 0U; };
    if (!deadline) {
      if (!state->drained.wait(lock, stop_token, drained)) {
        throw McpOperationCancelled();
      }
      return;
    }
    if (!state->drained.wait_until(lock, stop_token, *deadline, drained)) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      throw std::runtime_error(
          "TUI did not release the terminal run before the cleanup deadline");
    }
  }

  static void JoinAndShutdown(
      const std::shared_ptr<EditorRunContext>& context,
      bool propagate_worker_failure = true,
      bool* application_shutdown_succeeded = nullptr,
      std::optional<std::chrono::steady_clock::time_point> deadline =
          std::nullopt,
      std::stop_token stop_token = {},
      std::vector<std::jthread>* retired_workers = nullptr) {
    if (application_shutdown_succeeded != nullptr) {
      *application_shutdown_succeeded = false;
    }
    if (!context) {
      if (application_shutdown_succeeded != nullptr) {
        *application_shutdown_succeeded = true;
      }
      return;
    }
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      throw std::runtime_error(
          "managed run reaping did not start before the deadline");
    }
    if (context->worker.joinable()) {
      if (deadline || stop_token.stop_possible()) {
        {
          std::lock_guard<std::mutex> lock(context->mutex);
          if (!IsTerminalEditorRunState(context->state)) {
            throw std::logic_error(
                "cancelable managed run reaping requires terminal state");
          }
        }
        if (retired_workers == nullptr) {
          throw std::logic_error(
              "cancelable managed run reaping requires retired-worker "
              "storage");
        }
        retired_workers->push_back(std::move(context->worker));
      } else {
        context->worker.join();
      }
    }
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      throw std::runtime_error(
          "managed run worker did not join before the deadline");
    }
    std::exception_ptr worker_failure;
    {
      std::lock_guard<std::mutex> lock(context->mutex);
      worker_failure = context->failure;
    }
    std::exception_ptr shutdown_failure;
    try {
      if (deadline) {
        context->mcp_application->Shutdown(*deadline, stop_token);
      } else if (stop_token.stop_possible()) {
        context->mcp_application->Shutdown(stop_token);
      } else {
        context->mcp_application->Shutdown();
      }
    } catch (...) {
      shutdown_failure = std::current_exception();
    }
    if (!shutdown_failure && application_shutdown_succeeded != nullptr) {
      *application_shutdown_succeeded = true;
    }
    if (propagate_worker_failure && worker_failure && shutdown_failure) {
      throw std::runtime_error(
          "managed run failed: " + ExceptionMessage(worker_failure) +
          "; MCP application shutdown also failed: " +
          ExceptionMessage(shutdown_failure));
    }
    if (propagate_worker_failure && worker_failure) {
      std::rethrow_exception(worker_failure);
    }
    if (shutdown_failure) {
      std::rethrow_exception(shutdown_failure);
    }
  }

  void JoinRetiredWorkers() {
    for (std::jthread& worker : retired_workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    retired_workers_.clear();
  }

  void ReapTerminalRun(std::optional<std::chrono::steady_clock::time_point>
                           deadline = std::nullopt,
                       std::stop_token stop_token = {}) {
    std::shared_ptr<EditorRunContext> context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!active_) {
        return;
      }
      {
        std::lock_guard<std::mutex> run_lock(active_->mutex);
        if (!IsTerminalEditorRunState(active_->state)) {
          return;
        }
      }
      context = active_;
    }
    WaitForTuiReadLeaseDrain(context, deadline, stop_token);
    JoinAndShutdown(
        context, false, nullptr, deadline, stop_token,
        deadline || stop_token.stop_possible() ? &retired_workers_ : nullptr);
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_ == context) {
      active_.reset();
    }
  }

  std::unique_lock<std::timed_mutex> AcquireTransitionLock(
      std::stop_token stop_token) {
    std::unique_lock<std::timed_mutex> transition_lock(transition_mutex_,
                                                       std::defer_lock);
    while (!transition_lock.try_lock_for(std::chrono::milliseconds(25))) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
    }
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    return transition_lock;
  }

  std::shared_ptr<EditorRunContext> Launch(
      Options options, std::optional<boost::json::object> source_scenario,
      std::stop_token stop_token) {
    std::unique_lock<std::timed_mutex> transition_lock =
        AcquireTransitionLock(stop_token);
    return LaunchLocked(std::move(options), std::move(source_scenario),
                        stop_token);
  }

  std::shared_ptr<EditorRunContext> LaunchLocked(
      Options options, std::optional<boost::json::object> source_scenario,
      std::stop_token stop_token, bool reserve_replay_destination = false) {
    RequireSafeOutputDirectory(options.output_dir);
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    JoinRetiredWorkers();
    ReapTerminalRun(std::nullopt, stop_token);
    std::function<void(McpEvidenceRecord)> publish_evidence;
    std::function<void(std::string_view)> close_run_subscriptions;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (shutdown_) {
        throw McpOperationFailure(
            "application_stopping",
            "BBP is shutting down and cannot launch another run", false);
      }
      if (active_) {
        throw McpOperationFailure(
            "run_already_active",
            "a managed run is already starting, active, or stopping", true);
      }
      publish_evidence = publish_evidence_;
      close_run_subscriptions = close_run_subscriptions_;
    }

    const std::filesystem::path cleanup_run_root =
        CleanupRunRoot(options.output_dir, options.run_id);
    const std::optional<OwnedRunRootCleanupReceipt> durable_receipt =
        TryLoadOwnedRunRootCleanupReceipt(options.run_id, cleanup_run_root,
                                          std::nullopt, stop_token);
    auto receipt = cleanup_receipts_.find(options.run_id);
    bool retire_in_memory_receipt = false;
    if (durable_receipt) {
      if (receipt != cleanup_receipts_.end() &&
          (receipt->second.ownership != durable_receipt->ownership ||
           receipt->second.root_identity != durable_receipt->root_identity)) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "durable cleanup receipt disagrees with controller state", false);
      }
      const std::filesystem::path quarantine =
          OwnedRunRootCleanupQuarantinePath(durable_receipt->ownership);
      if (InspectRunRootIdentity(cleanup_run_root, std::nullopt, stop_token) ||
          InspectRunRootIdentity(quarantine, std::nullopt, stop_token)) {
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "an incomplete durable cleanup prevents reuse of this run id",
            false);
      }
      if (receipt != cleanup_receipts_.end()) {
        retire_in_memory_receipt = true;
      }
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
    } else if (receipt != cleanup_receipts_.end()) {
      if (InspectRunRootIdentity(cleanup_run_root, std::nullopt, stop_token) ||
          InspectRunRootIdentity(
              OwnedRunRootCleanupQuarantinePath(receipt->second.ownership),
              std::nullopt, stop_token)) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "a retained run identity must not be replaced by run.launch",
            false);
      }
      if (!receipt->second.complete) {
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "an incomplete cleanup receipt prevents reuse of this run id",
            false);
      }
      retire_in_memory_receipt = true;
    }

    std::shared_ptr<ReservedManagedRunRoot> reserved_run_root;
    if (reserve_replay_destination) {
      reserved_run_root = ReserveManagedReplayRunRoot(options);
    }

    auto context = std::make_shared<EditorRunContext>();
    context->generation = next_generation_++;
    context->options = std::make_shared<Options>(std::move(options));
    context->source_scenario = std::move(source_scenario);
    context->command_queue = std::make_shared<SimulationCommandQueue>();
    context->node_inventory =
        std::make_shared<RuntimeNodeInventory>(context->options->node_capacity);
    context->reserved_run_root = std::move(reserved_run_root);
    if (durable_receipt || retire_in_memory_receipt) {
      const std::string reused_run_id = context->options->run_id;
      context->run_root_prepared =
          [this, durable_receipt, retire_in_memory_receipt,
           reused_run_id](std::stop_token preparation_stop_token) {
            if (durable_receipt) {
              RemoveOwnedRunRootCleanupReceipt(*durable_receipt, std::nullopt,
                                               preparation_stop_token);
            }
            if (retire_in_memory_receipt) {
              cleanup_receipts_.erase(reused_run_id);
            }
          };
    }
    const std::weak_ptr<EditorRunContext> weak_context(context);
#ifdef BBP_FIRO_GUI_LAUNCHER
    std::unique_ptr<ChainDriver> launcher_driver =
        CreateChainDriver(context->options->chain);
    context->operator_connection_launcher =
        launcher_driver->CreateOperatorConnectionLauncher(
            [weak_context](std::string_view node_id,
                           std::stop_token stop_token) {
              ThrowIfStopRequested(stop_token);
              const std::shared_ptr<EditorRunContext> run = weak_context.lock();
              if (!run || !run->node_inventory || !run->options) {
                throw std::runtime_error(
                    "managed run operator launcher authority is unavailable");
              }

              const RuntimeNodeSnapshot snapshot =
                  run->node_inventory->Snapshot();
              const auto selected =
                  std::find_if(snapshot.begin(), snapshot.end(),
                               [node_id](const NodeRuntime& node) {
                                 return node.config.id == node_id;
                               });
              if (selected == snapshot.end()) {
                throw std::runtime_error(
                    "operator launcher references an unknown active node: " +
                    std::string(node_id));
              }

              ChainNodeConfig config;
              {
                auto process_guard = LockNodeProcessState(*selected);
                RequireNodeRunning(*selected, process_guard,
                                   "operator launcher");
                config = selected->config;
              }
              ThrowIfStopRequested(stop_token);

              std::unique_ptr<ChainDriver> driver =
                  CreateChainDriver(run->options->chain);
              std::optional<OperatorConnectionCommand> command =
                  driver->BuildOperatorConnectionCommand(
                      config, BenchmarkRunRoot(*run->options));
              if (!command) {
                throw std::runtime_error(
                    "the active chain has no operator launcher command");
              }
              ThrowIfStopRequested(stop_token);
              return OperatorConnectionLauncherAuthority{
                  .inventory_generation = snapshot.generation(),
                  .node_id = config.id,
                  .command = std::move(*command),
              };
            });
#endif
    context->mcp_application =
        std::make_shared<McpLiveApplication>(McpLiveApplication::Config{
            .run_id = context->options->run_id,
            .run_root = BenchmarkRunRoot(*context->options),
            .retained_run = std::nullopt,
            .options = context->options,
            .command_queue = context->command_queue,
#ifdef BBP_FIRO_GUI_LAUNCHER
            .operator_connection_launcher =
                context->operator_connection_launcher,
#endif
            .node_inventory_snapshot =
                [weak_context] {
                  const std::shared_ptr<EditorRunContext> run =
                      weak_context.lock();
                  if (!run || !run->node_inventory) {
                    throw std::runtime_error(
                        "managed run node inventory is unavailable");
                  }
                  const RuntimeNodeSnapshot snapshot =
                      run->node_inventory->Snapshot();
                  McpLiveNodeInventorySnapshot result{
                      .generation = snapshot.generation(), .node_ids = {}};
                  result.node_ids.reserve(snapshot.size());
                  for (const NodeRuntime& node : snapshot) {
                    result.node_ids.push_back(node.config.id);
                  }
                  return result;
                },
            .publication_mutex = RuntimePublicationMutex(),
            .request_run_stop =
                [weak_context] {
                  if (const std::shared_ptr<EditorRunContext> run =
                          weak_context.lock()) {
                    RequestHostedStop(run);
                  }
                },
            .run_started =
                [weak_context] {
                  if (const std::shared_ptr<EditorRunContext> run =
                          weak_context.lock()) {
                    std::lock_guard<std::mutex> lock(run->mutex);
                    if (run->state == EditorRunState::kStarting) {
                      run->reached_active = true;
                      run->state = EditorRunState::kActive;
                      run->state_changed.notify_all();
                    }
                  }
                },
            .run_stopping =
                [weak_context] {
                  if (const std::shared_ptr<EditorRunContext> run =
                          weak_context.lock()) {
                    std::lock_guard<std::mutex> lock(run->mutex);
                    if (!IsTerminalEditorRunState(run->state)) {
                      run->state = EditorRunState::kStopping;
                      run->state_changed.notify_all();
                    }
                  }
                },
            .run_stopped = {},
            .publish_evidence = std::move(publish_evidence),
            .close_run_subscriptions = std::move(close_run_subscriptions)});
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (active_) {
        throw std::logic_error(
            "managed run membership changed during serialized launch");
      }
      active_ = context;
      run_membership_revision_.fetch_add(1U, std::memory_order_release);
    }
    try {
      context->worker = std::jthread([this, context] {
        const auto finalize_lifecycle = [&context]() -> std::exception_ptr {
          try {
            context->mcp_application->MarkRunStopping();
            context->mcp_application->MarkRunStopped();
            return {};
          } catch (...) {
            return std::current_exception();
          }
        };
        const auto publish_terminal_summary =
            [&context](std::string_view state) -> std::exception_ptr {
          if (!context->retained_run_root_available) {
            return {};
          }
          try {
            const RuntimeNodeSnapshot nodes =
                context->node_inventory->Snapshot();
            if (nodes.size() > std::numeric_limits<std::uint32_t>::max()) {
              throw std::overflow_error(
                  "terminal retained run node count exceeds uint32");
            }
            WriteRetainedRunRegistrySummary(
                *context->options, state,
                static_cast<std::uint32_t>(nodes.size()));
            return {};
          } catch (...) {
            return std::current_exception();
          }
        };
        const auto log_summary_failure =
            [](std::exception_ptr failure) {
              if (failure) {
                BBP_LOG(error)
                    << "retained run registry summary publication failed: "
                    << ExceptionMessage(failure);
              }
            };
        const auto append_failure = [](std::exception_ptr primary,
                                       std::exception_ptr additional,
                                       std::string_view context) {
          if (!additional) {
            return primary;
          }
          if (!primary) {
            return additional;
          }
          return std::make_exception_ptr(std::runtime_error(
              ExceptionMessage(primary) + "; " + std::string(context) + ": " +
              ExceptionMessage(additional)));
        };
        try {
          const BenchmarkHeadlessResult completion =
              RunPreparedBenchmark(context);
          context->result = completion.result;
          const std::exception_ptr summary_failure = publish_terminal_summary(
              completion.terminal_outcome ==
                      BenchmarkTerminalOutcome::kCancelled
                  ? "cancelled"
                  : "finished");
          log_summary_failure(summary_failure);
          std::lock_guard<std::mutex> lock(context->mutex);
          if (!IsTerminalEditorRunState(context->state)) {
            context->state = EditorRunState::kStopped;
            run_membership_revision_.fetch_add(1U, std::memory_order_release);
          }
          context->state_changed.notify_all();
        } catch (const SimulationCancelled&) {
          std::exception_ptr failure = finalize_lifecycle();
          const std::exception_ptr summary_failure =
              publish_terminal_summary(failure ? "failed" : "cancelled");
          if (failure) {
            failure = append_failure(
                failure, summary_failure,
                "retained run registry summary publication failed");
          } else {
            log_summary_failure(summary_failure);
          }
          std::lock_guard<std::mutex> lock(context->mutex);
          const bool membership_was_visible =
              !IsTerminalEditorRunState(context->state);
          if (failure) {
            context->failure = failure;
            context->state = EditorRunState::kFailed;
          } else {
            context->result = 0;
            context->state = EditorRunState::kStopped;
          }
          if (membership_was_visible) {
            run_membership_revision_.fetch_add(1U, std::memory_order_release);
          }
          context->state_changed.notify_all();
        } catch (...) {
          std::exception_ptr failure = std::current_exception();
          failure = append_failure(failure, finalize_lifecycle(),
                                   "terminal launcher cleanup also failed");
          failure = append_failure(
              failure, publish_terminal_summary("failed"),
              "retained run registry summary publication failed");
          std::lock_guard<std::mutex> lock(context->mutex);
          const bool membership_was_visible =
              !IsTerminalEditorRunState(context->state);
          context->failure = failure;
          context->state = EditorRunState::kFailed;
          if (membership_was_visible) {
            run_membership_revision_.fetch_add(1U, std::memory_order_release);
          }
          context->state_changed.notify_all();
        }
      });
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (active_ == context) {
        active_.reset();
        run_membership_revision_.fetch_add(1U, std::memory_order_release);
      }
      throw;
    }
    return context;
  }

  mutable std::timed_mutex transition_mutex_;
  mutable std::mutex mutex_;
  std::vector<std::jthread> retired_workers_;
  std::map<std::string, RunCleanupReceipt, std::less<>> cleanup_receipts_;
  std::shared_ptr<EditorRunContext> active_;
  std::function<void(McpEvidenceRecord)> publish_evidence_;
  std::function<void(std::string_view)> close_run_subscriptions_;
  std::uint64_t next_generation_ = 1U;
  std::atomic<std::uint64_t> run_membership_revision_{0U};
  bool shutdown_ = false;
};

std::optional<McpHostedRunSnapshot> McpSnapshot(
    const EditorRunController& controller) {
  const std::optional<EditorRunSnapshot> snapshot =
      controller.CurrentRun(false);
  if (!snapshot) {
    return std::nullopt;
  }
  return McpHostedRunSnapshot{
      .generation = snapshot->generation,
      .run_id = snapshot->run_id,
      .state = std::string(EditorRunStateName(snapshot->state)),
      .chain = snapshot->chain,
      .node_count = snapshot->node_count,
      .node_capacity = snapshot->node_capacity,
      .chain_node_maximum = snapshot->chain_node_maximum,
      .available_node_capacity = snapshot->available_node_capacity,
      .application = snapshot->mcp_application,
  };
}

TuiRunSnapshot TuiSnapshot(const EditorRunController& controller) {
  const std::optional<EditorRunSnapshot> snapshot =
      controller.CurrentRun(false, true);
  if (!snapshot || snapshot->state == EditorRunState::kStarting) {
    return {};
  }
  return TuiRunSnapshot{
      .generation = snapshot->generation,
      .run_root = snapshot->run_root,
      .command_queue = snapshot->command_queue,
#ifdef BBP_FIRO_GUI_LAUNCHER
      .operator_connection_launcher = snapshot->operator_connection_launcher,
#endif
      .publication_mutex = RuntimePublicationMutex(),
      .read_lease = snapshot->tui_read_lease,
  };
}

void WaitForApplicationStop(std::stop_token stop_token) {
  std::condition_variable_any stopped;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  static_cast<void>(stopped.wait(lock, stop_token, [] { return false; }));
}

int RunEditorApplication(Options options,
                         const std::filesystem::path& state_directory) {
  SignalStopMonitor signal_monitor;
  std::stop_source application_stop_source;
  EnsureDirectory(options.output_dir);
  const std::filesystem::path benchmark_root =
      std::filesystem::canonical(options.output_dir);
  RequireSafeOutputDirectory(benchmark_root);
  options.output_dir = benchmark_root;
  EditorRunController run_controller;
  McpHostApplication host_application(McpHostApplication::Config{
      .host_id = options.run_id,
      .snapshot_run = [&] { return McpSnapshot(run_controller); },
      .snapshot_run_membership_revision =
          [&] { return run_controller.RunMembershipRevision(); },
      .snapshot_retained_runs =
          [benchmark_root](std::string_view active_run_id,
                           std::stop_token stop_token) {
            return DiscoverRetainedRuns(benchmark_root, active_run_id,
                                        stop_token);
          },
      .launch_run =
          [&](const boost::json::object& scenario, std::stop_token stop_token) {
            const std::shared_ptr<EditorRunContext> run =
                run_controller.LaunchScenario(scenario, benchmark_root,
                                              stop_token);
            const EditorRunSnapshot snapshot =
                run_controller.WaitUntilActive(run, stop_token);
            return McpRunLifecycleResult{
                .run_id = snapshot.run_id,
                .state = std::string(EditorRunStateName(snapshot.state)),
                .node_count = snapshot.node_count,
            };
          },
      .replay_run =
          [&](std::string_view source_run_id,
              std::optional<std::string> destination_run_id,
              std::stop_token stop_token) {
            const std::shared_ptr<EditorRunContext> run =
                run_controller.ReplayScenario(source_run_id,
                                              std::move(destination_run_id),
                                              benchmark_root, stop_token);
            const EditorRunSnapshot snapshot =
                run_controller.WaitUntilActive(run, stop_token);
            return McpRunLifecycleResult{
                .run_id = snapshot.run_id,
                .state = std::string(EditorRunStateName(snapshot.state)),
                .node_count = snapshot.node_count,
            };
          },
      .stop_run =
          [&](std::string_view run_id, std::chrono::seconds timeout,
              std::stop_token stop_token) {
            const EditorRunSnapshot snapshot =
                run_controller.StopRun(run_id, timeout, stop_token);
            return McpRunLifecycleResult{
                .run_id = snapshot.run_id,
                .state = std::string(EditorRunStateName(snapshot.state)),
                .node_count = snapshot.node_count,
            };
          },
      .clean_run =
          [&, benchmark_root](
              std::string_view run_id, std::chrono::seconds timeout,
              bool remove_retained_artifacts, std::stop_token stop_token) {
            return run_controller.CleanRun(benchmark_root, run_id, timeout,
                                           remove_retained_artifacts,
                                           stop_token);
          },
  });
  McpEndpoint mcp_endpoint(
      McpEndpointConfig{
          .state_directory = state_directory,
          .run_id = options.run_id,
          .server = {},
          .dispatcher = {},
          .allowed_operations = host_application.SupportedOperations(),
          .allowed_information_families =
              host_application.SupportedInformationFamilies(),
          .read_only = false,
      },
      host_application.OperationFactory(), host_application.ResourceReader());
  run_controller.SetEvidenceCallbacks(
      [&mcp_endpoint](McpEvidenceRecord record) {
        mcp_endpoint.PublishEvidence(std::move(record));
      },
      [&mcp_endpoint](std::string_view run_id) {
        mcp_endpoint.CloseRunSubscriptions(run_id);
      });
  std::stop_callback stop_on_signal(signal_monitor.GetToken(), [&] {
    application_stop_source.request_stop();
  });

  int result = 1;
  std::exception_ptr application_failure;
  try {
    mcp_endpoint.Start();
    const McpEndpointPublication publication = mcp_endpoint.publication();
    BBP_LOG(info) << "MCP endpoint listening at " << publication.endpoint
                  << "; client_config="
                  << publication.client_config_file.string();
    const TuiMcpConnectionInfo mcp_connection{
        .endpoint = publication.endpoint,
        .token_file = publication.token_file,
        .client_config_file = publication.client_config_file,
    };

    std::shared_ptr<EditorRunContext> initial_run;
    if (options.initial_run_requested) {
      initial_run = run_controller.LaunchOptions(options);
    }
    if (options.no_tui) {
      if (initial_run) {
        result = run_controller.WaitForInitialRun(
            initial_run, application_stop_source.get_token());
      } else {
        WaitForApplicationStop(application_stop_source.get_token());
        result = 0;
      }
    } else {
      SetConsoleLoggingEnabled(false);
      result = RunTuiReport([&] { return TuiSnapshot(run_controller); }, false,
                            options.tui_refresh_ms, mcp_connection,
                            application_stop_source.get_token());
      SetConsoleLoggingEnabled(true);
    }
  } catch (...) {
    SetConsoleLoggingEnabled(true);
    application_failure = std::current_exception();
  }

  std::exception_ptr cleanup_failure;
  const auto capture_cleanup_failure = [&](auto&& action) {
    try {
      action();
    } catch (...) {
      if (!cleanup_failure) {
        cleanup_failure = std::current_exception();
      }
    }
  };
  bool endpoint_drained = false;
  capture_cleanup_failure([&] {
    mcp_endpoint.StopAdmissionAndDrain();
    endpoint_drained = true;
  });
  if (endpoint_drained) {
    capture_cleanup_failure([&] { host_application.Shutdown(); });
    capture_cleanup_failure([&] { run_controller.Shutdown(); });
    capture_cleanup_failure([&] { mcp_endpoint.Stop(); });
  }

  if (application_failure) {
    std::rethrow_exception(application_failure);
  }
  if (cleanup_failure) {
    std::rethrow_exception(cleanup_failure);
  }
  if (signal_monitor.ReceivedSignal() != 0) {
    BBP_LOG(info) << "graceful shutdown completed after signal "
                  << signal_monitor.ReceivedSignal();
  }
  return result;
}

std::filesystem::path ResolveRunReference(
    const std::filesystem::path& benchmark_root,
    const std::filesystem::path& reference) {
  if (reference.is_absolute() || reference.has_parent_path() ||
      std::filesystem::exists(reference)) {
    return reference;
  }
  return benchmark_root / reference;
}

}  // namespace

#ifdef BBP_ENABLE_TEST_HOOKS
class LiveInstrumentationHarnessForTest::Impl {
 public:
  Impl(std::vector<std::string> node_ids,
       std::chrono::milliseconds default_sample_interval,
       std::filesystem::path run_root)
      : node_inventory_(CheckedNodeCapacity(node_ids.size())) {
    if (node_ids.empty()) {
      throw std::invalid_argument(
          "instrumentation test harness requires at least one node");
    }
    ValidateLiveInstrumentationDuration(default_sample_interval,
                                        "sample_interval");
    options_.run_id = "instrumentation-test";
    options_.nodes = static_cast<std::uint32_t>(node_ids.size());
    options_.metrics_interval = default_sample_interval;
    options_.topology.configured = true;
    options_.topology.node_count = options_.nodes;
    options_.topology.peer_topology.kind = PeerTopologyKind::kFullMesh;

    std::vector<NodeRuntime> nodes;
    nodes.reserve(node_ids.size());
    const std::vector<PerfCounterKind>& available = DefaultPerfCounterKinds();
    for (std::size_t index = 0U; index < node_ids.size(); ++index) {
      ValidateMcpIdentifier(node_ids[index], "test node id");
      if (!running_.emplace(node_ids[index], true).second) {
        throw std::invalid_argument(
            "instrumentation test node ids must be unique");
      }
      NodeRuntime node;
      node.config.id = std::move(node_ids[index]);
      node.run_process_state = &run_process_state_;
      node.perf_counter_kinds = {available[index % available.size()]};
      node.perf_counter_target_kind = PerfCounterTargetKind::kNode;
      node.perf_counter_target_id = node.config.id;
      node.perf_counter_target_pid = static_cast<pid_t>(100U + index);
      node.perf_counter_attached_pid = node.perf_counter_target_pid;
      node.perf_counter_process_generation = 10U + index;
      nodes.push_back(std::move(node));
    }
    node_inventory_.Initialize(nodes);
    wallet_registry_.Initialize(SimulationRegistry::FromTopology(
        options_.topology, options_.wallet_initialization));
    driver_ = CreateChainDriver(options_.chain);

    NodePerfCounterTransactionBackend backend;
    backend.is_running = [this](const NodeRuntime& node) {
      const auto found = running_.find(node.config.id);
      if (found == running_.end()) {
        throw std::logic_error(
            "instrumentation test backend found an unknown node");
      }
      return found->second;
    };
    backend.attach = [this](NodeRuntime& node,
                            const RunProcessState::Guard& guard,
                            bool require_attachment) {
      static_cast<void>(require_attachment);
      if (attachment_attempts_ == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error(
            "instrumentation test attachment sequence exceeds uint64");
      }
      ++attachment_attempts_;
      if (failed_attachment_attempt_ &&
          attachment_attempts_ == *failed_attachment_attempt_) {
        throw std::runtime_error("injected instrumentation attachment failure");
      }
      ResetNodePerfCounters(node, guard);
      node.perf_counter_process_generation = attachment_attempts_;
      if (node.perf_counter_target_kind == PerfCounterTargetKind::kNode ||
          node.perf_counter_target_kind == PerfCounterTargetKind::kWallet) {
        node.perf_counter_target_pid =
            static_cast<pid_t>(1000U + attachment_attempts_);
        node.perf_counter_attached_pid = node.perf_counter_target_pid;
      } else {
        node.perf_counter_cgroup_path =
            "/instrumentation-test/" + node.config.id;
        node.perf_counter_cpus = {0};
      }
    };
    backend.reset = [](NodeRuntime& node, const RunProcessState::Guard& guard) {
      ResetNodePerfCounters(node, guard);
    };
    LiveInstrumentationMeasurementCollector collector =
        [this](const std::set<std::string>& selected,
               std::stop_token stop_token) {
          ThrowIfStopRequested(stop_token);
          if (fail_next_sample_) {
            fail_next_sample_ = false;
            throw std::runtime_error(
                "injected instrumentation sampling failure");
          }
          if (sample_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error(
                "instrumentation test sample sequence exceeds uint64");
          }
          ++sample_sequence_;
          std::vector<std::string> records;
          records.reserve(selected.size());
          for (const std::string& node_id : selected) {
            records.push_back(boost::json::serialize(boost::json::object{
                {"node_id", node_id},
                {"test_sample", sample_sequence_},
            }));
          }
          return records;
        };
    std::filesystem::path metrics_path;
    std::filesystem::path events_path;
    if (!run_root.empty()) {
      EnsureDirectory(run_root);
      metrics_path = run_root / "metrics.jsonl";
      events_path = run_root / "events.jsonl";
    }
    controller_ = MakeLiveInstrumentationController(
        options_, std::move(metrics_path), std::move(events_path), *driver_,
        node_inventory_, wallet_registry_, run_process_state_,
        node_mutation_mutex_,
        {node_network_state_mutex, node_resource_state_mutex}, registry_,
        std::move(backend), std::move(collector));
    service_ = MakeLiveInstrumentationService(*controller_);
  }

  ~Impl() {
    try {
      Shutdown(false);
    } catch (...) {
    }
    service_.reset();
    controller_.reset();
  }

  std::shared_ptr<McpLiveInstrumentationService> service() const {
    return service_;
  }

  LiveInstrumentationNodeStateForTest NodeState(std::string_view node_id) {
    std::lock_guard<std::timed_mutex> mutation_lock(node_mutation_mutex_);
    RuntimeNodeSnapshot nodes = node_inventory_.Snapshot();
    NodeRuntime& node = FindNodeRuntimeById(nodes, std::string(node_id));
    auto process_guard = run_process_state_.Lock();
    static_cast<void>(process_guard);
    return LiveInstrumentationNodeStateForTest{
        .counters = node.perf_counter_kinds,
        .target_kind = node.perf_counter_target_kind,
        .target_id = node.perf_counter_target_id,
        .target_pid = node.perf_counter_target_pid,
        .attached_pid = node.perf_counter_attached_pid,
        .process_generation = node.perf_counter_process_generation,
        .cgroup_path = node.perf_counter_cgroup_path.string(),
        .cpus = node.perf_counter_cpus,
        .error_kind = node.perf_counter_error_kind,
        .error = node.perf_counter_error,
    };
  }

  void SetNodeRunning(std::string_view node_id, bool running) {
    std::lock_guard<std::timed_mutex> lock(node_mutation_mutex_);
    const auto found = running_.find(std::string(node_id));
    if (found == running_.end()) {
      throw std::invalid_argument(
          "instrumentation test backend found an unknown node");
    }
    found->second = running;
  }

  void FailAttachmentOnAttempt(std::optional<std::size_t> attempt) {
    std::lock_guard<std::timed_mutex> lock(node_mutation_mutex_);
    if (attempt && *attempt > std::numeric_limits<std::uint64_t>::max()) {
      throw std::invalid_argument(
          "instrumentation test attachment attempt exceeds uint64");
    }
    failed_attachment_attempt_ =
        attempt ? std::optional<std::uint64_t>(*attempt) : std::nullopt;
  }

  void FailNextSample() {
    std::lock_guard<std::timed_mutex> lock(node_mutation_mutex_);
    fail_next_sample_ = true;
  }

  std::uint64_t attachment_attempts() const {
    std::lock_guard<std::timed_mutex> lock(node_mutation_mutex_);
    return attachment_attempts_;
  }

  void ApplyPerfMutation(std::string_view node_id, PerfCounterKind counter) {
    simulator_app_internal::ApplyLiveInstrumentationPerfMutationForTest(
        *controller_, node_id, counter);
  }

  void SampleNow() {
    simulator_app_internal::SampleLiveInstrumentationNowForTest(*controller_);
  }

  void ExpireNow() {
    simulator_app_internal::ExpireLiveInstrumentationNowForTest(*controller_);
  }

  void SetExpiredWithoutWorkerWake() {
    simulator_app_internal::
        SetLiveInstrumentationExpiredWithoutWorkerWakeForTest(*controller_);
  }

  void Shutdown(bool run_failed) {
    if (shutdown_) {
      return;
    }
    ShutdownLiveInstrumentation(*controller_, run_failed);
    shutdown_ = true;
  }

 private:
  static std::uint32_t CheckedNodeCapacity(std::size_t count) {
    if (count == 0U || count > std::numeric_limits<std::uint32_t>::max()) {
      throw std::invalid_argument(
          "instrumentation test node count is out of range");
    }
    return static_cast<std::uint32_t>(count);
  }

  Options options_;
  std::unique_ptr<ChainDriver> driver_;
  RuntimeNodeInventory node_inventory_;
  RuntimeWalletRegistry wallet_registry_;
  RunProcessState run_process_state_;
  mutable std::timed_mutex node_mutation_mutex_;
  std::shared_ptr<simulator_app_internal::LiveInstrumentationRegistry>
      registry_ = MakeLiveInstrumentationRegistry();
  std::map<std::string, bool, std::less<>> running_;
  std::optional<std::uint64_t> failed_attachment_attempt_;
  std::uint64_t attachment_attempts_ = 0U;
  std::uint64_t sample_sequence_ = 0U;
  bool fail_next_sample_ = false;
  bool shutdown_ = false;
  LiveInstrumentationControllerPtr controller_;
  std::shared_ptr<McpLiveInstrumentationService> service_;
};

LiveInstrumentationHarnessForTest::LiveInstrumentationHarnessForTest(
    std::vector<std::string> node_ids,
    std::chrono::milliseconds default_sample_interval,
    std::filesystem::path run_root)
    : impl_(std::make_unique<Impl>(std::move(node_ids), default_sample_interval,
                                   std::move(run_root))) {}

LiveInstrumentationHarnessForTest::~LiveInstrumentationHarnessForTest() =
    default;

std::shared_ptr<McpLiveInstrumentationService>
LiveInstrumentationHarnessForTest::service() const {
  return impl_->service();
}

LiveInstrumentationNodeStateForTest
LiveInstrumentationHarnessForTest::NodeState(std::string_view node_id) const {
  return impl_->NodeState(node_id);
}

void LiveInstrumentationHarnessForTest::SetNodeRunning(std::string_view node_id,
                                                       bool running) {
  impl_->SetNodeRunning(node_id, running);
}

void LiveInstrumentationHarnessForTest::FailAttachmentOnAttempt(
    std::optional<std::size_t> attempt) {
  impl_->FailAttachmentOnAttempt(attempt);
}

void LiveInstrumentationHarnessForTest::FailNextSample() {
  impl_->FailNextSample();
}

std::uint64_t LiveInstrumentationHarnessForTest::attachment_attempts() const {
  return impl_->attachment_attempts();
}

void LiveInstrumentationHarnessForTest::ApplyPerfMutation(
    std::string_view node_id, PerfCounterKind counter) {
  impl_->ApplyPerfMutation(node_id, counter);
}

void LiveInstrumentationHarnessForTest::SampleNow() { impl_->SampleNow(); }

void LiveInstrumentationHarnessForTest::ExpireNow() { impl_->ExpireNow(); }

void LiveInstrumentationHarnessForTest::SetExpiredWithoutWorkerWake() {
  impl_->SetExpiredWithoutWorkerWake();
}

void LiveInstrumentationHarnessForTest::Shutdown(bool run_failed) {
  impl_->Shutdown(run_failed);
}

bool RuntimeNodeSupportDestructionAllowedForTest(
    bool daemon_absence_verified, bool exact_cgroup_acquired,
    bool exact_cgroup_empty, bool allow_partial_preparation) {
  return RuntimeNodeSupportDestructionAllowed(
      daemon_absence_verified, exact_cgroup_acquired, exact_cgroup_empty,
      allow_partial_preparation);
}

void SetRunCleanupRootRemovedHookForTest(std::function<void()> hook) {
  run_cleanup_root_removed_test_hook = std::move(hook);
}

McpRunCleanupResult CleanEditorRetainedRunForTest(
    const std::filesystem::path& benchmark_root, std::string_view run_id,
    std::chrono::seconds timeout, bool remove_retained_artifacts,
    std::stop_token stop_token) {
  EditorRunController controller;
  return controller.CleanRun(benchmark_root, run_id, timeout,
                             remove_retained_artifacts, stop_token);
}
#endif

Options ParseAndValidateScenario(const boost::json::object& scenario) {
  return ParseOptions(0, nullptr, &scenario);
}

boost::json::object ResolveScenario(const boost::json::object& scenario) {
  const Options options = ParseAndValidateScenario(scenario);
  return BuildResolvedScenarioDocument(options,
                                       ChainDriverSpecFor(options.chain));
}

SimulationCommand ParseAndValidateSimulationCommand(
    const boost::json::object& command, const Options& options) {
  const boost::json::value* kind_value = command.if_contains("kind");
  if (kind_value == nullptr || !kind_value->is_string()) {
    throw std::runtime_error(
        "runtime simulation command requires a string kind");
  }
  const std::string kind_name(kind_value->as_string());
  const std::optional<SimulationCommandKind> kind =
      SimulationCommandKindFromName(kind_name);
  if (!kind) {
    throw std::runtime_error("unsupported runtime simulation command: " +
                             kind_name);
  }
  if (command.if_contains("at") != nullptr ||
      command.if_contains("action") != nullptr) {
    throw std::runtime_error(
        "runtime simulation command must use kind, not at or action");
  }
  boost::json::object scheduled = command;
  scheduled.erase("kind");
  scheduled["at"] = "0ms";
  scheduled["action"] = kind_name;
  return ParseScheduledSimulationCommand(scheduled, *kind, options);
}

int SimulatorApp::Run(int argc, char** argv) {
  Options options = ParseOptions(argc, argv, nullptr);
  SetMinimumLogLevel(options.log_level);
  RequireSafeOutputDirectory(options.output_dir);
  ApplicationInstanceLock instance_lock;
  if (options.probe_network) {
    BBP_LOG(info) << simulator_app_internal::NetworkProbeJson();
    return 0;
  }
  if (!options.report_run.empty()) {
    BBP_LOG(info) << BuildRunReportJson(
        ResolveRunReference(options.output_dir, options.report_run));
    return 0;
  }
  if (!options.tui_run.empty()) {
    return RunRetainedTuiWithMcp(
        ResolveRunReference(options.output_dir, options.tui_run),
        instance_lock.state_directory(), options.tui_once,
        options.tui_refresh_ms);
  }
  if (options.probe_capabilities) {
    BBP_LOG(info) << simulator_app_internal::CapabilityProbeJson();
    return 0;
  }
  if (options.probe_cgroup_freeze) {
    BBP_LOG(info) << simulator_app_internal::CgroupFreezeProbeJson();
    return 0;
  }
  if (options.probe_drop_filter) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::DropFilterProbeJson();
    return 0;
  }
  if (options.probe_directional_network_condition) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info)
        << simulator_app_internal::DirectionalNetworkPolicyProbeJson();
    return 0;
  }
  if (options.probe_netns) {
    RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
    BBP_LOG(info) << simulator_app_internal::NetworkNamespaceProbeJson();
    return 0;
  }
  if (options.probe_veth) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::VethProbeJson();
    return 0;
  }
  if (options.probe_bandwidth_limit) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::BandwidthLimitProbeJson();
    return 0;
  }
  if (options.probe_network_condition) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::NetworkConditionProbeJson();
    return 0;
  }
  if (options.probe_combined_network_condition) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info)
        << simulator_app_internal::CombinedNetworkConditionProbeJson();
    return 0;
  }
  if (options.probe_network_condition_update) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::NetworkConditionUpdateProbeJson();
    return 0;
  }
  if (options.probe_address) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::AddressProbeJson();
    return 0;
  }
  if (options.probe_route) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::RouteProbeJson();
    return 0;
  }
  if (options.probe_qdisc) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::QdiscProbeJson();
    return 0;
  }
  if (options.probe_qdisc_mutation) {
    RequireNetworkSetupCapabilities();
    BBP_LOG(info) << simulator_app_internal::QdiscMutationProbeJson();
    return 0;
  }
  if (options.cleanup_run) {
    CleanupRun(options);
    return 0;
  }
  return RunEditorApplication(std::move(options),
                              instance_lock.state_directory());
}

}  // namespace bbp
