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
#include "simulator_editor_application.h"
#include "simulator_event_writing.h"
#include "simulator_height_wait_readback.h"
#include "simulator_host_probes.h"
#include "simulator_initial_peer_connectivity.h"
#include "simulator_json_field_decoding.h"
#include "simulator_live_block_generation_control.h"
#include "simulator_live_block_generation_workload_launcher.h"
#include "simulator_live_command_execution.h"
#include "simulator_live_height_wait_control.h"
#include "simulator_live_height_wait_workload_launcher.h"
#include "simulator_live_instrumentation_controller.h"
#include "simulator_live_lifecycle_supervisor.h"
#include "simulator_live_masternode_operations.h"
#include "simulator_live_miner_addition.h"
#include "simulator_live_miner_removal.h"
#include "simulator_live_peer_wait_workload_launcher.h"
#include "simulator_live_telemetry_collectors.h"
#include "simulator_live_wait_for_peers_control.h"
#include "simulator_live_wallet_addition.h"
#include "simulator_live_wallet_removal.h"
#include "simulator_live_wallet_workload_control.h"
#include "simulator_live_wallet_workload_launcher.h"
#include "simulator_live_workload_reading.h"
#include "simulator_live_workload_service_binding.h"
#include "simulator_live_workload_shutdown.h"
#include "simulator_live_workload_state.h"
#include "simulator_managed_run_root.h"
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
#include "simulator_one_shot_workload_dispatch.h"
#include "simulator_one_shot_workload_invocation.h"
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
using simulator_app_internal::AddLiveMinerRoles;
using simulator_app_internal::AddLiveWalletRoles;
using simulator_app_internal::AddRuntimeNodesTransactional;
using simulator_app_internal::ApplyDeclarativeStopDuringStart;
using simulator_app_internal::ApplyNetworkBlockRules;
using simulator_app_internal::ApplyNetworkPartitionRules;
using simulator_app_internal::ApplyNetworkProfileSwitch;
using simulator_app_internal::ApplyNodeConditions;
using simulator_app_internal::ApplyPerfCounterCommand;
using simulator_app_internal::ApplyResourceLimitPatches;
using simulator_app_internal::ApplyResourceLimitUpdate;
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
using simulator_app_internal::AttachNodePerfCounters;
using simulator_app_internal::BenchmarkHeadlessResult;
using simulator_app_internal::BenchmarkRunRoot;
using simulator_app_internal::BenchmarkTerminalOutcome;
using simulator_app_internal::CombinedStopToken;
using simulator_app_internal::ConsecutiveNodeIndexes;
using simulator_app_internal::DirectionalNetworkPoliciesForNode;
using simulator_app_internal::DiscoverRetainedRuns;
using simulator_app_internal::DispatchOneShotWorkload;
using simulator_app_internal::DynamicDirectionalNetworkPolicies;
using simulator_app_internal::DynamicPhysicalTopologyPeerEndpoints;
using simulator_app_internal::DynamicRestartPeerEndpoints;
using simulator_app_internal::DynamicTopologyPeerIds;
using simulator_app_internal::EditorApplicationDependencies;
using simulator_app_internal::EffectiveNodeBinary;
using simulator_app_internal::EffectiveNodeChainNetwork;
using simulator_app_internal::EffectiveNodeExtraArgs;
using simulator_app_internal::EffectiveNodeLifecyclePolicy;
using simulator_app_internal::EffectiveNodeWalletConfig;
using simulator_app_internal::ElapsedMilliseconds;
using simulator_app_internal::ExecuteLiveMasternodeOperation;
using simulator_app_internal::ExpectedTransactionLoadObservations;
using simulator_app_internal::ExportNodeReport;
using simulator_app_internal::FindPeerConnectivityPolicy;
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
using simulator_app_internal::IsTopologyEdgeConditionField;
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
using simulator_app_internal::LiveBlockGenerationWorkloadJson;
using simulator_app_internal::LiveBlockGenerationWorkloadRecord;
using simulator_app_internal::LiveBlockGenerationWorkloadRegistry;
using simulator_app_internal::LiveCommandExecutionContext;
using simulator_app_internal::LiveInstrumentationControllerPtr;
using simulator_app_internal::LiveInstrumentationMeasurementCollector;
using simulator_app_internal::LiveLifecycleSupervisorContext;
using simulator_app_internal::LiveMasternodeOperationContext;
using simulator_app_internal::LiveMinerAdditionContext;
using simulator_app_internal::LiveMinerRemovalContext;
using simulator_app_internal::LiveTelemetryCollectorsContext;
using simulator_app_internal::LiveWaitForPeersWorkloadJson;
using simulator_app_internal::LiveWaitForPeersWorkloadRecord;
using simulator_app_internal::LiveWaitForPeersWorkloadRegistry;
using simulator_app_internal::LiveWaitUntilHeightWorkloadJson;
using simulator_app_internal::LiveWaitUntilHeightWorkloadRecord;
using simulator_app_internal::LiveWaitUntilHeightWorkloadRegistry;
using simulator_app_internal::LiveWalletAdditionContext;
using simulator_app_internal::LiveWalletRemovalContext;
using simulator_app_internal::LiveWalletWorkloadRecord;
using simulator_app_internal::LiveWalletWorkloadRegistry;
using simulator_app_internal::LiveWalletWorkloadRequest;
using simulator_app_internal::LiveWalletWorkloadState;
using simulator_app_internal::LiveWalletWorkloadStateName;
using simulator_app_internal::LiveWorkloadRequest;
using simulator_app_internal::LiveWorkloadServiceBindingContext;
using simulator_app_internal::LiveWorkloadShutdownState;
using simulator_app_internal::LoadRetainedSourceScenario;
using simulator_app_internal::LockNodeProcessState;
using simulator_app_internal::MakeLiveBlockGenerationOperation;
using simulator_app_internal::MakeLiveBlockGenerationWorkloadLauncher;
using simulator_app_internal::MakeLiveChainCommandExecutor;
using simulator_app_internal::MakeLiveHeightWaitWorkloadLauncher;
using simulator_app_internal::MakeLiveInstrumentationController;
using simulator_app_internal::MakeLiveInstrumentationRegistry;
using simulator_app_internal::MakeLiveInstrumentationService;
using simulator_app_internal::MakeLiveNodeLogCollector;
using simulator_app_internal::MakeLivePeerWaitWorkloadLauncher;
using simulator_app_internal::MakeLivePeriodicMetricsCollector;
using simulator_app_internal::MakeLiveSimulationCommandProcessor;
using simulator_app_internal::MakeLiveWaitForPeersOperation;
using simulator_app_internal::MakeLiveWaitUntilHeightOperation;
using simulator_app_internal::MakeLiveWalletWorkloadLauncher;
using simulator_app_internal::MakeLiveWalletWorkloadOperation;
using simulator_app_internal::MakeLiveWorkloadServiceBinding;
using simulator_app_internal::MakeNodeVethConfig;
using simulator_app_internal::MakeOneShotWorkloadInvoker;
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
using simulator_app_internal::OneShotWorkloadContext;
using simulator_app_internal::OperatorWalletTransactionDetail;
using simulator_app_internal::ParseAmountDistribution;
using simulator_app_internal::ParseAndValidateLiveBlockGenerationWorkload;
using simulator_app_internal::ParseAndValidateLiveWaitForPeersWorkload;
using simulator_app_internal::ParseAndValidateLiveWaitUntilHeightWorkload;
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
using simulator_app_internal::PrepareNodeRuntime;
using simulator_app_internal::ProcessExitDetail;
using simulator_app_internal::PublishOperatorConnectionCommand;
using simulator_app_internal::RawTransactionDetail;
using simulator_app_internal::ReadLiveWorkloads;
using simulator_app_internal::RecordAndPublishGeneratedBlockWorkloadBoundary;
using simulator_app_internal::RecordGeneratedBlocks;
using simulator_app_internal::RecordRunStop;
using simulator_app_internal::RejectTopologyEdgeConditionFields;
using simulator_app_internal::RejectUnsupportedFields;
using simulator_app_internal::RejectUnsupportedScenarioActionFields;
using simulator_app_internal::RemoveLiveMinerRoles;
using simulator_app_internal::RemoveLiveWalletRoles;
using simulator_app_internal::RemovePreparedRunRoot;
using simulator_app_internal::RemoveRuntimeNodesTransactional;
using simulator_app_internal::ReplaceNodeNetworkConditionTransactional;
using simulator_app_internal::ReplaceRuntimeNodeTransactional;
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
using simulator_app_internal::RestartPolicyAppliedDetail;
using simulator_app_internal::RestartRequestedDetail;
using simulator_app_internal::RunEditorApplication;
using simulator_app_internal::RunLiveLifecycleSupervisor;
using simulator_app_internal::RunningNodeProcessGeneration;
using simulator_app_internal::RunRetainedTuiWithMcp;
using simulator_app_internal::RunStopTick;
using simulator_app_internal::RuntimeMasternodeIdentityJson;
using simulator_app_internal::RuntimeNodeAdditionDependencies;
using simulator_app_internal::RuntimeNodeAdditionRole;
using simulator_app_internal::RuntimeNodeAddResult;
using simulator_app_internal::RuntimeNodeRemovalDependencies;
using simulator_app_internal::RuntimeNodeRemoveResult;
using simulator_app_internal::RuntimeNodeReplacementDependencies;
using simulator_app_internal::RuntimeNodeReplaceResult;
using simulator_app_internal::RuntimeNodeResourceEntryFor;
using simulator_app_internal::RuntimeNodeResourceManifestFor;
using simulator_app_internal::RuntimeNodeSupportDestructionAllowed;
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
using simulator_app_internal::StopLiveWorkloads;
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
  std::mutex configured_miner_node_ids_mutex;
  std::vector<std::string> miner_node_ids;
  const auto is_configured_miner = [&](std::string_view node_id) {
    std::lock_guard<std::mutex> lock(configured_miner_node_ids_mutex);
    return std::find(miner_node_ids.begin(), miner_node_ids.end(), node_id) !=
           miner_node_ids.end();
  };
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
  LiveWorkloadShutdownState workload_shutdown;
  std::atomic<std::shared_ptr<McpLiveRoleService>> command_role_service;
  std::mutex lifecycle_failure_mutex;
  std::timed_mutex node_mutation_mutex;
  std::timed_mutex one_shot_workload_mutex;
  std::uint64_t next_one_shot_invocation = 1U;
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
  const auto stop_wallet_workloads = [&](bool run_failed) {
    StopLiveWorkloads(options, events_path, mcp_application,
                      installed_workload_service, wallet_workloads,
                      block_generation_workloads, wait_until_height_workloads,
                      wait_for_peers_workloads, run_stop_tick,
                      workload_shutdown, run_failed);
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
      if (!workload_shutdown.safe_to_destroy) {
        BBP_LOG(error)
            << "workload shutdown timeout escaped before a safe drain";
        std::terminate();
      }
      BBP_LOG(error) << "wallet workload shutdown failed during run cleanup: "
                     << error.what();
      return std::current_exception();
    } catch (const std::exception& error) {
      if (!workload_shutdown.safe_to_destroy) {
        BBP_LOG(error)
            << "workload shutdown failure escaped before a safe drain";
        std::terminate();
      }
      BBP_LOG(error) << "wallet workload shutdown failed during run cleanup: "
                     << error.what();
      return std::current_exception();
    } catch (...) {
      if (!workload_shutdown.safe_to_destroy) {
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
      const LiveCommandExecutionContext command_context{
          .options = options,
          .run_root = run_root,
          .events_path = events_path,
          .chain_spec = chain_spec,
          .driver = driver,
          .mcp_application = mcp_application,
          .node_inventory = node_inventory,
          .runtime_wallet_registry = runtime_wallet_registry,
          .block_scheduler = block_scheduler,
          .peer_connectivity_controller = peer_connectivity_controller,
          .chain_command_executor = chain_command_executor,
          .configured_miner_node_ids_mutex = configured_miner_node_ids_mutex,
          .miner_node_ids = miner_node_ids,
          .node_mutation_mutex = node_mutation_mutex,
          .block_generation_mutex = block_generation_mutex,
          .runtime_topology_mutex = runtime_topology_mutex,
          .node_network_state_mutex = node_network_state_mutex,
          .node_resource_state_mutex = node_resource_state_mutex,
          .runtime_topology = runtime_topology,
          .live_topology_config = live_topology_config,
          .run_process_state = run_process_state,
          .lifecycle_epoch = lifecycle_epoch,
          .command_rpc_stop_source = command_rpc_stop_source,
          .wallets_initialized = wallets_initialized,
          .transaction_tracker = transaction_tracker,
          .wallet_workloads = wallet_workloads,
          .block_generation_workloads = block_generation_workloads,
          .wait_until_height_workloads = wait_until_height_workloads,
          .wait_for_peers_workloads = wait_for_peers_workloads,
          .live_instrumentation = live_instrumentation,
          .runtime_node_addition_dependencies =
              runtime_node_addition_dependencies,
          .runtime_node_removal_dependencies =
              runtime_node_removal_dependencies,
          .runtime_node_replacement_dependencies =
              runtime_node_replacement_dependencies,
          .command_role_service = command_role_service,
          .is_configured_miner = is_configured_miner,
          .request_simulation_stop = request_simulation_stop,
          .record_scheduled_command_outcome = record_scheduled_command_outcome,
          .acquire_node_mutation_lock = AcquireNodeMutationLock,
          .acquire_runtime_publication_lock = AcquireRuntimePublicationLock,
          .find_node_runtime_by_id =
              [](const RuntimeNodeSnapshot& current_nodes,
                 const std::string& node_id) -> NodeRuntime& {
            return FindNodeRuntimeById(current_nodes, node_id);
          },
          .exception_message = ExceptionMessage,
          .start_node = StartNodeProcessWithPolicy,
      };
      chain_command_executor = MakeLiveChainCommandExecutor(command_context);
      command_processor = MakeLiveSimulationCommandProcessor(
          *active_command_queue, command_context);
    }
    if (block_scheduler) {
      for (const NodeRuntime& node : nodes) {
        if (is_configured_miner(node.config.id) && !node.AllowsChainMetrics()) {
          block_scheduler->StopMiner(node.config.id);
        }
      }
    }
    lifecycle_supervisor.emplace(
        RunLiveLifecycleSupervisor,
        LiveLifecycleSupervisorContext{
            .options = options,
            .run_root = run_root,
            .events_path = events_path,
            .chain_spec = chain_spec,
            .driver = driver,
            .node_inventory = node_inventory,
            .run_process_state = run_process_state,
            .node_mutation_mutex = node_mutation_mutex,
            .node_network_state_mutex = node_network_state_mutex,
            .lifecycle_epoch = lifecycle_epoch,
            .operator_connection_resolved = operator_connection_resolved,
            .lifecycle_failure_mutex = lifecycle_failure_mutex,
            .lifecycle_failure = lifecycle_failure,
            .mcp_application = mcp_application,
            .block_scheduler = block_scheduler,
            .peer_connectivity_controller = peer_connectivity_controller,
            .is_configured_miner = is_configured_miner,
            .request_simulation_stop = request_simulation_stop,
            .stop_token = stop_token,
            .acquire_node_mutation_lock = AcquireNodeMutationLock,
            .start_node = StartNodeProcessWithPolicy,
        });
    const LiveTelemetryCollectorsContext telemetry_context{
        .options = options,
        .events_path = events_path,
        .metrics_path = metrics_path,
        .wallet_metrics_path = wallet_metrics_path,
        .driver = driver,
        .node_inventory = node_inventory,
        .runtime_wallet_registry = runtime_wallet_registry,
        .run_process_state = run_process_state,
        .metrics_synchronization = {node_network_state_mutex,
                                    node_resource_state_mutex},
        .metrics_rpc_stop_source = metrics_rpc_stop_source,
        .wallets_initialized = wallets_initialized,
        .metrics_collector = metrics_collector,
        .stop_token = stop_token,
        .acquire_runtime_publication_lock = AcquireRuntimePublicationLock,
    };
    log_collector = MakeLiveNodeLogCollector(telemetry_context);
    log_collector->Start();
    metrics_collector = MakeLivePeriodicMetricsCollector(telemetry_context);
    metrics_collector->Start();
    InitializeWalletNodes(options, events_path, driver, nodes,
                          simulation_registry, stop_token);
    runtime_wallet_registry.Initialize(std::move(simulation_registry));
    wallets_initialized.store(true, std::memory_order_release);
    auto instrumentation_service =
        MakeLiveInstrumentationService(*instrumentation_controller);
    mcp_application.SetInstrumentationService(instrumentation_service);
    installed_instrumentation_service = std::move(instrumentation_service);
    auto [workload_service, launch_wallet_workload, execute_one_shot_workload] =
        MakeLiveWorkloadServiceBinding(LiveWorkloadServiceBindingContext{
            .options = options,
            .events_path = events_path,
            .metrics_path = metrics_path,
            .wallet_metrics_path = wallet_metrics_path,
            .chain_spec = chain_spec,
            .driver = driver,
            .node_inventory = node_inventory,
            .runtime_wallet_registry = runtime_wallet_registry,
            .transaction_tracker = transaction_tracker,
            .run_process_state = run_process_state,
            .peer_connectivity_controller = peer_connectivity_controller,
            .runtime_topology = runtime_topology,
            .live_topology_config = live_topology_config,
            .mcp_application = mcp_application,
            .node_mutation_mutex = node_mutation_mutex,
            .one_shot_workload_mutex = one_shot_workload_mutex,
            .block_generation_mutex = block_generation_mutex,
            .node_network_state_mutex = node_network_state_mutex,
            .node_resource_state_mutex = node_resource_state_mutex,
            .runtime_topology_mutex = runtime_topology_mutex,
            .lifecycle_epoch = lifecycle_epoch,
            .next_one_shot_invocation = next_one_shot_invocation,
            .run_stop_tick = run_stop_tick,
            .wallet_workloads = wallet_workloads,
            .block_generation_workloads = block_generation_workloads,
            .wait_until_height_workloads = wait_until_height_workloads,
            .wait_for_peers_workloads = wait_for_peers_workloads,
            .request_simulation_stop = request_simulation_stop,
            .acquire_node_mutation_lock = AcquireNodeMutationLock,
            .start_node = StartNodeProcessWithPolicy,
            .stop_token = stop_token,
        });
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
        return ExecuteLiveMasternodeOperation(
            LiveMasternodeOperationContext{
                .options = options,
                .run_root = run_root,
                .events_path = events_path,
                .chain_spec = chain_spec,
                .driver = driver,
                .mcp_application = mcp_application,
                .node_inventory = node_inventory,
                .runtime_wallet_registry = runtime_wallet_registry,
                .block_scheduler = block_scheduler,
                .configured_miner_node_ids_mutex =
                    configured_miner_node_ids_mutex,
                .miner_node_ids = miner_node_ids,
                .node_mutation_mutex = node_mutation_mutex,
                .block_generation_mutex = block_generation_mutex,
                .runtime_topology_mutex = runtime_topology_mutex,
                .peer_connectivity_controller = peer_connectivity_controller,
                .runtime_topology = runtime_topology,
                .live_topology_config = live_topology_config,
                .run_process_state = run_process_state,
                .lifecycle_epoch = lifecycle_epoch,
                .runtime_node_addition_dependencies =
                    runtime_node_addition_dependencies,
                .start_node = StartNodeProcessWithPolicy,
                .request_simulation_stop = request_simulation_stop,
                .acquire_node_mutation_lock = AcquireNodeMutationLock,
                .acquire_runtime_publication_lock =
                    AcquireRuntimePublicationLock,
                .exception_message = ExceptionMessage,
            },
            kind, arguments, operation_stop_token);
      }
      if (kind == McpOperationKind::kRemoveWallet) {
        return RemoveLiveWalletRoles(
            LiveWalletRemovalContext{
                .options = options,
                .events_path = events_path,
                .mcp_application = mcp_application,
                .node_inventory = node_inventory,
                .runtime_wallet_registry = runtime_wallet_registry,
                .wallet_workloads = wallet_workloads,
                .node_mutation_mutex = node_mutation_mutex,
                .request_simulation_stop = request_simulation_stop,
                .acquire_node_mutation_lock = AcquireNodeMutationLock,
                .acquire_runtime_publication_lock =
                    AcquireRuntimePublicationLock,
                .exception_message = ExceptionMessage,
            },
            arguments, operation_stop_token);
      }
      if (kind == McpOperationKind::kRemoveMiner) {
        return RemoveLiveMinerRoles(
            LiveMinerRemovalContext{
                .options = options,
                .events_path = events_path,
                .mcp_application = mcp_application,
                .node_inventory = node_inventory,
                .runtime_wallet_registry = runtime_wallet_registry,
                .block_scheduler = block_scheduler,
                .configured_miner_node_ids_mutex =
                    configured_miner_node_ids_mutex,
                .miner_node_ids = miner_node_ids,
                .node_mutation_mutex = node_mutation_mutex,
                .request_simulation_stop = request_simulation_stop,
                .acquire_node_mutation_lock = AcquireNodeMutationLock,
                .acquire_runtime_publication_lock =
                    AcquireRuntimePublicationLock,
                .exception_message = ExceptionMessage,
            },
            arguments, operation_stop_token);
      }
      if (kind == McpOperationKind::kAddMiner) {
        return AddLiveMinerRoles(
            LiveMinerAdditionContext{
                .options = options,
                .run_root = run_root,
                .events_path = events_path,
                .chain_spec = chain_spec,
                .driver = driver,
                .mcp_application = mcp_application,
                .node_inventory = node_inventory,
                .runtime_wallet_registry = runtime_wallet_registry,
                .block_scheduler = block_scheduler,
                .configured_miner_node_ids_mutex =
                    configured_miner_node_ids_mutex,
                .miner_node_ids = miner_node_ids,
                .node_mutation_mutex = node_mutation_mutex,
                .runtime_topology_mutex = runtime_topology_mutex,
                .peer_connectivity_controller = peer_connectivity_controller,
                .runtime_topology = runtime_topology,
                .live_topology_config = live_topology_config,
                .run_process_state = run_process_state,
                .lifecycle_epoch = lifecycle_epoch,
                .runtime_node_addition_dependencies =
                    runtime_node_addition_dependencies,
                .request_simulation_stop = request_simulation_stop,
                .acquire_node_mutation_lock = AcquireNodeMutationLock,
                .acquire_runtime_publication_lock =
                    AcquireRuntimePublicationLock,
                .exception_message = ExceptionMessage,
            },
            arguments, operation_stop_token);
      }
      if (kind != McpOperationKind::kAddWallet) {
        throw std::logic_error("unknown live role mutation operation");
      }
      return AddLiveWalletRoles(
          LiveWalletAdditionContext{
              .options = options,
              .run_root = run_root,
              .events_path = events_path,
              .chain_spec = chain_spec,
              .driver = driver,
              .mcp_application = mcp_application,
              .node_inventory = node_inventory,
              .runtime_wallet_registry = runtime_wallet_registry,
              .block_scheduler = block_scheduler,
              .configured_miner_node_ids_mutex =
                  configured_miner_node_ids_mutex,
              .miner_node_ids = miner_node_ids,
              .node_mutation_mutex = node_mutation_mutex,
              .runtime_topology_mutex = runtime_topology_mutex,
              .peer_connectivity_controller = peer_connectivity_controller,
              .runtime_topology = runtime_topology,
              .live_topology_config = live_topology_config,
              .run_process_state = run_process_state,
              .lifecycle_epoch = lifecycle_epoch,
              .runtime_node_addition_dependencies =
                  runtime_node_addition_dependencies,
              .request_simulation_stop = request_simulation_stop,
              .acquire_node_mutation_lock = AcquireNodeMutationLock,
              .acquire_runtime_publication_lock = AcquireRuntimePublicationLock,
              .exception_message = ExceptionMessage,
          },
          arguments, operation_stop_token);
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
  return simulator_app_internal::CleanEditorRetainedRunForTest(
      benchmark_root, run_id, timeout, remove_retained_artifacts, stop_token,
      run_cleanup_root_removed_test_hook,
      EditorApplicationDependencies{
          .run_benchmark_headless = RunBenchmarkHeadless,
          .exception_message = ExceptionMessage,
          .require_safe_output_directory = RequireSafeOutputDirectory,
          .runtime_publication_mutex = RuntimePublicationMutex,
      });
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
  return RunEditorApplication(
      std::move(options), instance_lock.state_directory(),
#ifdef BBP_ENABLE_TEST_HOOKS
      run_cleanup_root_removed_test_hook,
#endif
      EditorApplicationDependencies{
          .run_benchmark_headless = RunBenchmarkHeadless,
          .exception_message = ExceptionMessage,
          .require_safe_output_directory = RequireSafeOutputDirectory,
          .runtime_publication_mutex = RuntimePublicationMutex,
      });
}

}  // namespace bbp
