#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stop_token>
#include <thread>

#include "bbp/process.h"
#include "bbp/simulation_command.h"

BOOST_AUTO_TEST_CASE(simulation_command_kind_round_trips_names) {
  constexpr bbp::SimulationCommandKind kKinds[] = {
      bbp::SimulationCommandKind::kIncreaseLogVerbosity,
      bbp::SimulationCommandKind::kDecreaseLogVerbosity,
      bbp::SimulationCommandKind::kStopMining,
      bbp::SimulationCommandKind::kDisconnectNode,
      bbp::SimulationCommandKind::kReconnectNode,
      bbp::SimulationCommandKind::kSetBlockProductionPolicy,
      bbp::SimulationCommandKind::kSetMiningDifficulty,
      bbp::SimulationCommandKind::kKillNode,
      bbp::SimulationCommandKind::kConnectPeer,
      bbp::SimulationCommandKind::kDisconnectPeer,
      bbp::SimulationCommandKind::kSetPeerCountPolicy,
      bbp::SimulationCommandKind::kFreezeNode,
      bbp::SimulationCommandKind::kThawNode,
      bbp::SimulationCommandKind::kStopNode,
      bbp::SimulationCommandKind::kRestartNode,
      bbp::SimulationCommandKind::kGenerateBlocks,
      bbp::SimulationCommandKind::kSetResourceProfile,
      bbp::SimulationCommandKind::kSetResourceLimits,
      bbp::SimulationCommandKind::kSetNetworkProfile,
      bbp::SimulationCommandKind::kSetNetworkCondition,
      bbp::SimulationCommandKind::kBlockNetworkFlow,
      bbp::SimulationCommandKind::kUnblockNetworkFlow,
      bbp::SimulationCommandKind::kPartitionNodes,
      bbp::SimulationCommandKind::kHealPartition,
      bbp::SimulationCommandKind::kExportNodeReport,
      bbp::SimulationCommandKind::kSetPerfCounters,
      bbp::SimulationCommandKind::kSendWalletTransaction,
  };

  for (bbp::SimulationCommandKind kind : kKinds) {
    const std::optional<bbp::SimulationCommandKind> parsed =
        bbp::SimulationCommandKindFromName(
            bbp::SimulationCommandKindName(kind));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == kind);
  }
  BOOST_TEST(!bbp::SimulationCommandKindFromName("unknown"));
}

BOOST_AUTO_TEST_CASE(simulation_command_classifies_destructive_actions) {
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kKillNode));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kRestartNode));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSetNetworkProfile));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSetNetworkCondition));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSetResourceLimits));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kBlockNetworkFlow));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kPartitionNodes));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSendWalletTransaction));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kGenerateBlocks));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kThawNode));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kUnblockNetworkFlow));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kHealPartition));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kExportNodeReport));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSetPerfCounters));
}

BOOST_AUTO_TEST_CASE(simulation_command_cancellation_is_optional_and_shared) {
  bbp::SimulationCommand command;
  BOOST_TEST(!command.operation_control);

  command.operation_control = std::make_shared<bbp::SimulationCommandControl>();
  bbp::SimulationCommand admitted_command = command;
  BOOST_REQUIRE(admitted_command.operation_control);
  BOOST_TEST(!admitted_command.operation_control->stop_source.stop_requested());

  BOOST_TEST(command.operation_control->RequestCancellation(
      bbp::SimulationCommandCancellationCause::kClientCancel));
  BOOST_TEST(admitted_command.operation_control->stop_source.stop_requested());
  BOOST_CHECK(admitted_command.operation_control->cancellation_cause.load() ==
              bbp::SimulationCommandCancellationCause::kClientCancel);
  admitted_command.operation_control->restart_phase.store(
      bbp::SimulationNodeRestartPhase::kReplacementReady);
  BOOST_TEST(bbp::SimulationNodeRestartPhaseName(
                 command.operation_control->restart_phase.load()) ==
             "replacement_ready");
}

BOOST_AUTO_TEST_CASE(
    simulation_restart_cancellation_reads_back_signalled_process_exit) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-restart-reconciliation-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);

  bbp::ProcessSpec spec;
  spec.binary = "/bin/sleep";
  spec.argv = {"10"};
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";
  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  const bbp::SimulationNodeProcessObservation initial{
      .running = true,
      .pid = child.pid(),
      .restart_count = 7U,
  };
  BOOST_REQUIRE(child.RequestKill());

  std::size_t observations = 0U;
  const bbp::SimulationNodeRestartReconciliation reconciliation =
      bbp::ReconcileCancelledSimulationNodeRestart(
          initial, bbp::SimulationNodeRestartPhase::kStopRequested,
          std::chrono::steady_clock::now() + std::chrono::milliseconds(250),
          [&] {
            ++observations;
            if (observations == 1U) {
              // Model the exact production race: the stop/kill request has
              // committed but an immediate process snapshot still sees the
              // unchanged generation alive.
              return initial;
            }
            return bbp::SimulationNodeProcessObservation{
                .running = child.running(),
                .pid = child.pid(),
                .restart_count = 7U,
            };
          });
  BOOST_CHECK(reconciliation ==
              bbp::SimulationNodeRestartReconciliation::kStopped);
  BOOST_TEST(observations >= 2U);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  BOOST_TEST(!child.running());
  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(
    simulation_restart_cancellation_recognizes_rpc_ready_replacement) {
  const bbp::SimulationNodeProcessObservation initial{
      .running = true,
      .pid = 100,
      .restart_count = 7U,
  };
  const bbp::SimulationNodeRestartReconciliation reconciliation =
      bbp::ReconcileCancelledSimulationNodeRestart(
          initial, bbp::SimulationNodeRestartPhase::kReplacementReady,
          std::chrono::steady_clock::now() + std::chrono::milliseconds(250),
          [] {
            return bbp::SimulationNodeProcessObservation{
                .running = true,
                .pid = 101,
                .restart_count = 8U,
            };
          });
  BOOST_CHECK(reconciliation ==
              bbp::SimulationNodeRestartReconciliation::kReplacementReady);
}
