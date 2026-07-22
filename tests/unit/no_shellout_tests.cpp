#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/util.h"

namespace {

bool IsSourceFile(const std::filesystem::path& path) {
  const std::string extension = path.extension().string();
  return extension == ".cpp" || extension == ".h" || extension == ".cc" ||
         extension == ".hpp";
}

void CheckDirectoryForForbiddenTokens(const std::filesystem::path& directory) {
  const std::vector<std::string_view> forbidden = {
      "system(",       "system (",       "popen(",  "popen (",
      "execvp(",       "execvp (",       "execlp(", "execlp (",
      "posix_spawnp(", "posix_spawnp (", "/bin/sh", "bash -c",
  };

  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(directory)) {
    if (!entry.is_regular_file() || !IsSourceFile(entry.path())) {
      continue;
    }
    const std::string text = bbp::ReadText(entry.path());
    for (std::string_view token : forbidden) {
      if (text.find(token) != std::string::npos) {
        BOOST_FAIL("forbidden shell-out token '" << token << "' in "
                                                 << entry.path().string());
      }
    }
  }
}

}  // namespace

BOOST_AUTO_TEST_CASE(simulator_source_does_not_shell_out) {
  const std::filesystem::path source_root = BBP_SOURCE_DIR;
  CheckDirectoryForForbiddenTokens(source_root / "include");
  CheckDirectoryForForbiddenTokens(source_root / "src");
}

BOOST_AUTO_TEST_CASE(
    simulator_cleanup_gates_network_removed_on_verified_deletion) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);
  const std::string verified_step =
      "RunNodeCleanupStep(best_effort, \"verified node network removal\", [&] "
      "{";
  const std::size_t step = source.find(verified_step);
  BOOST_REQUIRE(step != std::string::npos);
  const std::size_t deletion =
      source.find("DeleteNodeVethNetwork(*node.network);", step);
  BOOST_REQUIRE(deletion != std::string::npos);
  const std::size_t event =
      source.find("SimulationEventKind::kNetworkRemoved", deletion);
  BOOST_REQUIRE(event != std::string::npos);
  const std::size_t verified_step_end = source.find("      });", event);
  BOOST_REQUIRE(verified_step_end != std::string::npos);
  const std::size_t cleaned_gate =
      source.find("if (network_cleanup_verified)", verified_step_end);
  BOOST_REQUIRE(cleaned_gate != std::string::npos);
  const std::size_t cleaned =
      source.find("NodeRuntimeLifecycle::kCleaned", cleaned_gate);
  BOOST_REQUIRE(cleaned != std::string::npos);
  const std::size_t failed =
      source.find("NodeRuntimeLifecycle::kFailed", cleaned);
  BOOST_REQUIRE(failed != std::string::npos);
  BOOST_TEST(deletion < event);
  BOOST_TEST(event < verified_step_end);
  BOOST_TEST(verified_step_end < cleaned_gate);
  BOOST_TEST(cleaned_gate < cleaned);
  BOOST_TEST(cleaned < failed);
}

BOOST_AUTO_TEST_CASE(
    simulator_transaction_load_uses_late_confirmation_accounting_path) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);
  const std::size_t confirmation =
      source.find("std::make_shared<TransactionLoadConfirmation>");
  BOOST_REQUIRE(confirmation != std::string::npos);
  const std::size_t ownership =
      source.find(".load_confirmation = confirmation", confirmation);
  BOOST_REQUIRE(ownership != std::string::npos);
  const std::size_t propagated =
      source.find("confirmation->RecordPropagated", ownership);
  BOOST_REQUIRE(propagated != std::string::npos);
  const std::size_t final_observation =
      source.rfind("transaction_tracker.ObserveAll");
  BOOST_REQUIRE(final_observation != std::string::npos);
  const std::size_t final_completion =
      source.find("WriteTransactionLoadCompletions", final_observation);
  BOOST_REQUIRE(final_completion != std::string::npos);
  BOOST_TEST(confirmation < ownership);
  BOOST_TEST(ownership < propagated);
  BOOST_TEST(propagated < final_observation);
  BOOST_TEST(final_observation < final_completion);
}

BOOST_AUTO_TEST_CASE(
    simulator_transaction_load_reconciles_failed_balance_reservations) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);
  const std::size_t ledger =
      source.find("TransactionLoadBalanceReservations balance_reservations");
  BOOST_REQUIRE(ledger != std::string::npos);
  const std::size_t failed =
      source.find("if (submission_started && !submitted &&", ledger);
  BOOST_REQUIRE(failed != std::string::npos);
  const std::size_t actual = source.find(".ReadWalletSnapshot(", failed);
  BOOST_REQUIRE(actual != std::string::npos);
  const std::size_t settlement =
      source.find("balance_reservations.Settle", actual);
  BOOST_REQUIRE(settlement != std::string::npos);
  const std::size_t plan =
      source.find("balance_reservations.PlanAndReserve", settlement);
  BOOST_REQUIRE(plan != std::string::npos);
  const std::size_t wait =
      source.find("balance_reservations.WaitForResolution", plan);
  BOOST_REQUIRE(wait != std::string::npos);
  BOOST_TEST(ledger < failed);
  BOOST_TEST(failed < actual);
  BOOST_TEST(actual < settlement);
  BOOST_TEST(settlement < plan);
  BOOST_TEST(plan < wait);
}

BOOST_AUTO_TEST_CASE(
    simulator_transaction_load_preserves_deadline_and_failure_classes) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);
  const std::size_t submit =
      source.find("transaction = driver.SubmitWalletTransaction(");
  BOOST_REQUIRE(submit != std::string::npos);
  const std::size_t configured_deadline =
      source.find("std::chrono::seconds(workload.timeout_sec),", submit);
  BOOST_REQUIRE(configured_deadline != std::string::npos);
  const std::size_t load_stop_argument =
      source.find("load_stop_token);", configured_deadline);
  BOOST_REQUIRE(load_stop_argument != std::string::npos);
  const std::size_t rejected =
      source.find("catch (const ChainTransactionRejected& error)", submit);
  const std::size_t timeout =
      source.find("catch (const ChainTransactionTimedOut& error)", rejected);
  const std::size_t warmup =
      source.find("catch (const ChainTransactionRpcWarmup& error)", timeout);
  const std::size_t unavailable = source.find(
      "catch (const ChainTransactionRpcMethodUnavailable& error)", warmup);
  const std::size_t transport = source.find(
      "catch (const ChainTransactionTransportFailure& error)", unavailable);
  const std::size_t internal = source.find(
      "catch (const ChainTransactionInternalRpcFailure& error)", transport);
  const std::size_t cancellation =
      source.find("catch (const SimulationCancelled& error)", internal);
  BOOST_REQUIRE(rejected != std::string::npos);
  BOOST_REQUIRE(timeout != std::string::npos);
  BOOST_REQUIRE(warmup != std::string::npos);
  BOOST_REQUIRE(unavailable != std::string::npos);
  BOOST_REQUIRE(transport != std::string::npos);
  BOOST_REQUIRE(internal != std::string::npos);
  BOOST_REQUIRE(cancellation != std::string::npos);
  const auto require_mapping = [&](std::size_t begin, std::size_t end,
                                   std::string_view outcome,
                                   std::string_view error_class_name) {
    const std::size_t mapped_outcome = source.find(outcome, begin);
    const std::size_t mapped_class = source.find(error_class_name, begin);
    BOOST_REQUIRE(mapped_outcome != std::string::npos);
    BOOST_REQUIRE(mapped_class != std::string::npos);
    BOOST_TEST(mapped_outcome < end);
    BOOST_TEST(mapped_class < end);
  };
  require_mapping(rejected, timeout,
                  "outcome = TransactionLoadOutcome::kRejected;",
                  "error_class = \"policy_rejection\";");
  require_mapping(timeout, warmup,
                  "outcome = TransactionLoadOutcome::kTimedOut;",
                  "error_class = \"timeout\";");
  require_mapping(warmup, unavailable,
                  "outcome = TransactionLoadOutcome::kFailed;",
                  "error_class = \"warmup\";");
  require_mapping(unavailable, transport,
                  "outcome = TransactionLoadOutcome::kFailed;",
                  "error_class = \"method_unavailable\";");
  require_mapping(transport, internal,
                  "outcome = TransactionLoadOutcome::kFailed;",
                  "error_class = \"transport\";");
  require_mapping(internal, cancellation,
                  "outcome = TransactionLoadOutcome::kFailed;",
                  "error_class = \"internal_rpc\";");
  const std::size_t ordinary_failure =
      source.find("catch (const std::exception& error)", cancellation);
  BOOST_REQUIRE(ordinary_failure != std::string::npos);
  require_mapping(cancellation, ordinary_failure,
                  "outcome = TransactionLoadOutcome::kCancelled;",
                  "error_class = \"cancellation\";");
  const std::size_t error_class =
      source.find("detail[\"error_class\"] = error_class");
  BOOST_REQUIRE(error_class != std::string::npos);

  const std::size_t observe_sets =
      source.find("ObserveTrackedSetsUntilVisible(");
  BOOST_REQUIRE(observe_sets != std::string::npos);
  const std::size_t shared_deadline = source.find(
      "const auto deadline = std::chrono::steady_clock::now() + "
      "timeout",
      observe_sets);
  BOOST_REQUIRE(shared_deadline != std::string::npos);
  const std::size_t bounded_observation =
      source.find("driver.ObserveTransactionUntil(", shared_deadline);
  BOOST_REQUIRE(bounded_observation != std::string::npos);
  const std::size_t deadline_argument =
      source.find("node.config, transaction.txid, deadline, stop_token",
                  bounded_observation);
  BOOST_REQUIRE(deadline_argument != std::string::npos);

  BOOST_TEST(submit < configured_deadline);
  BOOST_TEST(configured_deadline < load_stop_argument);
  BOOST_TEST(load_stop_argument < rejected);
  BOOST_TEST(rejected < timeout);
  BOOST_TEST(timeout < warmup);
  BOOST_TEST(warmup < unavailable);
  BOOST_TEST(unavailable < transport);
  BOOST_TEST(transport < internal);
  BOOST_TEST(internal < cancellation);
  BOOST_TEST(shared_deadline < bounded_observation);
  BOOST_TEST(bounded_observation < deadline_argument);
}

BOOST_AUTO_TEST_CASE(
    simulator_reserves_transaction_observation_before_every_mutating_send) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);

  const std::size_t raw_function =
      source.find("void ApplySendRawTransactionWorkload(");
  const std::size_t raw_reserve =
      source.find("transaction_tracker.Reserve(nodes);", raw_function);
  const std::size_t raw_send =
      source.find("driver.SendRawTransaction(", raw_function);
  const std::size_t load_function =
      source.find("void ApplyWalletTransactionsWorkload(", raw_function);
  BOOST_REQUIRE(raw_function != std::string::npos);
  BOOST_REQUIRE(raw_reserve != std::string::npos);
  BOOST_REQUIRE(raw_send != std::string::npos);
  BOOST_REQUIRE(load_function != std::string::npos);
  BOOST_TEST(raw_reserve < raw_send);
  BOOST_TEST(raw_send < load_function);

  const std::size_t load_reserve =
      source.find("transaction_tracker.TryReserve(nodes);", load_function);
  const std::size_t load_send =
      source.find("driver.SubmitWalletTransaction(", load_function);
  const std::size_t load_commit =
      source.find("transaction_tracker.TrackSet(", load_send);
  BOOST_REQUIRE(load_reserve != std::string::npos);
  BOOST_REQUIRE(load_send != std::string::npos);
  BOOST_REQUIRE(load_commit != std::string::npos);
  BOOST_TEST(load_reserve < load_send);
  BOOST_TEST(load_send < load_commit);

  const std::size_t ordinary_reserve =
      source.find("transaction_tracker.Reserve(nodes);", load_commit);
  const std::size_t ordinary_send =
      source.find("driver.SendWalletTransaction(", ordinary_reserve);
  BOOST_REQUIRE(ordinary_reserve != std::string::npos);
  BOOST_REQUIRE(ordinary_send != std::string::npos);
  BOOST_TEST(ordinary_reserve < ordinary_send);

  const std::size_t operator_command = source.find(
      "SimulationCommandKind::kSendWalletTransaction", ordinary_send);
  const std::size_t operator_reserve =
      source.find("transaction_tracker.Reserve(nodes);", operator_command);
  const std::size_t operator_send =
      source.find("driver.SendWalletTransaction(", operator_command);
  BOOST_REQUIRE(operator_command != std::string::npos);
  BOOST_REQUIRE(operator_reserve != std::string::npos);
  BOOST_REQUIRE(operator_send != std::string::npos);
  BOOST_TEST(operator_reserve < operator_send);
}

BOOST_AUTO_TEST_CASE(
    simulator_transaction_load_cancels_pending_queue_without_rpc_drain) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);
  const std::size_t stopped_pop = source.find("queue.Pop(load_stop_token)");
  const std::size_t cancelled_outcome =
      source.find("outcome = TransactionLoadOutcome::kCancelled;", stopped_pop);
  const std::size_t local_stop =
      source.find("load_stop_source.request_stop()", cancelled_outcome);
  const std::size_t cancel_queue =
      source.find("record_dropped_tasks(queue.Cancel())", cancelled_outcome);
  const std::size_t post_join_cancel =
      source.find("record_dropped_tasks(queue.Cancel())", cancel_queue + 1U);
  const std::size_t record_dropped_tasks =
      source.find("const auto record_dropped_tasks");
  const std::size_t dropped_outcome =
      source.find("TransactionLoadOutcome::kDropped", record_dropped_tasks);
  const std::size_t dropped_accounting =
      source.rfind("accounting->RecordOutcome", dropped_outcome);
  BOOST_REQUIRE(stopped_pop != std::string::npos);
  BOOST_REQUIRE(cancelled_outcome != std::string::npos);
  BOOST_REQUIRE(local_stop != std::string::npos);
  BOOST_REQUIRE(cancel_queue != std::string::npos);
  BOOST_REQUIRE(post_join_cancel != std::string::npos);
  BOOST_REQUIRE(record_dropped_tasks != std::string::npos);
  BOOST_REQUIRE(dropped_outcome != std::string::npos);
  BOOST_REQUIRE(dropped_accounting != std::string::npos);
  BOOST_TEST(record_dropped_tasks < dropped_accounting);
  BOOST_TEST(dropped_accounting < dropped_outcome);
  BOOST_TEST(stopped_pop < cancelled_outcome);
  BOOST_TEST(cancelled_outcome < local_stop);
  BOOST_TEST(local_stop < cancel_queue);
}

BOOST_AUTO_TEST_CASE(
    simulator_equal_fanout_paces_every_transaction_before_rpc_mutation) {
  const std::filesystem::path simulator =
      std::filesystem::path(BBP_SOURCE_DIR) / "src" / "simulator_app.cpp";
  const std::string source = bbp::ReadText(simulator);
  const std::size_t task_schedule = source.find(
      "ApplyTransactionLoadRateSchedule(\n"
      "                          &task");
  const std::size_t worker_wait =
      source.find("WaitForTransactionLoadSchedule(task, load_stop_token)");
  const std::size_t rpc_mutation =
      source.find("driver.SubmitWalletTransaction(", worker_wait);
  BOOST_REQUIRE(task_schedule != std::string::npos);
  BOOST_REQUIRE(worker_wait != std::string::npos);
  BOOST_REQUIRE(rpc_mutation != std::string::npos);
  BOOST_TEST(worker_wait < rpc_mutation);
}
