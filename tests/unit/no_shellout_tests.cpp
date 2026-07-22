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
  const std::size_t failed = source.find("if (!submitted)", ledger);
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
