#include <unistd.h>

#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <limits>
#include <stdexcept>

#include "../../src/simulator_scheduled_command_decoding.h"
#include "../../src/simulator_scheduled_command_event_details.h"
#include "../../src/simulator_workload_event_details.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/run_report.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "bbp/tui_command_parser.h"
#include "bbp/util.h"

namespace {

bbp::SimulationRegistry ManagedWallets() {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 2U;
  topology.wallet_node_count = 2U;
  topology.wallet_nodes = {0U, 1U};
  auto registry = bbp::SimulationRegistry::FromTopology(topology, {});
  for (std::uint32_t i = 0; i < 2U; ++i) {
    auto& wallet = registry.MutableWalletByIndex(i);
    wallet.node_id = "firo-" + std::to_string(i + 1U);
    wallet.address = "managed-" + std::to_string(i);
    wallet.funding_address = "funding-" + std::to_string(i);
  }
  return registry;
}

bbp::SimulationCommand TargetCommand(bbp::SimulationCommandKind kind,
                                     const std::string& address) {
  return bbp::simulator_app_internal::ParseScheduledSimulationCommand(
      {{"target_address", address}}, kind, bbp::Options{});
}

}  // namespace

BOOST_AUTO_TEST_SUITE(target_addresses)

BOOST_AUTO_TEST_CASE(registry_keeps_targets_separate_and_deduplicated) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(ManagedWallets());
  const auto before = registry.Snapshot();
  registry.SetTargetAddress("external", true);
  registry.SetTargetAddress("external", true);
  BOOST_TEST(registry.TargetAddresses() == std::vector<std::string>{"external"},
             boost::test_tools::per_element());
  BOOST_TEST(registry.Snapshot().generation() == before.generation());
  BOOST_TEST(registry.Snapshot().wallets().size() == 2U);
  BOOST_CHECK_THROW(registry.SetTargetAddress("managed-0", true),
                    std::invalid_argument);
  BOOST_CHECK_THROW(registry.SetTargetAddress("funding-1", true),
                    std::invalid_argument);
  BOOST_CHECK_THROW(
      registry.SetTargetAddress(
          "cancelled", true,
          [] { throw std::runtime_error("cancelled before commit"); }),
      std::runtime_error);
  BOOST_CHECK_THROW(
      registry.SetTargetAddress(
          "external", false,
          [] { throw std::runtime_error("cancelled before commit"); }),
      std::runtime_error);
  BOOST_TEST(registry.TargetAddresses().size() == 1U);
  registry.SetTargetAddress("missing", false);
  registry.SetTargetAddress("external", false);
  registry.SetTargetAddress("external", false);
  BOOST_TEST(registry.TargetAddresses().empty());
}

BOOST_AUTO_TEST_CASE(selection_reserves_one_slot_per_target) {
  bbp::RuntimeWalletRegistry registry;
  BOOST_TEST(!registry.SelectTargetAddress(0U, 19U));
  registry.SetTargetAddress("one", true);
  registry.SetTargetAddress("two", true);
  std::size_t one = 0U;
  std::size_t two = 0U;
  std::size_t managed = 0U;
  for (std::uint64_t i = 1U; i <= 210U; ++i) {
    const auto target = registry.SelectTargetAddress(i, 19U);
    if (!target)
      ++managed;
    else if (*target == "one")
      ++one;
    else if (*target == "two")
      ++two;
    else
      BOOST_FAIL("unknown target selected");
  }
  BOOST_TEST(one == 10U);
  BOOST_TEST(two == 10U);
  BOOST_TEST(managed == 190U);
  BOOST_CHECK_THROW(static_cast<void>(registry.SelectTargetAddress(
                        0U, std::numeric_limits<std::size_t>::max())),
                    std::overflow_error);
  registry.SetTargetAddress("one", false);
  for (std::uint64_t i = 0U; i < 40U; ++i) {
    const auto target = registry.SelectTargetAddress(i, 19U);
    if (target) BOOST_TEST(*target == "two");
  }
  registry.SetTargetAddress("two", false);
  BOOST_TEST(!registry.SelectTargetAddress(0U, 19U));
}

BOOST_AUTO_TEST_CASE(commands_validate_payload_and_round_trip) {
  for (const auto kind : {bbp::SimulationCommandKind::kAddTargetAddress,
                          bbp::SimulationCommandKind::kRemoveTargetAddress}) {
    const bool adding = kind == bbp::SimulationCommandKind::kAddTargetAddress;
    const auto parsed = bbp::TuiCommandParser::Parse(
        std::string(adding ? "add-target " : "remove-target ") + "external",
        0U);
    BOOST_CHECK(parsed.kind == kind);
    BOOST_REQUIRE(parsed.target_address);
    BOOST_TEST(*parsed.target_address == "external");
    BOOST_CHECK(bbp::SimulationCommandKindFromName(
                    bbp::SimulationCommandKindName(kind)) == kind);
    auto command = TargetCommand(kind, "external");
    BOOST_TEST(command.node_id == "sim");
    BOOST_TEST(command.confirmed);
    bbp::SimulationCommandQueue queue;
    BOOST_TEST(queue.PushRuntimeCommand(command) == 1U);
    const auto queued = queue.TryPop();
    BOOST_REQUIRE(queued);
    BOOST_REQUIRE(queued->operation_control);
    BOOST_REQUIRE(queued->target_address);
    BOOST_TEST(*queued->target_address == "external");
    command.node_id = "firo-1";
    BOOST_CHECK_THROW(queue.PushRuntimeCommand(command), std::invalid_argument);
    command.node_id = "sim";
    command.target_address.reset();
    BOOST_CHECK_THROW(queue.PushRuntimeCommand(command), std::runtime_error);
  }
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kAddTargetAddress));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kRemoveTargetAddress));
  for (const std::string address : {"", "a b", "a\nb", "a\x1b"}) {
    BOOST_CHECK_THROW(bbp::ValidateTargetAddressText(address),
                      std::invalid_argument);
  }
  BOOST_CHECK_THROW(bbp::ValidateTargetAddressText(std::string(513U, 'a')),
                    std::invalid_argument);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("add-target", 0U),
                    std::exception);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("remove-target a b", 0U),
                    std::exception);
}

BOOST_AUTO_TEST_CASE(
    report_does_not_credit_managed_receiver_for_external_payment) {
  namespace internal = bbp::simulator_app_internal;
  const auto dir = std::filesystem::temp_directory_path() /
                   ("bbp-target-report-" + std::to_string(getpid()));
  std::filesystem::create_directories(dir);
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"targets","chain":"firo","nodes":2})");
  bbp::WriteText(dir / "events.jsonl", "");
  const auto append = [&](bbp::SimulationEventKind kind,
                          const std::string& detail) {
    bbp::AppendLine(dir / "events.jsonl",
                    boost::json::serialize(boost::json::object{
                        {"run_id", "targets"},
                        {"node_id", "sim"},
                        {"event", bbp::SimulationEventKindName(kind)},
                        {"detail", detail}}));
  };
  auto command =
      TargetCommand(bbp::SimulationCommandKind::kAddTargetAddress, "external");
  append(bbp::SimulationEventKind::kOperatorCommandCompleted,
         internal::SimulationCommandDetail(command));
  const auto registry = ManagedWallets();
  bbp::WalletTransactionsWorkload workload;
  bbp::ChainWalletTransactionResult transaction;
  transaction.txids = {"payment"};
  const auto payment = internal::WalletTransactionDetail(
      0U, 1U, workload, 1U, registry.wallets()[0], registry.wallets()[1], 1U,
      0U, 100U, 100U, 100U, {}, 0U, 100000U, 1000U,
      std::chrono::milliseconds(0), std::nullopt, std::nullopt, transaction,
      "external");
  const auto payment_json = boost::json::parse(payment).as_object();
  BOOST_TEST(payment_json.at("receiver_wallet_index").is_null());
  BOOST_TEST(payment_json.at("receiver_node").is_null());
  BOOST_TEST(payment_json.at("receiver_address").as_string() == "external");
  append(bbp::SimulationEventKind::kWalletTransactionSubmitted, payment);
  bbp::IncrementalRunReport report(dir);
  auto snapshot = report.Refresh();
  BOOST_TEST(snapshot.at("wallets_summary").as_array().size() == 1U);
  const auto& sender =
      snapshot.at("wallets_summary").as_array().front().as_object();
  BOOST_TEST(sender.at("transactions_sent").as_uint64() == 1U);
  BOOST_TEST(sender.at("transactions_received").as_uint64() == 0U);
  const auto& target =
      snapshot.at("target_addresses").as_array().front().as_object();
  BOOST_TEST(target.at("active").as_bool());
  BOOST_TEST(target.at("transactions_submitted").as_uint64() == 1U);
  BOOST_TEST(target.at("amount_submitted_satoshis").as_uint64() == 1000U);
  command.kind = bbp::SimulationCommandKind::kRemoveTargetAddress;
  append(bbp::SimulationEventKind::kOperatorCommandFailed,
         internal::SimulationCommandDetail(command, "cancelled"));
  BOOST_TEST(report.Refresh()
                 .at("target_addresses")
                 .as_array()
                 .front()
                 .as_object()
                 .at("active")
                 .as_bool());
  append(bbp::SimulationEventKind::kOperatorCommandCompleted,
         internal::SimulationCommandDetail(command));
  BOOST_TEST(!report.Refresh()
                  .at("target_addresses")
                  .as_array()
                  .front()
                  .as_object()
                  .at("active")
                  .as_bool());
  append(bbp::SimulationEventKind::kWalletTransactionSubmitted, payment);
  snapshot = report.Refresh();
  const auto& removed =
      snapshot.at("target_addresses").as_array().front().as_object();
  BOOST_TEST(!removed.at("active").as_bool());
  BOOST_TEST(removed.at("transactions_submitted").as_uint64() == 2U);
  BOOST_TEST(report.Refresh() == snapshot);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_SUITE_END()
