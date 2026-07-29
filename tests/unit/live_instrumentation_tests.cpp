#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_dispatcher.h"
#include "bbp/perf_counter.h"
#include "bbp/simulator_app.h"

namespace {

boost::json::object Target(std::string_view node_id) {
  return boost::json::object{
      {"kind", "node"},
      {"id", node_id},
      {"node_ids", boost::json::array{node_id}},
  };
}

boost::json::array Counters(bbp::PerfCounterKind kind) {
  return boost::json::array{bbp::PerfCounterKindName(kind)};
}

boost::json::object StartArguments(
    boost::json::array targets, bbp::PerfCounterKind counter,
    std::optional<std::string_view> instrumentation_id = std::nullopt,
    std::optional<std::string_view> sample_interval = "24h") {
  boost::json::object arguments{
      {"run_id", "instrumentation-test"},
      {"targets", std::move(targets)},
      {"counters", Counters(counter)},
      {"window", "24h"},
  };
  if (instrumentation_id) {
    arguments["instrumentation_id"] = *instrumentation_id;
  }
  if (sample_interval) {
    arguments["sample_interval"] = *sample_interval;
  }
  return arguments;
}

boost::json::object Invoke(
    const std::shared_ptr<bbp::McpLiveInstrumentationService>& service,
    bbp::McpOperationKind kind, boost::json::object arguments) {
  return service->operation(kind, arguments, {});
}

void CheckLogicalConfiguration(
    const bbp::LiveInstrumentationNodeStateForTest& observed,
    const bbp::LiveInstrumentationNodeStateForTest& expected) {
  BOOST_CHECK(observed.counters == expected.counters);
  BOOST_CHECK(observed.target_kind == expected.target_kind);
  BOOST_TEST(observed.target_id == expected.target_id);
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    live_instrumentation_reconfigures_at_a_boundary_and_restores_baselines) {
  bbp::LiveInstrumentationHarnessForTest harness({"node-1", "node-2"},
                                                 std::chrono::hours(7));
  const auto service = harness.service();
  const bbp::LiveInstrumentationNodeStateForTest node_1_baseline =
      harness.NodeState("node-1");

  const boost::json::object started =
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{Target("node-1")},
                            bbp::PerfCounterKind::kInstructions, std::nullopt,
                            std::nullopt));
  const std::string instrumentation_id(
      started.at("instrumentation_id").as_string());
  BOOST_TEST(!instrumentation_id.empty());
  BOOST_TEST(started.at("state").as_string() == "running");
  BOOST_TEST(started.at("sample_count").as_uint64() == 1U);

  const bbp::LiveInstrumentationNodeStateForTest node_1_active =
      harness.NodeState("node-1");
  const std::uint64_t attempts_before_overlap = harness.attachment_attempts();
  BOOST_CHECK_THROW(
      harness.ApplyPerfMutation("node-1", bbp::PerfCounterKind::kCycles),
      std::runtime_error);
  BOOST_TEST(harness.attachment_attempts() == attempts_before_overlap);
  BOOST_CHECK(harness.NodeState("node-1") == node_1_active);

  harness.ApplyPerfMutation("node-2", bbp::PerfCounterKind::kCycles);
  const bbp::LiveInstrumentationNodeStateForTest node_2_baseline =
      harness.NodeState("node-2");
  BOOST_CHECK(node_2_baseline.counters ==
              std::vector<bbp::PerfCounterKind>{bbp::PerfCounterKind::kCycles});

  const boost::json::object active_before =
      service->read(bbp::McpInformationFamily::kInstrumentation, {})
          .as_array()
          .front()
          .as_object();
  const std::uint64_t committed_start =
      active_before.at("started_at_ms").as_uint64();
  BOOST_TEST(active_before.at("sample_interval_ms").as_int64() ==
             std::chrono::hours(7).count() * 60 * 60 * 1000);
  BOOST_TEST(active_before.at("window_ms").as_int64() ==
             std::chrono::hours(24).count() * 60 * 60 * 1000);

  const std::uint64_t attempts_before_reconfigure =
      harness.attachment_attempts();
  harness.FailAttachmentOnAttempt(
      static_cast<std::size_t>(attempts_before_reconfigure + 2U));
  BOOST_CHECK_THROW(
      Invoke(service, bbp::McpOperationKind::kReconfigureInstrumentation,
             boost::json::object{
                 {"run_id", "instrumentation-test"},
                 {"instrumentation_id", instrumentation_id},
                 {"targets",
                  boost::json::array{Target("node-1"), Target("node-2")}},
                 {"counters", Counters(bbp::PerfCounterKind::kCacheReferences)},
             }),
      std::exception);
  BOOST_TEST(harness.attachment_attempts() == attempts_before_reconfigure + 2U);
  BOOST_CHECK(harness.NodeState("node-1") == node_1_active);
  BOOST_CHECK(harness.NodeState("node-2") == node_2_baseline);
  const boost::json::object unchanged =
      service->read(bbp::McpInformationFamily::kInstrumentation, {})
          .as_array()
          .front()
          .as_object();
  BOOST_CHECK(unchanged == active_before);
  harness.FailAttachmentOnAttempt(std::nullopt);

  const boost::json::object reconfigured =
      Invoke(service, bbp::McpOperationKind::kReconfigureInstrumentation,
             boost::json::object{
                 {"run_id", "instrumentation-test"},
                 {"instrumentation_id", instrumentation_id},
                 {"targets", boost::json::array{Target("node-2")}},
                 {"counters", Counters(bbp::PerfCounterKind::kCacheReferences)},
                 {"sample_interval", "12h"},
             });
  BOOST_TEST(reconfigured.at("instrumentation_id").as_string() ==
             instrumentation_id);
  BOOST_TEST(reconfigured.at("sample_count").as_uint64() == 1U);
  CheckLogicalConfiguration(harness.NodeState("node-1"), node_1_baseline);
  const bbp::LiveInstrumentationNodeStateForTest node_2_active =
      harness.NodeState("node-2");
  BOOST_CHECK(node_2_active.counters ==
              std::vector<bbp::PerfCounterKind>{
                  bbp::PerfCounterKind::kCacheReferences});

  const boost::json::object active_after =
      service->read(bbp::McpInformationFamily::kInstrumentation, {})
          .as_array()
          .front()
          .as_object();
  BOOST_TEST(active_after.at("started_at_ms").as_uint64() == committed_start);
  BOOST_TEST(active_after.at("window_ms").as_int64() ==
             active_before.at("window_ms").as_int64());
  BOOST_TEST(active_after.at("sample_interval_ms").as_int64() ==
             std::chrono::hours(12).count() * 60 * 60 * 1000);

  harness.SampleNow();
  const boost::json::object measurements =
      service->read(bbp::McpInformationFamily::kMeasurements, {}).as_object();
  BOOST_TEST(measurements.at("sample_count").as_uint64() == 2U);
  const boost::json::array& measurement_records =
      measurements.at("measurement_records").as_array();
  BOOST_REQUIRE_EQUAL(measurement_records.size(), 2U);
  BOOST_TEST(measurement_records[1]
                 .as_object()
                 .at("record")
                 .as_object()
                 .at("node_id")
                 .as_string() == "node-2");

  harness.SetNodeRunning("node-2", false);
  const boost::json::object stopped =
      Invoke(service, bbp::McpOperationKind::kStopInstrumentation,
             boost::json::object{
                 {"run_id", "instrumentation-test"},
                 {"instrumentation_id", instrumentation_id},
                 {"timeout_sec", 1},
             });
  BOOST_TEST(stopped.at("state").as_string() == "succeeded");
  BOOST_TEST(stopped.at("sample_count").as_uint64() == 2U);
  const bbp::LiveInstrumentationNodeStateForTest node_2_restored =
      harness.NodeState("node-2");
  CheckLogicalConfiguration(node_2_restored, node_2_baseline);
  BOOST_TEST(node_2_restored.target_pid == -1);
  BOOST_TEST(node_2_restored.attached_pid == -1);

  const boost::json::array history =
      service->read(bbp::McpInformationFamily::kMeasurementHistory, {})
          .as_array();
  BOOST_REQUIRE_EQUAL(history.size(), 1U);
  const boost::json::object& retained = history.front().as_object();
  BOOST_TEST(retained.at("instrumentation_id").as_string() ==
             instrumentation_id);
  BOOST_TEST(retained.at("started_at_ms").as_uint64() == committed_start);
  BOOST_TEST(retained.at("sample_count").as_uint64() == 2U);
  BOOST_REQUIRE_EQUAL(retained.at("measurement_records").as_array().size(), 2U);
}

BOOST_AUTO_TEST_CASE(
    live_instrumentation_start_failures_roll_back_before_publication) {
  bbp::LiveInstrumentationHarnessForTest harness({"node-1", "node-2"});
  const auto service = harness.service();
  const bbp::LiveInstrumentationNodeStateForTest node_1_before =
      harness.NodeState("node-1");
  const bbp::LiveInstrumentationNodeStateForTest node_2_before =
      harness.NodeState("node-2");

  BOOST_CHECK_THROW(
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{boost::json::object{
                                {"kind", "wallet"},
                                {"id", "wallet-1"},
                                {"node_ids", boost::json::array{"node-1"}},
                            }},
                            bbp::PerfCounterKind::kInstructions)),
      std::invalid_argument);
  BOOST_TEST(harness.attachment_attempts() == 0U);

  const std::uint64_t attempts_before_start = harness.attachment_attempts();
  harness.FailAttachmentOnAttempt(
      static_cast<std::size_t>(attempts_before_start + 2U));
  BOOST_CHECK_THROW(
      Invoke(
          service, bbp::McpOperationKind::kStartInstrumentation,
          StartArguments(boost::json::array{Target("node-1"), Target("node-2")},
                         bbp::PerfCounterKind::kInstructions)),
      std::exception);
  BOOST_TEST(harness.attachment_attempts() == attempts_before_start + 2U);
  BOOST_CHECK(harness.NodeState("node-1") == node_1_before);
  BOOST_CHECK(harness.NodeState("node-2") == node_2_before);
  BOOST_TEST(service->read(bbp::McpInformationFamily::kInstrumentation, {})
                 .as_array()
                 .empty());
  BOOST_TEST(service->read(bbp::McpInformationFamily::kMeasurements, {})
                 .as_object()
                 .at("instrumentation_id")
                 .is_null());

  harness.FailAttachmentOnAttempt(std::nullopt);
  harness.FailNextSample();
  BOOST_CHECK_THROW(
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{Target("node-1")},
                            bbp::PerfCounterKind::kInstructions)),
      std::exception);
  BOOST_CHECK(harness.NodeState("node-1") == node_1_before);
  BOOST_TEST(service->read(bbp::McpInformationFamily::kInstrumentation, {})
                 .as_array()
                 .empty());

  const boost::json::object started =
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{Target("node-1")},
                            bbp::PerfCounterKind::kInstructions));
  const std::uint64_t attempts_before_rejection = harness.attachment_attempts();
  BOOST_CHECK_THROW(
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{Target("node-2")},
                            bbp::PerfCounterKind::kCycles)),
      bbp::McpOperationFailure);
  BOOST_TEST(harness.attachment_attempts() == attempts_before_rejection);
  BOOST_TEST(started.at("state").as_string() == "running");

  const bbp::LiveInstrumentationNodeStateForTest active =
      harness.NodeState("node-1");
  harness.FailAttachmentOnAttempt(
      static_cast<std::size_t>(harness.attachment_attempts() + 1U));
  const boost::json::object failed_stop = Invoke(
      service, bbp::McpOperationKind::kStopInstrumentation,
      boost::json::object{
          {"run_id", "instrumentation-test"},
          {"instrumentation_id", started.at("instrumentation_id").as_string()},
          {"timeout_sec", 1},
      });
  BOOST_TEST(failed_stop.at("state").as_string() == "failed");
  BOOST_CHECK(harness.NodeState("node-1") == active);
  const boost::json::array active_failure =
      service->read(bbp::McpInformationFamily::kInstrumentation, {}).as_array();
  BOOST_REQUIRE_EQUAL(active_failure.size(), 1U);
  BOOST_TEST(active_failure.front().as_object().at("state").as_string() ==
             "failed");
  BOOST_TEST(
      !active_failure.front().as_object().at("failure").as_string().empty());

  harness.FailAttachmentOnAttempt(std::nullopt);
  const boost::json::object retried_stop = Invoke(
      service, bbp::McpOperationKind::kStopInstrumentation,
      boost::json::object{
          {"run_id", "instrumentation-test"},
          {"instrumentation_id", started.at("instrumentation_id").as_string()},
          {"timeout_sec", 1},
      });
  BOOST_TEST(retried_stop.at("state").as_string() == "failed");
  CheckLogicalConfiguration(harness.NodeState("node-1"), node_1_before);
  BOOST_TEST(service->read(bbp::McpInformationFamily::kInstrumentation, {})
                 .as_array()
                 .empty());
  const boost::json::array failure_history =
      service->read(bbp::McpInformationFamily::kMeasurementHistory, {})
          .as_array();
  BOOST_REQUIRE_EQUAL(failure_history.size(), 1U);
  BOOST_TEST(failure_history.front().as_object().at("state").as_string() ==
             "failed");
  BOOST_TEST(
      !failure_history.front().as_object().at("failure").as_string().empty());
}

BOOST_AUTO_TEST_CASE(
    live_instrumentation_bounds_history_and_shares_expiry_shutdown_restore) {
  bbp::LiveInstrumentationHarnessForTest harness({"node-1"});
  const auto service = harness.service();
  const bbp::LiveInstrumentationNodeStateForTest baseline =
      harness.NodeState("node-1");

  static_cast<void>(
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{Target("node-1")},
                            bbp::PerfCounterKind::kInstructions)));
  for (std::size_t sample = 0U; sample < 1024U; ++sample) {
    harness.SampleNow();
  }
  const boost::json::object measurements =
      service->read(bbp::McpInformationFamily::kMeasurements, {}).as_object();
  BOOST_TEST(measurements.at("sample_count").as_uint64() == 1025U);
  BOOST_TEST(measurements.at("retained_measurement_count").as_uint64() ==
             1024U);
  BOOST_TEST(measurements.at("measurements_truncated").as_bool());
  BOOST_TEST(measurements.at("dropped_measurement_count").as_uint64() == 1U);
  const boost::json::array& records =
      measurements.at("measurement_records").as_array();
  BOOST_REQUIRE_EQUAL(records.size(), 1024U);
  BOOST_TEST(records.front().as_object().at("sample").as_uint64() == 2U);
  BOOST_TEST(records.back().as_object().at("sample").as_uint64() == 1025U);

  harness.ExpireNow();
  CheckLogicalConfiguration(harness.NodeState("node-1"), baseline);
  const boost::json::array expired_history =
      service->read(bbp::McpInformationFamily::kMeasurementHistory, {})
          .as_array();
  BOOST_REQUIRE_EQUAL(expired_history.size(), 1U);
  BOOST_TEST(expired_history.front().as_object().at("state").as_string() ==
             "succeeded");

  const boost::json::object shutdown_started =
      Invoke(service, bbp::McpOperationKind::kStartInstrumentation,
             StartArguments(boost::json::array{Target("node-1")},
                            bbp::PerfCounterKind::kCacheMisses));
  const std::string shutdown_id(
      shutdown_started.at("instrumentation_id").as_string());
  harness.Shutdown();
  CheckLogicalConfiguration(harness.NodeState("node-1"), baseline);
  const boost::json::array shutdown_history =
      service->read(bbp::McpInformationFamily::kMeasurementHistory, {})
          .as_array();
  BOOST_REQUIRE_EQUAL(shutdown_history.size(), 2U);
  const auto shutdown_record = std::find_if(
      shutdown_history.begin(), shutdown_history.end(),
      [&shutdown_id](const boost::json::value& value) {
        return value.as_object().at("instrumentation_id").as_string() ==
               shutdown_id;
      });
  BOOST_REQUIRE(shutdown_record != shutdown_history.end());
  BOOST_TEST(shutdown_record->as_object().at("state").as_string() ==
             "cancelled");
}
