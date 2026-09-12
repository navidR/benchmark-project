#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/json/array.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <csignal>
#include <filesystem>
#include <string>
#include <system_error>

#include "../../src/runtime_capacity_persistence.h"
#include "../../src/simulator_retained_run_registry.h"
#include "../../src/simulator_source_scenario_persistence.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/run_report.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace {

class RetainedCapacityRoot {
 public:
  explicit RetainedCapacityRoot(std::string run_id) {
    options.run_id = std::move(run_id);
    options.output_dir = std::filesystem::temp_directory_path() /
                         ("bbp-retained-capacity-" + options.run_id + "-" +
                          std::to_string(getpid()));
    std::filesystem::remove_all(options.output_dir);
    std::filesystem::create_directories(path());
    options.run_ownership = bbp::CreateRunOwnership(options.run_id, path());
    bbp::WriteRunOwnershipMarker(*options.run_ownership);
    options.nodes = 11U;
    options.node_capacity = 21U;
    options.network_address_pool = "10.77.0.0/20";
    options.network_address_plan = bbp::SimulationNetworkAddressPlan::FromCidr(
        options.network_address_pool, options.node_capacity);
  }

  ~RetainedCapacityRoot() {
    std::error_code ignored;
    std::filesystem::remove_all(options.output_dir, ignored);
  }

  std::filesystem::path path() const {
    return options.output_dir / options.run_id;
  }

  std::vector<bbp::McpRetainedRunSnapshot> Discover() const {
    return bbp::simulator_app_internal::DiscoverRetainedRuns(options.output_dir,
                                                             {}, {});
  }

  bbp::Options options;
};

}  // namespace

BOOST_AUTO_TEST_CASE(
    retained_registry_round_trips_capacity_and_allocation_past_16) {
  RetainedCapacityRoot root("grown-summary");
  bbp::simulator_app_internal::WriteRetainedRunRegistrySummary(root.options,
                                                               "finished", 21U);
  const auto retained = root.Discover();
  BOOST_REQUIRE_EQUAL(retained.size(), 1U);
  BOOST_TEST(retained.front().node_count == 21U);
  BOOST_TEST(retained.front().node_capacity == 21U);
  BOOST_CHECK(!retained.front().chain_node_maximum);
  BOOST_CHECK(retained.front().network_allocation ==
              root.options.network_address_plan->ToSerialized());
  const auto summary = boost::json::parse(
      bbp::ReadText(root.path() / "run-registry-summary.json"));
  BOOST_CHECK(!summary.as_object().contains("chain_node_maximum"));
}

BOOST_AUTO_TEST_CASE(retained_registry_preserves_legacy_summary_validation) {
  RetainedCapacityRoot root("legacy-summary");
  boost::json::object summary{{"format", "bbp-retained-run-registry"},
                              {"version", 1U},
                              {"run_id", root.options.run_id},
                              {"state", "finished"},
                              {"chain", "firo"},
                              {"node_count", 11U},
                              {"node_capacity", 16U},
                              {"chain_node_maximum", 16U}};
  bbp::WriteText(root.path() / "run-registry-summary.json",
                 boost::json::serialize(summary));
  const auto retained = root.Discover();
  BOOST_REQUIRE_EQUAL(retained.size(), 1U);
  BOOST_TEST(retained.front().node_count == 11U);
  BOOST_TEST(retained.front().node_capacity == 16U);
  BOOST_CHECK(retained.front().network_allocation.is_null());
  summary["node_count"] = 17U;
  bbp::WriteText(root.path() / "run-registry-summary.json",
                 boost::json::serialize(summary));
  BOOST_CHECK_THROW(root.Discover(), bbp::McpOperationFailure);
}

BOOST_AUTO_TEST_CASE(
    retained_registry_reconstructs_growth_from_events_and_manifest) {
  RetainedCapacityRoot root("grown-recovery");
  bbp::WriteText(root.path() / "resolved-scenario.json",
                 boost::json::serialize(
                     boost::json::object{{"run_id", root.options.run_id},
                                         {"chain", "firo"},
                                         {"nodes", 11U},
                                         {"node_capacity", 16U}}));
  boost::json::array ids;
  boost::json::array configs;
  bbp::RuntimeNodeResourceManifest manifest{
      .ownership = *root.options.run_ownership,
      .isolated_network = true,
      .node_capacity = 21U,
      .network_address_plan = root.options.network_address_plan,
      .nodes = {},
  };
  for (std::uint32_t slot = 0U; slot < 21U; ++slot) {
    const std::string id = "firo-" + std::to_string(slot + 1U);
    ids.emplace_back(id);
    configs.emplace_back(boost::json::object{{"id", id}});
    manifest.nodes.push_back(
        {.node_id = id,
         .slot = slot,
         .data_dir = std::filesystem::path("nodes") / id / "data",
         .root_name = std::nullopt});
  }
  const boost::json::object publication{
      {"generation", 2U},
      {"node_count", 21U},
      {"node_capacity", 21U},
      {"node_ids", ids},
      {"node_configs", configs},
      {"topology", boost::json::object{}},
      {"topology_current_edges", boost::json::array{}},
      {"network_allocation", root.options.network_address_plan->ToSerialized()},
      {"manifest_state", "live"}};
  bbp::AppendLine(root.path() / "events.jsonl",
                  boost::json::serialize(boost::json::object{
                      {"run_id", root.options.run_id},
                      {"event", "runtime_generation_published"},
                      {"detail", boost::json::serialize(publication)}}));
  auto retained = root.Discover();
  BOOST_REQUIRE_EQUAL(retained.size(), 1U);
  BOOST_TEST(retained.front().node_capacity == 21U);
  BOOST_TEST(retained.front().node_count == 21U);
  BOOST_CHECK(retained.front().network_allocation ==
              publication.at("network_allocation"));
  auto report = bbp::BuildRunReport(root.path());
  BOOST_CHECK(report.at("nodes") == 21U);
  BOOST_CHECK(report.at("node_capacity") == 21U);
  BOOST_CHECK(report.at("network_allocation") ==
              publication.at("network_allocation"));
  BOOST_CHECK(report.at("inventory_publication_complete").as_bool());
  BOOST_CHECK(report.at("node_configs") == configs);

  std::filesystem::remove(root.path() / "events.jsonl");
  bbp::WriteRuntimeNodeResourceManifest(manifest);
  retained = root.Discover();
  BOOST_REQUIRE_EQUAL(retained.size(), 1U);
  BOOST_TEST(retained.front().node_capacity == 21U);
  BOOST_TEST(retained.front().node_count == 21U);
  BOOST_CHECK(retained.front().network_allocation ==
              publication.at("network_allocation"));
  report = bbp::BuildRunReport(root.path());
  BOOST_CHECK(report.at("nodes") == 21U);
  BOOST_CHECK(report.at("node_capacity") == 21U);
  BOOST_CHECK(report.at("network_allocation") ==
              publication.at("network_allocation"));
  BOOST_CHECK(!report.at("inventory_publication_complete").as_bool());
  BOOST_CHECK(report.at("node_configs").is_null());
  BOOST_CHECK(report.at("topology").is_null());
  BOOST_CHECK(report.at("status") == "incomplete");
}

BOOST_AUTO_TEST_CASE(retained_capacity_recovery_excludes_pending_additions) {
  RetainedCapacityRoot root("pending-growth");
  const auto committed_plan = bbp::SimulationNetworkAddressPlan::FromCidr(
      root.options.network_address_pool, 16U);
  bbp::RuntimeNodeResourceManifest manifest{
      .ownership = *root.options.run_ownership,
      .isolated_network = true,
      .node_capacity = 16U,
      .network_address_plan = committed_plan,
      .nodes = {},
  };
  boost::json::array configs;
  for (std::uint32_t slot = 0U; slot < 21U; ++slot) {
    const std::string id = "firo-" + std::to_string(slot + 1U);
    if (slot < 11U) {
      configs.emplace_back(boost::json::object{{"id", id}});
    }
    manifest.nodes.push_back({
        .node_id = id,
        .slot = slot,
        .data_dir = std::filesystem::path("nodes") / id / "data",
        .root_name = std::nullopt,
        .state = slot < 11U ? bbp::RuntimeNodeResourceState::kLive
                            : bbp::RuntimeNodeResourceState::kPendingAdd,
    });
  }
  // A crash can leave grown documents while the manifest remains pending.
  const boost::json::object resolved{
      {"run_id", root.options.run_id},
      {"chain", "firo"},
      {"nodes", 11U},
      {"node_configs", configs},
      {"node_capacity", 21U},
      {"network_allocation", root.options.network_address_plan->ToSerialized()},
  };
  bbp::WriteText(root.path() / "resolved-scenario.json",
                 boost::json::serialize(resolved));
  const boost::json::object source{
      {"run_id", root.options.run_id},
      {"nodes", configs},
      {"node_capacity", 21U},
      {"actions", boost::json::array{boost::json::object{
                      {"command", "node.add"}, {"count", 10U}}}},
  };
  bbp::WriteText(root.path() / "source-scenario.json",
                 boost::json::serialize(source));
  bbp::WriteRuntimeNodeResourceManifest(manifest);

  const auto retained = root.Discover();
  BOOST_REQUIRE_EQUAL(retained.size(), 1U);
  BOOST_TEST(retained.front().node_count == 11U);
  BOOST_TEST(retained.front().node_capacity == 16U);
  BOOST_CHECK(retained.front().network_allocation ==
              committed_plan.ToSerialized());

  const auto report = bbp::BuildRunReport(root.path());
  BOOST_CHECK(report.at("nodes") == 11U);
  BOOST_CHECK(report.at("node_capacity") == 16U);
  BOOST_CHECK(report.at("network_allocation") == committed_plan.ToSerialized());
  BOOST_CHECK(report.at("node_configs") == configs);
  BOOST_TEST(report.at("node_ids").as_array().size() == 11U);

  const auto replay = bbp::simulator_app_internal::LoadRetainedSourceScenario(
      root.path(), root.options.run_id, {});
  BOOST_CHECK(replay.at("node_capacity") == 16U);
  BOOST_CHECK(std::string(replay.at("network_address_pool").as_string()) ==
              root.options.network_address_pool);
  BOOST_CHECK(replay.at("nodes") == source.at("nodes"));
  BOOST_CHECK(replay.at("actions") == source.at("actions"));
  BOOST_CHECK(!replay.contains("network_allocation"));
}

BOOST_AUTO_TEST_CASE(
    capacity_document_publication_preserves_replay_and_restores_exact_bytes) {
  RetainedCapacityRoot root("grown-documents");
  const std::string source =
      "{ \"run_id\":\"grown-documents\",\"nodes\":[{\"id\":\"firo-1\"}],"
      "\"node_capacity\":16,\"actions\":[{\"command\":\"node.add\",\"count\":"
      "10}]}\n";
  const std::string resolved =
      "{ \"run_id\":\"grown-documents\",\"nodes\":11,\"node_capacity\":16}\n";
  bbp::WriteText(root.path() / "source-scenario.json", source);
  bbp::WriteText(root.path() / "resolved-scenario.json", resolved);
  bbp::WriteText(root.path() / "scenario.yaml",
                 "# exact original YAML\nnodes: 11\n");
  const auto prepared =
      bbp::simulator_app_internal::PrepareRuntimeCapacityDocuments(
          root.options);
  bbp::simulator_app_internal::PublishRuntimeCapacityDocuments(prepared);
  const auto updated =
      boost::json::parse(bbp::ReadText(root.path() / "source-scenario.json"));
  const auto original = boost::json::parse(source);
  BOOST_CHECK(updated.at("nodes") == original.at("nodes"));
  BOOST_CHECK(updated.at("actions") == original.at("actions"));
  BOOST_TEST(updated.at("node_capacity").as_int64() == 21);
  BOOST_CHECK(!updated.as_object().contains("network_allocation"));
  bbp::simulator_app_internal::RestoreRuntimeCapacityDocuments(prepared);
  for (const auto& change : prepared.changes) {
    BOOST_TEST(bbp::ReadText(change.path) == change.before);
  }
}

BOOST_AUTO_TEST_CASE(capacity_document_partial_write_is_removed_and_retryable) {
  RetainedCapacityRoot root("partial-write");
  bbp::WriteText(
      root.path() / "resolved-scenario.json",
      "{\"run_id\":\"partial-write\",\"nodes\":11,\"node_capacity\":16}\n");
  bbp::WriteText(root.path() / "scenario.yaml", "nodes: 11\n");
  const auto prepared =
      bbp::simulator_app_internal::PrepareRuntimeCapacityDocuments(
          root.options);
  const pid_t child = fork();
  BOOST_REQUIRE(child >= 0);
  if (child == 0) {
    struct rlimit file_size{};
    if (getrlimit(RLIMIT_FSIZE, &file_size) != 0 ||
        std::signal(SIGXFSZ, SIG_IGN) == SIG_ERR) {
      _exit(1);
    }
    file_size.rlim_cur = 64U;
    if (setrlimit(RLIMIT_FSIZE, &file_size) != 0) {
      _exit(2);
    }
    try {
      bbp::simulator_app_internal::PublishRuntimeCapacityDocuments(prepared);
    } catch (const std::system_error& error) {
      _exit(error.code() == std::errc::file_too_large ? 0 : 3);
    } catch (...) {
      _exit(4);
    }
    _exit(5);
  }
  int status = 0;
  pid_t waited;
  do {
    waited = waitpid(child, &status, 0);
  } while (waited < 0 && errno == EINTR);
  BOOST_REQUIRE(waited == child);
  BOOST_REQUIRE(WIFEXITED(status));
  BOOST_REQUIRE_EQUAL(WEXITSTATUS(status), 0);
  for (const auto& change : prepared.changes) {
    BOOST_TEST(bbp::ReadText(change.path) == change.before);
    BOOST_CHECK(!std::filesystem::exists(
        root.path() /
        ("." + change.path.filename().string() + ".capacity.tmp")));
  }
  bbp::simulator_app_internal::RestoreRuntimeCapacityDocuments(prepared);
  bbp::simulator_app_internal::PublishRuntimeCapacityDocuments(prepared);
  for (const auto& change : prepared.changes) {
    BOOST_TEST(bbp::ReadText(change.path) == change.after);
  }
}

BOOST_AUTO_TEST_CASE(capacity_document_creation_preserves_existing_temporary) {
  RetainedCapacityRoot root("existing-temporary");
  bbp::WriteText(root.path() / "resolved-scenario.json",
                 "{\"run_id\":\"existing-temporary\",\"nodes\":11,\"node_"
                 "capacity\":16}\n");
  bbp::WriteText(root.path() / "scenario.yaml", "nodes: 11\n");
  const auto prepared =
      bbp::simulator_app_internal::PrepareRuntimeCapacityDocuments(
          root.options);
  const auto target = root.options.output_dir / "unrelated";
  const auto temporary = root.path() / ".resolved-scenario.json.capacity.tmp";
  bbp::WriteText(target, "preserve unrelated file\n");
  std::filesystem::create_symlink(target, temporary);
  BOOST_CHECK_THROW(
      bbp::simulator_app_internal::PublishRuntimeCapacityDocuments(prepared),
      std::system_error);
  bbp::simulator_app_internal::RestoreRuntimeCapacityDocuments(prepared);
  BOOST_CHECK(std::filesystem::is_symlink(temporary));
  BOOST_TEST(bbp::ReadText(target) == "preserve unrelated file\n");
  for (const auto& change : prepared.changes) {
    BOOST_TEST(bbp::ReadText(change.path) == change.before);
  }
}
