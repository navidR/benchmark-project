#include <unistd.h>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>

#include "bbp/node_file_pane.h"
#include "bbp/util.h"

namespace {

std::filesystem::path MakeTestDir(const std::string& name) {
  const std::filesystem::path directory =
      std::filesystem::temp_directory_path() /
      ("bbp-node-file-pane-" + name + "-" + std::to_string(getpid()));
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory / "nodes" / "firo-1" / "data" /
                                      "regtest");
  bbp::WriteText(directory / "nodes" / "firo-1" / "stdout.log", "out\n");
  bbp::WriteText(
      directory / "nodes" / "firo-1" / "data" / "regtest" / "debug.log",
      "debug\n");
  return directory;
}

boost::json::object MakeReport() {
  boost::json::object metrics;
  metrics["data_dir"] = "/run/nodes/firo-1/data";
  metrics["log_dir"] = "/run/nodes/firo-1";
  metrics["rpc_host"] = "10.0.0.2";
  metrics["rpc_port"] = 18888;
  boost::json::object node;
  node["node_id"] = "firo-1";
  node["last_metrics"] = std::move(metrics);
  boost::json::array nodes;
  nodes.push_back(std::move(node));

  boost::json::object resolved_resources;
  resolved_resources["memory_max_bytes"] = 2048;
  boost::json::object resources;
  resources["resolved"] = std::move(resolved_resources);
  boost::json::object config;
  config["index"] = 1;
  config["id"] = "firo-1";
  config["chain"] = "firo";
  config["role"] = "base";
  config["rpc_password"] = "must-not-render";
  config["resources"] = std::move(resources);
  boost::json::array configs;
  configs.push_back(std::move(config));

  boost::json::object report;
  report["nodes_summary"] = std::move(nodes);
  report["node_configs"] = std::move(configs);
  return report;
}

bool HasLineContaining(const std::vector<std::string>& lines,
                       std::string_view expected) {
  for (const std::string& line : lines) {
    if (line.find(expected) != std::string::npos) {
      return true;
    }
  }
  return false;
}

}  // namespace

BOOST_AUTO_TEST_CASE(node_file_pane_cycles_safe_artifact_sections) {
  const std::filesystem::path run_root = MakeTestDir("sections");
  const boost::json::object report = MakeReport();
  bbp::NodeFilePane pane;

  BOOST_TEST(!pane.IsOpen());
  pane.Toggle(run_root, report, 0U);
  BOOST_TEST(pane.IsOpen());
  BOOST_TEST(pane.NodeId() == "firo-1");
  BOOST_CHECK(pane.Section() == bbp::NodeFileSection::kDataDirectory);
  BOOST_TEST(HasLineContaining(pane.Lines(), "directory: nodes/firo-1/data"));
  BOOST_TEST(HasLineContaining(pane.Lines(), "regtest/debug.log"));

  pane.NextSection();
  BOOST_CHECK(pane.Section() == bbp::NodeFileSection::kConfiguration);
  BOOST_TEST(HasLineContaining(
      pane.Lines(), "node.resources.resolved.memory_max_bytes = 2048"));
  BOOST_TEST(
      HasLineContaining(pane.Lines(), "node.rpc_password = \"<redacted>\""));
  BOOST_TEST(!HasLineContaining(pane.Lines(), "must-not-render"));
  BOOST_TEST(
      HasLineContaining(pane.Lines(), "runtime.rpc_host = \"10.0.0.2\""));

  pane.NextSection();
  BOOST_CHECK(pane.Section() == bbp::NodeFileSection::kLogFiles);
  BOOST_TEST(HasLineContaining(pane.Lines(), "stdout.log"));
  BOOST_TEST(HasLineContaining(pane.Lines(), "data/regtest/debug.log"));
  BOOST_TEST(HasLineContaining(pane.Lines(), "press l"));

  pane.NextSection();
  BOOST_CHECK(pane.Section() == bbp::NodeFileSection::kDataDirectory);
  pane.PreviousSection();
  BOOST_CHECK(pane.Section() == bbp::NodeFileSection::kLogFiles);
  pane.Toggle(run_root, report, 0U);
  BOOST_TEST(!pane.IsOpen());

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(node_file_pane_scrolls_and_reloads) {
  const std::filesystem::path run_root = MakeTestDir("scroll");
  const boost::json::object report = MakeReport();
  for (std::size_t index = 0U; index < 8U; ++index) {
    bbp::WriteText(run_root / "nodes" / "firo-1" / "data" /
                       ("file-" + std::to_string(index)),
                   "");
  }
  bbp::NodeFilePane pane;
  pane.Toggle(run_root, report, 0U);

  BOOST_TEST(pane.FirstVisibleLine(3U) == 0U);
  pane.ScrollDown(3U, 2U);
  BOOST_TEST(pane.FirstVisibleLine(3U) == 2U);
  pane.ScrollEnd(3U);
  BOOST_TEST(pane.LastVisibleLine(3U) == pane.Lines().size());
  pane.ScrollHome();
  BOOST_TEST(pane.FirstVisibleLine(3U) == 0U);

  bbp::WriteText(run_root / "nodes" / "firo-1" / "data" / "new-file", "x");
  pane.Reload(run_root, report, 0U);
  BOOST_TEST(HasLineContaining(pane.Lines(), "new-file"));

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(node_file_pane_uses_resolved_custom_data_directory) {
  const std::filesystem::path run_root = MakeTestDir("custom-data");
  std::filesystem::create_directories(run_root / "nodes" / "firo-1" /
                                      "state.v1" / "regtest");
  bbp::WriteText(
      run_root / "nodes" / "firo-1" / "state.v1" / "regtest" / "debug.log",
      "debug\n");
  boost::json::object report = MakeReport();
  report.at("node_configs").as_array().front().as_object()["data_dir"] =
      "nodes/firo-1/state.v1";

  bbp::NodeFilePane pane;
  pane.Toggle(run_root, report, 0U);
  BOOST_TEST(
      HasLineContaining(pane.Lines(), "directory: nodes/firo-1/state.v1"));
  BOOST_TEST(HasLineContaining(pane.Lines(), "regtest/debug.log"));
  pane.PreviousSection();
  BOOST_TEST(HasLineContaining(pane.Lines(), "state.v1/regtest/debug.log"));

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(
    node_file_pane_redacts_sensitive_containers_and_bounds_nesting) {
  const std::filesystem::path run_root = MakeTestDir("safe-config");
  boost::json::object report = MakeReport();
  boost::json::object& config =
      report.at("node_configs").as_array().front().as_object();

  boost::json::object secret;
  secret["nested"] = "must-not-render-from-container";
  config["rpc_token_bundle"] = std::move(secret);
  config[std::string("unsafe-\x1B-key", 12U)] = true;

  boost::json::value nested = "must-not-reach-the-view";
  for (std::size_t depth = 0U; depth < 32U; ++depth) {
    boost::json::object parent;
    parent["level"] = std::move(nested);
    nested = std::move(parent);
  }
  config["deep"] = std::move(nested);

  bbp::NodeFilePane pane;
  pane.Toggle(run_root, report, 0U);
  pane.NextSection();

  BOOST_TEST(HasLineContaining(pane.Lines(),
                               "node.rpc_token_bundle = \"<redacted>\""));
  BOOST_TEST(
      !HasLineContaining(pane.Lines(), "must-not-render-from-container"));
  BOOST_TEST(
      HasLineContaining(pane.Lines(), "<nested value truncated at depth 16>"));
  BOOST_TEST(!HasLineContaining(pane.Lines(), "must-not-reach-the-view"));
  BOOST_TEST(HasLineContaining(pane.Lines(), "node.unsafe-\\x1B-key = true"));
  for (const std::string& line : pane.Lines()) {
    BOOST_TEST(line.find('\x1B') == std::string::npos);
  }

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(
    node_file_pane_marks_only_actual_configuration_line_truncation) {
  const std::filesystem::path run_root = MakeTestDir("exact-config-bound");
  boost::json::object node;
  node["node_id"] = "firo-1";
  boost::json::array nodes;
  nodes.push_back(std::move(node));

  boost::json::object config;
  config["id"] = "firo-1";
  for (std::size_t index = 0U; index < 254U; ++index) {
    config["field-" + std::to_string(index)] = index;
  }
  boost::json::array configs;
  configs.push_back(config);
  boost::json::object report;
  report["nodes_summary"] = nodes;
  report["node_configs"] = configs;

  bbp::NodeFilePane pane;
  pane.Toggle(run_root, report, 0U);
  pane.NextSection();
  BOOST_TEST(pane.Lines().size() == 256U);
  BOOST_TEST(!HasLineContaining(pane.Lines(), "configuration view truncated"));

  config["overflow"] = true;
  configs.clear();
  configs.push_back(std::move(config));
  report["node_configs"] = std::move(configs);
  pane.Reload(run_root, report, 0U);
  BOOST_TEST(pane.Lines().size() == 256U);
  BOOST_TEST(HasLineContaining(pane.Lines(),
                               "configuration view truncated at a safe line "
                               "bound"));

  std::filesystem::remove_all(run_root);
}
