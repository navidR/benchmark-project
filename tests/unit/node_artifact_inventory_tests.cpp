#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>

#include "bbp/node_artifact_inventory.h"
#include "bbp/util.h"

namespace {

std::filesystem::path MakeTestDir(const std::string& name) {
  const std::filesystem::path directory =
      std::filesystem::temp_directory_path() /
      ("bbp-node-artifacts-" + name + "-" + std::to_string(getpid()));
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  return directory;
}

const bbp::NodeArtifactEntry* FindEntry(
    const std::vector<bbp::NodeArtifactEntry>& entries,
    std::string_view relative_path) {
  for (const bbp::NodeArtifactEntry& entry : entries) {
    if (entry.relative_path == relative_path) {
      return &entry;
    }
  }
  return nullptr;
}

}  // namespace

BOOST_AUTO_TEST_CASE(node_artifact_inventory_lists_owned_data_and_logs) {
  const std::filesystem::path run_root = MakeTestDir("list");
  const std::filesystem::path node_root = run_root / "nodes" / "firo-1";
  const std::filesystem::path data_root = node_root / "data";
  std::filesystem::create_directories(data_root / "regtest" / "blocks");
  bbp::WriteText(node_root / "stdout.log", "stdout\n");
  bbp::WriteText(node_root / "stderr.log", "");
  bbp::WriteText(data_root / "regtest" / "debug.log", "daemon\n");
  bbp::WriteText(data_root / "regtest" / "blocks" / "blk00000.dat", "abc");

  const bbp::NodeArtifactInventory inventory =
      bbp::InspectNodeArtifacts(run_root, "firo-1");

  BOOST_TEST(inventory.error.empty());
  BOOST_TEST(inventory.warning.empty());
  BOOST_TEST(inventory.data_directory == "nodes/firo-1/data");
  const bbp::NodeArtifactEntry* block =
      FindEntry(inventory.data_entries, "regtest/blocks/blk00000.dat");
  BOOST_REQUIRE(block != nullptr);
  BOOST_CHECK(block->type == bbp::NodeArtifactType::kRegularFile);
  BOOST_TEST(block->size_bytes == 3U);
  BOOST_REQUIRE(FindEntry(inventory.log_files, "stdout.log") != nullptr);
  BOOST_REQUIRE(FindEntry(inventory.log_files, "stderr.log") != nullptr);
  BOOST_REQUIRE(FindEntry(inventory.log_files, "data/regtest/debug.log") !=
                nullptr);

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(node_artifact_inventory_never_follows_symbolic_links) {
  const std::filesystem::path run_root = MakeTestDir("symlink");
  const std::filesystem::path outside = MakeTestDir("outside");
  const std::filesystem::path data_root =
      run_root / "nodes" / "firo-1" / "data";
  std::filesystem::create_directories(data_root);
  bbp::WriteText(outside / "secret.log", "must not be listed\n");
  std::filesystem::create_directory_symlink(outside, data_root / "escape");

  const bbp::NodeArtifactInventory inventory =
      bbp::InspectNodeArtifacts(run_root, "firo-1");

  BOOST_TEST(inventory.error.empty());
  const bbp::NodeArtifactEntry* link =
      FindEntry(inventory.data_entries, "escape");
  BOOST_REQUIRE(link != nullptr);
  BOOST_CHECK(link->type == bbp::NodeArtifactType::kSymbolicLink);
  BOOST_TEST(FindEntry(inventory.data_entries, "escape/secret.log") == nullptr);
  BOOST_TEST(FindEntry(inventory.log_files, "data/escape/secret.log") ==
             nullptr);

  std::filesystem::remove_all(run_root);
  std::filesystem::remove_all(outside);
}

BOOST_AUTO_TEST_CASE(node_artifact_inventory_uses_custom_owned_data_path) {
  const std::filesystem::path run_root = MakeTestDir("custom-data");
  const std::filesystem::path data_root =
      run_root / "nodes" / "firo-1" / "state.v1" / "chain";
  std::filesystem::create_directories(data_root);
  bbp::WriteText(data_root / "debug.log", "daemon\n");

  const bbp::NodeArtifactInventory inventory = bbp::InspectNodeArtifacts(
      run_root, "firo-1", "nodes/firo-1/state.v1/chain");
  BOOST_TEST(inventory.error.empty());
  BOOST_TEST(inventory.warning.empty());
  BOOST_TEST(inventory.data_directory == "nodes/firo-1/state.v1/chain");
  BOOST_REQUIRE(FindEntry(inventory.data_entries, "debug.log") != nullptr);
  BOOST_REQUIRE(FindEntry(inventory.log_files, "state.v1/chain/debug.log") !=
                nullptr);

  const bbp::NodeArtifactInventory outside =
      bbp::InspectNodeArtifacts(run_root, "firo-1", "nodes/firo-2/data");
  BOOST_TEST(!outside.error.empty());

  std::filesystem::path excessive = std::filesystem::path("nodes") / "firo-1";
  for (std::size_t index = 0U; index < 17U; ++index) {
    excessive /= std::string(64U, 'a');
  }
  const bbp::NodeArtifactInventory excessive_inventory =
      bbp::InspectNodeArtifacts(run_root, "firo-1", excessive);
  BOOST_TEST(excessive_inventory.error ==
             "node artifact browser rejected an excessive data path");

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(node_artifact_inventory_rejects_unsafe_and_linked_nodes) {
  const std::filesystem::path run_root = MakeTestDir("unsafe");
  const std::filesystem::path outside = MakeTestDir("linked-node");
  std::filesystem::create_directories(run_root / "nodes");
  std::filesystem::create_directories(outside / "data");
  std::filesystem::create_directory_symlink(outside,
                                            run_root / "nodes" / "firo-1");

  const bbp::NodeArtifactInventory unsafe =
      bbp::InspectNodeArtifacts(run_root, "../linked-node");
  BOOST_TEST(!unsafe.error.empty());
  const bbp::NodeArtifactInventory linked =
      bbp::InspectNodeArtifacts(run_root, "firo-1");
  BOOST_TEST(!linked.error.empty());

  std::filesystem::remove_all(run_root);
  std::filesystem::remove_all(outside);
}

BOOST_AUTO_TEST_CASE(node_artifact_inventory_escapes_names_and_bounds_results) {
  const std::filesystem::path run_root = MakeTestDir("bounded");
  const std::filesystem::path data_root =
      run_root / "nodes" / "firo-1" / "data";
  std::filesystem::create_directories(data_root);
  bbp::WriteText(data_root / std::filesystem::path("\nline.dat"), "x");
  for (std::size_t index = 0U; index < 520U; ++index) {
    bbp::WriteText(data_root / ("entry-" + std::to_string(index)), "");
  }

  const bbp::NodeArtifactInventory inventory =
      bbp::InspectNodeArtifacts(run_root, "firo-1");

  BOOST_TEST(inventory.error.empty());
  BOOST_TEST(inventory.data_entries.size() == 512U);
  BOOST_TEST(inventory.data_entries_truncated);
  BOOST_REQUIRE(FindEntry(inventory.data_entries, "\\x0Aline.dat") != nullptr);
  for (const bbp::NodeArtifactEntry& entry : inventory.data_entries) {
    BOOST_TEST(entry.relative_path.find('\n') == std::string::npos);
  }

  std::filesystem::remove_all(run_root);
}

BOOST_AUTO_TEST_CASE(node_artifact_inventory_bounds_total_visited_entries) {
  const std::filesystem::path run_root = MakeTestDir("visited-bound");
  const std::filesystem::path node_root = run_root / "nodes" / "firo-1";
  const std::filesystem::path data_root = node_root / "data";
  std::filesystem::create_directories(data_root);
  bbp::WriteText(data_root / "beyond-total-bound", "hidden by bound");
  for (std::size_t index = 0U; index < 4095U; ++index) {
    bbp::WriteText(node_root / ("root-entry-" + std::to_string(index)), "");
  }

  const bbp::NodeArtifactInventory inventory =
      bbp::InspectNodeArtifacts(run_root, "firo-1");

  BOOST_TEST(inventory.error.empty());
  BOOST_TEST(inventory.data_entries.empty());
  BOOST_TEST(inventory.data_entries_truncated);
  BOOST_TEST(inventory.log_files_truncated);

  std::filesystem::remove_all(run_root);
}
