#include <sys/stat.h>
#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <thread>

#ifdef __linux__
#include <sys/mount.h>
#endif

#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator_app.h"
#include "bbp/util.h"

namespace {

class ManifestTestRoot {
 public:
  explicit ManifestTestRoot(std::string run_id = "manifest-test")
      : path_(std::filesystem::temp_directory_path() /
              ("bbp-runtime-manifest-" + run_id + "-" +
               std::to_string(getpid()))),
        run_id_(std::move(run_id)) {
    std::filesystem::remove_all(path_);
    std::filesystem::remove(
        bbp::OwnedRunRootCleanupReceiptPath(run_id_, path_));
    std::filesystem::remove(path_.parent_path() /
                            (".bbp-retired-run-cleanup-receipt-" + run_id_));
    std::filesystem::create_directories(path_ / "nodes");
    ownership_ = bbp::CreateRunOwnership(run_id_, path_);
    bbp::WriteRunOwnershipMarker(ownership_);
  }

  ~ManifestTestRoot() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
    std::filesystem::remove_all(
        bbp::OwnedRunRootCleanupQuarantinePath(ownership_), ignored);
    std::filesystem::remove(bbp::OwnedRunRootCleanupReceiptPath(
                                ownership_.run_id, ownership_.run_root),
                            ignored);
    std::filesystem::remove(
        ownership_.run_root.parent_path() /
            (".bbp-retired-run-cleanup-receipt-" + ownership_.run_id),
        ignored);
  }

  const std::filesystem::path& path() const { return path_; }
  const bbp::RunOwnership& ownership() const { return ownership_; }

 private:
  std::filesystem::path path_;
  std::string run_id_;
  bbp::RunOwnership ownership_;
};

class EditorCleanupTestRoot {
 public:
  explicit EditorCleanupTestRoot(std::string run_id)
      : benchmark_root_(std::filesystem::temp_directory_path() /
                        ("bbp-editor-cleanup-" + run_id + "-" +
                         std::to_string(static_cast<long long>(getpid())))),
        run_root_(benchmark_root_ / run_id) {
    std::filesystem::remove_all(benchmark_root_);
    std::filesystem::create_directories(run_root_ / "nodes");
    ownership_ = bbp::CreateRunOwnership(std::move(run_id), run_root_);
    bbp::WriteRunOwnershipMarker(ownership_);
    bbp::WriteRuntimeNodeResourceManifest(bbp::RuntimeNodeResourceManifest{
        .ownership = ownership_,
        .isolated_network = false,
        .nodes = {},
    });
  }

  ~EditorCleanupTestRoot() {
    std::error_code ignored;
    std::filesystem::remove_all(benchmark_root_, ignored);
  }

  const std::filesystem::path& benchmark_root() const {
    return benchmark_root_;
  }
  const std::filesystem::path& run_root() const { return run_root_; }
  const bbp::RunOwnership& ownership() const { return ownership_; }

 private:
  std::filesystem::path benchmark_root_;
  std::filesystem::path run_root_;
  bbp::RunOwnership ownership_;
};

class ScopedRunCleanupRootRemovedHook {
 public:
  explicit ScopedRunCleanupRootRemovedHook(std::function<void()> hook) {
    bbp::SetRunCleanupRootRemovedHookForTest(std::move(hook));
  }

  ~ScopedRunCleanupRootRemovedHook() {
    bbp::SetRunCleanupRootRemovedHookForTest({});
  }

  ScopedRunCleanupRootRemovedHook(const ScopedRunCleanupRootRemovedHook&) =
      delete;
  ScopedRunCleanupRootRemovedHook& operator=(
      const ScopedRunCleanupRootRemovedHook&) = delete;
};

bbp::RuntimeNodeResourceEntry Entry(
    std::string id, std::uint32_t slot,
    bbp::RuntimeNodeResourceState state = bbp::RuntimeNodeResourceState::kLive,
    std::optional<std::string> root_name = std::nullopt) {
  const std::string root =
      root_name ? *root_name : "firo-" + std::to_string(slot + 1U);
  return bbp::RuntimeNodeResourceEntry{
      .node_id = std::move(id),
      .slot = slot,
      .chain = bbp::ChainKind::kFiro,
      .data_dir = std::filesystem::path("nodes") / root / "data",
      .root_name = std::move(root_name),
      .state = state,
  };
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    runtime_node_manifest_round_trips_zero_live_and_pending_resources) {
  ManifestTestRoot root;
  bbp::RuntimeNodeResourceManifest manifest{
      .ownership = root.ownership(),
      .isolated_network = true,
      .nodes = {},
  };
  bbp::WriteRuntimeNodeResourceManifest(manifest);
  const auto empty = bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
  BOOST_REQUIRE(empty);
  BOOST_TEST(empty->nodes.empty());
  BOOST_TEST(empty->isolated_network);

  manifest.nodes = {
      Entry("firo-1", 0U),
      Entry("firo-2", 1U, bbp::RuntimeNodeResourceState::kPendingAdd),
      Entry("firo-3", 2U, bbp::RuntimeNodeResourceState::kPendingRemove),
  };
  bbp::WriteRuntimeNodeResourceManifest(manifest);
  const auto loaded = bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
  BOOST_REQUIRE(loaded);
  BOOST_CHECK(*loaded == manifest);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_manifest_round_trips_replacement_phases_and_root_name) {
  ManifestTestRoot root;
  for (const bbp::RuntimeNodeResourceState state :
       {bbp::RuntimeNodeResourceState::kPendingReplace,
        bbp::RuntimeNodeResourceState::kPendingReplaceExchanged,
        bbp::RuntimeNodeResourceState::kPendingReplaceCommitted,
        bbp::RuntimeNodeResourceState::kPendingReplaceUncertain}) {
    bbp::RuntimeNodeResourceManifest manifest{
        .ownership = root.ownership(),
        .isolated_network = true,
        .nodes = {Entry("firo-1", 0U),
                  Entry("firo-1", 0U, state, "replace-0-1")},
    };
    bbp::WriteRuntimeNodeResourceManifest(manifest);
    const auto loaded =
        bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
    BOOST_REQUIRE(loaded);
    BOOST_CHECK(*loaded == manifest);
  }
}

BOOST_AUTO_TEST_CASE(
    runtime_node_replacement_clone_and_exchange_preserve_both_owned_roots) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry live = Entry("firo-1", 0U);
  const bbp::RuntimeNodeResourceEntry staging =
      Entry("firo-1", 0U, bbp::RuntimeNodeResourceState::kPendingReplace,
            "replace-0-1");
  bbp::PrepareRuntimeNodeRoot(root.ownership(), live);
  const std::filesystem::path live_root = root.path() / "nodes" / live.node_id;
  std::filesystem::create_directories(live_root / "data" / "nested");
  bbp::WriteText(live_root / "data" / "nested" / "sentinel", "old\n");
  std::filesystem::create_symlink(
      "sentinel", live_root / "data" / "nested" / "sentinel-link");

  bbp::CloneRuntimeNodeRootForReplacement(root.ownership(), live, staging);
  const std::filesystem::path staging_root =
      root.path() / "nodes" / *staging.root_name;
  BOOST_TEST(bbp::ReadText(staging_root / "data" / "nested" / "sentinel") ==
             "old\n");
  BOOST_TEST(std::filesystem::read_symlink(staging_root / "data" / "nested" /
                                           "sentinel-link") ==
             std::filesystem::path("sentinel"));
  bbp::WriteText(staging_root / "data" / "nested" / "sentinel", "new\n");

  bbp::RuntimeNodeRootOrientation unknown_orientation =
      bbp::RuntimeNodeRootOrientation::kUnknown;
  BOOST_CHECK_THROW(bbp::ExchangeRuntimeNodeRootsForReplacement(
                        root.ownership(), live, staging, &unknown_orientation),
                    std::runtime_error);
  BOOST_TEST(bbp::ReadText(live_root / "data" / "nested" / "sentinel") ==
             "old\n");
  BOOST_TEST(bbp::ReadText(staging_root / "data" / "nested" / "sentinel") ==
             "new\n");

  bbp::RuntimeNodeRootOrientation orientation =
      bbp::RuntimeNodeRootOrientation::kOriginal;
  BOOST_CHECK_THROW(bbp::ExchangeRuntimeNodeRootsForReplacement(
                        root.ownership(), staging, live, &orientation),
                    std::runtime_error);
  bbp::ExchangeRuntimeNodeRootsForReplacement(root.ownership(), live, staging,
                                              &orientation);
  BOOST_CHECK(orientation == bbp::RuntimeNodeRootOrientation::kExchanged);
  BOOST_TEST(bbp::ReadText(live_root / "data" / "nested" / "sentinel") ==
             "new\n");
  BOOST_TEST(bbp::ReadText(staging_root / "data" / "nested" / "sentinel") ==
             "old\n");
  bbp::VerifyRuntimeNodeRootOwnership(root.ownership(), live);
  bbp::VerifyRuntimeNodeRootOwnership(root.ownership(), staging);
  bbp::RuntimeNodeResourceEntry uncertain = staging;
  uncertain.state = bbp::RuntimeNodeResourceState::kPendingReplaceUncertain;
  BOOST_CHECK_THROW(bbp::RemoveRuntimeNodeRoot(root.ownership(), uncertain),
                    std::runtime_error);

  bbp::ExchangeRuntimeNodeRootsForReplacement(root.ownership(), live, staging,
                                              &orientation);
  BOOST_CHECK(orientation == bbp::RuntimeNodeRootOrientation::kOriginal);
  BOOST_TEST(bbp::ReadText(live_root / "data" / "nested" / "sentinel") ==
             "old\n");
  bbp::RemoveRuntimeNodeRoot(root.ownership(), staging);
  bbp::RemoveRuntimeNodeRoot(root.ownership(), live);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_replacement_clone_reports_partial_root_acquisition) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry live = Entry("firo-1", 0U);
  const bbp::RuntimeNodeResourceEntry staging =
      Entry("firo-1", 0U, bbp::RuntimeNodeResourceState::kPendingReplace,
            "replace-0-1");
  bbp::PrepareRuntimeNodeRoot(root.ownership(), live);
  const std::filesystem::path live_root = root.path() / "nodes" / live.node_id;
  std::filesystem::create_directories(live_root / "data");
  BOOST_REQUIRE_EQUAL(mkfifo((live_root / "data" / "unsupported-fifo").c_str(),
                             S_IRUSR | S_IWUSR),
                      0);

  bool staging_acquired = false;
  BOOST_CHECK_THROW(
      bbp::CloneRuntimeNodeRootForReplacement(
          root.ownership(), live, staging, std::nullopt, {}, &staging_acquired),
      std::runtime_error);
  BOOST_TEST(staging_acquired);
  BOOST_TEST(bbp::RuntimeNodeRootEntryExists(root.ownership(), staging));
  bbp::RemoveRuntimeNodeRoot(root.ownership(), staging);
  bbp::RemoveRuntimeNodeRoot(root.ownership(), live);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_manifest_rejects_orphan_cross_paired_and_multiple_replacements) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry replacement =
      Entry("firo-1", 0U, bbp::RuntimeNodeResourceState::kPendingReplace,
            "replace-0-1");
  bbp::RuntimeNodeResourceManifest manifest{
      .ownership = root.ownership(),
      .isolated_network = true,
      .nodes = {replacement},
  };
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);

  manifest.nodes = {
      Entry("firo-1", 0U),
      Entry("firo-2", 1U),
      Entry("firo-1", 1U, bbp::RuntimeNodeResourceState::kPendingReplace,
            "replace-cross"),
  };
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);

  bbp::RuntimeNodeResourceEntry wrong_chain = replacement;
  wrong_chain.chain = bbp::ChainKind::kBitcoin;
  manifest.nodes = {Entry("firo-1", 0U), wrong_chain};
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);

  manifest.nodes = {
      Entry("firo-1", 0U),
      Entry("firo-2", 1U),
      replacement,
      Entry("firo-2", 1U, bbp::RuntimeNodeResourceState::kPendingReplace,
            "replace-1-1"),
  };
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);

  manifest.nodes = {
      Entry("firo-1", 0U),
      Entry("firo-1", 1U, bbp::RuntimeNodeResourceState::kPendingAdd)};
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);
  manifest.nodes = {
      Entry("firo-1", 0U),
      Entry("firo-2", 0U, bbp::RuntimeNodeResourceState::kPendingAdd)};
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_root_cleanup_is_descriptor_anchored_and_preserves_symlink_targets) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  BOOST_TEST(bbp::RuntimeNodeRootEntryExists(root.ownership(), entry.node_id));

  const std::filesystem::path outside =
      root.path().parent_path() /
      ("bbp-runtime-outside-" + std::to_string(getpid()));
  std::filesystem::remove_all(outside);
  std::filesystem::create_directories(outside);
  bbp::WriteText(outside / "sentinel", "preserve\n");
  std::filesystem::create_directory_symlink(
      outside, root.path() / "nodes" / entry.node_id / "outside-link");
  bbp::RemoveRuntimeNodeRoot(root.ownership(), entry);
  BOOST_TEST(!bbp::RuntimeNodeRootEntryExists(root.ownership(), entry.node_id));
  BOOST_TEST(std::filesystem::exists(outside / "sentinel"));

  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  const std::filesystem::path original_nodes = root.path() / "nodes-owned";
  std::filesystem::rename(root.path() / "nodes", original_nodes);
  std::filesystem::create_directory_symlink(outside, root.path() / "nodes");
  BOOST_CHECK_THROW(bbp::RemoveRuntimeNodeRoot(root.ownership(), entry),
                    std::runtime_error);
  BOOST_TEST(std::filesystem::exists(outside / "sentinel"));
  std::filesystem::remove(root.path() / "nodes");
  std::filesystem::rename(original_nodes, root.path() / "nodes");
  bbp::RemoveRuntimeNodeRoot(root.ownership(), entry);
  std::filesystem::remove_all(outside);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_root_cleanup_refuses_unmarked_empty_collisions) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  std::filesystem::create_directory(root.path() / "nodes" / entry.node_id);

  BOOST_CHECK_THROW(bbp::RemoveRuntimeNodeRoot(root.ownership(), entry),
                    std::runtime_error);
  BOOST_TEST(
      std::filesystem::is_directory(root.path() / "nodes" / entry.node_id));
}

BOOST_AUTO_TEST_CASE(
    runtime_node_root_acquisition_leaves_foreign_collision_and_manifest_unchanged) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceManifest manifest{
      .ownership = root.ownership(),
      .isolated_network = false,
      .nodes = {},
  };
  bbp::WriteRuntimeNodeResourceManifest(manifest);
  const std::filesystem::path manifest_path =
      root.path() / "runtime-node-resources.json";
  const std::string manifest_before = bbp::ReadText(manifest_path);

  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  const std::filesystem::path collision = root.path() / "nodes" / entry.node_id;
  std::filesystem::create_directory(collision);
  bbp::WriteText(collision / "sentinel", "foreign\n");
  bool acquired = true;
  BOOST_CHECK_THROW(
      bbp::PrepareRuntimeNodeRoot(root.ownership(), entry, &acquired),
      std::runtime_error);
  BOOST_TEST(!acquired);
  BOOST_TEST(bbp::ReadText(collision / "sentinel") == "foreign\n");
  BOOST_TEST(bbp::ReadText(manifest_path) == manifest_before);
  const auto loaded = bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
  BOOST_REQUIRE(loaded);
  BOOST_CHECK(*loaded == manifest);
}

BOOST_AUTO_TEST_CASE(
    runtime_owned_run_cleanup_is_descriptor_anchored_and_preserves_symlink_targets) {
  const std::filesystem::path outside =
      std::filesystem::temp_directory_path() /
      ("bbp-runtime-run-outside-" + std::to_string(getpid()));
  std::filesystem::remove_all(outside);
  std::filesystem::create_directories(outside);
  bbp::WriteText(outside / "sentinel", "outside\n");
  {
    ManifestTestRoot root;
    bbp::WriteText(root.path() / "artifact", "owned\n");
    std::filesystem::create_directory_symlink(outside,
                                              root.path() / "outside-link");
    const std::filesystem::path run_path = root.path();
    bbp::RemoveOwnedRunRoot(root.ownership());
    BOOST_TEST(!std::filesystem::exists(run_path));
    BOOST_TEST(std::filesystem::exists(outside / "sentinel"));
  }
  std::filesystem::remove_all(outside);
}

BOOST_AUTO_TEST_CASE(
    runtime_owned_run_cleanup_refuses_a_same_path_copied_marker_replacement) {
  ManifestTestRoot root;
  const std::filesystem::path public_root = root.path();
  const std::filesystem::path preserved_root =
      public_root.parent_path() / (public_root.filename().string() + "-owned");
  std::filesystem::remove_all(preserved_root);
  bbp::WriteText(public_root / "owned-sentinel", "owned\n");
  struct stat owned_status{};
  BOOST_REQUIRE_EQUAL(stat(public_root.c_str(), &owned_status), 0);
  const bbp::OwnedRunRootIdentity expected{
      .device = static_cast<std::uintmax_t>(owned_status.st_dev),
      .inode = static_cast<std::uintmax_t>(owned_status.st_ino),
  };
  const std::string copied_marker =
      bbp::ReadText(public_root / std::string(bbp::kRunMarkerFile));

  std::filesystem::rename(public_root, preserved_root);
  std::filesystem::create_directory(public_root);
  bbp::WriteText(public_root / std::string(bbp::kRunMarkerFile), copied_marker);
  bbp::WriteText(public_root / "foreign-sentinel", "foreign\n");
  BOOST_CHECK_THROW(
      bbp::RemoveOwnedRunRoot(root.ownership(), std::nullopt, {}, expected),
      bbp::OwnedRunRootIdentityMismatch);

  const bool foreign_preserved =
      std::filesystem::exists(public_root / "foreign-sentinel") &&
      std::filesystem::exists(public_root / std::string(bbp::kRunMarkerFile));
  const bool owned_preserved =
      std::filesystem::exists(preserved_root / "owned-sentinel") &&
      std::filesystem::exists(preserved_root /
                              std::string(bbp::kRunMarkerFile));
  const bool quarantine_absent = !std::filesystem::exists(
      bbp::OwnedRunRootCleanupQuarantinePath(root.ownership()));
  std::filesystem::remove_all(public_root);
  std::filesystem::rename(preserved_root, public_root);

  BOOST_TEST(foreign_preserved);
  BOOST_TEST(owned_preserved);
  BOOST_TEST(quarantine_absent);
}

BOOST_AUTO_TEST_CASE(
    runtime_owned_run_cleanup_receipt_recovers_a_quarantined_root) {
  ManifestTestRoot root;
  struct stat root_status{};
  BOOST_REQUIRE_EQUAL(stat(root.path().c_str(), &root_status), 0);
  const bbp::OwnedRunRootIdentity identity{
      .device = static_cast<std::uintmax_t>(root_status.st_dev),
      .inode = static_cast<std::uintmax_t>(root_status.st_ino),
  };
  const bbp::OwnedRunRootCleanupReceipt expected{
      .ownership = root.ownership(),
      .root_identity = identity,
  };
  bbp::WriteOwnedRunRootCleanupReceipt(root.ownership(), identity);
  const auto published = bbp::TryLoadOwnedRunRootCleanupReceipt(
      root.ownership().run_id, root.ownership().run_root);
  BOOST_REQUIRE(published);
  BOOST_CHECK(*published == expected);

  const std::filesystem::path quarantine =
      bbp::OwnedRunRootCleanupQuarantinePath(root.ownership());
  std::filesystem::rename(root.path(), quarantine);
  const auto recovered = bbp::TryLoadOwnedRunRootCleanupReceipt(
      root.ownership().run_id, root.ownership().run_root);
  BOOST_REQUIRE(recovered);
  BOOST_CHECK(*recovered == expected);
  bbp::RemoveOwnedRunRoot(recovered->ownership, std::nullopt, {},
                          recovered->root_identity);
  BOOST_TEST(!std::filesystem::exists(root.path()));
  BOOST_TEST(!std::filesystem::exists(quarantine));

  const auto tombstone = bbp::TryLoadOwnedRunRootCleanupReceipt(
      root.ownership().run_id, root.ownership().run_root);
  BOOST_REQUIRE(tombstone);
  BOOST_CHECK(*tombstone == expected);
  bbp::RemoveOwnedRunRootCleanupReceipt(*tombstone);
  BOOST_TEST(!bbp::TryLoadOwnedRunRootCleanupReceipt(
      root.ownership().run_id, root.ownership().run_root));
}

BOOST_AUTO_TEST_CASE(
    runtime_owned_run_cleanup_receipt_names_do_not_alias_other_run_ids) {
  ManifestTestRoot first("receipt-a");
  ManifestTestRoot second("retired-receipt-a");
  const auto identity = [](const std::filesystem::path& path) {
    struct stat status{};
    if (stat(path.c_str(), &status) != 0) {
      throw std::runtime_error("inspect cleanup receipt test root failed");
    }
    return bbp::OwnedRunRootIdentity{
        .device = static_cast<std::uintmax_t>(status.st_dev),
        .inode = static_cast<std::uintmax_t>(status.st_ino),
    };
  };
  bbp::WriteOwnedRunRootCleanupReceipt(first.ownership(),
                                       identity(first.path()));
  bbp::WriteOwnedRunRootCleanupReceipt(second.ownership(),
                                       identity(second.path()));

  const auto first_receipt = bbp::TryLoadOwnedRunRootCleanupReceipt(
      first.ownership().run_id, first.path());
  BOOST_REQUIRE(first_receipt);
  bbp::RemoveOwnedRunRootCleanupReceipt(*first_receipt);
  BOOST_TEST(!bbp::TryLoadOwnedRunRootCleanupReceipt(first.ownership().run_id,
                                                     first.path()));
  BOOST_REQUIRE(bbp::TryLoadOwnedRunRootCleanupReceipt(
      second.ownership().run_id, second.path()));
}

BOOST_AUTO_TEST_CASE(
    runtime_owned_run_cleanup_receipt_finishes_an_empty_markerless_quarantine) {
  ManifestTestRoot root("markerless-recovery");
  struct stat root_status{};
  BOOST_REQUIRE_EQUAL(stat(root.path().c_str(), &root_status), 0);
  const bbp::OwnedRunRootIdentity identity{
      .device = static_cast<std::uintmax_t>(root_status.st_dev),
      .inode = static_cast<std::uintmax_t>(root_status.st_ino),
  };
  bbp::WriteOwnedRunRootCleanupReceipt(root.ownership(), identity);
  const std::filesystem::path quarantine =
      bbp::OwnedRunRootCleanupQuarantinePath(root.ownership());
  std::filesystem::rename(root.path(), quarantine);
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(quarantine)) {
    std::filesystem::remove_all(entry.path());
  }

  bbp::RemoveOwnedRunRoot(root.ownership(), std::nullopt, {}, identity);
  BOOST_TEST(!std::filesystem::exists(root.path()));
  BOOST_TEST(!std::filesystem::exists(quarantine));
  const auto receipt = bbp::TryLoadOwnedRunRootCleanupReceipt(
      root.ownership().run_id, root.ownership().run_root);
  BOOST_REQUIRE(receipt);
  bbp::RemoveOwnedRunRootCleanupReceipt(*receipt);
}

BOOST_AUTO_TEST_CASE(
    editor_run_clean_resumes_an_exact_durable_quarantined_root) {
  using namespace std::chrono_literals;
  EditorCleanupTestRoot root("editor-recovery");
  struct stat root_status{};
  BOOST_REQUIRE_EQUAL(stat(root.run_root().c_str(), &root_status), 0);
  const bbp::OwnedRunRootIdentity identity{
      .device = static_cast<std::uintmax_t>(root_status.st_dev),
      .inode = static_cast<std::uintmax_t>(root_status.st_ino),
  };
  bbp::WriteOwnedRunRootCleanupReceipt(root.ownership(), identity);
  const std::filesystem::path quarantine =
      bbp::OwnedRunRootCleanupQuarantinePath(root.ownership());
  std::filesystem::rename(root.run_root(), quarantine);

  const bbp::McpRunCleanupResult result = bbp::CleanEditorRetainedRunForTest(
      root.benchmark_root(), root.ownership().run_id, 5s, true);

  BOOST_TEST(result.run_id == root.ownership().run_id);
  BOOST_TEST(result.verified_owned);
  BOOST_TEST(result.complete);
  BOOST_TEST(!std::filesystem::exists(root.run_root()));
  BOOST_TEST(!std::filesystem::exists(quarantine));
}

BOOST_AUTO_TEST_CASE(
    editor_run_clean_ignores_cancellation_after_artifact_removal_commit) {
  using namespace std::chrono_literals;
  EditorCleanupTestRoot root("editor-late-cancel");
  std::stop_source cancellation;
  ScopedRunCleanupRootRemovedHook hook([&] { cancellation.request_stop(); });

  const bbp::McpRunCleanupResult result = bbp::CleanEditorRetainedRunForTest(
      root.benchmark_root(), root.ownership().run_id, 5s, true,
      cancellation.get_token());

  BOOST_TEST(result.verified_owned);
  BOOST_TEST(result.complete);
  BOOST_TEST(!std::filesystem::exists(root.run_root()));
  BOOST_TEST(!std::filesystem::exists(
      bbp::OwnedRunRootCleanupQuarantinePath(root.ownership())));
}

BOOST_AUTO_TEST_CASE(
    editor_run_clean_ignores_deadline_after_artifact_removal_commit) {
  using namespace std::chrono_literals;
  EditorCleanupTestRoot root("editor-late-deadline");
  ScopedRunCleanupRootRemovedHook hook(
      [] { std::this_thread::sleep_for(1100ms); });

  const bbp::McpRunCleanupResult result = bbp::CleanEditorRetainedRunForTest(
      root.benchmark_root(), root.ownership().run_id, 1s, true);

  BOOST_TEST(result.verified_owned);
  BOOST_TEST(result.complete);
  BOOST_TEST(!std::filesystem::exists(root.run_root()));
  BOOST_TEST(!std::filesystem::exists(
      bbp::OwnedRunRootCleanupQuarantinePath(root.ownership())));
}

BOOST_AUTO_TEST_CASE(
    editor_run_clean_preserves_unsafe_root_types_as_unverified) {
  using namespace std::chrono_literals;
  const auto is_unverified = [](const bbp::McpOperationFailure& error) {
    return error.code() == "run_cleanup_unverified" && !error.retryable();
  };

  EditorCleanupTestRoot symlink_root("editor-unsafe-link");
  std::filesystem::remove_all(symlink_root.run_root());
  const std::filesystem::path symlink_target =
      symlink_root.benchmark_root() / "foreign-target";
  std::filesystem::create_directory(symlink_target);
  bbp::WriteText(symlink_target / "sentinel", "preserved\n");
  std::filesystem::create_directory_symlink(symlink_target,
                                            symlink_root.run_root());
  BOOST_CHECK_EXCEPTION(bbp::CleanEditorRetainedRunForTest(
                            symlink_root.benchmark_root(),
                            symlink_root.ownership().run_id, 5s, true),
                        bbp::McpOperationFailure, is_unverified);
  BOOST_TEST(std::filesystem::is_symlink(
      std::filesystem::symlink_status(symlink_root.run_root())));
  BOOST_TEST(bbp::ReadText(symlink_target / "sentinel") == "preserved\n");

  EditorCleanupTestRoot file_root("editor-unsafe-file");
  std::filesystem::remove_all(file_root.run_root());
  bbp::WriteText(file_root.run_root(), "foreign file\n");
  BOOST_CHECK_EXCEPTION(
      bbp::CleanEditorRetainedRunForTest(
          file_root.benchmark_root(), file_root.ownership().run_id, 5s, true),
      bbp::McpOperationFailure, is_unverified);
  BOOST_TEST(std::filesystem::is_regular_file(file_root.run_root()));
  BOOST_TEST(bbp::ReadText(file_root.run_root()) == "foreign file\n");
}

BOOST_AUTO_TEST_CASE(
    runtime_node_root_cleanup_preserves_marker_after_depth_bound_failure) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  const std::filesystem::path node_root = root.path() / "nodes" / entry.node_id;
  std::filesystem::path nested = node_root;
  for (std::size_t depth = 0U; depth < 66U; ++depth) {
    nested /= "d";
    std::filesystem::create_directory(nested);
  }

  BOOST_CHECK_THROW(bbp::RemoveRuntimeNodeRoot(root.ownership(), entry),
                    std::runtime_error);
  BOOST_TEST(std::filesystem::is_regular_file(node_root / ".bbp-node"));

  std::filesystem::remove_all(node_root / "d");
  bbp::RemoveRuntimeNodeRoot(root.ownership(), entry);
  BOOST_TEST(!bbp::RuntimeNodeRootEntryExists(root.ownership(), entry.node_id));
}

BOOST_AUTO_TEST_CASE(
    runtime_node_credential_cleanup_refuses_nodes_symlink_redirect) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  bbp::WriteText(root.path() / "nodes" / entry.node_id / ".bbp-rpc-cookie",
                 "owned\n");

  const std::filesystem::path outside =
      root.path().parent_path() /
      ("bbp-runtime-credential-outside-" + std::to_string(getpid()));
  std::filesystem::remove_all(outside);
  std::filesystem::create_directories(outside / entry.node_id);
  bbp::WriteText(outside / entry.node_id / ".bbp-rpc-cookie", "outside\n");
  const std::filesystem::path original_nodes = root.path() / "nodes-owned";
  std::filesystem::rename(root.path() / "nodes", original_nodes);
  std::filesystem::create_directory_symlink(outside, root.path() / "nodes");

  BOOST_CHECK_THROW(
      bbp::CleanupRuntimeNodeRpcCredential(root.ownership(), entry),
      std::runtime_error);
  BOOST_TEST(
      std::filesystem::exists(outside / entry.node_id / ".bbp-rpc-cookie"));

  std::filesystem::remove(root.path() / "nodes");
  std::filesystem::rename(original_nodes, root.path() / "nodes");
  bbp::CleanupRuntimeNodeRpcCredential(root.ownership(), entry);
  BOOST_TEST(!std::filesystem::exists(root.path() / "nodes" / entry.node_id /
                                      ".bbp-rpc-cookie"));
  bbp::RemoveRuntimeNodeRoot(root.ownership(), entry);
  std::filesystem::remove_all(outside);
}

#ifdef __linux__
BOOST_AUTO_TEST_CASE(
    runtime_node_root_cleanup_refuses_bind_mounted_descendant_when_available) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  const std::filesystem::path mounted =
      root.path() / "nodes" / entry.node_id / "mounted";
  const std::filesystem::path outside =
      root.path().parent_path() /
      ("bbp-runtime-mount-outside-" + std::to_string(getpid()));
  std::filesystem::remove_all(outside);
  std::filesystem::create_directories(mounted);
  std::filesystem::create_directories(outside);
  bbp::WriteText(outside / "sentinel", "outside\n");

  if (mount(outside.c_str(), mounted.c_str(), nullptr, MS_BIND, nullptr) == 0) {
    BOOST_CHECK_THROW(bbp::RemoveRuntimeNodeRoot(root.ownership(), entry),
                      std::runtime_error);
    BOOST_TEST(std::filesystem::exists(outside / "sentinel"));
    BOOST_TEST(std::filesystem::exists(root.path() / "nodes" / entry.node_id /
                                       ".bbp-node"));
    BOOST_REQUIRE_EQUAL(umount2(mounted.c_str(), MNT_DETACH), 0);
    std::filesystem::remove(mounted);
  } else {
    const bool unavailable = errno == EPERM || errno == EACCES;
    BOOST_TEST(unavailable);
  }
  bbp::RemoveRuntimeNodeRoot(root.ownership(), entry);
  std::filesystem::remove_all(outside);
}

BOOST_AUTO_TEST_CASE(
    runtime_owned_run_cleanup_preserves_marker_across_bind_mount_refusal) {
  ManifestTestRoot root;
  const std::filesystem::path mounted = root.path() / "mounted";
  const std::filesystem::path outside =
      root.path().parent_path() /
      ("bbp-runtime-run-mount-outside-" + std::to_string(getpid()));
  std::filesystem::remove_all(outside);
  std::filesystem::create_directories(mounted);
  std::filesystem::create_directories(outside);
  bbp::WriteText(outside / "sentinel", "outside\n");

  if (mount(outside.c_str(), mounted.c_str(), nullptr, MS_BIND, nullptr) == 0) {
    BOOST_CHECK_THROW(bbp::RemoveOwnedRunRoot(root.ownership()),
                      std::runtime_error);
    BOOST_TEST(std::filesystem::exists(root.path() /
                                       std::string(bbp::kRunMarkerFile)));
    BOOST_TEST(std::filesystem::exists(outside / "sentinel"));
    BOOST_REQUIRE_EQUAL(umount2(mounted.c_str(), MNT_DETACH), 0);
    std::filesystem::remove(mounted);
  } else {
    const bool unavailable = errno == EPERM || errno == EACCES;
    BOOST_TEST(unavailable);
  }
  std::filesystem::remove_all(outside);
}
#endif

BOOST_AUTO_TEST_CASE(
    runtime_node_manifest_rejects_unreconstructable_slots_and_paths) {
  ManifestTestRoot root;
  bbp::RuntimeNodeResourceManifest manifest{
      .ownership = root.ownership(),
      .isolated_network = false,
      .nodes = {Entry("firo-1", 16U)},
  };
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);

  manifest.nodes.front().slot = 0U;
  manifest.nodes.front().data_dir = "nodes/firo-1";
  BOOST_CHECK_THROW(bbp::WriteRuntimeNodeResourceManifest(manifest),
                    std::runtime_error);
}
