#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <filesystem>
#include <string>

#ifdef __linux__
#include <sys/mount.h>
#endif

#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulator/constants.h"
#include "bbp/util.h"

namespace {

class ManifestTestRoot {
 public:
  ManifestTestRoot()
      : path_(std::filesystem::temp_directory_path() /
              ("bbp-runtime-manifest-" + std::to_string(getpid()))) {
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_ / "nodes");
    ownership_ = bbp::CreateRunOwnership("manifest-test", path_);
    bbp::WriteRunOwnershipMarker(ownership_);
  }

  ~ManifestTestRoot() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::filesystem::path& path() const { return path_; }
  const bbp::RunOwnership& ownership() const { return ownership_; }

 private:
  std::filesystem::path path_;
  bbp::RunOwnership ownership_;
};

bbp::RuntimeNodeResourceEntry Entry(
    std::string id, std::uint32_t slot,
    bbp::RuntimeNodeResourceState state =
        bbp::RuntimeNodeResourceState::kLive) {
  return bbp::RuntimeNodeResourceEntry{
      .node_id = std::move(id),
      .slot = slot,
      .chain = bbp::ChainKind::kFiro,
      .data_dir = std::filesystem::path("nodes") /
                  ("firo-" + std::to_string(slot + 1U)) / "data",
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
  const auto empty =
      bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
  BOOST_REQUIRE(empty);
  BOOST_TEST(empty->nodes.empty());
  BOOST_TEST(empty->isolated_network);

  manifest.nodes = {
      Entry("firo-1", 0U),
      Entry("firo-2", 1U, bbp::RuntimeNodeResourceState::kPendingAdd),
  };
  bbp::WriteRuntimeNodeResourceManifest(manifest);
  const auto loaded =
      bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
  BOOST_REQUIRE(loaded);
  BOOST_CHECK(*loaded == manifest);
}

BOOST_AUTO_TEST_CASE(
    runtime_node_root_cleanup_is_descriptor_anchored_and_preserves_symlink_targets) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  BOOST_TEST(
      bbp::RuntimeNodeRootEntryExists(root.ownership(), entry.node_id));

  const std::filesystem::path outside = root.path().parent_path() /
                                        ("bbp-runtime-outside-" +
                                         std::to_string(getpid()));
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

  BOOST_CHECK_THROW(
      bbp::RemoveRuntimeNodeRoot(root.ownership(), entry),
      std::runtime_error);
  BOOST_TEST(std::filesystem::is_directory(
      root.path() / "nodes" / entry.node_id));
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
  const std::filesystem::path collision =
      root.path() / "nodes" / entry.node_id;
  std::filesystem::create_directory(collision);
  bbp::WriteText(collision / "sentinel", "foreign\n");
  bool acquired = true;
  BOOST_CHECK_THROW(
      bbp::PrepareRuntimeNodeRoot(root.ownership(), entry, &acquired),
      std::runtime_error);
  BOOST_TEST(!acquired);
  BOOST_TEST(bbp::ReadText(collision / "sentinel") == "foreign\n");
  BOOST_TEST(bbp::ReadText(manifest_path) == manifest_before);
  const auto loaded =
      bbp::TryLoadRuntimeNodeResourceManifest(root.ownership());
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
    runtime_node_root_cleanup_preserves_marker_after_depth_bound_failure) {
  ManifestTestRoot root;
  const bbp::RuntimeNodeResourceEntry entry = Entry("firo-1", 0U);
  bbp::PrepareRuntimeNodeRoot(root.ownership(), entry);
  const std::filesystem::path node_root =
      root.path() / "nodes" / entry.node_id;
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
  bbp::WriteText(root.path() / "nodes" / entry.node_id /
                     ".bbp-rpc-cookie",
                 "owned\n");

  const std::filesystem::path outside =
      root.path().parent_path() /
      ("bbp-runtime-credential-outside-" + std::to_string(getpid()));
  std::filesystem::remove_all(outside);
  std::filesystem::create_directories(outside / entry.node_id);
  bbp::WriteText(outside / entry.node_id / ".bbp-rpc-cookie",
                 "outside\n");
  const std::filesystem::path original_nodes = root.path() / "nodes-owned";
  std::filesystem::rename(root.path() / "nodes", original_nodes);
  std::filesystem::create_directory_symlink(outside, root.path() / "nodes");

  BOOST_CHECK_THROW(
      bbp::CleanupRuntimeNodeRpcCredential(root.ownership(), entry),
      std::runtime_error);
  BOOST_TEST(std::filesystem::exists(
      outside / entry.node_id / ".bbp-rpc-cookie"));

  std::filesystem::remove(root.path() / "nodes");
  std::filesystem::rename(original_nodes, root.path() / "nodes");
  bbp::CleanupRuntimeNodeRpcCredential(root.ownership(), entry);
  BOOST_TEST(!std::filesystem::exists(
      root.path() / "nodes" / entry.node_id / ".bbp-rpc-cookie"));
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
    BOOST_TEST(std::filesystem::exists(
        root.path() / "nodes" / entry.node_id / ".bbp-node"));
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
    BOOST_TEST(std::filesystem::exists(
        root.path() / std::string(bbp::kRunMarkerFile)));
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
