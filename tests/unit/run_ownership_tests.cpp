#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>

#include "bbp/run_ownership.h"
#include "bbp/simulator/constants.h"
#include "bbp/util.h"

namespace {

std::filesystem::path TestRoot(std::string_view name) {
  return std::filesystem::temp_directory_path() /
         ("bbp-run-ownership-" + std::string(name) + "-" +
          std::to_string(getpid()));
}

class ScopedDirectory {
 public:
  explicit ScopedDirectory(std::filesystem::path path)
      : path_(std::move(path)) {
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }
  ~ScopedDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

}  // namespace

BOOST_AUTO_TEST_CASE(run_ownership_separates_identical_ids_in_distinct_roots) {
  ScopedDirectory first(TestRoot("same-id-a"));
  ScopedDirectory second(TestRoot("same-id-b"));
  const bbp::RunOwnership first_owner =
      bbp::CreateRunOwnership("same-id", first.path());
  const bbp::RunOwnership second_owner =
      bbp::CreateRunOwnership("same-id", second.path());
  bbp::WriteRunOwnershipMarker(first_owner);
  bbp::WriteRunOwnershipMarker(second_owner);

  BOOST_TEST(first_owner.run_id == second_owner.run_id);
  BOOST_TEST(first_owner.run_root != second_owner.run_root);
  BOOST_TEST(first_owner.resource_id != second_owner.resource_id);
  BOOST_TEST(first_owner.cgroup_name != second_owner.cgroup_name);
  BOOST_TEST(bbp::RunInterfaceAlias(first_owner, 0U, 'h') !=
             bbp::RunInterfaceAlias(second_owner, 0U, 'h'));
  BOOST_CHECK(bbp::LoadRunOwnership("same-id", first.path()) == first_owner);
  BOOST_CHECK(bbp::LoadRunOwnership("same-id", second.path()) == second_owner);
}

BOOST_AUTO_TEST_CASE(run_ownership_rejects_a_marker_copied_to_another_root) {
  ScopedDirectory first(TestRoot("copy-source"));
  ScopedDirectory second(TestRoot("copy-target"));
  const bbp::RunOwnership owner =
      bbp::CreateRunOwnership("copy-id", first.path());
  bbp::WriteRunOwnershipMarker(owner);
  bbp::WriteText(second.path() / bbp::kRunMarkerFile,
                 bbp::ReadText(first.path() / bbp::kRunMarkerFile));

  BOOST_CHECK_EXCEPTION(
      bbp::LoadRunOwnership("copy-id", second.path()), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find("root does not match") !=
               std::string::npos;
      });
}

BOOST_AUTO_TEST_CASE(run_ownership_rejects_legacy_or_malformed_markers) {
  ScopedDirectory root(TestRoot("invalid"));
  bbp::WriteText(root.path() / bbp::kRunMarkerFile, "bbp run\n");
  BOOST_CHECK_THROW(bbp::LoadRunOwnership("invalid-id", root.path()),
                    std::runtime_error);

  bbp::WriteText(
      root.path() / bbp::kRunMarkerFile,
      R"({"version":1,"run_id":"invalid-id","run_root":"/","resource_id":"00000000000000000000000000000000","extra":true})");
  BOOST_CHECK_THROW(bbp::LoadRunOwnership("invalid-id", root.path()),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(run_interface_names_are_bound_to_the_resource_identity) {
  ScopedDirectory first(TestRoot("interfaces-a"));
  ScopedDirectory second(TestRoot("interfaces-b"));
  const std::string first_resource = "eac91500aaaaaaaaaaaaaaaaaaaaaaaa";
  const std::string second_resource = "eac91500bbbbbbbbbbbbbbbbbbbbbbbb";
  bbp::WriteText(first.path() / bbp::kRunMarkerFile,
                 "{\"version\":1,\"run_id\":\"collision-467\",\"run_root\":\"" +
                     first.path().string() + "\",\"resource_id\":\"" +
                     first_resource + "\"}\n");
  bbp::WriteText(
      second.path() / bbp::kRunMarkerFile,
      "{\"version\":1,\"run_id\":\"collision-33890\",\"run_root\":\"" +
          second.path().string() + "\",\"resource_id\":\"" + second_resource +
          "\"}\n");
  const bbp::RunOwnership first_owner =
      bbp::LoadRunOwnership("collision-467", first.path());
  const bbp::RunOwnership second_owner =
      bbp::LoadRunOwnership("collision-33890", second.path());

  BOOST_TEST(bbp::RunInterfaceName(first_owner, 15U, 'h').size() == 15U);
  BOOST_TEST(bbp::RunInterfaceName(first_owner, 0U, 'h') ==
             bbp::RunInterfaceName(second_owner, 0U, 'h'));
  BOOST_TEST(bbp::RunInterfaceAlias(first_owner, 0U, 'h') !=
             bbp::RunInterfaceAlias(second_owner, 0U, 'h'));
  BOOST_CHECK_THROW(bbp::RunInterfaceName(first_owner, 16U, 'h'),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::RunInterfaceName(first_owner, 0U, 'x'),
                    std::runtime_error);
}
