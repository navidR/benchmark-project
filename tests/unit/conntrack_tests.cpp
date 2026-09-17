#include <stdlib.h>

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "bbp/conntrack.h"
#include "bbp/util.h"

namespace {

struct ConntrackFixture {
  ConntrackFixture() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "bbp-conntrack-XXXXXX")
            .string();
    const char* created = mkdtemp(pattern.data());
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed for conntrack test");
    }
    directory = created;
    limit = directory / "nf_conntrack_max";
  }
  ~ConntrackFixture() { std::filesystem::remove_all(directory); }

  std::filesystem::path directory;
  std::filesystem::path limit;
};

}  // namespace

BOOST_FIXTURE_TEST_CASE(conntrack_raises_lower_limits_and_is_idempotent,
                        ConntrackFixture) {
  for (const auto* before : {"1\n", "262144\n", "524288\n", "1048575\n"}) {
    bbp::WriteText(limit, before);
    BOOST_TEST(bbp::EnsureConntrackCapacity(limit) == 1'048'576U);
    BOOST_TEST(bbp::ReadText(limit) == "1048576\n");
    BOOST_TEST(bbp::EnsureConntrackCapacity(limit) == 1'048'576U);
  }
}

BOOST_FIXTURE_TEST_CASE(conntrack_preserves_sufficient_limits_without_writing,
                        ConntrackFixture) {
  for (const auto* before : {"1048576\n", "2097152\n", "4294967295\n"}) {
    bbp::WriteText(limit, before);
    const auto timestamp =
        std::filesystem::file_time_type::clock::now() - std::chrono::hours(1);
    std::filesystem::last_write_time(limit, timestamp);
    BOOST_TEST(bbp::EnsureConntrackCapacity(limit) == std::stoul(before));
    BOOST_TEST(bbp::ReadText(limit) == before);
    BOOST_CHECK(std::filesystem::last_write_time(limit) == timestamp);
  }
}

BOOST_FIXTURE_TEST_CASE(conntrack_rejects_invalid_limits_without_writing,
                        ConntrackFixture) {
  for (const auto* before : {"", "0\n", "-1\n", "invalid\n", "262144x\n",
                             "262144 1\n", "4294967296\n"}) {
    bbp::WriteText(limit, before);
    BOOST_CHECK_THROW(bbp::EnsureConntrackCapacity(limit), std::runtime_error);
    BOOST_TEST(bbp::ReadText(limit) == before);
  }
}

BOOST_FIXTURE_TEST_CASE(conntrack_does_not_create_a_missing_sysctl,
                        ConntrackFixture) {
  BOOST_CHECK_THROW(bbp::EnsureConntrackCapacity(limit), std::runtime_error);
  BOOST_TEST(!std::filesystem::exists(limit));
}

BOOST_AUTO_TEST_CASE(conntrack_reports_unwritable_sysctl) {
  // Linux exposes NGROUPS_MAX (65536) as a sysctl that even root cannot write.
  const std::filesystem::path path = "/proc/sys/kernel/ngroups_max";
  const std::string before = bbp::ReadText(path);
  BOOST_REQUIRE(std::stoul(before) < bbp::kMinimumConntrackCapacity);
  BOOST_CHECK_THROW(bbp::EnsureConntrackCapacity(path), std::runtime_error);
  BOOST_TEST(bbp::ReadText(path) == before);
}
