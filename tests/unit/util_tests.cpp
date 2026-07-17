#define BOOST_TEST_MODULE BlockchainBenchmarkProjectUtilTests

#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>
#include <vector>

#include "bbp/util.h"

BOOST_AUTO_TEST_CASE(json_escape_uses_boost_json) {
  BOOST_TEST(bbp::JsonEscape("a\"b\\c\n") == "a\\\"b\\\\c\\n");
}

BOOST_AUTO_TEST_CASE(json_field_helpers_read_boost_values) {
  boost::json::object object;
  object["blocks"] = 7;
  object["bestblockhash"] = "abc";

  BOOST_TEST(bbp::JsonUint(object, "blocks") == 7U);
  BOOST_TEST(bbp::JsonString(object, "bestblockhash") == "abc");
}

BOOST_AUTO_TEST_CASE(split_whitespace_reads_cgroup_controller_lists) {
  const std::vector<std::string> words =
      bbp::SplitWhitespace(" cpu memory  pids ");

  BOOST_REQUIRE_EQUAL(words.size(), 3U);
  BOOST_TEST(words[0] == "cpu");
  BOOST_TEST(words[2] == "pids");
}

BOOST_AUTO_TEST_CASE(require_executable_rejects_directories) {
  BOOST_CHECK_THROW(
      bbp::RequireExecutable(std::filesystem::temp_directory_path()),
      std::runtime_error);
}
