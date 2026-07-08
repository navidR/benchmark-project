#define BOOST_TEST_MODULE BenchmarkSimUtilTests

#include "benchmark_sim/util.h"

#include <string>
#include <vector>

#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(json_escape_uses_boost_json) {
  BOOST_TEST(bsim::JsonEscape("a\"b\\c\n") == "a\\\"b\\\\c\\n");
}

BOOST_AUTO_TEST_CASE(json_field_helpers_read_boost_values) {
  boost::json::object object;
  object["blocks"] = 7;
  object["bestblockhash"] = "abc";

  BOOST_TEST(bsim::JsonUint(object, "blocks") == 7U);
  BOOST_TEST(bsim::JsonString(object, "bestblockhash") == "abc");
}

BOOST_AUTO_TEST_CASE(split_whitespace_reads_cgroup_controller_lists) {
  const std::vector<std::string> words =
      bsim::SplitWhitespace(" cpu memory  pids ");

  BOOST_REQUIRE_EQUAL(words.size(), 3U);
  BOOST_TEST(words[0] == "cpu");
  BOOST_TEST(words[2] == "pids");
}
