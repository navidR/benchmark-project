#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <limits>
#include <stdexcept>

#include "bbp/simulator/process_control_config.h"

namespace {

boost::json::object ParseObject(const char* text) {
  return boost::json::parse(text).as_object();
}

}  // namespace

BOOST_AUTO_TEST_CASE(process_control_config_round_trips_canonical_json) {
  const boost::json::object input = ParseObject(
      R"({"runtime_node_restarts":[{"node":2},{"node":1},{"node":2}],"runtime_node_freezes":[{"node":3,"duration_ms":25},{"node":1,"duration_ms":100}]})");

  const bbp::ProcessControlConfig config =
      bbp::ParseProcessControlConfig(input, 3U);
  BOOST_REQUIRE_EQUAL(config.restart_node_indexes.size(), 3U);
  BOOST_TEST(config.restart_node_indexes[0] == 1U);
  BOOST_TEST(config.restart_node_indexes[1] == 0U);
  BOOST_TEST(config.restart_node_indexes[2] == 1U);
  BOOST_REQUIRE_EQUAL(config.freezes.size(), 2U);
  BOOST_TEST(config.freezes[0].node_index == 2U);
  BOOST_TEST(config.freezes[0].duration_ms == 25U);
  BOOST_TEST(config.freezes[1].node_index == 0U);
  BOOST_TEST(config.freezes[1].duration_ms == 100U);

  const boost::json::object rendered = bbp::ProcessControlConfigJson(config);
  BOOST_TEST(rendered == input);
  const bbp::ProcessControlConfig reparsed =
      bbp::ParseProcessControlConfig(rendered, 3U);
  BOOST_TEST(reparsed.restart_node_indexes == config.restart_node_indexes,
             boost::test_tools::per_element());
  BOOST_REQUIRE_EQUAL(reparsed.freezes.size(), config.freezes.size());
  for (std::size_t index = 0; index < config.freezes.size(); ++index) {
    BOOST_TEST(reparsed.freezes[index].node_index ==
               config.freezes[index].node_index);
    BOOST_TEST(reparsed.freezes[index].duration_ms ==
               config.freezes[index].duration_ms);
  }
}

BOOST_AUTO_TEST_CASE(process_control_config_omits_absent_controls) {
  const bbp::ProcessControlConfig config =
      bbp::ParseProcessControlConfig({}, 1U);
  BOOST_TEST(config.restart_node_indexes.empty());
  BOOST_TEST(config.freezes.empty());
  BOOST_TEST(bbp::ProcessControlConfigJson(config).empty());
}

BOOST_AUTO_TEST_CASE(process_control_config_rejects_malformed_input) {
  BOOST_CHECK_THROW(bbp::ParseProcessControlConfig({}, 0U), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseProcessControlConfig(
                        ParseObject(R"({"runtime_node_restarts":true})"), 2U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseProcessControlConfig(
                        ParseObject(R"({"runtime_node_restarts":[1]})"), 2U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ParseProcessControlConfig(
          ParseObject(R"({"runtime_node_restarts":[{"node":0}]})"), 2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ParseProcessControlConfig(
          ParseObject(R"({"runtime_node_restarts":[{"node":3}]})"), 2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ParseProcessControlConfig(
          ParseObject(R"({"runtime_node_restarts":[{"node":4294967296}]})"),
          2U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseProcessControlConfig(
                        ParseObject(R"({"runtime_node_freezes":{}})"), 2U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ParseProcessControlConfig(
          ParseObject(R"({"runtime_node_freezes":[{"node":1}]})"), 2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ParseProcessControlConfig(
          ParseObject(
              R"({"runtime_node_freezes":[{"node":1,"duration_ms":0}]})"),
          2U),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(process_control_config_rejects_unrenderable_state) {
  BOOST_CHECK_THROW(
      bbp::ProcessControlConfigJson(bbp::ProcessControlConfig{
          .restart_node_indexes = {std::numeric_limits<std::uint32_t>::max()},
          .freezes = {},
      }),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::ProcessControlConfigJson(bbp::ProcessControlConfig{
                        .restart_node_indexes = {},
                        .freezes = {{.node_index = 0U, .duration_ms = 0U}},
                    }),
                    std::runtime_error);
}
