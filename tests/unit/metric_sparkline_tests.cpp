#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>

#include "bbp/metric_sparkline.h"

BOOST_AUTO_TEST_CASE(metric_sparkline_scales_numeric_history_and_gaps) {
  boost::json::array history;
  history.emplace_back(boost::json::object{{"value", 0}});
  history.emplace_back(boost::json::object{});
  history.emplace_back(boost::json::object{{"value", 1.0}});
  history.emplace_back(boost::json::object{{"value", 2U}});

  const bbp::MetricSparkline chart =
      bbp::BuildMetricSparkline(history, "value", 4U);

  BOOST_TEST(chart.text == ". +@");
  BOOST_TEST(chart.valid_samples == 3U);
  BOOST_REQUIRE(chart.minimum);
  BOOST_REQUIRE(chart.maximum);
  BOOST_REQUIRE(chart.latest);
  BOOST_TEST(*chart.minimum == 0.0);
  BOOST_TEST(*chart.maximum == 2.0);
  BOOST_TEST(*chart.latest == 2.0);
}

BOOST_AUTO_TEST_CASE(metric_sparkline_bounds_history_and_constant_values) {
  boost::json::array history;
  history.emplace_back(boost::json::object{{"value", 1}});
  history.emplace_back(boost::json::object{{"value", 5}});
  history.emplace_back(boost::json::object{{"value", 5}});

  const bbp::MetricSparkline chart =
      bbp::BuildMetricSparkline(history, "value", 2U);

  BOOST_TEST(chart.text == "++");
  BOOST_TEST(chart.valid_samples == 2U);
  BOOST_REQUIRE(chart.minimum);
  BOOST_REQUIRE(chart.maximum);
  BOOST_REQUIRE(chart.latest);
  BOOST_TEST(*chart.minimum == 5.0);
  BOOST_TEST(*chart.maximum == 5.0);
  BOOST_TEST(*chart.latest == 5.0);
}

BOOST_AUTO_TEST_CASE(metric_sparkline_handles_empty_and_nonnumeric_history) {
  const boost::json::array history = {
      boost::json::object{{"value", "not-a-number"}},
      nullptr,
  };
  const bbp::MetricSparkline missing =
      bbp::BuildMetricSparkline(history, "value", 8U);
  BOOST_TEST(missing.text == "  ");
  BOOST_TEST(missing.valid_samples == 0U);
  BOOST_TEST(!missing.minimum);
  BOOST_TEST(!missing.maximum);
  BOOST_TEST(!missing.latest);

  const bbp::MetricSparkline empty =
      bbp::BuildMetricSparkline(history, "value", 0U);
  BOOST_TEST(empty.text.empty());
}
