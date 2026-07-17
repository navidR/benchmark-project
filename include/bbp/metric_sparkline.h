#pragma once

#include <boost/json/array.hpp>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace bbp {

struct MetricSparkline {
  std::string text;
  std::size_t valid_samples = 0U;
  std::optional<double> minimum;
  std::optional<double> maximum;
  std::optional<double> latest;
};

MetricSparkline BuildMetricSparkline(const boost::json::array& history,
                                     std::string_view field,
                                     std::size_t maximum_columns);

}  // namespace bbp
