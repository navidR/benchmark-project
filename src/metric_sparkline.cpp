#include "bbp/metric_sparkline.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

namespace bbp {
namespace {

std::optional<double> JsonNumber(const boost::json::value& value) {
  double number = 0.0;
  if (value.is_double()) {
    number = value.as_double();
  } else if (value.is_uint64()) {
    number = static_cast<double>(value.as_uint64());
  } else if (value.is_int64()) {
    number = static_cast<double>(value.as_int64());
  } else {
    return std::nullopt;
  }
  return std::isfinite(number) ? std::optional<double>(number) : std::nullopt;
}

std::optional<double> SampleNumber(const boost::json::value& sample,
                                   std::string_view field) {
  if (!sample.is_object()) {
    return std::nullopt;
  }
  const boost::json::value* value = sample.as_object().if_contains(field);
  return value == nullptr || value->is_null() ? std::nullopt
                                              : JsonNumber(*value);
}

}  // namespace

MetricSparkline BuildMetricSparkline(const boost::json::array& history,
                                     std::string_view field,
                                     std::size_t maximum_columns) {
  MetricSparkline result;
  if (maximum_columns == 0U || history.empty()) {
    return result;
  }

  const std::size_t first =
      history.size() > maximum_columns ? history.size() - maximum_columns : 0U;
  std::vector<std::optional<double>> values;
  values.reserve(history.size() - first);
  for (std::size_t index = first; index < history.size(); ++index) {
    const std::optional<double> value = SampleNumber(history[index], field);
    values.push_back(value);
    if (!value) {
      continue;
    }
    ++result.valid_samples;
    result.latest = value;
    result.minimum = result.minimum ? std::min(*result.minimum, *value) : value;
    result.maximum = result.maximum ? std::max(*result.maximum, *value) : value;
  }

  result.text.assign(values.size(), ' ');
  if (!result.minimum || !result.maximum) {
    return result;
  }

  constexpr std::string_view kLevels = ".:-=+*#@";
  const double range = *result.maximum - *result.minimum;
  for (std::size_t index = 0U; index < values.size(); ++index) {
    if (!values[index]) {
      continue;
    }
    std::size_t level = kLevels.size() / 2U;
    if (range > std::numeric_limits<double>::epsilon()) {
      const double normalized = (*values[index] - *result.minimum) / range;
      const double scaled =
          normalized * static_cast<double>(kLevels.size() - 1U);
      level = static_cast<std::size_t>(std::llround(scaled));
      level = std::min(level, kLevels.size() - 1U);
    }
    result.text[index] = kLevels[level];
  }
  return result;
}

}  // namespace bbp
