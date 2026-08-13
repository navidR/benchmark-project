#include "simulator_yaml_decoding.h"

#include <yaml.h>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/string.hpp>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/simulator/yaml_helpers.h"

namespace bbp::simulator_app_internal {
namespace {

std::string YamlScalarText(const yaml_event_t& event) {
  return std::string(reinterpret_cast<const char*>(event.data.scalar.value),
                     event.data.scalar.length);
}

std::string LowerAscii(std::string_view text) {
  std::string lower;
  lower.reserve(text.size());
  for (const char c : text) {
    if (c >= 'A' && c <= 'Z') {
      lower.push_back(static_cast<char>(c - 'A' + 'a'));
    } else {
      lower.push_back(c);
    }
  }
  return lower;
}

bool IsDecimalInteger(std::string_view text) {
  if (text.empty()) {
    return false;
  }
  size_t offset = 0;
  if (text.front() == '-') {
    if (text.size() == 1U) {
      return false;
    }
    offset = 1U;
  }
  for (size_t i = offset; i < text.size(); ++i) {
    if (text[i] < '0' || text[i] > '9') {
      return false;
    }
  }
  return true;
}

boost::json::value ParseYamlPlainScalar(std::string_view text) {
  const std::string lower = LowerAscii(text);
  if (text.empty() || text == "~" || lower == "null") {
    return nullptr;
  }
  if (lower == "true") {
    return true;
  }
  if (lower == "false") {
    return false;
  }
  if (!IsDecimalInteger(text)) {
    double value = 0.0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(),
                                        value, std::chars_format::general);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size() &&
        std::isfinite(value)) {
      return value;
    }
    return boost::json::string(text);
  }
  if (text.front() == '-') {
    int64_t value = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size()) {
      return value;
    }
  } else {
    uint64_t value = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size()) {
      return value;
    }
  }
  return boost::json::string(text);
}

boost::json::value ParseYamlValue(YamlParser* parser, YamlEvent event);

boost::json::array ParseYamlSequence(YamlParser* parser) {
  boost::json::array array;
  while (true) {
    YamlEvent event = parser->Next();
    if (event.Type() == YAML_SEQUENCE_END_EVENT) {
      break;
    }
    array.push_back(ParseYamlValue(parser, std::move(event)));
  }
  return array;
}

boost::json::object ParseYamlMapping(YamlParser* parser) {
  boost::json::object object;
  while (true) {
    YamlEvent key_event = parser->Next();
    if (key_event.Type() == YAML_MAPPING_END_EVENT) {
      break;
    }
    if (key_event.Type() != YAML_SCALAR_EVENT) {
      throw std::runtime_error("YAML mapping keys must be scalars");
    }
    const std::string key = YamlScalarText(key_event.Raw());
    if (object.if_contains(key) != nullptr) {
      throw std::runtime_error("duplicate YAML mapping key: " + key);
    }
    object[key] = ParseYamlValue(parser, parser->Next());
  }
  return object;
}

boost::json::value ParseYamlValue(YamlParser* parser, YamlEvent event) {
  if (event.Type() == YAML_SCALAR_EVENT) {
    const std::string text = YamlScalarText(event.Raw());
    if (event.Raw().data.scalar.style != YAML_PLAIN_SCALAR_STYLE) {
      return boost::json::string(text);
    }
    return ParseYamlPlainScalar(text);
  }
  if (event.Type() == YAML_SEQUENCE_START_EVENT) {
    return ParseYamlSequence(parser);
  }
  if (event.Type() == YAML_MAPPING_START_EVENT) {
    return ParseYamlMapping(parser);
  }
  if (event.Type() == YAML_ALIAS_EVENT) {
    throw std::runtime_error("YAML aliases are not supported");
  }
  throw std::runtime_error("unexpected YAML event while parsing scenario");
}

void RequireYamlEvent(const YamlEvent& event, yaml_event_type_t expected,
                      std::string_view message) {
  if (event.Type() != expected) {
    throw std::runtime_error(std::string(message));
  }
}

}  // namespace

boost::json::value ParseYamlDocument(std::string input,
                                     const std::filesystem::path& source) {
  YamlParser parser(std::move(input), source.string());
  RequireYamlEvent(parser.Next(), YAML_STREAM_START_EVENT,
                   "YAML stream did not start");
  YamlEvent document_start = parser.Next();
  if (document_start.Type() == YAML_STREAM_END_EVENT) {
    return nullptr;
  }
  RequireYamlEvent(document_start, YAML_DOCUMENT_START_EVENT,
                   "YAML document did not start");

  boost::json::value root;
  YamlEvent root_event = parser.Next();
  if (root_event.Type() == YAML_DOCUMENT_END_EVENT) {
    root = nullptr;
  } else {
    root = ParseYamlValue(&parser, std::move(root_event));
    RequireYamlEvent(parser.Next(), YAML_DOCUMENT_END_EVENT,
                     "YAML document did not end");
  }
  RequireYamlEvent(parser.Next(), YAML_STREAM_END_EVENT,
                   "YAML scenario must contain exactly one document");
  return root;
}

}  // namespace bbp::simulator_app_internal
