#pragma once

#include <algorithm>
#include <boost/json.hpp>
#include <cctype>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace bbp::block_decoding {
inline std::optional<std::uint64_t> Uint(const boost::json::object& o,
                                         std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v || v->is_null()) return {};
  if (v->is_uint64()) return v->as_uint64();
  if (v->is_int64() && v->as_int64() >= 0)
    return static_cast<std::uint64_t>(v->as_int64());
  throw std::runtime_error("invalid block integer: " + std::string(key));
}
inline std::optional<std::int64_t> Signed(const boost::json::object& o,
                                          std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v || v->is_null()) return {};
  if (v->is_int64()) return v->as_int64();
  if (v->is_uint64() && v->as_uint64() <= static_cast<std::uint64_t>(INT64_MAX))
    return static_cast<std::int64_t>(v->as_uint64());
  throw std::runtime_error("invalid signed block integer: " + std::string(key));
}
inline std::optional<std::string> Text(const boost::json::object& o,
                                       std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v || v->is_null()) return {};
  if (!v->is_string())
    throw std::runtime_error("invalid block text: " + std::string(key));
  return std::string(v->as_string());
}
inline std::uint64_t HexBytes(std::string_view hex) {
  if (hex.size() % 2 ||
      !std::all_of(hex.begin(), hex.end(),
                   [](unsigned char ch) { return std::isxdigit(ch) != 0; }))
    throw std::runtime_error("invalid serialized block/transaction hex");
  return hex.size() / 2;
}
inline std::string Hash(const boost::json::object& o, std::string_view key) {
  const auto text = Text(o, key);
  if (!text || HexBytes(*text) != 32)
    throw std::runtime_error("missing or invalid block hash: " +
                             std::string(key));
  return *text;
}
inline std::uint64_t Height(const boost::json::object& o) {
  const auto height = Uint(o, "height");
  if (!height) throw std::runtime_error("missing block height");
  return *height;
}
inline std::optional<std::uint64_t> Count(const boost::json::object& o,
                                          std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v) return {};
  if (!v->is_array())
    throw std::runtime_error("invalid block array: " + std::string(key));
  return v->as_array().size();
}
}  // namespace bbp::block_decoding
