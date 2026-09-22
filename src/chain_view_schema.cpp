#include <boost/json.hpp>

#include "bbp/chain_view.h"

namespace bbp {
namespace {
boost::json::object Type(std::string_view name, bool nullable = false) {
  return {{"type", nullable ? boost::json::value(boost::json::array{
                                  boost::json::value(name), "null"})
                            : boost::json::value(name)}};
}
boost::json::object Object(boost::json::object properties,
                           boost::json::array required = {}) {
  return {{"type", "object"},
          {"properties", std::move(properties)},
          {"required", std::move(required)},
          {"additionalProperties", false}};
}
boost::json::object Array(boost::json::object item) {
  return {{"type", "array"}, {"items", std::move(item)}};
}
boost::json::object Nullable(boost::json::object schema) {
  return {{"anyOf", boost::json::array{std::move(schema), Type("null")}}};
}
}  // namespace

boost::json::object ChainViewPageSchema() {
  boost::json::object summary{{"height", Type("integer")},
                              {"hash", Type("string")}};
  for (auto name : {"previous_hash", "next_hash"})
    summary[name] = Type("string", true);
  for (auto name : {"timestamp", "confirmations", "serialized_size", "weight",
                    "header_size", "transaction_count"})
    summary[name] = Type("integer", true);
  const auto block = Object(std::move(summary), {"height", "hash"});
  boost::json::object transaction{{"id", Type("string")},
                                  {"fee", Type("string", true)},
                                  {"coinbase", Type("boolean", true)},
                                  {"error", Type("string")}};
  for (auto name : {"serialized_size", "weight", "metadata_size", "input_count",
                    "output_count"})
    transaction[name] = Type("integer", true);
  boost::json::object detail{
      {"block", block},
      {"transactions", Array(Object(std::move(transaction), {"id"}))},
      {"byte_definition", Type("string")},
      {"average_transaction_size", Type("number", true)}};
  for (auto name :
       {"transaction_bytes", "minimum_transaction_size",
        "maximum_transaction_size", "miner_transaction_size", "metadata_bytes"})
    detail[name] = Type("integer", true);
  auto rows = Array(Object({{"height", Type("integer")},
                            {"summary", block},
                            {"error", Type("string")}},
                           {"height", "error"}));
  rows["maxItems"] = 32;
  return Object(
      {{"mode",
        boost::json::object{{"enum", boost::json::array{"live", "retained"}}}},
       {"source_node", Type("string")},
       {"notice", Type("string")},
       {"error", Type("string")},
       {"tip", Nullable(block)},
       {"rows", std::move(rows)},
       {"detail", Nullable(Object(std::move(detail), {"block", "transactions",
                                                      "byte_definition"}))},
       {"detail_error", Type("string")},
       {"first_height", Type("integer")},
       {"selected_height", Type("integer")},
       {"cache_entries", Type("integer")}},
      {"mode", "source_node", "tip", "rows", "detail"});
}
}  // namespace bbp
