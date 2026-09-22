#include <boost/json.hpp>

#include "bbp/pool_view.h"

namespace bbp {
namespace {
template <typename T>
boost::json::value Optional(const std::optional<T>& value) {
  return value ? boost::json::value_from(*value) : boost::json::value(nullptr);
}
boost::json::object Statistics(const ChainPoolSizeStatistics& statistics) {
  return {{"total", Optional(statistics.total)},
          {"minimum", Optional(statistics.minimum)},
          {"maximum", Optional(statistics.maximum)},
          {"average", Optional(statistics.average)}};
}
}  // namespace
boost::json::object PoolTransactionJson(const ChainPoolTransaction& t) {
  return {{"id", t.id},
          {"first_seen", Optional(t.first_seen)},
          {"serialized_size", Optional(t.serialized_size)},
          {"weight", Optional(t.weight)},
          {"metadata_size", Optional(t.metadata_size)},
          {"fee", Optional(t.fee)},
          {"fee_rate", Optional(t.fee_rate)},
          {"input_count", Optional(t.input_count)},
          {"output_count", Optional(t.output_count)},
          {"dependencies", Optional(t.dependencies)},
          {"ancestor_count", Optional(t.ancestor_count)},
          {"ancestor_size", Optional(t.ancestor_size)},
          {"descendant_count", Optional(t.descendant_count)},
          {"descendant_size", Optional(t.descendant_size)},
          {"replaceable", Optional(t.replaceable)},
          {"relayed", Optional(t.relayed)},
          {"do_not_relay", Optional(t.do_not_relay)},
          {"validation", Optional(t.validation)}};
}
boost::json::object PoolSummaryJson(const ChainPoolSummary& s) {
  return {{"transaction_count", s.transaction_count},
          {"serialized_size", Statistics(s.size)},
          {"weight", Statistics(s.weight)},
          {"fees", Statistics(s.fees)},
          {"youngest_first_seen", Optional(s.youngest_first_seen)},
          {"average_first_seen", Optional(s.average_first_seen)},
          {"total_fees", Optional(s.total_fees)},
          {"oldest_first_seen", Optional(s.oldest_first_seen)},
          {"memory_usage", Optional(s.memory_usage)},
          {"minimum_fee_rate", Optional(s.minimum_fee_rate)},
          {"maximum_fee_rate", Optional(s.maximum_fee_rate)},
          {"average_fee_rate", Optional(s.average_fee_rate)},
          {"fee_unit", s.fee_unit},
          {"fee_rate_unit", s.fee_rate_unit},
          {"ancestor_size_unit", s.ancestor_size_unit},
          {"byte_definition", s.byte_definition}};
}

boost::json::object PoolViewPageSchema() {
  const auto type = [](std::string_view name, bool nullable = false) {
    return boost::json::object{
        {"type", nullable ? boost::json::value(boost::json::array{
                                boost::json::value(name), "null"})
                          : boost::json::value(name)}};
  };
  const auto object = [](boost::json::object properties) {
    boost::json::array required;
    for (const auto& p : properties) required.emplace_back(p.key());
    return boost::json::object{{"type", "object"},
                               {"properties", std::move(properties)},
                               {"required", std::move(required)},
                               {"additionalProperties", false}};
  };
  const auto nullable = [&](boost::json::object value) {
    return boost::json::object{
        {"anyOf", boost::json::array{std::move(value), type("null")}}};
  };
  boost::json::object fields{{"id", type("string")}};
  for (auto name : {"first_seen", "serialized_size", "weight", "metadata_size",
                    "fee", "input_count", "output_count", "ancestor_count",
                    "ancestor_size", "descendant_count", "descendant_size"})
    fields[name] = type("integer", true);
  fields["fee_rate"] = type("number", true);
  for (auto name : {"replaceable", "relayed", "do_not_relay"})
    fields[name] = type("boolean", true);
  fields["validation"] = type("string", true);
  fields["dependencies"] =
      nullable({{"type", "array"}, {"items", type("string")}});
  const auto transaction = object(std::move(fields));
  const auto statistics = object({{"total", type("integer", true)},
                                  {"minimum", type("integer", true)},
                                  {"maximum", type("integer", true)},
                                  {"average", type("number", true)}});
  boost::json::object summary{{"transaction_count", type("integer")},
                              {"serialized_size", statistics},
                              {"weight", statistics}};
  for (auto name : {"total_fees", "oldest_first_seen", "memory_usage"})
    summary[name] = type("integer", true);
  for (auto name : {"minimum_fee_rate", "maximum_fee_rate", "average_fee_rate"})
    summary[name] = type("number", true);
  for (auto name :
       {"fee_unit", "fee_rate_unit", "ancestor_size_unit", "byte_definition"})
    summary[name] = type("string");
  auto summary_schema = object(std::move(summary));
  // Additive fields remain optional for older retained snapshots.
  auto& summary_fields = summary_schema.at("properties").as_object();
  summary_fields["fees"] = statistics;
  summary_fields["youngest_first_seen"] = type("integer", true);
  summary_fields["average_first_seen"] = type("number", true);
  return object(
      {{"mode",
        boost::json::object{{"enum", boost::json::array{"live", "retained"}}}},
       {"source_node", type("string")},
       {"sampled_at_ms", type("integer", true)},
       {"summary", nullable(std::move(summary_schema))},
       {"notice", type("string")},
       {"error", type("string")},
       {"selected_id", type("string")},
       {"selected_index", type("integer")},
       {"first_index", type("integer")},
       {"rows",
        boost::json::object{
            {"type", "array"}, {"items", transaction}, {"maxItems", 32}}},
       {"detail", nullable(transaction)},
       {"detail_error", type("string")},
       {"departed_id", type("string")},
       {"departure_reason", type("string")}});
}
}  // namespace bbp
