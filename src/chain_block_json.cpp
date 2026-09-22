#include <boost/json.hpp>

#include "bbp/chain_view.h"

namespace bbp {
namespace {
template <typename T>
boost::json::value Optional(const std::optional<T>& value) {
  return value ? boost::json::value(*value) : boost::json::value(nullptr);
}
}  // namespace

boost::json::object ChainBlockSummaryJson(const ChainBlockSummary& b) {
  return {{"height", b.height},
          {"hash", b.hash},
          {"previous_hash", Optional(b.previous_hash)},
          {"next_hash", Optional(b.next_hash)},
          {"timestamp", Optional(b.timestamp)},
          {"confirmations", Optional(b.confirmations)},
          {"serialized_size", Optional(b.serialized_size)},
          {"weight", Optional(b.weight)},
          {"header_size", Optional(b.header_size)},
          {"transaction_count", Optional(b.transaction_count)}};
}

boost::json::object ChainBlockDetailJson(const ChainBlockDetail& d) {
  boost::json::array transactions;
  for (const auto& t : d.transactions) {
    transactions.emplace_back(
        boost::json::object{{"id", t.id},
                            {"serialized_size", Optional(t.serialized_size)},
                            {"weight", Optional(t.weight)},
                            {"metadata_size", Optional(t.metadata_size)},
                            {"fee", Optional(t.fee)},
                            {"input_count", Optional(t.input_count)},
                            {"output_count", Optional(t.output_count)},
                            {"coinbase", Optional(t.coinbase)},
                            {"error", t.error}});
  }
  return {{"block", ChainBlockSummaryJson(d.block)},
          {"transactions", std::move(transactions)},
          {"transaction_bytes", Optional(d.transaction_bytes)},
          {"minimum_transaction_size", Optional(d.minimum_transaction_size)},
          {"maximum_transaction_size", Optional(d.maximum_transaction_size)},
          {"average_transaction_size", Optional(d.average_transaction_size)},
          {"miner_transaction_size", Optional(d.miner_transaction_size)},
          {"metadata_bytes", Optional(d.metadata_bytes)},
          {"byte_definition", d.byte_definition}};
}
}  // namespace bbp
