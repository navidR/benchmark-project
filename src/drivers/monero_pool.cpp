#include "bbp/drivers/monero_driver.h"
#include "bbp/simulation_cancelled.h"
#include "block_decoding.h"

namespace bbp {
ChainPoolSnapshot MoneroDriver::ReadPoolSnapshot(const ChainNodeConfig& config,
                                                 std::stop_token stop) const {
  using namespace block_decoding;
  const auto response = PlainRpcCall(config, "/get_transaction_pool", {}, stop);
  ChainPoolSnapshot snapshot;
  snapshot.summary.fee_unit = "1e-12 coin";
  snapshot.summary.fee_rate_unit = "1e-12 coin / native weight unit";
  snapshot.summary.ancestor_size_unit = "unavailable";
  snapshot.summary.byte_definition =
      "Serialized size is the daemon's stored blob_size, which may be a pruned "
      "pool blob. Native weight is explicit RPC weight, not blob bytes. "
      "Metadata counts decoded extra bytes. Fee rate divides atomic fee by "
      "native weight. Dependency/ancestor/replacement information and pool "
      "memory usage are unavailable; ring members are not dependencies.";
  if (const auto* values = response.if_contains("transactions")) {
    const auto& entries = values->as_array();
    if (entries.size() > ChainPoolSnapshot::kMaximumTransactions)
      throw std::runtime_error("pool exceeds 65536 transaction display bound");
    for (const auto& value : entries) {
      if (stop.stop_requested()) throw SimulationCancelled();
      const auto& entry = value.as_object();
      ChainPoolTransaction tx;
      tx.id = Hash(entry, "id_hash");
      tx.first_seen = Uint(entry, "receive_time");
      if (tx.first_seen == 0) tx.first_seen.reset();
      tx.serialized_size = Uint(entry, "blob_size");
      tx.weight = Uint(entry, "weight");
      if (tx.weight == 0) tx.weight.reset();
      tx.fee = Uint(entry, "fee");
      if (tx.fee && tx.weight)
        tx.fee_rate =
            static_cast<double>(*tx.fee) / static_cast<double>(*tx.weight);
      if (auto json = Text(entry, "tx_json"); json && !json->empty()) {
        const auto decoded = boost::json::parse(*json).as_object();
        tx.metadata_size = Count(decoded, "extra");
        tx.input_count = Count(decoded, "vin");
        tx.output_count = Count(decoded, "vout");
      }
      if (const auto* v = entry.if_contains("relayed"))
        tx.relayed = v->as_bool();
      if (const auto* v = entry.if_contains("do_not_relay"))
        tx.do_not_relay = v->as_bool();
      if (const auto* v = entry.if_contains("double_spend_seen");
          v && v->as_bool())
        tx.validation = "Double spend seen (not a rejection verdict)";
      if (auto height = Uint(entry, "last_failed_height");
          height && *height > 0)
        tx.validation = "Validation failed at height " +
                        std::to_string(*height) +
                        (tx.validation ? "; " + *tx.validation : "");
      snapshot.transactions.push_back(std::move(tx));
    }
  }
  CalculatePoolSummary(snapshot);
  return snapshot;
}
}  // namespace bbp
