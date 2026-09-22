#include <boost/multiprecision/cpp_dec_float.hpp>

#include "bbp/drivers/bitcoin_driver.h"
#include "bbp/drivers/firo_driver.h"
#include "bbp/simulation_cancelled.h"
#include "block_decoding.h"

namespace bbp {
namespace {
using namespace block_decoding;
std::optional<std::uint64_t> Fee(const boost::json::object& entry) {
  const auto* value = entry.if_contains("fee");
  if (const auto* fees = entry.if_contains("fees"); fees && fees->is_object())
    value = fees->as_object().if_contains("base");
  if (!value || value->is_null()) return {};
  if (!value->is_number()) throw std::runtime_error("invalid pool fee");
  using Decimal = boost::multiprecision::cpp_dec_float_50;
  const Decimal amount =
      Decimal(boost::json::serialize(*value)) * Decimal(100000000);
  if (amount < 0 || amount > Decimal(UINT64_MAX) || trunc(amount) != amount)
    throw std::runtime_error(
        "pool fee is not an exact nonnegative atomic amount");
  return amount.convert_to<std::uint64_t>();
}
ChainPoolTransaction Entry(std::string_view id,
                           const boost::json::object& entry) {
  ChainPoolTransaction tx;
  if (HexBytes(id) != 32)
    throw std::runtime_error("invalid pool transaction id");
  tx.id = id;
  tx.first_seen = Uint(entry, "time");
  tx.weight = Uint(entry, "weight");
  tx.fee = Fee(entry);
  auto size = Uint(entry, "vsize_adjusted");
  if (!size) size = Uint(entry, "vsize");
  if (!size) size = Uint(entry, "size");
  if (tx.fee && size && *size > 0)
    tx.fee_rate = static_cast<double>(*tx.fee) / static_cast<double>(*size);
  tx.ancestor_count = Uint(entry, "ancestorcount");
  tx.ancestor_size = Uint(entry, "ancestorsize");
  tx.descendant_count = Uint(entry, "descendantcount");
  tx.descendant_size = Uint(entry, "descendantsize");
  if (const auto* dependencies = entry.if_contains("depends")) {
    tx.dependencies.emplace();
    for (const auto& id_value : dependencies->as_array()) {
      const std::string dependency(id_value.as_string());
      if (HexBytes(dependency) != 32)
        throw std::runtime_error("invalid pool dependency");
      tx.dependencies->push_back(dependency);
    }
  }
  if (const auto* v = entry.if_contains("bip125-replaceable"))
    tx.replaceable = v->as_bool();
  if (const auto* v = entry.if_contains("unbroadcast"))
    tx.relayed = !v->as_bool();
  return tx;
}
}  // namespace

ChainPoolSnapshot FiroDriver::ReadPoolSnapshot(const ChainNodeConfig& config,
                                               std::stop_token stop) const {
  const auto info = RpcCall(config, "getmempoolinfo", {}, stop).as_object();
  const auto entries =
      RpcCall(config, "getrawmempool", {true}, stop).as_object();
  if (entries.size() > ChainPoolSnapshot::kMaximumTransactions)
    throw std::runtime_error("pool exceeds 65536 transaction display bound");
  ChainPoolSnapshot snapshot;
  snapshot.summary.memory_usage = Uint(info, "usage");
  snapshot.summary.fee_unit = "1e-8 coin";
  snapshot.summary.fee_rate_unit = "1e-8 coin / daemon virtual byte";
  snapshot.summary.ancestor_size_unit = "daemon virtual bytes (including self)";
  snapshot.summary.byte_definition =
      "Pool size/vsize fields are virtual policy sizes, not serialized bytes. "
      "Full serialized size, input/output counts and explicit extra payload "
      "bytes are loaded only for the selected raw transaction. Native weight "
      "is reported only when explicit. Fees are base fees, excluding priority "
      "deltas. Fee rate uses the daemon's reported virtual policy size. "
      "Relay means initial broadcast acknowledged when unbroadcast is exposed. "
      "Replacement is reported only from explicit BIP125 data.";
  for (const auto& entry : entries) {
    if (stop.stop_requested()) throw SimulationCancelled();
    snapshot.transactions.push_back(
        Entry(entry.key(), entry.value().as_object()));
  }
  CalculatePoolSummary(snapshot);
  return snapshot;
}

ChainPoolTransaction FiroDriver::ReadPoolTransaction(
    const ChainNodeConfig& config, const ChainPoolTransaction& entry,
    std::stop_token stop) const {
  const auto raw =
      RpcCall(config, "getrawtransaction", {entry.id, true}, stop).as_object();
  if (Hash(raw, "txid") != entry.id)
    throw std::runtime_error("pool detail identity mismatch");
  auto tx = entry;
  tx.serialized_size = Uint(raw, "size");
  if (!tx.serialized_size) {
    if (const auto hex = Text(raw, "hex")) tx.serialized_size = HexBytes(*hex);
  }
  if (auto weight = Uint(raw, "weight")) tx.weight = weight;
  tx.metadata_size = Uint(raw, "extraPayloadSize");
  if (auto extra = Text(raw, "extraPayload"))
    tx.metadata_size = HexBytes(*extra);
  tx.input_count = Count(raw, "vin");
  tx.output_count = Count(raw, "vout");
  if (Uint(raw, "confirmations").value_or(0) > 0)
    tx.validation = "Mined since pool sample";
  return tx;
}

ChainPoolSnapshot BitcoinDriver::ReadPoolSnapshot(const ChainNodeConfig& config,
                                                  std::stop_token stop) const {
  return bitcoin_family_rpc_->ReadPoolSnapshot(config, stop);
}
ChainPoolTransaction BitcoinDriver::ReadPoolTransaction(
    const ChainNodeConfig& config, const ChainPoolTransaction& entry,
    std::stop_token stop) const {
  return bitcoin_family_rpc_->ReadPoolTransaction(config, entry, stop);
}
}  // namespace bbp
