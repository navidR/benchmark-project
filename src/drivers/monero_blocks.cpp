#include <map>

#include "bbp/drivers/monero_driver.h"
#include "bbp/simulation_cancelled.h"
#include "block_decoding.h"

namespace bbp {
namespace {
using namespace block_decoding;

ChainBlockSummary Summary(const boost::json::object& header) {
  ChainBlockSummary result;
  result.height = Height(header);
  result.hash = Hash(header, "hash");
  if (result.height != 0) result.previous_hash = Text(header, "prev_hash");
  result.timestamp = Uint(header, "timestamp");
  if (const auto* orphan = header.if_contains("orphan_status");
      orphan && orphan->as_bool()) {
    result.confirmations = -1;
  } else if (auto depth = Uint(header, "depth");
             depth && *depth < static_cast<std::uint64_t>(INT64_MAX)) {
    result.confirmations = static_cast<std::int64_t>(*depth + 1);
  }
  result.weight = Uint(header, "block_weight");
  if (auto count = Uint(header, "num_txes"); count && *count != UINT64_MAX)
    result.transaction_count = *count + 1;
  return result;
}

std::uint64_t HeaderBytes(std::string_view blob) {
  const auto size = HexBytes(blob);
  std::size_t pos = 0;
  const auto byte = [&](std::size_t i) {
    return std::stoul(std::string(blob.substr(i * 2, 2)), nullptr, 16);
  };
  // Canonical Monero header: three varints, previous hash, uint32 nonce.
  for (int field = 0; field < 3; ++field) {
    unsigned length = 0;
    do {
      if (pos >= size || ++length > 10)
        throw std::runtime_error("invalid Monero block header blob");
    } while ((byte(pos++) & 128U) != 0);
  }
  if (size - pos < 36)
    throw std::runtime_error("truncated Monero block header blob");
  return pos + 36;
}

ChainBlockTransaction Transaction(const boost::json::object& entry,
                                  bool miner) {
  ChainBlockTransaction tx;
  tx.id = Hash(entry, "tx_hash");
  tx.coinbase = miner;
  const auto hex = Text(entry, "as_hex");
  if (hex && !hex->empty()) {
    tx.serialized_size = HexBytes(*hex);
  } else {
    const auto base = Text(entry, "pruned_as_hex");
    const auto signatures = Text(entry, "prunable_as_hex");
    if (base && !base->empty() &&
        (miner || (signatures && !signatures->empty())))
      tx.serialized_size =
          HexBytes(*base) + (signatures ? HexBytes(*signatures) : 0);
    else
      tx.error = "Full transaction bytes unavailable (possibly pruned)";
  }
  if (auto json = Text(entry, "as_json"); json && !json->empty()) {
    const auto o = boost::json::parse(*json).as_object();
    tx.input_count = Count(o, "vin");
    tx.output_count = Count(o, "vout");
    tx.metadata_size = Count(o, "extra");
    if (!miner) {
      if (const auto* rct = o.if_contains("rct_signatures");
          rct && rct->is_object()) {
        if (auto fee = Uint(rct->as_object(), "txnFee"))
          tx.fee = std::to_string(*fee) + " atomic (1e-12 coin)";
      }
    }
  }
  return tx;
}
}  // namespace

ChainBlockSummary MoneroDriver::ReadChainTip(const ChainNodeConfig& config,
                                             std::stop_token stop) const {
  const auto result = JsonRpcCall(config, "get_last_block_header",
                                  {{"fill_pow_hash", false}}, stop);
  return Summary(result.at("block_header").as_object());
}

ChainBlockSummary MoneroDriver::ReadBlockSummary(const ChainNodeConfig& config,
                                                 std::uint64_t height,
                                                 std::stop_token stop) const {
  const auto result =
      JsonRpcCall(config, "get_block_header_by_height",
                  {{"height", height}, {"fill_pow_hash", false}}, stop);
  auto summary = Summary(result.at("block_header").as_object());
  if (summary.height != height)
    throw std::runtime_error("Monero block header height mismatch");
  return summary;
}

ChainBlockDetail MoneroDriver::ReadBlockDetail(const ChainNodeConfig& config,
                                               const std::string& hash,
                                               std::stop_token stop) const {
  const auto result = JsonRpcCall(
      config, "get_block",
      {{"hash", hash}, {"height", 0}, {"fill_pow_hash", false}}, stop);
  ChainBlockDetail detail;
  detail.block = Summary(result.at("block_header").as_object());
  if (detail.block.hash != hash)
    throw std::runtime_error("Monero block detail hash mismatch");
  const auto blob = Text(result, "blob");
  if (blob && !blob->empty()) {
    detail.block.serialized_size = HexBytes(*blob);
    detail.block.header_size = HeaderBytes(*blob);
  }
  if (detail.block.confirmations.value_or(0) > 1 &&
      detail.block.height != UINT64_MAX)
    detail.block.next_hash =
        ReadBlockSummary(config, detail.block.height + 1, stop).hash;
  std::vector<std::string> hashes{Hash(result, "miner_tx_hash")};
  const boost::json::array empty;
  const auto* tx_hashes = result.if_contains("tx_hashes");
  for (const auto& tx : tx_hashes ? tx_hashes->as_array() : empty) {
    const std::string id(tx.as_string());
    if (HexBytes(id) != 32)
      throw std::runtime_error("invalid Monero transaction hash");
    hashes.push_back(id);
  }
  detail.block.transaction_count = hashes.size();
  for (std::size_t begin = 0; begin < hashes.size(); begin += 64) {
    if (stop.stop_requested()) throw SimulationCancelled();
    boost::json::array batch;
    const auto end = std::min(begin + 64, hashes.size());
    for (auto i = begin; i < end; ++i) batch.emplace_back(hashes[i]);
    const auto response = PlainRpcCall(config, "/get_transactions",
                                       {{"txs_hashes", batch},
                                        {"decode_as_json", true},
                                        {"prune", false},
                                        {"split", false}},
                                       stop);
    std::map<std::string, ChainBlockTransaction> decoded;
    const auto* entries = response.if_contains("txs");
    for (const auto& entry : entries ? entries->as_array() : empty) {
      const auto id = Hash(entry.as_object(), "tx_hash");
      if (std::find(hashes.begin() + static_cast<std::ptrdiff_t>(begin),
                    hashes.begin() + static_cast<std::ptrdiff_t>(end),
                    id) == hashes.begin() + static_cast<std::ptrdiff_t>(end))
        throw std::runtime_error("unexpected Monero block transaction");
      decoded.emplace(id, Transaction(entry.as_object(), id == hashes.front()));
    }
    for (auto i = begin; i < end; ++i) {
      if (auto found = decoded.find(hashes[i]); found != decoded.end())
        detail.transactions.push_back(std::move(found->second));
      else {
        ChainBlockTransaction missing;
        missing.id = hashes[i];
        missing.coinbase = i == 0;
        missing.error = "Transaction unavailable";
        detail.transactions.push_back(std::move(missing));
      }
    }
  }
  CalculateBlockTransactionSizes(detail);
  if (detail.block.serialized_size && detail.miner_transaction_size &&
      *detail.block.serialized_size >= *detail.miner_transaction_size)
    detail.metadata_bytes =
        *detail.block.serialized_size - *detail.miner_transaction_size;
  detail.byte_definition =
      "Block bytes: get_block blob, containing header, miner transaction and "
      "non-miner transaction hashes, excluding non-miner bodies. Header bytes: "
      "three encoded varints, previous hash and nonce. Transaction bytes: "
      "complete RPC hex only; pruned bytes are not full sizes. Block metadata: "
      "block blob minus miner transaction. Transaction metadata: extra payload "
      "byte count. Native block weight is distinct from blob bytes; "
      "transaction weight is unavailable. Fee: explicit txnFee in atomic units "
      "(1e-12 coin).";
  return detail;
}
}  // namespace bbp
