#include "bbp/drivers/bitcoin_driver.h"
#include "bbp/drivers/firo_driver.h"
#include "block_decoding.h"

namespace bbp {
namespace {
using namespace block_decoding;

ChainBlockSummary Summary(const boost::json::object& o) {
  ChainBlockSummary result;
  result.height = Height(o);
  result.hash = Hash(o, "hash");
  result.previous_hash = Text(o, "previousblockhash");
  result.next_hash = Text(o, "nextblockhash");
  result.timestamp = Uint(o, "time");
  result.confirmations = Signed(o, "confirmations");
  result.serialized_size = Uint(o, "size");
  result.weight = Uint(o, "weight");
  result.transaction_count = Uint(o, "nTx");
  return result;
}

ChainBlockTransaction Transaction(const boost::json::object& o) {
  ChainBlockTransaction tx;
  tx.id = Hash(o, "txid");
  tx.serialized_size = Uint(o, "size");
  tx.weight = Uint(o, "weight");
  tx.metadata_size = Uint(o, "extraPayloadSize");
  if (auto extra = Text(o, "extraPayload")) tx.metadata_size = HexBytes(*extra);
  tx.input_count = Count(o, "vin");
  tx.output_count = Count(o, "vout");
  if (tx.input_count) {
    tx.coinbase = false;
    for (const auto& in : o.at("vin").as_array()) {
      if (in.is_object() && in.as_object().contains("coinbase"))
        tx.coinbase = true;
      if (in.is_object()) {
        const auto* fee = in.as_object().if_contains("nFees");
        if (fee && fee->is_number())
          tx.fee = boost::json::serialize(*fee) + " coin";
      }
    }
  }
  if (const auto* fee = o.if_contains("fee"); fee && fee->is_number())
    tx.fee = boost::json::serialize(*fee) + " coin";
  return tx;
}
}  // namespace

ChainBlockSummary FiroDriver::ReadChainTip(const ChainNodeConfig& config,
                                           std::stop_token stop) const {
  const auto hash = RpcCall(config, "getbestblockhash", {}, stop).as_string();
  return Summary(
      RpcCall(config, "getblockheader", {hash, true}, stop).as_object());
}

ChainBlockSummary FiroDriver::ReadBlockSummary(const ChainNodeConfig& config,
                                               std::uint64_t height,
                                               std::stop_token stop) const {
  const auto hash = RpcCall(config, "getblockhash", {height}, stop).as_string();
  auto summary = Summary(
      RpcCall(config, "getblockheader", {hash, true}, stop).as_object());
  if (summary.height != height || summary.hash != hash)
    throw std::runtime_error("block header identity mismatch");
  return summary;
}

ChainBlockDetail FiroDriver::ReadBlockDetail(const ChainNodeConfig& config,
                                             const std::string& hash,
                                             std::stop_token stop) const {
  ChainBlockDetail detail;
  const auto o = RpcCall(config, "getblock", {hash, 2}, stop).as_object();
  detail.block = Summary(o);
  if (detail.block.hash != hash)
    throw std::runtime_error("block detail identity mismatch");
  const auto& txs = o.at("tx").as_array();
  detail.block.transaction_count = txs.size();
  for (const auto& tx : txs)
    detail.transactions.push_back(Transaction(tx.as_object()));
  const auto header =
      RpcCall(config, "getblockheader", {hash, false}, stop).as_string();
  detail.block.header_size = HexBytes(header);
  CalculateBlockTransactionSizes(detail);
  if (detail.block.serialized_size && detail.transaction_bytes &&
      *detail.block.serialized_size >= *detail.transaction_bytes)
    detail.metadata_bytes =
        *detail.block.serialized_size - *detail.transaction_bytes;
  detail.byte_definition =
      "Block/transaction bytes: RPC serialized size including witness. Header: "
      "raw header hex bytes. Block metadata: block bytes minus all transaction "
      "bytes (header and count encoding). Transaction metadata: explicit extra "
      "payload only; otherwise unknown. Weight: RPC native weight only; vsize "
      "is not converted to weight. Fee: explicit RPC fee only, in native "
      "coins.";
  return detail;
}

ChainBlockSummary BitcoinDriver::ReadChainTip(const ChainNodeConfig& config,
                                              std::stop_token stop) const {
  return bitcoin_family_rpc_->ReadChainTip(config, stop);
}
ChainBlockSummary BitcoinDriver::ReadBlockSummary(const ChainNodeConfig& config,
                                                  std::uint64_t height,
                                                  std::stop_token stop) const {
  return bitcoin_family_rpc_->ReadBlockSummary(config, height, stop);
}
ChainBlockDetail BitcoinDriver::ReadBlockDetail(const ChainNodeConfig& config,
                                                const std::string& hash,
                                                std::stop_token stop) const {
  return bitcoin_family_rpc_->ReadBlockDetail(config, hash, stop);
}
}  // namespace bbp
