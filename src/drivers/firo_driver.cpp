#include "benchmark_sim/drivers/firo_driver.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <thread>

#include "benchmark_sim/util.h"

namespace bsim {
namespace {

std::string Arg(std::string key, const std::string& value) {
  key += "=";
  key += value;
  return key;
}

boost::json::value ParseRpcResponse(std::string_view body,
                                    std::string_view method) {
  boost::json::value value = boost::json::parse(body);
  const boost::json::object& object = value.as_object();
  const boost::json::value* error = object.if_contains("error");
  if (error == nullptr || !error->is_null()) {
    throw std::runtime_error("Firo RPC " + std::string(method) +
                             " returned error: " + std::string(body));
  }
  const boost::json::value* result = object.if_contains("result");
  if (result == nullptr) {
    throw std::runtime_error("Firo RPC " + std::string(method) +
                             " returned no result: " + std::string(body));
  }
  return *result;
}

std::vector<std::string> ParseStringArrayResult(
    const boost::json::value& value) {
  std::vector<std::string> values;
  for (const boost::json::value& item : value.as_array()) {
    values.emplace_back(item.as_string());
  }
  return values;
}

std::vector<std::string> ParseTxIdResult(const boost::json::value& value,
                                         std::string_view method) {
  if (value.is_string()) {
    return {std::string(value.as_string())};
  }
  if (value.is_array()) {
    std::vector<std::string> txids;
    txids.reserve(value.as_array().size());
    for (const boost::json::value& item : value.as_array()) {
      if (!item.is_string()) {
        throw std::runtime_error("Firo RPC " + std::string(method) +
                                 " returned a non-string txid");
      }
      txids.emplace_back(item.as_string());
    }
    if (txids.empty()) {
      throw std::runtime_error("Firo RPC " + std::string(method) +
                               " returned no txids");
    }
    return txids;
  }
  throw std::runtime_error("Firo RPC " + std::string(method) +
                           " returned non-string txid result");
}

bool ContainsPeerAddress(const std::vector<std::string>& addresses,
                         const std::string& address) {
  for (const std::string& candidate : addresses) {
    if (candidate == address) {
      return true;
    }
  }
  return false;
}

bool JsonStringArrayContains(const boost::json::value& value,
                             const std::string& needle) {
  if (!value.is_array()) {
    return false;
  }
  for (const boost::json::value& item : value.as_array()) {
    if (item.is_string() && std::string(item.as_string()) == needle) {
      return true;
    }
  }
  return false;
}

std::string JsonStringMember(const boost::json::object& object,
                             std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    throw std::runtime_error("missing Firo RPC string field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

uint64_t JsonUint64Member(const boost::json::object& object,
                          std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_uint64()) {
    return value->as_uint64();
  }
  if (value != nullptr && value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("missing Firo RPC uint64 field: " +
                           std::string(field));
}

bool TxOutPaysAddress(const boost::json::object& txout,
                      const std::string& source_address) {
  const boost::json::value* script = txout.if_contains("scriptPubKey");
  if (script == nullptr || !script->is_object()) {
    return false;
  }
  const boost::json::value* addresses =
      script->as_object().if_contains("addresses");
  return addresses != nullptr &&
         JsonStringArrayContains(*addresses, source_address);
}

std::string TxOutScriptPubKeyHex(const boost::json::object& txout) {
  const boost::json::value* script = txout.if_contains("scriptPubKey");
  if (script == nullptr || !script->is_object()) {
    throw std::runtime_error("Firo gettxout result has no scriptPubKey object");
  }
  return JsonStringMember(script->as_object(), "hex");
}

bool MempoolContains(const boost::json::value& mempool,
                     const std::string& txid) {
  if (!mempool.is_array()) {
    return false;
  }
  for (const boost::json::value& item : mempool.as_array()) {
    if (item.is_string() && std::string(item.as_string()) == txid) {
      return true;
    }
  }
  return false;
}

void RequireSupportedWalletMode(WalletMode wallet_mode,
                                std::string_view operation) {
  if (wallet_mode == WalletMode::kPublic) {
    return;
  }
  if (wallet_mode == WalletMode::kPrivate) {
    throw std::runtime_error(
        "Firo private wallet mode is not implemented for " +
        std::string(operation));
  }
}

}  // namespace

ProcessSpec FiroDriver::RenderProcess(const FiroNodeConfig& config) const {
  EnsureDirectory(config.data_dir);
  EnsureDirectory(config.log_dir);

  ProcessSpec spec;
  spec.binary = config.binary;
  spec.cwd = config.data_dir;
  spec.stdout_path = config.log_dir / "stdout.log";
  spec.stderr_path = config.log_dir / "stderr.log";
  spec.argv = {
      "-regtest",
      Arg("-datadir", config.data_dir.string()),
      "-server=1",
      Arg("-rpcuser", config.rpc_user),
      Arg("-rpcpassword", config.rpc_password),
      Arg("-rpcbind", config.rpc_bind),
      Arg("-rpcport", std::to_string(config.rpc_port)),
      Arg("-port", std::to_string(config.p2p_port)),
      Arg("-listen", config.listen ? "1" : "0"),
      "-dnsseed=0",
      "-fixedseeds=0",
      "-listenonion=0",
      "-discover=0",
      "-upnp=0",
      "-debug=net",
  };
  if (!config.wallet_enabled) {
    spec.argv.push_back("-disablewallet");
  }
  for (const auto& allow_ip : config.rpc_allow_ips) {
    spec.argv.push_back(Arg("-rpcallowip", allow_ip));
  }
  if (!config.p2p_bind.empty()) {
    spec.argv.push_back(Arg("-bind", config.p2p_bind));
  }
  for (const auto& peer : config.connect_peers) {
    spec.argv.push_back(Arg("-connect", peer));
  }
  return spec;
}

std::filesystem::path FiroDriver::LogPath(const FiroNodeConfig& config) const {
  return config.data_dir / "regtest" / "debug.log";
}

RpcEndpoint FiroDriver::Endpoint(const FiroNodeConfig& config) const {
  return RpcEndpoint{
      .host = config.rpc_host,
      .port = config.rpc_port,
      .user = config.rpc_user,
      .password = config.rpc_password,
  };
}

void FiroDriver::WaitReady(const FiroNodeConfig& config,
                           std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      ReadMetrics(config);
      return;
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error("Firo node " + config.id +
                           " did not become RPC-ready: " + last_error);
}

void FiroDriver::WaitForHeight(const FiroNodeConfig& config, uint64_t height,
                               std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  uint64_t last_height = 0;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      FiroMetrics metrics = ReadMetrics(config);
      last_height = metrics.height;
      if (metrics.height >= height) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error("Firo node " + config.id + " reached height " +
                           std::to_string(last_height) + " before timeout; " +
                           "target height " + std::to_string(height) +
                           (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::WaitForPeerCount(const FiroNodeConfig& config,
                                  uint64_t peer_count,
                                  std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  uint64_t last_peer_count = 0;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      FiroMetrics metrics = ReadMetrics(config);
      last_peer_count = metrics.peer_count;
      if (metrics.peer_count >= peer_count) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error("Firo node " + config.id + " reached peer count " +
                           std::to_string(last_peer_count) +
                           " before timeout; target peer count " +
                           std::to_string(peer_count) +
                           (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::WaitForPeerAddress(const FiroNodeConfig& config,
                                    const std::string& address,
                                    std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      const std::vector<std::string> addresses = PeerAddresses(config);
      for (const std::string& candidate : addresses) {
        if (candidate == address) {
          return;
        }
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "Firo node " + config.id + " did not connect to peer " + address +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::WaitForPeerAddressAbsent(const FiroNodeConfig& config,
                                          const std::string& address,
                                          std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      const std::vector<std::string> addresses = PeerAddresses(config);
      if (!ContainsPeerAddress(addresses, address)) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "Firo node " + config.id + " remained connected to peer " + address +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

FiroMetrics FiroDriver::ReadMetrics(const FiroNodeConfig& config) const {
  const auto start = std::chrono::steady_clock::now();
  const boost::json::value blockchain =
      RpcCall(config, "getblockchaininfo", boost::json::array{});
  const boost::json::value network =
      RpcCall(config, "getnetworkinfo", boost::json::array{});
  const boost::json::value mempool =
      RpcCall(config, "getmempoolinfo", boost::json::array{});
  const auto elapsed = std::chrono::steady_clock::now() - start;

  FiroMetrics metrics;
  metrics.version = JsonUint(network, "version");
  metrics.protocol_version = JsonUint(network, "protocolversion");
  metrics.subversion = JsonString(network, "subversion");
  metrics.height = JsonUint(blockchain, "blocks");
  metrics.best_hash = JsonString(blockchain, "bestblockhash");
  metrics.peer_count = JsonUint(network, "connections");
  metrics.mempool_tx_count = JsonUint(mempool, "size");
  metrics.mempool_bytes = JsonUint(mempool, "bytes");
  metrics.initial_block_download =
      JsonOptionalBool(blockchain, "initialblockdownload");
  metrics.difficulty = JsonOptionalDouble(blockchain, "difficulty");
  metrics.rpc_latency_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
  return metrics;
}

std::vector<std::string> FiroDriver::PeerAddresses(
    const FiroNodeConfig& config) const {
  const boost::json::value peers =
      RpcCall(config, "getpeerinfo", boost::json::array{});
  std::vector<std::string> addresses;
  for (const boost::json::value& peer : peers.as_array()) {
    if (!peer.is_object()) {
      continue;
    }
    const boost::json::value* address = peer.as_object().if_contains("addr");
    if (address != nullptr && address->is_string()) {
      addresses.emplace_back(address->as_string());
    }
  }
  return addresses;
}

std::vector<std::string> FiroDriver::GenerateBlocks(
    const FiroNodeConfig& config, uint32_t count,
    const std::string& address) const {
  boost::json::array params;
  params.push_back(count);
  params.emplace_back(address);
  return ParseStringArrayResult(RpcCall(config, "generatetoaddress", params));
}

std::string FiroDriver::CreateWalletAddress(const FiroNodeConfig& config,
                                            WalletMode wallet_mode) const {
  RequireSupportedWalletMode(wallet_mode, "address creation");
  const boost::json::value address =
      RpcCall(config, "getnewaddress", boost::json::array{});
  if (!address.is_string()) {
    throw std::runtime_error("Firo getnewaddress returned non-string");
  }
  return std::string(address.as_string());
}

FiroUtxo FiroDriver::FindSpendableOutput(
    const FiroNodeConfig& config, const std::vector<std::string>& block_hashes,
    const std::string& source_address, uint64_t minimum_amount_satoshis,
    uint64_t minimum_confirmations) const {
  constexpr uint32_t kMaxOutputsToProbe = 64;
  for (const std::string& block_hash : block_hashes) {
    boost::json::array block_params;
    block_params.emplace_back(block_hash);
    block_params.push_back(1);
    const boost::json::value block = RpcCall(config, "getblock", block_params);
    const boost::json::value* transactions =
        block.as_object().if_contains("tx");
    if (transactions == nullptr || !transactions->is_array()) {
      continue;
    }
    for (const boost::json::value& tx : transactions->as_array()) {
      if (!tx.is_string()) {
        continue;
      }
      const std::string txid(tx.as_string());
      for (uint32_t vout = 0; vout < kMaxOutputsToProbe; ++vout) {
        boost::json::array txout_params;
        txout_params.emplace_back(txid);
        txout_params.push_back(vout);
        txout_params.push_back(false);
        const boost::json::value txout =
            RpcCall(config, "gettxout", txout_params);
        if (txout.is_null() || !txout.is_object()) {
          continue;
        }
        const boost::json::object& txout_object = txout.as_object();
        if (!TxOutPaysAddress(txout_object, source_address)) {
          continue;
        }
        const boost::json::value* value = txout_object.if_contains("value");
        if (value == nullptr) {
          throw std::runtime_error("Firo gettxout result has no value field");
        }
        const uint64_t confirmations =
            JsonUint64Member(txout_object, "confirmations");
        const uint64_t amount_satoshis = JsonFixed8Amount(*value, "value");
        if (confirmations < minimum_confirmations ||
            amount_satoshis < minimum_amount_satoshis) {
          continue;
        }

        FiroUtxo utxo;
        utxo.txid = txid;
        utxo.vout = vout;
        utxo.amount_satoshis = amount_satoshis;
        utxo.amount = FormatFixed8Amount(amount_satoshis);
        utxo.script_pub_key = TxOutScriptPubKeyHex(txout_object);
        utxo.block_hash = block_hash;
        utxo.confirmations = confirmations;
        return utxo;
      }
    }
  }
  throw std::runtime_error("no spendable Firo UTXO found for " +
                           source_address);
}

FiroRawTransactionResult FiroDriver::SendRawTransaction(
    const FiroNodeConfig& config, const FiroUtxo& utxo,
    const std::string& source_address, const std::string& source_private_key,
    const std::string& destination_address, uint64_t amount_satoshis,
    uint64_t fee_satoshis, std::chrono::seconds timeout) const {
  if (utxo.amount_satoshis < amount_satoshis ||
      utxo.amount_satoshis - amount_satoshis < fee_satoshis) {
    throw std::runtime_error(
        "selected Firo UTXO does not cover amount and fee");
  }
  const uint64_t change_satoshis =
      utxo.amount_satoshis - amount_satoshis - fee_satoshis;

  boost::json::array inputs;
  boost::json::object input;
  input["txid"] = utxo.txid;
  input["vout"] = utxo.vout;
  inputs.push_back(std::move(input));

  boost::json::object outputs;
  outputs[destination_address] = FormatFixed8Amount(amount_satoshis);
  if (change_satoshis != 0U) {
    outputs[source_address] = FormatFixed8Amount(change_satoshis);
  }

  boost::json::array create_params;
  create_params.push_back(std::move(inputs));
  create_params.push_back(std::move(outputs));
  const boost::json::value raw =
      RpcCall(config, "createrawtransaction", create_params);
  if (!raw.is_string()) {
    throw std::runtime_error("Firo createrawtransaction returned non-string");
  }

  boost::json::array prevtxs;
  boost::json::object prevtx;
  prevtx["txid"] = utxo.txid;
  prevtx["vout"] = utxo.vout;
  prevtx["scriptPubKey"] = utxo.script_pub_key;
  prevtx["amount"] = utxo.amount;
  prevtxs.push_back(std::move(prevtx));
  boost::json::array privkeys;
  privkeys.emplace_back(source_private_key);
  boost::json::array sign_params;
  sign_params.push_back(raw);
  sign_params.push_back(std::move(prevtxs));
  sign_params.push_back(std::move(privkeys));
  const boost::json::value signed_tx =
      RpcCall(config, "signrawtransaction", sign_params);
  const boost::json::object& signed_object = signed_tx.as_object();
  const boost::json::value* complete = signed_object.if_contains("complete");
  if (complete == nullptr || !complete->is_bool() || !complete->as_bool()) {
    const boost::json::value* errors = signed_object.if_contains("errors");
    throw std::runtime_error("Firo signrawtransaction did not complete" +
                             (errors == nullptr
                                  ? std::string()
                                  : ": " + boost::json::serialize(*errors)));
  }
  const std::string signed_hex = JsonStringMember(signed_object, "hex");

  boost::json::array send_params;
  send_params.emplace_back(signed_hex);
  send_params.push_back(false);
  const boost::json::value txid_value =
      RpcCall(config, "sendrawtransaction", send_params);
  if (!txid_value.is_string()) {
    throw std::runtime_error("Firo sendrawtransaction returned non-string");
  }
  const std::string txid(txid_value.as_string());

  FiroRawTransactionResult result;
  result.utxo = utxo;
  result.raw_hex = std::string(raw.as_string());
  result.signed_hex = signed_hex;
  result.txid = txid;
  result.destination_amount = FormatFixed8Amount(amount_satoshis);
  result.change_amount = FormatFixed8Amount(change_satoshis);
  result.fee = FormatFixed8Amount(fee_satoshis);
  result.mempool_size = WaitForMempoolTransaction(config, txid, timeout);
  return result;
}

FiroWalletTransactionResult FiroDriver::SendWalletTransaction(
    const FiroNodeConfig& config, WalletMode wallet_mode,
    const std::string& destination_address, uint64_t amount_satoshis,
    uint64_t fee_satoshis, std::chrono::seconds timeout) const {
  RequireSupportedWalletMode(wallet_mode, "transaction submission");
  boost::json::array fee_params;
  fee_params.emplace_back(FormatFixed8Amount(fee_satoshis));
  const boost::json::value fee_result = RpcCall(config, "settxfee", fee_params);
  if (fee_result.is_bool() && !fee_result.as_bool()) {
    throw std::runtime_error("Firo settxfee returned false");
  }

  boost::json::array send_params;
  send_params.emplace_back(destination_address);
  send_params.emplace_back(FormatFixed8Amount(amount_satoshis));
  send_params.emplace_back("");
  send_params.emplace_back("");
  send_params.emplace_back(false);
  const std::vector<std::string> txids = ParseTxIdResult(
      RpcCall(config, "sendtoaddress", send_params), "sendtoaddress");

  FiroWalletTransactionResult result;
  result.txids = txids;
  result.destination_amount = FormatFixed8Amount(amount_satoshis);
  result.requested_fee_rate = FormatFixed8Amount(fee_satoshis);
  for (const std::string& txid : result.txids) {
    result.mempool_size = WaitForMempoolTransaction(config, txid, timeout);
  }
  return result;
}

uint64_t FiroDriver::WaitForMempoolTransaction(
    const FiroNodeConfig& config, const std::string& txid,
    std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  uint64_t last_size = 0;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      const boost::json::value mempool =
          RpcCall(config, "getrawmempool", boost::json::array{});
      if (mempool.is_array()) {
        last_size = mempool.as_array().size();
      }
      if (MempoolContains(mempool, txid)) {
        return last_size;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "Firo node " + config.id + " did not report mempool transaction " + txid +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::ConnectPeer(const FiroNodeConfig& config,
                             const std::string& address) const {
  boost::json::array params;
  params.emplace_back(address);
  params.emplace_back("onetry");
  RpcCall(config, "addnode", params);
}

void FiroDriver::DisconnectPeer(const FiroNodeConfig& config,
                                const std::string& address) const {
  boost::json::array params;
  params.emplace_back(address);
  RpcCall(config, "disconnectnode", params);
}

void FiroDriver::Stop(const FiroNodeConfig& config) const {
  try {
    RpcCall(config, "stop", boost::json::array{});
  } catch (const std::exception&) {
  }
}

boost::json::value FiroDriver::RpcCall(const FiroNodeConfig& config,
                                       std::string_view method,
                                       const boost::json::array& params) const {
  boost::json::object request;
  request["jsonrpc"] = "1.0";
  request["id"] = "benchmark-sim";
  request["method"] = method;
  request["params"] = params;
  const std::string body = boost::json::serialize(request);
  HttpResponse response = http_.PostJson(Endpoint(config), "/", body);
  if (response.status != 200) {
    throw std::runtime_error("Firo RPC HTTP status " +
                             std::to_string(response.status) + " for " +
                             std::string(method) + ": " + response.body);
  }
  return ParseRpcResponse(response.body, method);
}

}  // namespace bsim
