#include "bbp/drivers/firo_driver.h"

#include <algorithm>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/network_v4.hpp>
#include <boost/asio/ip/network_v6.hpp>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>
#include <boost/url/parse.hpp>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <set>

#include "bbp/simulation_cancelled.h"
#include "bbp/util.h"

namespace bbp {
namespace {

// Regtest activates Spark mints at height 100, but its version-3 special
// transaction gate rejects Spark spends until DIP0003Height.
constexpr std::uint64_t kRegtestSparkSpendActivationHeight = 500U;

class FiroRpcError final : public std::runtime_error {
 public:
  FiroRpcError(std::int64_t code, std::string message)
      : std::runtime_error(std::move(message)), code_(code) {}

  std::int64_t code() const noexcept { return code_; }

 private:
  std::int64_t code_;
};

std::string PeerHost(const std::string& endpoint) {
  const std::string uri = "tcp://" + endpoint;
  const boost::system::result<boost::urls::url_view> parsed =
      boost::urls::parse_uri(uri);
  if (!parsed) {
    throw std::runtime_error("invalid Firo peer endpoint: " + endpoint);
  }
  const std::string host(parsed->host());
  if (host.empty()) {
    throw std::runtime_error("Firo peer endpoint has no host: " + endpoint);
  }
  boost::system::error_code error;
  boost::asio::ip::make_address(host, error);
  if (error) {
    throw std::runtime_error("Firo peer endpoint host is not an IP address: " +
                             endpoint);
  }
  return host;
}

bool IsExactBanForAddress(std::string_view subnet,
                          const boost::asio::ip::address& address) {
  boost::system::error_code error;
  if (address.is_v4()) {
    const boost::asio::ip::network_v4 network =
        boost::asio::ip::make_network_v4(subnet, error);
    return !error && network.prefix_length() == 32U &&
           network.network() == address.to_v4();
  }
  const boost::asio::ip::network_v6 network =
      boost::asio::ip::make_network_v6(subnet, error);
  return !error && network.prefix_length() == 128U &&
         network.network() == address.to_v6();
}

bool IsPeerHostBanned(const boost::json::value& bans, const std::string& host) {
  const boost::asio::ip::address address = boost::asio::ip::make_address(host);
  for (const boost::json::value& ban : bans.as_array()) {
    if (!ban.is_object()) {
      continue;
    }
    const boost::json::value* subnet = ban.as_object().if_contains("address");
    if (subnet != nullptr && subnet->is_string() &&
        IsExactBanForAddress(subnet->as_string(), address)) {
      return true;
    }
  }
  return false;
}

void ThrowIfStopRequested(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
}

void WaitForNextPoll(std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  std::condition_variable_any condition;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  condition.wait_for(lock, stop_token, std::chrono::milliseconds(250),
                     [] { return false; });
  ThrowIfStopRequested(stop_token);
}

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
  if (error == nullptr) {
    throw std::runtime_error("Firo RPC " + std::string(method) +
                             " returned no error field: " + std::string(body));
  }
  if (!error->is_null()) {
    if (!error->is_object()) {
      throw std::runtime_error(
          "Firo RPC " + std::string(method) +
          " returned malformed error: " + std::string(body));
    }
    const boost::json::value* code = error->as_object().if_contains("code");
    std::optional<std::int64_t> parsed_code;
    if (code != nullptr && code->is_int64()) {
      parsed_code = code->as_int64();
    } else if (code != nullptr && code->is_uint64() &&
               code->as_uint64() <=
                   static_cast<std::uint64_t>(
                       std::numeric_limits<std::int64_t>::max())) {
      parsed_code = static_cast<std::int64_t>(code->as_uint64());
    }
    if (!parsed_code) {
      throw std::runtime_error(
          "Firo RPC " + std::string(method) +
          " returned error without an int64 code: " + std::string(body));
    }
    throw FiroRpcError(*parsed_code,
                       "Firo RPC " + std::string(method) +
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
  if (value.is_string() && !value.as_string().empty()) {
    return {std::string(value.as_string())};
  }
  if (value.is_array()) {
    std::vector<std::string> txids;
    txids.reserve(value.as_array().size());
    for (const boost::json::value& item : value.as_array()) {
      if (!item.is_string() || item.as_string().empty()) {
        throw std::runtime_error("Firo RPC " + std::string(method) +
                                 " returned an invalid txid");
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

std::string ParseWalletAddressResult(const boost::json::value& value,
                                     std::string_view method) {
  if (value.is_string() && !value.as_string().empty()) {
    return std::string(value.as_string());
  }
  if (value.is_array() && value.as_array().size() == 1U &&
      value.as_array().front().is_string() &&
      !value.as_array().front().as_string().empty()) {
    return std::string(value.as_array().front().as_string());
  }
  throw std::runtime_error("Firo RPC " + std::string(method) +
                           " returned an invalid wallet address");
}

bool ContainsPeerAddress(const std::vector<std::string>& addresses,
                         const std::string& address) {
  const std::string expected_host = PeerHost(address);
  for (const std::string& candidate : addresses) {
    if (PeerHost(candidate) == expected_host) {
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

std::int64_t JsonInt64Member(const boost::json::object& object,
                             std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_int64()) {
    return value->as_int64();
  }
  if (value != nullptr && value->is_uint64() &&
      value->as_uint64() <= static_cast<std::uint64_t>(
                                std::numeric_limits<std::int64_t>::max())) {
    return static_cast<std::int64_t>(value->as_uint64());
  }
  throw std::runtime_error("missing Firo RPC int64 field: " +
                           std::string(field));
}

std::string OptionalJsonStringMember(const boost::json::object& object,
                                     std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_string()
             ? std::string(value->as_string())
             : std::string();
}

std::optional<bool> OptionalJsonBoolMember(const boost::json::object& object,
                                           std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_bool()
             ? std::optional<bool>(value->as_bool())
             : std::nullopt;
}

double JsonNonNegativeFiniteNumber(const boost::json::value& value,
                                   std::string_view field) {
  double parsed = 0.0;
  if (value.is_double()) {
    parsed = value.as_double();
  } else if (value.is_int64()) {
    parsed = static_cast<double>(value.as_int64());
  } else if (value.is_uint64()) {
    parsed = static_cast<double>(value.as_uint64());
  } else {
    throw std::runtime_error("Firo RPC field is not numeric: " +
                             std::string(field));
  }
  if (!std::isfinite(parsed) || parsed < 0.0) {
    throw std::runtime_error("Firo RPC field is not finite and non-negative: " +
                             std::string(field));
  }
  return parsed;
}

std::optional<std::int64_t> OptionalJsonSignedFixed8Amount(
    const boost::json::object& object, std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || value->is_null()) {
    return std::nullopt;
  }
  using Decimal = boost::multiprecision::cpp_dec_float_50;
  const Decimal scaled =
      Decimal(boost::json::serialize(*value)) * Decimal(100000000);
  const Decimal integral = boost::multiprecision::trunc(scaled);
  if (scaled != integral ||
      integral < Decimal(std::numeric_limits<std::int64_t>::min()) ||
      integral > Decimal(std::numeric_limits<std::int64_t>::max())) {
    throw std::runtime_error("invalid Firo RPC fixed-8 field: " +
                             std::string(field));
  }
  return integral.convert_to<std::int64_t>();
}

std::uint64_t JsonFixed8Member(const boost::json::object& object,
                               std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing Firo RPC fixed-8 field: " +
                             std::string(field));
  }
  return JsonFixed8Amount(*value, field);
}

enum class FiroWalletTransactionCategory {
  kSend,
  kSpend,
  kMint,
  kReceive,
  kGenerate,
  kImmature,
  kOrphan,
  kZnode,
  kMove,
};

FiroWalletTransactionCategory ParseWalletTransactionCategory(
    std::string_view category) {
  if (category == "send" || category == "spend" || category == "mint") {
    if (category == "send") {
      return FiroWalletTransactionCategory::kSend;
    }
    if (category == "spend") {
      return FiroWalletTransactionCategory::kSpend;
    }
    return FiroWalletTransactionCategory::kMint;
  }
  if (category == "receive" || category == "generate" ||
      category == "immature" || category == "orphan" || category == "znode") {
    if (category == "receive") {
      return FiroWalletTransactionCategory::kReceive;
    }
    if (category == "generate") {
      return FiroWalletTransactionCategory::kGenerate;
    }
    if (category == "immature") {
      return FiroWalletTransactionCategory::kImmature;
    }
    if (category == "orphan") {
      return FiroWalletTransactionCategory::kOrphan;
    }
    return FiroWalletTransactionCategory::kZnode;
  }
  if (category == "move") {
    return FiroWalletTransactionCategory::kMove;
  }
  throw std::runtime_error("unknown Firo wallet transaction category: " +
                           std::string(category));
}

ChainWalletTransactionDirection WalletTransactionDirection(
    FiroWalletTransactionCategory category) {
  switch (category) {
    case FiroWalletTransactionCategory::kSend:
    case FiroWalletTransactionCategory::kSpend:
    case FiroWalletTransactionCategory::kMint:
      return ChainWalletTransactionDirection::kOutgoing;
    case FiroWalletTransactionCategory::kReceive:
    case FiroWalletTransactionCategory::kGenerate:
    case FiroWalletTransactionCategory::kImmature:
    case FiroWalletTransactionCategory::kOrphan:
    case FiroWalletTransactionCategory::kZnode:
      return ChainWalletTransactionDirection::kIncoming;
    case FiroWalletTransactionCategory::kMove:
      return ChainWalletTransactionDirection::kInternal;
  }
  return ChainWalletTransactionDirection::kInternal;
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

}  // namespace

ProcessSpec FiroDriver::RenderProcess(const FiroNodeConfig& config) const {
  if (config.network != ChainNetwork::kRegtest) {
    throw std::runtime_error("Firo driver supports only regtest network");
  }
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
      "-dandelion=0",
      "-txindex=1",
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
  return spec;
}

std::optional<LogTailChunk> FiroDriver::ReadLogTail(
    const FiroNodeConfig& config, ChainLogSource source,
    const LogTailCursor& cursor, std::uint64_t max_bytes) const {
  if (config.network != ChainNetwork::kRegtest) {
    throw std::runtime_error("Firo driver supports only regtest network");
  }
  std::filesystem::path path;
  switch (source) {
    case ChainLogSource::kDaemon:
      path = config.data_dir / "regtest" / "debug.log";
      break;
    case ChainLogSource::kStdout:
      path = config.log_dir / "stdout.log";
      break;
    case ChainLogSource::kStderr:
      path = config.log_dir / "stderr.log";
      break;
  }
  return TailLogFile(path, cursor, max_bytes);
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
                           std::chrono::seconds timeout,
                           std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      ReadMetrics(config, stop_token);
      return;
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error("Firo node " + config.id +
                           " did not become RPC-ready: " + last_error);
}

void FiroDriver::WaitForHeight(const FiroNodeConfig& config, uint64_t height,
                               std::chrono::seconds timeout,
                               std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  uint64_t last_height = 0;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      FiroMetrics metrics = ReadMetrics(config, stop_token);
      last_height = metrics.height;
      if (metrics.height >= height) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error("Firo node " + config.id + " reached height " +
                           std::to_string(last_height) + " before timeout; " +
                           "target height " + std::to_string(height) +
                           (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::WaitForPeerCount(const FiroNodeConfig& config,
                                  uint64_t peer_count,
                                  std::chrono::seconds timeout,
                                  std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  uint64_t last_peer_count = 0;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      FiroMetrics metrics = ReadMetrics(config, stop_token);
      last_peer_count = metrics.peer_count;
      if (metrics.peer_count >= peer_count) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error("Firo node " + config.id + " reached peer count " +
                           std::to_string(last_peer_count) +
                           " before timeout; target peer count " +
                           std::to_string(peer_count) +
                           (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::WaitForPeerAddress(const FiroNodeConfig& config,
                                    const std::string& address,
                                    std::chrono::seconds timeout,
                                    std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      const std::vector<std::string> addresses =
          PeerAddresses(config, stop_token);
      if (ContainsPeerAddress(addresses, address)) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Firo node " + config.id + " did not connect to peer " + address +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

void FiroDriver::WaitForPeerAddressAbsent(const FiroNodeConfig& config,
                                          const std::string& address,
                                          std::chrono::seconds timeout,
                                          std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      const std::vector<std::string> addresses =
          PeerAddresses(config, stop_token);
      if (!ContainsPeerAddress(addresses, address)) {
        return;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Firo node " + config.id + " remained connected to peer " + address +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

FiroMetrics FiroDriver::ReadMetrics(const FiroNodeConfig& config,
                                    std::stop_token stop_token) const {
  const auto start = std::chrono::steady_clock::now();
  const boost::json::value blockchain =
      RpcCall(config, "getblockchaininfo", boost::json::array{}, stop_token);
  const boost::json::value network =
      RpcCall(config, "getnetworkinfo", boost::json::array{}, stop_token);
  const boost::json::value mempool =
      RpcCall(config, "getmempoolinfo", boost::json::array{}, stop_token);

  const std::string best_hash = JsonString(blockchain, "bestblockhash");
  boost::json::array header_params;
  header_params.emplace_back(best_hash);
  header_params.emplace_back(true);
  const boost::json::value header =
      RpcCall(config, "getblockheader", header_params, stop_token);
  const boost::json::value network_hashrate =
      RpcCall(config, "getnetworkhashps", boost::json::array{}, stop_token);
  const std::vector<std::string> peer_addresses =
      PeerAddresses(config, stop_token);

  FiroMetrics metrics;
  metrics.version = JsonUint(network, "version");
  metrics.protocol_version = JsonUint(network, "protocolversion");
  metrics.subversion = JsonString(network, "subversion");
  metrics.height = JsonUint(blockchain, "blocks");
  metrics.headers = JsonUint(blockchain, "headers");
  metrics.best_hash = best_hash;
  metrics.peer_count = JsonUint(network, "connections");
  metrics.peer_addresses = peer_addresses;
  metrics.mempool_tx_count = JsonUint(mempool, "size");
  metrics.mempool_bytes = JsonUint(mempool, "bytes");
  metrics.initial_block_download =
      JsonOptionalBool(blockchain, "initialblockdownload");
  metrics.verification_progress =
      JsonOptionalDouble(blockchain, "verificationprogress");
  if (!metrics.verification_progress ||
      !std::isfinite(*metrics.verification_progress) ||
      *metrics.verification_progress < 0.0 ||
      *metrics.verification_progress > 1.0) {
    throw std::runtime_error(
        "Firo verificationprogress must be finite and in 0..1");
  }
  metrics.difficulty = JsonOptionalDouble(blockchain, "difficulty");
  if (!metrics.difficulty || !std::isfinite(*metrics.difficulty) ||
      *metrics.difficulty < 0.0) {
    throw std::runtime_error("Firo difficulty must be finite and non-negative");
  }
  metrics.hashrate_estimate =
      JsonNonNegativeFiniteNumber(network_hashrate, "getnetworkhashps result");
  metrics.last_block_time = JsonUint(header, "time");
  metrics.median_time = JsonUint(blockchain, "mediantime");
  metrics.chainwork = JsonString(blockchain, "chainwork");
  if (metrics.chainwork->empty()) {
    throw std::runtime_error("Firo chainwork must not be empty");
  }
  if (metrics.initial_block_download) {
    metrics.sync_status = *metrics.initial_block_download
                              ? ChainSyncStatus::kSyncing
                              : ChainSyncStatus::kSynced;
  } else {
    metrics.sync_status = *metrics.headers <= metrics.height &&
                                  *metrics.verification_progress >= 1.0
                              ? ChainSyncStatus::kSynced
                              : ChainSyncStatus::kSyncing;
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  metrics.rpc_latency_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
  return metrics;
}

std::vector<std::string> FiroDriver::PeerAddresses(
    const FiroNodeConfig& config, std::stop_token stop_token) const {
  const boost::json::value peers =
      RpcCall(config, "getpeerinfo", boost::json::array{}, stop_token);
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

std::vector<std::string> FiroDriver::ConnectedPeerAddresses(
    const FiroNodeConfig& config,
    const std::vector<std::string>& candidate_addresses,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  std::set<std::string> candidate_hosts;
  for (const std::string& candidate : candidate_addresses) {
    if (!candidate_hosts.insert(PeerHost(candidate)).second) {
      throw UnsupportedChainOperation(
          "Firo",
          "per-node peer identity without isolated networking; rerun with "
          "--isolate-network");
    }
  }
  const std::vector<std::string> reported = PeerAddresses(config, stop_token);
  std::vector<std::string> connected;
  connected.reserve(candidate_addresses.size());
  for (const std::string& candidate : candidate_addresses) {
    if (ContainsPeerAddress(reported, candidate)) {
      connected.push_back(candidate);
    }
  }
  return connected;
}

std::vector<std::string> FiroDriver::GenerateBlocks(
    const FiroNodeConfig& config, uint32_t count, const std::string& address,
    std::stop_token stop_token) const {
  boost::json::array params;
  params.push_back(count);
  params.emplace_back(address);
  return ParseStringArrayResult(
      RpcCall(config, "generatetoaddress", params, stop_token));
}

std::uint64_t FiroDriver::ReadBlockNonRewardTransactionCount(
    const FiroNodeConfig& config, const std::string& block_hash,
    std::stop_token stop_token) const {
  boost::json::array params;
  params.emplace_back(block_hash);
  params.emplace_back(1);
  const boost::json::value block =
      RpcCall(config, "getblock", params, stop_token);
  if (!block.is_object()) {
    throw std::runtime_error("Firo getblock returned non-object");
  }
  const boost::json::value* transactions = block.as_object().if_contains("tx");
  if (transactions == nullptr || !transactions->is_array() ||
      transactions->as_array().empty()) {
    throw std::runtime_error(
        "Firo getblock returned no reward transaction for " + block_hash);
  }
  return static_cast<std::uint64_t>(transactions->as_array().size() - 1U);
}

std::string FiroDriver::CreateWalletAddress(const FiroNodeConfig& config,
                                            WalletMode wallet_mode,
                                            std::stop_token stop_token) const {
  switch (wallet_mode) {
    case WalletMode::kPublic:
      return ParseWalletAddressResult(
          RpcCall(config, "getnewaddress", boost::json::array{}, stop_token),
          "getnewaddress");
    case WalletMode::kPrivate:
      return ParseWalletAddressResult(RpcCall(config, "getnewsparkaddress",
                                              boost::json::array{}, stop_token),
                                      "getnewsparkaddress");
  }
  throw std::runtime_error("unknown Firo wallet mode");
}

std::string FiroDriver::CreateWalletFundingAddress(
    const FiroNodeConfig& config, WalletMode wallet_mode,
    const std::string& wallet_address, std::stop_token stop_token) const {
  switch (wallet_mode) {
    case WalletMode::kPublic:
      return wallet_address;
    case WalletMode::kPrivate:
      return ParseWalletAddressResult(
          RpcCall(config, "getnewaddress", boost::json::array{}, stop_token),
          "getnewaddress");
  }
  throw std::runtime_error("unknown Firo wallet mode");
}

ChainWalletFundingResult FiroDriver::PrepareWalletFunding(
    const FiroNodeConfig& config, WalletMode wallet_mode,
    const std::string& wallet_address, uint64_t minimum_balance_satoshis,
    uint64_t minimum_confirmations, std::chrono::seconds timeout,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  switch (wallet_mode) {
    case WalletMode::kPublic:
      return {};
    case WalletMode::kPrivate:
      break;
  }
  if (wallet_address.empty()) {
    throw std::runtime_error(
        "Firo Spark funding requires a non-empty wallet address");
  }

  WaitForWalletBalance(config, WalletMode::kPublic, minimum_balance_satoshis,
                       minimum_confirmations, timeout, stop_token);
  boost::json::object mint;
  mint["amount"] = FormatFixed8Amount(minimum_balance_satoshis);
  mint["memo"] = "";
  boost::json::object recipients;
  recipients[wallet_address] = std::move(mint);
  boost::json::array params;
  params.emplace_back(std::move(recipients));

  ChainWalletFundingResult result;
  result.txids = ParseTxIdResult(
      RpcCall(config, "mintspark", params, stop_token), "mintspark");
  result.confirmation_blocks_required = 1U;
  result.minimum_chain_height = kRegtestSparkSpendActivationHeight;
  return result;
}

uint64_t FiroDriver::WaitForWalletBalance(const FiroNodeConfig& config,
                                          WalletMode wallet_mode,
                                          uint64_t minimum_balance_satoshis,
                                          uint64_t minimum_confirmations,
                                          std::chrono::seconds timeout,
                                          std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  uint64_t last_balance = 0;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      if (wallet_mode == WalletMode::kPublic) {
        boost::json::array params;
        params.emplace_back("*");
        params.push_back(minimum_confirmations);
        const boost::json::value balance =
            RpcCall(config, "getbalance", params, stop_token);
        last_balance = JsonFixed8Amount(balance, "getbalance");
      } else if (wallet_mode == WalletMode::kPrivate) {
        const boost::json::value balance = RpcCall(
            config, "getsparkbalance", boost::json::array{}, stop_token);
        if (!balance.is_object()) {
          throw std::runtime_error("Firo getsparkbalance returned non-object");
        }
        last_balance =
            JsonUint64Member(balance.as_object(), "availableBalance");
      } else {
        throw std::runtime_error("unknown Firo wallet mode");
      }
      if (last_balance >= minimum_balance_satoshis) {
        return last_balance;
      }
    } catch (const std::exception& e) {
      last_error = e.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Firo node " + config.id + " wallet balance " +
      FormatFixed8Amount(last_balance) + " remained below " +
      FormatFixed8Amount(minimum_balance_satoshis) + " with " +
      (wallet_mode == WalletMode::kPrivate
           ? std::string("driver-confirmed Spark availability")
           : std::to_string(minimum_confirmations) + " confirmations") +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

ChainWalletSnapshot FiroDriver::ReadWalletSnapshot(
    const FiroNodeConfig& config, WalletMode wallet_mode,
    std::uint32_t transaction_limit, std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  if (transaction_limit == 0U ||
      transaction_limit >
          static_cast<std::uint32_t>(std::numeric_limits<int>::max())) {
    throw std::runtime_error(
        "Firo wallet transaction limit must be in 1..INT_MAX");
  }

  const boost::json::value wallet_info =
      RpcCall(config, "getwalletinfo", boost::json::array{}, stop_token);
  if (!wallet_info.is_object()) {
    throw std::runtime_error("Firo getwalletinfo returned non-object");
  }
  const boost::json::object& info = wallet_info.as_object();
  ChainWalletSnapshot snapshot;
  if (wallet_mode == WalletMode::kPublic) {
    snapshot.available_balance_satoshis = JsonFixed8Member(info, "balance");
    snapshot.unconfirmed_balance_satoshis =
        JsonFixed8Member(info, "unconfirmed_balance");
    snapshot.immature_balance_satoshis =
        JsonFixed8Member(info, "immature_balance");
  } else if (wallet_mode == WalletMode::kPrivate) {
    const boost::json::value balance =
        RpcCall(config, "getsparkbalance", boost::json::array{}, stop_token);
    if (!balance.is_object()) {
      throw std::runtime_error("Firo getsparkbalance returned non-object");
    }
    snapshot.available_balance_satoshis =
        JsonUint64Member(balance.as_object(), "availableBalance");
    snapshot.unconfirmed_balance_satoshis =
        JsonUint64Member(balance.as_object(), "unconfirmedBalance");
  } else {
    throw std::runtime_error("unknown Firo wallet mode");
  }
  snapshot.transaction_count = JsonUint64Member(info, "txcount");

  boost::json::array list_params;
  list_params.emplace_back("*");
  list_params.push_back(transaction_limit);
  list_params.push_back(0);
  list_params.push_back(true);
  const boost::json::value transactions =
      RpcCall(config, "listtransactions", list_params, stop_token);
  if (!transactions.is_array()) {
    throw std::runtime_error("Firo listtransactions returned non-array");
  }
  snapshot.transaction_history_truncated =
      transactions.as_array().size() >= transaction_limit &&
      snapshot.transaction_count > transaction_limit;
  snapshot.transactions.reserve(transactions.as_array().size());
  for (const boost::json::value& value : transactions.as_array()) {
    if (!value.is_object()) {
      throw std::runtime_error(
          "Firo listtransactions returned a non-object entry");
    }
    const boost::json::object& object = value.as_object();
    ChainWalletTransaction transaction;
    const FiroWalletTransactionCategory category =
        ParseWalletTransactionCategory(JsonStringMember(object, "category"));
    transaction.direction = WalletTransactionDirection(category);
    transaction.txid = OptionalJsonStringMember(object, "txid");
    transaction.address = OptionalJsonStringMember(object, "address");
    const std::optional<std::int64_t> amount =
        OptionalJsonSignedFixed8Amount(object, "amount");
    if (!amount) {
      throw std::runtime_error(
          "Firo listtransactions entry has no amount field");
    }
    transaction.amount_satoshis = *amount;
    transaction.fee_satoshis = OptionalJsonSignedFixed8Amount(object, "fee");
    const boost::json::value* confirmations =
        object.if_contains("confirmations");
    if (confirmations != nullptr) {
      transaction.confirmations = JsonInt64Member(object, "confirmations");
    }
    transaction.block_hash = OptionalJsonStringMember(object, "blockhash");
    const boost::json::value* timestamp = object.if_contains("time");
    if (timestamp != nullptr) {
      transaction.timestamp = JsonUint64Member(object, "time");
    }
    transaction.abandoned = OptionalJsonBoolMember(object, "abandoned");
    snapshot.transactions.push_back(std::move(transaction));
  }
  return snapshot;
}

FiroUtxo FiroDriver::FindSpendableOutput(
    const FiroNodeConfig& config, const std::vector<std::string>& block_hashes,
    const std::string& source_address, uint64_t minimum_amount_satoshis,
    uint64_t minimum_confirmations, std::stop_token stop_token) const {
  constexpr uint32_t kMaxOutputsToProbe = 64;
  for (const std::string& block_hash : block_hashes) {
    ThrowIfStopRequested(stop_token);
    boost::json::array block_params;
    block_params.emplace_back(block_hash);
    block_params.push_back(1);
    const boost::json::value block =
        RpcCall(config, "getblock", block_params, stop_token);
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
        ThrowIfStopRequested(stop_token);
        boost::json::array txout_params;
        txout_params.emplace_back(txid);
        txout_params.push_back(vout);
        txout_params.push_back(false);
        const boost::json::value txout =
            RpcCall(config, "gettxout", txout_params, stop_token);
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
    uint64_t fee_satoshis, std::chrono::seconds timeout,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
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
      RpcCall(config, "createrawtransaction", create_params, stop_token);
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
      RpcCall(config, "signrawtransaction", sign_params, stop_token);
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
      RpcCall(config, "sendrawtransaction", send_params, stop_token);
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
  result.mempool_size =
      WaitForMempoolTransaction(config, txid, timeout, stop_token);
  return result;
}

FiroWalletTransactionResult FiroDriver::SendWalletTransaction(
    const FiroNodeConfig& config, WalletMode wallet_mode,
    const std::string& destination_address, uint64_t amount_satoshis,
    uint64_t fee_satoshis, std::chrono::seconds timeout,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  boost::json::array fee_params;
  fee_params.emplace_back(FormatFixed8Amount(fee_satoshis));
  const boost::json::value fee_result =
      RpcCall(config, "settxfee", fee_params, stop_token);
  if (!fee_result.is_bool() || !fee_result.as_bool()) {
    throw std::runtime_error("Firo settxfee did not return true");
  }

  std::vector<std::string> txids;
  if (wallet_mode == WalletMode::kPublic) {
    boost::json::array send_params;
    send_params.emplace_back(destination_address);
    send_params.emplace_back(FormatFixed8Amount(amount_satoshis));
    send_params.emplace_back("");
    send_params.emplace_back("");
    send_params.emplace_back(false);
    txids = ParseTxIdResult(
        RpcCall(config, "sendtoaddress", send_params, stop_token),
        "sendtoaddress");
  } else if (wallet_mode == WalletMode::kPrivate) {
    boost::json::object recipient;
    recipient["amount"] = FormatFixed8Amount(amount_satoshis);
    recipient["memo"] = "";
    recipient["subtractFee"] = false;
    boost::json::object recipients;
    recipients[destination_address] = std::move(recipient);
    boost::json::array send_params;
    send_params.emplace_back(std::move(recipients));
    txids = ParseTxIdResult(
        RpcCall(config, "spendspark", send_params, stop_token), "spendspark");
  } else {
    throw std::runtime_error("unknown Firo wallet mode");
  }

  FiroWalletTransactionResult result;
  result.txids = txids;
  result.destination_amount = FormatFixed8Amount(amount_satoshis);
  result.requested_fee_rate = FormatFixed8Amount(fee_satoshis);
  for (const std::string& txid : result.txids) {
    result.mempool_size =
        WaitForMempoolTransaction(config, txid, timeout, stop_token);
  }
  return result;
}

uint64_t FiroDriver::WaitForMempoolTransaction(
    const FiroNodeConfig& config, const std::string& txid,
    std::chrono::seconds timeout, std::stop_token stop_token) const {
  return WaitForTransaction(config, txid, timeout, stop_token).mempool_size;
}

ChainTransactionObservation FiroDriver::ObserveTransaction(
    const FiroNodeConfig& config, const std::string& txid,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  if (txid.empty()) {
    throw std::runtime_error("cannot observe an empty Firo transaction id");
  }

  const boost::json::value height_value =
      RpcCall(config, "getblockcount", boost::json::array{}, stop_token);
  std::uint64_t observed_height = 0;
  if (height_value.is_uint64()) {
    observed_height = height_value.as_uint64();
  } else if (height_value.is_int64() && height_value.as_int64() >= 0) {
    observed_height = static_cast<std::uint64_t>(height_value.as_int64());
  } else {
    throw std::runtime_error("Firo getblockcount returned a non-uint64 height");
  }

  const boost::json::value mempool =
      RpcCall(config, "getrawmempool", boost::json::array{}, stop_token);
  if (!mempool.is_array()) {
    throw std::runtime_error("Firo getrawmempool returned a non-array");
  }

  ChainTransactionObservation observation;
  observation.observed_height = observed_height;
  observation.mempool_size =
      static_cast<std::uint64_t>(mempool.as_array().size());

  boost::json::array params;
  params.emplace_back(txid);
  params.emplace_back(true);
  boost::json::value transaction;
  try {
    transaction = RpcCall(config, "getrawtransaction", params, stop_token);
  } catch (const FiroRpcError& error) {
    if (error.code() == -5) {
      return observation;
    }
    throw;
  }
  if (!transaction.is_object()) {
    throw std::runtime_error("Firo getrawtransaction returned a non-object");
  }
  const boost::json::object& object = transaction.as_object();
  if (JsonStringMember(object, "txid") != txid) {
    throw std::runtime_error(
        "Firo getrawtransaction returned a different transaction id");
  }

  const boost::json::value* block_hash = object.if_contains("blockhash");
  const boost::json::value* height = object.if_contains("height");
  const boost::json::value* confirmations = object.if_contains("confirmations");
  const bool has_confirmation_field =
      block_hash != nullptr || height != nullptr || confirmations != nullptr;
  if (!has_confirmation_field) {
    observation.state = ChainTransactionState::kMempool;
    return observation;
  }
  if (block_hash == nullptr || !block_hash->is_string() ||
      block_hash->as_string().empty()) {
    throw std::runtime_error(
        "Firo getrawtransaction returned an invalid block hash");
  }
  const std::uint64_t confirmation_height = JsonUint64Member(object, "height");
  const std::uint64_t confirmation_count =
      JsonUint64Member(object, "confirmations");
  if (confirmation_count == 0U) {
    throw std::runtime_error(
        "Firo getrawtransaction returned zero confirmations for a block");
  }
  if (confirmation_height >
      std::numeric_limits<std::uint64_t>::max() - (confirmation_count - 1U)) {
    throw std::runtime_error(
        "Firo transaction confirmation tip height overflow");
  }
  observation.state = ChainTransactionState::kConfirmed;
  observation.block_hash = std::string(block_hash->as_string());
  observation.confirmation_height = confirmation_height;
  observation.confirmations = confirmation_count;
  observation.observed_height =
      std::max(observation.observed_height,
               confirmation_height + confirmation_count - 1U);
  return observation;
}

ChainTransactionObservation FiroDriver::WaitForTransaction(
    const FiroNodeConfig& config, const std::string& txid,
    std::chrono::seconds timeout, std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  ChainTransactionObservation last_observation;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    last_observation = ObserveTransaction(config, txid, stop_token);
    if (last_observation.state != ChainTransactionState::kUnknown) {
      return last_observation;
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Firo node " + config.id + " did not report transaction " + txid +
      " in its mempool or confirmed chain before timeout; last height " +
      std::to_string(last_observation.observed_height) +
      ", last mempool size " + std::to_string(last_observation.mempool_size));
}

void FiroDriver::ConnectPeer(const FiroNodeConfig& config,
                             const std::string& address,
                             std::stop_token stop_token) const {
  if (ContainsPeerAddress(PeerAddresses(config, stop_token), address)) {
    return;
  }
  const std::string host = PeerHost(address);
  const boost::json::value bans =
      RpcCall(config, "listbanned", boost::json::array{}, stop_token);
  if (IsPeerHostBanned(bans, host)) {
    boost::json::array unban_params;
    unban_params.emplace_back(host);
    unban_params.emplace_back("remove");
    RpcCall(config, "setban", unban_params, stop_token);
  }
  boost::json::array params;
  params.emplace_back(address);
  params.emplace_back("onetry");
  RpcCall(config, "addnode", params, stop_token);
}

void FiroDriver::DisconnectPeer(const FiroNodeConfig& config,
                                const std::string& address,
                                std::stop_token stop_token) const {
  const std::string host = PeerHost(address);
  const boost::json::value bans =
      RpcCall(config, "listbanned", boost::json::array{}, stop_token);
  if (IsPeerHostBanned(bans, host)) {
    return;
  }
  boost::json::array params;
  params.emplace_back(host);
  params.emplace_back("add");
  RpcCall(config, "setban", params, stop_token);
}

void FiroDriver::ChangeLogVerbosity(const FiroNodeConfig& config,
                                    ChainLogVerbosityChange change,
                                    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  static_cast<void>(config);
  static_cast<void>(change);
  throw UnsupportedChainOperation("Firo", "runtime log verbosity adjustment");
}

void FiroDriver::SetMiningDifficulty(const FiroNodeConfig& config,
                                     MiningDifficulty difficulty,
                                     std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  static_cast<void>(config);
  static_cast<void>(difficulty);
  throw UnsupportedChainOperation("Firo regtest",
                                  "mining difficulty adjustment");
}

void FiroDriver::StartMining(const FiroNodeConfig& config,
                             const std::string& reward_address,
                             std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  static_cast<void>(config);
  static_cast<void>(reward_address);
  throw UnsupportedChainOperation("Firo regtest", "native continuous mining");
}

void FiroDriver::StopMining(const FiroNodeConfig& config,
                            std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  static_cast<void>(config);
  throw UnsupportedChainOperation("Firo regtest", "persistent mining stop");
}

void FiroDriver::SetNetworkActive(const FiroNodeConfig& config, bool active,
                                  std::stop_token stop_token) const {
  boost::json::array params;
  params.emplace_back(active);
  RpcCall(config, "setnetworkactive", params, stop_token);
}

void FiroDriver::Stop(const FiroNodeConfig& config,
                      std::stop_token stop_token) const {
  try {
    RpcCall(config, "stop", boost::json::array{}, stop_token);
  } catch (const SimulationCancelled&) {
    throw;
  } catch (const std::exception&) {
  }
}

boost::json::value FiroDriver::RpcCall(const FiroNodeConfig& config,
                                       std::string_view method,
                                       const boost::json::array& params,
                                       std::stop_token stop_token) const {
  boost::json::object request;
  request["jsonrpc"] = "1.0";
  request["id"] = "bbp";
  request["method"] = method;
  request["params"] = params;
  const std::string body = boost::json::serialize(request);
  HttpResponse response =
      http_.PostJson(Endpoint(config), "/", body, stop_token);
  if (response.status != 200) {
    try {
      static_cast<void>(ParseRpcResponse(response.body, method));
    } catch (const FiroRpcError&) {
      throw;
    } catch (const std::exception&) {
    }
    throw std::runtime_error("Firo RPC HTTP status " +
                             std::to_string(response.status) + " for " +
                             std::string(method) + ": " + response.body);
  }
  return ParseRpcResponse(response.body, method);
}

}  // namespace bbp
