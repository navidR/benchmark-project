#include "bbp/drivers/monero_driver.h"

#include <algorithm>
#include <boost/asio/ip/address.hpp>
#include <boost/json/array.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/url/parse.hpp>
#include <charconv>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <limits>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/simulation_cancelled.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::uint32_t kMoneroPeerLimit = 16U;
constexpr std::uint32_t kPeerBanSeconds = 3600U;

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
  key += '=';
  key += value;
  return key;
}

bool HasControlCharacter(std::string_view value) {
  return std::any_of(value.begin(), value.end(), [](unsigned char character) {
    return character < 0x20U || character == 0x7fU;
  });
}

void ValidateDigestConfiguration(const ChainNodeConfig& config) {
  if (config.rpc_authentication != RpcAuthenticationMode::kDigest) {
    throw std::runtime_error(
        "Monero daemon launch requires Digest RPC authentication");
  }
  if (!config.rpc_cookie_file.empty()) {
    throw std::runtime_error(
        "Monero Digest authentication rejects an RPC cookie file");
  }
  if (config.rpc_user.empty() || config.rpc_user.size() > 256U ||
      config.rpc_password.empty() || config.rpc_password.size() > 256U ||
      config.rpc_user.find(':') != std::string::npos ||
      config.rpc_password.find(':') != std::string::npos ||
      HasControlCharacter(config.rpc_user) ||
      HasControlCharacter(config.rpc_password)) {
    throw std::runtime_error("Monero Digest credentials are missing or unsafe");
  }
}

std::uint64_t JsonUint64(const boost::json::object& object,
                         std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_uint64()) {
    return value->as_uint64();
  }
  if (value != nullptr && value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  throw std::runtime_error("Monero RPC field is not uint64: " +
                           std::string(field));
}

std::int64_t JsonInt64(const boost::json::object& object,
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
  throw std::runtime_error("Monero RPC field is not int64: " +
                           std::string(field));
}

bool JsonBool(const boost::json::object& object, std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_bool()) {
    throw std::runtime_error("Monero RPC field is not boolean: " +
                             std::string(field));
  }
  return value->as_bool();
}

std::string JsonString(const boost::json::object& object,
                       std::string_view field, std::size_t maximum_bytes) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string() || value->as_string().empty() ||
      value->as_string().size() > maximum_bytes ||
      HasControlCharacter(value->as_string())) {
    throw std::runtime_error("Monero RPC field is not a safe string: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

void RequireOkStatus(const boost::json::object& object,
                     std::string_view operation) {
  const std::string status = JsonString(object, "status", 256U);
  if (status != "OK") {
    throw std::runtime_error("Monero RPC " + std::string(operation) +
                             " returned status: " + status);
  }
}

bool IsHexCharacter(unsigned char character) {
  return (character >= '0' && character <= '9') ||
         (character >= 'a' && character <= 'f') ||
         (character >= 'A' && character <= 'F');
}

void ValidateHash(std::string_view hash, std::string_view description) {
  if (hash.size() != 64U ||
      !std::all_of(hash.begin(), hash.end(), IsHexCharacter)) {
    throw std::runtime_error("Monero " + std::string(description) +
                             " must be a 64-character hexadecimal hash");
  }
}

void ValidateWideDifficulty(std::string_view value,
                            std::string_view description) {
  if (value.size() < 3U || value.size() > 256U || value[0] != '0' ||
      (value[1] != 'x' && value[1] != 'X') ||
      !std::all_of(value.begin() + 2, value.end(), IsHexCharacter)) {
    throw std::runtime_error("Monero " + std::string(description) +
                             " is not bounded hexadecimal text");
  }
}

void ValidateMiningAddress(std::string_view address) {
  if (address.size() != 95U ||
      !std::all_of(address.begin(), address.end(), [](unsigned char character) {
        return (character >= '0' && character <= '9') ||
               (character >= 'a' && character <= 'z') ||
               (character >= 'A' && character <= 'Z');
      })) {
    throw std::runtime_error(
        "Monero mining address must be a 95-character base58 address");
  }
}

std::string PeerHost(const std::string& endpoint) {
  const std::string uri = "tcp://" + endpoint;
  const boost::system::result<boost::urls::url_view> parsed =
      boost::urls::parse_uri(uri);
  if (!parsed || parsed->scheme() != "tcp" || !parsed->has_port() ||
      parsed->has_userinfo() || !parsed->encoded_query().empty() ||
      !parsed->encoded_fragment().empty() ||
      (!parsed->encoded_path().empty() && parsed->encoded_path() != "/")) {
    throw std::runtime_error("invalid Monero peer endpoint: " + endpoint);
  }
  const std::string host(parsed->host());
  boost::system::error_code address_error;
  boost::asio::ip::make_address(host, address_error);
  if (host.empty() || address_error) {
    throw std::runtime_error(
        "Monero peer endpoint host is not an IP address: " + endpoint);
  }
  std::uint32_t port = 0U;
  const std::string_view port_text = parsed->port();
  const auto [end, error] = std::from_chars(
      port_text.data(), port_text.data() + port_text.size(), port);
  if (error != std::errc{} || end != port_text.data() + port_text.size() ||
      port == 0U || port > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error("Monero peer endpoint has an invalid port: " +
                             endpoint);
  }
  return host;
}

bool ContainsPeer(const std::vector<std::string>& addresses,
                  const std::string& expected) {
  const std::string expected_host = PeerHost(expected);
  return std::any_of(addresses.begin(), addresses.end(),
                     [&](const std::string& candidate) {
                       return PeerHost(candidate) == expected_host;
                     });
}

std::vector<std::string> ParsePeerAddresses(const boost::json::object& result,
                                            bool require_completed_handshake) {
  const boost::json::value* connections = result.if_contains("connections");
  if (connections == nullptr) {
    return {};
  }
  if (!connections->is_array()) {
    throw std::runtime_error(
        "Monero RPC get_connections returned no connections array");
  }
  std::vector<std::string> addresses;
  addresses.reserve(connections->as_array().size());
  std::set<std::string> exact_addresses;
  for (const boost::json::value& connection : connections->as_array()) {
    if (!connection.is_object()) {
      throw std::runtime_error(
          "Monero RPC get_connections returned a non-object entry");
    }
    std::string address = JsonString(connection.as_object(), "address", 512U);
    static_cast<void>(PeerHost(address));
    const std::string state = JsonString(connection.as_object(), "state", 32U);
    if (state != "before_handshake" && state != "synchronizing" &&
        state != "standby" && state != "normal") {
      throw std::runtime_error(
          "Monero RPC get_connections returned an unknown protocol state");
    }
    if (!exact_addresses.insert(address).second) {
      throw std::runtime_error(
          "Monero RPC get_connections returned a duplicate address");
    }
    if (!require_completed_handshake || state != "before_handshake") {
      addresses.push_back(std::move(address));
    }
  }
  return addresses;
}

std::vector<std::string> ParseHashArray(const boost::json::object& object,
                                        std::string_view field,
                                        std::string_view operation,
                                        bool missing_is_empty = false) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr && missing_is_empty) {
    return {};
  }
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("Monero RPC " + std::string(operation) +
                             " returned no " + std::string(field) + " array");
  }
  std::vector<std::string> hashes;
  hashes.reserve(value->as_array().size());
  std::set<std::string> unique_hashes;
  for (const boost::json::value& item : value->as_array()) {
    if (!item.is_string()) {
      throw std::runtime_error("Monero RPC " + std::string(operation) +
                               " returned a non-string hash");
    }
    std::string hash(item.as_string());
    ValidateHash(hash, "RPC hash");
    if (!unique_hashes.insert(hash).second) {
      throw std::runtime_error("Monero RPC " + std::string(operation) +
                               " returned a duplicate hash");
    }
    hashes.push_back(std::move(hash));
  }
  return hashes;
}

std::uint64_t CheckedPeerCount(const boost::json::object& info) {
  const std::uint64_t outgoing = JsonUint64(info, "outgoing_connections_count");
  const std::uint64_t incoming = JsonUint64(info, "incoming_connections_count");
  if (outgoing > std::numeric_limits<std::uint64_t>::max() - incoming) {
    throw std::runtime_error("Monero RPC peer count overflow");
  }
  return outgoing + incoming;
}

std::uint64_t CheckedElapsedMilliseconds(
    std::chrono::steady_clock::time_point start) {
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
  if (elapsed < 0) {
    throw std::runtime_error("Monero RPC monotonic latency became negative");
  }
  return static_cast<std::uint64_t>(elapsed);
}

[[noreturn]] void Unsupported(std::stop_token stop_token,
                              std::string_view operation) {
  ThrowIfStopRequested(stop_token);
  throw UnsupportedChainOperation("Monero", operation);
}

}  // namespace

MoneroDriver::MoneroDriver(std::chrono::milliseconds rpc_timeout)
    : http_(rpc_timeout) {}

ProcessSpec MoneroDriver::RenderProcess(const ChainNodeConfig& config) const {
  if (config.network != ChainNetwork::kRegtest) {
    throw std::runtime_error("Monero driver supports only regtest fakechain");
  }
  ValidateDigestConfiguration(config);
  const std::string p2p_bind =
      config.p2p_bind.empty() ? config.p2p_host : config.p2p_bind;
  if (p2p_bind.empty() || config.rpc_bind.empty()) {
    throw std::runtime_error("Monero daemon bind addresses must not be empty");
  }
  for (const std::string& peer : config.connect_peers) {
    static_cast<void>(PeerHost(peer));
  }
  EnsureDirectory(config.data_dir);
  EnsureDirectory(config.log_dir);

  ProcessSpec spec;
  spec.binary = config.binary;
  spec.cwd = config.data_dir;
  spec.stdout_path = config.log_dir / "stdout.log";
  spec.stderr_path = config.log_dir / "stderr.log";
  spec.environment = {
      {"RPC_LOGIN", config.rpc_user + ":" + config.rpc_password},
  };
  spec.argv = {
      "--regtest",
      "--keep-fakechain",
      "--fixed-difficulty=1",
      Arg("--data-dir", config.data_dir.string()),
      Arg("--log-file", (config.log_dir / "monerod.log").string()),
      "--non-interactive",
      "--no-igd",
      "--disable-dns-checkpoints",
      "--check-updates=disabled",
      "--rpc-ssl=disabled",
      "--no-zmq",
      Arg("--p2p-bind-ip", p2p_bind),
      Arg("--p2p-bind-port", std::to_string(config.p2p_port)),
      Arg("--rpc-bind-ip", config.rpc_bind),
      Arg("--rpc-bind-port", std::to_string(config.rpc_port)),
      "--confirm-external-bind",
      "--allow-local-ip",
      Arg("--max-connections-per-ip", std::to_string(kMoneroPeerLimit)),
      Arg("--out-peers", std::to_string(config.listen ? kMoneroPeerLimit : 0U)),
      Arg("--in-peers", std::to_string(config.listen ? kMoneroPeerLimit : 0U)),
  };
  for (const std::string& peer : config.connect_peers) {
    spec.argv.push_back(Arg("--add-exclusive-node", peer));
  }
  spec.argv.insert(spec.argv.end(), config.extra_args.arguments().begin(),
                   config.extra_args.arguments().end());
  return spec;
}

std::optional<LogTailChunk> MoneroDriver::ReadLogTail(
    const ChainNodeConfig& config, ChainLogSource source,
    const LogTailCursor& cursor, std::uint64_t max_bytes) const {
  std::filesystem::path path;
  switch (source) {
    case ChainLogSource::kDaemon:
      path = config.log_dir / "monerod.log";
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

RpcEndpoint MoneroDriver::Endpoint(const ChainNodeConfig& config) const {
  return RpcEndpoint{
      .host = config.rpc_host,
      .port = config.rpc_port,
      .authentication = config.rpc_authentication,
      .user = config.rpc_user,
      .password = config.rpc_password,
      .cookie_file = config.rpc_cookie_file,
  };
}

void MoneroDriver::WaitReady(const ChainNodeConfig& config,
                             std::chrono::seconds timeout,
                             std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      const boost::json::object info =
          JsonRpcCall(config, "get_info", {}, stop_token);
      if (JsonString(info, "nettype", 32U) != "fakechain") {
        throw std::runtime_error("Monero get_info did not report fakechain");
      }
      if (JsonUint64(info, "height") < 1U) {
        throw std::runtime_error("Monero fakechain height is below genesis");
      }
      return;
    } catch (const std::exception& error) {
      last_error = error.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error("Monero node " + config.id +
                           " did not become RPC-ready: " + last_error);
}

void MoneroDriver::WaitForHeight(const ChainNodeConfig& config,
                                 std::uint64_t height,
                                 std::chrono::seconds timeout,
                                 std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::uint64_t last_height = 0U;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      const boost::json::object info =
          JsonRpcCall(config, "get_info", {}, stop_token);
      last_height = JsonUint64(info, "height");
      if (last_height >= height) {
        return;
      }
    } catch (const std::exception& error) {
      last_error = error.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Monero node " + config.id + " reached height " +
      std::to_string(last_height) + " before timeout; target height " +
      std::to_string(height) + (last_error.empty() ? "" : ": " + last_error));
}

void MoneroDriver::WaitForPeerCount(const ChainNodeConfig& config,
                                    std::uint64_t peer_count,
                                    std::chrono::seconds timeout,
                                    std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::uint64_t last_peer_count = 0U;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      last_peer_count =
          HandshakeCompletePeerAddresses(config, stop_token).size();
      if (last_peer_count >= peer_count) {
        return;
      }
    } catch (const std::exception& error) {
      last_error = error.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error("Monero node " + config.id + " reached peer count " +
                           std::to_string(last_peer_count) +
                           " before timeout; target peer count " +
                           std::to_string(peer_count) +
                           (last_error.empty() ? "" : ": " + last_error));
}

void MoneroDriver::WaitForPeerAddress(const ChainNodeConfig& config,
                                      const std::string& address,
                                      std::chrono::seconds timeout,
                                      std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      if (ContainsPeer(HandshakeCompletePeerAddresses(config, stop_token),
                       address)) {
        return;
      }
    } catch (const std::exception& error) {
      last_error = error.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Monero node " + config.id + " did not connect to peer " + address +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

void MoneroDriver::WaitForPeerAddressAbsent(const ChainNodeConfig& config,
                                            const std::string& address,
                                            std::chrono::seconds timeout,
                                            std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error;
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    try {
      if (!ContainsPeer(PeerAddresses(config, stop_token), address)) {
        return;
      }
    } catch (const std::exception& error) {
      last_error = error.what();
    }
    WaitForNextPoll(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  throw std::runtime_error(
      "Monero node " + config.id + " remained connected to peer " + address +
      " before timeout" + (last_error.empty() ? "" : ": " + last_error));
}

ChainMetrics MoneroDriver::ReadMetrics(const ChainNodeConfig& config,
                                       std::stop_token stop_token) const {
  const auto started = std::chrono::steady_clock::now();
  const boost::json::object info =
      JsonRpcCall(config, "get_info", {}, stop_token);
  const boost::json::object connections =
      JsonRpcCall(config, "get_connections", {}, stop_token);
  const boost::json::object pool =
      PlainRpcCall(config, "/get_transaction_pool_stats", {}, stop_token);
  const boost::json::object version =
      JsonRpcCall(config, "get_version", {}, stop_token);
  const boost::json::object last_header =
      JsonRpcCall(config, "get_last_block_header", {}, stop_token);

  if (JsonString(info, "nettype", 32U) != "fakechain") {
    throw std::runtime_error("Monero get_info did not report fakechain");
  }
  const std::uint64_t height = JsonUint64(info, "height");
  if (height < 1U) {
    throw std::runtime_error("Monero fakechain height is below genesis");
  }
  const std::uint64_t target_height = JsonUint64(info, "target_height");
  const std::uint64_t peer_count = CheckedPeerCount(info);
  std::vector<std::string> peer_addresses =
      ParsePeerAddresses(connections, false);

  const boost::json::value* pool_stats_value = pool.if_contains("pool_stats");
  if (pool_stats_value == nullptr || !pool_stats_value->is_object()) {
    throw std::runtime_error(
        "Monero get_transaction_pool_stats returned no pool_stats object");
  }
  const boost::json::object& pool_stats = pool_stats_value->as_object();

  const boost::json::value* header_value =
      last_header.if_contains("block_header");
  if (header_value == nullptr || !header_value->is_object()) {
    throw std::runtime_error(
        "Monero get_last_block_header returned no block_header object");
  }
  const boost::json::object& header = header_value->as_object();

  ChainMetrics metrics;
  metrics.version = JsonUint64(version, "version");
  metrics.protocol_version = JsonUint64(header, "major_version");
  metrics.subversion = JsonString(info, "version", 256U);
  metrics.height = height;
  metrics.headers = std::max(height, target_height);
  metrics.best_hash = JsonString(info, "top_block_hash", 64U);
  ValidateHash(metrics.best_hash, "top block hash");
  metrics.peer_count = peer_count;
  metrics.peer_addresses = std::move(peer_addresses);
  metrics.mempool_tx_count = JsonUint64(pool_stats, "txs_total");
  metrics.mempool_bytes = JsonUint64(pool_stats, "bytes_total");
  const bool synchronized = JsonBool(info, "synchronized");
  const bool busy_syncing = JsonBool(info, "busy_syncing");
  metrics.initial_block_download = !synchronized;
  metrics.sync_status = synchronized && !busy_syncing
                            ? ChainSyncStatus::kSynced
                            : ChainSyncStatus::kSyncing;
  metrics.verification_progress =
      target_height == 0U || height >= target_height
          ? 1.0
          : static_cast<double>(height) / static_cast<double>(target_height);
  if (!std::isfinite(*metrics.verification_progress) ||
      *metrics.verification_progress < 0.0 ||
      *metrics.verification_progress > 1.0) {
    throw std::runtime_error(
        "Monero verification progress is outside finite 0..1");
  }
  const std::uint64_t difficulty = JsonUint64(info, "difficulty");
  metrics.difficulty = static_cast<double>(difficulty);
  const std::uint64_t target = JsonUint64(info, "target");
  if (target > 0U) {
    metrics.hashrate_estimate =
        static_cast<double>(difficulty) / static_cast<double>(target);
    if (!std::isfinite(*metrics.hashrate_estimate) ||
        *metrics.hashrate_estimate < 0.0) {
      throw std::runtime_error(
          "Monero hashrate estimate is not finite and non-negative");
    }
  }
  metrics.last_block_time = JsonUint64(header, "timestamp");
  metrics.chainwork = JsonString(info, "wide_cumulative_difficulty", 256U);
  ValidateWideDifficulty(*metrics.chainwork, "wide cumulative difficulty");
  metrics.rpc_latency_ms = CheckedElapsedMilliseconds(started);
  return metrics;
}

std::vector<std::string> MoneroDriver::PeerAddresses(
    const ChainNodeConfig& config, std::stop_token stop_token) const {
  return ParsePeerAddresses(
      JsonRpcCall(config, "get_connections", {}, stop_token), false);
}

std::vector<std::string> MoneroDriver::HandshakeCompletePeerAddresses(
    const ChainNodeConfig& config, std::stop_token stop_token) const {
  return ParsePeerAddresses(
      JsonRpcCall(config, "get_connections", {}, stop_token), true);
}

std::vector<std::string> MoneroDriver::ConnectedPeerAddresses(
    const ChainNodeConfig& config,
    const std::vector<std::string>& candidate_addresses,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  std::set<std::string> candidate_hosts;
  for (const std::string& candidate : candidate_addresses) {
    if (!candidate_hosts.insert(PeerHost(candidate)).second) {
      throw UnsupportedChainOperation(
          "Monero",
          "per-node peer identity without isolated networking; rerun with "
          "--isolate-network");
    }
  }
  const std::vector<std::string> reported =
      HandshakeCompletePeerAddresses(config, stop_token);
  std::vector<std::string> connected;
  connected.reserve(candidate_addresses.size());
  for (const std::string& candidate : candidate_addresses) {
    if (ContainsPeer(reported, candidate)) {
      connected.push_back(candidate);
    }
  }
  return connected;
}

std::vector<std::string> MoneroDriver::GenerateBlocks(
    const ChainNodeConfig& config, std::uint32_t count,
    const std::string& address, std::stop_token stop_token) const {
  if (count == 0U) {
    throw std::runtime_error("Monero block generation count must be positive");
  }
  ValidateMiningAddress(address);
  boost::json::object params;
  params["amount_of_blocks"] = count;
  params["wallet_address"] = address;
  params["prev_block"] = "";
  params["starting_nonce"] = 0;
  const boost::json::object result =
      JsonRpcCall(config, "generateblocks", params, stop_token);
  const std::uint64_t generated_height = JsonUint64(result, "height");
  std::vector<std::string> hashes =
      ParseHashArray(result, "blocks", "generateblocks");
  if (hashes.size() != count) {
    throw std::runtime_error("Monero RPC generateblocks returned " +
                             std::to_string(hashes.size()) + " hashes for " +
                             std::to_string(count) + " requested blocks");
  }

  boost::json::object header_params;
  header_params["hash"] = hashes.back();
  header_params["fill_pow_hash"] = false;
  const boost::json::object header_result = JsonRpcCall(
      config, "get_block_header_by_hash", header_params, stop_token);
  const boost::json::value* header = header_result.if_contains("block_header");
  if (header == nullptr || !header->is_object()) {
    throw std::runtime_error(
        "Monero get_block_header_by_hash returned no block header");
  }
  if (JsonString(header->as_object(), "hash", 64U) != hashes.back()) {
    throw std::runtime_error(
        "Monero generated block header returned a different hash");
  }
  if (JsonUint64(header->as_object(), "height") != generated_height) {
    throw std::runtime_error(
        "Monero generateblocks height differs from its final block header");
  }
  return hashes;
}

std::uint64_t MoneroDriver::ReadBlockNonRewardTransactionCount(
    const ChainNodeConfig& config, const std::string& block_hash,
    std::stop_token stop_token) const {
  ValidateHash(block_hash, "block hash");
  boost::json::object params;
  params["hash"] = block_hash;
  params["height"] = 0;
  params["fill_pow_hash"] = false;
  const boost::json::object result =
      JsonRpcCall(config, "get_block", params, stop_token);
  const std::vector<std::string> transactions =
      ParseHashArray(result, "tx_hashes", "get_block", true);
  return static_cast<std::uint64_t>(transactions.size());
}

std::string MoneroDriver::CreateWalletAddress(
    const ChainNodeConfig&, ChainWalletMode, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet initialization");
}

std::string MoneroDriver::CreateWalletFundingAddress(
    const ChainNodeConfig&, ChainWalletMode, const std::string&,
    std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet funding");
}

ChainWalletFundingResult MoneroDriver::PrepareWalletFunding(
    const ChainNodeConfig&, ChainWalletMode, const std::string&, std::uint64_t,
    std::uint64_t, std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet funding");
}

std::uint64_t MoneroDriver::WaitForWalletBalance(
    const ChainNodeConfig&, ChainWalletMode, std::uint64_t, std::uint64_t,
    std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet balance polling");
}

ChainWalletSnapshot MoneroDriver::ReadWalletSnapshot(
    const ChainNodeConfig&, ChainWalletMode, std::uint32_t,
    std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet metrics");
}

ChainUtxo MoneroDriver::FindSpendableOutput(const ChainNodeConfig&,
                                            const std::vector<std::string>&,
                                            const std::string&, std::uint64_t,
                                            std::uint64_t,
                                            std::stop_token stop_token) const {
  Unsupported(stop_token, "raw transaction funding");
}

ChainRawTransactionResult MoneroDriver::SendRawTransaction(
    const ChainNodeConfig&, const ChainUtxo&, const std::string&,
    const std::string&, const std::string&, std::uint64_t, std::uint64_t,
    std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "raw transaction submission");
}

ChainWalletTransactionResult MoneroDriver::SendWalletTransaction(
    const ChainNodeConfig&, ChainWalletMode, const std::string&, std::uint64_t,
    std::uint64_t, std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet transaction submission");
}

ChainTransactionObservation MoneroDriver::ObserveTransaction(
    const ChainNodeConfig& config, const std::string& txid,
    std::stop_token stop_token) const {
  ThrowIfStopRequested(stop_token);
  ValidateHash(txid, "transaction id");

  const boost::json::object pool =
      PlainRpcCall(config, "/get_transaction_pool_stats", {}, stop_token);
  const boost::json::value* pool_stats_value = pool.if_contains("pool_stats");
  if (pool_stats_value == nullptr || !pool_stats_value->is_object()) {
    throw std::runtime_error(
        "Monero get_transaction_pool_stats returned no pool_stats object");
  }

  boost::json::object transaction_request;
  boost::json::array requested_hashes;
  requested_hashes.emplace_back(txid);
  transaction_request["txs_hashes"] = std::move(requested_hashes);
  transaction_request["decode_as_json"] = false;
  transaction_request["prune"] = true;
  transaction_request["split"] = false;
  const boost::json::object transaction = PlainRpcCall(
      config, "/get_transactions", transaction_request, stop_token);
  const boost::json::object info =
      JsonRpcCall(config, "get_info", {}, stop_token);

  ChainTransactionObservation observation;
  observation.observed_height = JsonUint64(info, "height");
  observation.mempool_size =
      JsonUint64(pool_stats_value->as_object(), "txs_total");

  const boost::json::value* transactions = transaction.if_contains("txs");
  const boost::json::value* missed = transaction.if_contains("missed_tx");
  if ((transactions != nullptr && !transactions->is_array()) ||
      (missed != nullptr && !missed->is_array())) {
    throw std::runtime_error(
        "Monero get_transactions returned malformed transaction arrays");
  }
  const std::vector<std::string> missed_hashes =
      ParseHashArray(transaction, "missed_tx", "get_transactions", true);
  if (missed_hashes.size() > 1U ||
      (!missed_hashes.empty() && missed_hashes.front() != txid)) {
    throw std::runtime_error(
        "Monero get_transactions returned unexpected missed transactions");
  }
  const bool requested_hash_missed = !missed_hashes.empty();
  if (transactions == nullptr || transactions->as_array().empty()) {
    return observation;
  }
  if (requested_hash_missed || transactions->as_array().size() != 1U ||
      !transactions->as_array().front().is_object()) {
    throw std::runtime_error(
        "Monero get_transactions returned conflicting transaction data");
  }
  const boost::json::object& entry =
      transactions->as_array().front().as_object();
  if (JsonString(entry, "tx_hash", 64U) != txid) {
    throw std::runtime_error(
        "Monero get_transactions returned a different transaction id");
  }
  if (JsonBool(entry, "in_pool")) {
    observation.state = ChainTransactionState::kMempool;
    return observation;
  }

  const std::uint64_t block_height = JsonUint64(entry, "block_height");
  const std::uint64_t reported_confirmations =
      JsonUint64(entry, "confirmations");
  if (reported_confirmations == 0U ||
      observation.observed_height <= block_height) {
    throw std::runtime_error(
        "Monero confirmed transaction has invalid height metadata");
  }
  const std::uint64_t confirmations =
      observation.observed_height - block_height;
  if (reported_confirmations > confirmations) {
    throw std::runtime_error(
        "Monero transaction confirmations exceed the observed chain");
  }

  boost::json::object header_params;
  header_params["height"] = block_height;
  header_params["fill_pow_hash"] = false;
  const boost::json::object header_result = JsonRpcCall(
      config, "get_block_header_by_height", header_params, stop_token);
  const boost::json::value* header = header_result.if_contains("block_header");
  if (header == nullptr || !header->is_object()) {
    throw std::runtime_error(
        "Monero get_block_header_by_height returned no block header");
  }
  observation.block_hash = JsonString(header->as_object(), "hash", 64U);
  ValidateHash(observation.block_hash, "confirmation block hash");
  if (JsonUint64(header->as_object(), "height") != block_height) {
    throw std::runtime_error(
        "Monero confirmation block header returned a different height");
  }
  observation.state = ChainTransactionState::kConfirmed;
  observation.confirmation_height = block_height;
  observation.confirmations = confirmations;
  return observation;
}

ChainTransactionObservation MoneroDriver::WaitForTransaction(
    const ChainNodeConfig& config, const std::string& txid,
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
      "Monero node " + config.id + " did not report transaction " + txid +
      " in its pool or confirmed chain before timeout; last height " +
      std::to_string(last_observation.observed_height) + ", last pool size " +
      std::to_string(last_observation.mempool_size));
}

std::uint64_t MoneroDriver::WaitForMempoolTransaction(
    const ChainNodeConfig& config, const std::string& txid,
    std::chrono::seconds timeout, std::stop_token stop_token) const {
  return WaitForTransaction(config, txid, timeout, stop_token).mempool_size;
}

void MoneroDriver::ConnectPeer(const ChainNodeConfig& config,
                               const std::string& address,
                               std::stop_token stop_token) const {
  if (!ContainsPeer(config.connect_peers, address)) {
    throw UnsupportedChainOperation(
        "Monero", "dynamic peer connection outside configured startup peers");
  }
  if (ContainsPeer(PeerAddresses(config, stop_token), address)) {
    return;
  }
  SetPeerBan(config, PeerHost(address), false, stop_token);
}

void MoneroDriver::DisconnectPeer(const ChainNodeConfig& config,
                                  const std::string& address,
                                  std::stop_token stop_token) const {
  if (!ContainsPeer(PeerAddresses(config, stop_token), address)) {
    return;
  }
  SetPeerBan(config, PeerHost(address), true, stop_token);
}

void MoneroDriver::ChangeLogVerbosity(const ChainNodeConfig&,
                                      ChainLogVerbosityChange,
                                      std::stop_token stop_token) const {
  Unsupported(stop_token, "runtime log verbosity adjustment");
}

void MoneroDriver::SetMiningDifficulty(const ChainNodeConfig&, MiningDifficulty,
                                       std::stop_token stop_token) const {
  Unsupported(stop_token, "runtime fakechain mining difficulty adjustment");
}

void MoneroDriver::StartMining(const ChainNodeConfig& config,
                               const std::string& reward_address,
                               std::stop_token stop_token) const {
  ValidateMiningAddress(reward_address);
  boost::json::object request;
  request["miner_address"] = reward_address;
  request["threads_count"] = 1;
  request["do_background_mining"] = false;
  request["ignore_battery"] = true;
  static_cast<void>(PlainRpcCall(config, "/start_mining", request, stop_token));
}

void MoneroDriver::StopMining(const ChainNodeConfig& config,
                              std::stop_token stop_token) const {
  static_cast<void>(PlainRpcCall(config, "/stop_mining", {}, stop_token));
}

void MoneroDriver::SetNetworkActive(const ChainNodeConfig& config, bool active,
                                    std::stop_token stop_token) const {
  const std::uint32_t requested = active ? kMoneroPeerLimit : 0U;
  const auto peer_limit = [&](std::string_view path, std::string_view field,
                              bool set, std::uint32_t value,
                              std::stop_token operation_stop_token) {
    boost::json::object request;
    request["set"] = set;
    request[field] = value;
    const boost::json::object response =
        PlainRpcCall(config, path, request, operation_stop_token);
    const std::uint64_t returned = JsonUint64(response, field);
    if (returned > std::numeric_limits<std::uint32_t>::max()) {
      throw std::runtime_error("Monero RPC " + std::string(path) +
                               " returned an invalid peer limit");
    }
    if (set && returned != value) {
      throw std::runtime_error("Monero RPC " + std::string(path) +
                               " did not apply the requested peer limit");
    }
    return static_cast<std::uint32_t>(returned);
  };

  const std::uint32_t original_out =
      peer_limit("/out_peers", "out_peers", false, 0U, stop_token);
  const std::uint32_t original_in =
      peer_limit("/in_peers", "in_peers", false, 0U, stop_token);

  try {
    static_cast<void>(
        peer_limit("/out_peers", "out_peers", true, requested, stop_token));
    static_cast<void>(
        peer_limit("/in_peers", "in_peers", true, requested, stop_token));
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    std::string original_message = "unknown Monero peer-limit failure";
    try {
      std::rethrow_exception(original_error);
    } catch (const std::exception& error) {
      original_message = error.what();
    } catch (...) {
    }

    std::vector<std::string> rollback_failures;
    const auto restore = [&](std::string_view path, std::string_view field,
                             std::uint32_t value) {
      try {
        static_cast<void>(
            peer_limit(path, field, true, value, std::stop_token{}));
      } catch (const std::exception& error) {
        rollback_failures.push_back(std::string(path) + ": " + error.what());
      } catch (...) {
        rollback_failures.push_back(std::string(path) + ": unknown failure");
      }
    };
    restore("/in_peers", "in_peers", original_in);
    restore("/out_peers", "out_peers", original_out);
    if (rollback_failures.empty()) {
      std::rethrow_exception(original_error);
    }
    std::string combined =
        original_message + "; Monero peer-limit rollback failed";
    for (const std::string& failure : rollback_failures) {
      combined += "; " + failure;
    }
    throw std::runtime_error(combined);
  }
}

void MoneroDriver::Stop(const ChainNodeConfig& config,
                        std::stop_token stop_token) const {
  try {
    static_cast<void>(PlainRpcCall(config, "/stop_daemon", {}, stop_token));
  } catch (const SimulationCancelled&) {
    throw;
  } catch (const std::exception&) {
  }
}

void MoneroDriver::CleanupRpcCredentials(const ChainNodeConfig& config) const {
  ValidateDigestConfiguration(config);
}

boost::json::object MoneroDriver::JsonRpcCall(
    const ChainNodeConfig& config, std::string_view method,
    const boost::json::object& params, std::stop_token stop_token) const {
  boost::json::object request;
  request["jsonrpc"] = "2.0";
  request["id"] = "bbp";
  request["method"] = method;
  request["params"] = params;
  const HttpResponse response =
      http_.PostJson(Endpoint(config), "/json_rpc",
                     boost::json::serialize(request), stop_token);
  if (response.status != 200) {
    throw std::runtime_error("Monero RPC HTTP status " +
                             std::to_string(response.status) + " for " +
                             std::string(method));
  }
  const boost::json::value parsed = boost::json::parse(response.body);
  if (!parsed.is_object()) {
    throw std::runtime_error("Monero JSON-RPC " + std::string(method) +
                             " returned a non-object response");
  }
  const boost::json::object& envelope = parsed.as_object();
  const boost::json::value* error = envelope.if_contains("error");
  if (error != nullptr && !error->is_null()) {
    if (!error->is_object()) {
      throw std::runtime_error("Monero JSON-RPC " + std::string(method) +
                               " returned a malformed error");
    }
    const std::string message =
        JsonString(error->as_object(), "message", 1024U);
    static_cast<void>(JsonInt64(error->as_object(), "code"));
    throw std::runtime_error("Monero JSON-RPC " + std::string(method) +
                             " returned error: " + message);
  }
  const boost::json::value* result = envelope.if_contains("result");
  if (result == nullptr || !result->is_object()) {
    throw std::runtime_error("Monero JSON-RPC " + std::string(method) +
                             " returned no result object");
  }
  boost::json::object output = result->as_object();
  RequireOkStatus(output, method);
  return output;
}

boost::json::object MoneroDriver::PlainRpcCall(
    const ChainNodeConfig& config, std::string_view path,
    const boost::json::object& request, std::stop_token stop_token) const {
  const HttpResponse response = http_.PostJson(
      Endpoint(config), path, boost::json::serialize(request), stop_token);
  if (response.status != 200) {
    throw std::runtime_error("Monero RPC HTTP status " +
                             std::to_string(response.status) + " for " +
                             std::string(path));
  }
  const boost::json::value parsed = boost::json::parse(response.body);
  if (!parsed.is_object()) {
    throw std::runtime_error("Monero RPC " + std::string(path) +
                             " returned a non-object response");
  }
  boost::json::object output = parsed.as_object();
  RequireOkStatus(output, path);
  return output;
}

void MoneroDriver::SetPeerBan(const ChainNodeConfig& config,
                              const std::string& host, bool ban,
                              std::stop_token stop_token) const {
  boost::json::object ban_entry;
  ban_entry["host"] = host;
  ban_entry["ip"] = 0;
  ban_entry["ban"] = ban;
  ban_entry["seconds"] = ban ? kPeerBanSeconds : 0U;
  boost::json::array bans;
  bans.emplace_back(std::move(ban_entry));
  boost::json::object params;
  params["bans"] = std::move(bans);
  static_cast<void>(JsonRpcCall(config, "set_bans", params, stop_token));
}

}  // namespace bbp
