#include "bbp/drivers/monero_driver.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <condition_variable>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <sys/stat.h>
#include <unistd.h>

#include "bbp/simulation_cancelled.h"
#include "bbp/util.h"

namespace bbp {
namespace {

// BBP uses eight decimal places; Monero RPC uses twelve.
constexpr std::uint64_t kAtomicUnitsPerBbpUnit = 10000U;
constexpr std::string_view kWalletName = "bbp-wallet";

class WalletRpcError : public std::runtime_error {
 public:
  WalletRpcError(std::int64_t code, const std::string& message)
      : std::runtime_error(message), code(code) {}
  std::int64_t code;
};

std::uint64_t Uint(const boost::json::object& object, std::string_view field) {
  const auto* value = object.if_contains(field);
  if (value && value->is_uint64()) return value->as_uint64();
  if (value && value->is_int64() && value->as_int64() >= 0)
    return static_cast<std::uint64_t>(value->as_int64());
  throw std::runtime_error("Monero wallet field is not uint64: " + std::string(field));
}

bool Bool(const boost::json::object& object, std::string_view field) {
  const auto* value = object.if_contains(field);
  if (value && value->is_bool()) return value->as_bool();
  throw std::runtime_error("Monero wallet field is not boolean: " + std::string(field));
}

std::string String(const boost::json::object& object, std::string_view field) {
  const auto* value = object.if_contains(field);
  if (!value || !value->is_string() || value->as_string().size() > 1024U)
    throw std::runtime_error("Monero wallet field is not bounded text: " + std::string(field));
  return std::string(value->as_string());
}

std::uint64_t Atomic(std::uint64_t amount) {
  if (amount > std::numeric_limits<std::uint64_t>::max() / kAtomicUnitsPerBbpUnit)
    throw ChainTransactionRejected("Monero amount exceeds atomic-unit range");
  return amount * kAtomicUnitsPerBbpUnit;
}

std::string DaemonAddress(const ChainNodeConfig& config) {
  std::string host = config.rpc_bind;
  if (host.empty()) host = config.rpc_host;
  if (host == "0.0.0.0") host = "127.0.0.1";
  if (host == "::") host = "::1";
  if (host.find(':') != std::string::npos) host = "[" + host + "]";
  return "http://" + host + ":" + std::to_string(config.rpc_port);
}

void CheckStop(std::stop_token stop_token) {
  if (stop_token.stop_requested()) throw SimulationCancelled();
}

void Poll(std::stop_token stop_token) {
  CheckStop(stop_token);
  std::mutex mutex;
  std::unique_lock lock(mutex);
  std::condition_variable_any condition;
  condition.wait_for(lock, stop_token, std::chrono::milliseconds(250), [] { return false; });
  CheckStop(stop_token);
}

void CheckHash(std::string_view hash) {
  if (hash.size() != 64U || !std::all_of(hash.begin(), hash.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
      }))
    throw std::runtime_error("Monero wallet returned an invalid transaction hash");
}

const boost::json::array& Array(const boost::json::object& object, std::string_view field) {
  static const boost::json::array empty;
  const auto* value = object.if_contains(field);
  if (!value) return empty;
  if (!value->is_array()) throw std::runtime_error("Monero wallet field is not an array: " + std::string(field));
  return value->as_array();
}

std::uint64_t EligibleBalance(const boost::json::object& transfers,
                             std::uint64_t height, std::uint64_t confirmations) {
  std::uint64_t eligible = 0;
  for (const auto& value : Array(transfers, "transfers")) {
    const auto& output = value.as_object();
    const auto block = Uint(output, "block_height");
    if (!Bool(output, "unlocked") || Bool(output, "frozen") ||
        block >= height || height - block < confirmations) continue;
    const auto amount = Uint(output, "amount");
    if (amount > std::numeric_limits<std::uint64_t>::max() - eligible)
      throw std::runtime_error("Monero wallet output amount overflow");
    eligible += amount;
  }
  return eligible;
}

void WriteDaemonLogin(const std::filesystem::path& path, const ChainNodeConfig& config) {
  const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) throw std::runtime_error("cannot create Monero wallet daemon credentials");
  const std::string content = "daemon-login=" + config.rpc_user + ":" + config.rpc_password + "\n";
  int failure = fchmod(fd, 0600) == 0 ? 0 : errno;
  std::size_t offset = 0;
  while (failure == 0 && offset < content.size()) {
    const auto count = write(fd, content.data() + offset, content.size() - offset);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) { failure = count == 0 ? EIO : errno; break; }
    offset += static_cast<std::size_t>(count);
  }
  if (close(fd) != 0 && failure == 0) failure = errno;
  if (failure != 0) throw std::runtime_error("cannot write Monero wallet daemon credentials: " + std::string(std::strerror(failure)));
}

}  // namespace

std::vector<ProcessSpec> MoneroDriver::RenderCompanionProcesses(const ChainNodeConfig& config) const {
  if (!config.wallet_enabled) return {};
  if (config.network != ChainNetwork::kRegtest)
    throw std::runtime_error("Monero driver supports only regtest fakechain");
  ValidateDigestConfiguration(config);
  if (config.wallet_rpc_port == 0U || config.wallet_rpc_port == config.rpc_port ||
      config.wallet_rpc_port == config.p2p_port)
    throw std::runtime_error("Monero wallet RPC requires a distinct nonzero port");
  const auto wallet_dir = config.data_dir / "wallets";
  EnsureDirectory(wallet_dir);
  EnsureDirectory(config.log_dir);
  ProcessSpec process;
  process.binary = config.binary.parent_path() / "monero-wallet-rpc";
  RequireExecutable(process.binary);
  const auto credential_path = config.log_dir / ".bbp-wallet-secret.conf";
  WriteDaemonLogin(credential_path, config);
  process.cwd = wallet_dir;
  process.stdout_path = config.log_dir / "wallet-stdout.log";
  process.stderr_path = config.log_dir / "wallet-stderr.log";
  process.environment = {{"RPC_LOGIN", config.rpc_user + ":" + config.rpc_password}};
  process.argv = {
      "--wallet-dir=" + wallet_dir.string(),
      "--config-file=" + credential_path.string(),
      "--rpc-bind-ip=" + config.rpc_bind,
      "--rpc-bind-port=" + std::to_string(config.wallet_rpc_port),
      "--confirm-external-bind", "--rpc-ssl=disabled", "--daemon-ssl=disabled",
      "--daemon-address=" + DaemonAddress(config),
      "--shared-ringdb-dir=" + (wallet_dir / "ringdb").string(), "--no-dns",
      "--trusted-daemon", "--non-interactive", "--no-initial-sync",
      "--allow-mismatched-daemon-version",
      "--log-file=" + (config.log_dir / "wallet-rpc.log").string()};
  return {std::move(process)};
}

bool MoneroDriver::SupportsWalletTransactionMode(ChainWalletMode) const {
  // Both generic modes use Monero's native private transfers.
  return true;
}

std::uint64_t MoneroDriver::WalletTransactionFeeReserveSatoshis(
    ChainWalletMode, std::uint64_t requested) const {
  return std::max(requested, std::uint64_t{1000000U});
}

boost::json::object MoneroDriver::WalletRpcCall(
    const ChainNodeConfig& config, std::string_view method,
    const boost::json::object& params, std::stop_token stop_token,
    std::optional<std::chrono::steady_clock::time_point> deadline) const {
  CheckStop(stop_token);
  if (!config.wallet_enabled || config.wallet_rpc_port == 0U)
    throw UnsupportedChainOperation("Monero", "wallet RPC on a daemon-only node");
  auto endpoint = Endpoint(config);
  endpoint.port = config.wallet_rpc_port;
  const boost::json::object request{{"jsonrpc", "2.0"}, {"id", "bbp"},
                                   {"method", method}, {"params", params}};
  HttpResponse response;
  try {
    response = wallet_http_.PostJsonUntil(endpoint, "/json_rpc",
        boost::json::serialize(request),
        deadline.value_or(std::chrono::steady_clock::now() + std::chrono::seconds(60)), stop_token);
  } catch (const SimulationCancelled&) {
    throw;
  } catch (const std::exception& error) {
    throw ChainTransactionTransportFailure("Monero wallet RPC " + std::string(method) + ": " + error.what());
  }
  if (response.status != 200)
    throw ChainTransactionTransportFailure("Monero wallet RPC HTTP status " + std::to_string(response.status));
  boost::system::error_code parse_error;
  const auto parsed = boost::json::parse(response.body, parse_error);
  if (parse_error) throw ChainTransactionInternalRpcFailure("Monero wallet returned invalid JSON");
  if (!parsed.is_object()) throw ChainTransactionInternalRpcFailure("Monero wallet returned a non-object envelope");
  const auto& envelope = parsed.as_object();
  if (const auto* error = envelope.if_contains("error"); error && !error->is_null()) {
    if (!error->is_object()) throw ChainTransactionInternalRpcFailure("Monero wallet returned a malformed error");
    const auto& object = error->as_object();
    const auto* code = object.if_contains("code");
    if (!code || !code->is_int64()) throw ChainTransactionInternalRpcFailure("Monero wallet returned a malformed error code");
    const std::string message = "Monero wallet RPC " + std::string(method) + ": " + String(object, "message");
    if (method == "transfer") {
      // Validation/funding errors mean no transfer was relayed. Other failures
      // may happen after relay and must not be treated as safe to retry.
      switch (code->as_int64()) {
        case -2: case -5: case -6: case -12: case -14: case -15:
        case -16: case -17: case -18: case -19: case -20: case -37: case -46: case -53:
          throw ChainTransactionRejected(message);
        default: throw ChainTransactionInternalRpcFailure(message);
      }
    }
    throw WalletRpcError(code->as_int64(), message);
  }
  const auto* result = envelope.if_contains("result");
  if (!result || !result->is_object()) throw ChainTransactionInternalRpcFailure("Monero wallet returned no result object");
  return result->as_object();
}

void MoneroDriver::InitializeWallet(const ChainNodeConfig& config,
                                  std::chrono::steady_clock::time_point deadline,
                                  std::stop_token stop_token) const {
  static_cast<void>(WalletRpcCall(config, "get_version", {}, stop_token, deadline));
  // Opening only when no wallet is loaded also makes readiness retries safe.
  try {
    static_cast<void>(WalletRpcCall(config, "get_address", {{"account_index", 0}}, stop_token, deadline));
  } catch (const ChainTransactionTransportFailure&) {
    throw;
  } catch (const SimulationCancelled&) {
    throw;
  } catch (const WalletRpcError& error) {
    if (error.code != -13) throw;
    const bool exists = std::filesystem::exists(config.data_dir / "wallets" / "bbp-wallet.keys");
    boost::json::object params{{"filename", kWalletName}, {"password", ""}};
    if (!exists) params["language"] = "English";
    static_cast<void>(WalletRpcCall(config, exists ? "open_wallet" : "create_wallet", params, stop_token, deadline));
  }
  static_cast<void>(WalletRpcCall(config, "set_daemon",
      {{"address", DaemonAddress(config)},
       {"username", config.rpc_user}, {"password", config.rpc_password},
       {"trusted", true}, {"ssl_support", "disabled"}}, stop_token, deadline));
  static_cast<void>(WalletRpcCall(config, "auto_refresh", {{"enable", true}, {"period", 1}}, stop_token, deadline));
}

std::string MoneroDriver::CreateWalletAddress(const ChainNodeConfig& config,
    ChainWalletMode, std::stop_token stop_token) const {
  return String(WalletRpcCall(config, "create_address", {{"account_index", 0}, {"label", "bbp"}}, stop_token), "address");
}

bool MoneroDriver::ValidateTargetAddress(const ChainNodeConfig& config,
    const std::string& address, std::stop_token stop_token) const {
  if (address.empty() || address.size() > 106U) { CheckStop(stop_token); return false; }
  const auto result = WalletRpcCall(config, "validate_address",
      {{"address", address}, {"any_net_type", true}, {"allow_openalias", false}}, stop_token);
  return Bool(result, "valid") && String(result, "nettype") == "mainnet";
}

std::string MoneroDriver::CreateWalletFundingAddress(const ChainNodeConfig& config,
    ChainWalletMode, const std::string&, std::stop_token stop_token) const {
  // Coinbase rewards need the primary address, not a subaddress.
  return String(WalletRpcCall(config, "get_address", {{"account_index", 0}}, stop_token), "address");
}

ChainWalletFundingResult MoneroDriver::PrepareWalletFunding(const ChainNodeConfig& config,
    ChainWalletMode, const std::string&, std::uint64_t minimum_balance_satoshis,
    std::uint64_t minimum_confirmations, std::chrono::seconds timeout, std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  static_cast<void>(WalletRpcCall(config, "refresh", {}, stop_token, deadline));
  const auto balance = WalletRpcCall(config, "get_balance", {{"account_index", 0}}, stop_token, deadline);
  const auto height = Uint(WalletRpcCall(config, "get_height", {}, stop_token, deadline), "height");
  const auto transfers = WalletRpcCall(config, "incoming_transfers",
      {{"transfer_type", "available"}, {"account_index", 0}}, stop_token, deadline);
  ChainWalletFundingResult result;
  if (std::min(Uint(balance, "unlocked_balance"),
               EligibleBalance(transfers, height, minimum_confirmations)) <
      Atomic(minimum_balance_satoshis)) {
    const auto blocks = Uint(balance, "blocks_to_unlock");
    const auto additional = std::max(blocks, minimum_confirmations);
    if (additional > std::numeric_limits<std::uint64_t>::max() - height)
      throw std::runtime_error("Monero funding unlock height overflow");
    result.minimum_chain_height = height + additional;
  }
  return result;
}

std::uint64_t MoneroDriver::WaitForWalletBalance(const ChainNodeConfig& config,
    ChainWalletMode, std::uint64_t minimum_balance_satoshis,
    std::uint64_t minimum_confirmations, std::chrono::seconds timeout,
    std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::uint64_t available = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    static_cast<void>(WalletRpcCall(config, "refresh", {}, stop_token, deadline));
    const auto balance = WalletRpcCall(config, "get_balance", {{"account_index", 0}}, stop_token, deadline);
    const auto height = Uint(WalletRpcCall(config, "get_height", {}, stop_token, deadline), "height");
    const auto transfers = WalletRpcCall(config, "incoming_transfers",
        {{"transfer_type", "available"}, {"account_index", 0}}, stop_token, deadline);
    const auto eligible = EligibleBalance(transfers, height, minimum_confirmations);
    available = std::min(eligible, Uint(balance, "unlocked_balance")) / kAtomicUnitsPerBbpUnit;
    if (available >= minimum_balance_satoshis) return available;
    Poll(stop_token);
  }
  CheckStop(stop_token);
  throw ChainTransactionTimedOut("Monero wallet balance timed out; available=" + FormatFixed8Amount(available));
}

ChainWalletTransactionResult MoneroDriver::SubmitWalletTransaction(const ChainNodeConfig& config,
    ChainWalletMode, const std::string& destination_address, std::uint64_t amount_satoshis,
    std::uint64_t, std::chrono::seconds timeout, std::stop_token stop_token) const {
  CheckStop(stop_token);
  if (amount_satoshis == 0U) throw ChainTransactionRejected("Monero transfer amount must be positive");
  boost::json::array destinations;
  destinations.emplace_back(boost::json::object{{"address", destination_address}, {"amount", Atomic(amount_satoshis)}});
  const auto result = WalletRpcCall(config, "transfer",
      {{"destinations", std::move(destinations)}, {"account_index", 0},
       {"priority", 1}, {"unlock_time", 0}, {"do_not_relay", false}},
      stop_token, std::chrono::steady_clock::now() + timeout);
  ChainWalletTransactionResult transaction;
  try {
    const auto hash = String(result, "tx_hash");
    CheckHash(hash);
    if (Uint(result, "amount") != Atomic(amount_satoshis))
      throw std::runtime_error("Monero transfer returned an unexpected amount");
    transaction.txids.push_back(hash);
  } catch (const std::exception& error) {
    throw ChainTransactionInternalRpcFailure(error.what());
  }
  transaction.destination_amount = FormatFixed8Amount(amount_satoshis);
  // Monero computes fees internally; there is no Bitcoin-style fee-rate override.
  return transaction;
}

ChainWalletTransactionResult MoneroDriver::SendWalletTransaction(const ChainNodeConfig& config,
    ChainWalletMode mode, const std::string& destination_address, std::uint64_t amount_satoshis,
    std::uint64_t fee_satoshis, std::chrono::seconds timeout, std::stop_token stop_token) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  auto result = SubmitWalletTransaction(config, mode, destination_address, amount_satoshis, fee_satoshis, timeout, stop_token);
  for (const auto& txid : result.txids) {
    const auto remaining = std::chrono::duration_cast<std::chrono::seconds>(deadline - std::chrono::steady_clock::now());
    if (remaining <= std::chrono::seconds::zero()) throw ChainTransactionTimedOut("Monero transfer visibility timed out after relay");
    result.mempool_size = WaitForMempoolTransaction(config, txid, remaining, stop_token);
  }
  return result;
}

ChainWalletSnapshot MoneroDriver::ReadWalletSnapshot(const ChainNodeConfig& config,
    ChainWalletMode, std::uint32_t transaction_limit, std::stop_token stop_token) const {
  const auto balance = WalletRpcCall(config, "get_balance", {{"account_index", 0}}, stop_token);
  const auto transfers = WalletRpcCall(config, "get_transfers",
      {{"in", true}, {"out", true}, {"pending", true}, {"failed", true},
       {"pool", true}, {"account_index", 0}}, stop_token);
  ChainWalletSnapshot snapshot;
  const auto total = Uint(balance, "balance");
  const auto unlocked = Uint(balance, "unlocked_balance");
  if (unlocked > total) throw std::runtime_error("Monero unlocked balance exceeds total balance");
  snapshot.available_balance_satoshis = unlocked / kAtomicUnitsPerBbpUnit;
  std::uint64_t unconfirmed = 0;
  for (const std::string_view category : {"in", "out", "pending", "failed", "pool"}) {
    for (const auto& value : Array(transfers, category)) {
      const auto& entry = value.as_object();
      ChainWalletTransaction transaction;
      transaction.txid = String(entry, "txid");
      CheckHash(transaction.txid);
      transaction.address = String(entry, "address");
      const auto units = Uint(entry, "amount") / kAtomicUnitsPerBbpUnit;
      transaction.amount_satoshis = static_cast<std::int64_t>(units);
      const bool outgoing = category == "out" || category == "pending" || category == "failed";
      transaction.direction = outgoing ? ChainWalletTransactionDirection::kOutgoing : ChainWalletTransactionDirection::kIncoming;
      if (outgoing) {
        const auto& destinations = Array(entry, "destinations");
        if (destinations.size() == 1U)
          transaction.address = String(destinations.front().as_object(), "address");
        transaction.amount_satoshis = -transaction.amount_satoshis;
        const auto fee = Uint(entry, "fee");
        transaction.fee_satoshis = -static_cast<std::int64_t>(fee / kAtomicUnitsPerBbpUnit + (fee % kAtomicUnitsPerBbpUnit != 0U));
      }
      const auto confirmations = entry.if_contains("confirmations") ? Uint(entry, "confirmations") : 0U;
      if (confirmations > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        throw std::runtime_error("Monero wallet confirmations exceed int64");
      transaction.confirmations = static_cast<std::int64_t>(confirmations);
      transaction.timestamp = Uint(entry, "timestamp");
      transaction.abandoned = category == "failed";
      if (category == "pool" || category == "pending") {
        const auto pending_amount = category == "pool" ? Uint(entry, "amount") :
            (entry.if_contains("change_amount") ? Uint(entry, "change_amount") : 0U);
        if (pending_amount > std::numeric_limits<std::uint64_t>::max() - unconfirmed)
          throw std::runtime_error("Monero unconfirmed balance overflow");
        unconfirmed += pending_amount;
      }
      snapshot.transactions.push_back(std::move(transaction));
    }
  }
  // Native balance includes pool receipts and pending change. Do not count
  // those twice; refresh between the two RPC calls can change their category.
  unconfirmed = std::min(total - unlocked, unconfirmed);
  snapshot.unconfirmed_balance_satoshis = unconfirmed / kAtomicUnitsPerBbpUnit;
  snapshot.immature_balance_satoshis =
      (total - unlocked - unconfirmed) / kAtomicUnitsPerBbpUnit;
  snapshot.transaction_count = snapshot.transactions.size();
  std::sort(snapshot.transactions.begin(), snapshot.transactions.end(), [](const auto& a, const auto& b) {
    if (a.timestamp != b.timestamp) return a.timestamp > b.timestamp;
    return a.txid < b.txid;
  });
  if (snapshot.transactions.size() > transaction_limit) {
    snapshot.transactions.resize(transaction_limit);
    snapshot.transaction_history_truncated = true;
  }
  return snapshot;
}

}  // namespace bbp
