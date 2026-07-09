#include "benchmark_sim/firo_driver.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
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
      "-disablewallet",
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

std::vector<std::string> FiroDriver::GenerateBlocks(
    const FiroNodeConfig& config, uint32_t count,
    const std::string& address) const {
  boost::json::array params;
  params.push_back(count);
  params.emplace_back(address);
  return ParseStringArrayResult(RpcCall(config, "generatetoaddress", params));
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
