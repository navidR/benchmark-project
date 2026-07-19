#include "bbp/drivers/chain_driver_registry.h"

#include <sys/random.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <limits>
#include <stdexcept>

#include "bbp/drivers/bitcoin_driver.h"
#include "bbp/drivers/firo_driver.h"
#include "bbp/drivers/monero_driver.h"

namespace bbp {
namespace {

constexpr const char* kFiroChainName = "firo";
constexpr const char* kFiroDaemonOptionName = "firod";
constexpr const char* kFiroNodeIdPrefix = "firo";
constexpr const char* kFiroDefaultRewardAddress =
    "TTJW6FsYqLbSiF3ZUwMXRghgQuXK7XTodR";
constexpr std::uint32_t kFiroMaxNodes = 16;
constexpr std::uint32_t kFiroCoinbaseSpendableConfirmations = 101;
constexpr std::uint16_t kFiroP2pPortBase = 18168;
constexpr std::uint16_t kFiroRpcPortBase = 18888;
constexpr const char* kBitcoinChainName = "bitcoin";
constexpr const char* kBitcoinDaemonOptionName = "bitcoind";
constexpr const char* kBitcoinNodeIdPrefix = "bitcoin";
constexpr const char* kBitcoinDefaultRewardAddress =
    "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn";
constexpr std::uint32_t kBitcoinMaxNodes = 16;
constexpr std::uint32_t kBitcoinCoinbaseSpendableConfirmations = 101;
constexpr std::uint16_t kBitcoinP2pPortBase = 18444;
constexpr std::uint16_t kBitcoinRpcPortBase = 19443;
constexpr const char* kMoneroChainName = "monero";
constexpr const char* kMoneroDaemonOptionName = "monerod";
constexpr const char* kMoneroNodeIdPrefix = "monero";
constexpr const char* kMoneroDefaultRewardAddress =
    "42ey1afDFnn4886T7196doS9GPMzexD9gXpsZJDwVjeRVdFCSoHnv7KPbBeGpzJBzHRCAs9"
    "UxqeoyFQMYbqSWYTfJJQAWDm";
constexpr std::uint32_t kMoneroMaxNodes = 16;
constexpr std::uint32_t kMoneroCoinbaseSpendableConfirmations = 60;
constexpr std::uint16_t kMoneroP2pPortBase = 18080;
constexpr std::uint16_t kMoneroRpcPortBase = 19081;

std::string RandomCredentialHex() {
  std::array<unsigned char, 32U> bytes{};
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t count =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("getrandom for RPC credentials failed: " +
                               std::string(std::strerror(errno)));
    }
    if (count == 0) {
      throw std::runtime_error(
          "getrandom for RPC credentials made no progress");
    }
    offset += static_cast<std::size_t>(count);
  }
  constexpr char kHex[] = "0123456789abcdef";
  std::string output;
  output.reserve(bytes.size() * 2U);
  for (const unsigned char byte : bytes) {
    output.push_back(kHex[byte >> 4U]);
    output.push_back(kHex[byte & 0x0fU]);
  }
  return output;
}

std::uint16_t AddPortOffset(std::uint16_t base, std::uint32_t offset) {
  const std::uint32_t port = static_cast<std::uint32_t>(base) + offset;
  if (port > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error("chain node port allocation exceeded uint16");
  }
  return static_cast<std::uint16_t>(port);
}

bool IsSafeDataDirectoryComponent(std::string_view component) {
  if (component.empty() || component.size() > 64U || component == "." ||
      component == "..") {
    return false;
  }
  for (const char character : component) {
    const bool safe = (character >= 'a' && character <= 'z') ||
                      (character >= 'A' && character <= 'Z') ||
                      (character >= '0' && character <= '9') ||
                      character == '-' || character == '_' || character == '.';
    if (!safe) {
      return false;
    }
  }
  return true;
}

std::filesystem::path ResolveNodeDataDirectory(
    const ChainNodeConfigRequest& request, std::string_view node_id) {
  const std::filesystem::path default_relative =
      std::filesystem::path("nodes") / node_id / "data";
  if (!request.data_dir) {
    return request.run_root / default_relative;
  }
  const std::filesystem::path& relative = *request.data_dir;
  const std::string native = relative.string();
  if (native.empty() || native.size() > 1024U ||
      native.find('\0') != std::string::npos || relative.is_absolute() ||
      relative.has_root_path()) {
    throw std::runtime_error(
        "node data_dir must be a nonempty run-relative path");
  }
  std::vector<std::string> components;
  for (const std::filesystem::path& component : relative) {
    const std::string text = component.string();
    if (!IsSafeDataDirectoryComponent(text)) {
      throw std::runtime_error("node data_dir contains an unsafe component");
    }
    components.push_back(text);
  }
  if (components.size() < 3U || components[0] != "nodes" ||
      components[1] != node_id) {
    throw std::runtime_error(
        "node data_dir must be below its owned nodes/<id> directory");
  }
  return request.run_root / relative.lexically_normal();
}

}  // namespace

const ChainDriverSpec& DefaultChainDriverSpec() {
  static const ChainDriverSpec spec{
      .name = kFiroChainName,
      .daemon_option_name = kFiroDaemonOptionName,
      .daemon_scenario_field = kFiroDaemonOptionName,
      .node_id_prefix = kFiroNodeIdPrefix,
      .default_reward_address = kFiroDefaultRewardAddress,
      .max_nodes = kFiroMaxNodes,
      .coinbase_spendable_confirmations = kFiroCoinbaseSpendableConfirmations,
      .p2p_port_base = kFiroP2pPortBase,
      .rpc_port_base = kFiroRpcPortBase,
      .rpc_authentication = RpcAuthenticationMode::kCookieFile,
  };
  return spec;
}

const ChainDriverSpec& BitcoinChainDriverSpec() {
  static const ChainDriverSpec spec{
      .name = kBitcoinChainName,
      .daemon_option_name = kBitcoinDaemonOptionName,
      .daemon_scenario_field = kBitcoinDaemonOptionName,
      .node_id_prefix = kBitcoinNodeIdPrefix,
      .default_reward_address = kBitcoinDefaultRewardAddress,
      .max_nodes = kBitcoinMaxNodes,
      .coinbase_spendable_confirmations =
          kBitcoinCoinbaseSpendableConfirmations,
      .p2p_port_base = kBitcoinP2pPortBase,
      .rpc_port_base = kBitcoinRpcPortBase,
      .rpc_authentication = RpcAuthenticationMode::kCookieFile,
  };
  return spec;
}

const ChainDriverSpec& MoneroChainDriverSpec() {
  static const ChainDriverSpec spec{
      .name = kMoneroChainName,
      .daemon_option_name = kMoneroDaemonOptionName,
      .daemon_scenario_field = kMoneroDaemonOptionName,
      .node_id_prefix = kMoneroNodeIdPrefix,
      .default_reward_address = kMoneroDefaultRewardAddress,
      .max_nodes = kMoneroMaxNodes,
      .coinbase_spendable_confirmations = kMoneroCoinbaseSpendableConfirmations,
      .p2p_port_base = kMoneroP2pPortBase,
      .rpc_port_base = kMoneroRpcPortBase,
      .rpc_authentication = RpcAuthenticationMode::kDigest,
  };
  return spec;
}

const ChainDriverSpec& ChainDriverSpecFor(ChainKind chain) {
  switch (chain) {
    case ChainKind::kFiro:
      return DefaultChainDriverSpec();
    case ChainKind::kBitcoin:
      return BitcoinChainDriverSpec();
    case ChainKind::kMonero:
      return MoneroChainDriverSpec();
  }
  throw std::runtime_error("chain driver is not implemented: " +
                           std::string(ChainKindName(chain)));
}

std::unique_ptr<ChainDriver> CreateDefaultChainDriver() {
  return std::make_unique<FiroDriver>(std::chrono::seconds(5));
}

std::unique_ptr<ChainDriver> CreateChainDriver(ChainKind chain) {
  switch (chain) {
    case ChainKind::kFiro:
      return CreateDefaultChainDriver();
    case ChainKind::kBitcoin:
      return std::make_unique<BitcoinDriver>(std::chrono::seconds(5));
    case ChainKind::kMonero:
      return std::make_unique<MoneroDriver>(std::chrono::seconds(5));
  }
  throw std::runtime_error("chain driver is not implemented: " +
                           std::string(ChainKindName(chain)));
}

ChainNodeConfig MakeChainNodeConfig(const ChainDriverSpec& spec,
                                    const ChainNodeConfigRequest& request) {
  const std::string node_id =
      request.node_id.empty()
          ? spec.node_id_prefix + "-" + std::to_string(request.node_index + 1U)
          : request.node_id;
  const std::filesystem::path node_root = request.run_root / "nodes" / node_id;

  ChainNodeConfig config;
  config.id = node_id;
  config.network = request.network;
  config.extra_args = request.extra_args;
  config.binary = request.daemon_binary;
  config.data_dir = ResolveNodeDataDirectory(request, node_id);
  config.log_dir = node_root;
  config.p2p_port = AddPortOffset(spec.p2p_port_base, request.node_index);
  config.rpc_port = AddPortOffset(spec.rpc_port_base, request.node_index);
  config.rpc_authentication = spec.rpc_authentication;
  switch (spec.rpc_authentication) {
    case RpcAuthenticationMode::kCookieFile:
      config.rpc_cookie_file = node_root / ".bbp-rpc-cookie";
      break;
    case RpcAuthenticationMode::kDigest:
      config.rpc_user = "bbp-" + node_id;
      config.rpc_password = RandomCredentialHex();
      break;
    case RpcAuthenticationMode::kBasic:
      throw std::runtime_error(
          "registered chain driver cannot generate Basic RPC credentials");
  }
  config.listen = true;
  config.wallet_enabled = request.wallet_enabled;
  config.connect_peers = request.connect_peers;
  return config;
}

}  // namespace bbp
