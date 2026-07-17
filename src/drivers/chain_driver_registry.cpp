#include "bbp/drivers/chain_driver_registry.h"

#include <chrono>
#include <cstddef>
#include <limits>
#include <stdexcept>

#include "bbp/drivers/firo_driver.h"

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
  };
  return spec;
}

const ChainDriverSpec& ChainDriverSpecFor(ChainKind chain) {
  if (chain == ChainKind::kFiro) {
    return DefaultChainDriverSpec();
  }
  throw std::runtime_error("chain driver is not implemented: " +
                           std::string(ChainKindName(chain)));
}

std::unique_ptr<ChainDriver> CreateDefaultChainDriver() {
  return std::make_unique<FiroDriver>(std::chrono::seconds(5));
}

std::unique_ptr<ChainDriver> CreateChainDriver(ChainKind chain) {
  if (chain == ChainKind::kFiro) {
    return CreateDefaultChainDriver();
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
  config.binary = request.daemon_binary;
  config.data_dir = ResolveNodeDataDirectory(request, node_id);
  config.log_dir = node_root;
  config.p2p_port = AddPortOffset(spec.p2p_port_base, request.node_index);
  config.rpc_port = AddPortOffset(spec.rpc_port_base, request.node_index);
  config.rpc_user = "sim-" + request.run_id;
  config.rpc_password =
      "pass-" + request.run_id + "-" + std::to_string(request.node_index);
  config.listen = true;
  config.wallet_enabled = request.wallet_enabled;
  config.connect_peers = request.connect_peers;
  return config;
}

}  // namespace bbp
