#include "bbp/drivers/chain_driver_registry.h"

#include <chrono>
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

std::unique_ptr<ChainDriver> CreateDefaultChainDriver() {
  return std::make_unique<FiroDriver>(std::chrono::seconds(5));
}

ChainNodeConfig MakeChainNodeConfig(const ChainDriverSpec& spec,
                                    const ChainNodeConfigRequest& request) {
  const std::string node_id =
      spec.node_id_prefix + "-" + std::to_string(request.node_index + 1U);
  const std::filesystem::path node_root = request.run_root / "nodes" / node_id;

  ChainNodeConfig config;
  config.id = node_id;
  config.binary = request.daemon_binary;
  config.data_dir = node_root / "data";
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
