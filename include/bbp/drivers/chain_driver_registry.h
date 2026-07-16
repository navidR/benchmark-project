#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver.h"

namespace bbp {

struct ChainDriverSpec {
  std::string name;
  std::string daemon_option_name;
  std::string daemon_scenario_field;
  std::string node_id_prefix;
  std::string default_reward_address;
  std::uint32_t max_nodes = 0;
  std::uint32_t coinbase_spendable_confirmations = 0;
  std::uint16_t p2p_port_base = 0;
  std::uint16_t rpc_port_base = 0;
};

struct ChainNodeConfigRequest {
  std::string run_id;
  std::filesystem::path run_root;
  std::filesystem::path daemon_binary;
  std::uint32_t node_index = 0;
  std::string node_id;
  bool wallet_enabled = false;
  std::vector<std::string> connect_peers;
};

const ChainDriverSpec& DefaultChainDriverSpec();
std::unique_ptr<ChainDriver> CreateDefaultChainDriver();
const ChainDriverSpec& ChainDriverSpecFor(ChainKind chain);
std::unique_ptr<ChainDriver> CreateChainDriver(ChainKind chain);
ChainNodeConfig MakeChainNodeConfig(const ChainDriverSpec& spec,
                                    const ChainNodeConfigRequest& request);

}  // namespace bbp
