#include <boost/test/unit_test.hpp>
#include <stdexcept>

#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver_registry.h"

BOOST_AUTO_TEST_CASE(chain_kind_parses_supported_names) {
  BOOST_TEST(static_cast<int>(bbp::ParseChainKind("firo")) ==
             static_cast<int>(bbp::ChainKind::kFiro));
  BOOST_TEST(static_cast<int>(bbp::ParseChainKind("bitcoin")) ==
             static_cast<int>(bbp::ChainKind::kBitcoin));
  BOOST_TEST(static_cast<int>(bbp::ParseChainKind("monero")) ==
             static_cast<int>(bbp::ChainKind::kMonero));
}

BOOST_AUTO_TEST_CASE(chain_kind_rejects_unknown_name) {
  BOOST_CHECK_THROW(bbp::ParseChainKind("unknown"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(chain_kind_has_canonical_names) {
  BOOST_TEST(bbp::ChainKindName(bbp::ChainKind::kFiro) == "firo");
  BOOST_TEST(bbp::ChainKindName(bbp::ChainKind::kBitcoin) == "bitcoin");
  BOOST_TEST(bbp::ChainKindName(bbp::ChainKind::kMonero) == "monero");
}

BOOST_AUTO_TEST_CASE(unimplemented_chain_driver_is_explicitly_rejected) {
  BOOST_CHECK_THROW(bbp::ChainDriverSpecFor(bbp::ChainKind::kBitcoin),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::CreateChainDriver(bbp::ChainKind::kMonero),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(chain_node_config_uses_explicit_safe_scenario_id) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "profile-test";
  request.run_root = "/tmp/profile-test";
  request.daemon_binary = "/tmp/firod";
  request.node_index = 1U;
  request.node_id = "firo-wallet-a";

  const bbp::ChainNodeConfig config =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request);

  BOOST_TEST(config.id == "firo-wallet-a");
  BOOST_TEST(
      config.data_dir ==
      std::filesystem::path("/tmp/profile-test/nodes/firo-wallet-a/data"));
  BOOST_TEST(config.p2p_port == 18169U);
  BOOST_TEST(config.rpc_port == 18889U);
}

BOOST_AUTO_TEST_CASE(chain_node_config_retains_generated_id_by_default) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "legacy-test";
  request.run_root = "/tmp/legacy-test";
  request.daemon_binary = "/tmp/firod";
  request.node_index = 2U;

  const bbp::ChainNodeConfig config =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request);

  BOOST_TEST(config.id == "firo-3");
  BOOST_TEST(config.data_dir ==
             std::filesystem::path("/tmp/legacy-test/nodes/firo-3/data"));
}
