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
  BOOST_TEST(static_cast<int>(config.network) ==
             static_cast<int>(bbp::ChainNetwork::kRegtest));
}

BOOST_AUTO_TEST_CASE(chain_node_config_retains_requested_network) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "network-test";
  request.run_root = "/tmp/network-test";
  request.daemon_binary = "/tmp/firod";
  request.network = bbp::ChainNetwork::kRegtest;

  const bbp::ChainNodeConfig config =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request);

  BOOST_TEST(static_cast<int>(config.network) ==
             static_cast<int>(bbp::ChainNetwork::kRegtest));
}

BOOST_AUTO_TEST_CASE(chain_node_config_retains_protected_extra_args) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "extra-args-test";
  request.run_root = "/tmp/extra-args-test";
  request.daemon_binary = "/tmp/firod";
  request.extra_args = bbp::ChainExtraArgs({"-dbcache=64"});

  const bbp::ChainNodeConfig config =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request);

  BOOST_REQUIRE_EQUAL(config.extra_args.arguments().size(), 1U);
  BOOST_TEST(config.extra_args.arguments()[0] == "-dbcache=64");
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

BOOST_AUTO_TEST_CASE(chain_node_config_accepts_only_owned_custom_data_dirs) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "custom-data";
  request.run_root = "/tmp/custom-data";
  request.daemon_binary = "/tmp/firod";
  request.node_id = "node-a";
  request.data_dir = "nodes/node-a/state.v1/chain";

  const bbp::ChainNodeConfig config =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request);
  BOOST_TEST(
      config.data_dir ==
      std::filesystem::path("/tmp/custom-data/nodes/node-a/state.v1/chain"));

  request.data_dir = "/tmp/outside";
  BOOST_CHECK_THROW(
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request),
      std::runtime_error);
  request.data_dir = "nodes/other/data";
  BOOST_CHECK_THROW(
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request),
      std::runtime_error);
  request.data_dir = "nodes/node-a/../escape";
  BOOST_CHECK_THROW(
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request),
      std::runtime_error);
  request.data_dir = std::filesystem::path("nodes") / "node-a";
  for (std::size_t index = 0U; index < 17U; ++index) {
    *request.data_dir /= std::string(64U, 'a');
  }
  BOOST_CHECK_THROW(
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request),
      std::runtime_error);
}
