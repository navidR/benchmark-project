#include <array>
#include <boost/test/unit_test.hpp>
#include <limits>
#include <stdexcept>

#include "bbp/chain_kind.h"
#include "bbp/drivers/bitcoin_driver.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/drivers/monero_driver.h"

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

BOOST_AUTO_TEST_CASE(later_chain_drivers_are_registered) {
  const bbp::ChainDriverSpec& bitcoin =
      bbp::ChainDriverSpecFor(bbp::ChainKind::kBitcoin);
  BOOST_TEST(bitcoin.name == "bitcoin");
  BOOST_TEST(bitcoin.daemon_option_name == "bitcoind");
  BOOST_TEST(bitcoin.p2p_port_base == 18444U);
  BOOST_TEST(bitcoin.rpc_port_base == 19443U);
  const std::unique_ptr<bbp::ChainDriver> driver =
      bbp::CreateChainDriver(bbp::ChainKind::kBitcoin);
  BOOST_REQUIRE(driver != nullptr);
  BOOST_CHECK(dynamic_cast<bbp::BitcoinDriver*>(driver.get()) != nullptr);

  const bbp::ChainDriverSpec& monero =
      bbp::ChainDriverSpecFor(bbp::ChainKind::kMonero);
  BOOST_TEST(monero.name == "monero");
  BOOST_TEST(monero.daemon_option_name == "monerod");
  BOOST_TEST(monero.coinbase_spendable_confirmations == 60U);
  BOOST_TEST(monero.p2p_port_base == 18080U);
  BOOST_TEST(monero.rpc_port_base == 19081U);
  BOOST_CHECK(monero.rpc_authentication == bbp::RpcAuthenticationMode::kDigest);
  const std::unique_ptr<bbp::ChainDriver> monero_driver =
      bbp::CreateChainDriver(bbp::ChainKind::kMonero);
  BOOST_REQUIRE(monero_driver != nullptr);
  BOOST_CHECK(dynamic_cast<bbp::MoneroDriver*>(monero_driver.get()) != nullptr);
}

BOOST_AUTO_TEST_CASE(chain_p2p_bind_defaults_match_daemon_argv_semantics) {
  bbp::ChainNodeConfig config;
  config.p2p_host = "127.0.0.1";
  BOOST_TEST(bbp::EffectiveP2pBindAddress(bbp::ChainKind::kFiro, config) ==
             "0.0.0.0");
  BOOST_TEST(bbp::EffectiveP2pBindAddress(bbp::ChainKind::kBitcoin, config) ==
             "0.0.0.0");
  BOOST_TEST(bbp::EffectiveP2pBindAddress(bbp::ChainKind::kMonero, config) ==
             "127.0.0.1");
  config.p2p_bind = "10.20.30.40";
  BOOST_TEST(bbp::EffectiveP2pBindAddress(bbp::ChainKind::kFiro, config) ==
             "10.20.30.40");
}

BOOST_AUTO_TEST_CASE(bitcoin_node_config_uses_chain_ports_and_prefix) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "bitcoin-test";
  request.run_root = "/tmp/bitcoin-test";
  request.daemon_binary = "/tmp/bitcoind";
  request.node_index = 2U;

  const bbp::ChainNodeConfig config = bbp::MakeChainNodeConfig(
      bbp::ChainDriverSpecFor(bbp::ChainKind::kBitcoin), request);

  BOOST_TEST(config.id == "bitcoin-3");
  BOOST_TEST(config.p2p_port == 18446U);
  BOOST_TEST(config.rpc_port == 19445U);
  BOOST_TEST(config.rpc_cookie_file ==
             std::filesystem::path(
                 "/tmp/bitcoin-test/nodes/bitcoin-3/.bbp-rpc-cookie"));
}

BOOST_AUTO_TEST_CASE(monero_node_config_uses_unique_digest_credentials) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "monero-test";
  request.run_root = "/tmp/monero-test";
  request.daemon_binary = "/tmp/monerod";
  request.node_index = 0U;

  const bbp::ChainNodeConfig first = bbp::MakeChainNodeConfig(
      bbp::ChainDriverSpecFor(bbp::ChainKind::kMonero), request);
  request.node_index = 1U;
  const bbp::ChainNodeConfig second = bbp::MakeChainNodeConfig(
      bbp::ChainDriverSpecFor(bbp::ChainKind::kMonero), request);

  BOOST_TEST(first.id == "monero-1");
  BOOST_TEST(first.p2p_port == 18080U);
  BOOST_TEST(first.rpc_port == 19081U);
  BOOST_CHECK(first.rpc_authentication == bbp::RpcAuthenticationMode::kDigest);
  BOOST_TEST(first.rpc_user == "bbp-monero-1");
  BOOST_TEST(first.rpc_password.size() == 64U);
  BOOST_TEST(first.rpc_cookie_file.empty());
  BOOST_TEST(second.rpc_user == "bbp-monero-2");
  BOOST_TEST(second.rpc_password.size() == 64U);
  BOOST_TEST(first.rpc_password != second.rpc_password);
  BOOST_TEST(second.rpc_cookie_file.empty());
}

BOOST_AUTO_TEST_CASE(isolated_chain_nodes_reuse_fixed_ports_at_high_indices) {
  const std::array<const bbp::ChainDriverSpec*, 3U> specs = {
      &bbp::DefaultChainDriverSpec(),
      &bbp::ChainDriverSpecFor(bbp::ChainKind::kBitcoin),
      &bbp::ChainDriverSpecFor(bbp::ChainKind::kMonero),
  };
  for (const bbp::ChainDriverSpec* spec : specs) {
    for (const std::uint32_t node_index :
         {0U, 262143U, std::numeric_limits<std::uint32_t>::max()}) {
      bbp::ChainNodeConfigRequest request;
      request.run_id = "port-plan-test";
      request.run_root = "/tmp/port-plan-test";
      request.daemon_binary = "/tmp/daemon";
      request.node_index = node_index;
      request.isolated_network = true;
      const bbp::ChainNodeConfig config =
          bbp::MakeChainNodeConfig(*spec, request);
      BOOST_TEST(config.p2p_port == spec->p2p_port_base);
      BOOST_TEST(config.rpc_port == spec->rpc_port_base);
      BOOST_TEST(
          config.id ==
          spec->node_id_prefix + "-" +
              std::to_string(static_cast<std::uint64_t>(node_index) + 1U));
    }
  }
}

BOOST_AUTO_TEST_CASE(loopback_chain_nodes_report_port_exhaustion_without_wrap) {
  const bbp::ChainDriverSpec& spec = bbp::DefaultChainDriverSpec();
  bbp::ChainNodeConfigRequest request;
  request.run_root = "/tmp/port-plan-test";
  request.daemon_binary = "/tmp/daemon";
  request.node_index =
      std::numeric_limits<std::uint16_t>::max() - spec.rpc_port_base;
  const bbp::ChainNodeConfig final_config =
      bbp::MakeChainNodeConfig(spec, request);
  BOOST_TEST(final_config.rpc_port == 65535U);

  ++request.node_index;
  BOOST_CHECK_EXCEPTION(
      bbp::MakeChainNodeConfig(spec, request), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()) ==
               "loopback port exhaustion: requested port 65536, available "
               "port range 1..65535";
      });
  request.node_index = std::numeric_limits<std::uint32_t>::max();
  BOOST_CHECK_EXCEPTION(
      bbp::MakeChainNodeConfig(spec, request), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find("loopback port exhaustion") !=
               std::string::npos;
      });
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
  BOOST_CHECK(config.rpc_authentication ==
              bbp::RpcAuthenticationMode::kCookieFile);
  BOOST_TEST(config.rpc_user.empty());
  BOOST_TEST(config.rpc_password.empty());
  BOOST_TEST(config.rpc_cookie_file ==
             std::filesystem::path(
                 "/tmp/profile-test/nodes/firo-wallet-a/.bbp-rpc-cookie"));
  BOOST_TEST(static_cast<int>(config.network) ==
             static_cast<int>(bbp::ChainNetwork::kRegtest));
}

BOOST_AUTO_TEST_CASE(chain_node_rpc_cookie_paths_are_node_scoped) {
  bbp::ChainNodeConfigRequest first_request;
  first_request.run_id = "same-run";
  first_request.run_root = "/tmp/same-run";
  first_request.daemon_binary = "/tmp/firod";
  first_request.node_index = 0U;
  bbp::ChainNodeConfigRequest second_request = first_request;
  second_request.node_index = 1U;

  const bbp::ChainNodeConfig first =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), first_request);
  const bbp::ChainNodeConfig second =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), second_request);

  BOOST_TEST(first.rpc_cookie_file != second.rpc_cookie_file);
  BOOST_TEST(first.rpc_user.empty());
  BOOST_TEST(first.rpc_password.empty());
  BOOST_TEST(second.rpc_user.empty());
  BOOST_TEST(second.rpc_password.empty());
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

BOOST_AUTO_TEST_CASE(chain_node_config_retains_wallet_enablement) {
  bbp::ChainNodeConfigRequest request;
  request.run_id = "wallet-config-test";
  request.run_root = "/tmp/wallet-config-test";
  request.daemon_binary = "/tmp/firod";
  request.wallet_enabled = true;

  const bbp::ChainNodeConfig config =
      bbp::MakeChainNodeConfig(bbp::DefaultChainDriverSpec(), request);

  BOOST_TEST(config.wallet_enabled);
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
