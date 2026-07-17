#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>

#include "bbp/drivers/bitcoin_driver.h"

namespace {

bool HasArgument(const bbp::ProcessSpec& process, const std::string& expected) {
  return std::find(process.argv.begin(), process.argv.end(), expected) !=
         process.argv.end();
}

std::filesystem::path TestRoot() {
  return std::filesystem::temp_directory_path() /
         ("bbp-bitcoin-driver-" + std::to_string(getpid()));
}

bbp::ChainNodeConfig TestConfig() {
  const std::filesystem::path root = TestRoot();
  bbp::ChainNodeConfig config;
  config.id = "bitcoin-1";
  config.binary = "/opt/bitcoin/bin/bitcoind";
  config.data_dir = root / "data";
  config.log_dir = root;
  config.p2p_port = 18444U;
  config.rpc_port = 18443U;
  config.rpc_authentication = bbp::RpcAuthenticationMode::kCookieFile;
  config.rpc_cookie_file = root / ".bbp-rpc-cookie";
  config.rpc_host = "10.210.1.2";
  config.rpc_bind = "10.210.1.2";
  config.rpc_allow_ips = {"10.210.1.1"};
  config.p2p_bind = "10.210.1.2";
  config.wallet_enabled = false;
  return config;
}

}  // namespace

BOOST_AUTO_TEST_CASE(bitcoin_driver_renders_owned_regtest_process) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  bbp::BitcoinDriver driver(std::chrono::seconds(1));
  const bbp::ProcessSpec process = driver.RenderProcess(TestConfig());

  BOOST_TEST(process.binary ==
             std::filesystem::path("/opt/bitcoin/bin/bitcoind"));
  BOOST_TEST(process.cwd == root / "data");
  BOOST_TEST(process.stdout_path == root / "stdout.log");
  BOOST_TEST(process.stderr_path == root / "stderr.log");
  BOOST_CHECK(HasArgument(process, "-regtest"));
  BOOST_CHECK(HasArgument(process, "-server=1"));
  BOOST_CHECK(HasArgument(
      process, "-rpccookiefile=" + (root / ".bbp-rpc-cookie").string()));
  BOOST_CHECK(HasArgument(process, "-rpccookieperms=owner"));
  BOOST_CHECK(HasArgument(process, "-rpcbind=10.210.1.2"));
  BOOST_CHECK(HasArgument(process, "-rpcallowip=10.210.1.1"));
  BOOST_CHECK(HasArgument(process, "-rpcport=18443"));
  BOOST_CHECK(HasArgument(process, "-bind=10.210.1.2"));
  BOOST_CHECK(HasArgument(process, "-port=18444"));
  BOOST_CHECK(HasArgument(process, "-disablewallet=1"));
  BOOST_CHECK(HasArgument(process, "-txindex=1"));
  BOOST_CHECK(HasArgument(process, "-dnsseed=0"));
  BOOST_CHECK(HasArgument(process, "-fixedseeds=0"));
  BOOST_CHECK(HasArgument(process, "-discover=0"));
  BOOST_CHECK(HasArgument(process, "-listenonion=0"));
  BOOST_CHECK(HasArgument(process, "-natpmp=0"));

  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(bitcoin_driver_rejects_inline_rpc_credentials) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  bbp::ChainNodeConfig config = TestConfig();
  config.rpc_user = "unsafe";
  config.rpc_password = "unsafe";
  bbp::BitcoinDriver driver(std::chrono::seconds(1));
  BOOST_CHECK_EXCEPTION(driver.RenderProcess(config), std::runtime_error,
                        [](const std::runtime_error& error) {
                          const std::string message = error.what();
                          return message.find("Bitcoin Core") !=
                                     std::string::npos &&
                                 message.find("Firo") == std::string::npos;
                        });
  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(bitcoin_driver_reports_unsupported_wallet_boundary) {
  bbp::BitcoinDriver driver(std::chrono::seconds(1));
  BOOST_CHECK_EXCEPTION(
      driver.CreateWalletAddress(TestConfig(), bbp::ChainWalletMode::kPublic),
      bbp::UnsupportedChainOperation, [](const std::runtime_error& error) {
        return std::string(error.what()).find("Bitcoin Core") !=
               std::string::npos;
      });
}
