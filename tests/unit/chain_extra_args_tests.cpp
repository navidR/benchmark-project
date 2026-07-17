#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/chain_extra_args.h"

BOOST_AUTO_TEST_CASE(chain_extra_args_accept_bounded_unowned_options) {
  const bbp::ChainExtraArgs arguments(
      {"-dbcache=64", "--maxmempool=128", "-checkblocks=0"});
  BOOST_REQUIRE_EQUAL(arguments.arguments().size(), 3U);
  BOOST_TEST(arguments.arguments()[0] == "-dbcache=64");
  BOOST_TEST(arguments.arguments()[1] == "--maxmempool=128");
  BOOST_TEST(arguments.arguments()[2] == "-checkblocks=0");
}

BOOST_AUTO_TEST_CASE(chain_extra_args_reject_structural_abuse) {
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({""}), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({"dbcache=64"}), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({"---dbcache=64"}), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({"-=64"}), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({"-db_cache=64"}), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({"-dbcache=64\n"}), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs({std::string(1025U, 'a')}),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::ChainExtraArgs(std::vector<std::string>(33U, "-dbcache=64")),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::ChainExtraArgs(std::vector<std::string>(
                        9U, "-" + std::string(1023U, 'a'))),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(chain_extra_args_reject_simulator_owned_controls) {
  const std::vector<std::string> arguments = {
      "-datadir=/tmp/escape",
      "--testnet",
      "-noregtest",
      "-rpcport=1",
      "-norpcauth",
      "-bind=0.0.0.0",
      "-port=1",
      "-noconnect",
      "-addnode=host",
      "-dnsseed=1",
      "-maxconnections=1",
      "-disablewallet=0",
      "-keypool=1",
      "-debug=all",
      "-printtoconsole",
      "-txindex=0",
      "-dandelion=1",
      "-daemon",
      "-blocknotify=command",
      "-zmqpubrawtx=tcp://host:1",
      "-dropmessagestest=2",
      "-paytxfee=0.1",
      "-mnemonic=secret",
      "-prune=1",
  };
  for (const std::string& argument : arguments) {
    BOOST_CHECK_THROW(bbp::ChainExtraArgs({argument}), std::runtime_error);
  }
}
