#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>

#include "bbp/operator_connection.h"

BOOST_AUTO_TEST_CASE(posix_shell_quote_handles_hostile_arguments) {
  BOOST_TEST(bbp::PosixShellQuote("") == "''");
  BOOST_TEST(bbp::PosixShellQuote("plain") == "'plain'");
  BOOST_TEST(bbp::PosixShellQuote("two words") == "'two words'");
  BOOST_TEST(bbp::PosixShellQuote("a'b") == "'a'\"'\"'b'");
  BOOST_TEST(bbp::PosixShellQuote("$HOME;$(id);`id`\nnext") ==
             "'$HOME;$(id);`id`\nnext'");
  const std::string nul_argument("safe\0unsafe", 11U);
  BOOST_CHECK_THROW(bbp::PosixShellQuote(nul_argument), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(operator_connection_renders_every_argv_element) {
  bbp::OperatorConnectionCommand connection;
  connection.executable = "/tmp/Firo GUI/firo-qt";
  connection.arguments = {"-regtest", "-datadir=/tmp/a'b", "$(touch /tmp/x)"};
  BOOST_TEST(connection.ShellCommand() ==
             "'/tmp/Firo GUI/firo-qt' '-regtest' "
             "'-datadir=/tmp/a'\"'\"'b' '$(touch /tmp/x)'");

  connection.executable.clear();
  BOOST_CHECK_THROW(connection.ShellCommand(), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(operator_connection_is_recovered_from_run_report) {
  boost::json::object report;
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report).empty());
  report["operator_connection_command"] = nullptr;
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report).empty());
  report["operator_connection_command"] = boost::json::object{{"command", 1}};
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report).empty());
  report["operator_connection_command"] =
      boost::json::object{{"command", "'/opt/firo/firo-qt' '-regtest'"}};
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report) ==
             "'/opt/firo/firo-qt' '-regtest'");
}
