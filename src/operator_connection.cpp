#include "bbp/operator_connection.h"

#include <boost/json/value.hpp>
#include <stdexcept>

namespace bbp {

std::string PosixShellQuote(std::string_view value) {
  if (value.find('\0') != std::string_view::npos) {
    throw std::invalid_argument("shell argument contains NUL");
  }
  std::string quoted;
  quoted.reserve(value.size() + 2U);
  quoted.push_back('\'');
  for (const char character : value) {
    if (character == '\'') {
      quoted += "'\"'\"'";
    } else {
      quoted.push_back(character);
    }
  }
  quoted.push_back('\'');
  return quoted;
}

std::string OperatorConnectionCommand::ShellCommand() const {
  if (executable.empty()) {
    throw std::invalid_argument("operator connection executable is empty");
  }
  std::string command = PosixShellQuote(executable.string());
  for (const std::string& argument : arguments) {
    command.push_back(' ');
    command += PosixShellQuote(argument);
  }
  return command;
}

std::string OperatorConnectionCommandFromReport(
    const boost::json::object& report) {
  const boost::json::value* connection =
      report.if_contains("operator_connection_command");
  if (connection == nullptr || !connection->is_object()) {
    return {};
  }
  const boost::json::value* command =
      connection->as_object().if_contains("command");
  if (command == nullptr || !command->is_string()) {
    return {};
  }
  return std::string(command->as_string());
}

}  // namespace bbp
