#include "bbp/operator_connection.h"

#include <algorithm>
#include <boost/json/value.hpp>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

std::string ShellWord(std::string_view value) {
  constexpr std::string_view safe =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/"
      "_-.,:=+@%";
  if (!value.empty() &&
      value.find_first_not_of(safe) == std::string_view::npos) {
    return std::string(value);
  }
  return PosixShellQuote(value);
}

}  // namespace

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
  std::string command = ShellWord(executable.string());
  for (const std::string& argument : arguments) {
    command.push_back(' ');
    command += ShellWord(argument);
  }
  return command;
}

std::vector<std::string> OperatorConnectionArgvFromReport(
    const boost::json::object& report) {
  const auto* connection = report.if_contains("operator_connection_command");
  if (!connection || !connection->is_object()) {
    return {};
  }
  const auto* argv = connection->as_object().if_contains("argv");
  if (!argv || !argv->is_array()) {
    return {};
  }
  std::vector<std::string> result;
  for (const auto& value : argv->as_array()) {
    if (!value.is_string() ||
        std::any_of(value.as_string().begin(), value.as_string().end(),
                    [](unsigned char c) { return c < 32U || c == 127U; })) {
      return {};
    }
    result.emplace_back(value.as_string());
  }
  return result;
}

std::vector<std::string> CopyableShellCommandLines(
    const std::vector<std::string>& argv, std::size_t width) {
  if (argv.empty() || width < 16U) {
    return {};
  }
  std::vector<std::string> lines;
  std::string line;
  for (const auto& argument : argv) {
    const std::string word = ShellWord(argument);
    if (!line.empty()) {
      if (line.size() + 1U + word.size() > width - 2U) {
        lines.push_back(line + " \\");
        line.clear();
      } else {
        line.push_back(' ');
      }
    }
    if (line.size() + word.size() <= width - 2U) {
      line += word;
      continue;
    }
    // Close quotes before a continuation; adjacent fragments remain one word.
    // Reserve for the worst case: an apostrophe expands to five shell bytes.
    const std::size_t chunk_size = (width - 4U) / 5U;
    for (std::size_t offset = 0U; offset < argument.size();
         offset += chunk_size) {
      const std::string fragment =
          ShellWord(std::string_view(argument).substr(offset, chunk_size));
      if (!line.empty() && line.size() + fragment.size() > width - 2U) {
        lines.push_back(line + "\\");
        line.clear();
      }
      line += fragment;
    }
  }
  if (!line.empty()) {
    lines.push_back(std::move(line));
  }
  return lines;
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
