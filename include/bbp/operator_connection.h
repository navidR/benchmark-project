#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

struct OperatorConnectionCommand {
  std::filesystem::path executable;
  std::vector<std::string> arguments;
  std::filesystem::path data_dir;
  std::string peer_address;
  std::uint16_t peer_port = 0;

  [[nodiscard]] std::string ShellCommand() const;
};

std::string PosixShellQuote(std::string_view value);
std::string OperatorConnectionCommandFromReport(
    const boost::json::object& report);

}  // namespace bbp
