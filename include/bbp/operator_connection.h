#pragma once

#include <boost/json/object.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <stop_token>
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

#ifdef BBP_FIRO_GUI_LAUNCHER
enum class OperatorConnectionLauncherCleanupResult {
  kRemoved,
  kAlreadyAbsent,
  kOwnershipChanged,
};

class OperatorConnectionLauncherCleanupUnverified final
    : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

struct OperatorConnectionLauncherSnapshot {
  std::string node_id;
  std::string operator_command;
  std::filesystem::path launcher_path;
};

struct OperatorConnectionLauncherAuthority {
  std::uint64_t inventory_generation = 0U;
  std::string node_id;
  OperatorConnectionCommand command;
};

using OperatorConnectionLauncherAuthorityResolver =
    std::function<OperatorConnectionLauncherAuthority(
        std::string_view node_id, std::stop_token stop_token)>;

class OperatorConnectionLauncher {
 public:
  virtual ~OperatorConnectionLauncher() = default;

  virtual OperatorConnectionLauncherSnapshot ReplaceFromReport(
      const boost::json::object& report,
      std::optional<std::string_view> required_node_id = std::nullopt,
      std::stop_token stop_token = {}) = 0;
  [[nodiscard]] virtual std::optional<OperatorConnectionLauncherSnapshot>
  Snapshot() const = 0;
  virtual OperatorConnectionLauncherCleanupResult CloseAndCleanup() = 0;
  virtual OperatorConnectionLauncherCleanupResult CloseAndCleanup(
      std::stop_token stop_token) = 0;
  virtual OperatorConnectionLauncherCleanupResult CloseAndCleanup(
      std::chrono::steady_clock::time_point deadline,
      std::stop_token stop_token = {}) = 0;
};
#endif

std::string PosixShellQuote(std::string_view value);
std::string OperatorConnectionCommandFromReport(
    const boost::json::object& report);

}  // namespace bbp
