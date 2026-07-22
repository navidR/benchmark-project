#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

enum class FiroQtLauncherCleanupResult {
  kRemoved,
  kAlreadyAbsent,
  kOwnershipChanged,
};

class OwnedFiroQtLauncher {
 public:
  OwnedFiroQtLauncher() = default;
  OwnedFiroQtLauncher(const OwnedFiroQtLauncher&) = delete;
  OwnedFiroQtLauncher& operator=(const OwnedFiroQtLauncher&) = delete;
  OwnedFiroQtLauncher(OwnedFiroQtLauncher&& other) noexcept;
  OwnedFiroQtLauncher& operator=(OwnedFiroQtLauncher&& other);
  ~OwnedFiroQtLauncher();

  static OwnedFiroQtLauncher Create(std::string_view shell_command);

  [[nodiscard]] const std::filesystem::path& path() const { return path_; }
  [[nodiscard]] bool active() const { return active_; }
  [[nodiscard]] FiroQtLauncherCleanupResult Cleanup();

 private:
  OwnedFiroQtLauncher(std::filesystem::path path, std::uintmax_t device,
                      std::uintmax_t inode, int descriptor);
  void CleanupNoThrow() noexcept;
  void ResetOwnership() noexcept;

  std::filesystem::path path_;
  std::uintmax_t device_ = 0U;
  std::uintmax_t inode_ = 0U;
  int descriptor_ = -1;
  bool active_ = false;
};

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
