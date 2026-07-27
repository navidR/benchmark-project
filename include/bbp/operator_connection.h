#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stop_token>
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

struct FiroQtLauncherSnapshot {
  std::string node_id;
  std::string operator_command;
  std::filesystem::path launcher_path;
};

struct FiroQtLauncherAuthority {
  std::uint64_t inventory_generation = 0U;
  std::string node_id;
  OperatorConnectionCommand command;
};

using FiroQtLauncherAuthorityResolver = std::function<FiroQtLauncherAuthority(
    std::string_view node_id, std::stop_token stop_token)>;

// One run-scoped owner shared by every local control surface. Replacement is
// serialized and publishes only after the candidate exists and the previous
// owned launcher has been verified and removed.
class FiroQtLauncherService {
 public:
  FiroQtLauncherService() = default;
  explicit FiroQtLauncherService(FiroQtLauncherAuthorityResolver resolver);
  ~FiroQtLauncherService();

  FiroQtLauncherService(const FiroQtLauncherService&) = delete;
  FiroQtLauncherService& operator=(const FiroQtLauncherService&) = delete;

  FiroQtLauncherSnapshot ReplaceFromReport(
      const boost::json::object& report,
      std::optional<std::string_view> required_node_id = std::nullopt,
      std::stop_token stop_token = {});
  [[nodiscard]] std::optional<FiroQtLauncherSnapshot> Snapshot() const;
  FiroQtLauncherCleanupResult CloseAndCleanup();

 private:
  std::unique_lock<std::timed_mutex> Acquire(std::stop_token stop_token) const;
  void ReconcileSnapshotAfterCleanupFailure();
  void CloseAndCleanupNoThrow() noexcept;

  mutable std::timed_mutex mutation_mutex_;
  mutable std::mutex snapshot_mutex_;
  std::optional<OwnedFiroQtLauncher> pending_cleanup_;
  std::optional<OwnedFiroQtLauncher> launcher_;
  std::optional<FiroQtLauncherSnapshot> snapshot_;
  std::string unverified_cleanup_failure_;
  FiroQtLauncherAuthorityResolver authority_resolver_;
  bool closed_ = false;
};

#ifdef BBP_ENABLE_TEST_HOOKS
enum class FiroQtLauncherCleanupTestPhase {
  kAfterPublicIdentityCheck,
  kAfterAtomicCapture,
};

using FiroQtLauncherCleanupTestHook = std::function<void(
    FiroQtLauncherCleanupTestPhase, const std::filesystem::path&,
    const std::optional<std::filesystem::path>&)>;

void SetFiroQtLauncherCleanupTestHook(FiroQtLauncherCleanupTestHook hook);
#endif

std::string PosixShellQuote(std::string_view value);
std::string OperatorConnectionCommandFromReport(
    const boost::json::object& report);

}  // namespace bbp
