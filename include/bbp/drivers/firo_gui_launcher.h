#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

#include "bbp/operator_connection.h"

namespace bbp {

using FiroQtLauncherCleanupResult = OperatorConnectionLauncherCleanupResult;
using FiroQtLauncherCleanupUnverified =
    OperatorConnectionLauncherCleanupUnverified;
using FiroQtLauncherSnapshot = OperatorConnectionLauncherSnapshot;
using FiroQtLauncherAuthority = OperatorConnectionLauncherAuthority;
using FiroQtLauncherAuthorityResolver =
    OperatorConnectionLauncherAuthorityResolver;

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
  [[nodiscard]] FiroQtLauncherCleanupResult Cleanup(std::stop_token stop_token);
  [[nodiscard]] FiroQtLauncherCleanupResult Cleanup(
      std::chrono::steady_clock::time_point deadline,
      std::stop_token stop_token = {});

 private:
  OwnedFiroQtLauncher(std::filesystem::path path, std::uintmax_t device,
                      std::uintmax_t inode, int descriptor) noexcept;
  FiroQtLauncherCleanupResult CleanupImpl(
      std::optional<std::chrono::steady_clock::time_point> deadline,
      std::stop_token stop_token);
  void CleanupNoThrow() noexcept;
  void ResetOwnership() noexcept;

  std::filesystem::path path_;
  std::uintmax_t device_ = 0U;
  std::uintmax_t inode_ = 0U;
  int descriptor_ = -1;
  bool active_ = false;
};

// One run-scoped owner shared by every local control surface. Replacement is
// serialized and publishes only after the candidate exists and the previous
// owned launcher has been verified and removed.
class FiroQtLauncherService final : public OperatorConnectionLauncher {
 public:
  FiroQtLauncherService() = default;
  explicit FiroQtLauncherService(FiroQtLauncherAuthorityResolver resolver);
  ~FiroQtLauncherService() override;

  FiroQtLauncherService(const FiroQtLauncherService&) = delete;
  FiroQtLauncherService& operator=(const FiroQtLauncherService&) = delete;

  FiroQtLauncherSnapshot ReplaceFromReport(
      const boost::json::object& report,
      std::optional<std::string_view> required_node_id = std::nullopt,
      std::stop_token stop_token = {}) override;
  [[nodiscard]] std::optional<FiroQtLauncherSnapshot> Snapshot() const override;
  FiroQtLauncherCleanupResult CloseAndCleanup() override;
  FiroQtLauncherCleanupResult CloseAndCleanup(
      std::stop_token stop_token) override;
  FiroQtLauncherCleanupResult CloseAndCleanup(
      std::chrono::steady_clock::time_point deadline,
      std::stop_token stop_token = {}) override;

 private:
  std::unique_lock<std::timed_mutex> Acquire(
      std::optional<std::chrono::steady_clock::time_point> deadline,
      std::stop_token stop_token) const;
  FiroQtLauncherCleanupResult CloseAndCleanupImpl(
      std::optional<std::chrono::steady_clock::time_point> deadline,
      std::stop_token stop_token);
  void ReconcileSnapshotAfterCleanupFailure();
  void CloseAndCleanupNoThrow() noexcept;

  mutable std::timed_mutex mutation_mutex_;
  mutable std::mutex snapshot_mutex_;
  std::optional<OwnedFiroQtLauncher> pending_cleanup_;
  std::optional<OwnedFiroQtLauncher> launcher_;
  std::optional<FiroQtLauncherSnapshot> snapshot_;
  bool cleanup_unverified_ = false;
  std::string unverified_cleanup_failure_;
  FiroQtLauncherAuthorityResolver authority_resolver_;
  bool closed_ = false;
};

#ifdef BBP_ENABLE_TEST_HOOKS
enum class FiroQtLauncherCleanupTestPhase {
  kAfterPublicIdentityCheck,
  kAfterAtomicCapture,
  kBeforeQuarantineUnlink,
};

using FiroQtLauncherCleanupTestHook = std::function<void(
    FiroQtLauncherCleanupTestPhase, const std::filesystem::path&,
    const std::optional<std::filesystem::path>&)>;

void SetFiroQtLauncherCleanupTestHook(FiroQtLauncherCleanupTestHook hook);
#endif

}  // namespace bbp
