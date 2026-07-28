#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/run_ownership.h"

namespace bbp {

enum class RuntimeNodeResourceState {
  kPendingAdd,
  kPendingReplace,
  kPendingReplaceExchanged,
  kPendingReplaceCommitted,
  kPendingReplaceUncertain,
  kPendingRemove,
  kLive,
};

enum class RuntimeNodeRootOrientation {
  kOriginal,
  kExchanged,
  kUnknown,
};

struct RuntimeNodeResourceEntry {
  std::string node_id;
  std::uint32_t slot = 0U;
  ChainKind chain = ChainKind::kFiro;
  std::filesystem::path data_dir;
  std::optional<std::string> root_name;
  RuntimeNodeResourceState state = RuntimeNodeResourceState::kLive;

  bool operator==(const RuntimeNodeResourceEntry&) const = default;
};

struct RuntimeNodeResourceManifest {
  RunOwnership ownership;
  bool isolated_network = false;
  std::vector<RuntimeNodeResourceEntry> nodes;

  bool operator==(const RuntimeNodeResourceManifest&) const = default;
};

struct OwnedRunRootIdentity {
  std::uintmax_t device = 0U;
  std::uintmax_t inode = 0U;

  bool operator==(const OwnedRunRootIdentity&) const = default;
};

struct OwnedRunRootCleanupReceipt {
  RunOwnership ownership;
  OwnedRunRootIdentity root_identity;

  bool operator==(const OwnedRunRootCleanupReceipt&) const = default;
};

class OwnedRunRootIdentityMismatch final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

void WriteRuntimeNodeResourceManifest(
    const RuntimeNodeResourceManifest& manifest,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});
std::optional<RuntimeNodeResourceManifest> TryLoadRuntimeNodeResourceManifest(
    const RunOwnership& ownership,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});

bool RuntimeNodeRootEntryExists(
    const RunOwnership& ownership, std::string_view node_id,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});
bool RuntimeNodeRootEntryExists(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& entry,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});
void PrepareRuntimeNodeRoot(const RunOwnership& ownership,
                            const RuntimeNodeResourceEntry& entry,
                            bool* acquired = nullptr);
void VerifyRuntimeNodeRootOwnership(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& entry,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});
void CleanupRuntimeNodeRpcCredential(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& entry,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});
void CleanupLegacyRuntimeNodeRpcCredential(
    const RunOwnership& ownership, std::string_view node_id, ChainKind chain,
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt,
    std::stop_token stop_token = {});
void RemoveRuntimeNodeRoot(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& entry,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {},
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt);
void CloneRuntimeNodeRootForReplacement(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& source_entry,
    const RuntimeNodeResourceEntry& staging_entry,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {}, bool* acquired = nullptr);
void ExchangeRuntimeNodeRootsForReplacement(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& first_entry,
    const RuntimeNodeResourceEntry& second_entry,
    RuntimeNodeRootOrientation* orientation);
std::filesystem::path OwnedRunRootCleanupQuarantinePath(
    const RunOwnership& ownership);
std::filesystem::path OwnedRunRootCleanupReceiptPath(
    std::string_view run_id, const std::filesystem::path& run_root);
std::optional<OwnedRunRootCleanupReceipt> TryLoadOwnedRunRootCleanupReceipt(
    std::string_view run_id, const std::filesystem::path& run_root,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {});
void WriteOwnedRunRootCleanupReceipt(
    const RunOwnership& ownership, OwnedRunRootIdentity root_identity,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {});
void RemoveOwnedRunRootCleanupReceipt(
    const OwnedRunRootCleanupReceipt& receipt,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {});
void RemoveOwnedRunRoot(
    const RunOwnership& ownership,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
        std::nullopt,
    std::stop_token stop_token = {},
    std::optional<OwnedRunRootIdentity> expected_root = std::nullopt);

}  // namespace bbp
