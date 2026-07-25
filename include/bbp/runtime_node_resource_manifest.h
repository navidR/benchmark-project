#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
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

void WriteRuntimeNodeResourceManifest(
    const RuntimeNodeResourceManifest& manifest);
std::optional<RuntimeNodeResourceManifest> TryLoadRuntimeNodeResourceManifest(
    const RunOwnership& ownership);

bool RuntimeNodeRootEntryExists(const RunOwnership& ownership,
                                std::string_view node_id);
bool RuntimeNodeRootEntryExists(const RunOwnership& ownership,
                                const RuntimeNodeResourceEntry& entry);
void PrepareRuntimeNodeRoot(const RunOwnership& ownership,
                            const RuntimeNodeResourceEntry& entry,
                            bool* acquired = nullptr);
void VerifyRuntimeNodeRootOwnership(const RunOwnership& ownership,
                                    const RuntimeNodeResourceEntry& entry);
void CleanupRuntimeNodeRpcCredential(const RunOwnership& ownership,
                                     const RuntimeNodeResourceEntry& entry);
void CleanupLegacyRuntimeNodeRpcCredential(const RunOwnership& ownership,
                                           std::string_view node_id,
                                           ChainKind chain);
void RemoveRuntimeNodeRoot(const RunOwnership& ownership,
                           const RuntimeNodeResourceEntry& entry,
                           std::optional<std::chrono::steady_clock::time_point>
                               absolute_deadline = std::nullopt,
                           std::stop_token stop_token = {});
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
void RemoveOwnedRunRoot(const RunOwnership& ownership,
                        std::optional<std::chrono::steady_clock::time_point>
                            absolute_deadline = std::nullopt,
                        std::stop_token stop_token = {});

}  // namespace bbp
