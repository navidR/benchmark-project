#pragma once

#include <filesystem>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/runtime_node_resource_manifest.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

struct RuntimeCapacityDocumentChange {
  std::filesystem::path path;
  std::string before;
  std::string after;
};

struct PreparedRuntimeCapacityDocuments {
  RunOwnership ownership;
  OwnedRunRootIdentity root_identity;
  std::vector<RuntimeCapacityDocumentChange> changes;
};

// Reads one owned regular metadata file at its observed size without allowing
// concurrent growth or replacement; allocation limits come from std::string.
std::string ReadRuntimeCapacityDocumentAt(int root, std::string_view name,
                                          std::stop_token stop_token = {});
PreparedRuntimeCapacityDocuments PrepareRuntimeCapacityDocuments(
    const Options& candidate);
// The caller holds the runtime publication lock through publication or restore.
void PublishRuntimeCapacityDocuments(
    const PreparedRuntimeCapacityDocuments& prepared);
// Attempts every restoration and reports any failures to the caller.
void RestoreRuntimeCapacityDocuments(
    const PreparedRuntimeCapacityDocuments& prepared);

}  // namespace simulator_app_internal
}  // namespace bbp
