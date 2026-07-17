#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

enum class NodeArtifactType {
  kDirectory,
  kRegularFile,
  kSymbolicLink,
  kOther,
};

std::string_view NodeArtifactTypeName(NodeArtifactType type);

struct NodeArtifactEntry {
  std::string relative_path;
  NodeArtifactType type = NodeArtifactType::kOther;
  std::uint64_t size_bytes = 0U;
};

struct NodeArtifactInventory {
  std::string node_id;
  std::string data_directory;
  std::vector<NodeArtifactEntry> data_entries;
  std::vector<NodeArtifactEntry> log_files;
  bool data_entries_truncated = false;
  bool log_files_truncated = false;
  std::string warning;
  std::string error;
};

NodeArtifactInventory InspectNodeArtifacts(
    const std::filesystem::path& run_root, std::string_view node_id);

}  // namespace bbp
