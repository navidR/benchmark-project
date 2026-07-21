#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace bbp {

struct RunOwnership {
  std::string run_id;
  std::filesystem::path run_root;
  std::string resource_id;
  std::string cgroup_name;
  std::string interface_token;

  bool operator==(const RunOwnership&) const = default;
};

RunOwnership CreateRunOwnership(std::string run_id,
                                const std::filesystem::path& run_root);
RunOwnership LoadRunOwnership(std::string run_id,
                              const std::filesystem::path& run_root);
void WriteRunOwnershipMarker(const RunOwnership& ownership);

std::string RunInterfaceName(const RunOwnership& ownership,
                             std::uint32_t node_index, char suffix);
std::string RunInterfaceAlias(const RunOwnership& ownership,
                              std::uint32_t node_index, char suffix);

}  // namespace bbp
