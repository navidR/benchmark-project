#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_registry.h"

namespace bbp {

struct McpRunEvidenceQuery {
  std::vector<McpInformationFamily> families;
  std::vector<std::string> node_ids;
  std::string cursor;
  std::uint64_t start_sequence = 0U;
  std::size_t limit = kMcpListPageSize;
  std::optional<std::uint64_t> end_sequence;
};

// Reads authoritative run JSONL files through a bounded opaque cursor. The
// cursor must be passed through unchanged and is bound to the exact ordered
// family selection in the query. Cancellation throws McpOperationCancelled.
boost::json::object QueryMcpRunEvidence(std::string_view run_id,
                                        const std::filesystem::path& run_root,
                                        const McpRunEvidenceQuery& query,
                                        std::stop_token stop_token = {});

// Returns a bounded inventory of current run-owned entries. Credentials and
// secrets are never readable. Opaque ids are collision checked when resolved.
boost::json::object BuildMcpRunArtifactInventory(
    std::string_view run_id, const std::filesystem::path& run_root,
    std::stop_token stop_token = {});

boost::json::object ReadMcpRunArtifact(std::string_view run_id,
                                       const std::filesystem::path& run_root,
                                       std::string_view artifact_id,
                                       std::uint64_t offset, std::size_t limit,
                                       std::stop_token stop_token = {});

}  // namespace bbp
