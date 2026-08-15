#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
struct BlockGenerationWorkload;
struct ChainNodeConfig;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

void RecordGeneratedBlocks(const ChainDriver& driver, NodeRuntime& node,
                           const std::vector<std::string>& block_hashes,
                           std::stop_token stop_token);
std::vector<std::string> GenerateBlocksSerialized(
    std::timed_mutex& block_generation_mutex, const ChainDriver& driver,
    const ChainNodeConfig& node, std::uint32_t count,
    const std::string& reward_address, std::stop_token stop_token,
    const std::function<void()>& authorize_mutation = {});
NodeRuntime& RequireRuntimeNodeNumber(const RuntimeNodeSnapshot& nodes,
                                      std::uint32_t node,
                                      std::string_view context);

struct GeneratedBlockWorkloadBoundary {
  std::uint32_t generator_node = 0U;
  std::string generator_node_id;
  std::uint64_t start_height = 0U;
  std::uint64_t target_height = 0U;
  std::vector<std::string> hashes;
  std::string reward_address;
};

class BlockGenerationOutcomeUnconfirmed final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

GeneratedBlockWorkloadBoundary GenerateBlockWorkloadBoundary(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    const RuntimeNodeSnapshot& nodes, const BlockGenerationWorkload& workload,
    const std::string& reward_address, std::stop_token mutation_stop_token,
    std::stop_token boundary_stop_token,
    const std::function<void()>& authorize_mutation = {});
void RecordAndPublishGeneratedBlockWorkloadBoundary(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const GeneratedBlockWorkloadBoundary& boundary,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::stop_token reconciliation_stop_token,
    std::optional<std::string_view> workload_id = std::nullopt);
void SynchronizeBlockWorkloadBoundary(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const GeneratedBlockWorkloadBoundary& boundary,
    std::uint32_t sync_timeout_sec, std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
