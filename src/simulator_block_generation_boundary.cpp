#include "simulator_block_generation_boundary.h"

#include <chrono>
#include <limits>
#include <utility>

#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/block_generation_workload.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_node_process_state.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {
namespace {

std::unique_lock<std::timed_mutex> AcquireBlockGenerationLock(
    std::timed_mutex& block_generation_mutex, std::stop_token stop_token) {
  std::unique_lock<std::timed_mutex> generation_lock(block_generation_mutex,
                                                     std::defer_lock);
  while (!generation_lock.try_lock_for(std::chrono::milliseconds(25))) {
    ThrowIfStopRequested(stop_token);
  }
  ThrowIfStopRequested(stop_token);
  return generation_lock;
}

}  // namespace

void RecordGeneratedBlocks(const ChainDriver& driver, NodeRuntime& node,
                           const std::vector<std::string>& block_hashes,
                           std::stop_token stop_token) {
  node.AddGeneratedBlocks(static_cast<std::uint64_t>(block_hashes.size()));
  std::uint64_t mined_transaction_count = 0;
  for (const std::string& block_hash : block_hashes) {
    std::uint64_t block_transaction_count = 0;
    try {
      block_transaction_count = driver.ReadBlockNonRewardTransactionCount(
          node.config, block_hash, stop_token);
    } catch (const SimulationCancelled&) {
      throw;
    } catch (const std::exception& error) {
      node.MarkMinedTransactionCountIncomplete();
      BBP_LOG(warning) << "could not count transactions in generated block "
                       << block_hash << " for " << node.config.id << ": "
                       << error.what();
      continue;
    }
    if (mined_transaction_count >
        std::numeric_limits<std::uint64_t>::max() - block_transaction_count) {
      throw std::runtime_error("mined transaction count overflow");
    }
    mined_transaction_count += block_transaction_count;
  }
  node.AddMinedTransactions(mined_transaction_count);
}

std::vector<std::string> GenerateBlocksSerialized(
    std::timed_mutex& block_generation_mutex, const ChainDriver& driver,
    const ChainNodeConfig& node, std::uint32_t count,
    const std::string& reward_address, std::stop_token stop_token,
    const std::function<void()>& authorize_mutation) {
  std::unique_lock<std::timed_mutex> generation_lock =
      AcquireBlockGenerationLock(block_generation_mutex, stop_token);
  if (authorize_mutation) {
    authorize_mutation();
  }
  return driver.GenerateBlocks(node, count, reward_address, stop_token);
}

NodeRuntime& RequireRuntimeNodeNumber(const RuntimeNodeSnapshot& nodes,
                                      std::uint32_t node,
                                      std::string_view context) {
  if (node == 0U || node > nodes.size()) {
    throw std::runtime_error(std::string(context) +
                             " references an inactive node number " +
                             std::to_string(node));
  }
  return nodes[node - 1U];
}

GeneratedBlockWorkloadBoundary GenerateBlockWorkloadBoundary(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    const RuntimeNodeSnapshot& nodes, const BlockGenerationWorkload& workload,
    const std::string& reward_address, std::stop_token mutation_stop_token,
    std::stop_token boundary_stop_token,
    const std::function<void()>& authorize_mutation) {
  if (workload.count == 0U) {
    throw std::logic_error(
        "block-generation boundary requires a positive count");
  }
  NodeRuntime& generator = RequireRuntimeNodeNumber(
      nodes, workload.node, "block generation workload");
  std::unique_lock<std::timed_mutex> generation_lock =
      AcquireBlockGenerationLock(block_generation_mutex, boundary_stop_token);
  RequireNodeRunning(generator, "block generation workload");
  const std::uint64_t start_height =
      driver.ReadMetrics(generator.config, boundary_stop_token).height;
  ThrowIfStopRequested(boundary_stop_token);
  std::vector<std::string> hashes;
  if (authorize_mutation) {
    authorize_mutation();
  }
  try {
    hashes = driver.GenerateBlocks(generator.config, workload.count,
                                   reward_address, mutation_stop_token);
  } catch (const SimulationCancelled&) {
    if (!mutation_stop_token.stop_requested()) {
      throw BlockGenerationOutcomeUnconfirmed(
          "block generation RPC cancelled after mutation admission with an "
          "unconfirmed outcome");
    }
    throw;
  } catch (const std::exception& error) {
    throw BlockGenerationOutcomeUnconfirmed(
        "block generation RPC outcome is unconfirmed: " +
        std::string(error.what()));
  } catch (...) {
    throw BlockGenerationOutcomeUnconfirmed(
        "block generation RPC outcome is unconfirmed after an unknown "
        "failure");
  }
  if (hashes.size() != workload.count ||
      start_height >
          std::numeric_limits<std::uint64_t>::max() - hashes.size()) {
    throw BlockGenerationOutcomeUnconfirmed(
        "block generation workload returned an invalid block count or height");
  }

  const std::uint64_t target_height = start_height + hashes.size();
  return GeneratedBlockWorkloadBoundary{
      .generator_node = workload.node,
      .generator_node_id = generator.config.id,
      .start_height = start_height,
      .target_height = target_height,
      .hashes = std::move(hashes),
      .reward_address = reward_address,
  };
}

void RecordAndPublishGeneratedBlockWorkloadBoundary(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const GeneratedBlockWorkloadBoundary& boundary,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::stop_token reconciliation_stop_token,
    std::optional<std::string_view> workload_id) {
  NodeRuntime& generator = RequireRuntimeNodeNumber(
      nodes, boundary.generator_node, "block generation workload");
  RecordGeneratedBlocks(driver, generator, boundary.hashes,
                        reconciliation_stop_token);
  WriteEvent(events_path, options.run_id, generator.config.id,
             SimulationEventKind::kGeneratedBlocks,
             GeneratedBlocksDetail(
                 workload_index, workload_count, boundary.generator_node,
                 boundary.start_height, boundary.target_height, boundary.hashes,
                 boundary.reward_address, std::nullopt, workload_id));
}

void SynchronizeBlockWorkloadBoundary(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const GeneratedBlockWorkloadBoundary& boundary,
    std::uint32_t sync_timeout_sec, std::stop_token stop_token) {
  for (NodeRuntime& node : nodes) {
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    driver.WaitForHeight(node.config, boundary.target_height,
                         std::chrono::seconds(sync_timeout_sec), stop_token);
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kHeightReached,
               std::to_string(boundary.target_height));
  }
}

}  // namespace bbp::simulator_app_internal
