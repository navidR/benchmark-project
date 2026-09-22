#include "simulator_chain_view.h"

#include "bbp/chain_view.h"
#include "bbp/pool_view.h"
#include "bbp/runtime_node_inventory.h"
#include "simulator_node_process_state.h"

namespace bbp {
std::shared_ptr<ChainViewService> MakeLiveChainViewService(
    const std::filesystem::path& run_root,
    std::shared_ptr<const ChainDriver> driver,
    RuntimeNodeInventory& inventory) {
  return std::make_shared<ChainViewService>(
      run_root, [driver = std::move(driver), &inventory] {
        const auto nodes =
            std::make_shared<NodeConfigSnapshot>(inventory.ConfigSnapshot());
        std::vector<ChainBlockReader> readers;
        for (const auto& config : nodes->nodes()) {
          readers.push_back(
              {.node_id = config.id,
               .tip =
                   [driver, nodes, config](std::stop_token stop) {
                     return driver->ReadChainTip(config, stop);
                   },
               .summary =
                   [driver, nodes, config](std::uint64_t height,
                                           std::stop_token stop) {
                     return driver->ReadBlockSummary(config, height, stop);
                   },
               .detail =
                   [driver, nodes, config](const std::string& hash,
                                           std::stop_token stop) {
                     return driver->ReadBlockDetail(config, hash, stop);
                   }});
        }
        return readers;
      });
}
std::shared_ptr<PoolViewService> MakeLivePoolViewService(
    const std::filesystem::path& run_root,
    std::shared_ptr<const ChainDriver> driver,
    RuntimeNodeInventory& inventory) {
  return std::make_shared<PoolViewService>(
      run_root, [driver = std::move(driver), &inventory] {
        const auto runtime = inventory.Snapshot();
        const auto nodes =
            std::make_shared<NodeConfigSnapshot>(runtime.ConfigSnapshot());
        std::vector<PoolReader> readers;
        for (std::size_t index = 0; index < runtime.size(); ++index) {
          if (!runtime[index].AllowsChainMetrics() ||
              !simulator_app_internal::NodeProcessRunning(runtime[index]))
            continue;
          const auto& config = nodes->nodes()[index];
          readers.push_back(
              {.node_id = config.id,
               .snapshot =
                   [driver, nodes, config](std::stop_token stop) {
                     return driver->ReadPoolSnapshot(config, stop);
                   },
               .detail =
                   [driver, nodes, config](const ChainPoolTransaction& tx,
                                           std::stop_token stop) {
                     return driver->ReadPoolTransaction(config, tx, stop);
                   },
               .departure =
                   [driver, nodes, config](const std::string& id,
                                           std::stop_token stop) {
                     const auto observed = driver->ObserveTransactionUntil(
                         config, id,
                         std::chrono::steady_clock::now() +
                             std::chrono::seconds(2),
                         stop);
                     return observed.state == ChainTransactionState::kConfirmed
                                ? "Mined in block " + observed.block_hash
                                : std::string{};
                   }});
        }
        return readers;
      });
}
}  // namespace bbp
