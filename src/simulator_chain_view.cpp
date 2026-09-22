#include "simulator_chain_view.h"

#include "bbp/chain_view.h"
#include "bbp/runtime_node_inventory.h"

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
}  // namespace bbp
