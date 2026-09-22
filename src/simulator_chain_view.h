#pragma once
#include <filesystem>
#include <memory>

namespace bbp {
class ChainDriver;
class ChainViewService;
class RuntimeNodeInventory;
std::shared_ptr<ChainViewService> MakeLiveChainViewService(
    const std::filesystem::path& run_root,
    std::shared_ptr<const ChainDriver> driver, RuntimeNodeInventory& inventory);
}  // namespace bbp
