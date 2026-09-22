#pragma once
#include <filesystem>
#include <memory>

namespace bbp {
class ChainDriver;
class ChainViewService;
class PoolViewService;
class RuntimeNodeInventory;
std::shared_ptr<ChainViewService> MakeLiveChainViewService(
    const std::filesystem::path& run_root,
    std::shared_ptr<const ChainDriver> driver, RuntimeNodeInventory& inventory);
std::shared_ptr<PoolViewService> MakeLivePoolViewService(
    const std::filesystem::path& run_root,
    std::shared_ptr<const ChainDriver> driver, RuntimeNodeInventory& inventory);
}  // namespace bbp
