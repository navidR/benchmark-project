#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "bbp/simulation_registry.h"

namespace bbp {

class RuntimeWalletSnapshot {
 public:
  RuntimeWalletSnapshot() = default;

  [[nodiscard]] std::uint64_t generation() const;
  [[nodiscard]] const SimulationRegistry& registry() const;
  [[nodiscard]] const std::vector<WalletIdentity>& wallets() const;

 private:
  struct Generation;
  friend class RuntimeWalletRegistry;

  explicit RuntimeWalletSnapshot(
      std::shared_ptr<const Generation> generation) noexcept
      : generation_(std::move(generation)) {}

  std::shared_ptr<const Generation> generation_;
};

class RuntimeWalletRegistry {
 public:
  class PreparedAppend {
   public:
    PreparedAppend(PreparedAppend&&) noexcept = default;
    PreparedAppend& operator=(PreparedAppend&&) noexcept = default;

    PreparedAppend(const PreparedAppend&) = delete;
    PreparedAppend& operator=(const PreparedAppend&) = delete;

    [[nodiscard]] RuntimeWalletSnapshot Commit() noexcept;

   private:
    friend class RuntimeWalletRegistry;

    PreparedAppend(
        RuntimeWalletRegistry* owner, std::unique_lock<std::mutex> lock,
        std::shared_ptr<const RuntimeWalletSnapshot::Generation> generation)
        : owner_(owner),
          lock_(std::move(lock)),
          generation_(std::move(generation)) {}

    RuntimeWalletRegistry* owner_ = nullptr;
    std::unique_lock<std::mutex> lock_;
    std::shared_ptr<const RuntimeWalletSnapshot::Generation> generation_;
  };

  RuntimeWalletRegistry();

  RuntimeWalletRegistry(const RuntimeWalletRegistry&) = delete;
  RuntimeWalletRegistry& operator=(const RuntimeWalletRegistry&) = delete;

  void Initialize(SimulationRegistry registry);
  [[nodiscard]] RuntimeWalletSnapshot Snapshot() const;
  PreparedAppend PrepareAppend(std::uint64_t expected_generation,
                               std::vector<WalletIdentity> wallets,
                               std::uint32_t runtime_node_count);

 private:
  mutable std::mutex mutex_;
  std::shared_ptr<const RuntimeWalletSnapshot::Generation> generation_;
};

}  // namespace bbp
