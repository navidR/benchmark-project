#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include "bbp/run_ownership.h"
#include "bbp/runtime_node_resource_manifest.h"

namespace bbp {

struct NodeVethConfig;
struct Options;

namespace simulator_app_internal {

using ManagedRunNodeVethConfigFactory = NodeVethConfig (*)(const Options&,
                                                           std::uint32_t);

class ReservedManagedRunRoot {
 public:
  ~ReservedManagedRunRoot();

  ReservedManagedRunRoot(ReservedManagedRunRoot&&) noexcept;
  ReservedManagedRunRoot(const ReservedManagedRunRoot&) = delete;
  ReservedManagedRunRoot& operator=(const ReservedManagedRunRoot&) = delete;
  ReservedManagedRunRoot& operator=(ReservedManagedRunRoot&&) = delete;

  [[nodiscard]] const RunOwnership& ownership() const;
  [[nodiscard]] int descriptor() const;
  [[nodiscard]] OwnedRunRootIdentity root_identity() const;

  void Adopt();

 private:
  class State;

  explicit ReservedManagedRunRoot(std::unique_ptr<State> state) noexcept;

  std::unique_ptr<State> state_;

  friend std::shared_ptr<ReservedManagedRunRoot> ReserveManagedReplayRunRoot(
      const Options& options);
};

std::filesystem::path BenchmarkRunRoot(const Options& options);
std::shared_ptr<ReservedManagedRunRoot> ReserveManagedReplayRunRoot(
    const Options& options);
void PrepareManagedRunRoot(
    Options* options, ManagedRunNodeVethConfigFactory make_node_veth_config,
    const std::shared_ptr<ReservedManagedRunRoot>& reservation = {});
void RemovePreparedRunRoot(
    const Options& options,
    const std::shared_ptr<ReservedManagedRunRoot>& reservation = {});

}  // namespace simulator_app_internal
}  // namespace bbp
