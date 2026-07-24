#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver.h"

namespace bbp {

// Immutable node routing snapshot. lifetime_ leases the owning runtime
// generation so node removal can publish a new snapshot, wait for outstanding
// readers to drain, and only then clean the removed node's resources.
class NodeConfigSnapshot {
 public:
  NodeConfigSnapshot() = default;
  explicit NodeConfigSnapshot(
      std::vector<ChainNodeConfig> nodes,
      std::shared_ptr<const void> lifetime = std::shared_ptr<const void>{})
      : nodes_(std::move(nodes)), lifetime_(std::move(lifetime)) {}

  const std::vector<ChainNodeConfig>& nodes() const { return nodes_; }

 private:
  std::vector<ChainNodeConfig> nodes_;
  std::shared_ptr<const void> lifetime_;
};

}  // namespace bbp
