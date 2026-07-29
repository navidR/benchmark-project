#include <boost/test/unit_test.hpp>
#include <string>
#include <utility>
#include <vector>

#include "bbp/runtime_node_inventory.h"
#include "bbp/simulator/scenario_workload_admission.h"
#include "bbp/simulator/workload_kind.h"

namespace {

bbp::NodeRuntime RuntimeNode(std::string id) {
  bbp::NodeRuntime node;
  node.config.id = std::move(id);
  return node;
}

bool OldGenerationDrainsAfterDispatch(bbp::WorkloadKind kind) {
  bbp::RuntimeNodeInventory inventory(2U);
  std::vector<bbp::NodeRuntime> initial;
  initial.push_back(RuntimeNode("node-1"));
  initial.push_back(RuntimeNode("node-2"));
  inventory.Initialize(initial);

  bbp::RuntimeNodeSnapshot removal_reader = inventory.Snapshot();
  bbp::RuntimeNodeSnapshot dispatch_reader =
      bbp::SnapshotScenarioDispatchNodes(inventory, kind);
  const std::vector<bbp::ChainNodeConfig> retained{
      removal_reader.front().config};
  bbp::RuntimeNodeInventory::PreparedRemoval prepared =
      inventory.PrepareRemoval(removal_reader.generation(), {"node-2"},
                               retained);
  static_cast<void>(prepared.Commit());
  removal_reader = {};

  const bool drained = prepared.ReadersDrained();
  dispatch_reader = {};
  BOOST_TEST(prepared.ReadersDrained());
  return drained;
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    scenario_dispatch_snapshot_respects_node_mutation_admission) {
  BOOST_TEST(
      OldGenerationDrainsAfterDispatch(bbp::WorkloadKind::kBlockGeneration));
  BOOST_TEST(OldGenerationDrainsAfterDispatch(bbp::WorkloadKind::kFreezeNode));
  BOOST_TEST(
      !OldGenerationDrainsAfterDispatch(bbp::WorkloadKind::kWaitUntilHeight));
}
