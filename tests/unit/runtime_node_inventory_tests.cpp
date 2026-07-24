#include <boost/test/unit_test.hpp>
#include <memory>
#include <stdexcept>
#include <vector>

#include "bbp/runtime_node_inventory.h"

namespace {

bbp::NodeRuntime RuntimeNode(std::string id) {
  bbp::NodeRuntime node;
  node.config.id = std::move(id);
  return node;
}

}  // namespace

BOOST_AUTO_TEST_CASE(runtime_node_inventory_publishes_immutable_generations) {
  bbp::RuntimeNodeInventory inventory(3U);
  std::vector<bbp::NodeRuntime> initial;
  initial.push_back(RuntimeNode("node-1"));
  inventory.Initialize(initial);

  const bbp::RuntimeNodeSnapshot first = inventory.Snapshot();
  BOOST_REQUIRE(first.size() == 1U);
  BOOST_TEST(first.generation() == 1U);
  BOOST_TEST(first.slot(0U) == 0U);
  BOOST_TEST(first[0U].config.id == "node-1");
  bbp::NodeRuntime* first_node = &first[0U];

  std::vector<bbp::RuntimeNodeInsertion> additions;
  bbp::NodeRuntime sparse_node = RuntimeNode("node-3");
  sparse_node.config.p2p_host = "10.42.0.3";
  sparse_node.config.p2p_port = 18444U;
  additions.push_back(bbp::RuntimeNodeInsertion{
      .slot = 2U,
      .runtime = std::make_shared<bbp::NodeRuntime>(std::move(sparse_node)),
  });
  const bbp::RuntimeNodeSnapshot second =
      inventory.PublishAppend(first.generation(), additions);

  BOOST_TEST(first.size() == 1U);
  BOOST_TEST(second.size() == 2U);
  BOOST_TEST(second.generation() == 2U);
  BOOST_TEST(&second[0U] == first_node);
  BOOST_TEST(second.slot(1U) == 2U);
  BOOST_TEST(second[1U].config.id == "node-3");
  BOOST_TEST(second[1U].config.p2p_host == "10.42.0.3");
  BOOST_TEST(second[1U].config.p2p_port == 18444U);

  const bbp::NodeConfigSnapshot configs = inventory.ConfigSnapshot();
  BOOST_REQUIRE(configs.nodes().size() == 2U);
  BOOST_TEST(configs.nodes()[0U].id == "node-1");
  BOOST_TEST(configs.nodes()[1U].id == "node-3");
}

BOOST_AUTO_TEST_CASE(runtime_node_inventory_rejects_nonatomic_publication) {
  {
    bbp::RuntimeNodeInventory too_small(1U);
    std::vector<bbp::NodeRuntime> staged_initial;
    staged_initial.push_back(RuntimeNode("node-1"));
    staged_initial.push_back(RuntimeNode("node-2"));
    BOOST_CHECK_THROW(too_small.Initialize(staged_initial),
                      std::invalid_argument);
    BOOST_REQUIRE(staged_initial.size() == 2U);
    BOOST_TEST(staged_initial[0U].config.id == "node-1");
    BOOST_TEST(staged_initial[1U].config.id == "node-2");
    BOOST_TEST(too_small.Snapshot().empty());
  }

  bbp::RuntimeNodeInventory inventory(2U);
  std::vector<bbp::NodeRuntime> initial;
  initial.push_back(RuntimeNode("node-1"));
  inventory.Initialize(initial);
  const std::uint64_t generation = inventory.Snapshot().generation();

  const auto insertion = [](std::uint32_t slot, std::string id) {
    std::vector<bbp::RuntimeNodeInsertion> additions;
    additions.push_back(bbp::RuntimeNodeInsertion{
        .slot = slot,
        .runtime =
            std::make_shared<bbp::NodeRuntime>(RuntimeNode(std::move(id))),
    });
    return additions;
  };

  std::vector<bbp::RuntimeNodeInsertion> staged = insertion(1U, "node-2");
  const std::weak_ptr<bbp::NodeRuntime> staged_runtime = staged.front().runtime;
  BOOST_CHECK_THROW(inventory.PublishAppend(generation + 1U, staged),
                    std::runtime_error);
  BOOST_TEST(!staged_runtime.expired());
  BOOST_REQUIRE(staged.front().runtime);
  BOOST_TEST(staged.front().runtime->config.id == "node-2");
  BOOST_CHECK_THROW(
      inventory.PublishAppend(generation, insertion(0U, "node-2")),
      std::invalid_argument);
  BOOST_CHECK_THROW(
      inventory.PublishAppend(generation, insertion(1U, "node-1")),
      std::invalid_argument);
  BOOST_TEST(inventory.Snapshot().size() == 1U);

  BOOST_CHECK_THROW(
      inventory.PublishAppend(generation, insertion(2U, "node-2")),
      std::invalid_argument);
  BOOST_TEST(inventory.Snapshot().size() == 1U);

  static_cast<void>(inventory.PublishAppend(generation, staged));
  BOOST_CHECK_THROW(inventory.PublishAppend(inventory.Snapshot().generation(),
                                            insertion(2U, "node-3")),
                    std::runtime_error);
  BOOST_TEST(inventory.Snapshot().size() == 2U);
}
