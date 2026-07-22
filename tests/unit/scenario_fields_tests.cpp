#include <boost/test/unit_test.hpp>
#include <cstddef>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/workload_kind.h"

namespace bbp {
namespace {

void RequireUniqueFields(std::span<const std::string_view> fields) {
  std::set<std::string_view> unique;
  for (const std::string_view field : fields) {
    BOOST_TEST(!field.empty());
    BOOST_TEST(unique.emplace(field).second);
  }
}

}  // namespace

BOOST_AUTO_TEST_CASE(scenario_object_fields_cover_every_typed_context) {
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ScenarioObjectKind::kCount); ++index) {
    const auto kind = static_cast<ScenarioObjectKind>(index);
    BOOST_TEST(!ScenarioObjectKindName(kind).empty());
    RequireUniqueFields(ScenarioObjectFields(kind));
    for (const std::string_view field : ScenarioObjectFields(kind)) {
      BOOST_TEST(ScenarioObjectFieldAllowed(kind, field));
    }
    BOOST_TEST(!ScenarioObjectFieldAllowed(kind, "x-bbp-unsupported"));
  }
  BOOST_CHECK_THROW(ScenarioObjectKindName(ScenarioObjectKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(ScenarioObjectFields(ScenarioObjectKind::kCount),
                    std::logic_error);
}

BOOST_AUTO_TEST_CASE(scenario_topology_fields_cover_every_kind) {
  RequireUniqueFields(ScenarioTopologyCommonFields());
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(PeerTopologyKind::kCount); ++index) {
    const auto kind = static_cast<PeerTopologyKind>(index);
    BOOST_TEST(PeerTopologyKindName(kind) != "unknown");
    RequireUniqueFields(ScenarioTopologyKindFields(kind));
    for (const std::string_view field : ScenarioTopologyCommonFields()) {
      BOOST_TEST(ScenarioTopologyFieldAllowed(kind, field));
    }
    for (const std::string_view field : ScenarioTopologyKindFields(kind)) {
      BOOST_TEST(ScenarioTopologyFieldAllowed(kind, field));
    }
    BOOST_TEST(!ScenarioTopologyFieldAllowed(kind, "x-bbp-unsupported"));
  }
  BOOST_CHECK_THROW(ScenarioTopologyKindFields(PeerTopologyKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(
      ScenarioTopologyFieldAllowed(PeerTopologyKind::kCount, "type"),
      std::logic_error);
}

BOOST_AUTO_TEST_CASE(scenario_workload_and_command_fields_cover_every_kind) {
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    const auto kind = static_cast<WorkloadKind>(index);
    BOOST_TEST(WorkloadKindName(kind) != "unknown");
    RequireUniqueFields(ScenarioWorkloadFields(kind));
    for (const std::string_view field : ScenarioWorkloadFields(kind)) {
      BOOST_TEST(ScenarioWorkloadFieldAllowed(kind, field));
    }
    BOOST_TEST(!ScenarioWorkloadFieldAllowed(kind, "x-bbp-unsupported"));
  }
  BOOST_CHECK_THROW(ScenarioWorkloadFields(WorkloadKind::kCount),
                    std::logic_error);

  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(SimulationCommandKind::kCount);
       ++index) {
    const auto kind = static_cast<SimulationCommandKind>(index);
    BOOST_TEST(SimulationCommandKindName(kind) != "unknown");
    RequireUniqueFields(ScenarioCommandFields(kind));
    for (const std::string_view field : ScenarioCommandFields(kind)) {
      BOOST_TEST(ScenarioCommandFieldAllowed(kind, field));
    }
    BOOST_TEST(!ScenarioCommandFieldAllowed(kind, "x-bbp-unsupported"));
  }
  BOOST_CHECK_THROW(ScenarioCommandFields(SimulationCommandKind::kCount),
                    std::logic_error);
  BOOST_CHECK_THROW(
      ScenarioCommandFieldAllowed(SimulationCommandKind::kCount, "node"),
      std::logic_error);
}

BOOST_AUTO_TEST_CASE(
    scenario_member_registry_is_descriptor_derived_and_unique) {
  const std::vector<std::string> members = BuildScenarioMemberRegistry();
  const std::set<std::string> unique(members.begin(), members.end());
  BOOST_TEST(unique.size() == members.size());
  BOOST_TEST(unique.contains("scenario.simulation"));
  BOOST_TEST(unique.contains("simulation.name"));
  BOOST_TEST(unique.contains("scenario.firod"));
  BOOST_TEST(unique.contains("scenario.bitcoind"));
  BOOST_TEST(unique.contains("scenario.monerod"));
  BOOST_TEST(unique.contains("topology.average_degree"));
  BOOST_TEST(unique.contains("workload.wallet_transactions.queue_capacity"));
  BOOST_TEST(unique.contains("command.set_resource_limits.resource_limits"));
  BOOST_TEST(!unique.contains("topology.peer_topology.edge_probability"));
  BOOST_TEST(!unique.contains("distribution.minimum"));
}

}  // namespace bbp
