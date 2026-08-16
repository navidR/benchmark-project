#pragma once

#include <boost/json/object.hpp>

namespace bbp {

class RuntimeNodeSnapshot;
class RuntimeWalletSnapshot;
struct BlockGenerationWorkload;
struct Options;
struct PeerTopologyConfig;
struct ScenarioWorkload;
struct WaitForPeersWorkload;
struct WaitUntilHeightWorkload;

namespace simulator_app_internal {

BlockGenerationWorkload ParseAndValidateLiveBlockGenerationWorkload(
    const boost::json::object& workload, const Options& options,
    const RuntimeNodeSnapshot& nodes);
WaitUntilHeightWorkload ParseAndValidateLiveWaitUntilHeightWorkload(
    const boost::json::object& workload, const Options& options,
    const RuntimeNodeSnapshot& nodes);
WaitForPeersWorkload ParseAndValidateLiveWaitForPeersWorkload(
    const boost::json::object& workload, const Options& options,
    const RuntimeNodeSnapshot& nodes);

Options RuntimeOneShotWorkloadValidationOptions(
    const Options& options, const RuntimeNodeSnapshot& nodes,
    const RuntimeWalletSnapshot& roles,
    const PeerTopologyConfig& live_topology_config);
ScenarioWorkload ParseAndValidateOneShotWorkload(
    const boost::json::object& workload, Options validation_options);

}  // namespace simulator_app_internal
}  // namespace bbp
