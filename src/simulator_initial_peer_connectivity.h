#pragma once

#include <map>
#include <set>
#include <string>

#include "bbp/peer_connectivity_controller.h"

namespace bbp {

class RuntimeNodeSnapshot;
class RuntimePeerTopology;
struct Options;

namespace simulator_app_internal {

std::map<std::string, PeerCountPolicy> InitialPeerCountPolicies(
    const Options& options, const RuntimeNodeSnapshot& nodes);

std::set<std::string> InitialAllPeerPolicyNodeIds(
    const Options& options, const RuntimeNodeSnapshot& nodes);

PeerConnectivityController::AllowedPeerMap InitialAllowedPeers(
    const RuntimePeerTopology& topology, const RuntimeNodeSnapshot& nodes);

}  // namespace simulator_app_internal
}  // namespace bbp
