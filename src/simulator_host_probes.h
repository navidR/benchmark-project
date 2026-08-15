#pragma once

#include <string>

namespace bbp::simulator_app_internal {

std::string NetworkProbeJson();
std::string CapabilityProbeJson();
std::string CgroupFreezeProbeJson();
std::string NetworkNamespaceProbeJson();
std::string VethProbeJson();
std::string NetworkConditionProbeJson();
std::string CombinedNetworkConditionProbeJson();
std::string DirectionalNetworkPolicyProbeJson();
std::string BandwidthLimitProbeJson();
std::string NetworkConditionUpdateProbeJson();
std::string DropFilterProbeJson();
std::string QdiscProbeJson();
std::string QdiscMutationProbeJson();
std::string RouteProbeJson();
std::string AddressProbeJson();

}  // namespace bbp::simulator_app_internal
