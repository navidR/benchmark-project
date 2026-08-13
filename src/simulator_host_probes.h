#pragma once

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <string>
#include <vector>

#include "bbp/network.h"

namespace bbp::simulator_app_internal {

boost::json::array LinksJson(const std::vector<LinkInfo>& links);
boost::json::array AddressesJson(const std::vector<AddressInfo>& addresses);
boost::json::array RoutesJson(const std::vector<RouteInfo>& routes);
boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs);
boost::json::array TcFiltersJson(const std::vector<TcFilterInfo>& filters);

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
