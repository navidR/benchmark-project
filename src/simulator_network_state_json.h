#pragma once

#include <boost/json/array.hpp>
#include <vector>

#include "bbp/network.h"

namespace bbp::simulator_app_internal {

boost::json::array LinksJson(const std::vector<LinkInfo>& links);
boost::json::array AddressesJson(const std::vector<AddressInfo>& addresses);
boost::json::array RoutesJson(const std::vector<RouteInfo>& routes);
boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs);
boost::json::array TcFiltersJson(const std::vector<TcFilterInfo>& filters);

}  // namespace bbp::simulator_app_internal
