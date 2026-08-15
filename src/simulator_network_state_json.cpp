#include "simulator_network_state_json.h"

#include <boost/json/object.hpp>
#include <utility>

#include "simulator_network_event_details.h"

namespace bbp::simulator_app_internal {

boost::json::array LinksJson(const std::vector<LinkInfo>& links) {
  boost::json::array links_json;
  for (const LinkInfo& link : links) {
    boost::json::object link_json;
    link_json["index"] = link.index;
    link_json["name"] = link.name;
    link_json["ownership_alias"] = link.ownership_alias;
    link_json["up"] = link.up;
    link_json["has_stats"] = link.has_stats;
    link_json["rx_bytes"] = link.rx_bytes;
    link_json["tx_bytes"] = link.tx_bytes;
    link_json["rx_packets"] = link.rx_packets;
    link_json["tx_packets"] = link.tx_packets;
    link_json["rx_dropped"] = link.rx_dropped;
    link_json["tx_dropped"] = link.tx_dropped;
    link_json["rx_errors"] = link.rx_errors;
    link_json["tx_errors"] = link.tx_errors;
    links_json.push_back(std::move(link_json));
  }
  return links_json;
}

boost::json::array AddressesJson(const std::vector<AddressInfo>& addresses) {
  boost::json::array addresses_json;
  for (const AddressInfo& address : addresses) {
    boost::json::object address_json;
    address_json["if_index"] = address.if_index;
    address_json["if_name"] = address.if_name;
    address_json["address"] = address.address;
    address_json["prefix_len"] = address.prefix_len;
    addresses_json.push_back(std::move(address_json));
  }
  return addresses_json;
}

boost::json::array RoutesJson(const std::vector<RouteInfo>& routes) {
  boost::json::array routes_json;
  for (const RouteInfo& route : routes) {
    boost::json::object route_json;
    route_json["destination"] = route.destination;
    route_json["prefix_len"] = route.prefix_len;
    route_json["oif_index"] = route.oif_index;
    route_json["oif_name"] = route.oif_name;
    route_json["gateway"] = route.gateway;
    route_json["table"] = route.table;
    route_json["protocol"] = route.protocol;
    route_json["scope"] = route.scope;
    route_json["type"] = route.type;
    routes_json.push_back(std::move(route_json));
  }
  return routes_json;
}

boost::json::array QdiscsJson(const std::vector<QdiscInfo>& qdiscs) {
  boost::json::array qdiscs_json;
  for (const QdiscInfo& qdisc : qdiscs) {
    boost::json::object qdisc_json = QdiscJson(qdisc);
    qdiscs_json.push_back(std::move(qdisc_json));
  }
  return qdiscs_json;
}

boost::json::array TcFiltersJson(const std::vector<TcFilterInfo>& filters) {
  boost::json::array filters_json;
  for (const TcFilterInfo& filter : filters) {
    boost::json::object filter_json;
    filter_json["if_index"] = filter.if_index;
    filter_json["if_name"] = filter.if_name;
    filter_json["kind"] = filter.kernel_kind;
    filter_json["handle"] = filter.handle;
    filter_json["parent"] = filter.parent;
    filter_json["priority"] = filter.priority;
    filter_json["protocol"] = filter.protocol;
    filter_json["egress"] = filter.egress;
    filter_json["ingress"] = filter.ingress;
    filter_json["has_eth_type"] = filter.has_eth_type;
    filter_json["eth_type"] = filter.eth_type;
    filter_json["has_ip_proto"] = filter.has_ip_proto;
    filter_json["ip_proto"] = filter.ip_proto;
    filter_json["has_ipv4_src"] = filter.has_ipv4_src;
    filter_json["ipv4_src"] = filter.ipv4_src;
    filter_json["has_ipv4_src_mask"] = filter.has_ipv4_src_mask;
    filter_json["ipv4_src_mask"] = filter.ipv4_src_mask;
    filter_json["has_ipv4_dst"] = filter.has_ipv4_dst;
    filter_json["ipv4_dst"] = filter.ipv4_dst;
    filter_json["has_ipv4_dst_mask"] = filter.has_ipv4_dst_mask;
    filter_json["ipv4_dst_mask"] = filter.ipv4_dst_mask;
    filter_json["has_tcp_src"] = filter.has_tcp_src;
    filter_json["tcp_src"] = filter.tcp_src;
    filter_json["has_tcp_src_mask"] = filter.has_tcp_src_mask;
    filter_json["tcp_src_mask"] = filter.tcp_src_mask;
    filter_json["has_tcp_dst"] = filter.has_tcp_dst;
    filter_json["tcp_dst"] = filter.tcp_dst;
    filter_json["has_tcp_dst_mask"] = filter.has_tcp_dst_mask;
    filter_json["tcp_dst_mask"] = filter.tcp_dst_mask;
    filter_json["has_class_id"] = filter.has_class_id;
    filter_json["class_id"] = filter.class_id;
    filter_json["has_drop_action"] = filter.has_drop_action;
    filter_json["has_stats"] = filter.has_stats;
    filter_json["match_bytes"] = filter.match_bytes;
    filter_json["match_packets"] = filter.match_packets;
    filter_json["drop_packets"] = filter.drop_packets;
    filters_json.push_back(std::move(filter_json));
  }
  return filters_json;
}

}  // namespace bbp::simulator_app_internal
