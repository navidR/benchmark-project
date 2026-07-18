#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

struct NetworkRuleSummary {
  std::uint32_t handle = 0;
  std::string source_address;
  std::uint16_t source_port = 0;
  std::string destination_address;
  std::uint16_t destination_port = 0;
  std::uint64_t match_packets = 0;
  std::uint64_t drop_packets = 0;
};

class NetworkRulePane {
 public:
  void Toggle(const boost::json::object& report, std::size_t selected_node);
  void Close();
  void Refresh(const boost::json::object& report, std::size_t selected_node);

  void ScrollUp(std::size_t visible_rows, std::size_t line_count);
  void ScrollDown(std::size_t visible_rows, std::size_t line_count);
  void ScrollHome();
  void ScrollEnd(std::size_t visible_rows);

  [[nodiscard]] bool IsOpen() const;
  [[nodiscard]] std::string_view NodeId() const;
  [[nodiscard]] const std::vector<NetworkRuleSummary>& Rules() const;
  [[nodiscard]] std::size_t FirstVisibleRule(std::size_t visible_rows) const;
  [[nodiscard]] std::size_t LastVisibleRule(std::size_t visible_rows) const;

 private:
  void Load(const boost::json::object& report, std::size_t selected_node);
  [[nodiscard]] std::size_t MaximumScroll(std::size_t visible_rows) const;

  bool open_ = false;
  std::string node_id_;
  std::vector<NetworkRuleSummary> rules_;
  std::size_t first_visible_rule_ = 0;
};

}  // namespace bbp
