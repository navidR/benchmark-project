#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/drivers/chain_driver.h"

namespace bbp {

class NodeLogPane {
 public:
  void Toggle(const boost::json::object& report, std::size_t selected_node);
  void Refresh(const boost::json::object& report, std::size_t selected_node);

  void ScrollUp(std::size_t visible_rows, std::size_t line_count);
  void ScrollDown(std::size_t visible_rows, std::size_t line_count);
  void ScrollHome(std::size_t visible_rows);
  void ScrollEnd();

  [[nodiscard]] bool IsOpen() const;
  [[nodiscard]] std::string_view NodeId() const;
  [[nodiscard]] std::string_view SourceName() const;
  [[nodiscard]] const std::vector<std::string>& Lines() const;
  [[nodiscard]] std::size_t FirstVisibleLine(std::size_t visible_rows) const;
  [[nodiscard]] std::size_t LastVisibleLine(std::size_t visible_rows) const;

 private:
  void Load(const boost::json::object& report, std::size_t selected_node);
  [[nodiscard]] std::size_t MaximumScroll(std::size_t visible_rows) const;

  bool open_ = false;
  ChainLogSource source_ = ChainLogSource::kDaemon;
  std::string node_id_;
  std::vector<std::string> lines_;
  std::size_t lines_from_bottom_ = 0;
};

}  // namespace bbp
