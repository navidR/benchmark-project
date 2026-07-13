#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

class PeerListPane {
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
  [[nodiscard]] std::uint64_t PeerCount() const;
  [[nodiscard]] const std::vector<std::string>& Peers() const;
  [[nodiscard]] std::size_t FirstVisiblePeer(std::size_t visible_rows) const;
  [[nodiscard]] std::size_t LastVisiblePeer(std::size_t visible_rows) const;

 private:
  void Load(const boost::json::object& report, std::size_t selected_node);
  [[nodiscard]] std::size_t MaximumScroll(std::size_t visible_rows) const;

  bool open_ = false;
  std::string node_id_;
  std::uint64_t peer_count_ = 0;
  std::vector<std::string> peers_;
  std::size_t first_visible_peer_ = 0;
};

}  // namespace bbp
