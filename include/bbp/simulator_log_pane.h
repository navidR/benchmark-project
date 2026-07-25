#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace bbp {

struct SimulatorLogVisualRow {
  std::size_t record_index = 0U;
  std::size_t byte_offset = 0U;
  std::string text;
  bool starts_record = false;
};

class SimulatorLogPane {
 public:
  void Refresh(const std::vector<std::string>& records,
               std::size_t content_width, std::size_t visible_rows);

  void ScrollUp(std::size_t row_count);
  void ScrollDown(std::size_t row_count);
  void ScrollHome();
  void ScrollEnd();

  [[nodiscard]] const std::vector<SimulatorLogVisualRow>& Rows() const;
  [[nodiscard]] std::size_t FirstVisibleRow() const;
  [[nodiscard]] std::size_t LastVisibleRow() const;
  [[nodiscard]] std::size_t VisibleRowCount() const;
  [[nodiscard]] bool IsFollowingTail() const;

 private:
  [[nodiscard]] std::size_t MaximumFirstVisibleRow() const;

  std::vector<std::string> records_;
  std::vector<SimulatorLogVisualRow> rows_;
  std::size_t visible_rows_ = 0U;
  std::size_t first_visible_row_ = 0U;
  bool follow_tail_ = true;
};

}  // namespace bbp
