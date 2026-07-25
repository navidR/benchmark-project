#include "bbp/simulator_log_pane.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <optional>
#include <string_view>

namespace bbp {
namespace {

struct VisualRowAnchor {
  std::size_t record_index = 0U;
  std::size_t byte_offset = 0U;
  std::string record;
};

bool IsWrapBoundary(unsigned char byte) { return std::isspace(byte) != 0; }

std::size_t WrappedRowLength(std::string_view remaining,
                             std::size_t content_width) {
  if (remaining.size() <= content_width) {
    return remaining.size();
  }
  for (std::size_t offset = content_width; offset > 0U; --offset) {
    if (IsWrapBoundary(static_cast<unsigned char>(remaining[offset - 1U]))) {
      return offset;
    }
  }
  return content_width;
}

std::vector<SimulatorLogVisualRow> WrapRecords(
    const std::vector<std::string>& records, std::size_t content_width) {
  std::vector<SimulatorLogVisualRow> rows;
  if (content_width == 0U) {
    return rows;
  }
  for (std::size_t record_index = 0U; record_index < records.size();
       ++record_index) {
    const std::string& record = records[record_index];
    if (record.empty()) {
      rows.push_back(SimulatorLogVisualRow{
          .record_index = record_index,
          .byte_offset = 0U,
          .text = {},
          .starts_record = true,
      });
      continue;
    }
    std::size_t byte_offset = 0U;
    while (byte_offset < record.size()) {
      const std::string_view remaining(record.data() + byte_offset,
                                       record.size() - byte_offset);
      const std::size_t length = WrappedRowLength(remaining, content_width);
      rows.push_back(SimulatorLogVisualRow{
          .record_index = record_index,
          .byte_offset = byte_offset,
          .text = std::string(remaining.substr(0U, length)),
          .starts_record = byte_offset == 0U,
      });
      byte_offset += length;
    }
  }
  return rows;
}

std::optional<std::size_t> FindAnchorRecord(
    const std::vector<std::string>& records, const VisualRowAnchor& anchor) {
  if (anchor.record_index < records.size() &&
      records[anchor.record_index] == anchor.record) {
    return anchor.record_index;
  }

  std::optional<std::size_t> closest;
  std::size_t closest_distance = std::numeric_limits<std::size_t>::max();
  for (std::size_t index = 0U; index < records.size(); ++index) {
    if (records[index] != anchor.record) {
      continue;
    }
    const std::size_t distance = index > anchor.record_index
                                     ? index - anchor.record_index
                                     : anchor.record_index - index;
    if (!closest || distance < closest_distance) {
      closest = index;
      closest_distance = distance;
    }
  }
  return closest;
}

std::optional<std::size_t> FindAnchorRow(
    const std::vector<SimulatorLogVisualRow>& rows, std::size_t record_index,
    std::size_t byte_offset) {
  std::optional<std::size_t> result;
  for (std::size_t index = 0U; index < rows.size(); ++index) {
    const SimulatorLogVisualRow& row = rows[index];
    if (row.record_index != record_index) {
      continue;
    }
    if (row.byte_offset > byte_offset) {
      break;
    }
    result = index;
  }
  return result;
}

}  // namespace

void SimulatorLogPane::Refresh(const std::vector<std::string>& records,
                               std::size_t content_width,
                               std::size_t visible_rows) {
  std::optional<VisualRowAnchor> anchor;
  if (!follow_tail_ && first_visible_row_ < rows_.size()) {
    const SimulatorLogVisualRow& row = rows_[first_visible_row_];
    if (row.record_index < records_.size()) {
      anchor = VisualRowAnchor{
          .record_index = row.record_index,
          .byte_offset = row.byte_offset,
          .record = records_[row.record_index],
      };
    }
  }

  records_ = records;
  rows_ = WrapRecords(records_, content_width);
  visible_rows_ = visible_rows;
  if (follow_tail_) {
    first_visible_row_ = MaximumFirstVisibleRow();
    return;
  }

  if (anchor) {
    const std::optional<std::size_t> record_index =
        FindAnchorRecord(records_, *anchor);
    if (record_index) {
      const std::optional<std::size_t> row_index =
          FindAnchorRow(rows_, *record_index, anchor->byte_offset);
      if (row_index) {
        first_visible_row_ = *row_index;
      }
    }
  }
  first_visible_row_ = std::min(first_visible_row_, MaximumFirstVisibleRow());
}

void SimulatorLogPane::ScrollUp(std::size_t row_count) {
  first_visible_row_ =
      row_count >= first_visible_row_ ? 0U : first_visible_row_ - row_count;
  follow_tail_ = false;
}

void SimulatorLogPane::ScrollDown(std::size_t row_count) {
  const std::size_t maximum = MaximumFirstVisibleRow();
  first_visible_row_ = std::min(maximum, first_visible_row_ + row_count);
  follow_tail_ = first_visible_row_ == maximum;
}

void SimulatorLogPane::ScrollHome() {
  first_visible_row_ = 0U;
  follow_tail_ = rows_.size() <= visible_rows_;
}

void SimulatorLogPane::ScrollEnd() {
  follow_tail_ = true;
  first_visible_row_ = MaximumFirstVisibleRow();
}

const std::vector<SimulatorLogVisualRow>& SimulatorLogPane::Rows() const {
  return rows_;
}

std::size_t SimulatorLogPane::FirstVisibleRow() const {
  return std::min(first_visible_row_, rows_.size());
}

std::size_t SimulatorLogPane::LastVisibleRow() const {
  return std::min(rows_.size(), FirstVisibleRow() + visible_rows_);
}

std::size_t SimulatorLogPane::VisibleRowCount() const { return visible_rows_; }

bool SimulatorLogPane::IsFollowingTail() const { return follow_tail_; }

std::size_t SimulatorLogPane::MaximumFirstVisibleRow() const {
  return rows_.size() > visible_rows_ ? rows_.size() - visible_rows_ : 0U;
}

}  // namespace bbp
