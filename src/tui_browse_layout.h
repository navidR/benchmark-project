#pragma once

#include <algorithm>
#include <array>
#include <boost/json.hpp>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace bbp::browse {
struct Rect {
  int x = 0, y = 0, width = 0, height = 0;
  int Columns() const { return width ? std::max(1, width - 2) : 80; }
  int Rows() const { return std::max(0, height - 2); }
};

// Each explorer side has two stacked panes. Short terminals expose the
// focused pane; the caller places both sides together on wide terminals.
inline std::array<Rect, 4> Layout(int rows, int columns, unsigned focus,
                                  bool pool) {
  std::array<Rect, 4> result{};
  const int height = std::max(0, rows - 4);
  if (height >= 10) {
    const int top = height / 2;
    result[0] = {0, 3, columns, top};
    const unsigned detail = pool ? (focus == 1 ? 1 : 2)
                                 : (focus == 1   ? 1
                                    : focus == 3 ? 3
                                                 : 2);
    result[detail] = {0, 3 + top, columns, height - top};
  } else {
    result[focus] = {0, 3, columns, height};
  }
  return result;
}
inline std::string Text(const boost::json::object& object,
                        std::string_view key) {
  const auto* value = object.if_contains(key);
  if (!value || value->is_null()) return "N/A";
  if (value->is_string()) return std::string(value->as_string());
  if (value->is_double()) {
    std::ostringstream out;
    out << std::setprecision(6) << value->as_double();
    return out.str();
  }
  return boost::json::serialize(*value);
}
inline const boost::json::object* Object(const boost::json::object& object,
                                         std::string_view key) {
  const auto* value = object.if_contains(key);
  return value && value->is_object() ? &value->as_object() : nullptr;
}
inline std::optional<std::uint64_t> Uint(const boost::json::object& object,
                                         std::string_view key) {
  const auto* value = object.if_contains(key);
  if (!value || value->is_null()) return {};
  return boost::json::value_to<std::uint64_t>(*value);
}
inline std::string Age(const boost::json::object& object, std::string_view key,
                       std::uint64_t now) {
  const auto timestamp = Uint(object, key);
  if (!timestamp) return "N/A";
  return now >= *timestamp ? std::to_string(now - *timestamp) + "s" : "future";
}
inline std::string Clean(std::string text) {
  for (auto& c : text)
    if (static_cast<unsigned char>(c) < 32 ||
        static_cast<unsigned char>(c) >= 127)
      c = '?';
  return text;
}
inline std::string Cell(std::string text, int width) {
  text = Clean(std::move(text));
  const auto size = static_cast<std::size_t>(std::max(0, width));
  if (text.size() > size) {
    if (size > 6)
      text = text.substr(0, size - 5) + "~" + text.substr(text.size() - 4);
    else
      text = text.substr(0, size);
  }
  text.resize(size, ' ');
  return text;
}
struct Column {
  std::string key, label;
  int width;
};
// Keep identity and the most useful fields first; discard lower priorities.
inline std::vector<Column> Columns(int width, std::vector<Column> columns) {
  int used = 0;
  std::size_t count = 0;
  for (const auto& column : columns) {
    if (used + column.width + (count ? 1 : 0) > width) break;
    used += column.width + (count ? 1 : 0);
    ++count;
  }
  columns.resize(std::max<std::size_t>(1, count));
  if (!count)
    columns.front().width = width;
  else
    columns.front().width += width - used;
  return columns;
}
inline std::string TableRow(const std::vector<Column>& columns,
                            const boost::json::object* object = nullptr) {
  std::string result;
  for (const auto& column : columns) {
    if (!result.empty()) result += ' ';
    result +=
        Cell(object ? Text(*object, column.key) : column.label, column.width);
  }
  return result;
}
inline void Wrap(std::vector<std::string>& target, std::string text,
                 int width) {
  text = Clean(std::move(text));
  const auto size = static_cast<std::size_t>(std::max(1, width));
  if (text.empty()) target.emplace_back();
  for (std::size_t i = 0; i < text.size(); i += size)
    target.push_back(text.substr(i, size));
}
// Consume only the normalized public contract. Recursion keeps every optional
// field, relationship and future driver-owned display section reachable.
inline void Fields(std::vector<std::string>& target,
                   const boost::json::object& object, int width,
                   std::initializer_list<std::string_view> excluded = {}) {
  for (const auto& field : object) {
    if (std::find(excluded.begin(), excluded.end(), field.key()) !=
        excluded.end())
      continue;
    std::string label(field.key());
    std::replace(label.begin(), label.end(), '_', ' ');
    if (field.value().is_object()) {
      Wrap(target, "[" + label + "]", width);
      Fields(target, field.value().as_object(), width);
    } else if (field.value().is_array() && !field.value().as_array().empty()) {
      Wrap(target, label + ":", width);
      std::size_t i = 0;
      for (const auto& value : field.value().as_array()) {
        Wrap(target,
             "  " + std::to_string(++i) + ": " +
                 (value.is_string() ? std::string(value.as_string())
                                    : boost::json::serialize(value)),
             width);
      }
    } else {
      Wrap(target, label + ": " + Text(object, field.key()), width);
    }
  }
}
inline std::string Position(std::size_t first, std::size_t visible,
                            std::size_t count) {
  if (!count) return "0/0";
  return std::to_string(first + 1) + "-" +
         std::to_string(std::min(count, first + visible)) + "/" +
         std::to_string(count);
}
struct Line {
  std::string text;
  bool selected = false;
  int selected_column = 0, selected_width = 0;
};
class Canvas {
 public:
  Canvas(int rows, int columns)
      : lines(static_cast<std::size_t>(std::max(0, rows))),
        columns_(std::max(0, columns)) {}
  void Put(int row, int column, int width, std::string text,
           bool selected = false) {
    if (row < 0 || row >= static_cast<int>(lines.size()) || column < 0 ||
        column >= columns_ || width <= 0)
      return;
    width = std::min(width, columns_ - column);
    auto& line = lines[static_cast<std::size_t>(row)];
    line.text.resize(static_cast<std::size_t>(columns_), ' ');
    line.text.replace(static_cast<std::size_t>(column),
                      static_cast<std::size_t>(width), Cell(text, width));
    if (selected) {
      line.selected = true;
      line.selected_column = column;
      line.selected_width = width;
    }
  }
  void Frame(Rect rect, std::string title, bool focused,
             const std::string& position = {}) {
    if (rect.width < 2 || rect.height < 2) return;
    for (int i = 1; i < rect.height - 1; ++i) {
      Put(rect.y + i, rect.x, 1, "|");
      Put(rect.y + i, rect.x + rect.width - 1, 1, "|");
    }
    std::string border(static_cast<std::size_t>(rect.width), '-');
    border.front() = border.back() = '+';
    Put(rect.y, rect.x, rect.width, border);
    Put(rect.y + rect.height - 1, rect.x, rect.width, border);
    title = (focused ? "> " : "  ") + title;
    const int available = std::max(1, rect.width - 4);
    if (title.size() + position.size() + 1 <=
        static_cast<std::size_t>(available)) {
      title.resize(static_cast<std::size_t>(available) - position.size(), ' ');
      title += position;
    }
    Put(rect.y, rect.x + 1, rect.width - 2, title);
    // Position remains visible even if a small title bar cannot hold both.
    if (!position.empty())
      Put(rect.y + rect.height - 1, rect.x + 1, rect.width - 2, position);
  }
  void Body(Rect rect, int row, std::string text, bool selected = false) {
    if (row >= 0 && row < rect.Rows())
      Put(rect.y + 1 + row, rect.x + 1, rect.Columns(), std::move(text),
          selected);
  }
  std::size_t Section(Rect rect, const std::string& title, bool focused,
                      const std::vector<std::string>& text,
                      std::size_t offset) {
    const auto count = static_cast<std::size_t>(rect.Rows());
    const auto maximum = text.size() > count ? text.size() - count : 0;
    offset = std::min(offset, maximum);
    Frame(rect, title, focused, Position(offset, count, text.size()));
    for (std::size_t i = 0; i < count && offset + i < text.size(); ++i)
      Body(rect, static_cast<int>(i), text[offset + i]);
    return maximum;
  }
  std::vector<Line> lines;

 private:
  int columns_;
};
}  // namespace bbp::browse
