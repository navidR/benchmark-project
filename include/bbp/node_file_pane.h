#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

enum class NodeFileSection {
  kDataDirectory,
  kConfiguration,
  kLogFiles,
};

std::string_view NodeFileSectionName(NodeFileSection section);

class NodeFilePane {
 public:
  void Toggle(const std::filesystem::path& run_root,
              const boost::json::object& report, std::size_t selected_node);
  void Close();
  void Refresh(const std::filesystem::path& run_root,
               const boost::json::object& report, std::size_t selected_node);
  void Reload(const std::filesystem::path& run_root,
              const boost::json::object& report, std::size_t selected_node);

  void PreviousSection();
  void NextSection();
  void ScrollUp(std::size_t visible_rows, std::size_t line_count);
  void ScrollDown(std::size_t visible_rows, std::size_t line_count);
  void ScrollHome();
  void ScrollEnd(std::size_t visible_rows);

  [[nodiscard]] bool IsOpen() const;
  [[nodiscard]] std::string_view NodeId() const;
  [[nodiscard]] NodeFileSection Section() const;
  [[nodiscard]] const std::vector<std::string>& Lines() const;
  [[nodiscard]] std::size_t FirstVisibleLine(std::size_t visible_rows) const;
  [[nodiscard]] std::size_t LastVisibleLine(std::size_t visible_rows) const;

 private:
  void Load(const std::filesystem::path& run_root,
            const boost::json::object& report, std::size_t selected_node,
            bool reset_section);
  [[nodiscard]] std::size_t MaximumScroll(std::size_t visible_rows) const;

  bool open_ = false;
  NodeFileSection section_ = NodeFileSection::kDataDirectory;
  std::string node_id_;
  std::vector<std::string> data_lines_;
  std::vector<std::string> configuration_lines_;
  std::vector<std::string> log_lines_;
  std::size_t first_visible_line_ = 0U;
};

}  // namespace bbp
