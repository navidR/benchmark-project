#include "benchmark_sim/tui.h"

#include "benchmark_sim/run_report.h"

#include <ncursesw/curses.h>

#include <algorithm>
#include <chrono>
#include <clocale>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>

namespace bsim {
namespace {

constexpr int kColorTitle = 1;
constexpr int kColorOk = 2;
constexpr int kColorWarning = 3;
constexpr int kColorMuted = 4;

class CursesSession {
 public:
  CursesSession() {
    std::setlocale(LC_ALL, "");
    if (initscr() == nullptr) {
      throw std::runtime_error("ncurses initialization failed");
    }
    active_ = true;
    cbreak();
    noecho();
    keypad(stdscr, TRUE);
    nodelay(stdscr, TRUE);
    curs_set(0);
    if (has_colors() == TRUE) {
      start_color();
      use_default_colors();
      init_pair(kColorTitle, COLOR_CYAN, -1);
      init_pair(kColorOk, COLOR_GREEN, -1);
      init_pair(kColorWarning, COLOR_YELLOW, -1);
      init_pair(kColorMuted, COLOR_WHITE, -1);
    }
  }

  CursesSession(const CursesSession&) = delete;
  CursesSession& operator=(const CursesSession&) = delete;

  ~CursesSession() {
    if (active_) {
      endwin();
    }
  }

 private:
  bool active_ = false;
};

std::string JsonString(const boost::json::object& object,
                       std::string_view field,
                       std::string_view fallback = "") {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    return std::string(fallback);
  }
  return std::string(value->as_string());
}

std::string JsonBoolText(const boost::json::object& object,
                         std::string_view field,
                         std::string_view fallback = "unknown") {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_bool()) {
    return std::string(fallback);
  }
  return value->as_bool() ? "true" : "false";
}

std::string JsonIntegerText(const boost::json::object& object,
                            std::string_view field,
                            std::string_view fallback = "-") {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::string(fallback);
  }
  if (value->is_uint64()) {
    return std::to_string(value->as_uint64());
  }
  if (value->is_int64()) {
    return std::to_string(value->as_int64());
  }
  return std::string(fallback);
}

std::string JsonMetricText(const boost::json::object& object,
                           std::string_view field,
                           std::string_view fallback = "-") {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || value->is_null()) {
    return std::string(fallback);
  }
  if (value->is_string()) {
    return std::string(value->as_string());
  }
  if (value->is_bool()) {
    return value->as_bool() ? "true" : "false";
  }
  if (value->is_uint64()) {
    return std::to_string(value->as_uint64());
  }
  if (value->is_int64()) {
    return std::to_string(value->as_int64());
  }
  if (value->is_double()) {
    return std::to_string(value->as_double());
  }
  return boost::json::serialize(*value);
}

void AddText(int y, int x, int width, std::string_view text, int attributes) {
  if (width <= 0 || y < 0 || x < 0) {
    return;
  }
  int rows = 0;
  int cols = 0;
  getmaxyx(stdscr, rows, cols);
  if (y >= rows || x >= cols) {
    return;
  }
  const int clipped_width = std::min(width, cols - x);
  if (clipped_width <= 0) {
    return;
  }
  const std::size_t count =
      std::min(text.size(), static_cast<std::size_t>(clipped_width));
  if (attributes != 0) {
    attron(attributes);
  }
  mvaddnstr(y, x, std::string(text.substr(0, count)).c_str(),
            static_cast<int>(count));
  if (attributes != 0) {
    attroff(attributes);
  }
}

void AddText(int y, int x, int width, std::string_view text) {
  AddText(y, x, width, text, 0);
}

void DrawHorizontalLine(int y) {
  int rows = 0;
  int cols = 0;
  getmaxyx(stdscr, rows, cols);
  if (y >= 0 && y < rows && cols > 0) {
    mvhline(y, 0, ACS_HLINE, cols);
  }
}

boost::json::object LoadReport(const std::filesystem::path& run_root,
                               std::string* error) {
  try {
    boost::json::value value = boost::json::parse(BuildRunReportJson(run_root));
    if (!value.is_object()) {
      *error = "run report root is not a JSON object";
      return {};
    }
    error->clear();
    return value.as_object();
  } catch (const std::exception& e) {
    *error = e.what();
    return {};
  }
}

void DrawSummary(const std::filesystem::path& run_root,
                 const boost::json::object& report, std::string_view error) {
  int rows = 0;
  int cols = 0;
  getmaxyx(stdscr, rows, cols);
  erase();

  AddText(0, 0, cols, "Benchmark Project TUI", COLOR_PAIR(kColorTitle) | A_BOLD);
  DrawHorizontalLine(1);

  if (!error.empty()) {
    AddText(3, 0, cols, "run: " + run_root.string(), A_BOLD);
    AddText(5, 0, cols, "error: " + std::string(error),
            COLOR_PAIR(kColorWarning) | A_BOLD);
    refresh();
    return;
  }

  const std::string status = JsonString(report, "status", "unknown");
  const bool ok = JsonBoolText(report, "ok", "false") == "true";
  AddText(3, 0, cols, "run: " + run_root.string(), A_BOLD);
  AddText(4, 0, cols / 2, "status: " + status,
          ok ? COLOR_PAIR(kColorOk) | A_BOLD
             : COLOR_PAIR(kColorWarning) | A_BOLD);
  AddText(4, cols / 2, cols - (cols / 2),
          "chain: " + JsonString(report, "chain", "-"));
  AddText(5, 0, cols / 2,
          "events: " + JsonIntegerText(report, "event_count"));
  AddText(5, cols / 2, cols - (cols / 2),
          "metrics: " + JsonIntegerText(report, "metric_count"));
  AddText(6, 0, cols / 2,
          "nodes: " + JsonIntegerText(report, "nodes"));
  AddText(6, cols / 2, cols - (cols / 2),
          "isolated: " + JsonBoolText(report, "isolated_network"));

  DrawHorizontalLine(8);
  AddText(9, 0, 12, "Node", A_BOLD);
  AddText(9, 12, 12, "State", A_BOLD);
  AddText(9, 24, 10, "Height", A_BOLD);
  AddText(9, 34, 8, "Peers", A_BOLD);
  AddText(9, 42, 10, "Blocks", A_BOLD);
  AddText(9, 52, 10, "RPC ms", A_BOLD);
  AddText(9, 62, std::max(0, cols - 62), "Qdisc", A_BOLD);
  DrawHorizontalLine(10);

  const boost::json::value* nodes_value = report.if_contains("nodes_summary");
  if (nodes_value == nullptr || !nodes_value->is_array()) {
    AddText(12, 0, cols, "No node summaries in report.",
            COLOR_PAIR(kColorMuted));
    refresh();
    return;
  }

  int y = 11;
  for (const boost::json::value& node_value : nodes_value->as_array()) {
    if (y >= rows - 2) {
      break;
    }
    if (!node_value.is_object()) {
      continue;
    }
    const boost::json::object& node = node_value.as_object();
    const boost::json::value* metrics_value = node.if_contains("last_metrics");
    const boost::json::object* metrics = nullptr;
    if (metrics_value != nullptr && metrics_value->is_object()) {
      metrics = &metrics_value->as_object();
    }
    const boost::json::object empty_metrics;
    const boost::json::object& metric_object =
        metrics == nullptr ? empty_metrics : *metrics;

    AddText(y, 0, 12, JsonString(node, "node_id", "-"));
    AddText(y, 12, 12, JsonString(node, "final_state", "-"));
    AddText(y, 24, 10, JsonMetricText(metric_object, "height"));
    AddText(y, 34, 8, JsonMetricText(metric_object, "peer_count"));
    AddText(y, 42, 10,
            JsonMetricText(metric_object, "generated_block_count"));
    AddText(y, 52, 10, JsonMetricText(metric_object, "rpc_latency_ms"));
    AddText(y, 62, std::max(0, cols - 62),
            JsonMetricText(metric_object, "qdisc_kind"));
    ++y;
  }

  DrawHorizontalLine(rows - 2);
  AddText(rows - 1, 0, cols, "Read-only run report. q or Esc exits.",
          COLOR_PAIR(kColorMuted));
  refresh();
}

bool ShouldExit(int ch) {
  return ch == 'q' || ch == 'Q' || ch == 27;
}

}  // namespace

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms) {
  CursesSession curses;
  const std::uint32_t sleep_step_ms = 50;

  while (true) {
    std::string error;
    const boost::json::object report = LoadReport(run_root, &error);
    DrawSummary(run_root, report, error);
    if (once) {
      return error.empty() ? 0 : 1;
    }

    std::uint32_t slept_ms = 0;
    while (slept_ms < refresh_ms) {
      const int ch = getch();
      if (ShouldExit(ch)) {
        return 0;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_step_ms));
      slept_ms += sleep_step_ms;
    }
  }
}

}  // namespace bsim
