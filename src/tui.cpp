#include "benchmark_sim/tui.h"

#include <ncursesw/curses.h>

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <clocale>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "benchmark_sim/log_view.h"
#include "benchmark_sim/run_report.h"

namespace bsim {
namespace {

constexpr int kColorTitle = 1;
constexpr int kColorOk = 2;
constexpr int kColorWarning = 3;
constexpr int kColorMuted = 4;
constexpr int kMinLogPaneRows = 5;
constexpr int kMaxLogPaneRows = 10;

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
                       std::string_view field, std::string_view fallback = "") {
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

std::optional<std::uint64_t> JsonUnsignedMetric(
    const boost::json::object& object, std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || value->is_null()) {
    return std::nullopt;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  return std::nullopt;
}

std::string JsonBytesMiBText(const boost::json::object& object,
                             std::string_view field) {
  const std::optional<std::uint64_t> bytes = JsonUnsignedMetric(object, field);
  if (!bytes) {
    return "-";
  }
  constexpr std::uint64_t kMiB = 1024ULL * 1024ULL;
  return std::to_string((*bytes + kMiB - 1U) / kMiB) + "M";
}

std::string JsonBytesKiBText(const boost::json::object& object,
                             std::string_view field) {
  const std::optional<std::uint64_t> bytes = JsonUnsignedMetric(object, field);
  if (!bytes) {
    return "-";
  }
  constexpr std::uint64_t kKiB = 1024ULL;
  return std::to_string((*bytes + kKiB - 1U) / kKiB) + "K";
}

std::string JsonUsecMillisText(const boost::json::object& object,
                               std::string_view field) {
  const std::optional<std::uint64_t> usec = JsonUnsignedMetric(object, field);
  if (!usec) {
    return "-";
  }
  return std::to_string(*usec / 1000ULL);
}

std::string WorkloadsSummaryText(const boost::json::object& report) {
  const boost::json::value* workloads_value = report.if_contains("workloads");
  if (workloads_value == nullptr || !workloads_value->is_array()) {
    return "workloads: -";
  }

  const boost::json::array& workloads = workloads_value->as_array();
  if (workloads.empty()) {
    return "workloads: 0";
  }
  std::string text = "workloads: " + std::to_string(workloads.size());
  std::size_t index = 0;
  for (const boost::json::value& workload_value : workloads) {
    ++index;
    if (!workload_value.is_object()) {
      continue;
    }
    const boost::json::object& workload = workload_value.as_object();
    text += index == 1 ? " " : ", ";
    text += "#" + std::to_string(index) + " ";
    const std::string type = JsonString(workload, "type", "-");
    if (type == "block_generation") {
      text += "gen n";
      text += JsonMetricText(workload, "node");
      text += " x";
      text += JsonMetricText(workload, "count");
      text += " ";
      text += JsonMetricText(workload, "sync_timeout_sec");
      text += "s";
    } else if (type == "wait_until_height") {
      text += "height n";
      text += JsonMetricText(workload, "node");
      text += ">=";
      text += JsonMetricText(workload, "height");
      text += " ";
      text += JsonMetricText(workload, "timeout_sec");
      text += "s";
    } else if (type == "wait_for_peers") {
      text += "peers n";
      text += JsonMetricText(workload, "node");
      text += ">=";
      text += JsonMetricText(workload, "peer_count");
      text += " ";
      text += JsonMetricText(workload, "timeout_sec");
      text += "s";
    } else if (type == "connect_peer") {
      text += "connect n";
      text += JsonMetricText(workload, "node");
      text += "->n";
      text += JsonMetricText(workload, "peer");
      text += " ";
      text += JsonMetricText(workload, "timeout_sec");
      text += "s";
    } else if (type == "disconnect_peer") {
      text += "disconnect n";
      text += JsonMetricText(workload, "node");
      text += "->n";
      text += JsonMetricText(workload, "peer");
      text += " ";
      text += JsonMetricText(workload, "timeout_sec");
      text += "s";
    } else if (type == "restart_node") {
      text += "restart n";
      text += JsonMetricText(workload, "node");
    } else if (type == "freeze_node") {
      text += "freeze n";
      text += JsonMetricText(workload, "node");
      text += " ";
      text += JsonMetricText(workload, "duration_ms");
      text += "ms";
    } else if (type == "update_resource_limits") {
      text += "limits n";
      text += JsonMetricText(workload, "node");
    } else if (type == "partition_nodes") {
      text += "partition";
    } else if (type == "heal_partition") {
      text += "heal";
    } else if (type == "send_raw_transaction") {
      text += "tx n";
      text += JsonMetricText(workload, "funding_node");
      text += "->n";
      text += JsonMetricText(workload, "submit_node");
      text += " ";
      text += JsonString(workload, "amount", "-");
    } else {
      text += type;
    }
  }
  return text;
}

std::string LifecycleSummaryText(const boost::json::object& report) {
  std::string text = "started: ";
  text += JsonString(report, "started_at", "-");
  text += " finished: ";
  const std::string finished_at = JsonString(report, "finished_at", "");
  if (!finished_at.empty()) {
    text += finished_at;
  } else {
    text += JsonString(report, "failed_at", "-");
  }
  return text;
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

int LogPaneRows(int rows) {
  if (rows < 20) {
    return 0;
  }
  return std::clamp(rows / 4, kMinLogPaneRows, kMaxLogPaneRows);
}

void DrawLogPane(int top, int rows, int cols,
                 const std::vector<std::string>& log_lines) {
  if (top <= 0 || cols <= 0 || rows - top < 4) {
    return;
  }
  DrawHorizontalLine(top);
  AddText(top + 1, 0, cols, "Logs", A_BOLD);
  const int first_line = top + 2;
  const int last_line = rows - 3;
  const int capacity = last_line - first_line + 1;
  if (capacity <= 0) {
    return;
  }
  if (log_lines.empty()) {
    AddText(first_line, 0, cols, "No log output.", COLOR_PAIR(kColorMuted));
    return;
  }
  const std::size_t line_count = static_cast<std::size_t>(capacity);
  const std::size_t first_log =
      log_lines.size() > line_count ? log_lines.size() - line_count : 0U;
  int y = first_line;
  for (std::size_t i = first_log; i < log_lines.size() && y <= last_line; ++i) {
    AddText(y, 0, cols, log_lines[i]);
    ++y;
  }
}

boost::json::object LoadReport(const std::filesystem::path& run_root,
                               std::string* error) {
  try {
    Result<std::string> report_json = BuildRunReportJson(run_root);
    if (!report_json) {
      *error = report_json.error();
      return {};
    }
    boost::json::value value =
        boost::json::parse(std::move(report_json).unsafe_value());
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

std::vector<std::string> LoadRecentLogLines(
    const std::filesystem::path& run_root) {
  Result<std::vector<std::string>> log_lines =
      ReadRecentLogLines(RunLogPath(run_root), 256U);
  if (!log_lines) {
    return {};
  }
  return std::move(log_lines).unsafe_value();
}

void DrawSummary(const std::filesystem::path& run_root,
                 const boost::json::object& report, std::string_view error,
                 const std::vector<std::string>& log_lines) {
  int rows = 0;
  int cols = 0;
  getmaxyx(stdscr, rows, cols);
  erase();
  const int log_rows = LogPaneRows(rows);
  const int log_top = log_rows == 0 ? rows - 2 : rows - log_rows - 2;
  const int content_bottom = log_rows == 0 ? rows - 2 : log_top;

  AddText(0, 0, cols, "Benchmark Project TUI",
          COLOR_PAIR(kColorTitle) | A_BOLD);
  DrawHorizontalLine(1);

  if (!error.empty()) {
    AddText(3, 0, cols, "run: " + run_root.string(), A_BOLD);
    AddText(5, 0, cols, "error: " + std::string(error),
            COLOR_PAIR(kColorWarning) | A_BOLD);
    if (log_rows != 0) {
      DrawLogPane(log_top, rows, cols, log_lines);
    }
    DrawHorizontalLine(rows - 2);
    AddText(rows - 1, 0, cols, "Read-only run report. q or Esc exits.",
            COLOR_PAIR(kColorMuted));
    refresh();
    return;
  }

  const std::string status = JsonString(report, "status", "unknown");
  const bool ok = JsonBoolText(report, "ok", "false") == "true";
  AddText(3, 0, cols, "run: " + run_root.string(), A_BOLD);
  AddText(
      4, 0, cols / 2, "status: " + status,
      ok ? COLOR_PAIR(kColorOk) | A_BOLD : COLOR_PAIR(kColorWarning) | A_BOLD);
  AddText(4, cols / 2, cols - (cols / 2),
          "chain: " + JsonString(report, "chain", "-"));
  AddText(5, 0, cols / 2, "events: " + JsonIntegerText(report, "event_count"));
  AddText(5, cols / 2, cols - (cols / 2),
          "metrics: " + JsonIntegerText(report, "metric_count"));
  AddText(6, 0, cols / 2, "nodes: " + JsonIntegerText(report, "nodes"));
  AddText(6, cols / 2, cols - (cols / 2),
          "isolated: " + JsonBoolText(report, "isolated_network"));
  AddText(7, 0, cols / 2,
          "generate node: " + JsonIntegerText(report, "generate_node"));
  AddText(7, cols / 2, cols - (cols / 2),
          "generate blocks: " + JsonIntegerText(report, "generate_blocks"));
  AddText(8, 0, cols, LifecycleSummaryText(report));
  AddText(9, 0, cols, WorkloadsSummaryText(report));

  DrawHorizontalLine(10);
  AddText(11, 0, 10, "Node", A_BOLD);
  AddText(11, 10, 10, "State", A_BOLD);
  AddText(11, 20, 7, "Height", A_BOLD);
  AddText(11, 27, 6, "Peers", A_BOLD);
  AddText(11, 33, 7, "Blocks", A_BOLD);
  AddText(11, 40, 7, "Pool", A_BOLD);
  AddText(11, 47, 9, "Mem", A_BOLD);
  AddText(11, 56, 9, "CPUms", A_BOLD);
  AddText(11, 65, 8, "RX", A_BOLD);
  AddText(11, 73, std::max(0, cols - 73), "Qdisc", A_BOLD);
  DrawHorizontalLine(12);

  const boost::json::value* nodes_value = report.if_contains("nodes_summary");
  if (nodes_value == nullptr || !nodes_value->is_array()) {
    AddText(13, 0, cols, "No node summaries in report.",
            COLOR_PAIR(kColorMuted));
    if (log_rows != 0) {
      DrawLogPane(log_top, rows, cols, log_lines);
    }
    DrawHorizontalLine(rows - 2);
    AddText(rows - 1, 0, cols, "Read-only run report. q or Esc exits.",
            COLOR_PAIR(kColorMuted));
    refresh();
    return;
  }

  int y = 13;
  for (const boost::json::value& node_value : nodes_value->as_array()) {
    if (y >= content_bottom) {
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

    AddText(y, 0, 10, JsonString(node, "node_id", "-"));
    AddText(y, 10, 10, JsonString(node, "final_state", "-"));
    AddText(y, 20, 7, JsonMetricText(metric_object, "height"));
    AddText(y, 27, 6, JsonMetricText(metric_object, "peer_count"));
    AddText(y, 33, 7, JsonMetricText(metric_object, "generated_block_count"));
    AddText(y, 40, 7, JsonMetricText(metric_object, "mempool_tx_count"));
    AddText(y, 47, 9, JsonBytesMiBText(metric_object, "memory_current"));
    AddText(y, 56, 9, JsonUsecMillisText(metric_object, "cpu_usage_usec"));
    AddText(y, 65, 8, JsonBytesKiBText(metric_object, "network_rx_bytes"));
    AddText(y, 73, std::max(0, cols - 73),
            JsonMetricText(metric_object, "qdisc_kind"));
    ++y;
  }

  if (log_rows != 0) {
    DrawLogPane(log_top, rows, cols, log_lines);
  }
  DrawHorizontalLine(rows - 2);
  AddText(rows - 1, 0, cols, "Read-only run report. q or Esc exits.",
          COLOR_PAIR(kColorMuted));
  refresh();
}

bool ShouldExit(int ch) { return ch == 'q' || ch == 'Q' || ch == 27; }

}  // namespace

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms) {
  CursesSession curses;
  const std::uint32_t sleep_step_ms = 50;

  while (true) {
    std::string error;
    const boost::json::object report = LoadReport(run_root, &error);
    const std::vector<std::string> log_lines = LoadRecentLogLines(run_root);
    DrawSummary(run_root, report, error, log_lines);
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
