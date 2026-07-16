#include "bbp/tui.h"

#include <ncursesw/curses.h>

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cctype>
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

#include "bbp/drivers/chain_wallet_transaction.h"
#include "bbp/log_view.h"
#include "bbp/node_log_pane.h"
#include "bbp/operator_command_status.h"
#include "bbp/peer_list_pane.h"
#include "bbp/run_report.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulator/workload_kind.h"
#include "bbp/tui_command_parser.h"
#include "bbp/tui_view.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr int kColorTitle = 1;
constexpr int kColorOk = 2;
constexpr int kColorWarning = 3;
constexpr int kColorMuted = 4;
constexpr int kMinLogPaneRows = 5;
constexpr int kMaxLogPaneRows = 10;
constexpr int kDetailPaneRows = 13;

struct TuiState {
  std::size_t selected_node = 0;
  std::size_t selected_wallet = 0;
  TuiView view = TuiView::kNodes;
  NodeLogPane node_log_pane;
  PeerListPane peer_list_pane;
  std::string command_status;
  std::uint64_t last_command_result_sequence = 0;
  bool command_error_open = false;
  std::string command_error;
  bool command_palette_open = false;
  std::string command_input;
  std::string command_input_error;
};

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

std::optional<bool> JsonBool(const boost::json::object& object,
                             std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_bool()) {
    return std::nullopt;
  }
  return value->as_bool();
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

std::string JsonSatoshisText(const boost::json::object& object,
                             std::string_view field,
                             std::string_view fallback = "-") {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::string(fallback);
  }
  if (value->is_uint64()) {
    return FormatFixed8Amount(value->as_uint64());
  }
  if (!value->is_int64()) {
    return std::string(fallback);
  }
  const std::int64_t amount = value->as_int64();
  const std::uint64_t magnitude =
      amount < 0 ? static_cast<std::uint64_t>(-(amount + 1)) + 1U
                 : static_cast<std::uint64_t>(amount);
  return std::string(amount < 0 ? "-" : "") + FormatFixed8Amount(magnitude);
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

std::string JsonStringArrayText(const boost::json::object& object,
                                std::string_view field,
                                std::string_view fallback = "-") {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_array()) {
    return std::string(fallback);
  }
  std::vector<std::string> values;
  values.reserve(value->as_array().size());
  for (const boost::json::value& item : value->as_array()) {
    if (item.is_string()) {
      values.emplace_back(item.as_string());
    }
  }
  return values.empty() ? std::string(fallback)
                        : boost::algorithm::join(values, ", ");
}

std::string PeerListSummaryText(const boost::json::object& metrics) {
  const boost::json::value* value = metrics.if_contains("peer_addresses");
  if (value == nullptr || !value->is_array()) {
    return "-";
  }
  const std::size_t count = value->as_array().size();
  if (count > 1U) {
    return std::to_string(count) + " addresses [p]";
  }
  return JsonStringArrayText(metrics, "peer_addresses");
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

std::string JsonBytesPerSecondText(const boost::json::object& object,
                                   std::string_view field) {
  const std::optional<std::uint64_t> bytes = JsonUnsignedMetric(object, field);
  if (!bytes) {
    return "-";
  }
  constexpr std::uint64_t kKiB = 1024ULL;
  if (*bytes < kKiB) {
    return std::to_string(*bytes) + "B/s";
  }
  return std::to_string((*bytes + kKiB - 1U) / kKiB) + "K/s";
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
    const std::string type_name = JsonString(workload, "type", "-");
    const std::optional<WorkloadKind> kind = ParseWorkloadKind(type_name);
    if (!kind) {
      text += type_name;
      continue;
    }
    switch (*kind) {
      case WorkloadKind::kBlockGeneration:
        text += "gen n";
        text += JsonMetricText(workload, "node");
        text += " x";
        text += JsonMetricText(workload, "count");
        text += " ";
        text += JsonMetricText(workload, "sync_timeout_sec");
        text += "s";
        break;
      case WorkloadKind::kWaitUntilHeight:
        text += "height n";
        text += JsonMetricText(workload, "node");
        text += ">=";
        text += JsonMetricText(workload, "height");
        text += " ";
        text += JsonMetricText(workload, "timeout_sec");
        text += "s";
        break;
      case WorkloadKind::kWaitForPeers:
        text += "peers n";
        text += JsonMetricText(workload, "node");
        text += ">=";
        text += JsonMetricText(workload, "peer_count");
        text += " ";
        text += JsonMetricText(workload, "timeout_sec");
        text += "s";
        break;
      case WorkloadKind::kConnectPeer:
        text += "connect n";
        text += JsonMetricText(workload, "node");
        text += "->n";
        text += JsonMetricText(workload, "peer");
        text += " ";
        text += JsonMetricText(workload, "timeout_sec");
        text += "s";
        break;
      case WorkloadKind::kDisconnectPeer:
        text += "disconnect n";
        text += JsonMetricText(workload, "node");
        text += "->n";
        text += JsonMetricText(workload, "peer");
        text += " ";
        text += JsonMetricText(workload, "timeout_sec");
        text += "s";
        break;
      case WorkloadKind::kRestartNode:
        text += "restart n";
        text += JsonMetricText(workload, "node");
        break;
      case WorkloadKind::kFreezeNode:
        text += "freeze n";
        text += JsonMetricText(workload, "node");
        text += " ";
        text += JsonMetricText(workload, "duration_ms");
        text += "ms";
        break;
      case WorkloadKind::kUpdateResourceLimits:
        text += "limits n";
        text += JsonMetricText(workload, "node");
        break;
      case WorkloadKind::kPartitionNodes:
        text += "partition";
        break;
      case WorkloadKind::kHealPartition:
        text += "heal";
        break;
      case WorkloadKind::kSendRawTransaction:
        text += "tx n";
        text += JsonMetricText(workload, "funding_node");
        text += "->n";
        text += JsonMetricText(workload, "submit_node");
        text += " ";
        text += JsonString(workload, "amount", "-");
        break;
      case WorkloadKind::kResourcePressure:
      case WorkloadKind::kWalletTransactions:
        text += type_name;
        break;
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

const boost::json::array* NodeSummaries(const boost::json::object& report) {
  const boost::json::value* nodes_value = report.if_contains("nodes_summary");
  if (nodes_value == nullptr || !nodes_value->is_array()) {
    return nullptr;
  }
  return &nodes_value->as_array();
}

const boost::json::array* WalletSummaries(const boost::json::object& report) {
  const boost::json::value* wallets_value =
      report.if_contains("wallets_summary");
  if (wallets_value == nullptr || !wallets_value->is_array()) {
    return nullptr;
  }
  return &wallets_value->as_array();
}

const boost::json::object* NodeAt(const boost::json::array& nodes,
                                  std::size_t index) {
  if (index >= nodes.size() || !nodes[index].is_object()) {
    return nullptr;
  }
  return &nodes[index].as_object();
}

void RefreshCommandResults(const boost::json::object& report, TuiState* state) {
  const boost::json::value* commands_value =
      report.if_contains("operator_commands");
  if (commands_value == nullptr || !commands_value->is_array()) {
    return;
  }
  for (const boost::json::value& command_value : commands_value->as_array()) {
    if (!command_value.is_object()) {
      continue;
    }
    const boost::json::object& command = command_value.as_object();
    const boost::json::value* detail_value = command.if_contains("detail");
    if (detail_value == nullptr || !detail_value->is_object()) {
      continue;
    }
    const boost::json::object& detail = detail_value->as_object();
    const std::optional<std::uint64_t> sequence =
        JsonUnsignedMetric(detail, "sequence");
    if (!sequence || *sequence <= state->last_command_result_sequence) {
      continue;
    }
    state->last_command_result_sequence = *sequence;
    const std::optional<OperatorCommandStatus> status =
        OperatorCommandStatusFromName(JsonString(command, "status"));
    if (!status) {
      continue;
    }
    switch (*status) {
      case OperatorCommandStatus::kFailed:
        state->command_error =
            JsonString(detail, "error", "The chain rejected this command.");
        state->command_error_open = true;
        break;
      case OperatorCommandStatus::kCompleted:
        state->command_status =
            "Command #" + std::to_string(*sequence) + " completed for " +
            JsonString(command, "node_id", "selected node") + ".";
        break;
    }
  }
}

std::string SelectedPeerPolicyText(const boost::json::object& report,
                                   std::size_t selected_node,
                                   std::string_view node_id) {
  const boost::json::value* commands_value =
      report.if_contains("operator_commands");
  if (commands_value != nullptr && commands_value->is_array()) {
    const boost::json::array& commands = commands_value->as_array();
    for (auto command = commands.rbegin(); command != commands.rend();
         ++command) {
      if (!command->is_object()) {
        continue;
      }
      const boost::json::object& command_object = command->as_object();
      const std::optional<OperatorCommandStatus> status =
          OperatorCommandStatusFromName(JsonString(command_object, "status"));
      if (!status || *status != OperatorCommandStatus::kCompleted ||
          JsonString(command_object, "node_id") != node_id) {
        continue;
      }
      const boost::json::value* detail_value =
          command_object.if_contains("detail");
      if (detail_value == nullptr || !detail_value->is_object()) {
        continue;
      }
      const boost::json::object& detail = detail_value->as_object();
      const std::optional<SimulationCommandKind> kind =
          SimulationCommandKindFromName(JsonString(detail, "kind"));
      if (!kind || *kind != SimulationCommandKind::kSetPeerCountPolicy) {
        continue;
      }
      return "min " + JsonIntegerText(detail, "minimum_peer_count") +
             " / max " + JsonIntegerText(detail, "maximum_peer_count");
    }
  }

  const boost::json::value* topology_value = report.if_contains("topology");
  if (topology_value == nullptr || !topology_value->is_object()) {
    return "unmanaged";
  }
  const boost::json::value* policies_value =
      topology_value->as_object().if_contains("peer_connectivity");
  if (policies_value == nullptr || !policies_value->is_array()) {
    return "unmanaged";
  }
  for (const boost::json::value& policy_value : policies_value->as_array()) {
    if (!policy_value.is_object()) {
      continue;
    }
    const boost::json::object& policy = policy_value.as_object();
    const std::optional<std::uint64_t> policy_node =
        JsonUnsignedMetric(policy, "node");
    if (!policy_node || *policy_node != selected_node + 1U) {
      continue;
    }
    if (JsonBool(policy, "all_peers").value_or(false)) {
      const boost::json::array* nodes = NodeSummaries(report);
      const std::size_t count =
          nodes == nullptr || nodes->empty() ? 0U : nodes->size() - 1U;
      return "all (" + std::to_string(count) + ")";
    }
    return "min " + JsonIntegerText(policy, "min_peer_count") + " / max " +
           JsonIntegerText(policy, "max_peer_count");
  }
  return "unmanaged";
}

const boost::json::object* WalletForNode(const boost::json::object& report,
                                         std::size_t one_based_node) {
  const boost::json::array* wallets = WalletSummaries(report);
  if (wallets == nullptr) {
    return nullptr;
  }
  for (const boost::json::value& wallet_value : *wallets) {
    if (!wallet_value.is_object()) {
      continue;
    }
    const boost::json::object& wallet = wallet_value.as_object();
    const std::optional<std::uint64_t> node =
        JsonUnsignedMetric(wallet, "node");
    if (node && *node == one_based_node) {
      return &wallet;
    }
  }
  return nullptr;
}

const boost::json::object* WalletAt(const boost::json::array& wallets,
                                    std::size_t index) {
  if (index >= wallets.size() || !wallets[index].is_object()) {
    return nullptr;
  }
  return &wallets[index].as_object();
}

std::optional<std::size_t> WalletNodeIndex(const boost::json::object& wallet) {
  const std::optional<std::uint64_t> one_based_node =
      JsonUnsignedMetric(wallet, "node");
  if (!one_based_node || *one_based_node == 0U) {
    return std::nullopt;
  }
  return static_cast<std::size_t>(*one_based_node - 1U);
}

std::optional<std::size_t> SelectedNodeIndex(const boost::json::object& report,
                                             const TuiState& state) {
  if (state.view == TuiView::kNodes) {
    const boost::json::array* nodes = NodeSummaries(report);
    if (nodes == nullptr || state.selected_node >= nodes->size()) {
      return std::nullopt;
    }
    return state.selected_node;
  }
  const boost::json::array* wallets = WalletSummaries(report);
  const boost::json::object* wallet =
      wallets == nullptr ? nullptr : WalletAt(*wallets, state.selected_wallet);
  if (wallet == nullptr) {
    return std::nullopt;
  }
  return WalletNodeIndex(*wallet);
}

std::size_t ClampNodeSelection(const boost::json::object& report,
                               std::size_t selected_node) {
  const boost::json::array* nodes = NodeSummaries(report);
  if (nodes == nullptr || nodes->empty()) {
    return 0;
  }
  return std::min(selected_node, nodes->size() - 1U);
}

std::size_t ClampWalletSelection(const boost::json::object& report,
                                 std::size_t selected_wallet) {
  const boost::json::array* wallets = WalletSummaries(report);
  if (wallets == nullptr || wallets->empty()) {
    return 0;
  }
  return std::min(selected_wallet, wallets->size() - 1U);
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
  AddText(top + 1, 0, cols, "Simulator Logs", A_BOLD);
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
  const std::size_t visible_lines = log_lines.size() - first_log;
  int y = last_line - static_cast<int>(visible_lines) + 1;
  for (std::size_t i = first_log; i < log_lines.size() && y <= last_line; ++i) {
    AddText(y, 0, cols, log_lines[i]);
    ++y;
  }
}

int NodeLogVisibleRows(int content_bottom) {
  constexpr int kPaneTop = 2;
  constexpr int kBorderAndTitleRows = 3;
  return std::max(0, content_bottom - kPaneTop - kBorderAndTitleRows);
}

void DrawNodeLogPane(int content_bottom, int cols,
                     const NodeLogPane& node_log_pane) {
  constexpr int kPaneTop = 2;
  if (!node_log_pane.IsOpen() || cols <= 0 ||
      NodeLogVisibleRows(content_bottom) <= 0) {
    return;
  }

  for (int y = kPaneTop; y < content_bottom; ++y) {
    mvhline(y, 0, ' ', cols);
  }
  DrawHorizontalLine(kPaneTop);

  const std::size_t visible_rows =
      static_cast<std::size_t>(NodeLogVisibleRows(content_bottom));
  const std::size_t first = node_log_pane.FirstVisibleLine(visible_rows);
  const std::size_t last = node_log_pane.LastVisibleLine(visible_rows);
  std::string title = "Node Log: ";
  title += node_log_pane.NodeId().empty() ? "-" : node_log_pane.NodeId();
  title += " / ";
  title += node_log_pane.SourceName();
  if (!node_log_pane.Lines().empty()) {
    title += " [" + std::to_string(first + 1U) + "-" + std::to_string(last) +
             "/" + std::to_string(node_log_pane.Lines().size()) + "]";
  }
  AddText(kPaneTop + 1, 0, cols, title, A_BOLD);

  if (node_log_pane.Lines().empty()) {
    AddText(kPaneTop + 2, 0, cols, "No node log output.",
            COLOR_PAIR(kColorMuted));
  } else {
    int y = kPaneTop + 2;
    for (std::size_t index = first; index < last; ++index) {
      AddText(y, 0, cols, node_log_pane.Lines()[index]);
      ++y;
    }
  }
  DrawHorizontalLine(content_bottom - 1);
}

void DrawPeerListPane(int content_bottom, int cols,
                      const PeerListPane& peer_list_pane) {
  constexpr int kPaneTop = 2;
  if (!peer_list_pane.IsOpen() || cols <= 0 ||
      NodeLogVisibleRows(content_bottom) <= 0) {
    return;
  }

  for (int y = kPaneTop; y < content_bottom; ++y) {
    mvhline(y, 0, ' ', cols);
  }
  DrawHorizontalLine(kPaneTop);

  const std::size_t visible_rows =
      static_cast<std::size_t>(NodeLogVisibleRows(content_bottom));
  const std::size_t first = peer_list_pane.FirstVisiblePeer(visible_rows);
  const std::size_t last = peer_list_pane.LastVisiblePeer(visible_rows);
  std::string title = "Connected Peers: ";
  title += peer_list_pane.NodeId().empty() ? "-" : peer_list_pane.NodeId();
  title += " / " + std::to_string(peer_list_pane.PeerCount());
  if (!peer_list_pane.Peers().empty()) {
    title += " [" + std::to_string(first + 1U) + "-" + std::to_string(last) +
             "/" + std::to_string(peer_list_pane.Peers().size()) + "]";
  }
  AddText(kPaneTop + 1, 0, cols, title, A_BOLD);

  if (peer_list_pane.Peers().empty()) {
    AddText(kPaneTop + 2, 0, cols, "No connected peer addresses reported.",
            COLOR_PAIR(kColorMuted));
  } else {
    int y = kPaneTop + 2;
    for (std::size_t index = first; index < last; ++index) {
      AddText(
          y, 0, cols,
          std::to_string(index + 1U) + "  " + peer_list_pane.Peers()[index]);
      ++y;
    }
  }
  DrawHorizontalLine(content_bottom - 1);
}

void DrawCommandErrorPopup(int rows, int cols, std::string_view message) {
  constexpr int kPopupRows = 7;
  if (rows < kPopupRows + 2 || cols < 32) {
    return;
  }
  const int popup_cols = std::min(cols - 4, 72);
  const int top = (rows - kPopupRows) / 2;
  const int left = (cols - popup_cols) / 2;
  for (int row = 0; row < kPopupRows; ++row) {
    mvhline(top + row, left, ' ', popup_cols);
  }
  mvhline(top, left + 1, ACS_HLINE, popup_cols - 2);
  mvhline(top + kPopupRows - 1, left + 1, ACS_HLINE, popup_cols - 2);
  mvvline(top + 1, left, ACS_VLINE, kPopupRows - 2);
  mvvline(top + 1, left + popup_cols - 1, ACS_VLINE, kPopupRows - 2);
  mvaddch(top, left, ACS_ULCORNER);
  mvaddch(top, left + popup_cols - 1, ACS_URCORNER);
  mvaddch(top + kPopupRows - 1, left, ACS_LLCORNER);
  mvaddch(top + kPopupRows - 1, left + popup_cols - 1, ACS_LRCORNER);
  AddText(top + 1, left + 2, popup_cols - 4, "Command error",
          A_BOLD | COLOR_PAIR(kColorWarning));
  AddText(top + 3, left + 2, popup_cols - 4, message);
  AddText(top + 5, left + 2, popup_cols - 4, "Press Enter or Esc to dismiss.",
          COLOR_PAIR(kColorMuted));
}

void DrawCommandPalette(int rows, int cols, std::string_view input,
                        std::string_view error) {
  constexpr int kPopupRows = 12;
  if (rows < kPopupRows + 2 || cols < 48) {
    return;
  }
  const int popup_cols = std::min(cols - 4, 78);
  const int top = (rows - kPopupRows) / 2;
  const int left = (cols - popup_cols) / 2;
  for (int row = 0; row < kPopupRows; ++row) {
    mvhline(top + row, left, ' ', popup_cols);
  }
  mvhline(top, left + 1, ACS_HLINE, popup_cols - 2);
  mvhline(top + kPopupRows - 1, left + 1, ACS_HLINE, popup_cols - 2);
  mvvline(top + 1, left, ACS_VLINE, kPopupRows - 2);
  mvvline(top + 1, left + popup_cols - 1, ACS_VLINE, kPopupRows - 2);
  mvaddch(top, left, ACS_ULCORNER);
  mvaddch(top, left + popup_cols - 1, ACS_URCORNER);
  mvaddch(top + kPopupRows - 1, left, ACS_LLCORNER);
  mvaddch(top + kPopupRows - 1, left + popup_cols - 1, ACS_LRCORNER);
  AddText(top + 1, left + 2, popup_cols - 4, "Node command", A_BOLD);
  AddText(top + 2, left + 2, popup_cols - 4,
          "block-production <probability> <period-ms>");
  AddText(top + 3, left + 2, popup_cols - 4, "mining-difficulty <value>");
  AddText(top + 4, left + 2, popup_cols - 4,
          "stop-mining  disconnect  reconnect  log-more  log-less");
  AddText(top + 5, left + 2, popup_cols - 4,
          "connect-peer <node-id>  disconnect-peer <node-id>");
  AddText(top + 6, left + 2, popup_cols - 4, "peer-policy <minimum> <maximum>");
  AddText(top + 8, left + 2, popup_cols - 4, "> " + std::string(input) + "_",
          A_BOLD);
  if (!error.empty()) {
    AddText(top + 9, left + 2, popup_cols - 4, error,
            COLOR_PAIR(kColorWarning));
  }
  AddText(top + 10, left + 2, popup_cols - 4,
          "Enter submits. Tab completes. Esc closes.", COLOR_PAIR(kColorMuted));
}

void AddDetailPair(int y, int x, int width, std::string_view label,
                   std::string_view value) {
  if (width <= 0) {
    return;
  }
  std::string text(label);
  text += ": ";
  text += value;
  AddText(y, x, width, text);
}

void DrawSelectedNodeDetail(int top, int bottom, int cols,
                            const boost::json::object& report,
                            std::size_t selected_node,
                            const boost::json::object* node) {
  if (top < 0 || bottom - top < 4 || cols <= 0) {
    return;
  }
  DrawHorizontalLine(top);
  AddText(top + 1, 0, cols, "Selected Node", A_BOLD);
  if (node == nullptr) {
    AddText(top + 2, 0, cols, "No selectable node.", COLOR_PAIR(kColorMuted));
    return;
  }

  const boost::json::value* metrics_value = node->if_contains("last_metrics");
  const boost::json::object* metrics = nullptr;
  if (metrics_value != nullptr && metrics_value->is_object()) {
    metrics = &metrics_value->as_object();
  }
  const boost::json::object empty_metrics;
  const boost::json::object& metric_object =
      metrics == nullptr ? empty_metrics : *metrics;

  const int left_width = std::max(0, cols / 2);
  const int right_width = std::max(0, cols - left_width);
  int y = top + 2;
  AddDetailPair(y, 0, left_width, "id", JsonString(*node, "node_id", "-"));
  AddDetailPair(y, left_width, right_width, "state",
                JsonString(*node, "final_state", "-"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "height",
                JsonMetricText(metric_object, "height"));
  AddDetailPair(y, left_width, right_width, "best",
                JsonMetricText(metric_object, "best_hash"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "generated blocks",
                JsonMetricText(metric_object, "generated_block_count"));
  AddDetailPair(y, left_width, right_width, "mined non-reward tx",
                JsonMetricText(metric_object, "mined_transaction_count") +
                    (JsonBool(metric_object, "mined_transaction_count_complete")
                             .value_or(true)
                         ? ""
                         : "+"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "chain version",
                JsonMetricText(metric_object, "chain_version") + " / " +
                    JsonMetricText(metric_object, "chain_protocol_version") +
                    " / " + JsonMetricText(metric_object, "chain_subversion"));
  AddDetailPair(y, left_width, right_width, "RPC latency",
                JsonMetricText(metric_object, "rpc_latency_ms") + " ms");
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "initial sync",
                JsonMetricText(metric_object, "initial_block_download"));
  AddDetailPair(y, left_width, right_width, "difficulty",
                JsonMetricText(metric_object, "difficulty"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "peers",
                JsonMetricText(metric_object, "peer_count"));
  AddDetailPair(y, left_width, right_width, "mempool",
                JsonMetricText(metric_object, "mempool_tx_count") + " tx / " +
                    JsonBytesKiBText(metric_object, "mempool_bytes"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, cols, "peer set", PeerListSummaryText(metric_object));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "memory",
                JsonBytesMiBText(metric_object, "memory_current") + " cur / " +
                    JsonBytesMiBText(metric_object, "memory_peak") + " peak");
  AddDetailPair(y, left_width, right_width, "cpu",
                JsonUsecMillisText(metric_object, "cpu_usage_usec") + " ms");
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(
      y, 0, left_width, "net total",
      "down " + JsonBytesKiBText(metric_object, "network_downlink_bytes") +
          " / up " + JsonBytesKiBText(metric_object, "network_uplink_bytes"));
  AddDetailPair(y, left_width, right_width, "net rate",
                "down " +
                    JsonBytesPerSecondText(metric_object,
                                           "network_downlink_bytes_per_sec") +
                    " / up " +
                    JsonBytesPerSecondText(metric_object,
                                           "network_uplink_bytes_per_sec"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(
      y, 0, left_width, "block rules",
      JsonMetricText(metric_object, "network_filter_policy_count") +
          " active / " +
          JsonMetricText(metric_object, "network_filter_drop_packets") +
          " drops");
  AddDetailPair(
      y, left_width, right_width, "filter matches",
      JsonMetricText(metric_object, "network_filter_match_packets") +
          " packets / " +
          JsonBytesKiBText(metric_object, "network_filter_match_bytes"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "peer policy",
                SelectedPeerPolicyText(report, selected_node,
                                       JsonString(*node, "node_id")));
  AddDetailPair(y, left_width, right_width, "qdisc",
                JsonMetricText(metric_object, "qdisc_kind") + " drops " +
                    JsonMetricText(metric_object, "qdisc_drops"));
  ++y;
  if (y >= bottom) {
    return;
  }
  const boost::json::object* wallet = WalletForNode(report, selected_node + 1U);
  if (wallet == nullptr) {
    AddDetailPair(y, 0, left_width, "wallet", "-");
    return;
  }
  AddDetailPair(y, 0, left_width, "wallet",
                "#" + JsonMetricText(*wallet, "wallet_index") + " sent " +
                    JsonMetricText(*wallet, "transactions_sent") + " recv " +
                    JsonMetricText(*wallet, "transactions_received"));
  AddDetailPair(y, left_width, right_width, "addr",
                JsonString(*wallet, "address", "-"));
}

const boost::json::object& JsonObjectFieldOrEmpty(
    const boost::json::object& object, std::string_view field,
    const boost::json::object& empty) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_object() ? value->as_object() : empty;
}

const boost::json::object* LastWalletTransaction(
    const boost::json::object& metrics,
    ChainWalletTransactionDirection direction) {
  const boost::json::value* value = metrics.if_contains("transactions");
  if (value == nullptr || !value->is_array()) {
    return nullptr;
  }
  const boost::json::object* latest = nullptr;
  std::uint64_t latest_timestamp = 0;
  for (const boost::json::value& item : value->as_array()) {
    if (!item.is_object()) {
      continue;
    }
    const boost::json::object& transaction = item.as_object();
    const std::optional<ChainWalletTransactionDirection> transaction_direction =
        ChainWalletTransactionDirectionFromName(
            JsonString(transaction, "direction"));
    if (!transaction_direction || *transaction_direction != direction) {
      continue;
    }
    const std::optional<std::uint64_t> timestamp =
        JsonUnsignedMetric(transaction, "timestamp");
    if (latest == nullptr || timestamp.value_or(0U) >= latest_timestamp) {
      latest = &transaction;
      latest_timestamp = timestamp.value_or(0U);
    }
  }
  return latest;
}

void DrawSelectedWalletDetail(int top, int bottom, int cols,
                              const boost::json::object* wallet) {
  if (top < 0 || bottom - top < 4 || cols <= 0) {
    return;
  }
  DrawHorizontalLine(top);
  AddText(top + 1, 0, cols, "Selected Wallet", A_BOLD);
  if (wallet == nullptr) {
    AddText(top + 2, 0, cols, "No selectable wallet.", COLOR_PAIR(kColorMuted));
    return;
  }

  const boost::json::object empty;
  const boost::json::object& metrics =
      JsonObjectFieldOrEmpty(*wallet, "last_metrics", empty);
  const boost::json::object* outgoing = LastWalletTransaction(
      metrics, ChainWalletTransactionDirection::kOutgoing);
  const boost::json::object* incoming = LastWalletTransaction(
      metrics, ChainWalletTransactionDirection::kIncoming);
  const boost::json::object& outgoing_object =
      outgoing == nullptr ? empty : *outgoing;
  const boost::json::object& incoming_object =
      incoming == nullptr ? empty : *incoming;
  const int left_width = std::max(0, cols / 2);
  const int right_width = std::max(0, cols - left_width);
  int y = top + 2;
  AddDetailPair(y, 0, left_width, "wallet",
                "#" + JsonIntegerText(*wallet, "wallet_index"));
  AddDetailPair(y, left_width, right_width, "node",
                JsonIntegerText(*wallet, "node"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "mode", JsonString(*wallet, "mode", "-"));
  AddDetailPair(y, left_width, right_width, "strategy",
                JsonString(*wallet, "strategy", "-"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, cols, "address", JsonString(*wallet, "address", "-"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "available",
                JsonSatoshisText(metrics, "available_balance_satoshis"));
  AddDetailPair(y, left_width, right_width, "spent by benchmark",
                JsonSatoshisText(*wallet, "simulated_amount_sent_satoshis",
                                 "0.00000000"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "unconfirmed",
                JsonSatoshisText(metrics, "unconfirmed_balance_satoshis"));
  AddDetailPair(y, left_width, right_width, "immature",
                JsonSatoshisText(metrics, "immature_balance_satoshis"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "benchmark sent/recv",
                JsonIntegerText(*wallet, "transactions_sent", "0") + "/" +
                    JsonIntegerText(*wallet, "transactions_received", "0"));
  AddDetailPair(
      y, left_width, right_width, "driver tx count",
      JsonIntegerText(metrics, "transaction_count") +
          (JsonBool(metrics, "transaction_history_truncated").value_or(false)
               ? " (history truncated)"
               : ""));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, cols, "last outgoing tx",
                JsonString(outgoing_object, "txid", "-"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "out amount",
                JsonSatoshisText(outgoing_object, "amount_satoshis"));
  AddDetailPair(y, left_width, right_width, "out fee",
                JsonSatoshisText(outgoing_object, "fee_satoshis"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "out confirmations",
                JsonIntegerText(outgoing_object, "confirmations"));
  AddDetailPair(y, left_width, right_width, "out destination",
                JsonString(outgoing_object, "address", "-"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, cols, "last incoming tx/address",
                JsonString(incoming_object, "txid", "-") + " / " +
                    JsonString(incoming_object, "address", "-"));
  ++y;
  if (y >= bottom) {
    return;
  }
  AddDetailPair(y, 0, left_width, "in amount",
                JsonSatoshisText(incoming_object, "amount_satoshis"));
  AddDetailPair(y, left_width, right_width, "in confirmations",
                JsonIntegerText(incoming_object, "confirmations"));
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
                 const boost::json::object& report, std::string_view error,
                 const std::vector<std::string>& log_lines, TuiView view,
                 std::size_t selected_node, std::size_t selected_wallet,
                 const NodeLogPane& node_log_pane,
                 const PeerListPane& peer_list_pane,
                 std::string_view command_status, bool command_error_open,
                 std::string_view command_error, bool command_palette_open,
                 std::string_view command_input,
                 std::string_view command_input_error) {
  int rows = 0;
  int cols = 0;
  getmaxyx(stdscr, rows, cols);
  erase();
  const int log_rows = LogPaneRows(rows);
  const int log_top = log_rows == 0 ? rows - 2 : rows - log_rows - 2;
  const int content_bottom = log_rows == 0 ? rows - 2 : log_top;

  AddText(0, 0, cols, "Blockchain Benchmark Project TUI",
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
    AddText(rows - 1, 0, cols, "Arrows select. l node log. q or Esc exits.",
            COLOR_PAIR(kColorMuted));
    refresh();
    return;
  }

  const std::string status = JsonString(report, "status", "unknown");
  const bool ok = JsonBool(report, "ok").value_or(false);
  const boost::json::array* wallets = WalletSummaries(report);
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
  AddText(
      6, cols / 2, cols - (cols / 2),
      "wallets: " + std::to_string(wallets == nullptr ? 0U : wallets->size()));
  const boost::json::value* block_production_value =
      report.if_contains("block_production");
  const boost::json::object empty_block_production;
  const boost::json::object& block_production =
      block_production_value != nullptr && block_production_value->is_object()
          ? block_production_value->as_object()
          : empty_block_production;
  const bool block_production_enabled =
      JsonBool(block_production, "enabled").value_or(false);
  const bool native_mining =
      JsonBool(block_production, "native_mining").value_or(false);
  AddText(7, 0, cols / 2,
          "mining: " + std::string(!block_production_enabled ? "disabled"
                                   : native_mining           ? "native"
                                                             : "scheduled"));
  AddText(7, cols / 2, cols - (cols / 2),
          "block draw: p=" + JsonMetricText(block_production, "probability") +
              " / " + JsonIntegerText(block_production, "period_ms") + "ms");
  AddText(8, 0, cols, LifecycleSummaryText(report));
  AddText(9, 0, cols, WorkloadsSummaryText(report));

  DrawHorizontalLine(10);
  if (view == TuiView::kNodes) {
    AddText(11, 0, 10, "Node [n]", A_BOLD);
    AddText(11, 10, 10, "State", A_BOLD);
    AddText(11, 20, 7, "Height", A_BOLD);
    AddText(11, 27, 6, "Peers", A_BOLD);
    AddText(11, 33, 7, "Blocks", A_BOLD);
    AddText(11, 40, 7, "Pool", A_BOLD);
    AddText(11, 47, 9, "Mem", A_BOLD);
    AddText(11, 56, 9, "CPUms", A_BOLD);
    AddText(11, 65, 8, "RX", A_BOLD);
    AddText(11, 73, std::max(0, cols - 73), "Qdisc", A_BOLD);
  } else {
    AddText(11, 0, 10, "Wallet [w]", A_BOLD);
    AddText(11, 10, 7, "Node", A_BOLD);
    AddText(11, 17, 10, "Mode", A_BOLD);
    AddText(11, 27, 12, "Strategy", A_BOLD);
    AddText(11, 39, 8, "Sent", A_BOLD);
    AddText(11, 47, 8, "Recv", A_BOLD);
    AddText(11, 55, std::max(0, cols - 55), "Address", A_BOLD);
  }
  DrawHorizontalLine(12);

  const boost::json::array* nodes = NodeSummaries(report);
  const boost::json::array* selected_items =
      view == TuiView::kNodes ? nodes : wallets;
  if (selected_items == nullptr) {
    AddText(13, 0, cols,
            view == TuiView::kNodes ? "No node summaries in report."
                                    : "No wallet summaries in report.",
            COLOR_PAIR(kColorMuted));
    if (log_rows != 0) {
      DrawLogPane(log_top, rows, cols, log_lines);
    }
    DrawHorizontalLine(rows - 2);
    AddText(rows - 1, 0, cols, "Arrows select. l node log. q or Esc exits.",
            COLOR_PAIR(kColorMuted));
    refresh();
    return;
  }

  const int data_top = 13;
  const int available_data_rows = content_bottom - data_top;
  const bool has_detail_pane = available_data_rows >= kDetailPaneRows + 3;
  const int detail_top =
      has_detail_pane ? content_bottom - kDetailPaneRows : content_bottom;
  const int table_bottom = has_detail_pane ? detail_top - 1 : content_bottom;
  const int table_capacity = std::max(0, table_bottom - data_top);
  const std::size_t selected_item =
      view == TuiView::kNodes ? selected_node : selected_wallet;
  std::size_t first_item = 0;
  if (table_capacity > 0 &&
      selected_item >= static_cast<std::size_t>(table_capacity)) {
    first_item = selected_item - static_cast<std::size_t>(table_capacity) + 1U;
  }

  int y = data_top;
  for (std::size_t index = first_item; index < selected_items->size();
       ++index) {
    if (y >= table_bottom) {
      break;
    }
    const int attributes = index == selected_item ? A_REVERSE : 0;
    if (view == TuiView::kNodes) {
      const boost::json::object* node = NodeAt(*selected_items, index);
      if (node == nullptr) {
        continue;
      }
      const boost::json::value* metrics_value =
          node->if_contains("last_metrics");
      const boost::json::object* metrics = nullptr;
      if (metrics_value != nullptr && metrics_value->is_object()) {
        metrics = &metrics_value->as_object();
      }
      const boost::json::object empty_metrics;
      const boost::json::object& metric_object =
          metrics == nullptr ? empty_metrics : *metrics;
      AddText(y, 0, 10, JsonString(*node, "node_id", "-"), attributes);
      AddText(y, 10, 10, JsonString(*node, "final_state", "-"), attributes);
      AddText(y, 20, 7, JsonMetricText(metric_object, "height"), attributes);
      AddText(y, 27, 6, JsonMetricText(metric_object, "peer_count"),
              attributes);
      AddText(y, 33, 7, JsonMetricText(metric_object, "generated_block_count"),
              attributes);
      AddText(y, 40, 7, JsonMetricText(metric_object, "mempool_tx_count"),
              attributes);
      AddText(y, 47, 9, JsonBytesMiBText(metric_object, "memory_current"),
              attributes);
      AddText(y, 56, 9, JsonUsecMillisText(metric_object, "cpu_usage_usec"),
              attributes);
      AddText(y, 65, 8, JsonBytesKiBText(metric_object, "network_rx_bytes"),
              attributes);
      AddText(y, 73, std::max(0, cols - 73),
              JsonMetricText(metric_object, "qdisc_kind"), attributes);
    } else {
      const boost::json::object* wallet = WalletAt(*selected_items, index);
      if (wallet == nullptr) {
        continue;
      }
      AddText(y, 0, 10, "#" + JsonIntegerText(*wallet, "wallet_index", "-"),
              attributes);
      AddText(y, 10, 7, JsonIntegerText(*wallet, "node"), attributes);
      AddText(y, 17, 10, JsonString(*wallet, "mode", "-"), attributes);
      AddText(y, 27, 12, JsonString(*wallet, "strategy", "-"), attributes);
      AddText(y, 39, 8, JsonIntegerText(*wallet, "transactions_sent", "0"),
              attributes);
      AddText(y, 47, 8, JsonIntegerText(*wallet, "transactions_received", "0"),
              attributes);
      AddText(y, 55, std::max(0, cols - 55),
              JsonString(*wallet, "address", "-"), attributes);
    }
    ++y;
  }

  if (has_detail_pane) {
    if (view == TuiView::kNodes) {
      DrawSelectedNodeDetail(detail_top, content_bottom, cols, report,
                             selected_node, NodeAt(*nodes, selected_node));
    } else {
      DrawSelectedWalletDetail(
          detail_top, content_bottom, cols,
          wallets == nullptr ? nullptr : WalletAt(*wallets, selected_wallet));
    }
  }

  if (log_rows != 0) {
    DrawLogPane(log_top, rows, cols, log_lines);
  }
  DrawNodeLogPane(content_bottom, cols, node_log_pane);
  DrawPeerListPane(content_bottom, cols, peer_list_pane);
  DrawHorizontalLine(rows - 2);
  std::string footer;
  if (!command_status.empty()) {
    footer = std::string(command_status) + " | ";
  }
  if (node_log_pane.IsOpen()) {
    footer +=
        "Node log: arrows/PgUp/PgDn/Home/End scroll. +/- verbosity. "
        "l closes. q exits.";
  } else if (peer_list_pane.IsOpen()) {
    footer += "Peers: arrows/PgUp/PgDn/Home/End scroll. p closes. q exits.";
  } else {
    footer +=
        "Tab/n/w view. Arrows select. p peers. l log. c command. s stop. "
        "d/r disconnect/reconnect. k kill. q or Esc exits.";
  }
  AddText(rows - 1, 0, cols, footer, COLOR_PAIR(kColorMuted));
  if (command_error_open) {
    DrawCommandErrorPopup(rows, cols, command_error);
  }
  if (command_palette_open) {
    DrawCommandPalette(rows, cols, command_input, command_input_error);
  }
  refresh();
}

bool ShouldExit(int ch) { return ch == 'q' || ch == 'Q' || ch == 27; }

std::size_t CurrentNodeLogVisibleRows() {
  int rows = 0;
  int cols = 0;
  getmaxyx(stdscr, rows, cols);
  static_cast<void>(cols);
  const int log_rows = LogPaneRows(rows);
  const int log_top = log_rows == 0 ? rows - 2 : rows - log_rows - 2;
  const int content_bottom = log_rows == 0 ? rows - 2 : log_top;
  return static_cast<std::size_t>(NodeLogVisibleRows(content_bottom));
}

std::uint64_t BlockProductionSeed(const boost::json::object& report) {
  const boost::json::value* value = report.if_contains("block_production");
  if (value == nullptr || !value->is_object()) {
    return 0U;
  }
  return JsonUnsignedMetric(value->as_object(), "seed").value_or(0U);
}

bool QueueParsedNodeCommand(const ParsedTuiCommand& parsed,
                            const boost::json::object& report,
                            SimulationCommandQueue* command_queue,
                            TuiState* state) {
  if (command_queue == nullptr) {
    state->command_input_error =
        "Live commands are unavailable in report mode.";
    return false;
  }

  try {
    std::uint64_t sequence = 0U;
    std::string target = "the simulation";
    if (parsed.kind == SimulationCommandKind::kSetBlockProductionPolicy) {
      if (!parsed.block_production_policy) {
        throw std::runtime_error("block production policy is missing");
      }
      sequence = command_queue->PushBlockProductionPolicy(
          *parsed.block_production_policy);
    } else {
      const boost::json::array* nodes = NodeSummaries(report);
      const std::optional<std::size_t> selected_node =
          SelectedNodeIndex(report, *state);
      const boost::json::object* node = nodes == nullptr || !selected_node
                                            ? nullptr
                                            : NodeAt(*nodes, *selected_node);
      if (node == nullptr) {
        state->command_input_error = "No backing node is selected.";
        return false;
      }
      const std::string node_id = JsonString(*node, "node_id");
      target = node_id;
      if (parsed.kind == SimulationCommandKind::kSetMiningDifficulty) {
        if (!parsed.mining_difficulty) {
          throw std::runtime_error("mining difficulty is missing");
        }
        sequence = command_queue->PushMiningDifficulty(
            node_id, *parsed.mining_difficulty);
      } else if (parsed.kind == SimulationCommandKind::kSetPeerCountPolicy) {
        if (!parsed.peer_count_policy) {
          throw std::runtime_error("peer count policy is missing");
        }
        sequence = command_queue->PushPeerCountPolicy(
            node_id, *parsed.peer_count_policy);
      } else if (parsed.kind == SimulationCommandKind::kConnectPeer ||
                 parsed.kind == SimulationCommandKind::kDisconnectPeer) {
        if (!parsed.peer_node_id) {
          throw std::runtime_error("peer target node is missing");
        }
        sequence = command_queue->PushPeerCommand(parsed.kind, node_id,
                                                  *parsed.peer_node_id);
      } else {
        sequence = command_queue->Push(parsed.kind, node_id);
      }
    }
    state->command_status =
        "Queued #" + std::to_string(sequence) + " " +
        std::string(SimulationCommandKindName(parsed.kind)) + " for " + target +
        ".";
    state->command_input_error.clear();
    return true;
  } catch (const std::exception& error) {
    state->command_input_error =
        "Command rejected: " + std::string(error.what());
    return false;
  }
}

bool HandleCommandPaletteInput(int ch, const boost::json::object& report,
                               SimulationCommandQueue* command_queue,
                               TuiState* state) {
  if (ch == 27) {
    state->command_palette_open = false;
    state->command_input.clear();
    state->command_input_error.clear();
    return true;
  }
  if (ch == '\n' || ch == KEY_ENTER) {
    try {
      const ParsedTuiCommand parsed = TuiCommandParser::Parse(
          state->command_input, BlockProductionSeed(report));
      if (QueueParsedNodeCommand(parsed, report, command_queue, state)) {
        state->command_palette_open = false;
        state->command_input.clear();
      }
    } catch (const std::exception& error) {
      state->command_input_error = error.what();
    }
    return true;
  }
  if (ch == '\t') {
    state->command_input = TuiCommandParser::Complete(state->command_input);
    state->command_input_error.clear();
    return true;
  }
  if (ch == KEY_BACKSPACE || ch == 127 || ch == '\b') {
    if (!state->command_input.empty()) {
      state->command_input.pop_back();
    }
    state->command_input_error.clear();
    return true;
  }
  if (ch >= 0 && ch <= 255 &&
      std::isprint(static_cast<unsigned char>(ch)) != 0 &&
      state->command_input.size() < 128U) {
    state->command_input.push_back(static_cast<char>(ch));
    state->command_input_error.clear();
    return true;
  }
  return ch != ERR;
}

bool QueueSelectedNodeCommand(SimulationCommandKind kind,
                              const boost::json::object& report,
                              SimulationCommandQueue* command_queue,
                              TuiState* state) {
  const boost::json::array* nodes = NodeSummaries(report);
  const std::optional<std::size_t> selected_node =
      SelectedNodeIndex(report, *state);
  const boost::json::object* node = nodes == nullptr || !selected_node
                                        ? nullptr
                                        : NodeAt(*nodes, *selected_node);
  if (node == nullptr) {
    state->command_status = "No backing node is selected.";
    return true;
  }
  if (command_queue == nullptr) {
    state->command_status = "Live commands are unavailable in report mode.";
    return true;
  }

  const std::string node_id = JsonString(*node, "node_id");
  try {
    const std::uint64_t sequence = command_queue->Push(kind, node_id);
    state->command_status = "Queued #" + std::to_string(sequence) + " " +
                            std::string(SimulationCommandKindName(kind)) +
                            " for " + node_id + ".";
  } catch (const std::exception& error) {
    state->command_status = "Command rejected: " + std::string(error.what());
  }
  return true;
}

bool HandleInput(int ch, const boost::json::object& report,
                 SimulationCommandQueue* command_queue, TuiState* state) {
  if (state->command_palette_open) {
    return HandleCommandPaletteInput(ch, report, command_queue, state);
  }
  if (state->command_error_open) {
    if (ch == '\n' || ch == KEY_ENTER || ch == 27) {
      state->command_error_open = false;
      state->command_error.clear();
      return true;
    }
    return ch != ERR && ch != 'q' && ch != 'Q';
  }

  const boost::json::array* nodes = NodeSummaries(report);
  if (nodes == nullptr || nodes->empty()) {
    state->selected_node = 0;
    return false;
  }

  const boost::json::array* wallets = WalletSummaries(report);
  if (ch == '\t') {
    state->view =
        state->view == TuiView::kNodes ? TuiView::kWallets : TuiView::kNodes;
    return true;
  }
  if (ch == 'n' || ch == 'N') {
    state->view = TuiView::kNodes;
    return true;
  }
  if (ch == 'w' || ch == 'W') {
    state->view = TuiView::kWallets;
    return true;
  }

  if (ch == 'l' || ch == 'L') {
    const std::optional<std::size_t> selected_node =
        SelectedNodeIndex(report, *state);
    if (!selected_node) {
      state->command_status = "No backing node is selected.";
      return true;
    }
    state->peer_list_pane.Close();
    state->node_log_pane.Toggle(report, *selected_node);
    return true;
  }

  if (ch == 'p' || ch == 'P') {
    const std::optional<std::size_t> selected_node =
        SelectedNodeIndex(report, *state);
    if (!selected_node) {
      state->command_status = "No backing node is selected.";
      return true;
    }
    state->node_log_pane.Close();
    state->peer_list_pane.Toggle(report, *selected_node);
    return true;
  }

  if (ch == 'c' || ch == 'C') {
    state->command_palette_open = true;
    state->command_input.clear();
    state->command_input_error.clear();
    return true;
  }

  if (ch == 's' || ch == 'S') {
    return QueueSelectedNodeCommand(SimulationCommandKind::kStopMining, report,
                                    command_queue, state);
  }
  if (ch == 'd' || ch == 'D') {
    return QueueSelectedNodeCommand(SimulationCommandKind::kDisconnectNode,
                                    report, command_queue, state);
  }
  if (ch == 'r' || ch == 'R') {
    return QueueSelectedNodeCommand(SimulationCommandKind::kReconnectNode,
                                    report, command_queue, state);
  }
  if (ch == 'k' || ch == 'K') {
    return QueueSelectedNodeCommand(SimulationCommandKind::kKillNode, report,
                                    command_queue, state);
  }

  if (state->node_log_pane.IsOpen()) {
    if (ch == '+') {
      return QueueSelectedNodeCommand(
          SimulationCommandKind::kIncreaseLogVerbosity, report, command_queue,
          state);
    }
    if (ch == '-') {
      return QueueSelectedNodeCommand(
          SimulationCommandKind::kDecreaseLogVerbosity, report, command_queue,
          state);
    }
    const std::size_t visible_rows = CurrentNodeLogVisibleRows();
    const std::size_t page_rows = std::max<std::size_t>(visible_rows, 1U);
    if (ch == KEY_UP) {
      state->node_log_pane.ScrollUp(visible_rows, 1U);
    } else if (ch == KEY_DOWN) {
      state->node_log_pane.ScrollDown(visible_rows, 1U);
    } else if (ch == KEY_PPAGE) {
      state->node_log_pane.ScrollUp(visible_rows, page_rows);
    } else if (ch == KEY_NPAGE) {
      state->node_log_pane.ScrollDown(visible_rows, page_rows);
    } else if (ch == KEY_HOME) {
      state->node_log_pane.ScrollHome(visible_rows);
    } else if (ch == KEY_END) {
      state->node_log_pane.ScrollEnd();
    } else {
      return false;
    }
    return true;
  }

  if (state->peer_list_pane.IsOpen()) {
    const std::size_t visible_rows = CurrentNodeLogVisibleRows();
    const std::size_t page_rows = std::max<std::size_t>(visible_rows, 1U);
    if (ch == KEY_UP) {
      state->peer_list_pane.ScrollUp(visible_rows, 1U);
    } else if (ch == KEY_DOWN) {
      state->peer_list_pane.ScrollDown(visible_rows, 1U);
    } else if (ch == KEY_PPAGE) {
      state->peer_list_pane.ScrollUp(visible_rows, page_rows);
    } else if (ch == KEY_NPAGE) {
      state->peer_list_pane.ScrollDown(visible_rows, page_rows);
    } else if (ch == KEY_HOME) {
      state->peer_list_pane.ScrollHome();
    } else if (ch == KEY_END) {
      state->peer_list_pane.ScrollEnd(visible_rows);
    } else {
      return false;
    }
    return true;
  }

  std::size_t* selected = state->view == TuiView::kNodes
                              ? &state->selected_node
                              : &state->selected_wallet;
  const std::size_t item_count =
      state->view == TuiView::kNodes
          ? nodes->size()
          : (wallets == nullptr ? 0U : wallets->size());
  if (ch == KEY_UP && *selected > 0U) {
    --*selected;
    return true;
  }
  if (ch == KEY_DOWN && *selected + 1U < item_count) {
    ++*selected;
    return true;
  }
  return false;
}

}  // namespace

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms,
                 SimulationCommandQueue* command_queue) {
  CursesSession curses;
  const std::uint32_t sleep_step_ms = 50;
  TuiState state;

  while (true) {
    std::string error;
    const boost::json::object report = LoadReport(run_root, &error);
    state.selected_node = ClampNodeSelection(report, state.selected_node);
    state.selected_wallet = ClampWalletSelection(report, state.selected_wallet);
    if (error.empty()) {
      const std::optional<std::size_t> selected_node =
          SelectedNodeIndex(report, state);
      if (selected_node) {
        state.node_log_pane.Refresh(report, *selected_node);
        state.peer_list_pane.Refresh(report, *selected_node);
      }
      RefreshCommandResults(report, &state);
    }
    const std::vector<std::string> log_lines =
        ReadRecentLogLines(RunLogPath(run_root), 256U);
    DrawSummary(run_root, report, error, log_lines, state.view,
                state.selected_node, state.selected_wallet, state.node_log_pane,
                state.peer_list_pane, state.command_status,
                state.command_error_open, state.command_error,
                state.command_palette_open, state.command_input,
                state.command_input_error);
    if (once) {
      return error.empty() ? 0 : 1;
    }

    std::uint32_t slept_ms = 0;
    while (slept_ms < refresh_ms) {
      const int ch = getch();
      if (HandleInput(ch, report, command_queue, &state)) {
        break;
      }
      if (ShouldExit(ch)) {
        return 0;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_step_ms));
      slept_ms += sleep_step_ms;
    }
  }
}

}  // namespace bbp
