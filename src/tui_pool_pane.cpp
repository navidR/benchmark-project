#include "bbp/tui_pool_pane.h"

#include <algorithm>
#include <boost/json.hpp>
#include <limits>

#include "bbp/util.h"
#include "tui_browse_layout.h"

namespace bbp {
namespace {
using browse::Age;
using browse::Object;
using browse::Text;
void Move(std::size_t& offset, std::size_t maximum, PoolNavigation key,
          std::size_t page) {
  offset = std::min(offset, maximum);
  if (key == PoolNavigation::kHome) offset = 0;
  if (key == PoolNavigation::kEnd) offset = maximum;
  if (key == PoolNavigation::kUp && offset) --offset;
  if (key == PoolNavigation::kDown && offset < maximum) ++offset;
  if (key == PoolNavigation::kPageUp) offset -= std::min(offset, page);
  if (key == PoolNavigation::kPageDown)
    offset += std::min(maximum - offset, page);
}
}  // namespace

TuiPoolPane::TuiPoolPane()
    : worker_([this](std::stop_token stop) { Work(stop); }) {}
TuiPoolPane::~TuiPoolPane() {
  Cancel();
  worker_.request_stop();
  ready_.notify_all();
}
void TuiPoolPane::Cancel() {
  std::lock_guard lock(mutex_);
  request_stop_.request_stop();
  ++generation_;
  queued_ = false;
  pending_ = false;
  completed_.reset();
  queued_service_.reset();
  last_request_.reset();
}
void TuiPoolPane::Reset() {
  Cancel();
  service_.reset();
  page_.reset();
  selected_id_.clear();
  switching_selection_.clear();
  pinned_source_.reset();
  sources_.clear();
  retained_ = false;
  notice_.clear();
  index_ = count_ = focus_ = 0;
  summary_offset_ = detail_offset_ = 0;
}
void TuiPoolPane::Work(std::stop_token stop) {
  while (!stop.stop_requested()) {
    std::unique_lock lock(mutex_);
    if (!ready_.wait(lock, stop, [&] { return queued_; })) return;
    const auto request = queued_request_;
    const auto service = queued_service_;
    const auto token = request_stop_.get_token();
    const auto generation = generation_;
    queued_ = false;
    lock.unlock();
    boost::json::object page;
    try {
      page = service->Query(request, token);
    } catch (const std::exception& e) {
      page = {{"error", e.what()}};
    }
    lock.lock();
    if (generation == generation_ && !token.stop_requested()) {
      completed_ = std::make_shared<const boost::json::object>(std::move(page));
      pending_ = false;
      ++revision_;
    }
  }
}
void TuiPoolPane::Poll() {
  std::shared_ptr<const boost::json::object> completed;
  {
    std::lock_guard lock(mutex_);
    completed = std::exchange(completed_, {});
  }
  if (!completed) return;
  page_ = std::move(completed);
  retained_ = Text(*page_, "mode") == "retained";
  if (retained_) pinned_source_.reset();
  sources_.clear();
  if (const auto* sources = page_->if_contains("available_sources");
      sources && sources->is_array())
    for (const auto& source : sources->as_array())
      sources_.emplace_back(source.as_string());
  if (const auto* summary = Object(*page_, "summary")) {
    switching_selection_.clear();
    count_ =
        static_cast<std::uint32_t>(JsonUint(*summary, "transaction_count"));
    index_ = static_cast<std::uint32_t>(JsonUint(*page_, "selected_index"));
    selected_id_ = JsonString(*page_, "selected_id");
    if (!JsonString(*page_, "departed_id").empty())
      notice_ = JsonString(*page_, "departure_reason") + " [" +
                JsonString(*page_, "departed_id").substr(0, 12) + "]";
    else if (!JsonString(*page_, "notice").empty())
      notice_ = JsonString(*page_, "notice");
  }
}
void TuiPoolPane::Refresh(std::shared_ptr<PoolViewService> service, int rows,
                          int columns) {
  if (service_ != service) {
    Reset();
    service_ = std::move(service);
  }
  Poll();
  const auto layout = browse::Layout(rows, columns, focus_, true);
  page_rows_ =
      static_cast<std::uint32_t>(std::clamp(layout[0].Rows() - 1, 1, 32));
  if (!service_) return;
  const PoolViewRequest request{
      switching_selection_.empty() ? selected_id_ : switching_selection_,
      index_, page_rows_, pinned_source_};
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard lock(mutex_);
  const bool changed = !last_request_ || *last_request_ != request;
  if (!changed && (pending_ || now < next_refresh_)) return;
  request_stop_.request_stop();
  request_stop_ = std::stop_source{};
  queued_request_ = request;
  queued_service_ = service_;
  queued_ = pending_ = true;
  ++generation_;
  last_request_ = request;
  next_refresh_ = now + std::chrono::milliseconds(500);
  ready_.notify_one();
}
void TuiPoolPane::Navigate(PoolNavigation key) {
  if (key == PoolNavigation::kNextSource ||
      key == PoolNavigation::kAutomaticSource) {
    if (retained_) return;
    std::optional<std::string> next;
    if (key == PoolNavigation::kNextSource && !sources_.empty()) {
      const auto current =
          pinned_source_
              ? std::find(sources_.begin(), sources_.end(), *pinned_source_)
              : sources_.end();
      if (current == sources_.end())
        next = sources_.front();
      else if (std::next(current) != sources_.end())
        next = *std::next(current);
    }
    if (next == pinned_source_) return;
    Cancel();
    pinned_source_ = std::move(next);
    if (!selected_id_.empty()) switching_selection_ = selected_id_;
    selected_id_.clear();
    page_.reset();
    index_ = count_ = 0;
    summary_offset_ = detail_offset_ = 0;
    notice_ = "Loading selected source...";
    return;
  }
  if (key == PoolNavigation::kInspect) {
    focus_ = focus_ == 2 ? 1 : 2;
    return;
  }
  if (key == PoolNavigation::kBack) {
    focus_ = 0;
    return;
  }

  if (key == PoolNavigation::kFocus) {
    focus_ = (focus_ + 1) % 3;
    return;
  }
  if (focus_ != 0) {
    Move(focus_ == 1 ? summary_offset_ : detail_offset_,
         focus_ == 1 ? summary_max_ : detail_max_, key,
         focus_ == 1 ? summary_rows_ : detail_rows_);
    return;
  }
  if (count_ == 0) return;
  std::size_t next = index_;
  Move(next, count_ - 1, key, page_rows_);
  if (next == index_) return;
  index_ = static_cast<std::uint32_t>(next);
  selected_id_.clear();
  // Pin an already visible target by identity before the next pool refresh.
  if (page_) {
    const auto* values = page_->if_contains("rows");
    if (values && values->is_array()) {
      const auto first = JsonUint(*page_, "first_index");
      if (next >= first && next - first < values->as_array().size())
        selected_id_ =
            JsonString(values->as_array()[next - first].as_object(), "id");
    }
  }
  detail_offset_ = 0;
  notice_.clear();
  // A result completed just before this keystroke must not undo navigation.
  Cancel();
}

std::vector<PoolViewLine> TuiPoolPane::Lines(int rows, int columns,
                                             bool active) const {
  if (rows <= 0 || columns <= 0) return {};
  using namespace browse;
  Canvas canvas(rows, columns);
  const auto layout = Layout(rows, columns, focus_, true);
  const boost::json::object empty;
  const auto& page = page_ ? *page_ : empty;
  const bool retained = Text(page, "mode") == "retained";
  const auto sampled = Uint(page, "sampled_at_ms");
  const auto now_ms = NowUnixMillis();
  const auto age_at = retained && sampled ? *sampled / 1000 : now_ms / 1000;
  const auto fresh =
      sampled ? std::to_string(now_ms >= *sampled ? (now_ms - *sampled) / 1000
                                                  : 0) +
                    "s"
              : "N/A";
  canvas.Put(0, 0, columns,
             std::string("POOL | ") + Text(page, "mode") + " | " +
                 (Object(page, "summary") ? std::to_string(count_) : "N/A") +
                 " transactions");
  const auto source = pinned_source_.value_or(Text(page, "source_node"));
  canvas.Put(1, 0, columns,
             "Source: " + (source.empty() ? std::string("N/A") : source) +
                 (retained         ? " [captured]"
                  : pinned_source_ ? " [pinned]"
                                   : " [auto]") +
                 " | Snapshot age: " + fresh +
                 (retained ? " | captured " : " | sampled ") +
                 Text(page, "sampled_at_ms"));
  const auto error = Text(page, "error");
  canvas.Put(2, 0, columns,
             error != "" && error != "N/A"
                 ? error
                 : std::string(pending_ ? "Loading... " : "") + notice_);
  const auto list = layout[0];
  const auto visible =
      static_cast<std::size_t>(std::clamp(list.Rows() - 1, 1, 32));
  const auto first = Uint(page, "first_index").value_or(0);
  const auto table_columns =
      Columns(list.Columns(), {{"id", "Transaction ID", 18},
                               {"fee", "Fee*", 10},
                               {"serialized_size", "Bytes*", 9},
                               {"age", "Age", 9},
                               {"fee_rate", "Rate*", 10},
                               {"weight", "Weight", 9},
                               {"dependency_count", "Deps", 5},
                               {"relayed", "Relayed", 7},
                               {"ancestor_count", "Ancestors", 9},
                               {"descendant_count", "Descendants", 11}});
  canvas.Frame(list, "1 Pool transactions", active && focus_ == 0,
               Position(first, visible, count_));
  canvas.Body(list, 0, TableRow(table_columns));
  if (const auto* values = page.if_contains("rows");
      values && values->is_array()) {
    for (std::size_t i = 0; i < values->as_array().size() && i < visible; ++i) {
      auto fields = values->as_array()[i].as_object();
      fields["age"] = Age(fields, "first_seen", age_at);
      const auto* dependencies = fields.if_contains("dependencies");
      fields["dependency_count"] =
          dependencies && dependencies->is_array()
              ? boost::json::value(dependencies->as_array().size())
              : boost::json::value(nullptr);
      canvas.Body(list, static_cast<int>(i + 1),
                  TableRow(table_columns, &fields),
                  !selected_id_.empty() && Text(fields, "id") == selected_id_);
    }
    if (values->as_array().empty())
      canvas.Body(
          list, 1,
          Object(page, "summary") ? "Pool is empty" : "No pool snapshot");
  } else
    canvas.Body(list, 1, "Loading pool snapshot...");

  std::vector<std::string> summary_lines, detail_lines;
  const auto* summary = Object(page, "summary");
  const int width = layout[1].Columns();
  if (summary) {
    Wrap(summary_lines,
         "Transactions: " + Text(*summary, "transaction_count") +
             " | Memory bytes: " + Text(*summary, "memory_usage"),
         width);
    const auto stats_columns = Columns(width, {{"name", "Distribution", 13},
                                               {"minimum", "Minimum", 10},
                                               {"average", "Mean", 10},
                                               {"maximum", "Maximum", 10}});
    Wrap(summary_lines, TableRow(stats_columns), width);
    for (auto name : {"serialized_size", "weight", "fees"}) {
      const auto* stats = Object(*summary, name);
      auto fields = stats ? *stats : boost::json::object{};
      fields["name"] =
          name == std::string_view("serialized_size") ? "Bytes*" : name;
      Wrap(summary_lines, TableRow(stats_columns, &fields), width);
      Wrap(summary_lines,
           std::string(name) + " total: " + Text(fields, "total"), width);
    }
    boost::json::object rates{{"name", "Fee rate*"}};
    for (auto key : {"minimum", "average", "maximum"}) {
      const auto* value = summary->if_contains(std::string(key) + "_fee_rate");
      rates[key] = value ? *value : boost::json::value(nullptr);
    }
    Wrap(summary_lines, TableRow(stats_columns, &rates), width);
    Wrap(summary_lines,
         "Age min/max: " + Age(*summary, "youngest_first_seen", age_at) +
             " / " + Age(*summary, "oldest_first_seen", age_at),
         width);
    const auto* mean = summary->if_contains("average_first_seen");
    std::string mean_age = "N/A";
    if (mean && mean->is_number()) {
      const auto time = boost::json::value_to<double>(*mean);
      mean_age = static_cast<double>(age_at) >= time
                     ? std::to_string(static_cast<double>(age_at) - time) + "s"
                     : "future";
    }
    Wrap(summary_lines, "Age mean: " + mean_age, width);
    Fields(
        summary_lines, *summary, width,
        {"transaction_count", "memory_usage", "serialized_size", "weight",
         "fees", "minimum_fee_rate", "maximum_fee_rate", "average_fee_rate"});
  } else {
    Wrap(summary_lines, "Pool totals: N/A", width);
    Wrap(summary_lines,
         error == "N/A" || error.empty() ? "Waiting for a snapshot" : error,
         width);
    auto unavailable = PoolSummaryJson(ChainPoolSummary{});
    for (auto& field : unavailable)
      if (!field.value().is_object()) field.value() = nullptr;
    Fields(summary_lines, unavailable, width);
  }
  summary_rows_ = static_cast<std::size_t>(std::max(1, layout[1].Rows()));
  summary_max_ =
      canvas.Section(layout[1], "2 Pool totals / distributions",
                     active && focus_ == 1, summary_lines, summary_offset_);
  const auto* detail = Object(page, "detail");
  if (detail && !selected_id_.empty() && Text(*detail, "id") == selected_id_) {
    Fields(detail_lines, *detail, layout[2].Columns());
    Wrap(detail_lines, "age: " + Age(*detail, "first_seen", age_at),
         layout[2].Columns());
    if (summary)
      for (auto name : {"fee_unit", "fee_rate_unit", "ancestor_size_unit",
                        "byte_definition"})
        Wrap(detail_lines, std::string(name) + ": " + Text(*summary, name),
             layout[2].Columns());
    if (Text(page, "detail_error") != "")
      Wrap(detail_lines, Text(page, "detail_error"), layout[2].Columns());
  } else {
    Wrap(detail_lines,
         count_ ? "Selected transaction details pending / unavailable"
                : "No selected transaction",
         layout[2].Columns());
    auto unavailable = PoolTransactionJson(ChainPoolTransaction{});
    unavailable["id"] = nullptr;
    Fields(detail_lines, unavailable, layout[2].Columns());
  }
  detail_rows_ = static_cast<std::size_t>(std::max(1, layout[2].Rows()));
  detail_max_ =
      canvas.Section(layout[2], "2 Transaction details", active && focus_ == 2,
                     detail_lines, detail_offset_);
  canvas.Put(
      rows - 1, 0, columns,
      retained
          ? "x Switch Pane | Enter inspect | Backspace back | captured source"
          : "s source | a auto | x Switch Pane | Enter inspect | Backspace "
            "back");
  std::vector<PoolViewLine> result;
  for (auto& line : canvas.lines)
    result.push_back({std::move(line.text), line.selected, line.selected_column,
                      line.selected_width});
  return result;
}
}  // namespace bbp
