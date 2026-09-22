#include "bbp/tui_pool_pane.h"

#include <algorithm>
#include <boost/json.hpp>
#include <limits>

#include "bbp/util.h"

namespace bbp {
namespace {
std::string Text(const boost::json::object& o, std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v || v->is_null()) return "N/A";
  return v->is_string() ? std::string(v->as_string())
                        : boost::json::serialize(*v);
}
const boost::json::object* Object(const boost::json::object& o,
                                  std::string_view key) {
  const auto* v = o.if_contains(key);
  return v && v->is_object() ? &v->as_object() : nullptr;
}
int PageRows(int rows) { return std::clamp((rows - 9) / 3, 1, 16); }
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
std::string Age(const boost::json::object& object, std::string_view key,
                std::uint64_t now) {
  const auto* v = object.if_contains(key);
  if (!v || v->is_null()) return "N/A";
  const auto timestamp = boost::json::value_to<std::uint64_t>(*v);
  return now >= timestamp ? std::to_string(now - timestamp) + " s"
                          : "future timestamp";
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
  if (const auto* summary = Object(*page_, "summary")) {
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
void TuiPoolPane::Refresh(std::shared_ptr<PoolViewService> service, int rows) {
  if (service_ != service) {
    Reset();
    service_ = std::move(service);
  }
  Poll();
  page_rows_ = static_cast<std::uint32_t>(PageRows(rows));
  if (!service_) return;
  const PoolViewRequest request{selected_id_, index_, page_rows_};
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
  if (key == PoolNavigation::kFocus) {
    focus_ = (focus_ + 1) % 3;
    return;
  }
  if (focus_ != 0) {
    Move(focus_ == 1 ? summary_offset_ : detail_offset_,
         focus_ == 1 ? summary_max_ : detail_max_, key, page_rows_);
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

std::vector<PoolViewLine> TuiPoolPane::Lines(int rows, int columns) const {
  if (rows <= 0 || columns <= 0) return {};
  std::vector<PoolViewLine> lines(static_cast<std::size_t>(rows));
  const auto put = [&](int row, std::string text, bool selected = false) {
    if (row >= 0 && row < rows)
      lines[static_cast<std::size_t>(row)] = {
          text.substr(0, static_cast<std::size_t>(columns)), selected};
  };
  const boost::json::object empty;
  const auto& page = page_ ? *page_ : empty;
  const bool retained = Text(page, "mode") == "retained";
  put(0, "Blockchain Benchmark Project | Pool [o] | " + Text(page, "mode"));
  put(1, "Source: " + Text(page, "source_node") + " | sample Unix ms: " +
             Text(page, "sampled_at_ms") + (retained ? " (captured)" : ""));
  const auto error = Text(page, "error");
  put(2, error != "" && error != "N/A"
             ? error
             : std::string(pending_ ? "Loading... " : "") + notice_);
  const int count = PageRows(rows), list_top = 4;
  const int summary_top = list_top + count + 1;
  const int detail_top = summary_top + count + 1;
  put(3, std::string(focus_ == 0 ? "> " : "  ") + "Pool transactions: " +
             std::to_string(count_) + " (ordered by ID)");
  if (const auto* values = page.if_contains("rows");
      values && values->is_array()) {
    for (std::size_t i = 0;
         i < values->as_array().size() && i < static_cast<std::size_t>(count);
         ++i) {
      const auto& row = values->as_array()[i].as_object();
      const auto id = Text(row, "id");
      const bool selected = !selected_id_.empty() && id == selected_id_;
      put(list_top + static_cast<int>(i),
          (selected ? "> " : "  ") + id + " bytes=" +
              Text(row, "serialized_size") + " fee=" + Text(row, "fee"),
          selected);
    }
    if (values->as_array().empty() && Object(page, "summary"))
      put(list_top, "Pool is empty");
  }
  std::vector<std::string> summary_lines, detail_lines;
  std::uint64_t age_at = NowUnixMillis() / 1000;
  if (retained && page.contains("sampled_at_ms") &&
      !page.at("sampled_at_ms").is_null())
    age_at = JsonUint(page, "sampled_at_ms") / 1000;
  const auto append = [&](std::vector<std::string>& target, std::string text) {
    do {
      target.push_back(text.substr(0, static_cast<std::size_t>(columns)));
      text.erase(0, std::min(text.size(), static_cast<std::size_t>(columns)));
    } while (!text.empty());
  };
  if (const auto* summary = Object(page, "summary")) {
    for (auto name : {"serialized_size", "weight"}) {
      const auto* stats = Object(*summary, name);
      for (auto field : {"total", "minimum", "maximum", "average"})
        append(summary_lines, std::string(name) + " " + field + ": " +
                                  (stats ? Text(*stats, field) : "N/A"));
    }
    for (auto name : {"total_fees", "memory_usage", "minimum_fee_rate",
                      "maximum_fee_rate", "average_fee_rate", "fee_unit",
                      "fee_rate_unit", "ancestor_size_unit"})
      append(summary_lines, std::string(name) + ": " + Text(*summary, name));
    append(summary_lines,
           "oldest age: " + Age(*summary, "oldest_first_seen", age_at));
    append(summary_lines, Text(*summary, "byte_definition"));
  }
  const auto* detail = Object(page, "detail");
  if (detail && !selected_id_.empty() && Text(*detail, "id") == selected_id_) {
    for (auto name : {"id", "first_seen", "serialized_size", "weight",
                      "metadata_size", "fee", "fee_rate", "input_count",
                      "output_count", "dependencies", "ancestor_count",
                      "ancestor_size", "descendant_count", "descendant_size",
                      "replaceable", "relayed", "do_not_relay", "validation"})
      append(detail_lines, std::string(name) + ": " + Text(*detail, name));
    append(detail_lines, "age: " + Age(*detail, "first_seen", age_at));
    if (Text(page, "detail_error") != "")
      append(detail_lines, Text(page, "detail_error"));
  } else
    detail_lines.push_back(count_ ? "Selected transaction details pending"
                                  : "N/A");
  const auto section = [&](int top, int available,
                           const std::vector<std::string>& text,
                           std::size_t offset, std::size_t& maximum) {
    const auto size = static_cast<std::size_t>(std::max(0, available));
    maximum = text.size() > size ? text.size() - size : 0;
    offset = std::min(offset, maximum);
    for (std::size_t i = 0; i < size && offset + i < text.size(); ++i)
      put(top + static_cast<int>(i), text[offset + i]);
  };
  put(summary_top - 1,
      std::string(focus_ == 1 ? "> " : "  ") +
          "Pool totals (bytes; native weight; stated fee units)");
  section(summary_top, count, summary_lines, summary_offset_, summary_max_);
  put(detail_top - 1, std::string(focus_ == 2 ? "> " : "  ") +
                          "Selected transaction (x changes focus)");
  section(detail_top, rows - detail_top - 1, detail_lines, detail_offset_,
          detail_max_);
  put(rows - 1,
      "o pool | Tab views | x focus | arrows/PgUp/PgDn/Home/End | q quit");
  return lines;
}
}  // namespace bbp
