#include "bbp/tui_chain_pane.h"

#include <algorithm>
#include <boost/json.hpp>

#include "bbp/util.h"

namespace bbp {
namespace {
std::string Text(const boost::json::object& o, std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v || v->is_null()) return "N/A";
  if (v->is_string()) return std::string(v->as_string());
  return boost::json::serialize(*v);
}
std::optional<std::uint64_t> Uint(const boost::json::object& o,
                                  std::string_view key) {
  const auto* v = o.if_contains(key);
  if (!v || v->is_null()) return {};
  return boost::json::value_to<std::uint64_t>(*v);
}
const boost::json::object* Object(const boost::json::object& o,
                                  std::string_view key) {
  const auto* v = o.if_contains(key);
  return v && v->is_object() ? &v->as_object() : nullptr;
}
int PageRows(int rows) { return std::clamp((rows - 7) / 3, 1, 16); }
void MoveOffset(std::size_t& offset, ChainNavigation key, std::size_t page) {
  if (key == ChainNavigation::kHome)
    offset = 0;
  else if (key == ChainNavigation::kEnd)
    offset = 100000000;
  else if (key == ChainNavigation::kUp) {
    if (offset) --offset;
  } else if (key == ChainNavigation::kDown)
    ++offset;
  else if (key == ChainNavigation::kPageUp)
    offset -= std::min(offset, page);
  else if (key == ChainNavigation::kPageDown)
    offset += page;
}
}  // namespace

TuiChainPane::TuiChainPane()
    : worker_([this](std::stop_token stop) { Work(stop); }) {}
TuiChainPane::~TuiChainPane() {
  Cancel();
  worker_.request_stop();
  ready_.notify_all();
}
void TuiChainPane::Cancel() {
  std::lock_guard lock(mutex_);
  request_stop_.request_stop();
  ++generation_;
  queued_ = false;
  pending_ = false;
  completed_.reset();
  last_request_.reset();
}
void TuiChainPane::Reset() {
  Cancel();
  service_.reset();
  page_.reset();
  tip_.reset();
  selected_.reset();
  following_ = true;
  focus_ = 0;
  detail_offset_ = transaction_offset_ = 0;
}
void TuiChainPane::Work(std::stop_token stop) {
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
    } catch (const std::exception& error) {
      page = {{"error", error.what()}};
    }
    lock.lock();
    if (generation == generation_ && !token.stop_requested()) {
      completed_ = std::make_shared<const boost::json::object>(std::move(page));
      pending_ = false;
      ++revision_;
    }
  }
}
void TuiChainPane::Poll() {
  std::shared_ptr<const boost::json::object> completed;
  {
    std::lock_guard lock(mutex_);
    completed = std::exchange(completed_, {});
  }
  if (!completed) return;
  page_ = std::move(completed);
  const auto* tip = Object(*page_, "tip");
  if (!tip) {
    tip_.reset();
    return;
  }
  tip_ = Uint(*tip, "height");
  if (tip_)
    selected_ = following_ ? *tip_ : std::min(selected_.value_or(*tip_), *tip_);
}
void TuiChainPane::Refresh(std::shared_ptr<ChainViewService> service,
                           int rows) {
  if (service_ != service) {
    Reset();
    service_ = std::move(service);
  }
  Poll();
  page_rows_ = static_cast<std::uint32_t>(PageRows(rows));
  if (!service_) return;
  ChainViewRequest request{
      .first_height = {}, .selected_height = {}, .limit = page_rows_};
  if (!following_ && selected_) {
    request.selected_height = selected_;
    request.first_height =
        *selected_ >= page_rows_ / 2 ? *selected_ - page_rows_ / 2 : 0;
  }
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
void TuiChainPane::Navigate(ChainNavigation key) {
  if (key == ChainNavigation::kFocus) {
    focus_ = (focus_ + 1) % 3;
    return;
  }
  if (focus_ != 0) {
    auto& offset = focus_ == 1 ? detail_offset_ : transaction_offset_;
    const auto maximum =
        focus_ == 1 ? detail_scroll_max_ : transaction_scroll_max_;
    offset = std::min(offset, maximum);
    MoveOffset(offset, key, page_rows_);
    offset = std::min(offset, maximum);
    return;
  }
  if (!selected_ || !tip_) return;
  if (key == ChainNavigation::kHome)
    selected_ = 0;
  else if (key == ChainNavigation::kEnd)
    selected_ = tip_;
  else if (key == ChainNavigation::kUp) {
    if (*selected_) --*selected_;
  } else if (key == ChainNavigation::kDown) {
    if (*selected_ < *tip_) ++*selected_;
  } else if (key == ChainNavigation::kPageUp)
    *selected_ -= std::min<std::uint64_t>(*selected_, page_rows_);
  else if (key == ChainNavigation::kPageDown)
    *selected_ += std::min<std::uint64_t>(*tip_ - *selected_, page_rows_);
  following_ = selected_ == tip_;
  detail_offset_ = transaction_offset_ = 0;
}

std::vector<ChainViewLine> TuiChainPane::Lines(int rows, int columns) const {
  if (rows <= 0 || columns <= 0) return {};
  std::vector<ChainViewLine> lines(static_cast<std::size_t>(rows));
  const auto put = [&](int line, std::string text, bool selected = false) {
    if (line >= 0 && line < rows)
      lines[static_cast<std::size_t>(line)] = {
          text.substr(0, static_cast<std::size_t>(columns)), selected};
  };
  const boost::json::object empty;
  const auto& page = page_ ? *page_ : empty;
  put(0, "Blockchain Benchmark Project | Chain [v] | " + Text(page, "mode"));
  put(1, "Source: " + Text(page, "source_node") +
             " | Tip: " + (tip_ ? std::to_string(*tip_) : "N/A") +
             (following_ ? " | following tip" : " | selection held"));
  put(2, page.contains("error")
             ? Text(page, "error")
             : (pending_.load() ? "Loading... " : "") + Text(page, "notice"));
  const int count = PageRows(rows), block_top = 4,
            detail_top = block_top + count + 1, tx_top = detail_top + count + 1;
  put(3, std::string(focus_ == 0 ? "> " : "  ") + "Blocks: genesis -> tip");
  const auto first =
      following_ && tip_ ? (*tip_ >= static_cast<std::uint64_t>(count - 1)
                                ? *tip_ - static_cast<std::uint64_t>(count - 1)
                                : 0)
      : selected_ && *selected_ >= static_cast<std::uint64_t>(count / 2)
          ? *selected_ - static_cast<std::uint64_t>(count / 2)
          : 0;
  const auto* values = page.if_contains("rows");
  for (int i = 0;
       i < count && tip_ && first + static_cast<std::uint64_t>(i) <= *tip_;
       ++i) {
    const auto height = first + static_cast<std::uint64_t>(i);
    std::string text = std::to_string(height) + "  loading...";
    if (values && values->is_array())
      for (const auto& value : values->as_array()) {
        const auto& row = value.as_object();
        if (Uint(row, "height") != height) continue;
        if (Text(row, "error") != "")
          text = std::to_string(height) + "  " + Text(row, "error");
        else if (const auto* summary = Object(row, "summary"))
          text = std::to_string(height) + "  " + Text(*summary, "hash");
        else
          text = std::to_string(height) + "  " + Text(row, "error");
        break;
      }
    const bool selected = selected_ == height;
    put(block_top + i, (selected ? "> " : "  ") + text, selected);
  }
  std::vector<std::string> details, transactions;
  const auto* detail = Object(page, "detail");
  const auto* block = detail ? Object(*detail, "block") : nullptr;
  if (block && Uint(*block, "height") == selected_) {
    for (auto name : {"height", "hash", "previous_hash", "next_hash",
                      "timestamp", "confirmations", "serialized_size", "weight",
                      "header_size", "transaction_count"})
      details.push_back(std::string(name) + ": " + Text(*block, name));
    const auto timestamp = Uint(*block, "timestamp");
    const auto now = NowUnixMillis() / 1000;
    details.push_back(
        "age: " + (timestamp ? (now >= *timestamp
                                    ? std::to_string(now - *timestamp) + " s"
                                    : "future timestamp")
                             : "N/A"));
    for (auto name : {"transaction_bytes", "minimum_transaction_size",
                      "maximum_transaction_size", "average_transaction_size",
                      "miner_transaction_size", "metadata_bytes"})
      details.push_back(std::string(name) + ": " + Text(*detail, name));
    const auto definition = Text(*detail, "byte_definition");
    for (std::size_t i = 0; i < definition.size();
         i += static_cast<std::size_t>(columns))
      details.push_back(
          definition.substr(i, static_cast<std::size_t>(columns)));
    if (const auto* txs = detail->if_contains("transactions");
        txs && txs->is_array()) {
      for (const auto& item : txs->as_array()) {
        const auto& tx = item.as_object();
        transactions.push_back("id: " + Text(tx, "id"));
        transactions.push_back("bytes: " + Text(tx, "serialized_size") +
                               " weight: " + Text(tx, "weight") +
                               " extra: " + Text(tx, "metadata_size"));
        transactions.push_back("fee: " + Text(tx, "fee") +
                               " in/out: " + Text(tx, "input_count") + "/" +
                               Text(tx, "output_count") +
                               " coinbase: " + Text(tx, "coinbase"));
        if (Text(tx, "error") != "") transactions.push_back(Text(tx, "error"));
      }
    }
  } else
    details.push_back(page.contains("detail_error")
                          ? Text(page, "detail_error")
                          : "Block details pending / not captured");
  put(detail_top - 1, std::string(focus_ == 1 ? "> " : "  ") +
                          "Selected block (bytes; native weight)");
  detail_scroll_max_ = details.size() > static_cast<std::size_t>(count)
                           ? details.size() - static_cast<std::size_t>(count)
                           : 0;
  const auto detail_offset = std::min(
      detail_offset_, details.size() > static_cast<std::size_t>(count)
                          ? details.size() - static_cast<std::size_t>(count)
                          : 0);
  for (int i = 0; i < count &&
                  detail_offset + static_cast<std::size_t>(i) < details.size();
       ++i)
    put(detail_top + i, details[detail_offset + static_cast<std::size_t>(i)]);
  put(tx_top - 1, std::string(focus_ == 2 ? "> " : "  ") +
                      "Transactions (x changes focus)");
  const auto tx_count =
      static_cast<std::size_t>(std::max(0, rows - tx_top - 1));
  transaction_scroll_max_ =
      transactions.size() > tx_count ? transactions.size() - tx_count : 0;
  const auto tx_offset = std::min(
      transaction_offset_,
      transactions.size() > tx_count ? transactions.size() - tx_count : 0);
  for (std::size_t i = 0; i < tx_count && tx_offset + i < transactions.size();
       ++i)
    put(tx_top + static_cast<int>(i), transactions[tx_offset + i]);
  put(rows - 1,
      "v chain | Tab views | x focus | arrows/PgUp/PgDn/Home/End | q quit");
  return lines;
}
}  // namespace bbp
