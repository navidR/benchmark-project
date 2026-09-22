#include "bbp/tui_chain_pane.h"

#include <algorithm>
#include <boost/json.hpp>

#include "bbp/util.h"
#include "tui_browse_layout.h"

namespace bbp {
namespace {
using browse::Object;
using browse::Text;
using browse::Uint;
void Move(std::size_t& offset, std::size_t maximum, ChainNavigation key,
          std::size_t page) {
  offset = std::min(offset, maximum);
  if (key == ChainNavigation::kHome) offset = 0;
  if (key == ChainNavigation::kEnd) offset = maximum;
  if (key == ChainNavigation::kUp && offset) --offset;
  if (key == ChainNavigation::kDown && offset < maximum) ++offset;
  if (key == ChainNavigation::kPageUp) offset -= std::min(offset, page);
  if (key == ChainNavigation::kPageDown)
    offset += std::min(maximum - offset, page);
}
const boost::json::array* Transactions(const boost::json::object* page,
                                       std::optional<std::uint64_t> selected) {
  const auto* detail = page ? Object(*page, "detail") : nullptr;
  const auto* block = detail ? Object(*detail, "block") : nullptr;
  const auto* values = detail ? detail->if_contains("transactions") : nullptr;
  return block && Uint(*block, "height") == selected && values &&
                 values->is_array()
             ? &values->as_array()
             : nullptr;
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
  detail_offset_ = transaction_index_ = transaction_detail_offset_ = 0;
  transaction_block_.clear();
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
  const auto* transactions = Transactions(page_.get(), selected_);
  if (!transactions) return;
  const auto& block = Object(*page_, "detail")->at("block").as_object();
  const auto hash = Text(block, "hash");
  if (hash != transaction_block_) {
    transaction_index_ = transaction_detail_offset_ = 0;
    transaction_block_ = hash;
  }
  transaction_index_ =
      transactions->empty()
          ? 0
          : std::min(transaction_index_, transactions->size() - 1);
}
void TuiChainPane::Refresh(std::shared_ptr<ChainViewService> service, int rows,
                           int columns) {
  if (service_ != service) {
    Reset();
    service_ = std::move(service);
  }
  Poll();
  const auto layout = browse::Layout(rows, columns, focus_, false);
  page_rows_ =
      static_cast<std::uint32_t>(std::clamp(layout[0].Rows() - 1, 1, 32));
  if (!service_) return;
  ChainViewRequest request{
      .first_height = {}, .selected_height = {}, .limit = page_rows_};
  if (!following_ && selected_) {
    request.selected_height = selected_;
    request.first_height =
        *selected_ >= page_rows_ / 2 ? *selected_ - page_rows_ / 2 : 0;
    if (tip_)
      request.first_height =
          std::min(*request.first_height,
                   *tip_ >= page_rows_ - 1 ? *tip_ - page_rows_ + 1 : 0);
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
  if (key == ChainNavigation::kInspect) {
    focus_ = focus_ ^ 1U;
    return;
  }
  if (key == ChainNavigation::kBack) {
    focus_ = focus_ < 2 ? 0 : 2;
    return;
  }

  if (key == ChainNavigation::kFocus) {
    focus_ = (focus_ + 1) % 4;
    return;
  }
  if (focus_ == 1 || focus_ == 3) {
    Move(focus_ == 1 ? detail_offset_ : transaction_detail_offset_,
         focus_ == 1 ? detail_scroll_max_ : transaction_detail_max_, key,
         focus_ == 1 ? detail_rows_ : transaction_detail_rows_);
    return;
  }
  if (focus_ == 2) {
    const auto* transactions = Transactions(page_.get(), selected_);
    if (!transactions || transactions->empty()) return;
    Move(transaction_index_, transactions->size() - 1, key, transaction_rows_);
    transaction_detail_offset_ = 0;
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
  detail_offset_ = transaction_index_ = transaction_detail_offset_ = 0;
  transaction_block_.clear();
  Cancel();
}

std::vector<ChainViewLine> TuiChainPane::Lines(int rows, int columns,
                                               bool active) const {
  if (rows <= 0 || columns <= 0) return {};
  using namespace browse;
  Canvas canvas(rows, columns);
  const auto layout = Layout(rows, columns, focus_, false);
  const boost::json::object empty;
  const auto& page = page_ ? *page_ : empty;
  canvas.Put(0, 0, columns, std::string("CHAIN | ") + Text(page, "mode"));
  const auto source = Text(page, "source_node");
  canvas.Put(1, 0, columns,
             "Source: " + (source.empty() ? std::string("N/A") : source) +
                 " | Tip: " + (tip_ ? std::to_string(*tip_) : "N/A") +
                 (following_ ? " | following tip" : " | selection held"));
  const auto error = Text(page, "error");
  canvas.Put(
      2, 0, columns,
      error != "" && error != "N/A"
          ? error
          : (pending_ ? "Loading... " : "") +
                (page.contains("notice") ? Text(page, "notice")
                                         : "Waiting for chain snapshot"));
  const auto now = NowUnixMillis() / 1000;
  const auto list = layout[0];
  const auto count =
      static_cast<std::uint64_t>(std::clamp(list.Rows() - 1, 1, 32));
  auto first = following_ && tip_ ? (*tip_ >= count - 1 ? *tip_ - count + 1 : 0)
               : selected_ && *selected_ >= count / 2 ? *selected_ - count / 2
                                                      : 0;
  if (tip_) first = std::min(first, *tip_ >= count - 1 ? *tip_ - count + 1 : 0);
  const auto block_columns =
      Columns(list.Columns(), {{"height", "Height", 8},
                               {"transaction_count", "Txs", 6},
                               {"serialized_size", "Bytes", 10},
                               {"age", "Age", 9},
                               {"confirmations", "Conf", 7},
                               {"weight", "Weight", 10},
                               {"hash", "Hash", 18},
                               {"timestamp", "Unix time", 12}});
  canvas.Frame(list, "3 Blocks", active && focus_ == 0,
               tip_ ? Position(first, count, *tip_ + 1) : "0/0");
  canvas.Body(list, 0, TableRow(block_columns));
  const auto* values = page.if_contains("rows");
  for (std::uint64_t i = 0; i < count && tip_ && first + i <= *tip_; ++i) {
    const auto height = first + i;
    std::string text = std::to_string(height) + "  loading / unavailable";
    if (values && values->is_array()) {
      for (const auto& value : values->as_array()) {
        const auto& row = value.as_object();
        if (Uint(row, "height") != height) continue;
        if (const auto* summary = Object(row, "summary")) {
          auto fields = *summary;
          const auto* selected_detail = Object(page, "detail");
          const auto* selected_block =
              selected_detail ? Object(*selected_detail, "block") : nullptr;
          if (selected_block &&
              Text(*selected_block, "hash") == Text(*summary, "hash"))
            fields = *selected_block;
          fields["age"] = Age(fields, "timestamp", now);
          text = TableRow(block_columns, &fields);
        } else
          text = std::to_string(height) + " " + Text(row, "error");
        break;
      }
    }
    canvas.Body(list, static_cast<int>(i + 1), std::move(text),
                selected_ == height);
  }
  if (!tip_)
    canvas.Body(list, 1, pending_ ? "Loading blocks..." : "No block snapshot");

  const auto* detail = Object(page, "detail");
  const auto* block = detail ? Object(*detail, "block") : nullptr;
  const bool matches = block && Uint(*block, "height") == selected_;
  std::vector<std::string> block_fields;
  if (matches) {
    Fields(block_fields, *block, layout[1].Columns());
    Wrap(block_fields, "age: " + Age(*block, "timestamp", now),
         layout[1].Columns());
    Fields(block_fields, *detail, layout[1].Columns(),
           {"block", "transactions"});
  } else {
    Wrap(block_fields, Text(page, "detail_error"), layout[1].Columns());
    Wrap(block_fields, "Block details pending / not captured",
         layout[1].Columns());
    auto unavailable = ChainBlockDetailJson(ChainBlockDetail{});
    unavailable.at("block").as_object()["height"] = nullptr;
    unavailable.at("block").as_object()["hash"] = nullptr;
    Fields(block_fields, unavailable.at("block").as_object(),
           layout[1].Columns());
    Fields(block_fields, unavailable, layout[1].Columns(),
           {"block", "transactions"});
  }
  detail_rows_ = static_cast<std::size_t>(std::max(1, layout[1].Rows()));
  detail_scroll_max_ =
      canvas.Section(layout[1], "4 Block details", active && focus_ == 1,
                     block_fields, detail_offset_);

  const auto tx_rect = layout[2];
  const auto* transactions = Transactions(page_.get(), selected_);
  const std::size_t total = transactions ? transactions->size() : 0;
  transaction_rows_ = static_cast<std::size_t>(std::max(1, tx_rect.Rows() - 1));
  const auto tx_index = total ? std::min(transaction_index_, total - 1) : 0;
  const auto tx_first = total > transaction_rows_
                            ? std::min(tx_index > transaction_rows_ / 2
                                           ? tx_index - transaction_rows_ / 2
                                           : 0,
                                       total - transaction_rows_)
                            : 0;
  const auto tx_columns =
      Columns(tx_rect.Columns(), {{"id", "Transaction ID", 18},
                                  {"serialized_size", "Bytes", 9},
                                  {"fee", "Fee (stated units)", 23},
                                  {"weight", "Weight", 9},
                                  {"input_count", "Inputs", 6},
                                  {"output_count", "Outputs", 7},
                                  {"metadata_size", "Metadata B", 10},
                                  {"coinbase", "Coinbase", 8}});
  canvas.Frame(tx_rect, "4 Block transactions", active && focus_ == 2,
               Position(tx_first, transaction_rows_, total));
  canvas.Body(tx_rect, 0, TableRow(tx_columns));
  for (std::size_t i = 0; i < transaction_rows_ && tx_first + i < total; ++i)
    canvas.Body(
        tx_rect, static_cast<int>(i + 1),
        TableRow(tx_columns, &(*transactions)[tx_first + i].as_object()),
        tx_first + i == tx_index);
  if (!total)
    canvas.Body(
        tx_rect, 1,
        matches ? "No transactions supplied" : "Waiting for selected block");
  std::vector<std::string> tx_fields;
  if (total) {
    Fields(tx_fields, (*transactions)[tx_index].as_object(),
           layout[3].Columns());
  } else {
    Wrap(tx_fields, "Transaction details: N/A", layout[3].Columns());
    ChainBlockDetail unavailable;
    unavailable.transactions.emplace_back();
    const auto fields = ChainBlockDetailJson(unavailable)
                            .at("transactions")
                            .as_array()
                            .front()
                            .as_object();
    Fields(tx_fields, fields, layout[3].Columns());
  }
  transaction_detail_rows_ =
      static_cast<std::size_t>(std::max(1, layout[3].Rows()));
  transaction_detail_max_ =
      canvas.Section(layout[3], "4 Transaction details", active && focus_ == 3,
                     tx_fields, transaction_detail_offset_);
  canvas.Put(rows - 1, 0, columns,
             "x Switch Pane | Enter inspect | Backspace back");
  std::vector<ChainViewLine> result;
  for (auto& line : canvas.lines)
    result.push_back({std::move(line.text), line.selected, line.selected_column,
                      line.selected_width});
  return result;
}
}  // namespace bbp
