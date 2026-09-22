#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <thread>

#include "bbp/chain_view.h"

namespace bbp {
enum class ChainNavigation {
  kUp,
  kDown,
  kPageUp,
  kPageDown,
  kHome,
  kEnd,
  kFocus,
  kInspect,
  kBack
};
struct ChainViewLine {
  std::string text;
  bool selected = false;
  int selected_column = 0, selected_width = 0;
};

class TuiChainPane {
 public:
  TuiChainPane();
  ~TuiChainPane();
  void Reset();
  void Cancel();
  void Refresh(std::shared_ptr<ChainViewService> service, int rows,
               int columns = 80);
  void Navigate(ChainNavigation key);
  std::vector<ChainViewLine> Lines(int rows, int columns,
                                   bool active = true) const;
  void FocusBlocks() { focus_ = 0; }
  void FocusTransactions() { focus_ = 2; }
  bool block_focused() const { return focus_ < 2; }
  std::uint64_t revision() const { return revision_.load(); }
  std::optional<std::uint64_t> selected_height() const { return selected_; }
  bool following_tip() const { return following_; }

 private:
  void Work(std::stop_token stop);
  void Poll();
  std::shared_ptr<ChainViewService> service_;
  std::shared_ptr<const boost::json::object> page_;
  std::optional<std::uint64_t> tip_, selected_;
  bool following_ = true;
  unsigned focus_ = 0;
  std::size_t detail_offset_ = 0, transaction_index_ = 0;
  std::size_t transaction_detail_offset_ = 0;
  std::string transaction_block_;
  mutable std::size_t transaction_rows_ = 1, detail_rows_ = 1,
                      transaction_detail_rows_ = 1;
  std::uint32_t page_rows_ = 8;
  mutable std::size_t detail_scroll_max_ = 0, transaction_detail_max_ = 0;
  std::optional<ChainViewRequest> last_request_;
  std::chrono::steady_clock::time_point next_refresh_{};
  std::mutex mutex_;
  std::condition_variable_any ready_;
  std::stop_source request_stop_;
  std::shared_ptr<ChainViewService> queued_service_;
  ChainViewRequest queued_request_;
  std::uint64_t generation_ = 0;
  bool queued_ = false;
  std::atomic<bool> pending_ = false;
  std::shared_ptr<const boost::json::object> completed_;
  std::atomic<std::uint64_t> revision_ = 0;
  std::jthread worker_;
};
}  // namespace bbp
