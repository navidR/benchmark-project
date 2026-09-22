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
  kFocus
};
struct ChainViewLine {
  std::string text;
  bool selected = false;
};

class TuiChainPane {
 public:
  TuiChainPane();
  ~TuiChainPane();
  void Reset();
  void Cancel();
  void Refresh(std::shared_ptr<ChainViewService> service, int rows);
  void Navigate(ChainNavigation key);
  std::vector<ChainViewLine> Lines(int rows, int columns) const;
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
  std::size_t detail_offset_ = 0, transaction_offset_ = 0;
  std::uint32_t page_rows_ = 8;
  mutable std::size_t detail_scroll_max_ = 0, transaction_scroll_max_ = 0;
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
