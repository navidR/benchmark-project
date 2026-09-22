#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <thread>

#include "bbp/pool_view.h"

namespace bbp {
enum class PoolNavigation {
  kUp,
  kDown,
  kPageUp,
  kPageDown,
  kHome,
  kEnd,
  kFocus,
  kInspect,
  kBack,
  kNextSource,
  kAutomaticSource
};
struct PoolViewLine {
  std::string text;
  bool selected = false;
  int selected_column = 0, selected_width = 0;
};

class TuiPoolPane {
 public:
  TuiPoolPane();
  ~TuiPoolPane();
  void Reset();
  void Cancel();
  void Refresh(std::shared_ptr<PoolViewService> service, int rows,
               int columns = 80);
  void Navigate(PoolNavigation key);
  std::vector<PoolViewLine> Lines(int rows, int columns,
                                  bool active = true) const;
  void FocusList() { focus_ = 0; }
  void FocusDetails() { focus_ = 2; }
  bool list_focused() const { return focus_ == 0; }
  std::uint64_t revision() const { return revision_.load(); }
  const std::string& selected_id() const { return selected_id_; }

 private:
  void Work(std::stop_token stop);
  void Poll();
  std::shared_ptr<PoolViewService> service_;
  std::shared_ptr<const boost::json::object> page_;
  std::string selected_id_, notice_;
  std::string switching_selection_;
  std::optional<std::string> pinned_source_;
  std::vector<std::string> sources_;
  bool retained_ = false;
  std::uint32_t index_ = 0, count_ = 0, page_rows_ = 8;
  unsigned focus_ = 0;
  std::size_t summary_offset_ = 0, detail_offset_ = 0;
  mutable std::size_t summary_rows_ = 1, detail_rows_ = 1;
  mutable std::size_t summary_max_ = 0, detail_max_ = 0;
  std::optional<PoolViewRequest> last_request_;
  std::chrono::steady_clock::time_point next_refresh_{};
  std::mutex mutex_;
  std::condition_variable_any ready_;
  std::stop_source request_stop_;
  std::shared_ptr<PoolViewService> queued_service_;
  PoolViewRequest queued_request_;
  std::uint64_t generation_ = 0;
  bool queued_ = false;
  std::atomic<bool> pending_ = false;
  std::shared_ptr<const boost::json::object> completed_;
  std::atomic<std::uint64_t> revision_ = 0;
  std::jthread worker_;
};
}  // namespace bbp
