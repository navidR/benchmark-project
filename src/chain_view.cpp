#include "bbp/chain_view.h"

#include <unistd.h>

#include <algorithm>
#include <boost/json.hpp>
#include <limits>

#include "bbp/simulation_cancelled.h"
#include "bbp/util.h"

namespace bbp {
namespace {
constexpr std::size_t kMaximumRecordBytes = 4 * 1024 * 1024;
void Check(std::stop_token stop) {
  if (stop.stop_requested()) throw SimulationCancelled();
}
ChainBlockSummary ParseSummary(const boost::json::object& o) {
  ChainBlockSummary s;
  s.height = JsonUint(o, "height");
  s.hash = JsonString(o, "hash");
  if (s.hash.size() != 64)
    throw std::runtime_error("invalid captured block hash");
  const auto number = [&](const char* name, auto& field) {
    const auto* v = o.if_contains(name);
    if (v && !v->is_null()) field = JsonUint(o, name);
  };
  for (const auto& [key, destination] :
       {std::pair{"previous_hash", &s.previous_hash},
        std::pair{"next_hash", &s.next_hash}}) {
    const auto* v = o.if_contains(key);
    if (v && !v->is_null()) *destination = JsonString(o, key);
  }
  number("timestamp", s.timestamp);
  number("serialized_size", s.serialized_size);
  number("weight", s.weight);
  number("header_size", s.header_size);
  number("transaction_count", s.transaction_count);
  if (const auto* v = o.if_contains("confirmations"); v && !v->is_null())
    s.confirmations = boost::json::value_to<std::int64_t>(*v);
  return s;
}
}  // namespace

ChainViewService::ChainViewService(std::filesystem::path run_root,
                                   Readers readers)
    : path_(std::move(run_root) / "chain-blocks.json"),
      readers_(std::move(readers)) {}
ChainViewService::~ChainViewService() { Close(); }

void ChainViewService::SetObserver(
    std::function<void(const boost::json::object&)> observer) {
  std::lock_guard lock(mutex_);
  if (!closing_.stop_requested()) observer_ = std::move(observer);
}

void ChainViewService::Close() {
  closing_.request_stop();
  std::lock_guard lock(mutex_);
  readers_ = {};
  observer_ = {};
}

void ChainViewService::Load(std::stop_token stop) {
  if (loaded_) return;
  if (!std::filesystem::exists(path_)) {
    loaded_ = true;
    return;
  }
  const auto doc =
      boost::json::parse(ReadText(path_, kMaximumRecordBytes, stop))
          .as_object();
  if (JsonUint(doc, "version") != 1)
    throw std::runtime_error("unsupported captured block format");
  if (const auto* t = doc.if_contains("tip"); t && t->is_object())
    tip_ = ParseSummary(t->as_object());
  source_ = JsonString(doc, "source_node");
  const auto& records = doc.at("records").as_array();
  if (records.size() > kMaximumEntries)
    throw std::runtime_error("captured block cache exceeds bound");
  for (const auto& record : records) {
    const auto& o = record.as_object();
    Entry entry;
    entry.summary = ParseSummary(o.at("summary").as_object());
    entry.used = ++used_;
    if (const auto* d = o.if_contains("detail"); d && d->is_object()) {
      entry.detail = d->as_object();
      const auto summary = ParseSummary(entry.detail.at("block").as_object());
      if (summary.height != entry.summary.height ||
          summary.hash != entry.summary.hash)
        throw std::runtime_error("captured block detail identity mismatch");
    }
    entries_[entry.summary.height] = std::move(entry);
  }
  Trim();
  loaded_ = true;
}

void ChainViewService::Save(std::stop_token stop) {
  Check(stop);
  if (!dirty_) return;
  boost::json::array records;
  for (const auto& [height, e] : entries_) {
    static_cast<void>(height);
    records.emplace_back(boost::json::object{
        {"summary", ChainBlockSummaryJson(e.summary)},
        {"detail", e.detail.empty() ? boost::json::value(nullptr)
                                    : boost::json::value(e.detail)}});
  }
  const auto text = boost::json::serialize(boost::json::object{
      {"version", 1},
      {"source_node", source_},
      {"tip", tip_ ? boost::json::value(ChainBlockSummaryJson(*tip_))
                   : boost::json::value(nullptr)},
      {"records", std::move(records)}});
  if (text.size() > kMaximumRecordBytes)
    throw std::runtime_error("captured block record exceeds byte bound");
  const auto temporary = path_.string() + ".tmp-" + std::to_string(getpid());
  WriteText(temporary, text);
  std::filesystem::rename(temporary, path_);
  dirty_ = false;
}

void ChainViewService::Trim() {
  while (entries_.size() > kMaximumEntries) {
    const auto oldest = std::min_element(entries_.begin(), entries_.end(),
                                         [](const auto& a, const auto& b) {
                                           return a.second.used < b.second.used;
                                         });
    entries_.erase(oldest);
    dirty_ = true;
  }
  std::vector<Entry*> details;
  for (auto& [height, e] : entries_) {
    static_cast<void>(height);
    if (!e.detail.empty()) details.push_back(&e);
  }
  std::sort(details.begin(), details.end(),
            [](const auto* a, const auto* b) { return a->used > b->used; });
  std::size_t bytes = 0;
  for (std::size_t i = 0; i < details.size(); ++i) {
    const auto size = boost::json::serialize(details[i]->detail).size();
    if (i >= 4 || size > kMaximumDetailBytes - bytes) {
      details[i]->detail.clear();
      dirty_ = true;
    } else
      bytes += size;
  }
}

void ChainViewService::UpdateTip(ChainBlockReader& reader,
                                 const ChainBlockSummary& next,
                                 std::stop_token stop) {
  if (tip_ && tip_->hash != next.hash) {
    bool extends = false;
    if (tip_->height < next.height) {
      extends = reader.summary(tip_->height, stop).hash == tip_->hash;
    }
    if (!extends) {
      // Only cached history can be invalidated. A matching cached anchor may
      // be below the exact fork; conservatively discard the uncertain gap.
      std::optional<std::uint64_t> anchor;
      for (auto it = entries_.rbegin(); it != entries_.rend(); ++it) {
        Check(stop);
        if (it->first <= next.height &&
            reader.summary(it->first, stop).hash == it->second.summary.hash) {
          anchor = it->first;
          break;
        }
      }
      if (anchor)
        entries_.erase(entries_.upper_bound(*anchor), entries_.end());
      else
        entries_.clear();
      notice_ = "Reorg: invalidated " +
                (anchor ? "cached blocks above verified shared height " +
                              std::to_string(*anchor)
                        : std::string("all uncertain cached blocks"));
    }
  }
  if (!tip_ || tip_->hash != next.hash) dirty_ = true;
  tip_ = next;
}

boost::json::object ChainViewService::Query(const ChainViewRequest& request,
                                            std::stop_token caller_stop) {
  if (request.limit == 0 || request.limit > 32)
    throw std::invalid_argument("chain page limit must be 1..32");
  std::unique_lock lock(mutex_, std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(10))) Check(caller_stop);
  Check(caller_stop);
  const bool live = static_cast<bool>(readers_);
  std::stop_source cancellation;
  std::stop_callback caller_callback(caller_stop,
                                     [&] { cancellation.request_stop(); });
  std::stop_callback close_callback(closing_.get_token(), [&] {
    if (live) cancellation.request_stop();
  });
  const auto stop = cancellation.get_token();
  Load(stop);
  std::optional<ChainBlockReader> reader;
  std::string source_error;
  if (live) {
    auto sources = readers_();
    std::stable_sort(sources.begin(), sources.end(),
                     [&](const auto& a, const auto& b) {
                       return a.node_id == source_ && b.node_id != source_;
                     });
    for (auto& candidate : sources) {
      Check(stop);
      try {
        const auto tip = candidate.tip(stop);
        if (!source_.empty() && source_ != candidate.node_id) {
          notice_ = "Source changed: " + source_ + " -> " + candidate.node_id +
                    " (previous source unavailable)";
          entries_.clear();
          tip_.reset();
        }
        source_ = candidate.node_id;
        UpdateTip(candidate, tip, stop);
        reader = std::move(candidate);
        break;
      } catch (const std::exception& e) {
        Check(stop);
        source_error = candidate.node_id + ": " + e.what();
      }
    }
    if (!reader)
      return {{"mode", "live"},
              {"source_node", source_},
              {"error",
               source_error.empty() ? "No healthy source node" : source_error},
              {"tip", nullptr},
              {"rows", boost::json::array{}},
              {"detail", nullptr}};
  }
  boost::json::object result{
      {"mode", live ? "live" : "retained"},
      {"source_node", source_},
      {"notice", notice_},
      {"tip", tip_ ? boost::json::value(ChainBlockSummaryJson(*tip_))
                   : boost::json::value(nullptr)},
      {"rows", boost::json::array{}},
      {"detail", nullptr},
      {"detail_error", ""}};
  if (!tip_) {
    result["error"] = "No captured blocks; details were not captured";
    return result;
  }
  const auto selected =
      std::min(request.selected_height.value_or(tip_->height), tip_->height);
  const auto first = std::min(
      request.first_height.value_or(
          selected >= request.limit - 1 ? selected - (request.limit - 1) : 0),
      tip_->height);
  const auto last =
      first + std::min<std::uint64_t>(request.limit - 1, tip_->height - first);
  result["selected_height"] = selected;
  result["first_height"] = first;
  auto& rows = result["rows"].as_array();
  const auto fetch = [&](std::uint64_t height) -> Entry* {
    auto it = entries_.find(height);
    if (it == entries_.end() && reader) {
      auto summary = reader->summary(height, stop);
      dirty_ = true;
      if (summary.height != height || summary.confirmations.value_or(1) < 0)
        throw std::runtime_error("block is not on source canonical chain");
      it = entries_
               .emplace(height, Entry{.summary = std::move(summary),
                                      .detail = {},
                                      .used = ++used_})
               .first;
      Trim();
    }
    if (it == entries_.end()) return nullptr;
    it->second.used = ++used_;
    if (reader) {
      auto& summary = it->second.summary;
      const auto depth = tip_->height - height;
      summary.confirmations = depth < static_cast<std::uint64_t>(INT64_MAX)
                                  ? std::optional<std::int64_t>(depth + 1)
                                  : std::nullopt;
      const auto next =
          height < tip_->height ? entries_.find(height + 1) : entries_.end();
      summary.next_hash = next != entries_.end()
                              ? std::optional(next->second.summary.hash)
                              : std::nullopt;
    }
    return &it->second;
  };
  for (auto height = first;; ++height) {
    Check(stop);
    boost::json::object row{{"height", height}, {"error", ""}};
    try {
      if (auto* entry = fetch(height))
        row["summary"] = ChainBlockSummaryJson(entry->summary);
      else
        row["error"] = "Block was not captured";
    } catch (const std::exception& e) {
      Check(stop);
      row["error"] = e.what();
    }
    rows.emplace_back(std::move(row));
    if (height == last) break;
  }
  try {
    auto* entry = fetch(selected);
    if (!entry)
      result["detail_error"] = "Block details were not captured";
    else {
      if (entry->detail.empty() && reader) {
        const auto detail = reader->detail(entry->summary.hash, stop);
        if (detail.block.hash != entry->summary.hash ||
            detail.block.height != selected)
          throw std::runtime_error("selected block detail identity mismatch");
        auto json = ChainBlockDetailJson(detail);
        if (boost::json::serialize(json).size() > kMaximumDetailBytes)
          throw std::runtime_error("block detail exceeds 2 MiB display limit");
        entry->detail = std::move(json);
        dirty_ = true;
        Trim();
      }
      if (!entry->detail.empty()) {
        if (reader) {
          entry->detail.at("block").as_object()["confirmations"] =
              ChainBlockSummaryJson(entry->summary).at("confirmations");
          if (selected < tip_->height)
            entry->detail.at("block").as_object()["next_hash"] =
                reader->summary(selected + 1, stop).hash;
          else
            entry->detail.at("block").as_object()["next_hash"] = nullptr;
        }
        result["detail"] = entry->detail;
      } else
        result["detail_error"] = "Block details were not captured";
    }
  } catch (const std::exception& e) {
    Check(stop);
    result["detail_error"] = e.what();
  }
  if (!result.at("detail_error").as_string().empty()) {
    for (auto& row : rows) {
      if (JsonUint(row.as_object(), "height") == selected)
        row.as_object()["error"] = result.at("detail_error");
    }
  }
  if (reader) {
    // At most two adjacent summaries, never a full-chain scan/prefetch.
    for (auto height : {first > 0 ? first - 1 : first,
                        last < tip_->height ? last + 1 : last}) {
      try {
        static_cast<void>(fetch(height));
      } catch (...) {
        Check(stop);
      }
    }
    const auto final_tip = reader->tip(stop);
    if (final_tip.hash != tip_->hash &&
        (final_tip.height < tip_->height ||
         reader->summary(tip_->height, stop).hash != tip_->hash)) {
      UpdateTip(*reader, final_tip, stop);
      result["rows"] = boost::json::array{};
      result["detail"] = nullptr;
      result["detail_error"] = "Chain reorganized during read; refresh pending";
      result["tip"] = ChainBlockSummaryJson(*tip_);
      result["notice"] = notice_;
    }
    Trim();
    const bool changed = dirty_;
    Save(stop);
    if (changed && observer_) observer_(result);
  }
  result["cache_entries"] = entries_.size();
  return result;
}
}  // namespace bbp
