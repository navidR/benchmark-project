#include "bbp/pool_view.h"

#include <unistd.h>

#include <algorithm>
#include <boost/json.hpp>
#include <cctype>

#include "bbp/simulation_cancelled.h"
#include "bbp/util.h"

namespace bbp {
namespace {
void Check(std::stop_token stop) {
  if (stop.stop_requested()) throw SimulationCancelled();
}
}  // namespace
PoolViewService::PoolViewService(std::filesystem::path root, Readers readers)
    : path_(std::move(root) / "transaction-pool.json"),
      readers_(std::move(readers)) {}
PoolViewService::~PoolViewService() { Close(); }
void PoolViewService::Close() {
  closing_.request_stop();
  std::lock_guard lock(mutex_);
  readers_ = {};
  observer_ = {};
}
void PoolViewService::SetObserver(
    std::function<void(const boost::json::object&)> observer) {
  std::lock_guard lock(mutex_);
  if (!closing_.stop_requested()) observer_ = std::move(observer);
}
boost::json::object PoolViewService::Query(const PoolViewRequest& request,
                                           std::stop_token caller) {
  if (request.limit == 0 || request.limit > 32)
    throw std::invalid_argument("pool page limit must be 1..32");
  if (request.index >= ChainPoolSnapshot::kMaximumTransactions ||
      (!request.selected_id.empty() &&
       (request.selected_id.size() != 64 ||
        !std::all_of(request.selected_id.begin(), request.selected_id.end(),
                     [](unsigned char c) { return std::isxdigit(c) != 0; }))))
    throw std::invalid_argument("invalid pool transaction selector");
  std::unique_lock lock(mutex_, std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(10))) Check(caller);
  Check(caller);
  const bool live = static_cast<bool>(readers_);
  std::stop_source cancellation;
  std::stop_callback requested(caller, [&] { cancellation.request_stop(); });
  std::stop_callback closed(closing_.get_token(), [&] {
    if (live) cancellation.request_stop();
  });
  const auto stop = cancellation.get_token();
  boost::json::object page{{"mode", live ? "live" : "retained"},
                           {"source_node", ""},
                           {"sampled_at_ms", nullptr},
                           {"summary", nullptr},
                           {"notice", ""},
                           {"error", ""},
                           {"selected_id", ""},
                           {"selected_index", 0U},
                           {"first_index", 0U},
                           {"rows", boost::json::array{}},
                           {"detail", nullptr},
                           {"detail_error", ""},
                           {"departed_id", ""},
                           {"departure_reason", ""}};
  if (!live && !loaded_) {
    if (std::filesystem::exists(path_)) {
      auto captured =
          boost::json::parse(ReadText(path_, kMaximumBytes, stop)).as_object();
      if (JsonUint(captured, "version") != 1 ||
          captured.at("transactions").as_array().size() >
              ChainPoolSnapshot::kMaximumTransactions)
        throw std::runtime_error("invalid captured pool snapshot");
      captured_ = std::move(captured);
    }
    loaded_ = true;
  }
  std::optional<PoolReader> reader;
  ChainPoolSnapshot snapshot;
  const std::string previous_source =
      captured_.empty() ? "" : JsonString(captured_, "source_node");
  if (live) {
    auto sources = readers_();
    std::stable_sort(
        sources.begin(), sources.end(), [&](const auto& a, const auto& b) {
          return a.node_id == previous_source && b.node_id != previous_source;
        });
    for (auto& candidate : sources) {
      Check(stop);
      try {
        snapshot = candidate.snapshot(stop);
        if (snapshot.transactions.size() >
            ChainPoolSnapshot::kMaximumTransactions)
          throw std::runtime_error("pool exceeds transaction display bound");
        std::sort(snapshot.transactions.begin(), snapshot.transactions.end(),
                  [](const auto& a, const auto& b) { return a.id < b.id; });
        reader = std::move(candidate);
        break;
      } catch (const std::exception& e) {
        Check(stop);
        page["error"] = candidate.node_id + ": " + e.what();
      }
    }
    if (!reader) {
      page["source_node"] = previous_source;
      if (page.at("error").as_string().empty())
        page["error"] = "No healthy pool source";
      return page;
    }
    page["error"] = "";
    boost::json::array entries;
    for (const auto& tx : snapshot.transactions) {
      Check(stop);
      entries.emplace_back(PoolTransactionJson(tx));
    }
    boost::json::object next{{"version", 1},
                             {"source_node", reader->node_id},
                             {"sampled_at_ms", NowUnixMillis()},
                             {"summary", PoolSummaryJson(snapshot.summary)},
                             {"transactions", std::move(entries)},
                             {"detail", nullptr}};
    if (boost::json::serialize(next).size() > kMaximumBytes)
      throw std::runtime_error("normalized pool exceeds 32 MiB display bound");
    captured_ = std::move(next);
    loaded_ = true;
  }
  if (captured_.empty()) {
    page["error"] = "Pool snapshot was not captured";
    return page;
  }
  for (auto field : {"source_node", "sampled_at_ms", "summary"})
    page[field] = captured_.at(field);
  const bool source_changed =
      !previous_source.empty() &&
      previous_source != JsonString(captured_, "source_node");
  if (source_changed)
    notice_ = "Source changed: " + previous_source + " -> " +
              JsonString(captured_, "source_node") + "; pools may disagree";
  page["notice"] = notice_;
  const auto& entries = captured_.at("transactions").as_array();
  std::size_t index = entries.empty() ? 0
                                      : std::min<std::size_t>(
                                            request.index, entries.size() - 1);
  if (!request.selected_id.empty()) {
    const auto found =
        std::find_if(entries.begin(), entries.end(), [&](const auto& entry) {
          return JsonString(entry.as_object(), "id") == request.selected_id;
        });
    if (found != entries.end())
      index = static_cast<std::size_t>(found - entries.begin());
    else {
      page["departed_id"] = request.selected_id;
      std::string reason = source_changed
                               ? "Not present on new source; reason unknown"
                               : "Left pool; removal reason unknown";
      if (reader && !source_changed && reader->departure) {
        try {
          const auto known = reader->departure(request.selected_id, stop);
          if (!known.empty()) reason = known;
        } catch (const std::exception&) {
          Check(stop);
        }
      }
      page["departure_reason"] = reason;
    }
  }
  page["selected_index"] = index;
  const auto first = std::min<std::size_t>(
      index >= request.limit / 2 ? index - request.limit / 2 : 0,
      entries.size() > request.limit ? entries.size() - request.limit : 0);
  page["first_index"] = first;
  for (std::size_t i = first; i < entries.size() && i - first < request.limit;
       ++i)
    page.at("rows").as_array().push_back(entries[i]);
  if (!entries.empty()) {
    const auto selected = JsonString(entries[index].as_object(), "id");
    page["selected_id"] = selected;
    if (reader) {
      try {
        const auto detail = reader->detail(snapshot.transactions[index], stop);
        if (detail.id != selected)
          throw std::runtime_error("pool detail identity mismatch");
        auto detail_json = PoolTransactionJson(detail);
        if (boost::json::serialize(captured_).size() +
                boost::json::serialize(detail_json).size() >
            kMaximumBytes)
          throw std::runtime_error(
              "selected pool detail exceeds capture bound");
        captured_["detail"] = std::move(detail_json);
      } catch (const std::exception& e) {
        Check(stop);
        page["detail_error"] = e.what();
      }
    }
    const auto* detail = captured_.if_contains("detail");
    page["detail"] = detail && detail->is_object() &&
                             JsonString(detail->as_object(), "id") == selected
                         ? *detail
                         : entries[index];
    if (!live && (!detail || !detail->is_object() ||
                  JsonString(detail->as_object(), "id") != selected))
      page["detail_error"] =
          "Additional transaction details were not captured; showing snapshot "
          "fields";
  }
  if (live) {
    Check(stop);
    const auto text = boost::json::serialize(captured_);
    if (text.size() > kMaximumBytes)
      throw std::runtime_error("captured pool exceeds 32 MiB bound");
    const auto temporary = path_.string() + ".tmp-" + std::to_string(getpid());
    WriteText(temporary, text);
    std::filesystem::rename(temporary, path_);
    if (observer_) observer_(page);
  }
  return page;
}
}  // namespace bbp
